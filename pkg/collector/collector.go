// Package collector wires SharedInformer caches, an owner-chain resolver,
// and a path-evaluator into a custom prometheus.Collector. Every Prometheus
// scrape walks the informer caches and emits one constant-value gauge per
// (anchor, forEach-item) tuple. Series carry both a fixed set of labels
// declared by the rule and a dynamic label set produced by expandLabels
// (kube_pod_labels-style flattening of metadata maps).
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	"github.com/example/metadata-exporter/pkg/config"
)

// Options tunes runtime behavior of the Collector. All fields are optional.
type Options struct {
	// Registerer receives self-metrics (collect duration, totals, anchor
	// count). Nil disables self-metric registration (handy in tests).
	Registerer prometheus.Registerer
}

// Collector implements prometheus.Collector. It owns the informer
// lifecycle (Start/Stop) and the rule compilation, but holds no per-series
// state — every scrape rebuilds output from the informer cache.
type Collector struct {
	cfg       *config.Config
	informers *ScopedInformers
	resolver  *Resolver
	evaluator *Evaluator
	log       *slog.Logger

	rules       []*CompiledRule
	metricNames map[string]string

	// scrapeMu serialises Collect calls so concurrent scrapes do not
	// double-charge self-metrics or interleave per-rule logging in
	// confusing ways. Collect remains cheap: scrapes are typically
	// seconds-scale apart and the lock only spans cache iteration.
	scrapeMu sync.Mutex

	self *selfMetrics
}

// New constructs a Collector. The caller is responsible for:
//   - Calling Start to drive the informers
//   - Registering the Collector with a prometheus.Registerer to expose its
//     metrics (Collect is invoked on every scrape).
func New(cfg *config.Config, client kubernetes.Interface, log *slog.Logger, opts Options) (*Collector, error) {
	if log == nil {
		log = slog.Default()
	}
	infs := NewScopedInformers(client, cfg.Watch, log)
	c := &Collector{
		cfg:         cfg,
		informers:   infs,
		resolver:    NewResolver(infs, log),
		evaluator:   NewEvaluator(),
		log:         log,
		metricNames: map[string]string{},
	}

	ruleAnchors := map[string]string{}
	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		compiled, err := Compile(rule)
		if err != nil {
			return nil, err
		}
		c.rules = append(c.rules, compiled)
		c.metricNames[rule.Name] = cfg.MetricName(rule)
		ruleAnchors[rule.Name] = rule.Anchor
	}

	c.self = newSelfMetrics(opts.Registerer, ruleAnchors)
	return c, nil
}

// Start launches informers, runs a one-shot LIST dry-run for selectors so
// configuration mistakes surface immediately, and blocks until ctx is
// cancelled. Start does NOT trigger metric emission; Prometheus pulls via
// Collect on every scrape.
func (c *Collector) Start(ctx context.Context) error {
	c.logParentChainKindGaps()
	c.informers.LogDanglingSelectorWarnings()
	if err := c.informers.DryRunSelectors(ctx); err != nil {
		return err
	}
	if err := c.informers.Start(ctx); err != nil {
		return err
	}
	c.log.Info("collector started", "rules", len(c.rules))
	<-ctx.Done()
	return nil
}

// Describe implements prometheus.Collector. We are an "unchecked collector":
// every rule's metric label set may differ between scrapes (because
// expandLabels are dynamic), so we describe nothing and let the registry
// validate per-scrape consistency.
func (c *Collector) Describe(_ chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector. For every configured rule it:
//  1. Lists every anchor of cr.Rule.Anchor from the informer cache.
//  2. Resolves the owner chain (Pod -> ReplicaSet -> Deployment, etc.).
//  3. Evaluates fixed labels and any expandLabels per series.
//  4. Computes the union of dynamic label keys observed in this scrape and
//     emits each series with values aligned to that union (missing dynamic
//     label values are emitted as the empty string).
//
// This guarantees prometheus.Registry's "all metrics with the same name
// share the same label name set" invariant on a per-scrape basis without
// keeping any state between scrapes.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.scrapeMu.Lock()
	defer c.scrapeMu.Unlock()
	for _, cr := range c.rules {
		c.collectRule(cr, ch)
	}
}

// pendingSeries is one materialised series before label-union finalisation.
type pendingSeries struct {
	fixed   []string          // aligned with cr.LabelOrder
	dynamic map[string]string // sanitised (already prefixed) dynamic label name -> value
}

func (c *Collector) collectRule(cr *CompiledRule, ch chan<- prometheus.Metric) {
	started := time.Now()
	defer func() {
		c.self.observeCollectDuration(cr.Rule.Name, time.Since(started))
	}()

	anchors := c.informers.ListAll(cr.Rule.Anchor)
	c.self.observeAnchorCount(cr.Rule.Name, cr.Rule.Anchor, len(anchors))
	if len(anchors) == 0 {
		c.self.incCollect(cr.Rule.Name, "ok")
		return
	}

	pending, dynamicKeys, err := c.materialiseSeries(cr, anchors)
	if err != nil {
		c.self.incCollect(cr.Rule.Name, "error")
		c.log.Warn("rule materialisation failed", "rule", cr.Rule.Name, "err", err)
		return
	}

	c.emitSeries(ch, cr, pending, dynamicKeys)
	c.self.incCollect(cr.Rule.Name, "ok")
}

// materialiseSeries iterates anchors, resolves their owner chains, and
// builds one pendingSeries per (anchor, forEach-item). It also accumulates
// the union of dynamic label keys observed across all series.
func (c *Collector) materialiseSeries(cr *CompiledRule, anchors []runtime.Object) ([]pendingSeries, []string, error) {
	pending := make([]pendingSeries, 0, len(anchors))
	dynamicSet := map[string]struct{}{}

	for _, anchor := range anchors {
		anchorMap, err := c.evaluator.ToUnstructured(anchor)
		if err != nil {
			c.log.Warn("anchor unstructured failed", "rule", cr.Rule.Name, "err", err)
			continue
		}
		chain := c.resolver.Resolve(anchor)

		sourceMaps := make(map[string]map[string]interface{}, len(chain))
		for k, v := range chain {
			m, err := c.evaluator.ToUnstructured(v)
			if err != nil {
				c.log.Debug("chain object unstructured failed", "rule", cr.Rule.Name, "source", k, "err", err)
				continue
			}
			sourceMaps[k] = m
		}

		items := c.evaluator.EvaluateForEach(cr, anchorMap)
		for _, item := range items {
			lookup := newSrcLookup(sourceMaps, item)
			ps := c.evaluateOneSeries(cr, lookup)
			for k := range ps.dynamic {
				dynamicSet[k] = struct{}{}
			}
			pending = append(pending, ps)
		}
	}

	dynamicKeys := make([]string, 0, len(dynamicSet))
	for k := range dynamicSet {
		dynamicKeys = append(dynamicKeys, k)
	}
	sort.Strings(dynamicKeys)
	return pending, dynamicKeys, nil
}

// evaluateOneSeries computes both fixed labels (in cr.LabelOrder order) and
// dynamic expand labels for one series.
func (c *Collector) evaluateOneSeries(cr *CompiledRule, lookup func(string) map[string]interface{}) pendingSeries {
	fixed := make([]string, len(cr.LabelOrder))
	for i, cl := range cr.Labels {
		fixed[i] = c.evaluator.EvaluateLabel(cl, lookup)
	}
	var dynamic map[string]string
	for i := range cr.Expands {
		out := c.evaluator.EvaluateExpand(cr.Expands[i], lookup)
		if dynamic == nil && len(out) > 0 {
			dynamic = make(map[string]string, len(out))
		}
		for k, v := range out {
			dynamic[k] = v
		}
	}
	return pendingSeries{fixed: fixed, dynamic: dynamic}
}

// emitSeries writes one prometheus.Metric per pendingSeries using a single
// Desc whose label name set is the union of fixed + dynamic labels.
func (c *Collector) emitSeries(ch chan<- prometheus.Metric, cr *CompiledRule, pending []pendingSeries, dynamicKeys []string) {
	labelNames := make([]string, 0, len(cr.LabelOrder)+len(dynamicKeys))
	labelNames = append(labelNames, cr.LabelOrder...)
	labelNames = append(labelNames, dynamicKeys...)

	desc := prometheus.NewDesc(
		c.metricNames[cr.Rule.Name],
		ruleHelp(cr.Rule),
		labelNames,
		nil,
	)

	values := make([]string, len(labelNames))
	for _, ps := range pending {
		copy(values, ps.fixed)
		for i, name := range dynamicKeys {
			values[len(cr.LabelOrder)+i] = ps.dynamic[name]
		}
		m, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, 1, values...)
		if err != nil {
			c.log.Warn("metric construction failed",
				"rule", cr.Rule.Name,
				"err", err,
			)
			continue
		}
		ch <- m
	}
}

// newSrcLookup builds the per-series source -> map[string]interface{}
// callback consumed by Evaluator.{EvaluateLabel,EvaluateExpand}.
func newSrcLookup(sourceMaps map[string]map[string]interface{}, item map[string]interface{}) func(string) map[string]interface{} {
	return func(source string) map[string]interface{} {
		if source == "item" {
			return item
		}
		return sourceMaps[source]
	}
}

func ruleHelp(r *config.Rule) string {
	if strings.TrimSpace(r.Help) != "" {
		return r.Help
	}
	return fmt.Sprintf("Metadata series for anchor=%s (auto-generated).", r.Anchor)
}

// logParentChainKindGaps emits a startup warning when any rule reads from
// ownerController/topController but the corresponding parent kind is not
// configured to be watched. Owner-chain resolution will silently miss in
// that case.
func (c *Collector) logParentChainKindGaps() {
	if !c.anyRuleUsesParentChain() {
		return
	}
	en := c.informers.EnabledKindSet()
	var missing []string
	for _, k := range []string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"} {
		if _, ok := en[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return
	}
	c.log.Warn(
		"rules reference ownerController/topController but not all parent kinds are watched; owner chain resolution may miss",
		"missingParentKinds", missing,
		"watchedKinds", c.informers.WatchedKinds(),
	)
}

func (c *Collector) anyRuleUsesParentChain() bool {
	for _, cr := range c.rules {
		if ruleUsesParentChain(cr.Rule) {
			return true
		}
	}
	return false
}

func ruleUsesParentChain(r *config.Rule) bool {
	check := func(src string) bool {
		return src == "ownerController" || src == "topController"
	}
	for _, ext := range r.Labels {
		if check(ext.EffectiveSource()) {
			return true
		}
		for _, f := range ext.Fallbacks {
			if check(f.EffectiveSource()) {
				return true
			}
		}
	}
	for _, ex := range r.ExpandLabels {
		if check(ex.EffectiveSource()) {
			return true
		}
	}
	return false
}
