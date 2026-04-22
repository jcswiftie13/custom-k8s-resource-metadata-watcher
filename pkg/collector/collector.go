package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/sink"
)

// Collector wires informers, resolver and evaluator together and pushes
// computed series into a MetadataSink.
type Collector struct {
	cfg       *config.Config
	informers *ScopedInformers
	resolver  *Resolver
	evaluator *Evaluator
	sink      sink.MetadataSink
	log       *slog.Logger

	// compiled rules indexed by anchor kind.
	byAnchor map[string][]*CompiledRule

	// rules that read from any non-anchor kind; when those kinds are
	// mutated we walk every anchor of each affected rule.
	rulesByReadKind map[string][]*CompiledRule

	// Fully-qualified metric names per rule, matching what sink expects.
	metricNames map[string]string

	once sync.Once
}

// New constructs a Collector. The caller is responsible for calling Start.
func New(cfg *config.Config, client kubernetes.Interface, s sink.MetadataSink, log *slog.Logger) (*Collector, error) {
	if log == nil {
		log = slog.Default()
	}
	infs := NewScopedInformers(client, cfg.Watch, log)
	c := &Collector{
		cfg:             cfg,
		informers:       infs,
		resolver:        NewResolver(infs, log),
		evaluator:       NewEvaluator(),
		sink:            s,
		log:             log,
		byAnchor:        map[string][]*CompiledRule{},
		rulesByReadKind: map[string][]*CompiledRule{},
		metricNames:     map[string]string{},
	}

	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		compiled, err := Compile(rule)
		if err != nil {
			return nil, err
		}
		metricName := cfg.MetricName(rule)
		c.metricNames[rule.Name] = metricName
		c.byAnchor[rule.Anchor] = append(c.byAnchor[rule.Anchor], compiled)

		readKinds := collectReadKinds(rule)
		for kind := range readKinds {
			if kind == rule.Anchor {
				continue
			}
			c.rulesByReadKind[kind] = append(c.rulesByReadKind[kind], compiled)
		}

		if err := s.RegisterRule(sink.RuleSchema{
			Name:   metricName,
			Help:   ruleHelp(rule),
			Labels: compiled.LabelOrder,
		}); err != nil {
			return nil, fmt.Errorf("register rule %q: %w", rule.Name, err)
		}
	}
	return c, nil
}

func ruleHelp(r *config.Rule) string {
	if strings.TrimSpace(r.Help) != "" {
		return r.Help
	}
	return fmt.Sprintf("Metadata series for anchor=%s (auto-generated).", r.Anchor)
}

// collectReadKinds returns every kind name that may be read when evaluating
// the rule (excluding "item"/"anchor" which are not kind names).
func collectReadKinds(r *config.Rule) map[string]struct{} {
	out := map[string]struct{}{}
	addSource := func(src string) {
		resolved := r.ResolveRelation(src)
		switch resolved {
		case "", "anchor", "item":
			return
		case "ownerController", "topController":
			// These may resolve to any supported kind; be conservative and
			// register the rule against every parent kind so updates to any
			// parent trigger a requeue.
			for _, k := range []string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"} {
				out[k] = struct{}{}
			}
			return
		}
		out[resolved] = struct{}{}
	}
	for _, ext := range r.Labels {
		addSource(ext.EffectiveSource())
		for _, f := range ext.Fallbacks {
			addSource(f.EffectiveSource())
		}
	}
	for _, fl := range r.Flatten {
		addSource(fl.EffectiveSource())
	}
	return out
}

// Start performs dry-run, launches informers, and registers event handlers.
// Start blocks until ctx is cancelled.
func (c *Collector) Start(ctx context.Context) error {
	c.informers.LogDanglingSelectorWarnings()
	if err := c.informers.DryRunSelectors(ctx); err != nil {
		return err
	}
	c.registerHandlers()
	if err := c.informers.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// registerHandlers attaches anchor + parent-kind event handlers.
func (c *Collector) registerHandlers() {
	for _, kind := range allKinds {
		informers := c.informers.Informers(kind)
		for _, inf := range informers {
			c.attachAnchorHandler(kind, inf)
			c.attachParentHandler(kind, inf)
		}
	}
}

func (c *Collector) attachAnchorHandler(kind string, inf cache.SharedIndexInformer) {
	rules, has := c.byAnchor[kind]
	if !has || len(rules) == 0 {
		return
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if o, ok := asRuntimeObject(obj); ok {
				c.reconcileAnchor(o, rules)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if o, ok := asRuntimeObject(newObj); ok {
				c.reconcileAnchor(o, rules)
			}
		},
		DeleteFunc: func(obj interface{}) {
			o, ok := asRuntimeObject(obj)
			if !ok {
				if ts, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
					o, ok = asRuntimeObject(ts.Obj)
				}
			}
			if !ok {
				return
			}
			anchorKey := NamespaceName(o)
			for _, rule := range rules {
				c.sink.ReplaceForAnchor(c.metricNames[rule.Rule.Name], anchorKey, nil)
			}
		},
	})
}

// attachParentHandler requeues rules whose non-anchor sources changed.
// v1 simplification: on any such event we re-evaluate every anchor in the
// affected namespace. This keeps the control flow simple at the cost of
// doing more work when parents change; anchors are bounded by the selector
// scope so this remains cheap in practice.
func (c *Collector) attachParentHandler(kind string, inf cache.SharedIndexInformer) {
	rules, has := c.rulesByReadKind[kind]
	if !has || len(rules) == 0 {
		return
	}
	requeue := func(obj interface{}) {
		o, ok := asRuntimeObject(obj)
		if !ok {
			if ts, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
				o, ok = asRuntimeObject(ts.Obj)
			}
		}
		if !ok {
			return
		}
		meta, ok := metaAccessor(o)
		if !ok {
			return
		}
		ns := meta.GetNamespace()
		byAnchorKind := map[string][]*CompiledRule{}
		for _, r := range rules {
			byAnchorKind[r.Rule.Anchor] = append(byAnchorKind[r.Rule.Anchor], r)
		}
		for anchorKind, rs := range byAnchorKind {
			for _, anchor := range c.informers.ListAll(anchorKind) {
				if ns != "" {
					if m, ok := metaAccessor(anchor); ok && m.GetNamespace() != ns {
						continue
					}
				}
				c.reconcileAnchor(anchor, rs)
			}
		}
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    requeue,
		UpdateFunc: func(_, newObj interface{}) { requeue(newObj) },
		DeleteFunc: requeue,
	})
}

// reconcileAnchor runs evaluation for every matching rule and pushes the
// resulting series to the sink.
func (c *Collector) reconcileAnchor(obj runtime.Object, rules []*CompiledRule) {
	chain := c.resolver.Resolve(obj)

	anchorMap, err := c.evaluator.ToUnstructured(obj)
	if err != nil {
		c.log.Warn("convert anchor to unstructured failed", "err", err)
		return
	}
	anchorKey := NamespaceName(obj)

	maps := make(map[string]map[string]interface{}, len(chain))
	for k, v := range chain {
		m, err := c.evaluator.ToUnstructured(v)
		if err != nil {
			c.log.Debug("convert chain object failed", "source", k, "err", err)
			continue
		}
		maps[k] = m
	}

	for _, cr := range rules {
		items := c.evaluator.EvaluateForEach(cr, anchorMap)
		seriesByKey := map[string]map[string]string{}
		for _, item := range items {
			labels := map[string]string{}
			for _, cl := range cr.Labels {
				labels[cl.Name] = c.evaluator.EvaluateLabel(cl, func(source string) map[string]interface{} {
					if source == "item" {
						return item
					}
					return maps[source]
				})
			}
			seriesKey := buildSeriesKey(cr, anchorKey, labels)
			seriesByKey[seriesKey] = labels
		}
		c.sink.ReplaceForAnchor(c.metricNames[cr.Rule.Name], anchorKey, seriesByKey)
	}
}

// buildSeriesKey derives a stable key for a series. It uses the anchor key
// plus every label value (in canonical order) so that distinct series from
// the same anchor (e.g. different containers) do not collide.
func buildSeriesKey(cr *CompiledRule, anchorKey string, labels map[string]string) string {
	var b strings.Builder
	b.WriteString(anchorKey)
	for _, name := range cr.LabelOrder {
		b.WriteByte('|')
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(labels[name])
	}
	return b.String()
}

// asRuntimeObject defensively casts an informer payload to runtime.Object.
func asRuntimeObject(obj interface{}) (runtime.Object, bool) {
	if obj == nil {
		return nil, false
	}
	switch v := obj.(type) {
	case *corev1.Pod:
		return v, true
	case *appsv1.ReplicaSet:
		return v, true
	case *appsv1.Deployment:
		return v, true
	case *appsv1.StatefulSet:
		return v, true
	case *appsv1.DaemonSet:
		return v, true
	case runtime.Object:
		return v, true
	}
	return nil, false
}

// metav1 unused-import guard.
var _ = metav1.ObjectMeta{}
