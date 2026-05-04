package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/sink"
)

// defaultWorkers is the number of reconcile goroutines started when the
// caller does not override it. A small pool gives meaningful parallelism
// while bounding concurrent calls to the sink.
const defaultWorkers = 4

// Options tunes runtime behavior of the collector. All fields are optional
// and zero values pick sensible defaults.
type Options struct {
	// Workers is the number of reconcile goroutines. Defaults to 4.
	Workers int

	// Registerer receives self-metrics (queue depth, reconcile totals, ...).
	// Nil disables self-metrics registration (useful in tests).
	Registerer prometheus.Registerer
}

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

	// queue dedupes and rate-limits reconcile work. Informer handlers push
	// anchorRefs into it and a small worker pool drains it.
	queue workqueue.TypedRateLimitingInterface[anchorRef]

	// parents is the reverse index parentUID -> {anchorRef} used to route
	// parent events to exactly the anchors that observed them last.
	parents *parentIndex

	workers int
	metrics *selfMetrics

	once sync.Once
}

// New constructs a Collector. The caller is responsible for calling Start.
func New(cfg *config.Config, client kubernetes.Interface, s sink.MetadataSink, log *slog.Logger, opts Options) (*Collector, error) {
	if log == nil {
		log = slog.Default()
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = defaultWorkers
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
		parents:         newParentIndex(),
		workers:         workers,
	}

	c.queue = workqueue.NewTypedRateLimitingQueueWithConfig[anchorRef](
		workqueue.DefaultTypedControllerRateLimiter[anchorRef](),
		workqueue.TypedRateLimitingQueueConfig[anchorRef]{Name: "metadata-exporter-reconcile"},
	)

	c.metrics = newSelfMetrics(opts.Registerer, sizeProviders{
		queueDepth: func() float64 { return float64(c.queue.Len()) },
		parentByParent: func() float64 {
			byParent, _ := c.parents.Len()
			return float64(byParent)
		},
		parentByAnchor: func() float64 {
			_, byAnchor := c.parents.Len()
			return float64(byAnchor)
		},
	})

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
	for i := range c.cfg.Rules {
		if ruleUsesParentChain(&c.cfg.Rules[i]) {
			return true
		}
	}
	return false
}

func ruleUsesParentChain(r *config.Rule) bool {
	try := func(src string) bool {
		res := r.ResolveRelation(src)
		return res == "ownerController" || res == "topController"
	}
	for _, ext := range r.Labels {
		if try(ext.EffectiveSource()) {
			return true
		}
		for _, f := range ext.Fallbacks {
			if try(f.EffectiveSource()) {
				return true
			}
		}
	}
	return false
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
	return out
}

// Start performs dry-run, launches informers, and registers event handlers.
// Start blocks until ctx is cancelled.
func (c *Collector) Start(ctx context.Context) error {
	c.logParentChainKindGaps()
	c.informers.LogDanglingSelectorWarnings()
	if err := c.informers.DryRunSelectors(ctx); err != nil {
		return err
	}
	c.registerHandlers()
	if err := c.informers.Start(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.runWorker(ctx)
		}()
	}
	c.log.Info("reconcile workers started", "count", c.workers)

	<-ctx.Done()
	c.queue.ShutDown()
	wg.Wait()
	return nil
}

// runWorker pulls items off the queue until it is shut down.
func (c *Collector) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

// processNext blocks for one queue item, runs reconcile, and handles retry
// semantics. It returns false only after the queue has been shut down.
func (c *Collector) processNext(ctx context.Context) bool {
	ref, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(ref)

	if ctx.Err() != nil {
		return false
	}

	err := c.reconcileRef(ref)
	if err != nil {
		c.log.Warn("reconcile failed; requeueing", "ref", ref, "err", err)
		c.queue.AddRateLimited(ref)
		return true
	}
	c.queue.Forget(ref)
	return true
}

// registerHandlers attaches anchor + parent-kind event handlers.
func (c *Collector) registerHandlers() {
	for _, kind := range c.informers.WatchedKinds() {
		informers := c.informers.Informers(kind)
		for _, inf := range informers {
			c.attachAnchorHandler(kind, inf)
			c.attachParentHandler(kind, inf)
		}
	}
}

// enqueueAnchor pushes an anchorRef onto the workqueue. The queue dedupes
// identical refs already pending, so burst updates collapse naturally.
func (c *Collector) enqueueAnchor(ref anchorRef) {
	c.queue.Add(ref)
}

// enqueueObject derives an anchorRef from a cached object and enqueues it.
func (c *Collector) enqueueObject(obj runtime.Object, kind string) {
	m, ok := metaAccessor(obj)
	if !ok {
		return
	}
	c.enqueueAnchor(anchorRef{
		AnchorKind: kind,
		Namespace:  m.GetNamespace(),
		Name:       m.GetName(),
	})
}

func (c *Collector) attachAnchorHandler(kind string, inf cache.SharedIndexInformer) {
	if _, has := c.byAnchor[kind]; !has {
		return
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			o, ok := asRuntimeObject(obj)
			if !ok {
				return
			}
			c.enqueueObject(o, kind)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newO, ok := asRuntimeObject(newObj)
			if !ok {
				return
			}
			oldO, _ := asRuntimeObject(oldObj)
			if oldO != nil && !updateEnqueueCandidate(oldO, newO) {
				return
			}
			c.enqueueObject(newO, kind)
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
			m, mok := metaAccessor(o)
			if !mok {
				return
			}
			ref := anchorRef{AnchorKind: kind, Namespace: m.GetNamespace(), Name: m.GetName()}
			c.parents.Forget(ref)
			anchorKey := NamespaceName(o)
			for _, rule := range c.byAnchor[kind] {
				c.sink.ReplaceForAnchor(c.metricNames[rule.Rule.Name], anchorKey, nil)
			}
		},
	})
}

// attachParentHandler routes parent events to the anchors that depend on
// them using the reverse index, falling back to a per-namespace rescan only
// on cold lookups.
func (c *Collector) attachParentHandler(kind string, inf cache.SharedIndexInformer) {
	rules, has := c.rulesByReadKind[kind]
	if !has || len(rules) == 0 {
		return
	}
	dispatch := func(obj interface{}) {
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
		// Index lookup first.
		if refs, hit := c.parents.AnchorsFor(meta.GetUID()); hit {
			c.metrics.parentIndexed.Inc()
			for _, ref := range refs {
				c.enqueueAnchor(ref)
			}
			return
		}
		// Cold-path fallback: enqueue every anchor in the same namespace
		// for every rule that reads this kind.
		c.metrics.parentFallback.Inc()
		ns := meta.GetNamespace()
		byAnchorKind := map[string]struct{}{}
		for _, r := range rules {
			byAnchorKind[r.Rule.Anchor] = struct{}{}
		}
		for anchorKind := range byAnchorKind {
			for _, anchor := range c.informers.ListAll(anchorKind) {
				am, aok := metaAccessor(anchor)
				if !aok {
					continue
				}
				if ns != "" && am.GetNamespace() != ns {
					continue
				}
				c.enqueueAnchor(anchorRef{
					AnchorKind: anchorKind,
					Namespace:  am.GetNamespace(),
					Name:       am.GetName(),
				})
			}
		}
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { dispatch(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) {
			newO, ok := asRuntimeObject(newObj)
			if !ok {
				return
			}
			oldO, _ := asRuntimeObject(oldObj)
			if oldO != nil && !updateEnqueueCandidate(oldO, newO) {
				return
			}
			dispatch(newObj)
		},
		DeleteFunc: func(obj interface{}) { dispatch(obj) },
	})
}

// reconcileRef reads the latest cached state of the anchor identified by
// ref and runs reconcileAnchor against every rule compiled for its kind.
func (c *Collector) reconcileRef(ref anchorRef) error {
	rules, ok := c.byAnchor[ref.AnchorKind]
	if !ok || len(rules) == 0 {
		return nil
	}
	obj, err := c.informers.Get(ref.AnchorKind, ref.Namespace, ref.Name)
	if err != nil || obj == nil {
		// Anchor gone; delete handler already purged its series and index
		// entries. No reason to requeue.
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	c.reconcileAnchor(obj, ref, rules)
	return nil
}

// reconcileAnchor runs evaluation for every matching rule and pushes the
// resulting series to the sink.
func (c *Collector) reconcileAnchor(obj runtime.Object, ref anchorRef, rules []*CompiledRule) {
	started := time.Now()
	chain := c.resolver.Resolve(obj)

	anchorMap, err := c.evaluator.ToUnstructured(obj)
	if err != nil {
		c.log.Warn("convert anchor to unstructured failed", "err", err)
		for _, cr := range rules {
			c.metrics.reconcileTotal.WithLabelValues(cr.Rule.Name, "error").Inc()
		}
		return
	}
	anchorKey := NamespaceName(obj)

	maps := make(map[string]map[string]interface{}, len(chain))
	parentUIDs := make([]types.UID, 0, len(chain))
	seenUID := map[types.UID]struct{}{}
	for k, v := range chain {
		m, err := c.evaluator.ToUnstructured(v)
		if err != nil {
			c.log.Debug("convert chain object failed", "source", k, "err", err)
			continue
		}
		maps[k] = m
		if pm, ok := metaAccessor(v); ok {
			uid := pm.GetUID()
			if uid == "" {
				continue
			}
			if _, dup := seenUID[uid]; dup {
				continue
			}
			seenUID[uid] = struct{}{}
			parentUIDs = append(parentUIDs, uid)
		}
	}

	// Record the reverse index before emitting to the sink so that a
	// downstream parent event observed concurrently with this reconcile
	// cannot miss the newly-established link and fall back to a rescan.
	c.parents.Record(ref, parentUIDs)

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
		c.metrics.reconcileTotal.WithLabelValues(cr.Rule.Name, "ok").Inc()
	}

	c.metrics.reconcileDur.WithLabelValues(ref.AnchorKind).Observe(time.Since(started).Seconds())
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

// updateEnqueueCandidate reports whether an informer Update should enqueue
// work. Identical resourceVersion pairs are skipped so duplicate relist
// deliveries do not churn the queue; any real apiserver revision bump
// (including status-only) enqueues, matching kube-state-metrics semantics.
func updateEnqueueCandidate(oldObj, newObj runtime.Object) bool {
	if oldObj == nil {
		return true
	}
	oldMeta, okOld := metaAccessor(oldObj)
	newMeta, okNew := metaAccessor(newObj)
	if !okOld || !okNew {
		return true
	}
	return oldMeta.GetResourceVersion() != newMeta.GetResourceVersion()
}

