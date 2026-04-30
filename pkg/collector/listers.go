package collector

import (
	"context"
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/config"
)

// ScopedInformers holds one SharedInformerFactory per (namespace, kind),
// allowing per-kind label/field selectors without cross-contamination.
//
// The empty-string namespace represents cluster-wide scope (used when
// Config.Watch.Namespaces is empty).
type ScopedInformers struct {
	client   kubernetes.Interface
	log      *slog.Logger
	resync   int
	watch    config.WatchScope
	kinds    []string
	kindSet  map[string]struct{}
	selector map[string]config.KindWatch // per watched kind (resolved KindWatch)

	// factories[namespace][kind] -> SharedInformerFactory tweaked for that kind.
	factories map[string]map[string]informers.SharedInformerFactory

	// Kind-specific typed informers/listers, keyed by namespace. Absent for unwatched kinds.
	podInformers map[string]cache.SharedIndexInformer
	rsInformers  map[string]cache.SharedIndexInformer
	depInformers map[string]cache.SharedIndexInformer
	stsInformers map[string]cache.SharedIndexInformer
	dsInformers  map[string]cache.SharedIndexInformer

	podListers map[string]corelisters.PodLister
	rsListers  map[string]appslisters.ReplicaSetLister
	depListers map[string]appslisters.DeploymentLister
	stsListers map[string]appslisters.StatefulSetLister
	dsListers  map[string]appslisters.DaemonSetLister

	// When true, namespace "" is the only lister key for cluster-scoped mode.
	clusterWide bool

	namespaces []string
}

// NewScopedInformers constructs factories for every (namespace, kind) using
// the supplied watch scope. When w.Namespaces is empty a single cluster-wide
// scope ("") is used; one factory per (namespace, kind) is created, so the
// apiserver sees N_namespaces_or_1 * N_watched_kinds watches.
func NewScopedInformers(client kubernetes.Interface, w config.WatchScope, log *slog.Logger) *ScopedInformers {
	if log == nil {
		log = slog.Default()
	}
	namespaces := w.Namespaces
	clusterWide := len(namespaces) == 0
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	kinds := w.EffectiveKinds()
	selector := make(map[string]config.KindWatch, len(kinds))
	ks := make(map[string]struct{}, len(kinds))
	for _, k := range kinds {
		ks[k] = struct{}{}
		selector[k] = w.KindWatchFor(k)
	}
	if clusterWide {
		log.Info("watch mode = cluster-wide",
			"factoriesPerKind", 1,
			"watchKinds", kinds,
		)
	} else {
		log.Info("watch mode = per-namespace",
			"namespaces", namespaces,
			"factoriesPerKind", len(namespaces),
			"watchKinds", kinds,
		)
	}
	si := &ScopedInformers{
		client:       client,
		log:          log,
		watch:        w,
		kinds:        kinds,
		kindSet:      ks,
		selector:     selector,
		factories:    map[string]map[string]informers.SharedInformerFactory{},
		podInformers: map[string]cache.SharedIndexInformer{},
		rsInformers:  map[string]cache.SharedIndexInformer{},
		depInformers: map[string]cache.SharedIndexInformer{},
		stsInformers: map[string]cache.SharedIndexInformer{},
		dsInformers:  map[string]cache.SharedIndexInformer{},
		podListers:   map[string]corelisters.PodLister{},
		rsListers:    map[string]appslisters.ReplicaSetLister{},
		depListers:   map[string]appslisters.DeploymentLister{},
		stsListers:   map[string]appslisters.StatefulSetLister{},
		dsListers:    map[string]appslisters.DaemonSetLister{},
		clusterWide:  clusterWide,
		namespaces:   namespaces,
	}

	for _, ns := range namespaces {
		perKind := make(map[string]informers.SharedInformerFactory, len(kinds))
		for _, kind := range kinds {
			sel := selector[kind]
			opts := []informers.SharedInformerOption{
				informers.WithTweakListOptions(func(o *metav1.ListOptions) {
					if sel.LabelSelector != "" {
						o.LabelSelector = sel.LabelSelector
					}
					if sel.FieldSelector != "" {
						o.FieldSelector = sel.FieldSelector
					}
				}),
			}
			if ns != "" {
				opts = append(opts, informers.WithNamespace(ns))
			}
			perKind[kind] = informers.NewSharedInformerFactoryWithOptions(client, 0, opts...)
		}
		si.factories[ns] = perKind

		for _, kind := range kinds {
			f := perKind[kind]
			switch kind {
			case "Pod":
				si.podInformers[ns] = f.Core().V1().Pods().Informer()
				si.podListers[ns] = f.Core().V1().Pods().Lister()
			case "ReplicaSet":
				si.rsInformers[ns] = f.Apps().V1().ReplicaSets().Informer()
				si.rsListers[ns] = f.Apps().V1().ReplicaSets().Lister()
			case "Deployment":
				si.depInformers[ns] = f.Apps().V1().Deployments().Informer()
				si.depListers[ns] = f.Apps().V1().Deployments().Lister()
			case "StatefulSet":
				si.stsInformers[ns] = f.Apps().V1().StatefulSets().Informer()
				si.stsListers[ns] = f.Apps().V1().StatefulSets().Lister()
			case "DaemonSet":
				si.dsInformers[ns] = f.Apps().V1().DaemonSets().Informer()
				si.dsListers[ns] = f.Apps().V1().DaemonSets().Lister()
			}
		}
	}
	return si
}

// WatchedKinds returns a copy of the kind names this scope watches, in fixed order.
func (s *ScopedInformers) WatchedKinds() []string {
	return append([]string(nil), s.kinds...)
}

// HasKind returns true if this informer set watches the given kind.
func (s *ScopedInformers) HasKind(kind string) bool {
	_, ok := s.kindSet[kind]
	return ok
}

// EnabledKindSet returns the set of watched kinds.
func (s *ScopedInformers) EnabledKindSet() map[string]struct{} {
	out := make(map[string]struct{}, len(s.kinds))
	for k := range s.kindSet {
		out[k] = struct{}{}
	}
	return out
}

// Start launches all informers and waits for initial cache sync.
func (s *ScopedInformers) Start(ctx context.Context) error {
	for _, perKind := range s.factories {
		for _, f := range perKind {
			f.Start(ctx.Done())
		}
	}
	for ns, perKind := range s.factories {
		for kind, f := range perKind {
			synced := f.WaitForCacheSync(ctx.Done())
			for typ, ok := range synced {
				if !ok {
					return fmt.Errorf("informer cache sync failed: namespace=%q kind=%s type=%v", ns, kind, typ)
				}
			}
		}
	}
	return nil
}

// DryRunSelectors issues one small List per (namespace, kind) that has any
// selector configured, so bad field selectors are rejected on startup.
func (s *ScopedInformers) DryRunSelectors(ctx context.Context) error {
	for _, ns := range s.namespaces {
		for _, kind := range s.kinds {
			sel, ok := s.selector[kind]
			if !ok || (sel.LabelSelector == "" && sel.FieldSelector == "") {
				continue
			}
			opts := metav1.ListOptions{
				LabelSelector: sel.LabelSelector,
				FieldSelector: sel.FieldSelector,
				Limit:         1,
			}
			var err error
			switch kind {
			case "Pod":
				_, err = s.client.CoreV1().Pods(ns).List(ctx, opts)
			case "ReplicaSet":
				_, err = s.client.AppsV1().ReplicaSets(ns).List(ctx, opts)
			case "Deployment":
				_, err = s.client.AppsV1().Deployments(ns).List(ctx, opts)
			case "StatefulSet":
				_, err = s.client.AppsV1().StatefulSets(ns).List(ctx, opts)
			case "DaemonSet":
				_, err = s.client.AppsV1().DaemonSets(ns).List(ctx, opts)
			}
			if err != nil {
				return fmt.Errorf("dry-run list %s in ns=%q with selector %+v: %w", kind, ns, sel, err)
			}
		}
	}
	return nil
}

// Get implements ListerGetter by consulting the cache for (kind, namespace, name).
// Cluster-wide factories are used when no namespace-scoped lister is configured.
func (s *ScopedInformers) Get(kind, namespace, name string) (runtime.Object, error) {
	if !s.HasKind(kind) {
		return nil, notFoundf("kind %q is not watched", kind)
	}
	nsKey := s.nsKey(namespace)
	switch kind {
	case "Pod":
		l, ok := s.podListers[nsKey]
		if !ok {
			return nil, notFoundf("pod lister for ns=%q missing", namespace)
		}
		return l.Pods(namespace).Get(name)
	case "ReplicaSet":
		l, ok := s.rsListers[nsKey]
		if !ok {
			return nil, notFoundf("replicaset lister for ns=%q missing", namespace)
		}
		return l.ReplicaSets(namespace).Get(name)
	case "Deployment":
		l, ok := s.depListers[nsKey]
		if !ok {
			return nil, notFoundf("deployment lister for ns=%q missing", namespace)
		}
		return l.Deployments(namespace).Get(name)
	case "StatefulSet":
		l, ok := s.stsListers[nsKey]
		if !ok {
			return nil, notFoundf("statefulset lister for ns=%q missing", namespace)
		}
		return l.StatefulSets(namespace).Get(name)
	case "DaemonSet":
		l, ok := s.dsListers[nsKey]
		if !ok {
			return nil, notFoundf("daemonset lister for ns=%q missing", namespace)
		}
		return l.DaemonSets(namespace).Get(name)
	}
	return nil, fmt.Errorf("unsupported kind %q", kind)
}

// nsKey chooses the appropriate factory key for a namespace. If the
// collector is cluster-wide ("") we always use the "" factory.
func (s *ScopedInformers) nsKey(namespace string) string {
	if s.clusterWide {
		return ""
	}
	return namespace
}

// Informers returns the anchor informer for a given Kind, iterating over
// every namespace scope so callers can register handlers consistently.
func (s *ScopedInformers) Informers(kind string) []cache.SharedIndexInformer {
	var out []cache.SharedIndexInformer
	switch kind {
	case "Pod":
		for _, v := range s.podInformers {
			if v != nil {
				out = append(out, v)
			}
		}
	case "ReplicaSet":
		for _, v := range s.rsInformers {
			if v != nil {
				out = append(out, v)
			}
		}
	case "Deployment":
		for _, v := range s.depInformers {
			if v != nil {
				out = append(out, v)
			}
		}
	case "StatefulSet":
		for _, v := range s.stsInformers {
			if v != nil {
				out = append(out, v)
			}
		}
	case "DaemonSet":
		for _, v := range s.dsInformers {
			if v != nil {
				out = append(out, v)
			}
		}
	}
	return out
}

// ListAllPods returns all cached pods across namespace scopes, optionally
// restricted to a namespace (used when requeueing on controller events).
func (s *ScopedInformers) ListAllPods(namespace string) ([]*corev1.Pod, error) {
	if !s.HasKind("Pod") {
		return nil, nil
	}
	var out []*corev1.Pod
	nsKey := s.nsKey(namespace)
	l, ok := s.podListers[nsKey]
	if !ok {
		return nil, nil
	}
	pods, err := l.Pods(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	out = append(out, pods...)
	return out, nil
}

// ListAll returns every cached anchor object of the given kind for requeue
// purposes. namespace="" means all cached namespaces.
func (s *ScopedInformers) ListAll(kind string) []runtime.Object {
	if !s.HasKind(kind) {
		return nil
	}
	var out []runtime.Object
	for nsKey := range s.factories {
		switch kind {
		case "Pod":
			if l, ok := s.podListers[nsKey]; ok {
				items, _ := l.List(labels.Everything())
				for _, it := range items {
					out = append(out, it)
				}
			}
		case "ReplicaSet":
			if l, ok := s.rsListers[nsKey]; ok {
				items, _ := l.List(labels.Everything())
				for _, it := range items {
					out = append(out, it)
				}
			}
		case "Deployment":
			if l, ok := s.depListers[nsKey]; ok {
				items, _ := l.List(labels.Everything())
				for _, it := range items {
					out = append(out, it)
				}
			}
		case "StatefulSet":
			if l, ok := s.stsListers[nsKey]; ok {
				items, _ := l.List(labels.Everything())
				for _, it := range items {
					out = append(out, it)
				}
			}
		case "DaemonSet":
			if l, ok := s.dsListers[nsKey]; ok {
				items, _ := l.List(labels.Everything())
				for _, it := range items {
					out = append(out, it)
				}
			}
		}
	}
	return out
}

// LogDanglingSelectorWarnings prints a warning when a Pod selector is set but
// parent resources (ReplicaSet/Deployment/StatefulSet/DaemonSet) lack equally
// permissive selectors, which would break owner-chain resolution.
func (s *ScopedInformers) LogDanglingSelectorWarnings() {
	if !s.HasKind("Pod") {
		return
	}
	podSel, ok := s.selector["Pod"]
	if !ok || (podSel.LabelSelector == "" && podSel.FieldSelector == "") {
		return
	}
	for _, kind := range []string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"} {
		if !s.HasKind(kind) {
			continue
		}
		parentSel := s.selector[kind]
		if parentSel.LabelSelector == "" && parentSel.FieldSelector == "" {
			continue
		}
		s.log.Warn(
			"pod selector combined with stricter parent selector may break owner-chain resolution",
			"kind", kind,
			"podSelector", podSel,
			"parentSelector", parentSel,
		)
	}
}

// kindOfTyped detects typed Kubernetes objects (unused outside this file but
// kept for symmetry with resolver).
var _ = (*corev1.Pod)(nil)
var _ = (*appsv1.ReplicaSet)(nil)
