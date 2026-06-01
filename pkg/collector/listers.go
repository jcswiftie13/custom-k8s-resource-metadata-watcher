package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/config"
)

// ScopedInformers holds one SharedInformerFactory per (namespace, kind),
// allowing per-kind label/field selectors without cross-contamination.
//
// The empty-string namespace represents cluster-wide scope (used when
// watch.resources chooses cluster-wide or namespaced scopes per kind).
type ScopedInformers struct {
	client   dynamic.Interface
	log      *slog.Logger
	resync   int
	watch    config.WatchScope
	kinds    []string
	kindSet  map[string]struct{}
	selector map[string]config.WatchResource // per watched kind (resolved resource watch)
	// factories[kind][namespaceKey] -> SharedInformerFactory tweaked for that kind/scope.
	factories map[string]map[string]dynamicinformer.DynamicSharedInformerFactory

	// Dynamic informers/listers, keyed by kind and namespace. Absent for unwatched kinds.
	informers map[string]map[string]cache.SharedIndexInformer
	listers   map[string]map[string]cache.GenericLister

	// kindClusterWide indicates namespace key handling per kind.
	kindClusterWide map[string]bool
	kindNamespaces  map[string][]string
}

// NewScopedInformers constructs factories for every (namespace, kind) using
// the supplied watch scope. When w.Namespaces is empty a single cluster-wide
// scope ("") is used; one factory per (namespace, kind) is created, so the
// apiserver sees N_namespaces_or_1 * N_watched_kinds watches.
func NewScopedInformers(client dynamic.Interface, w config.WatchScope, log *slog.Logger) *ScopedInformers {
	if log == nil {
		log = slog.Default()
	}
	kinds := w.EffectiveKinds()
	selector := make(map[string]config.WatchResource, len(kinds))
	ks := make(map[string]struct{}, len(kinds))
	kindClusterWide := make(map[string]bool, len(kinds))
	kindNamespaces := make(map[string][]string, len(kinds))
	for _, k := range kinds {
		res, _ := w.ResourceFor(k)
		ks[k] = struct{}{}
		selector[k] = res
		if res.Scope == config.ScopeCluster || k == "Node" {
			kindClusterWide[k] = true
			kindNamespaces[k] = []string{""}
			continue
		}
		if len(res.Namespaces) == 0 {
			kindClusterWide[k] = true
			kindNamespaces[k] = []string{""}
		} else {
			kindClusterWide[k] = false
			kindNamespaces[k] = append([]string(nil), res.Namespaces...)
		}
	}
	perNamespaceKinds := 0
	for _, kind := range kinds {
		if !kindClusterWide[kind] {
			perNamespaceKinds++
		}
	}
	if perNamespaceKinds == 0 {
		log.Info("watch mode = cluster-wide",
			"factoriesPerKind", 1,
			"watchKinds", kinds,
		)
	} else {
		log.Info("watch mode = per-namespace",
			"watchKinds", kinds,
		)
	}
	log.Info("watch resources configured", "resources", w.EffectiveResources())
	si := &ScopedInformers{
		client:          client,
		log:             log,
		watch:           w,
		kinds:           kinds,
		kindSet:         ks,
		selector:        selector,
		factories:       map[string]map[string]dynamicinformer.DynamicSharedInformerFactory{},
		informers:       map[string]map[string]cache.SharedIndexInformer{},
		listers:         map[string]map[string]cache.GenericLister{},
		kindClusterWide: kindClusterWide,
		kindNamespaces:  kindNamespaces,
	}

	for _, kind := range kinds {
		info, ok := config.SupportedResource(kind)
		if !ok {
			continue
		}
		perNS := make(map[string]dynamicinformer.DynamicSharedInformerFactory, len(kindNamespaces[kind]))
		si.informers[kind] = map[string]cache.SharedIndexInformer{}
		si.listers[kind] = map[string]cache.GenericLister{}
		for _, ns := range kindNamespaces[kind] {
			sel := selector[kind]
			namespace := metav1.NamespaceAll
			if ns != "" {
				namespace = ns
			}
			f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
				client,
				time.Duration(si.resync),
				namespace,
				func(o *metav1.ListOptions) {
					if sel.LabelSelector != "" {
						o.LabelSelector = sel.LabelSelector
					}
					if sel.FieldSelector != "" {
						o.FieldSelector = sel.FieldSelector
					}
				},
			)
			perNS[ns] = f
		}
		si.factories[kind] = perNS
		for _, ns := range kindNamespaces[kind] {
			gi := perNS[ns].ForResource(info.GVR())
			si.informers[kind][ns] = gi.Informer()
			si.listers[kind][ns] = gi.Lister()
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
	for _, perNS := range s.factories {
		for _, f := range perNS {
			f.Start(ctx.Done())
		}
	}
	for kind, perNS := range s.factories {
		for ns, f := range perNS {
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
	for _, kind := range s.kinds {
		sel, ok := s.selector[kind]
		if !ok || (sel.LabelSelector == "" && sel.FieldSelector == "") {
			continue
		}
		for _, ns := range s.kindNamespaces[kind] {
			opts := metav1.ListOptions{
				LabelSelector: sel.LabelSelector,
				FieldSelector: sel.FieldSelector,
				Limit:         1,
			}
			var err error
			info, ok := config.SupportedResource(kind)
			if !ok {
				return fmt.Errorf("dry-run list %s: unsupported kind", kind)
			}
			ri := s.client.Resource(info.GVR())
			if info.Scope == config.ScopeNamespaced {
				_, err = ri.Namespace(ns).List(ctx, opts)
			} else {
				_, err = ri.List(ctx, opts)
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
	nsKey := s.nsKey(kind, namespace)
	l, ok := s.listers[kind][nsKey]
	if !ok {
		return nil, notFoundf("%s lister for ns=%q missing", kind, namespace)
	}
	info, ok := config.SupportedResource(kind)
	if !ok {
		return nil, fmt.Errorf("unsupported kind %q", kind)
	}
	if info.Scope == config.ScopeCluster {
		return l.Get(name)
	}
	if namespace == "" {
		return nil, notFoundf("%s lookup requires namespace", kind)
	}
	return l.ByNamespace(namespace).Get(name)
}

// nsKey chooses the appropriate factory key for a namespace. If the
// collector is cluster-wide ("") we always use the "" factory.
func (s *ScopedInformers) nsKey(kind, namespace string) string {
	if s.kindClusterWide[kind] {
		return ""
	}
	return namespace
}

// Informers returns the anchor informer for a given Kind, iterating over
// every namespace scope so callers can register handlers consistently.
func (s *ScopedInformers) Informers(kind string) []cache.SharedIndexInformer {
	var out []cache.SharedIndexInformer
	for _, v := range s.informers[kind] {
		if v != nil {
			out = append(out, v)
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
	nsKey := s.nsKey("Pod", namespace)
	l, ok := s.listers["Pod"][nsKey]
	if !ok {
		return nil, nil
	}
	var items []runtime.Object
	var err error
	if namespace == "" {
		items, err = l.List(labels.Everything())
	} else {
		items, err = l.ByNamespace(namespace).List(labels.Everything())
	}
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		pod, err := objectToPod(item)
		if err != nil {
			return nil, err
		}
		out = append(out, pod)
	}
	return out, nil
}

// ListAll returns every cached anchor object of the given kind for requeue
// purposes. namespace="" means all cached namespaces.
func (s *ScopedInformers) ListAll(kind string) []runtime.Object {
	if !s.HasKind(kind) {
		return nil
	}
	var out []runtime.Object
	for _, nsKey := range s.kindNamespaces[kind] {
		if l, ok := s.listers[kind][nsKey]; ok {
			items, _ := l.List(labels.Everything())
			out = append(out, items...)
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

func objectToPod(obj runtime.Object) (*corev1.Pod, error) {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod, nil
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object %T is not a pod", obj)
	}
	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, pod); err != nil {
		return nil, fmt.Errorf("convert pod: %w", err)
	}
	return pod, nil
}
