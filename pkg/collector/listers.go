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

// ScopedInformers holds one SharedInformerFactory per (namespace, resource),
// allowing per-kind label/field selectors without cross-contamination.
//
// The empty-string namespace represents cluster-wide scope (used when
// watch.resources chooses cluster-wide or namespaced scopes per kind).
type ScopedInformers struct {
	client   dynamic.Interface
	log      *slog.Logger
	resync   int
	watch    config.WatchScope
	registry *config.Registry
	names    []string
	nameSet  map[string]struct{}
	selector map[string]config.WatchResource // per watched resource (resolved resource watch)
	// factories[name][namespaceKey] -> SharedInformerFactory tweaked for that resource/scope.
	factories map[string]map[string]dynamicinformer.DynamicSharedInformerFactory

	// Dynamic informers/listers, keyed by resource name and namespace.
	informers map[string]map[string]cache.SharedIndexInformer
	listers   map[string]map[string]cache.GenericLister

	// resourceClusterWide indicates namespace key handling per resource.
	resourceClusterWide map[string]bool
	resourceNamespaces  map[string][]string
}

// NewScopedInformers constructs factories for every (namespace, kind) using
// the supplied watch scope. When w.Namespaces is empty a single cluster-wide
// scope ("") is used; one factory per (namespace, kind) is created, so the
// apiserver sees N_namespaces_or_1 * N_watched_kinds watches.
func NewScopedInformers(client dynamic.Interface, w config.WatchScope, registry *config.Registry, log *slog.Logger) *ScopedInformers {
	if log == nil {
		log = slog.Default()
	}
	names := registry.Names()
	selector := make(map[string]config.WatchResource, len(names))
	ns := make(map[string]struct{}, len(names))
	resourceClusterWide := make(map[string]bool, len(names))
	resourceNamespaces := make(map[string][]string, len(names))
	for _, name := range names {
		info, _ := registry.ResourceFor(name)
		res, _ := w.ResourceFor(name)
		ns[name] = struct{}{}
		selector[name] = res
		if info.Scope == config.ScopeCluster {
			resourceClusterWide[name] = true
			resourceNamespaces[name] = []string{""}
			continue
		}
		if len(res.Namespaces) == 0 {
			resourceClusterWide[name] = true
			resourceNamespaces[name] = []string{""}
		} else {
			resourceClusterWide[name] = false
			resourceNamespaces[name] = append([]string(nil), res.Namespaces...)
		}
	}
	perNamespaceResources := 0
	for _, name := range names {
		if !resourceClusterWide[name] {
			perNamespaceResources++
		}
	}
	if len(names) == 0 {
		log.Info("watch mode = none", "watchResources", names)
	} else if perNamespaceResources == 0 {
		log.Info("watch mode = cluster-wide",
			"factoriesPerResource", 1,
			"watchResources", names,
		)
	} else {
		log.Info("watch mode = per-namespace",
			"watchResources", names,
		)
	}
	log.Info("watch resources configured", "resources", w.EffectiveResources())
	si := &ScopedInformers{
		client:              client,
		log:                 log,
		watch:               w,
		registry:            registry,
		names:               names,
		nameSet:             ns,
		selector:            selector,
		factories:           map[string]map[string]dynamicinformer.DynamicSharedInformerFactory{},
		informers:           map[string]map[string]cache.SharedIndexInformer{},
		listers:             map[string]map[string]cache.GenericLister{},
		resourceClusterWide: resourceClusterWide,
		resourceNamespaces:  resourceNamespaces,
	}

	for _, name := range names {
		info, _ := registry.ResourceFor(name)
		perNS := make(map[string]dynamicinformer.DynamicSharedInformerFactory, len(resourceNamespaces[name]))
		si.informers[name] = map[string]cache.SharedIndexInformer{}
		si.listers[name] = map[string]cache.GenericLister{}
		for _, ns := range resourceNamespaces[name] {
			sel := selector[name]
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
		si.factories[name] = perNS
		for _, ns := range resourceNamespaces[name] {
			gi := perNS[ns].ForResource(info.GVR())
			si.informers[name][ns] = gi.Informer()
			si.listers[name][ns] = gi.Lister()
		}
	}
	return si
}

// WatchedKinds returns a copy of the resource names this scope watches. The
// method name is retained for compatibility with older tests and metrics.
func (s *ScopedInformers) WatchedKinds() []string {
	return append([]string(nil), s.names...)
}

// HasKind returns true if this informer set watches the given resource name or
// unambiguous kind alias. The method name is retained for compatibility.
func (s *ScopedInformers) HasKind(ref string) bool {
	_, err := s.registry.ResourceFor(ref)
	if err != nil {
		return false
	}
	name, _ := s.resourceName(ref)
	_, ok := s.nameSet[name]
	return ok
}

// EnabledKindSet returns the set of watched resource names.
func (s *ScopedInformers) EnabledKindSet() map[string]struct{} {
	out := make(map[string]struct{}, len(s.names))
	for k := range s.nameSet {
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
	for name, perNS := range s.factories {
		for ns, f := range perNS {
			synced := f.WaitForCacheSync(ctx.Done())
			for typ, ok := range synced {
				if !ok {
					return fmt.Errorf("informer cache sync failed: namespace=%q resource=%s type=%v", ns, name, typ)
				}
			}
		}
	}
	return nil
}

// DryRunSelectors issues one small List per (namespace, kind) that has any
// selector configured, so bad field selectors are rejected on startup.
func (s *ScopedInformers) DryRunSelectors(ctx context.Context) error {
	for _, name := range s.names {
		sel, ok := s.selector[name]
		if !ok || (sel.LabelSelector == "" && sel.FieldSelector == "") {
			continue
		}
		info, _ := s.registry.ResourceFor(name)
		for _, ns := range s.resourceNamespaces[name] {
			opts := metav1.ListOptions{
				LabelSelector: sel.LabelSelector,
				FieldSelector: sel.FieldSelector,
				Limit:         1,
			}
			var err error
			ri := s.client.Resource(info.GVR())
			if info.Scope == config.ScopeNamespaced {
				_, err = ri.Namespace(ns).List(ctx, opts)
			} else {
				_, err = ri.List(ctx, opts)
			}
			if err != nil {
				return fmt.Errorf("dry-run list resource=%s gvr=%s scope=%s ns=%q selector=%+v: %w", name, info.GVR().String(), info.Scope, ns, sel, err)
			}
		}
	}
	return nil
}

// Get implements ListerGetter by consulting the cache for (kind, namespace, name).
// Cluster-wide factories are used when no namespace-scoped lister is configured.
func (s *ScopedInformers) Get(ref, namespace, objectName string) (runtime.Object, error) {
	info, err := s.registry.ResourceFor(ref)
	if err != nil {
		return nil, notFoundf("%v", err)
	}
	name := info.Name
	if !s.HasKind(name) {
		return nil, notFoundf("resource %q is not watched", name)
	}
	nsKey := s.nsKey(name, namespace)
	l, ok := s.listers[name][nsKey]
	if !ok {
		return nil, notFoundf("%s lister for ns=%q missing", name, namespace)
	}
	if info.Scope == config.ScopeCluster {
		return l.Get(objectName)
	}
	if namespace == "" {
		return nil, notFoundf("%s lookup requires namespace", name)
	}
	return l.ByNamespace(namespace).Get(objectName)
}

// nsKey chooses the appropriate factory key for a namespace. If the
// collector is cluster-wide ("") we always use the "" factory.
func (s *ScopedInformers) nsKey(name, namespace string) string {
	if s.resourceClusterWide[name] {
		return ""
	}
	return namespace
}

// Informers returns the anchor informer for a given Kind, iterating over
// every namespace scope so callers can register handlers consistently.
func (s *ScopedInformers) Informers(ref string) []cache.SharedIndexInformer {
	name, ok := s.resourceName(ref)
	if !ok {
		return nil
	}
	var out []cache.SharedIndexInformer
	for _, v := range s.informers[name] {
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
	name, _ := s.resourceName("Pod")
	nsKey := s.nsKey(name, namespace)
	l, ok := s.listers[name][nsKey]
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
func (s *ScopedInformers) ListAll(ref string) []runtime.Object {
	name, ok := s.resourceName(ref)
	if !ok || !s.HasKind(name) {
		return nil
	}
	var out []runtime.Object
	for _, nsKey := range s.resourceNamespaces[name] {
		if l, ok := s.listers[name][nsKey]; ok {
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
	podName, _ := s.resourceName("Pod")
	podSel, ok := s.selector[podName]
	if !ok || (podSel.LabelSelector == "" && podSel.FieldSelector == "") {
		return
	}
	for _, kind := range []string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"} {
		if !s.HasKind(kind) {
			continue
		}
		parentName, _ := s.resourceName(kind)
		parentSel := s.selector[parentName]
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

func (s *ScopedInformers) resourceName(ref string) (string, bool) {
	info, err := s.registry.ResourceFor(ref)
	if err != nil {
		return "", false
	}
	return info.Name, true
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
