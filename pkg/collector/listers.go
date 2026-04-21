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

// Supported kinds tracked by the collector. Iteration order is deterministic.
var allKinds = []string{"Pod", "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"}

// ScopedInformers holds one SharedInformerFactory per (namespace, kind),
// allowing per-kind label/field selectors without cross-contamination.
//
// The empty-string namespace represents cluster-wide scope (used when
// Config.Watch.Namespaces is empty).
type ScopedInformers struct {
	client    kubernetes.Interface
	log       *slog.Logger
	resync    int
	selectors map[string]config.KindSelector

	// factories[namespace][kind] -> SharedInformerFactory tweaked for that kind.
	factories map[string]map[string]informers.SharedInformerFactory

	// Kind-specific typed informers/listers, keyed by namespace.
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

	namespaces []string
}

// NewScopedInformers constructs factories for every (namespace, kind) using
// the supplied selectors. When w.Namespaces is empty, a single cluster-wide
// scope ("") is used.
func NewScopedInformers(client kubernetes.Interface, w config.WatchScope, log *slog.Logger) *ScopedInformers {
	if log == nil {
		log = slog.Default()
	}
	namespaces := w.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	si := &ScopedInformers{
		client:       client,
		log:          log,
		selectors:    w.Selectors,
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
		namespaces:   namespaces,
	}

	for _, ns := range namespaces {
		perKind := map[string]informers.SharedInformerFactory{}
		for _, kind := range allKinds {
			sel := si.selectors[kind]
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

		si.podInformers[ns] = perKind["Pod"].Core().V1().Pods().Informer()
		si.podListers[ns] = perKind["Pod"].Core().V1().Pods().Lister()
		si.rsInformers[ns] = perKind["ReplicaSet"].Apps().V1().ReplicaSets().Informer()
		si.rsListers[ns] = perKind["ReplicaSet"].Apps().V1().ReplicaSets().Lister()
		si.depInformers[ns] = perKind["Deployment"].Apps().V1().Deployments().Informer()
		si.depListers[ns] = perKind["Deployment"].Apps().V1().Deployments().Lister()
		si.stsInformers[ns] = perKind["StatefulSet"].Apps().V1().StatefulSets().Informer()
		si.stsListers[ns] = perKind["StatefulSet"].Apps().V1().StatefulSets().Lister()
		si.dsInformers[ns] = perKind["DaemonSet"].Apps().V1().DaemonSets().Informer()
		si.dsListers[ns] = perKind["DaemonSet"].Apps().V1().DaemonSets().Lister()
	}
	return si
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
		for _, kind := range allKinds {
			sel, has := s.selectors[kind]
			if !has || (sel.LabelSelector == "" && sel.FieldSelector == "") {
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
	if _, ok := s.podListers[""]; ok {
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
			out = append(out, v)
		}
	case "ReplicaSet":
		for _, v := range s.rsInformers {
			out = append(out, v)
		}
	case "Deployment":
		for _, v := range s.depInformers {
			out = append(out, v)
		}
	case "StatefulSet":
		for _, v := range s.stsInformers {
			out = append(out, v)
		}
	case "DaemonSet":
		for _, v := range s.dsInformers {
			out = append(out, v)
		}
	}
	return out
}

// ListAllPods returns all cached pods across namespace scopes, optionally
// restricted to a namespace (used when requeueing on controller events).
func (s *ScopedInformers) ListAllPods(namespace string) ([]*corev1.Pod, error) {
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
	podSel, has := s.selectors["Pod"]
	if !has || (podSel.LabelSelector == "" && podSel.FieldSelector == "") {
		return
	}
	for _, kind := range []string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"} {
		parentSel, has := s.selectors[kind]
		if has && (parentSel.LabelSelector != "" || parentSel.FieldSelector != "") {
			s.log.Warn(
				"pod selector combined with stricter parent selector may break owner-chain resolution",
				"kind", kind,
				"podSelector", podSel,
				"parentSelector", parentSel,
			)
		}
	}
}

// kindOfTyped detects typed Kubernetes objects (unused outside this file but
// kept for symmetry with resolver).
var _ = (*corev1.Pod)(nil)
var _ = (*appsv1.ReplicaSet)(nil)
