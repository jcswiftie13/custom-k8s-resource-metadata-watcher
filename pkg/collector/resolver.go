package collector

import (
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/example/metadata-exporter/pkg/config"
)

// Chain is the per-anchor map of resolved related objects. Keys include the
// builtin names "anchor", "ownerController", "topController" and capitalised
// kind names ("Pod", "Deployment", ...). Missing entries are represented by
// absent map keys (never a non-nil interface wrapping a nil pointer).
type Chain map[string]runtime.Object

// ListerGetter abstracts cache reads for the supported resources so the
// resolver never issues API calls. Implementations return a NotFound-class
// error when the cache does not contain the object.
type ListerGetter interface {
	Get(kind, namespace, name string) (runtime.Object, error)
}

// Resolver walks ownerReferences using only the supplied ListerGetter.
type Resolver struct {
	lg       ListerGetter
	registry *config.Registry
	log      *slog.Logger
}

// NewResolver constructs a resolver.
func NewResolver(lg ListerGetter, registry *config.Registry, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	return &Resolver{lg: lg, registry: registry, log: log}
}

// Resolve builds the Chain for the given anchor object, following
// ownerReferences up to a top-level workload kind or until the lister cache
// no longer contains the parent.
func (r *Resolver) Resolve(obj runtime.Object) Chain {
	chain := Chain{"anchor": obj}

	meta, ok := metaAccessor(obj)
	if !ok {
		return chain
	}

	kind := kindOf(obj)
	if kind != "" {
		chain[kind] = obj
	}
	if info, ok := r.resourceForObject(obj); ok {
		r.addChainObject(chain, info, obj)
	}

	const maxDepth = 8 // defensive: K8s owner chains are shallow (Pod -> RS -> Deployment).
	current := obj
	currentMeta := meta
	for i := 0; i < maxDepth; i++ {
		ref := findControllerRef(currentMeta.GetOwnerReferences())
		if ref == nil {
			break
		}
		parentInfo, ok := r.resourceForOwnerRef(ref)
		if !ok || !parentInfo.Owner.FollowEnabled() {
			r.log.Warn("owner GVK is not watched; stopping owner-chain walk",
				"apiVersion", ref.APIVersion,
				"kind", ref.Kind,
				"namespace", currentMeta.GetNamespace(),
				"name", ref.Name,
				"suggestion", "add the owner resource to watch.resources when source labels need it",
			)
			break
		}
		parent, err := r.lg.Get(parentInfo.Name, currentMeta.GetNamespace(), ref.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				r.log.Warn("owner not found in cache; stopping owner-chain walk",
					"apiVersion", ref.APIVersion,
					"kind", ref.Kind,
					"resource", parentInfo.Name,
					"namespace", currentMeta.GetNamespace(),
					"name", ref.Name,
				)
			} else {
				r.log.Warn("owner lookup failed; stopping owner-chain walk",
					"apiVersion", ref.APIVersion,
					"kind", ref.Kind,
					"resource", parentInfo.Name,
					"namespace", currentMeta.GetNamespace(),
					"name", ref.Name,
					"err", err,
				)
			}
			break
		}
		if i == 0 {
			chain["ownerController"] = parent
		}
		r.addChainObject(chain, parentInfo, parent)
		parentMeta, ok := metaAccessor(parent)
		if !ok {
			break
		}
		current = parent
		currentMeta = parentMeta
		_ = current
	}

	return chain
}

func (r *Resolver) addChainObject(chain Chain, info config.ResourceInfo, obj runtime.Object) {
	chain[info.Name] = obj
	if byKind, err := r.registry.ResourceFor(info.Kind); err == nil && byKind.Name == info.Name {
		chain[info.Kind] = obj
	}
	if info.Owner.TopController {
		chain["topController"] = obj
	}
}

func (r *Resolver) resourceForOwnerRef(ref *metav1.OwnerReference) (config.ResourceInfo, bool) {
	if ref.APIVersion == "" {
		info, err := r.registry.ResourceFor(ref.Kind)
		return info, err == nil
	}
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return config.ResourceInfo{}, false
	}
	return r.registry.ResourceForGVK(gv.WithKind(ref.Kind))
}

func (r *Resolver) resourceForObject(obj runtime.Object) (config.ResourceInfo, bool) {
	gvk := gvkOf(obj)
	if gvk.Empty() {
		return config.ResourceInfo{}, false
	}
	return r.registry.ResourceForGVK(gvk)
}

// findControllerRef returns the owner reference with Controller=true, or the
// first owner if none is flagged.
func findControllerRef(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	if len(refs) > 0 {
		return &refs[0]
	}
	return nil
}

// kindOf returns the capitalised Kubernetes Kind for a typed object.
func kindOf(obj runtime.Object) string {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GetKind()
	}
	switch obj.(type) {
	case *corev1.Pod:
		return "Pod"
	case *corev1.Service:
		return "Service"
	case *appsv1.ReplicaSet:
		return "ReplicaSet"
	case *appsv1.Deployment:
		return "Deployment"
	case *appsv1.StatefulSet:
		return "StatefulSet"
	case *appsv1.DaemonSet:
		return "DaemonSet"
	case *corev1.Node:
		return "Node"
	case *discoveryv1.EndpointSlice:
		return "EndpointSlice"
	}
	return ""
}

func gvkOf(obj runtime.Object) schema.GroupVersionKind {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.GroupVersionKind()
	}
	switch obj.(type) {
	case *corev1.Pod:
		return schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	case *corev1.Service:
		return schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	case *appsv1.ReplicaSet:
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "ReplicaSet"}
	case *appsv1.Deployment:
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	case *appsv1.StatefulSet:
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"}
	case *appsv1.DaemonSet:
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "DaemonSet"}
	case *corev1.Node:
		return schema.GroupVersionKind{Version: "v1", Kind: "Node"}
	case *discoveryv1.EndpointSlice:
		return schema.GroupVersionKind{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSlice"}
	}
	return schema.GroupVersionKind{}
}

// metaAccessor returns the metav1.Object view of a runtime.Object.
func metaAccessor(obj runtime.Object) (metav1.Object, bool) {
	acc, ok := obj.(metav1.ObjectMetaAccessor)
	if ok {
		return acc.GetObjectMeta(), true
	}
	m, ok := obj.(metav1.Object)
	return m, ok
}

// NamespaceName formats a "namespace/name" key (or "name" for cluster-scoped).
func NamespaceName(obj runtime.Object) string {
	m, ok := metaAccessor(obj)
	if !ok {
		return ""
	}
	if ns := m.GetNamespace(); ns != "" {
		return fmt.Sprintf("%s/%s", ns, m.GetName())
	}
	return m.GetName()
}
