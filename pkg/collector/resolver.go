package collector

import (
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	lg  ListerGetter
	log *slog.Logger
}

// NewResolver constructs a resolver.
func NewResolver(lg ListerGetter, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	return &Resolver{lg: lg, log: log}
}

// supportedKinds indexes the kinds whose owner references we follow and for
// which we keep a slot in the Chain.
var topControllerKinds = map[string]struct{}{
	"Deployment":  {},
	"StatefulSet": {},
	"DaemonSet":   {},
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
	if _, top := topControllerKinds[kind]; top {
		chain["topController"] = obj
	}

	const maxDepth = 8 // defensive: K8s owner chains are shallow (Pod -> RS -> Deployment).
	current := obj
	currentMeta := meta
	for i := 0; i < maxDepth; i++ {
		ref := findControllerRef(currentMeta.GetOwnerReferences())
		if ref == nil {
			break
		}
		if !isSupportedKind(ref.Kind) {
			// Unknown controller (e.g. a CRD); stop here without failing.
			break
		}
		parent, err := r.lg.Get(ref.Kind, currentMeta.GetNamespace(), ref.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				r.log.Warn("owner not found in cache; stopping owner-chain walk",
					"kind", ref.Kind,
					"namespace", currentMeta.GetNamespace(),
					"name", ref.Name,
				)
			} else {
				r.log.Warn("owner lookup failed; stopping owner-chain walk",
					"kind", ref.Kind,
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
		chain[ref.Kind] = parent
		if _, top := topControllerKinds[ref.Kind]; top {
			chain["topController"] = parent
		}
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

func isSupportedKind(kind string) bool {
	switch kind {
	case "Pod", "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet":
		return true
	}
	return false
}

// kindOf returns the capitalised Kubernetes Kind for a typed object.
func kindOf(obj runtime.Object) string {
	switch obj.(type) {
	case *corev1.Pod:
		return "Pod"
	case *appsv1.ReplicaSet:
		return "ReplicaSet"
	case *appsv1.Deployment:
		return "Deployment"
	case *appsv1.StatefulSet:
		return "StatefulSet"
	case *appsv1.DaemonSet:
		return "DaemonSet"
	}
	return ""
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
