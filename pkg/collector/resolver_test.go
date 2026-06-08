package collector

import (
	"fmt"
	"log/slog"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/example/metadata-exporter/pkg/config"
)

// fakeLister is a table-driven ListerGetter for tests.
type fakeLister struct {
	objects map[string]runtime.Object // key = kind/namespace/name
}

func (f *fakeLister) Get(kind, namespace, name string) (runtime.Object, error) {
	key := fmt.Sprintf("%s/%s/%s", kind, namespace, name)
	obj, ok := f.objects[key]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "cache"}, name)
	}
	return obj, nil
}

func ptrBool(b bool) *bool { return &b }

func resolverRegistry(t *testing.T, kinds ...string) *config.Registry {
	t.Helper()
	resources := make([]config.WatchResource, 0, len(kinds))
	for _, kind := range kinds {
		info, _ := config.SupportedResource(kind)
		resources = append(resources, config.WatchResource{Kind: kind, Scope: info.Scope})
	}
	cfg := &config.Config{Watch: config.WatchScope{Resources: resources}}
	reg, err := cfg.Registry()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

func newPodOwnedBy(ns, name, parentKind, parentName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       parentKind,
				Name:       parentName,
				Controller: ptrBool(true),
			}},
		},
	}
}

func TestResolve_PodToReplicaSetToDeployment(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "dep"},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "n", Name: "rs",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "dep", Controller: ptrBool(true),
			}},
		},
	}
	pod := newPodOwnedBy("n", "pod", "ReplicaSet", "rs")

	fl := &fakeLister{objects: map[string]runtime.Object{
		"ReplicaSet/n/rs":  rs,
		"Deployment/n/dep": dep,
	}}
	r := NewResolver(fl, resolverRegistry(t, "Pod", "ReplicaSet", "Deployment"), slog.Default())
	chain := r.Resolve(pod)

	if chain["anchor"] != pod {
		t.Fatalf("anchor mismatch")
	}
	if chain["ownerController"] != rs {
		t.Fatalf("ownerController expected rs, got %T", chain["ownerController"])
	}
	if chain["topController"] != dep {
		t.Fatalf("topController expected dep, got %T", chain["topController"])
	}
	if chain["ReplicaSet"] != rs || chain["Deployment"] != dep {
		t.Fatalf("kind-keyed entries missing")
	}
}

func TestResolve_StaticPodHasNoOwnerChain(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "pod"},
	}
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, resolverRegistry(t, "Pod"), slog.Default())
	chain := r.Resolve(pod)

	if chain["anchor"] != pod {
		t.Fatalf("anchor mismatch")
	}
	if _, has := chain["ownerController"]; has {
		t.Fatalf("ownerController should be absent for static pod")
	}
	if _, has := chain["topController"]; has {
		t.Fatalf("topController should be absent for static pod")
	}
}

func TestResolve_StopsOnCacheMiss(t *testing.T) {
	pod := newPodOwnedBy("n", "pod", "ReplicaSet", "missing-rs")
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, resolverRegistry(t, "Pod", "ReplicaSet"), slog.Default())
	chain := r.Resolve(pod)

	if chain["anchor"] != pod {
		t.Fatalf("anchor mismatch")
	}
	if _, has := chain["ownerController"]; has {
		t.Fatalf("ownerController should be absent when cache miss")
	}
	if _, has := chain["ReplicaSet"]; has {
		t.Fatalf("ReplicaSet entry should be absent when cache miss")
	}
}

func TestResolve_DeploymentAnchorIsItsOwnTop(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "dep"},
	}
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, resolverRegistry(t, "Deployment"), slog.Default())
	chain := r.Resolve(dep)
	if chain["topController"] != dep {
		t.Fatalf("expected topController == anchor for Deployment anchor")
	}
	if chain["Deployment"] != dep {
		t.Fatalf("expected Deployment entry == anchor")
	}
}

func TestResolve_StatefulSetAnchor(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "sts"},
	}
	pod := newPodOwnedBy("n", "pod", "StatefulSet", "sts")
	fl := &fakeLister{objects: map[string]runtime.Object{"StatefulSet/n/sts": sts}}
	r := NewResolver(fl, resolverRegistry(t, "Pod", "StatefulSet"), slog.Default())

	chain := r.Resolve(pod)
	if chain["topController"] != sts {
		t.Fatalf("topController should be StatefulSet directly")
	}
	if chain["StatefulSet"] != sts {
		t.Fatalf("StatefulSet kind entry missing")
	}
}

func TestResolve_UnstructuredEndpointSliceToService(t *testing.T) {
	svc := &unstructured.Unstructured{}
	svc.SetAPIVersion("v1")
	svc.SetKind("Service")
	svc.SetNamespace("n")
	svc.SetName("svc")

	ep := &unstructured.Unstructured{}
	ep.SetAPIVersion("discovery.k8s.io/v1")
	ep.SetKind("EndpointSlice")
	ep.SetNamespace("n")
	ep.SetName("svc-abc")
	ep.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "Service",
		Name:       "svc",
		Controller: ptrBool(true),
	}})

	fl := &fakeLister{objects: map[string]runtime.Object{"Service/n/svc": svc}}
	r := NewResolver(fl, resolverRegistry(t, "EndpointSlice", "Service"), slog.Default())
	chain := r.Resolve(ep)

	if got := kindOf(ep); got != "EndpointSlice" {
		t.Fatalf("kindOf endpoint slice = %q, want EndpointSlice", got)
	}
	if chain["anchor"] != ep {
		t.Fatalf("anchor mismatch")
	}
	if chain["ownerController"] != svc {
		t.Fatalf("ownerController expected service, got %T", chain["ownerController"])
	}
	if chain["Service"] != svc {
		t.Fatalf("Service kind entry missing")
	}
	if _, has := chain["topController"]; has {
		t.Fatalf("EndpointSlice -> Service must not set topController")
	}
}
