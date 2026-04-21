package collector

import (
	"fmt"
	"log/slog"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	r := NewResolver(fl, slog.Default())
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
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, slog.Default())
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
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, slog.Default())
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
	r := NewResolver(&fakeLister{objects: map[string]runtime.Object{}}, slog.Default())
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
	r := NewResolver(fl, slog.Default())

	chain := r.Resolve(pod)
	if chain["topController"] != sts {
		t.Fatalf("topController should be StatefulSet directly")
	}
	if chain["StatefulSet"] != sts {
		t.Fatalf("StatefulSet kind entry missing")
	}
}
