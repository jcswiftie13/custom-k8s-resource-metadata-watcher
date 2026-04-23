package collector

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func pod(name, rv string, gen int64, labels, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "ns",
			UID:             types.UID("uid-" + name),
			ResourceVersion: rv,
			Generation:      gen,
			Labels:          labels,
			Annotations:     annotations,
		},
	}
}

func TestUpdateDigest_ResourceVersionUnchangedSkips(t *testing.T) {
	c := newUpdateDigestCache()
	a := pod("p", "1", 1, map[string]string{"app": "x"}, nil)
	b := pod("p", "1", 1, map[string]string{"app": "x"}, nil)
	// Seed the cache with the first observation.
	if !c.Changed(pod("p", "0", 0, nil, nil), a) {
		t.Fatalf("initial observation should register as changed")
	}
	if c.Changed(a, b) {
		t.Fatalf("identical resourceVersion must be treated as unchanged")
	}
}

func TestUpdateDigest_SameLabelsDifferentRVNoop(t *testing.T) {
	c := newUpdateDigestCache()
	a := pod("p", "1", 1, map[string]string{"app": "x"}, nil)
	b := pod("p", "2", 1, map[string]string{"app": "x"}, nil)

	if !c.Changed(pod("p", "0", 0, nil, nil), a) {
		t.Fatalf("initial observation should register as changed")
	}
	// resourceVersion moved, but metadata (generation, labels, annotations)
	// did not. Collector should skip this update.
	if c.Changed(a, b) {
		t.Fatalf("update that only bumps RV must be skipped")
	}
}

func TestUpdateDigest_AnnotationChangeTriggers(t *testing.T) {
	c := newUpdateDigestCache()
	a := pod("p", "1", 1, map[string]string{"app": "x"}, map[string]string{"note": "v1"})
	b := pod("p", "2", 1, map[string]string{"app": "x"}, map[string]string{"note": "v2"})
	if !c.Changed(pod("p", "0", 0, nil, nil), a) {
		t.Fatalf("initial observation should register as changed")
	}
	if !c.Changed(a, b) {
		t.Fatalf("annotation change must invalidate digest")
	}
}

func TestUpdateDigest_GenerationChangeTriggers(t *testing.T) {
	c := newUpdateDigestCache()
	a := pod("p", "1", 1, nil, nil)
	b := pod("p", "2", 2, nil, nil)
	if !c.Changed(pod("p", "0", 0, nil, nil), a) {
		t.Fatalf("initial observation should register as changed")
	}
	if !c.Changed(a, b) {
		t.Fatalf("generation bump must invalidate digest")
	}
}

func TestUpdateDigest_Forget(t *testing.T) {
	c := newUpdateDigestCache()
	a := pod("p", "1", 1, nil, nil)
	if !c.Changed(pod("p", "0", 0, nil, nil), a) {
		t.Fatalf("seed")
	}
	c.Forget(a.UID)
	// After Forget, a new comparison with a different RV should be treated
	// as changed because there is no cached digest to compare against.
	if !c.Changed(pod("p", "3", 1, nil, nil), pod("p", "4", 1, nil, nil)) {
		t.Fatalf("after Forget, first observation must re-register as changed")
	}
}
