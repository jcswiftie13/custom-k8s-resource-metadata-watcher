package collector

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func testPod(name, rv string, gen int64, labels, annotations map[string]string) *corev1.Pod {
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

func TestUpdateEnqueueCandidate_SameResourceVersion(t *testing.T) {
	a := testPod("p", "1", 1, map[string]string{"app": "x"}, nil)
	b := testPod("p", "1", 1, map[string]string{"app": "x"}, nil)
	if updateEnqueueCandidate(a, b) {
		t.Fatalf("identical resourceVersion must not enqueue")
	}
}

func TestUpdateEnqueueCandidate_DifferentResourceVersion(t *testing.T) {
	a := testPod("p", "1", 1, map[string]string{"app": "x"}, nil)
	b := testPod("p", "2", 1, map[string]string{"app": "x"}, nil)
	if !updateEnqueueCandidate(a, b) {
		t.Fatalf("resourceVersion bump must enqueue (including status-only updates)")
	}
}

func TestUpdateEnqueueCandidate_NilOld(t *testing.T) {
	b := testPod("p", "2", 1, nil, nil)
	if !updateEnqueueCandidate(nil, b) {
		t.Fatalf("nil old object should enqueue")
	}
}

func TestUpdateEnqueueCandidate_StatusOnlySameMetadata(t *testing.T) {
	a := testPod("p", "1", 1, map[string]string{"app": "x"}, nil)
	a.Status.Phase = corev1.PodPending
	b := a.DeepCopy()
	b.ResourceVersion = "2"
	b.Status.Phase = corev1.PodRunning
	if !updateEnqueueCandidate(a, b) {
		t.Fatalf("status-only change with new RV must enqueue")
	}
}
