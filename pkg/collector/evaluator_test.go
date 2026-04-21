package collector

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/example/metadata-exporter/pkg/config"
)

func mustCompile(t *testing.T, r *config.Rule) *CompiledRule {
	t.Helper()
	cr, err := Compile(r)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	return cr
}

func TestEvaluator_BasicPathExtraction(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"namespace": {Path: "metadata.namespace"},
			"pod":       {Path: "metadata.name"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "p"},
	}
	m, err := ev.ToUnstructured(pod)
	if err != nil {
		t.Fatal(err)
	}
	for _, cl := range cr.Labels {
		got := ev.EvaluateLabel(cl, func(source string) map[string]interface{} {
			if source == "anchor" {
				return m
			}
			return nil
		})
		switch cl.Name {
		case "namespace":
			if got != "n" {
				t.Fatalf("namespace = %q", got)
			}
		case "pod":
			if got != "p" {
				t.Fatalf("pod = %q", got)
			}
		}
	}
}

func TestEvaluator_AnnotationWithDotsAndSlashes(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"app": {Path: `metadata.annotations["argocd.argoproj.io/tracking-id"]`},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{"argocd.argoproj.io/tracking-id": "my-app"},
	}}
	m, _ := ev.ToUnstructured(pod)
	got := ev.EvaluateLabel(cr.Labels[0], func(string) map[string]interface{} { return m })
	if got != "my-app" {
		t.Fatalf("got %q", got)
	}
}

func TestEvaluator_ForEachContainer(t *testing.T) {
	rule := &config.Rule{
		Name:    "pod_container_info",
		Anchor:  "Pod",
		ForEach: "spec.containers[*]",
		Labels: map[string]config.Extract{
			"container": {Source: "item", Path: "name"},
			"image":     {Source: "item", Path: "image"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "a", Image: "img-a"},
				{Name: "b", Image: "img-b"},
			},
		},
	}
	m, _ := ev.ToUnstructured(pod)
	items := ev.EvaluateForEach(cr, m)
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}

	got := map[string]string{}
	for _, item := range items {
		var c, img string
		for _, cl := range cr.Labels {
			v := ev.EvaluateLabel(cl, func(source string) map[string]interface{} {
				if source == "item" {
					return item
				}
				return m
			})
			switch cl.Name {
			case "container":
				c = v
			case "image":
				img = v
			}
		}
		got[c] = img
	}
	if got["a"] != "img-a" || got["b"] != "img-b" {
		t.Fatalf("container->image mapping wrong: %+v", got)
	}
}

func TestEvaluator_FallbacksAndOnMissing(t *testing.T) {
	onMissing := "n/a"
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"team": {
				Path: `metadata.annotations["team"]`,
				Fallbacks: []config.Extract{
					{Path: `metadata.labels["team"]`},
				},
				OnMissing: &onMissing,
			},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()

	cases := []struct {
		name    string
		pod     *corev1.Pod
		want    string
		skipOnM bool
	}{
		{
			name: "primary hits",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"team": "payments"},
			}},
			want: "payments",
		},
		{
			name: "fallback hits",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"team": "billing"},
			}},
			want: "billing",
		},
		{
			name: "onMissing",
			pod:  &corev1.Pod{},
			want: "n/a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := ev.ToUnstructured(tc.pod)
			got := ev.EvaluateLabel(cr.Labels[0], func(string) map[string]interface{} { return m })
			if got != tc.want {
				t.Fatalf("want %q, got %q", tc.want, got)
			}
		})
	}
}

func TestEvaluator_MissingSourceReturnsOnMissing(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"x": {Source: "topController", Path: "metadata.name"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	got := ev.EvaluateLabel(cr.Labels[0], func(source string) map[string]interface{} {
		return nil
	})
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestCompile_InvalidJSONPath(t *testing.T) {
	rule := &config.Rule{
		Name:   "bad",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"x": {Path: "["},
		},
	}
	if _, err := Compile(rule); err == nil {
		t.Fatal("expected invalid JSONPath error")
	}
}

func TestCompile_ScalarTypeStringification(t *testing.T) {
	rule := &config.Rule{
		Name:   "deployment_info",
		Anchor: "Deployment",
		Labels: map[string]config.Extract{
			"replicas": {Path: "spec.replicas"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	replicas := int32(3)
	// Minimal unstructured input shaped like a Deployment
	input := map[string]interface{}{
		"spec": map[string]interface{}{"replicas": replicas},
	}
	got := ev.EvaluateLabel(cr.Labels[0], func(string) map[string]interface{} { return input })
	if got != "3" {
		t.Fatalf("expected '3', got %q", got)
	}
}
