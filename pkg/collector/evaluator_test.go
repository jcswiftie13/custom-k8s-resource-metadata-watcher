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
		name string
		pod  *corev1.Pod
		want string
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

func TestCompile_LabelsWithQuotedAnnotationPaths(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"controller_annotation_integration_test_controller_note": {
				Path: `metadata.annotations["integration.test/controller-note"]`,
			},
			"controller_annotation_integration_test_missing": {
				Path: `metadata.annotations["integration.test/missing"]`,
			},
			"namespace": {Path: "metadata.namespace"},
		},
	}
	cr := mustCompile(t, rule)

	wantOrder := []string{
		"controller_annotation_integration_test_controller_note",
		"controller_annotation_integration_test_missing",
		"namespace",
	}
	if len(cr.LabelOrder) != len(wantOrder) {
		t.Fatalf("LabelOrder len = %d, want %d: %+v", len(cr.LabelOrder), len(wantOrder), cr.LabelOrder)
	}
	for i, n := range wantOrder {
		if cr.LabelOrder[i] != n {
			t.Fatalf("LabelOrder[%d] = %q, want %q", i, cr.LabelOrder[i], n)
		}
		if cr.Labels[i].Name != n {
			t.Fatalf("Labels[%d].Name = %q, want %q", i, cr.Labels[i].Name, n)
		}
	}

	ev := NewEvaluator()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns",
		Annotations: map[string]string{
			"integration.test/controller-note": "from-fixture-deployment",
		},
	}}
	m, _ := ev.ToUnstructured(pod)

	values := map[string]string{}
	for _, cl := range cr.Labels {
		values[cl.Name] = ev.EvaluateLabel(cl, func(string) map[string]interface{} { return m })
	}
	if got := values["controller_annotation_integration_test_controller_note"]; got != "from-fixture-deployment" {
		t.Fatalf("annotation label hit = %q", got)
	}
	if got := values["controller_annotation_integration_test_missing"]; got != "" {
		t.Fatalf("missing key should be empty, got %q", got)
	}
}

func TestCompile_LabelFromTopController(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"controller_annotation_integration_test_controller_note": {
				Source: "topController",
				Path:   `metadata.annotations["integration.test/controller-note"]`,
			},
			"namespace": {Path: "metadata.namespace"},
		},
	}
	cr := mustCompile(t, rule)

	var noteLabel CompiledLabel
	for _, cl := range cr.Labels {
		if cl.Name == "controller_annotation_integration_test_controller_note" {
			noteLabel = cl
			break
		}
	}
	if noteLabel.Name == "" {
		t.Fatalf("expected controller annotation label")
	}
	if noteLabel.Primary.Source != "topController" {
		t.Fatalf("Primary.Source = %q, want topController", noteLabel.Primary.Source)
	}

	ev := NewEvaluator()
	top := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				"integration.test/controller-note": "from-top",
			},
		},
	}
	got := ev.EvaluateLabel(noteLabel, func(source string) map[string]interface{} {
		if source == "topController" {
			return top
		}
		return nil
	})
	if got != "from-top" {
		t.Fatalf("topController label = %q, want from-top", got)
	}
}

func TestCompile_LabelOnMissingForMissingMapKey(t *testing.T) {
	onMissing := "N/A"
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"namespace": {Path: "metadata.namespace"},
			"pod_label_team": {
				Path:      `metadata.labels["team"]`,
				OnMissing: &onMissing,
			},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	m, _ := ev.ToUnstructured(&corev1.Pod{})
	for _, cl := range cr.Labels {
		if cl.Name != "pod_label_team" {
			continue
		}
		got := ev.EvaluateLabel(cl, func(string) map[string]interface{} { return m })
		if got != "N/A" {
			t.Fatalf("OnMissing = %q, want N/A", got)
		}
	}
}

func TestCompile_InvalidPathOnQuotedSubscriptLabel(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_info",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"namespace": {Path: "metadata.namespace"},
			"bad":       {Path: "["},
		},
	}
	if _, err := Compile(rule); err == nil {
		t.Fatal("expected invalid path error")
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
	input := map[string]interface{}{
		"spec": map[string]interface{}{"replicas": replicas},
	}
	got := ev.EvaluateLabel(cr.Labels[0], func(string) map[string]interface{} { return input })
	if got != "3" {
		t.Fatalf("expected '3', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// expandLabels (dynamic label name) coverage
// ---------------------------------------------------------------------------

func TestEvaluator_ExpandLabels_FlattensAndSanitisesKeys(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_meta",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"namespace": {Path: "metadata.namespace"},
		},
		ExpandLabels: []config.ExpandLabel{
			{Path: "metadata.labels", Prefix: "label_"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{
			"app.kubernetes.io/name": "api",
			"team":                   "payments",
			"with-dashes/and.dots":   "v",
		},
	}}
	m, _ := ev.ToUnstructured(pod)

	srcLookup := func(s string) map[string]interface{} {
		if s == "anchor" {
			return m
		}
		return nil
	}
	out := ev.EvaluateExpand(cr.Expands[0], srcLookup)
	if got, want := out["label_app_kubernetes_io_name"], "api"; got != want {
		t.Fatalf("label_app_kubernetes_io_name = %q, want %q (full=%v)", got, want, out)
	}
	if got, want := out["label_team"], "payments"; got != want {
		t.Fatalf("label_team = %q, want %q", got, want)
	}
	if got, want := out["label_with_dashes_and_dots"], "v"; got != want {
		t.Fatalf("label_with_dashes_and_dots = %q, want %q (full=%v)", got, want, out)
	}
}

func TestEvaluator_ExpandLabels_AllowDeny(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_meta",
		Anchor: "Pod",
		Labels: map[string]config.Extract{
			"namespace": {Path: "metadata.namespace"},
		},
		ExpandLabels: []config.ExpandLabel{
			{
				Path:   "metadata.labels",
				Prefix: "label_",
				Allow:  []string{"team", "app"},
				Deny:   []string{"team"}, // deny wins
			},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Labels: map[string]string{"team": "x", "app": "y", "extra": "z"},
	}}
	m, _ := ev.ToUnstructured(pod)
	out := ev.EvaluateExpand(cr.Expands[0], func(string) map[string]interface{} { return m })
	if _, has := out["label_team"]; has {
		t.Fatalf("team should be denied, got %v", out)
	}
	if got := out["label_app"]; got != "y" {
		t.Fatalf("label_app = %q, want y", got)
	}
	if _, has := out["label_extra"]; has {
		t.Fatalf("extra should be filtered (not in allow), got %v", out)
	}
}

func TestEvaluator_ExpandLabels_MaxKeysCapsLexicographically(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_meta",
		Anchor: "Pod",
		ExpandLabels: []config.ExpandLabel{
			{Path: "metadata.annotations", Prefix: "annotation_", MaxKeys: 2},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{"a": "1", "b": "2", "c": "3"},
	}}
	m, _ := ev.ToUnstructured(pod)
	out := ev.EvaluateExpand(cr.Expands[0], func(string) map[string]interface{} { return m })
	if len(out) != 2 {
		t.Fatalf("MaxKeys=2 should cap output to 2, got %d (full=%v)", len(out), out)
	}
	if _, has := out["annotation_a"]; !has {
		t.Fatalf("expected annotation_a in capped output, got %v", out)
	}
	if _, has := out["annotation_b"]; !has {
		t.Fatalf("expected annotation_b in capped output, got %v", out)
	}
	if _, has := out["annotation_c"]; has {
		t.Fatalf("annotation_c should have been dropped by MaxKeys, got %v", out)
	}
}

func TestEvaluator_ExpandLabels_EmptyMapYieldsNoOutput(t *testing.T) {
	rule := &config.Rule{
		Name:   "pod_meta",
		Anchor: "Pod",
		ExpandLabels: []config.ExpandLabel{
			{Path: "metadata.labels", Prefix: "label_"},
		},
	}
	cr := mustCompile(t, rule)
	ev := NewEvaluator()
	pod := &corev1.Pod{}
	m, _ := ev.ToUnstructured(pod)
	out := ev.EvaluateExpand(cr.Expands[0], func(string) map[string]interface{} { return m })
	if len(out) != 0 {
		t.Fatalf("expected empty output for absent map, got %v", out)
	}
}

func TestSanitizeLabelKey(t *testing.T) {
	cases := map[string]string{
		"":                     "_",
		"app":                  "app",
		"app.kubernetes.io":    "app_kubernetes_io",
		"team/owner":           "team_owner",
		"with-dashes":          "with_dashes",
		"123":                  "123",
		"a/b.c-d_e":            "a_b_c_d_e",
		"upper.CASE_Mixed/123": "upper_CASE_Mixed_123",
	}
	for in, want := range cases {
		if got := sanitizeLabelKey(in); got != want {
			t.Errorf("sanitizeLabelKey(%q) = %q, want %q", in, got, want)
		}
	}
}
