package collector

import (
	"testing"
)

func TestParsePath_DottedAndSubscripts(t *testing.T) {
	cases := []struct {
		expr string
		want int // number of segments
	}{
		{"metadata.namespace", 2},
		{"metadata.name", 2},
		{"spec.containers[*]", 3},
		{"spec.containers[*].image", 4},
		{"spec.containers[0].image", 4},
		{`metadata.annotations["example.com/foo"]`, 3},
		{`metadata.labels['app.kubernetes.io/name']`, 3},
		{"{.spec.replicas}", 2},
		{"kind", 1},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			p, err := parsePath(tc.expr)
			if err != nil {
				t.Fatalf("parsePath(%q) err = %v", tc.expr, err)
			}
			if len(p.segments) != tc.want {
				t.Fatalf("want %d segments, got %d: %+v", tc.want, len(p.segments), p.segments)
			}
		})
	}
}

func TestParsePath_Errors(t *testing.T) {
	bad := []string{"", "[", "foo[", "foo[bad]", "foo..bar"}
	for _, b := range bad {
		t.Run(b, func(t *testing.T) {
			if _, err := parsePath(b); err == nil {
				t.Fatalf("expected error for %q", b)
			}
		})
	}
}

func TestEvaluatePath_MapAccess(t *testing.T) {
	p, err := parsePath(`metadata.annotations["argocd.argoproj.io/tracking-id"]`)
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				"argocd.argoproj.io/tracking-id": "value-123",
			},
		},
	}
	results := p.evaluate(input)
	if len(results) != 1 || results[0] != "value-123" {
		t.Fatalf("unexpected: %+v", results)
	}
}

func TestEvaluatePath_Wildcard(t *testing.T) {
	p, err := parsePath("spec.containers[*].image")
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]interface{}{
		"spec": map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{"name": "a", "image": "img-a"},
				map[string]interface{}{"name": "b", "image": "img-b"},
			},
		},
	}
	results := p.evaluate(input)
	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
	if results[0] != "img-a" || results[1] != "img-b" {
		t.Fatalf("unexpected: %+v", results)
	}
}

func TestEvaluatePath_WildcardExpandsToMaps(t *testing.T) {
	p, err := parsePath("spec.containers[*]")
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]interface{}{
		"spec": map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{"name": "a"},
				map[string]interface{}{"name": "b"},
			},
		},
	}
	results := p.evaluate(input)
	if len(results) != 2 {
		t.Fatalf("want 2 items, got %d", len(results))
	}
}

func TestEvaluatePath_MissingKey(t *testing.T) {
	p, err := parsePath("metadata.missing")
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]interface{}{
		"metadata": map[string]interface{}{},
	}
	if results := p.evaluate(input); len(results) != 0 {
		t.Fatalf("want no results, got %+v", results)
	}
}
