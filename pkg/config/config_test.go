package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidate_AcceptsMinimalRule(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsInvalidAnchor(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "bad",
			Anchor: "Job",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for unsupported anchor")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsInvalidLabelName(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"bad-label": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for invalid label name")
	}
	if !strings.Contains(err.Error(), "invalid Prometheus label name") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsReservedLabelPrefix(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"__reserved": {Path: "metadata.namespace"},
			},
		}},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for __ prefixed label")
	}
}

func TestValidate_RequiresForEachWhenSourceIsItem(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"container": {Source: "item", Path: "name"},
			},
		}},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "source=item requires forEach") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_AcceptsItemSourceWithForEach(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:    "pod_container_info",
			Anchor:  "Pod",
			ForEach: "spec.containers[*]",
			Labels: map[string]Extract{
				"container": {Source: "item", Path: "name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_AcceptsRelationAliases(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Relations: []RelationAlias{
				{Name: "top", Via: "topController"},
			},
			Labels: map[string]Extract{
				"controller_name": {Source: "top", Path: "metadata.name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsUnknownSource(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"x": {Source: "mystery", Path: "metadata.name"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "not recognised") {
		t.Fatalf("expected unknown source error, got: %v", err)
	}
}

func TestValidate_RejectsDuplicateMetricNames(t *testing.T) {
	c := &Config{
		Rules: []Rule{
			{
				Name:   "pod_info",
				Anchor: "Pod",
				Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			},
			{
				Name:   "pod_info",
				Anchor: "Deployment",
				Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			},
		},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected duplicate metric name error")
	}
}

func TestValidate_NestedFallbacksRejected(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"x": {
					Path: "metadata.namespace",
					Fallbacks: []Extract{{
						Path: "metadata.name",
						Fallbacks: []Extract{{
							Path: "metadata.uid",
						}},
					}},
				},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "nested fallbacks") {
		t.Fatalf("expected nested fallbacks error, got: %v", err)
	}
}

func TestSanitizeLabelName(t *testing.T) {
	cases := map[string]string{
		"integration.test/controller-note": "integration_test_controller_note",
		"app.kubernetes.io/name":           "app_kubernetes_io_name",
		"123abc":                           "_123abc",
		"_already_ok":                      "_already_ok",
		"simple":                           "simple",
		"with spaces!":                     "with_spaces_",
	}
	for in, want := range cases {
		if got := SanitizeLabelName(in); got != want {
			t.Fatalf("SanitizeLabelName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidate_AcceptsFlatten(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Relations: []RelationAlias{
				{Name: "top", Via: "topController"},
			},
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			Flatten: []FlattenExtract{{
				NamePrefix: "controller_annotation_",
				Source:     "top",
				Path:       "metadata.annotations",
				Keys: []string{
					"integration.test/controller-note",
					"integration.test/owner",
				},
			}},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_FlattenRejectsEmptyKeys(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{{
				Path: "metadata.annotations",
				Keys: []string{},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "keys: at least one key is required") {
		t.Fatalf("expected empty keys error, got %v", err)
	}
}

func TestValidate_FlattenRejectsBlankKey(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{{
				Path: "metadata.annotations",
				Keys: []string{"  "},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "must be a non-empty string") {
		t.Fatalf("expected blank key error, got %v", err)
	}
}

func TestValidate_FlattenRejectsMissingPath(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{{
				Keys: []string{"team"},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "path: required") {
		t.Fatalf("expected missing path error, got %v", err)
	}
}

func TestValidate_FlattenRejectsCollisionWithLabel(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
				"team":      {Path: "metadata.labels.team"},
			},
			Flatten: []FlattenExtract{{
				Path: "metadata.annotations",
				Keys: []string{"team"},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "collides with an existing label") {
		t.Fatalf("expected label-collision error, got %v", err)
	}
}

func TestValidate_FlattenRejectsCollisionBetweenTwoFlattenEntries(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{
				{
					NamePrefix: "a_",
					Path:       "metadata.annotations",
					Keys:       []string{"team"},
				},
				{
					NamePrefix: "a_",
					Path:       "metadata.labels",
					Keys:       []string{"team"},
				},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "collides with an existing label") {
		t.Fatalf("expected flatten-collision error, got %v", err)
	}
}

func TestValidate_FlattenRejectsInvalidGeneratedName(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{{
				NamePrefix: "__",
				Path:       "metadata.annotations",
				Keys:       []string{"team"},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "starting with __") {
		t.Fatalf("expected __ prefix error, got %v", err)
	}
}

func TestValidate_FlattenItemSourceRequiresForEach(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_container_info",
			Anchor: "Pod",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
			Flatten: []FlattenExtract{{
				Source: "item",
				Path:   "metadata.annotations",
				Keys:   []string{"team"},
			}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "source=item requires forEach") {
		t.Fatalf("expected item+forEach error, got %v", err)
	}
}

func TestValidate_RejectsUnknownKindInWatchKinds(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Kinds: map[string]KindWatch{
				"Job": {},
			},
		},
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("expected unknown kind in watch.kinds, got: %v", err)
	}
}

func TestValidate_RejectsAnchorMissingFromWatchKinds(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Kinds: map[string]KindWatch{
				"Pod": {},
			},
		},
		Rules: []Rule{{
			Name:   "dep_info",
			Anchor: "Deployment",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "not included in watch.kinds") {
		t.Fatalf("expected anchor not in watch.kinds error, got: %v", err)
	}
}

func TestValidate_RejectsExplicitKindSourceMissingFromWatchKinds(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Kinds: map[string]KindWatch{
				"Pod": {},
			},
		},
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns":   {Path: "metadata.namespace"},
				"dep":  {Source: "Deployment", Path: "metadata.name"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "not included in watch.kinds") {
		t.Fatalf("expected required kind error, got: %v", err)
	}
}

func TestValidate_AllowsTopControllerWithSubsetWatchKinds(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Kinds: map[string]KindWatch{
				"Pod": {},
			},
		},
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Relations: []RelationAlias{
				{Name: "top", Via: "topController"},
			},
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
				"x":  {Source: "top", Path: "metadata.name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_DefaultsEmptyWatchKindsToAllSupported(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	eff := c.Watch.EffectiveKinds()
	if len(eff) != 5 {
		t.Fatalf("EffectiveKinds: got len %d, want 5", len(eff))
	}
}

func TestWatchScope_EffectiveKinds_ExplicitOrder(t *testing.T) {
	w := WatchScope{
		Kinds: map[string]KindWatch{
			"Deployment":  {},
			"Pod":           {FieldSelector: "status.phase=Running"},
			"ReplicaSet":    {},
			"StatefulSet":   {},
			"DaemonSet":     {},
		},
	}
	got := w.EffectiveKinds()
	want := []string{"Pod", "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("EffectiveKinds order mismatch: got %v want %v", got, want)
		}
	}
}

func TestLoad_RejectsLegacyWatchSelectors(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "cfg.yaml")
	content := `metricPrefix: "x_"
watch:
  selectors:
    Pod: { fieldSelector: "status.phase=Running" }
rules:
  - name: "a"
    anchor: Pod
    labels:
      n: { path: "metadata.namespace" }
`
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	_, err := Load(p)
	if err == nil || !strings.Contains(err.Error(), "watch.selectors is no longer supported") {
		t.Fatalf("Load: want legacy selectors error, got %v", err)
	}
}

func TestExtract_OnMissingValue(t *testing.T) {
	empty := Extract{}
	if got := empty.OnMissingValue(); got != "" {
		t.Fatalf("expected empty default, got %q", got)
	}
	s := "N/A"
	e := Extract{OnMissing: &s}
	if got := e.OnMissingValue(); got != "N/A" {
		t.Fatalf("expected N/A, got %q", got)
	}
}
