package config

import (
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func watchResources(kinds ...string) WatchScope {
	resources := make([]WatchResource, 0, len(kinds))
	for _, kind := range kinds {
		info, _ := SupportedResource(kind)
		resources = append(resources, WatchResource{Kind: kind, Scope: info.Scope})
	}
	return WatchScope{Resources: resources}
}

type fakeDiscovery struct {
	lists map[string]*metav1.APIResourceList
	err   error
}

func (f fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	if f.err != nil {
		return nil, f.err
	}
	list, ok := f.lists[groupVersion]
	if !ok {
		return nil, fmt.Errorf("missing discovery data for %s", groupVersion)
	}
	return list, nil
}

func TestValidate_AcceptsMinimalRule(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
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
		Watch: watchResources("Pod"),
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
	if !strings.Contains(err.Error(), "not recognised") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsInvalidLabelName(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
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
		Watch: watchResources("Pod"),
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
		Watch: watchResources("Pod"),
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
		Watch: watchResources("Pod"),
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

func TestValidate_AcceptsTopControllerSourceDirectly(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"controller_name": {Source: "topController", Path: "metadata.name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsUnknownSource(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
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
		Watch: watchResources("Pod", "Deployment"),
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
		Watch: watchResources("Pod"),
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

func TestValidate_AcceptsQuotedAnnotationPathsInLabels(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
				"controller_annotation_integration_test_controller_note": {
					Source: "topController",
					Path:   `metadata.annotations["integration.test/controller-note"]`,
				},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_AcceptsArbitraryCompleteGVRInWatchResources(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{
				Name:     "Job",
				Group:    "batch",
				Version:  "v1",
				Resource: "jobs",
				Kind:     "Job",
				Scope:    ScopeNamespaced,
			}},
		},
		Rules: []Rule{{
			Name:   "job_info",
			Anchor: "Job",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsIncompleteGVRWhenDiscoveryDisabled(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{Kind: "Job", Scope: ScopeNamespaced}},
		},
		Rules: []Rule{{
			Name:   "job_info",
			Anchor: "Job",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "discovery.enabled=false") {
		t.Fatalf("expected incomplete GVR error, got: %v", err)
	}
}

func TestResolveDiscovery_FillsIncompleteGVR(t *testing.T) {
	c := &Config{
		Discovery: DiscoveryConfig{Enabled: true},
		Watch: WatchScope{Resources: []WatchResource{{
			Name:       "CronJob",
			APIVersion: "batch/v1",
			Kind:       "CronJob",
		}}},
		Rules: []Rule{{
			Name:   "cronjob_info",
			Anchor: "CronJob",
			Labels: map[string]Extract{
				"name": {Path: "metadata.name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("validate before discovery should allow placeholder: %v", err)
	}
	err := c.ResolveDiscovery(fakeDiscovery{lists: map[string]*metav1.APIResourceList{
		"batch/v1": {
			GroupVersion: "batch/v1",
			APIResources: []metav1.APIResource{{
				Name:       "cronjobs",
				Kind:       "CronJob",
				Namespaced: true,
				Verbs:      metav1.Verbs{"get", "list", "watch"},
			}},
		},
	}})
	if err != nil {
		t.Fatalf("ResolveDiscovery: %v", err)
	}
	reg, err := c.Registry()
	if err != nil {
		t.Fatalf("Registry: %v", err)
	}
	info, err := reg.ResourceFor("CronJob")
	if err != nil {
		t.Fatalf("ResourceFor: %v", err)
	}
	if info.Group != "batch" || info.Version != "v1" || info.Resource != "cronjobs" || info.Scope != ScopeNamespaced {
		t.Fatalf("unexpected discovered resource: %+v", info)
	}
}

func TestResolveDiscovery_ReportsDiscoveryFailure(t *testing.T) {
	c := &Config{
		Discovery: DiscoveryConfig{Enabled: true},
		Watch: WatchScope{Resources: []WatchResource{{
			Name:       "Ingress",
			APIVersion: "networking.k8s.io/v1",
			Kind:       "Ingress",
		}}},
		Rules: []Rule{{
			Name:   "ingress_info",
			Anchor: "Ingress",
			Labels: map[string]Extract{
				"name": {Path: "metadata.name"},
			},
		}},
	}
	err := c.ResolveDiscovery(fakeDiscovery{err: fmt.Errorf("forbidden")})
	if err == nil || !strings.Contains(err.Error(), "provide complete group/version/resource/kind/scope") {
		t.Fatalf("expected actionable discovery failure, got: %v", err)
	}
}

func TestValidate_RejectsDuplicateGVR(t *testing.T) {
	c := &Config{
		Watch: WatchScope{Resources: []WatchResource{
			{Name: "jobA", Group: "batch", Version: "v1", Resource: "jobs", Kind: "Job", Scope: ScopeNamespaced},
			{Name: "jobB", Group: "batch", Version: "v1", Resource: "jobs", Kind: "JobCopy", Scope: ScopeNamespaced},
		}},
		Rules: []Rule{{
			Name:   "job_info",
			Anchor: "jobA",
			Labels: map[string]Extract{"name": {Path: "metadata.name"}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicated GVR") {
		t.Fatalf("expected duplicated GVR error, got: %v", err)
	}
}

func TestValidate_RejectsAmbiguousKindSource(t *testing.T) {
	c := &Config{
		Watch: WatchScope{Resources: []WatchResource{
			{Name: "alphaWidget", Group: "alpha.example.com", Version: "v1", Resource: "widgets", Kind: "Widget", Scope: ScopeNamespaced},
			{Name: "betaWidget", Group: "beta.example.com", Version: "v1", Resource: "widgets", Kind: "Widget", Scope: ScopeNamespaced},
		}},
		Rules: []Rule{{
			Name:   "widget_info",
			Anchor: "alphaWidget",
			Labels: map[string]Extract{
				"name":  {Path: "metadata.name"},
				"other": {Source: "Widget", Path: "metadata.name"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguous source error, got: %v", err)
	}
}

func TestValidate_RejectsAnchorMissingFromWatchResources(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{Kind: "Pod", Scope: ScopeNamespaced}},
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
	if err == nil || !strings.Contains(err.Error(), "not recognised") {
		t.Fatalf("expected anchor not in watch.resources error, got: %v", err)
	}
}

func TestValidate_RejectsExplicitKindSourceMissingFromWatchResources(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{Kind: "Pod", Scope: ScopeNamespaced}},
		},
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns":  {Path: "metadata.namespace"},
				"dep": {Source: "Deployment", Path: "metadata.name"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "not recognised") {
		t.Fatalf("expected required kind error, got: %v", err)
	}
}

func TestValidate_AllowsTopControllerWithSubsetWatchKinds(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{Kind: "Pod", Scope: ScopeNamespaced}},
		},
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
				"x":  {Source: "topController", Path: "metadata.name"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsEmptyWatchResourcesWhenRulesNeedAnchors(t *testing.T) {
	c := &Config{
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"ns": {Path: "metadata.namespace"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "watch.resources is empty") {
		t.Fatalf("expected empty watch error, got: %v", err)
	}
	eff := c.Watch.EffectiveKinds()
	if len(eff) != 0 {
		t.Fatalf("EffectiveKinds: got len %d, want 0", len(eff))
	}
}

func TestWatchScope_EffectiveKinds_ExplicitOrder(t *testing.T) {
	w := WatchScope{
		Resources: []WatchResource{
			{Kind: "Deployment", Scope: ScopeNamespaced},
			{Kind: "Pod", Scope: ScopeNamespaced, FieldSelector: "status.phase=Running"},
			{Kind: "ReplicaSet", Scope: ScopeNamespaced},
			{Kind: "StatefulSet", Scope: ScopeNamespaced},
			{Kind: "DaemonSet", Scope: ScopeNamespaced},
			{Kind: "EndpointSlice", Scope: ScopeNamespaced},
			{Kind: "Service", Scope: ScopeNamespaced},
		},
	}
	got := w.EffectiveKinds()
	want := []string{"Deployment", "Pod", "ReplicaSet", "StatefulSet", "DaemonSet", "EndpointSlice", "Service"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("EffectiveKinds order mismatch: got %v want %v", got, want)
		}
	}
}

func TestValidate_AcceptsServiceAndEndpointSliceAnchors(t *testing.T) {
	for _, kind := range []string{"Service", "EndpointSlice"} {
		t.Run(kind, func(t *testing.T) {
			c := &Config{
				Watch: WatchScope{
					Resources: []WatchResource{{Kind: kind, Scope: ScopeNamespaced}},
				},
				Rules: []Rule{{
					Name:   "info",
					Anchor: kind,
					Labels: map[string]Extract{
						"name": {Path: "metadata.name"},
					},
				}},
			}
			if err := c.Validate(); err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		})
	}
}

func TestValidate_NodeMustBeClusterScoped(t *testing.T) {
	c := &Config{
		Watch: WatchScope{
			Resources: []WatchResource{{Kind: "Node", Scope: ScopeNamespaced}},
		},
		Rules: []Rule{{
			Name:   "node_info",
			Anchor: "Node",
			Labels: map[string]Extract{"name": {Path: "metadata.name"}},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "must use scope") {
		t.Fatalf("expected node cluster-scope validation error, got: %v", err)
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

func TestValidate_AcceptsExpandLabels(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
				{Path: "metadata.annotations", Prefix: "annotation_", Source: "topController"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsExpandLabelMissingPrefix(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "prefix: required") {
		t.Fatalf("expected prefix required error, got: %v", err)
	}
}

func TestValidate_RejectsExpandLabelInvalidPrefix(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "1bad-"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "valid Prometheus label name") {
		t.Fatalf("expected invalid prefix error, got: %v", err)
	}
}

func TestValidate_RejectsExpandLabelReservedPrefix(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "__internal_"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("expected reserved prefix error, got: %v", err)
	}
}

func TestValidate_RejectsExpandLabelNegativeMaxKeys(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_", MaxKeys: -1},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "maxKeys") {
		t.Fatalf("expected maxKeys error, got: %v", err)
	}
}

func TestValidate_RejectsExpandLabelPrefixCollidingWithFixedLabel(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"label_": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("expected prefix collision error, got: %v", err)
	}
}

func TestValidate_AcceptsRuleWithOnlyExpandLabels(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_meta",
			Anchor: "Pod",
			ExpandLabels: []ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestValidate_RejectsExpandLabelItemSourceWithoutForEach(t *testing.T) {
	c := &Config{
		Watch: watchResources("Pod"),
		Rules: []Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]Extract{
				"namespace": {Path: "metadata.namespace"},
			},
			ExpandLabels: []ExpandLabel{
				{Source: "item", Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "requires forEach") {
		t.Fatalf("expected forEach error, got: %v", err)
	}
}
