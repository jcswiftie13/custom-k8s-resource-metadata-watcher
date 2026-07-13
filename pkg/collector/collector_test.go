package collector

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/example/metadata-exporter/pkg/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func legacyTestWatch(kinds ...string) config.WatchScope {
	if len(kinds) == 0 {
		kinds = config.SupportedKinds()
	}
	resources := make([]config.WatchResource, 0, len(kinds))
	for _, kind := range kinds {
		info, _ := config.SupportedResource(kind)
		resources = append(resources, config.WatchResource{Kind: kind, Scope: info.Scope})
	}
	return config.WatchScope{Resources: resources}
}

func podWithOwner(name, ns, rsName string, labels, annotations map[string]string) *corev1.Pod {
	ctrl := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   ns,
			UID:         types.UID("uid-" + name),
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: rsName, UID: types.UID("uid-" + rsName), Controller: &ctrl},
			},
		},
	}
}

func rsObj(name, ns, depName string) *appsv1.ReplicaSet {
	ctrl := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: depName, UID: types.UID("uid-" + depName), Controller: &ctrl},
			},
		},
	}
}

func depObj(name, ns string, labels, annotations map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   ns,
			UID:         types.UID("uid-" + name),
			Labels:      labels,
			Annotations: annotations,
		},
	}
}

// startCollector boots an informer-backed Collector against the supplied
// fake clientset and waits for caches to sync. The returned cancel must be
// called by the caller (typically via defer) to stop informers.
func startCollector(t *testing.T, cfg *config.Config, client dynamic.Interface) (*Collector, *prometheus.Registry, context.CancelFunc) {
	t.Helper()
	if len(cfg.Watch.Resources) == 0 {
		cfg.Watch = legacyTestWatch()
	}
	col, err := New(cfg, client, discardLogger(), Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(col)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = col.Start(ctx) }()
	waitForCacheReady(t, col, 5*time.Second)
	return col, reg, cancel
}

// waitForCacheReady polls until each watched informer's cache contains at
// least one of the seeded objects (when the fake clientset has any).
func waitForCacheReady(t *testing.T, col *Collector, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, kind := range col.informers.WatchedKinds() {
			items := col.informers.ListAll(kind)
			if len(items) == 0 {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	// We don't fail here — some tests intentionally seed only specific
	// kinds. Callers asserting on metric content will surface the failure
	// loudly enough.
}

func gatherText(t *testing.T, gatherer prometheus.Gatherer) string {
	t.Helper()
	mfs, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var b bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&b, mf); err != nil {
			t.Fatalf("encode metric family %s: %v", mf.GetName(), err)
		}
	}
	return b.String()
}

func runtimeObjs(items ...runtime.Object) []runtime.Object {
	return items
}

func dynamicClientForObjects(t testing.TB, items ...runtime.Object) dynamic.Interface {
	return dynamicClientForObjectsWithListKinds(t, dynamicListKinds(), items...)
}

func dynamicClientForObjectsWithListKinds(t testing.TB, listKinds map[schema.GroupVersionResource]string, items ...runtime.Object) dynamic.Interface {
	t.Helper()
	objs := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		objs = append(objs, dynamicObject(t, item))
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

func dynamicListKinds() map[schema.GroupVersionResource]string {
	out := map[schema.GroupVersionResource]string{}
	for _, kind := range config.SupportedKinds() {
		info, _ := config.SupportedResource(kind)
		out[info.GVR()] = info.Kind + "List"
	}
	return out
}

func dynamicObject(t testing.TB, obj runtime.Object) runtime.Object {
	t.Helper()
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("to unstructured %T: %v", obj, err)
	}
	u := &unstructured.Unstructured{Object: m}
	gvks, _, err := clientscheme.Scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		t.Fatalf("object kind %T: %v", obj, err)
	}
	u.SetGroupVersionKind(gvks[0])
	return u
}

// ---------------------------------------------------------------------------
// Owner-chain non-regression: Pod -> ReplicaSet -> Deployment via
// topController must continue to populate metric labels with the parent's
// metadata even after the architecture switched to scrape-time collection.
// ---------------------------------------------------------------------------

func TestCollect_OwnerChainTopControllerLabels(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace":       {Path: "metadata.namespace"},
				"pod":             {Path: "metadata.name"},
				"controller_kind": {Source: "topController", Path: "kind"},
				"controller_name": {Source: "topController", Path: "metadata.name"},
				"controller_annotation_team": {
					Source: "topController",
					Path:   `metadata.annotations["team"]`,
				},
			},
		}},
	}

	d := depObj("dep1", "ns", nil, map[string]string{"team": "payments"})
	r := rsObj("rs1", "ns", "dep1")
	p := podWithOwner("p1", "ns", "rs1", nil, nil)

	client := dynamicClientForObjects(t, runtimeObjs(d, r, p)...)
	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	expectContain(t, out, `test_pod_info{`)
	expectContain(t, out, `controller_kind="Deployment"`)
	expectContain(t, out, `controller_name="dep1"`)
	expectContain(t, out, `controller_annotation_team="payments"`)
	expectContain(t, out, `namespace="ns"`)
	expectContain(t, out, `pod="p1"`)
}

// TestCollect_OwnerControllerVsTopController makes sure ownerController
// resolves to the immediate parent (ReplicaSet) while topController walks
// all the way to the Deployment, even when both rules are emitted at scrape
// time on the same anchor.
func TestCollect_OwnerControllerVsTopController(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace":  {Path: "metadata.namespace"},
				"pod":        {Path: "metadata.name"},
				"owner_kind": {Source: "ownerController", Path: "kind"},
				"owner_name": {Source: "ownerController", Path: "metadata.name"},
				"top_kind":   {Source: "topController", Path: "kind"},
				"top_name":   {Source: "topController", Path: "metadata.name"},
			},
		}},
	}

	d := depObj("dep1", "ns", nil, nil)
	r := rsObj("rs1", "ns", "dep1")
	p := podWithOwner("p1", "ns", "rs1", nil, nil)
	client := dynamicClientForObjects(t, runtimeObjs(d, r, p)...)
	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	expectContain(t, out, `owner_kind="ReplicaSet"`)
	expectContain(t, out, `owner_name="rs1"`)
	expectContain(t, out, `top_kind="Deployment"`)
	expectContain(t, out, `top_name="dep1"`)
}

// ---------------------------------------------------------------------------
// expandLabels: dynamic label names per metric, computed at scrape time. The
// union of dynamic keys is shared by all series so prometheus' "metric label
// set" invariant holds within a single scrape.
// ---------------------------------------------------------------------------

func TestCollect_DynamicExpandLabelsUnionAcrossPods(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
			},
			ExpandLabels: []config.ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	a := podWithOwner("a", "ns", "rs1", map[string]string{
		"app":          "shipping",
		"team":         "payments",
		"feature/flag": "true", // sanitised to label_feature_flag
	}, nil)
	b := podWithOwner("b", "ns", "rs1", map[string]string{
		"team": "search",
	}, nil)
	r := rsObj("rs1", "ns", "dep1")
	d := depObj("dep1", "ns", nil, nil)
	client := dynamicClientForObjects(t, runtimeObjs(d, r, a, b)...)

	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	got := gatherText(t, reg)

	// Pod a has all three labels populated; Pod b has only team. Both
	// series are emitted under the same metric, with the union of label
	// keys (label_app, label_feature_flag, label_team).
	expectContain(t, got, `test_pod_info{`)
	expectContain(t, got, `label_app="shipping"`)
	expectContain(t, got, `label_feature_flag="true"`)
	expectContain(t, got, `label_team="payments"`)
	expectContain(t, got, `label_team="search"`)
	// Pod b's series must carry empty values for keys it doesn't have.
	if !strings.Contains(got, `label_app=""`) {
		t.Errorf("pod b should carry empty label_app under union semantics:\n%s", got)
	}
	if !strings.Contains(got, `label_feature_flag=""`) {
		t.Errorf("pod b should carry empty label_feature_flag under union semantics:\n%s", got)
	}
}

// ---------------------------------------------------------------------------
// forEach + dynamic labels: per-container series should still inherit the
// pod's dynamic labels.
// ---------------------------------------------------------------------------

func TestCollect_ForEachWithExpandLabels(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:    "pod_container_info",
			Anchor:  "Pod",
			ForEach: "spec.containers[*]",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
				"container": {Source: "item", Path: "name"},
				"image":     {Source: "item", Path: "image"},
			},
			ExpandLabels: []config.ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api",
			Namespace: "ns",
			Labels:    map[string]string{"team": "payments"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "main", Image: "img:1"},
			{Name: "side", Image: "img:2"},
		}},
	}

	client := dynamicClientForObjects(t, runtimeObjs(p)...)
	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	expectContain(t, out, `container="main"`)
	expectContain(t, out, `container="side"`)
	expectContain(t, out, `image="img:1"`)
	expectContain(t, out, `image="img:2"`)
	// Dynamic label is inherited by every container series.
	expectContain(t, out, `label_team="payments"`)
}

func TestCollect_ServiceAndEndpointSliceAnchors(t *testing.T) {
	ctrl := true
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api",
			Namespace: "ns",
			UID:       types.UID("uid-service-api"),
			Labels:    map[string]string{"app": "api"},
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
	}
	ep := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-abc",
			Namespace: "ns",
			UID:       types.UID("uid-endpointslice-api"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Service",
				Name:       "api",
				UID:        svc.UID,
				Controller: &ctrl,
			}},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"10.0.0.10"},
		}},
	}
	cfg := &config.Config{
		MetricPrefix: "test_",
		Watch: config.WatchScope{Resources: []config.WatchResource{
			{Kind: "Service", Scope: config.ScopeNamespaced},
			{Kind: "EndpointSlice", Scope: config.ScopeNamespaced},
		}},
		Rules: []config.Rule{
			{
				Name:   "service_info",
				Anchor: "Service",
				Labels: map[string]config.Extract{
					"namespace": {Path: "metadata.namespace"},
					"service":   {Path: "metadata.name"},
					"type":      {Path: "spec.type"},
				},
			},
			{
				Name:   "endpointslice_info",
				Anchor: "EndpointSlice",
				Labels: map[string]config.Extract{
					"namespace": {Path: "metadata.namespace"},
					"slice":     {Path: "metadata.name"},
					"service":   {Source: "Service", Path: "metadata.name"},
					"addr_type": {Path: "addressType"},
				},
			},
		},
	}
	client := dynamicClientForObjects(t, svc, ep)
	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	expectContain(t, out, `test_service_info{`)
	expectContain(t, out, `service="api"`)
	expectContain(t, out, `type="ClusterIP"`)
	expectContain(t, out, `test_endpointslice_info{`)
	expectContain(t, out, `slice="api-abc"`)
	expectContain(t, out, `addr_type="IPv4"`)
}

func TestCollect_ArbitraryGVRAnchor(t *testing.T) {
	widgetGVR := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "widgets"}
	widget := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "example.com/v1",
		"kind":       "Widget",
		"metadata": map[string]interface{}{
			"name":      "w1",
			"namespace": "ns",
			"labels": map[string]interface{}{
				"team": "platform",
			},
		},
		"spec": map[string]interface{}{
			"size": "large",
		},
	}}
	cfg := &config.Config{
		MetricPrefix: "test_",
		Watch: config.WatchScope{Resources: []config.WatchResource{{
			Name:     "Widget",
			Group:    "example.com",
			Version:  "v1",
			Resource: "widgets",
			Kind:     "Widget",
			Scope:    config.ScopeNamespaced,
		}}},
		Rules: []config.Rule{{
			Name:   "widget_info",
			Anchor: "Widget",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"widget":    {Path: "metadata.name"},
				"size":      {Path: "spec.size"},
			},
		}},
	}
	client := dynamicClientForObjectsWithListKinds(t, map[schema.GroupVersionResource]string{
		widgetGVR: "WidgetList",
	}, widget)
	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	expectContain(t, out, `test_widget_info{`)
	expectContain(t, out, `namespace="ns"`)
	expectContain(t, out, `widget="w1"`)
	expectContain(t, out, `size="large"`)
}

// ---------------------------------------------------------------------------
// Cardinality controls: allow/deny/maxKeys.
// ---------------------------------------------------------------------------

func TestCollect_ExpandLabelsRespectsAllowAndMaxKeys(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:   "pod_meta",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
			},
			ExpandLabels: []config.ExpandLabel{
				{
					Path:    "metadata.annotations",
					Prefix:  "annotation_",
					Allow:   []string{"annotation-a", "annotation-b", "annotation-c"},
					Deny:    []string{"annotation-c"},
					MaxKeys: 1,
				},
			},
		}},
	}
	p := podWithOwner("p1", "ns", "rs1", nil, map[string]string{
		"annotation-a": "1",
		"annotation-b": "2",
		"annotation-c": "3",
		"unrelated":    "4",
	})
	r := rsObj("rs1", "ns", "dep1")
	d := depObj("dep1", "ns", nil, nil)
	client := dynamicClientForObjects(t, runtimeObjs(d, r, p)...)

	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	out := gatherText(t, reg)
	// Allow filters out 'unrelated'; deny removes 'annotation-c'; MaxKeys=1
	// keeps the lexicographically smallest survivor (annotation-a).
	expectContain(t, out, `annotation_annotation_a="1"`)
	if strings.Contains(out, "annotation_annotation_b") {
		t.Errorf("MaxKeys=1 should drop annotation_annotation_b:\n%s", out)
	}
	if strings.Contains(out, "annotation_annotation_c") {
		t.Errorf("denied annotation_annotation_c should not appear:\n%s", out)
	}
	if strings.Contains(out, "annotation_unrelated") {
		t.Errorf("unrelated annotation should not appear (allow restricts):\n%s", out)
	}
}

// ---------------------------------------------------------------------------
// Scrape-time freshness: changing a Pod's labels and re-scraping must reflect
// the new labels without restarting the exporter and without affecting other
// pods that did not change.
// ---------------------------------------------------------------------------

func TestCollect_PerPodIndependence(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
			},
			ExpandLabels: []config.ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
	a := podWithOwner("a", "ns", "rs1", map[string]string{"team": "payments"}, nil)
	b := podWithOwner("b", "ns", "rs1", map[string]string{"team": "search"}, nil)
	r := rsObj("rs1", "ns", "dep1")
	d := depObj("dep1", "ns", nil, nil)
	client := dynamicClientForObjects(t, runtimeObjs(d, r, a, b)...)

	_, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	first := gatherText(t, reg)
	expectContain(t, first, `pod="a"`)
	expectContain(t, first, `pod="b"`)
	expectContain(t, first, `label_team="payments"`)
	expectContain(t, first, `label_team="search"`)

	// Mutate Pod a's labels via the fake client and let the informer
	// observe the change.
	updated := a.DeepCopy()
	updated.Labels = map[string]string{"team": "platform", "owner": "alice"}
	updated.ResourceVersion = "2"
	podInfo, _ := config.SupportedResource("Pod")
	updatedObj := dynamicObject(t, updated).(*unstructured.Unstructured)
	if _, err := client.Resource(podInfo.GVR()).Namespace("ns").Update(context.Background(), updatedObj, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod a: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		out := gatherText(t, reg)
		if strings.Contains(out, `label_owner="alice"`) {
			expectContain(t, out, `label_team="platform"`)
			// Pod b's series must remain unaffected by Pod a's mutation.
			expectContain(t, out, `pod="b"`)
			expectContain(t, out, `label_team="search"`)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("scrape never reflected updated labels for pod a")
}

// ---------------------------------------------------------------------------
// Self-metric registration is idempotent and exposes the canonical names.
// ---------------------------------------------------------------------------

func TestSelfMetrics_Names(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Watch:        legacyTestWatch("Pod"),
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
			},
		}},
	}
	reg := prometheus.NewRegistry()
	col, err := New(cfg, dynamicClientForObjects(t), discardLogger(), Options{Registerer: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg.MustRegister(col)

	got := gatherText(t, reg)
	for _, name := range []string{
		"exporter_collect_total",
		"exporter_anchor_count",
	} {
		if !strings.Contains(got, name) {
			t.Errorf("self-metric %q absent in /metrics output:\n%s", name, got)
		}
	}
}

// TestCollect_ZeroRules covers history-only mode, where the collector exists
// solely to own the informers the history ingest borrows. It must construct,
// register, start its informers, and scrape cleanly with no rules at all.
func TestCollect_ZeroRules(t *testing.T) {
	cfg := &config.Config{
		MetricPrefix: "test_",
		Watch:        legacyTestWatch(),
	}
	client := dynamicClientForObjects(t, runtimeObjs(podWithOwner("p1", "ns", "", nil, nil))...)
	col, reg, cancel := startCollector(t, cfg, client)
	defer cancel()

	// The informers still cover watch.resources — that is what history needs.
	if got := col.Informers().Informers("Pod"); len(got) == 0 {
		t.Fatal("Pod informer should be running even with zero rules")
	}

	out := gatherText(t, reg)
	if strings.Contains(out, "test_") {
		t.Errorf("zero rules should emit no rule metrics, got:\n%s", out)
	}
}

// expectContain is a small helper that fails the test with the full output
// for easier debugging when an expected substring is missing.
func expectContain(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("expected output to contain %q, got:\n%s", want, got)
	}
}
