//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type customGVRFixture struct {
	group    string
	version  string
	plural   string
	singular string
	kind     string
}

func (f customGVRFixture) apiVersion() string {
	return f.group + "/" + f.version
}

func (f customGVRFixture) crdName() string {
	return f.plural + "." + f.group
}

func (f customGVRFixture) gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: f.group, Version: f.version, Resource: f.plural}
}

var (
	crdGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

	arbitraryWidgetGVR = customGVRFixture{
		group:    "integration.metadata-exporter.example.com",
		version:  "v1",
		plural:   "widgets",
		singular: "widget",
		kind:     "Widget",
	}
	discoveryWidgetGVR = customGVRFixture{
		group:    "integration.metadata-exporter.example.com",
		version:  "v1",
		plural:   "discoverywidgets",
		singular: "discoverywidget",
		kind:     "DiscoveryWidget",
	}
)

func TestCorrectness_ArbitraryGVRWidget(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-custom-gvr-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })
	t.Cleanup(func() { printExporterMetricsSnapshotIfEnabled(t, t.Name(), nil) })

	dyn := mustDynamicClient(t)
	fixture := arbitraryWidgetGVR
	ensureWidgetCRD(t, dyn, fixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, fixture) })
	createWidget(t, dyn, fixture, ns, "w1", "large")

	setExporterConfig(t, customGVRConfigYAML(ns, fixture.group, fixture.version, fixture.plural, fixture.kind))

	if err := waitFor(context.Background(), 60*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return metricHasExactLabels(mfs, "it_widget_info", map[string]string{
			"namespace": ns,
			"widget":    "w1",
			"size":      "large",
		}), nil
	}); err != nil {
		t.Fatalf("custom GVR widget metric did not converge: %v", err)
	}
}

func TestCorrectness_DiscoveryResolvesCustomGVRWidget(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-custom-gvr-discovery-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })
	t.Cleanup(func() { printExporterMetricsSnapshotIfEnabled(t, t.Name(), nil) })

	dyn := mustDynamicClient(t)
	fixture := discoveryWidgetGVR
	ensureWidgetCRD(t, dyn, fixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, fixture) })
	createWidget(t, dyn, fixture, ns, "w-discovery", "medium")

	setExporterConfig(t, customGVRDiscoveryConfigYAML(ns, fixture.apiVersion(), fixture.kind))
	assertExporterLogContains(t, `"discoveryEnabled":true`)

	if err := waitFor(context.Background(), 60*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return metricHasExactLabels(mfs, "it_widget_info", map[string]string{
			"namespace": ns,
			"widget":    "w-discovery",
			"size":      "medium",
		}), nil
	}); err != nil {
		t.Fatalf("discovered custom GVR widget metric did not converge: %v", err)
	}
}

func mustDynamicClient(t *testing.T) dynamic.Interface {
	t.Helper()
	client, err := dynamic.NewForConfig(shared.cfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	return client
}

func ensureWidgetCRD(t *testing.T, dyn dynamic.Interface, fixture customGVRFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for {
		_, err := dyn.Resource(crdGVR).Create(ctx, widgetCRD(fixture), metav1.CreateOptions{})
		if err == nil {
			break
		}
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create widget CRD: %v", err)
		}
		got, err := dyn.Resource(crdGVR).Get(ctx, fixture.crdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			t.Fatalf("get existing widget CRD: %v", err)
		}
		if got.GetDeletionTimestamp() == nil {
			break
		}
		if err := waitForWidgetCRDDeleted(ctx, dyn, fixture); err != nil {
			t.Fatalf("wait for deleting widget CRD: %v", err)
		}
	}
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		got, err := dyn.Resource(crdGVR).Get(ctx, fixture.crdName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		conditions, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
		for _, raw := range conditions {
			cond, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			if cond["type"] == "Established" && cond["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("wait for widget CRD established: %v", err)
	}
	if err := waitForWidgetAPIResource(ctx, dyn, fixture); err != nil {
		t.Fatalf("wait for widget API resource: %v", err)
	}
}

func widgetCRD(fixture customGVRFixture) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata": map[string]interface{}{
			"name": fixture.crdName(),
		},
		"spec": map[string]interface{}{
			"group": fixture.group,
			"scope": "Namespaced",
			"names": map[string]interface{}{
				"plural":   fixture.plural,
				"singular": fixture.singular,
				"kind":     fixture.kind,
			},
			"versions": []interface{}{map[string]interface{}{
				"name":    fixture.version,
				"served":  true,
				"storage": true,
				"schema": map[string]interface{}{
					"openAPIV3Schema": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"spec": map[string]interface{}{
								"type":                                 "object",
								"x-kubernetes-preserve-unknown-fields": true,
							},
						},
					},
				},
			}},
		},
	}}
}

func waitForWidgetAPIResource(ctx context.Context, dyn dynamic.Interface, fixture customGVRFixture) error {
	return waitFor(ctx, 500*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(fixture.gvr()).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{Limit: 1})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	})
}

func waitForWidgetCRDDeleted(ctx context.Context, dyn dynamic.Interface, fixture customGVRFixture) error {
	return waitFor(ctx, 500*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(crdGVR).Get(ctx, fixture.crdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func deleteWidgetCRD(t *testing.T, dyn dynamic.Interface, fixture customGVRFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err := dyn.Resource(crdGVR).Delete(ctx, fixture.crdName(), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		t.Logf("delete widget CRD: %v", err)
		return
	}
	if err := waitForWidgetCRDDeleted(ctx, dyn, fixture); err != nil {
		t.Logf("wait for widget CRD deletion: %v", err)
	}
}

func createWidget(t *testing.T, dyn dynamic.Interface, fixture customGVRFixture, namespace, name, size string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	widget := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": fixture.apiVersion(),
		"kind":       fixture.kind,
		"metadata": map[string]interface{}{
			"namespace": namespace,
			"name":      name,
		},
		"spec": map[string]interface{}{
			"size": size,
		},
	}}
	if err := waitFor(ctx, 500*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(fixture.gvr()).Namespace(namespace).Create(ctx, widget, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return true, nil
		}
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatalf("create widget: %v", err)
	}
}
