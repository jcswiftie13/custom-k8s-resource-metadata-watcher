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

const (
	customGVRGroup   = "integration.metadata-exporter.example.com"
	customGVRVersion = "v1"
	customGVRPlural  = "widgets"
	customGVRKind    = "Widget"
	customCRDName    = customGVRPlural + "." + customGVRGroup
)

var (
	crdGVR    = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	widgetGVR = schema.GroupVersionResource{Group: customGVRGroup, Version: customGVRVersion, Resource: customGVRPlural}
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
	ensureWidgetCRD(t, dyn)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn) })
	createWidget(t, dyn, ns, "w1", "large")

	setExporterConfig(t, customGVRConfigYAML(ns))
	restartExporter(t)

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

func mustDynamicClient(t *testing.T) dynamic.Interface {
	t.Helper()
	client, err := dynamic.NewForConfig(shared.cfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	return client
}

func ensureWidgetCRD(t *testing.T, dyn dynamic.Interface) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	crd := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata": map[string]interface{}{
			"name": customCRDName,
		},
		"spec": map[string]interface{}{
			"group": customGVRGroup,
			"scope": "Namespaced",
			"names": map[string]interface{}{
				"plural":   customGVRPlural,
				"singular": "widget",
				"kind":     customGVRKind,
			},
			"versions": []interface{}{map[string]interface{}{
				"name":    customGVRVersion,
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
	if _, err := dyn.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create widget CRD: %v", err)
	}
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		got, err := dyn.Resource(crdGVR).Get(ctx, customCRDName, metav1.GetOptions{})
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
}

func deleteWidgetCRD(t *testing.T, dyn dynamic.Interface) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := dyn.Resource(crdGVR).Delete(ctx, customCRDName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Logf("delete widget CRD: %v", err)
	}
}

func createWidget(t *testing.T, dyn dynamic.Interface, namespace, name, size string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	widget := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": customGVRGroup + "/" + customGVRVersion,
		"kind":       customGVRKind,
		"metadata": map[string]interface{}{
			"namespace": namespace,
			"name":      name,
		},
		"spec": map[string]interface{}{
			"size": size,
		},
	}}
	if _, err := dyn.Resource(widgetGVR).Namespace(namespace).Create(ctx, widget, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create widget: %v", err)
	}
}
