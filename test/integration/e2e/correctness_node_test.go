//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCorrectness_NodeMetrics(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	t.Cleanup(func() { printExporterMetricsSnapshotIfEnabled(t, t.Name(), snapMetricsCorrectnessNode) })

	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	wantCount := len(listNodes(t))
	if wantCount == 0 {
		t.Fatalf("cluster has no nodes")
	}

	// Re-apply ExternalIP patches: kubelet may have removed addresses added at
	// cluster bring-up before earlier tests (e.g. burden) finish.
	patchKindNodeExternalIPs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		cur := listNodes(t)
		if len(cur) != wantCount {
			return false, nil
		}
		mfs := scrapeExporterMetrics(t)
		for i := range cur {
			if !nodeReadyInMetrics(mfs, &cur[i]) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("node metrics did not converge for all %d nodes: %v", wantCount, err)
	}

	ctxFinal, cancelFinal := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelFinal()
	if err := waitFor(ctxFinal, 2*time.Second, func(ctx context.Context) (bool, error) {
		cur := listNodes(t)
		if len(cur) != wantCount {
			return false, nil
		}
		mfs := scrapeExporterMetrics(t)
		for i := range cur {
			if !nodeReadyInMetrics(mfs, &cur[i]) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatalf("final node metrics consistency check failed for %d nodes: %v", wantCount, err)
	}
}

func TestCorrectness_NodeDynamicMetadataSingleKey(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	setExporterConfig(t, dynamicMetadataConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	nodes := listNodes(t)
	if len(nodes) == 0 {
		t.Fatalf("cluster has no nodes")
	}
	nodeName := nodes[0].Name

	labelKey := "integration.test/node-label-single"
	annoKey := "integration.test/node-annotation-single"
	if err := patchNodeMetadataKeys(t, nodeName,
		map[string]interface{}{labelKey: "label-single"},
		map[string]interface{}{annoKey: "anno-single"},
	); err != nil {
		t.Fatalf("patch node metadata (single key): %v", err)
	}
	t.Cleanup(func() {
		_ = patchNodeMetadataKeys(t, nodeName,
			map[string]interface{}{labelKey: nil},
			map[string]interface{}{annoKey: nil},
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return metricHasExactLabels(mfs, "it_node_dynamic_metadata", map[string]string{
			"node_name":                              nodeName,
			"label_integration_test_node_label_single":      "label-single",
			"annotation_integration_test_node_annotation_single": "anno-single",
		}), nil
	}); err != nil {
		t.Fatalf("dynamic node metadata (single key) did not converge: %v", err)
	}
}

func TestCorrectness_NodeDynamicMetadataMultiKey(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	setExporterConfig(t, dynamicMetadataConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	nodes := listNodes(t)
	if len(nodes) == 0 {
		t.Fatalf("cluster has no nodes")
	}
	nodeName := nodes[0].Name

	labelPatch := map[string]interface{}{
		"integration.test/node-label-a": "label-a",
		"integration.test/node.label.b": "label-b",
	}
	annotationPatch := map[string]interface{}{
		"integration.test/node-annotation-a": "anno-a",
		"integration.test/node.annotation.b": "anno-b",
	}
	if err := patchNodeMetadataKeys(t, nodeName, labelPatch, annotationPatch); err != nil {
		t.Fatalf("patch node metadata (multi key): %v", err)
	}
	t.Cleanup(func() {
		_ = patchNodeMetadataKeys(t, nodeName,
			map[string]interface{}{
				"integration.test/node-label-a": nil,
				"integration.test/node.label.b": nil,
			},
			map[string]interface{}{
				"integration.test/node-annotation-a": nil,
				"integration.test/node.annotation.b": nil,
			},
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return metricHasExactLabels(mfs, "it_node_dynamic_metadata", map[string]string{
			"node_name":                                nodeName,
			"label_integration_test_node_label_a":      "label-a",
			"label_integration_test_node_label_b":      "label-b",
			"annotation_integration_test_node_annotation_a": "anno-a",
			"annotation_integration_test_node_annotation_b": "anno-b",
		}), nil
	}); err != nil {
		t.Fatalf("dynamic node metadata (multi key) did not converge: %v", err)
	}
}

func nodeReadyInMetrics(mfs map[string]*dto.MetricFamily, n *corev1.Node) bool {
	internalIPs, externalIPs := nodeIPs(n)
	if len(internalIPs) == 0 || len(externalIPs) == 0 || len(n.Status.Conditions) == 0 {
		return false
	}
	return hasNodeInfo(mfs, n.Name, n.Status.NodeInfo.SystemUUID) &&
		hasNodeAddress(mfs, n.Name, "InternalIP", internalIPs) &&
		hasNodeAddress(mfs, n.Name, "ExternalIP", externalIPs) &&
		hasNodeCondition(mfs, n.Name, n.Status.Conditions)
}

func assertNodeMetricsExported(t *testing.T, node *corev1.Node) {
	t.Helper()
	internalIPs, externalIPs := nodeIPs(node)
	if len(internalIPs) == 0 {
		t.Fatalf("node %q has no InternalIP", node.Name)
	}
	if len(externalIPs) == 0 {
		t.Fatalf("node %q has no ExternalIP in API (integration runner should patch RFC5737 addresses)", node.Name)
	}
	if len(node.Status.Conditions) == 0 {
		t.Fatalf("node %q has no conditions", node.Name)
	}

	mfs := scrapeExporterMetrics(t)
	if !hasNodeInfo(mfs, node.Name, node.Status.NodeInfo.SystemUUID) {
		t.Fatalf("it_node_info missing or mismatched for node_name=%q", node.Name)
	}
	if !hasNodeAddress(mfs, node.Name, "InternalIP", internalIPs) {
		t.Fatalf("it_node_address missing InternalIP for node_name=%q", node.Name)
	}
	if !hasNodeAddress(mfs, node.Name, "ExternalIP", externalIPs) {
		t.Fatalf("it_node_address missing ExternalIP for node_name=%q", node.Name)
	}
	if !hasNodeCondition(mfs, node.Name, node.Status.Conditions) {
		t.Fatalf("it_node_condition missing expected (condition,status) pair for node_name=%q", node.Name)
	}
}

func nodeIPs(node *corev1.Node) (internal []string, external []string) {
	for _, a := range node.Status.Addresses {
		switch a.Type {
		case corev1.NodeInternalIP:
			internal = append(internal, a.Address)
		case corev1.NodeExternalIP:
			external = append(external, a.Address)
		}
	}
	return internal, external
}

func hasNodeInfo(mfs map[string]*dto.MetricFamily, nodeName, systemUUID string) bool {
	mf, ok := mfs["it_node_info"]
	if !ok {
		return false
	}
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["node_name"] == nodeName && l["system_uuid"] == systemUUID {
			return true
		}
	}
	return false
}

func hasNodeAddress(mfs map[string]*dto.MetricFamily, nodeName, addrType string, want []string) bool {
	mf, ok := mfs["it_node_address"]
	if !ok {
		return false
	}
	seen := map[string]struct{}{}
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["node_name"] != nodeName || l["address_type"] != addrType {
			continue
		}
		seen[l["address"]] = struct{}{}
	}
	for _, a := range want {
		if _, ok := seen[a]; ok {
			return true
		}
	}
	return false
}

func hasNodeCondition(mfs map[string]*dto.MetricFamily, nodeName string, conds []corev1.NodeCondition) bool {
	mf, ok := mfs["it_node_condition"]
	if !ok {
		return false
	}
	seen := map[string]struct{}{}
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["node_name"] != nodeName {
			continue
		}
		seen[l["condition"]+"="+l["status"]] = struct{}{}
	}
	for _, c := range conds {
		key := string(c.Type) + "=" + string(c.Status)
		if _, ok := seen[key]; ok {
			return true
		}
	}
	return false
}

func patchNodeMetadataKeys(t *testing.T, nodeName string, labels map[string]interface{}, annotations map[string]interface{}) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := map[string]interface{}{
		"metadata": map[string]interface{}{},
	}
	meta := body["metadata"].(map[string]interface{})
	if labels != nil {
		meta["labels"] = labels
	}
	if annotations != nil {
		meta["annotations"] = annotations
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	cs := mustClient(t)
	_, err = cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, raw, metav1.PatchOptions{})
	return err
}
