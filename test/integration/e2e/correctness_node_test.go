//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
)

func TestCorrectness_NodeMetrics(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

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

	finalNodes := listNodes(t)
	if len(finalNodes) != wantCount {
		t.Fatalf("node count changed: had %d, now %d", wantCount, len(finalNodes))
	}
	for i := range finalNodes {
		assertNodeMetricsExported(t, &finalNodes[i])
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
