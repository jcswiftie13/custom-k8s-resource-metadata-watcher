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

	node := pickNodeForMetricsTest(t)
	internalIPs, externalIPs := nodeIPs(node)
	if len(internalIPs) == 0 {
		t.Fatalf("node %q has no InternalIP; cannot validate internal_ip metric", node.Name)
	}
	if len(node.Status.Conditions) == 0 {
		t.Fatalf("node %q has no conditions; cannot validate condition/status metrics", node.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return hasNodeInfo(mfs, node.Name, node.Status.NodeInfo.SystemUUID) &&
			hasNodeAddress(mfs, node.Name, "InternalIP", internalIPs) &&
			hasNodeCondition(mfs, node.Name, node.Status.Conditions), nil
	}); err != nil {
		t.Fatalf("node metrics did not converge for node=%q: %v", node.Name, err)
	}

	mfs := scrapeExporterMetrics(t)
	if !hasNodeInfo(mfs, node.Name, node.Status.NodeInfo.SystemUUID) {
		t.Fatalf("it_node_info missing or mismatched for node_name=%q", node.Name)
	}
	if !hasNodeAddress(mfs, node.Name, "InternalIP", internalIPs) {
		t.Fatalf("it_node_address missing InternalIP for node_name=%q", node.Name)
	}
	if len(externalIPs) > 0 {
		if !hasNodeAddress(mfs, node.Name, "ExternalIP", externalIPs) {
			t.Fatalf("it_node_address missing ExternalIP for node_name=%q", node.Name)
		}
	} else {
		t.Logf("node %q has no ExternalIP in API; skip strict external_ip assertion", node.Name)
	}
	if !hasNodeCondition(mfs, node.Name, node.Status.Conditions) {
		t.Fatalf("it_node_condition missing expected (condition,status) pair for node_name=%q", node.Name)
	}
}

func pickNodeForMetricsTest(t *testing.T) *corev1.Node {
	t.Helper()
	nodes := listNodes(t)
	if len(nodes) == 0 {
		t.Fatalf("cluster has no nodes")
	}
	for i := range nodes {
		n := &nodes[i]
		if hasAddressType(n, corev1.NodeInternalIP) {
			return n
		}
	}
	return &nodes[0]
}

func hasAddressType(node *corev1.Node, typ corev1.NodeAddressType) bool {
	for _, a := range node.Status.Addresses {
		if a.Type == typ {
			return true
		}
	}
	return false
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
