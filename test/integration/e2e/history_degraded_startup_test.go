//go:build integration

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// TestHistory_DegradedStartupReconnect covers the history manager's
// degraded-startup contract end-to-end:
//
//  1. With ClickHouse unreachable, the exporter still starts and turns Ready
//     (readiness is /readyz, which ignores history health) — previously this
//     crashlooped on store.Open.
//  2. /readyz serves 200 with history state "connecting";
//     exporter_history_store_connected is 0.
//  3. Once ClickHouse returns, the manager connects in the background and the
//     cache replay lands version rows for objects that existed before startup
//     AND objects created during the outage.
//  4. ReconcileOpens: an object deleted while the exporter was down has its
//     orphaned open row closed after restart.
func TestHistory_DegradedStartupReconnect(t *testing.T) {
	ns := "e2e-history-reconn-0"
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	// Start from a healthy ClickHouse and fresh tables (the exporter recreates
	// them via createSchema:true when it first connects).
	chWaitReady(t)
	for _, tbl := range []string{"service_versions", "deploy_versions", "gw_versions", "vs_versions"} {
		if _, err := chExec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}

	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	dyn := mustDynamicClient(t)
	ensureWidgetCRD(t, dyn, istioGatewayFixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, istioGatewayFixture) })
	ensureWidgetCRD(t, dyn, istioVSFixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, istioVSFixture) })

	// svc-pre exists before the exporter (re)starts against a dead store.
	createObject(t, dyn, svcGVR, ns, lbServiceObj(ns, "svc-pre"))

	// Take ClickHouse down BEFORE pointing the exporter at it.
	scaleClickHouse(t, 0)
	t.Cleanup(func() { scaleClickHouse(t, 1) })

	// The core assertion: the rollout completes — the pod turns Ready with the
	// store unreachable. Before the manager existed, this wait timed out on a
	// crashloop.
	setExporterConfig(t, istioHistoryConfigYAMLWithDSN(ns, clickhouseNativeDSN))

	body := fetchExporterReadyz(t)
	if !strings.Contains(body, `"informersSynced":true`) {
		t.Fatalf("/readyz should report synced informers, got: %s", body)
	}
	if !strings.Contains(body, `"state":"connecting"`) {
		t.Fatalf("/readyz should report history connecting while the store is down, got: %s", body)
	}
	waitForConnectedGauge(t, 0)

	// Created during the outage: must land later via the late-registration
	// cache replay, not be lost.
	createObject(t, dyn, svcGVR, ns, lbServiceObj(ns, "svc-outage"))

	scaleClickHouse(t, 1)
	chWaitReady(t)
	waitForConnectedGauge(t, 1)

	waitForCHCount(t, "SELECT count() FROM service_versions WHERE namespace='"+ns+"' AND name='svc-pre'", 1)
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE namespace='"+ns+"' AND name='svc-outage'", 1)

	// --- ReconcileOpens: delete svc-outage while the exporter is down; the
	// restarted exporter must close the orphaned open row (closeMode=update
	// patches valid_to in place, so the row count stays 1). ---
	scaleExporter(t, 0)
	deleteObject(t, dyn, svcGVR, ns, "svc-outage")
	scaleExporter(t, 1)

	waitForCHCount(t, "SELECT count() FROM service_versions WHERE namespace='"+ns+
		"' AND name='svc-outage' AND valid_to < '2100-01-01'", 1)
	// svc-pre still exists and must keep its open row.
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE namespace='"+ns+
		"' AND name='svc-pre' AND valid_to > '2100-01-01'", 1)

	// --- Runtime outage: an object changes while the store is down (the
	// write is lost), so on recovery the manager must detect the loss and
	// rebuild the pipeline — the object's CURRENT state gets written instead
	// of staying stale until its next change. ---
	scaleClickHouse(t, 0)
	waitForConnectedGauge(t, 0) // health ping flips to degraded
	updateServicePortName(t, dyn, ns, "svc-pre", "grpc")
	scaleClickHouse(t, 1)
	chWaitReady(t)
	// Loss-triggered rebuild: reconnect (spaced up to 60s), Recover, replay.
	waitForConnectedGauge(t, 1)
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE namespace='"+ns+
		"' AND name='svc-pre' AND port_name='grpc' AND valid_to > '2100-01-01'", 1)
}

// lbServiceObj is a minimal LoadBalancer Service that passes the history
// filter (type in {LoadBalancer, NodePort}).
func lbServiceObj(ns, name string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]interface{}{"namespace": ns, "name": name},
		"spec": map[string]interface{}{
			"type":     "LoadBalancer",
			"selector": map[string]interface{}{"app": name},
			"ports": []interface{}{
				map[string]interface{}{"port": int64(80), "name": "http", "protocol": "TCP"},
			},
		},
	}
}

// scaleClickHouse scales the in-cluster ClickHouse Deployment and waits for
// the replica count to settle (rollout readiness is chWaitReady's job).
func scaleClickHouse(t *testing.T, replicas int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cs := mustClient(t)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dep, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, "clickhouse", metav1.GetOptions{})
		if err != nil {
			return err
		}
		dep.Spec.Replicas = &replicas
		_, err = cs.AppsV1().Deployments(exporterNamespace).Update(ctx, dep, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("scale clickhouse deployment: %v", err)
	}
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, "clickhouse", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return d.Status.Replicas == replicas && d.Status.ReadyReplicas == replicas, nil
	}); err != nil {
		t.Fatalf("clickhouse replicas did not settle to %d: %v", replicas, err)
	}
}

// fetchExporterReadyz GETs /readyz from the ready exporter pod via the
// API-server pod proxy and returns the JSON body.
func fetchExporterReadyz(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	pod, err := exporterPodNameErr(ctx, cs)
	if err != nil {
		t.Fatalf("find exporter pod: %v", err)
	}
	body, err := cs.CoreV1().RESTClient().
		Get().
		Namespace(exporterNamespace).
		Resource("pods").
		Name(fmt.Sprintf("http:%s:8080", pod)).
		SubResource("proxy").
		Suffix("readyz").
		DoRaw(ctx)
	if err != nil {
		t.Fatalf("GET /readyz via pod proxy: %v", err)
	}
	return string(body)
}

// waitForConnectedGauge polls the exporter /metrics until
// exporter_history_store_connected reports want.
func waitForConnectedGauge(t *testing.T, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	needle := fmt.Sprintf("exporter_history_store_connected %d", want)
	if err := waitFor(ctx, 2*time.Second, func(context.Context) (bool, error) {
		body, err := fetchExporterMetricsRaw(t)
		if err != nil {
			return false, nil // pod may be mid-roll; keep polling
		}
		return strings.Contains(string(body), needle), nil
	}); err != nil {
		t.Fatalf("exporter_history_store_connected never became %d: %v", want, err)
	}
}
