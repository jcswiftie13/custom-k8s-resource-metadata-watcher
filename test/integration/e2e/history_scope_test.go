//go:build integration

package e2e

import (
	"fmt"
	"testing"
	"time"
)

// scopedHistoryConfigYAML renders a minimal history config whose writer
// identity is the `cluster` constant (inline value, so the test can switch
// identities without touching the Deployment env) and whose recovery reads
// are scoped to it via store.scopeColumn.
func scopedHistoryConfigYAML(namespace, cluster string) string {
	return fmt.Sprintf(`metricPrefix: "it_"

watch:
  resources:
    - kind: Service
      scope: Namespaced
      namespaces:
        - %[1]s

history:
  enabled: true
  store:
    type: clickhouse
    dsn: %[3]q
    createSchema: true
    closeMode: update
    scopeColumn: cluster
    batch:
      maxRows: 100
      flushIntervalMs: 200
  constants:
    - { name: cluster, type: String, value: %[2]q }
  resources:
    - kind: Service
      table: scope_svc_versions
      columns:
        - { name: host, type: "String", path: "metadata.name" }
`, namespace, cluster, clickhouseNativeDSN)
}

// TestHistory_ScopeIsolatesClusters is the end-to-end regression for the
// multi-writer shared-table incident: exporters in different k8s clusters
// write to one ClickHouse, and without scoping, writer B's restart recovery
// adopted writer A's open rows and its orphan reconciliation closed them
// (A's objects are absent from B's informers) — then A's next restart
// re-opened them, and the two fought forever.
//
// One kind cluster suffices: switching the exporter's config to a different
// `cluster` constant + a different watched namespace IS, from ClickHouse's
// point of view, another cluster's exporter starting up (fresh Recover +
// ReconcileOpens under a different identity, with an informer cache that
// does not contain the other identity's objects).
func TestHistory_ScopeIsolatesClusters(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	chWaitReady(t)
	if _, err := chExec("DROP TABLE IF EXISTS scope_svc_versions"); err != nil {
		t.Fatalf("drop scope_svc_versions: %v", err)
	}

	nsA, nsB := "e2e-scope-a", "e2e-scope-b"
	createNamespaces(t, nsA, nsB)
	t.Cleanup(func() { deleteNamespaces(t, nsA, nsB) })
	dyn := mustDynamicClient(t)

	mkSvc := func(ns, name string) map[string]interface{} {
		return map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata":   map[string]interface{}{"namespace": ns, "name": name},
			"spec": map[string]interface{}{
				"type":  "ClusterIP",
				"ports": []interface{}{map[string]interface{}{"port": int64(80), "name": "http", "protocol": "TCP"}},
			},
		}
	}
	openSentinel := "valid_to = toDateTime64('2200-01-01 00:00:00', 3)"
	svcXOpen := "SELECT count() FROM scope_svc_versions FINAL WHERE name='svc-x' AND cluster='cluster-a' AND " + openSentinel

	// Phase A: the exporter runs as cluster-a watching ns-a and records svc-x.
	createObject(t, dyn, svcGVR, nsA, mkSvc(nsA, "svc-x"))
	setExporterConfig(t, scopedHistoryConfigYAML(nsA, "cluster-a"))
	waitForCHCount(t, svcXOpen, 1)

	// Phase B: the exporter comes back as cluster-b watching ns-b — the exact
	// shape of the incident. Its Recover must not adopt cluster-a's rows, so
	// its ReconcileOpens must not close them. svc-y landing proves B's replay
	// completed; the observation window then catches the old behavior, where
	// the reconcile close (flushed within 200ms) zeroed svc-x's open count at
	// B's startup time.
	createObject(t, dyn, svcGVR, nsB, mkSvc(nsB, "svc-y"))
	setExporterConfig(t, scopedHistoryConfigYAML(nsB, "cluster-b"))
	waitForCHCount(t, "SELECT count() FROM scope_svc_versions FINAL WHERE name='svc-y' AND cluster='cluster-b' AND "+openSentinel, 1)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := chQuery(t, svcXOpen); got != "1" {
			t.Fatalf("cluster-a's svc-x row was closed by the cluster-b exporter (open count %q); scope isolation failed", got)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Reverse check: scoping must not disable own-scope orphan reconciliation.
	// A fake open row under cluster-b's identity, with no backing object,
	// must be closed on cluster-b's next restart — while cluster-a's row
	// still stays untouched.
	if _, err := chExec("INSERT INTO scope_svc_versions " +
		"(namespace, name, uid, valid_from, valid_to, ingest_seq, cluster, host) VALUES " +
		"('" + nsB + "', 'ghost', 'uid-ghost', now64(3), toDateTime64('2200-01-01 00:00:00', 3), 1, 'cluster-b', 'ghost')"); err != nil {
		t.Fatalf("insert ghost row: %v", err)
	}
	restartExporter(t)
	waitForCHCount(t, "SELECT count() FROM scope_svc_versions FINAL WHERE name='ghost' AND "+openSentinel, 0)
	if got := chQuery(t, svcXOpen); got != "1" {
		t.Fatalf("cluster-a's svc-x row must survive cluster-b's reconcile, open count %q", got)
	}
}
