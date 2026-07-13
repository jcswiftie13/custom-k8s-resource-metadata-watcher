//go:build integration

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// This e2e mirrors the POC's ClickHouse routing-history shape end-to-end: it
// creates real Istio-flavored resources in the kind cluster (Service,
// Deployment, Istio Gateway, VirtualService) and asserts the exporter Pod (with
// history enabled) writes the expected version rows into the in-cluster
// ClickHouse. The four required aspects are all exercised: create table, type,
// filter, and server-side labelSelector. No poc/ code is imported — the POC
// schema/resource set is reproduced purely via history config + minimal CRDs.

var (
	svcGVR    = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	deployGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	istioGatewayFixture = customGVRFixture{
		group: "networking.istio.io", version: "v1beta1",
		plural: "gateways", singular: "gateway", kind: "Gateway",
	}
	istioVSFixture = customGVRFixture{
		group: "networking.istio.io", version: "v1beta1",
		plural: "virtualservices", singular: "virtualservice", kind: "VirtualService",
	}
)

func TestHistory_ClickHouseIstioSchema(t *testing.T) {
	runHistoryClickHouseIstioSchema(t, "e2e-history-0", clickhouseNativeDSN)
}

// TestHistory_ClickHouseHTTP is the HTTP-protocol twin of
// TestHistory_ClickHouseIstioSchema. It points the exporter at ClickHouse's
// HTTP interface (8123) via an http:// DSN — the path needed when traffic
// reaches ClickHouse through an HTTP proxy / Istio VirtualService on 80/443.
// Assertions are identical: DDL, typed columns, filters, labelSelector, and
// closeMode=update version chaining must work over HTTP without code changes.
func TestHistory_ClickHouseHTTP(t *testing.T) {
	runHistoryClickHouseIstioSchema(t, "e2e-history-http-0", clickhouseHTTPDSN)
}

// runHistoryClickHouseIstioSchema drives the shared history write-path scenario
// against the given ClickHouse DSN (native or HTTP). Assertions read back via
// clickhouse-client inside the pod (always native) so the reader path is
// independent of the writer protocol under test.
func runHistoryClickHouseIstioSchema(t *testing.T, ns, dsn string) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	// ClickHouse must be Ready before we point the exporter at it, otherwise the
	// exporter crashloops on store.Open and setExporterConfig's rollout wait
	// fails.
	chWaitReady(t)

	// Start each table fresh so assertions are deterministic across reruns; the
	// exporter recreates them on startup via createSchema:true.
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

	// --- Create the real resources BEFORE enabling history so the exporter's
	// initial LIST observes them all in one batch. ---

	// Deployment (replicas 0 keeps it light; only its pod-template labels matter).
	createObject(t, dyn, deployGVR, ns, map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "dep-1"},
		"spec": map[string]interface{}{
			"replicas": int64(0),
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "api"}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": "api", "tier": "web"}},
				"spec": map[string]interface{}{
					"containers": []interface{}{map[string]interface{}{"name": "c", "image": "nginx"}},
				},
			},
		},
	})

	// Service svc-lb: passes the type filter; gets a loadBalancer ingress IP.
	createObject(t, dyn, svcGVR, ns, map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "svc-lb"},
		"spec": map[string]interface{}{
			"type":     "LoadBalancer",
			"selector": map[string]interface{}{"app": "api"},
			"ports": []interface{}{
				map[string]interface{}{"port": int64(80), "name": "http", "protocol": "TCP"},
			},
		},
	})
	setServiceLBIngressIP(t, dyn, ns, "svc-lb", "203.0.113.5")

	// Service svc-clusterip: filtered OUT (type not in {LoadBalancer,NodePort}).
	createObject(t, dyn, svcGVR, ns, map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "svc-clusterip"},
		"spec": map[string]interface{}{
			"type":     "ClusterIP",
			"selector": map[string]interface{}{"app": "web"},
			"ports": []interface{}{
				map[string]interface{}{"port": int64(8080), "name": "http", "protocol": "TCP"},
			},
		},
	})

	// Gateway gw-keep: matches the istio=ingressgateway labelSelector.
	createObject(t, dyn, istioGatewayFixture.gvr(), ns, map[string]interface{}{
		"apiVersion": istioGatewayFixture.apiVersion(),
		"kind":       "Gateway",
		"metadata": map[string]interface{}{
			"namespace": ns, "name": "gw-keep",
			"labels": map[string]interface{}{"istio": "ingressgateway"},
		},
		"spec": map[string]interface{}{
			"selector": map[string]interface{}{"istio": "ingressgateway"},
			"servers": []interface{}{map[string]interface{}{
				"port":  map[string]interface{}{"number": int64(80), "name": "http", "protocol": "HTTP"},
				"hosts": []interface{}{"a.example.com", "b.example.com"},
			}},
		},
	})

	// Gateway gw-drop: lacks the label -> excluded server-side by labelSelector.
	createObject(t, dyn, istioGatewayFixture.gvr(), ns, map[string]interface{}{
		"apiVersion": istioGatewayFixture.apiVersion(),
		"kind":       "Gateway",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "gw-drop"},
		"spec": map[string]interface{}{
			"selector": map[string]interface{}{"istio": "other"},
			"servers": []interface{}{map[string]interface{}{
				"port":  map[string]interface{}{"number": int64(80), "name": "http", "protocol": "HTTP"},
				"hosts": []interface{}{"drop.example.com"},
			}},
		},
	})

	// VirtualService vs-1: bound to gw-keep.
	createObject(t, dyn, istioVSFixture.gvr(), ns, map[string]interface{}{
		"apiVersion": istioVSFixture.apiVersion(),
		"kind":       "VirtualService",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "vs-1"},
		"spec": map[string]interface{}{
			"hosts":    []interface{}{"a.example.com"},
			"gateways": []interface{}{"gw-keep"},
			"http": []interface{}{map[string]interface{}{
				"route": []interface{}{map[string]interface{}{
					"destination": map[string]interface{}{
						"host": "api",
						"port": map[string]interface{}{"number": int64(80)},
					},
				}},
			}},
		},
	})

	// Enable history -> exporter restarts, connects to ClickHouse, builds the
	// four tables, and ingests the objects above.
	setExporterConfig(t, istioHistoryConfigYAMLWithDSN(ns, dsn))

	// Wait until every positive row has landed. Once these are present the
	// initial LIST has been fully processed, so the negative assertions
	// (filtered / label-excluded objects) are stably absent.
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE name='svc-lb' AND has(ingress_ips,'203.0.113.5')", 1)
	waitForCHCount(t, "SELECT count() FROM deploy_versions WHERE name='dep-1'", 1)
	waitForCHCount(t, "SELECT count() FROM gw_versions WHERE name='gw-keep'", 1)
	waitForCHCount(t, "SELECT count() FROM vs_versions WHERE name='vs-1'", 1)

	t.Run("create_table", func(t *testing.T) {
		if got := chQuery(t, "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name IN ('service_versions','deploy_versions','gw_versions','vs_versions')"); got != "4" {
			t.Fatalf("expected 4 history tables, got %q", got)
		}
		// Envelope + declared columns present with the declared types.
		assertColumnType(t, "service_versions", "ingress_ips", "Array(String)")
		assertColumnType(t, "service_versions", "port", "Int64")
		assertColumnType(t, "service_versions", "valid_from", "DateTime64(3)")
		assertColumnType(t, "service_versions", "valid_to", "DateTime64(3)")
		assertColumnType(t, "gw_versions", "spec_json", "String")
		// Bloom skip indexes mirror the POC (idx on ingress_ips and selector_kv).
		if got := chQuery(t, "SELECT count() FROM system.data_skipping_indices WHERE table='service_versions' AND name='idx_ingress_ips'"); got != "1" {
			t.Fatalf("service_versions idx_ingress_ips missing (got %q)", got)
		}
		if got := chQuery(t, "SELECT count() FROM system.data_skipping_indices WHERE table='gw_versions' AND name='idx_selector_kv'"); got != "1" {
			t.Fatalf("gw_versions idx_selector_kv missing (got %q)", got)
		}
	})

	t.Run("types", func(t *testing.T) {
		// String + Int64 scalars.
		if got := chQuery(t, "SELECT port FROM service_versions WHERE name='svc-lb' ORDER BY valid_from DESC LIMIT 1"); got != "80" {
			t.Fatalf("service port = %q, want 80", got)
		}
		if got := chQuery(t, "SELECT port_name FROM service_versions WHERE name='svc-lb' ORDER BY valid_from DESC LIMIT 1"); got != "http" {
			t.Fatalf("service port_name = %q, want http", got)
		}
		// encode:kv -> sorted k=v Array(String).
		if got := chQuery(t, "SELECT arrayStringConcat(pod_labels_kv, ',') FROM deploy_versions WHERE name='dep-1' ORDER BY valid_from DESC LIMIT 1"); got != "app=api,tier=web" {
			t.Fatalf("deploy pod_labels_kv = %q, want app=api,tier=web", got)
		}
		if got := chQuery(t, "SELECT arrayStringConcat(selector_kv, ',') FROM gw_versions WHERE name='gw-keep' ORDER BY valid_from DESC LIMIT 1"); got != "istio=ingressgateway" {
			t.Fatalf("gw selector_kv = %q, want istio=ingressgateway", got)
		}
		// Array(String) from wildcard path.
		if got := chQuery(t, "SELECT has(server_hosts,'a.example.com') AND has(server_hosts,'b.example.com') FROM gw_versions WHERE name='gw-keep' ORDER BY valid_from DESC LIMIT 1"); got != "1" {
			t.Fatalf("gw server_hosts missing expected hosts (got %q)", got)
		}
		if got := chQuery(t, "SELECT has(bound_gateways,'gw-keep') FROM vs_versions WHERE name='vs-1' ORDER BY valid_from DESC LIMIT 1"); got != "1" {
			t.Fatalf("vs bound_gateways missing gw-keep (got %q)", got)
		}
		// encode:json -> valid JSON object.
		if got := chQuery(t, "SELECT isValidJSON(spec_json) FROM gw_versions WHERE name='gw-keep' ORDER BY valid_from DESC LIMIT 1"); got != "1" {
			t.Fatalf("gw spec_json is not valid JSON (got %q)", got)
		}
	})

	t.Run("filter", func(t *testing.T) {
		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE name='svc-lb' AND deleted=0"); got == "0" {
			t.Fatalf("svc-lb should pass the type filter, got count %q", got)
		}
		if got := chQuery(t, "SELECT count() FROM service_versions WHERE name='svc-clusterip'"); got != "0" {
			t.Fatalf("svc-clusterip should be filtered out, got count %q", got)
		}
	})

	t.Run("label_selector", func(t *testing.T) {
		if got := chQuery(t, "SELECT count() FROM gw_versions FINAL WHERE name='gw-keep' AND deleted=0"); got == "0" {
			t.Fatalf("gw-keep should pass the labelSelector, got count %q", got)
		}
		if got := chQuery(t, "SELECT count() FROM gw_versions WHERE name='gw-drop'"); got != "0" {
			t.Fatalf("gw-drop should be excluded by labelSelector, got count %q", got)
		}
	})

	// Runs last: it mutates and then deletes svc-lb, which the subtests above
	// still expect to be present and single-versioned.
	t.Run("valid_to", func(t *testing.T) {
		const svc = "name='svc-lb'"

		// A live object has exactly one open version, carrying the FarFuture
		// sentinel rather than a null.
		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to > now()"); got != "1" {
			t.Fatalf("expected 1 open version of svc-lb, got %q", got)
		}
		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to = toDateTime64('2200-01-01 00:00:00', 3)"); got != "1" {
			t.Fatalf("open version should carry the FarFuture sentinel, got count %q", got)
		}

		// A real change supersedes v1: the writer re-inserts v1 with valid_to set
		// and appends the new open version. Waiting on the closed row means the
		// batch holding both has landed.
		updateServicePortName(t, dyn, ns, "svc-lb", "http2")
		waitForCHCount(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to <= now()", 1)

		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE "+svc); got != "2" {
			t.Fatalf("expected 2 versions of svc-lb after update, got %q", got)
		}
		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to > now()"); got != "1" {
			t.Fatalf("expected exactly 1 open version after update, got %q", got)
		}
		// The closed version ends exactly where the open one begins: no gap, no overlap.
		chain := "SELECT (SELECT valid_to FROM service_versions FINAL WHERE " + svc + " AND valid_to <= now()) = " +
			"(SELECT valid_from FROM service_versions FINAL WHERE " + svc + " AND valid_to > now())"
		if got := chQuery(t, chain); got != "1" {
			t.Fatalf("v1.valid_to should equal v2.valid_from (got %q); rows:\n%s", got,
				chQuery(t, "SELECT valid_from, valid_to, port_name FROM service_versions FINAL WHERE "+svc+" ORDER BY valid_from"))
		}

		// Deleting closes the last version. Deletion is expressed purely by
		// valid_to — no deleted=1 tombstone row is ever written.
		deleteObject(t, dyn, svcGVR, ns, "svc-lb")
		waitForCHCount(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to <= now()", 2)

		if got := chQuery(t, "SELECT count() FROM service_versions FINAL WHERE "+svc+" AND valid_to > now()"); got != "0" {
			t.Fatalf("no version should remain open after delete, got %q", got)
		}
		if got := chQuery(t, "SELECT count() FROM service_versions WHERE "+svc+" AND deleted=1"); got != "0" {
			t.Fatalf("delete must not write a tombstone row, got %q", got)
		}
	})
}

// --- helpers ---

func createObject(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns string, obj map[string]interface{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitFor(ctx, 500*time.Millisecond, func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(gvr).Namespace(ns).Create(ctx, &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{})
		if err == nil || strings.Contains(err.Error(), "already exists") {
			return true, nil
		}
		// CRD API may not be registered yet immediately after Established.
		if strings.Contains(err.Error(), "could not find") || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}); err != nil {
		t.Fatalf("create %s/%s: %v", gvr.Resource, obj["metadata"].(map[string]interface{})["name"], err)
	}
}

// updateServicePortName renames the first port, changing both a declared column
// (port_name) and the spec hash, so the ingester records a genuinely new version.
func updateServicePortName(t *testing.T, dyn dynamic.Interface, ns, name, portName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := dyn.Resource(svcGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	ports, found, err := unstructured.NestedSlice(got.Object, "spec", "ports")
	if err != nil || !found || len(ports) == 0 {
		t.Fatalf("service %s has no spec.ports (found=%v): %v", name, found, err)
	}
	ports[0].(map[string]interface{})["name"] = portName
	if err := unstructured.SetNestedSlice(got.Object, ports, "spec", "ports"); err != nil {
		t.Fatalf("set spec.ports: %v", err)
	}
	if _, err := dyn.Resource(svcGVR).Namespace(ns).Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update service %s: %v", name, err)
	}
}

func deleteObject(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := dyn.Resource(gvr).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete %s/%s: %v", gvr.Resource, name, err)
	}
}

func setServiceLBIngressIP(t *testing.T, dyn dynamic.Interface, ns, name, ip string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := dyn.Resource(svcGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	if err := unstructured.SetNestedSlice(got.Object,
		[]interface{}{map[string]interface{}{"ip": ip}},
		"status", "loadBalancer", "ingress"); err != nil {
		t.Fatalf("set status.loadBalancer.ingress: %v", err)
	}
	if _, err := dyn.Resource(svcGVR).Namespace(ns).UpdateStatus(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update service %s status: %v", name, err)
	}
}

// chExec runs a ClickHouse query inside the in-cluster ClickHouse pod and
// returns TabSeparated stdout. It returns an error (not t.Fatal) so callers can
// poll while a table is still being created.
func chExec(sql string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", exporterNamespace,
		"deploy/clickhouse", "--",
		"clickhouse-client", "--query", sql, "--format", "TabSeparated")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("clickhouse query failed: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimRight(stdout.String(), "\n"), nil
}

func chQuery(t *testing.T, sql string) string {
	t.Helper()
	out, err := chExec(sql)
	if err != nil {
		t.Fatalf("chQuery %q: %v", sql, err)
	}
	return out
}

func chCount(sql string) int {
	out, err := chExec(sql)
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return -1
	}
	return n
}

func waitForCHCount(t *testing.T, sql string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 1*time.Second, func(ctx context.Context) (bool, error) {
		return chCount(sql) >= want, nil
	}); err != nil {
		t.Fatalf("timed out waiting for %q >= %d (last=%d)", sql, want, chCount(sql))
	}
}

func chWaitReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "rollout", "status",
		"-n", exporterNamespace, "deploy/clickhouse", "--timeout=150s")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("clickhouse not ready: %v: %s", err, strings.TrimSpace(string(out)))
	}
}

func assertColumnType(t *testing.T, table, column, want string) {
	t.Helper()
	got := chQuery(t, "SELECT type FROM system.columns WHERE database=currentDatabase() AND table='"+table+"' AND name='"+column+"'")
	if got != want {
		t.Fatalf("%s.%s type = %q, want %q", table, column, got, want)
	}
}
