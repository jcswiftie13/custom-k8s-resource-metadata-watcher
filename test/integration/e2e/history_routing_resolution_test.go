//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// TestHistory_RoutingResolution closes the loop of the routing-history design
// (docs/istio-virtualservice-routing-history-design.md): the real exporter
// ingests Istio-flavored resources into the four ClickHouse version tables, a
// VirtualService change produces a second version, and the routesim range
// engine (test/integration/routesim — a copied, exporter-schema-adapted port of
// poc/route2a's bench-worst pipeline: LoadTrafficWindow -> memwindow segments ->
// in-memory 3-hop IP->Gateway -> gwresolve -> in-process istiod translate ->
// native router_check_tool) resolves host+path over a window straddling the
// change and must return each version's destination cluster.
//
// The routesim half runs as a precompiled linux test binary INSIDE the Envoy
// tools image (native router_check_tool — no docker-per-batch fallback) on the
// `kind` docker network, dialing ClickHouse through the NodePort (30900) on the
// kind control-plane node. run.sh builds the binary and exports ROUTESIM_*;
// a missing binary or tool is a hard failure, never a skip.
func TestHistory_RoutingResolution(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	chWaitReady(t)

	// Fresh tables so version counts and timestamps are deterministic; the
	// exporter recreates them on startup via createSchema:true.
	for _, tbl := range []string{"service_versions", "deploy_versions", "gw_versions", "vs_versions"} {
		if _, err := chExec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}

	ns := "e2e-routing-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	dyn := mustDynamicClient(t)
	ensureWidgetCRD(t, dyn, istioGatewayFixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, istioGatewayFixture) })
	ensureWidgetCRD(t, dyn, istioVSFixture)
	t.Cleanup(func() { deleteWidgetCRD(t, dyn, istioVSFixture) })

	const (
		reqHost   = "shop.example.com"
		reqPath   = "/api/checkout"
		ingressIP = "203.0.113.10"
	)
	backendCluster := func(name string) string {
		return fmt.Sprintf("outbound|8080||%s.%s.svc.cluster.local", name, ns)
	}

	// --- Fixtures, all created (and status-patched) BEFORE history is enabled so
	// the exporter's initial LIST observes v1 of everything in one batch. ---

	// Ingress workload: pod-template labels must be a superset of BOTH the
	// ingress Service selector (hop 2) and the Gateway selector (hop 3).
	createObject(t, dyn, deployGVR, ns, map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "ingress-dep"},
		"spec": map[string]interface{}{
			"replicas": int64(0),
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "ingress"}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": "ingress", "istio": "ingressgateway"}},
				"spec": map[string]interface{}{
					"containers": []interface{}{map[string]interface{}{"name": "c", "image": "nginx"}},
				},
			},
		},
	})

	// Ingress LB Service: hop 1 maps the request's destination IP to this
	// Service via its status.loadBalancer ingress IP (patched before ingest so
	// v1 already carries it — the spec hash retains status).
	createObject(t, dyn, svcGVR, ns, map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "ingress-lb"},
		"spec": map[string]interface{}{
			"type":     "LoadBalancer",
			"selector": map[string]interface{}{"app": "ingress"},
			"ports": []interface{}{
				map[string]interface{}{"port": int64(80), "name": "http", "protocol": "TCP"},
			},
		},
	})
	setServiceLBIngressIP(t, dyn, ns, "ingress-lb", ingressIP)

	// Backend Services: NodePort so they pass the history filter
	// (spec.type in LoadBalancer,NodePort). Both exist from the start; only the
	// VirtualService's destination changes between versions.
	for _, name := range []string{"backend-a", "backend-b"} {
		createObject(t, dyn, svcGVR, ns, map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata":   map[string]interface{}{"namespace": ns, "name": name},
			"spec": map[string]interface{}{
				"type":     "NodePort",
				"selector": map[string]interface{}{"app": name},
				"ports": []interface{}{
					map[string]interface{}{"port": int64(8080), "name": "http", "protocol": "TCP"},
				},
			},
		})
	}

	// Gateway: the metadata label passes the watch's server-side labelSelector;
	// spec.selector ⊆ the ingress Deployment's pod labels (hop 3).
	createObject(t, dyn, istioGatewayFixture.gvr(), ns, map[string]interface{}{
		"apiVersion": istioGatewayFixture.apiVersion(),
		"kind":       "Gateway",
		"metadata": map[string]interface{}{
			"namespace": ns, "name": "gw",
			"labels": map[string]interface{}{"istio": "ingressgateway"},
		},
		"spec": map[string]interface{}{
			"selector": map[string]interface{}{"istio": "ingressgateway"},
			"servers": []interface{}{map[string]interface{}{
				"port":  map[string]interface{}{"number": int64(80), "name": "http", "protocol": "HTTP"},
				"hosts": []interface{}{reqHost},
			}},
		},
	})

	// VirtualService v1 -> backend-a. The gateway ref is QUALIFIED (<ns>/gw) and
	// the destination host is the backend's full FQDN — both are identity forms
	// the reader derives joins from (bound_gateways match, ParseBackendHost).
	createObject(t, dyn, istioVSFixture.gvr(), ns, map[string]interface{}{
		"apiVersion": istioVSFixture.apiVersion(),
		"kind":       "VirtualService",
		"metadata":   map[string]interface{}{"namespace": ns, "name": "vs-shop"},
		"spec": map[string]interface{}{
			"hosts":    []interface{}{reqHost},
			"gateways": []interface{}{ns + "/gw"},
			"http": []interface{}{map[string]interface{}{
				"match": []interface{}{map[string]interface{}{
					"uri": map[string]interface{}{"prefix": "/api"},
				}},
				"route": []interface{}{map[string]interface{}{
					"destination": map[string]interface{}{
						"host": fmt.Sprintf("backend-a.%s.svc.cluster.local", ns),
						"port": map[string]interface{}{"number": int64(8080)},
					},
				}},
			}},
		},
	})

	// Enable history -> the exporter builds the four tables and ingests v1 of
	// everything from its initial LIST.
	setExporterConfig(t, istioHistoryConfigYAML(ns))

	waitForCHCount(t, "SELECT count() FROM service_versions WHERE name='ingress-lb' AND has(ingress_ips,'"+ingressIP+"')", 1)
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE name='backend-a'", 1)
	waitForCHCount(t, "SELECT count() FROM service_versions WHERE name='backend-b'", 1)
	waitForCHCount(t, "SELECT count() FROM deploy_versions WHERE name='ingress-dep'", 1)
	waitForCHCount(t, "SELECT count() FROM gw_versions WHERE name='gw'", 1)
	waitForCHCount(t, "SELECT count() FROM vs_versions WHERE name='vs-shop'", 1)

	// --- Version 2: repoint the VS at backend-b. The writer closes v1 (rewrites
	// it with valid_to = v2.valid_from) and appends the new open version. ---
	updateVSDestinationHost(t, dyn, ns, "vs-shop", fmt.Sprintf("backend-b.%s.svc.cluster.local", ns))
	waitForCHCount(t, "SELECT count() FROM vs_versions FINAL WHERE name='vs-shop' AND valid_to <= now()", 1)

	// --- Window bounds in unix milliseconds (exact at DateTime64(3) precision).
	// boundary = v2's valid_from; t0 = the latest v1 valid_from across all
	// fixture rows, so every resource is live at t0 and the window's first
	// segment is fully resolvable; t1 = far in the future but before the
	// FarFuture sentinel (2200-01-01). ---
	boundaryMS := chQueryInt64(t, "SELECT toUnixTimestamp64Milli(valid_from) FROM vs_versions FINAL WHERE name='vs-shop' AND valid_to > now()")
	t0MS := chQueryInt64(t,
		"SELECT toUnixTimestamp64Milli(max(vf)) FROM ("+
			"SELECT valid_from AS vf FROM service_versions FINAL"+
			" UNION ALL SELECT valid_from FROM deploy_versions FINAL"+
			" UNION ALL SELECT valid_from FROM gw_versions FINAL"+
			" UNION ALL SELECT valid_from FROM vs_versions FINAL"+
			") WHERE toUnixTimestamp64Milli(vf) < "+strconv.FormatInt(boundaryMS, 10))
	const t1MS = int64(4102444800000) // 2100-01-01T00:00:00Z
	if boundaryMS <= t0MS {
		t.Fatalf("VS version boundary (%d) must be after the latest v1 valid_from (%d)", boundaryMS, t0MS)
	}

	expect := []map[string]interface{}{
		{"gateway": "gw", "cluster": backendCluster("backend-a"), "from_ms": t0MS, "to_ms": boundaryMS},
		{"gateway": "gw", "cluster": backendCluster("backend-b"), "from_ms": boundaryMS, "to_ms": t1MS},
	}
	expectJSON, err := json.Marshal(expect)
	if err != nil {
		t.Fatalf("marshal expectations: %v", err)
	}

	runRoutesim(t, map[string]string{
		"ROUTESIM_HOST":   reqHost,
		"ROUTESIM_PATH":   reqPath,
		"ROUTESIM_IP":     ingressIP,
		"ROUTESIM_PORT":   "80",
		"ROUTESIM_T0_MS":  strconv.FormatInt(t0MS, 10),
		"ROUTESIM_T1_MS":  strconv.FormatInt(t1MS, 10),
		"ROUTESIM_EXPECT": string(expectJSON),
	})

	// --- closeMode=update invariants + restart idempotency ---
	// Under update-close every version is exactly one PHYSICAL row (raw count,
	// no FINAL): vs-shop = v1 (patched closed) + v2 (open). Rewrite mode would
	// leave up to 3 rows here until background merges collapse the closing pair.
	t.Run("update_close_restart_idempotency", func(t *testing.T) {
		assertRowShape := func(stage string) {
			t.Helper()
			if got := chQuery(t, "SELECT count() FROM vs_versions WHERE name='vs-shop'"); got != "2" {
				t.Fatalf("[%s] vs-shop physical rows = %s, want 2 (one row per version)", stage, got)
			}
			if got := chQuery(t, "SELECT count() FROM vs_versions WHERE name='vs-shop' AND valid_to > now()"); got != "1" {
				t.Fatalf("[%s] vs-shop open rows = %s, want exactly 1", stage, got)
			}
			if got := chQuery(t, "SELECT count() FROM service_versions WHERE name='ingress-lb'"); got != "1" {
				t.Fatalf("[%s] ingress-lb physical rows = %s, want 1", stage, got)
			}
			if got := chQuery(t, "SELECT count() FROM gw_versions WHERE name='gw'"); got != "1" {
				t.Fatalf("[%s] gw physical rows = %s, want 1", stage, got)
			}
		}
		assertRowShape("pre-restart")

		// Restart the exporter: Recover() must rebuild last-state from the store
		// so the initial re-LIST dedups against what was already written instead
		// of re-inserting a row per live object.
		restartExporter(t)
		time.Sleep(10 * time.Second) // initial LIST + flush interval settle
		assertRowShape("post-restart")

		// A change AFTER the restart must close the RECOVERED open version in
		// place (the close op pins the valid_from recovered from the store) and
		// open v3 — still one row per version, contiguous chain.
		updateVSDestinationHost(t, dyn, ns, "vs-shop", fmt.Sprintf("backend-a.%s.svc.cluster.local", ns))
		waitForCHCount(t, "SELECT count() FROM vs_versions WHERE name='vs-shop'", 3)
		// The flush cycle sends the v3 INSERT before executing v2's close, so
		// wait for the close to land (2 closed versions) instead of asserting
		// the instant the row count reaches 3.
		waitForCHCount(t, "SELECT count() FROM vs_versions WHERE name='vs-shop' AND valid_to <= now()", 2)
		if got := chQuery(t, "SELECT count() FROM vs_versions WHERE name='vs-shop' AND valid_to > now()"); got != "1" {
			t.Fatalf("post-restart update: open rows = %s, want exactly 1", got)
		}
		chain := "SELECT (SELECT valid_to FROM vs_versions WHERE name='vs-shop' AND valid_to <= now() ORDER BY valid_from DESC LIMIT 1) = " +
			"(SELECT valid_from FROM vs_versions WHERE name='vs-shop' AND valid_to > now())"
		if got := chQuery(t, chain); got != "1" {
			t.Fatalf("v2.valid_to must equal v3.valid_from (got %q); rows:\n%s", got,
				chQuery(t, "SELECT valid_from, valid_to FROM vs_versions WHERE name='vs-shop' ORDER BY valid_from"))
		}
	})
}

// runRoutesim executes the precompiled routesim test binary inside the Envoy
// tools image (which carries the native router_check_tool) on the kind docker
// network, so router_check_tool runs natively even when the host is macOS.
// Everything it needs comes in as ROUTESIM_* env vars; a non-zero exit fails
// the calling test. The binary and image are prepared by run.sh (or
// `make routesim-build`) — their absence is an environment error, not a skip.
func runRoutesim(t *testing.T, params map[string]string) {
	t.Helper()

	bin := os.Getenv("ROUTESIM_TEST_BIN")
	if bin == "" {
		t.Fatal("ROUTESIM_TEST_BIN not set; run via test/integration/run.sh, or build with `make routesim-build` and export ROUTESIM_TEST_BIN")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("routesim test binary %s: %v (build with `make routesim-build`)", bin, err)
	}
	image := envOrDefault("ROUTESIM_IMAGE", "envoyproxy/envoy:tools-v1.34-latest")
	node := envOrDefault("E2E_KIND_CONTROL_PLANE", "metadata-exporter-it-control-plane")
	chAddr := envOrDefault("ROUTESIM_CH_ADDR", node+":30900")

	args := []string{"run", "--rm", "--network", "kind", "--user", "0:0"}
	if platform := os.Getenv("ROUTESIM_PLATFORM"); platform != "" {
		args = append(args, "--platform", platform)
	}
	args = append(args,
		"-e", "TMPDIR=/tmp", "-e", "HOME=/tmp",
		"-v", bin+":/routesim/routesim.test:ro",
		"-e", "ROUTESIM_CH_ADDR="+chAddr,
		"-e", "ROUTESIM_ROUTERCHECK_BIN=/usr/local/bin/router_check_tool",
		// The exporter config (istioHistoryConfigYAML) runs closeMode=update, so
		// the reader may prune closed versions in SQL and must find zero
		// duplicates for its dedup safety net to collapse.
		"-e", "ROUTESIM_UNIQUE_ROWS=1",
	)
	for k, v := range params {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, "--entrypoint", "/routesim/routesim.test", image,
		"-test.run", "TestResolveRoutingHistory", "-test.v")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	t.Logf("routesim output:\n%s", out)
	if err != nil {
		t.Fatalf("routesim failed: %v", err)
	}
}

// updateVSDestinationHost repoints the first route of the first HTTP rule at a
// new destination host — a genuine spec change, so the ingester records a new
// version.
func updateVSDestinationHost(t *testing.T, dyn dynamic.Interface, ns, name, host string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := dyn.Resource(istioVSFixture.gvr()).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get virtualservice %s: %v", name, err)
	}
	http, found, err := unstructured.NestedSlice(got.Object, "spec", "http")
	if err != nil || !found || len(http) == 0 {
		t.Fatalf("virtualservice %s has no spec.http (found=%v): %v", name, found, err)
	}
	route := http[0].(map[string]interface{})["route"].([]interface{})
	route[0].(map[string]interface{})["destination"].(map[string]interface{})["host"] = host
	if err := unstructured.SetNestedSlice(got.Object, http, "spec", "http"); err != nil {
		t.Fatalf("set spec.http: %v", err)
	}
	if _, err := dyn.Resource(istioVSFixture.gvr()).Namespace(ns).Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update virtualservice %s: %v", name, err)
	}
}

// chQueryInt64 runs a ClickHouse query expected to return one integer.
func chQueryInt64(t *testing.T, sql string) int64 {
	t.Helper()
	out := chQuery(t, sql)
	n, err := strconv.ParseInt(out, 10, 64)
	if err != nil {
		t.Fatalf("query %q returned non-integer %q: %v", sql, out, err)
	}
	return n
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
