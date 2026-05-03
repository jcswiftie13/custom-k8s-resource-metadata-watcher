//go:build integration

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
)

// watchedKinds are the plural resource names recorded by kube-apiserver in
// `apiserver_longrunning_requests{resource=...}`. Keep in sync with
// pkg/config/config.go:allSupportedKindOrder when the default is to watch all kinds.
var watchedKinds = []string{"pods", "replicasets", "deployments", "statefulsets", "daemonsets", "nodes"}

// TestTopology_ClusterWide verifies that the exporter opens exactly one
// WATCH per kind regardless of how many namespaces exist in the cluster.
// The watchers-per-namespace property is what keeps the apiserver load
// bounded on large clusters.
func TestTopology_ClusterWide(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	nss := makeScenarioNamespaces(t, "top-cw", 5)
	createNamespaces(t, nss...)
	t.Cleanup(func() { deleteNamespaces(t, nss...) })

	// 1) Scale the exporter down so our baseline excludes its watches.
	scaleExporter(t, 0)
	waitForSteadyState(t, 5*time.Second)
	baseline := watchDeltaSnapshot(t)

	// 2) Apply cluster-wide config and scale back up.
	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	// Give the informers a moment to establish every watch.
	waitForSteadyState(t, 10*time.Second)
	after := watchDeltaSnapshot(t)

	delta := map[string]float64{}
	for _, k := range watchedKinds {
		delta[k] = after[k] - baseline[k]
	}
	t.Logf("cluster-wide watch delta: %v", delta)

	for _, k := range watchedKinds {
		if got := delta[k]; got != 1 {
			t.Errorf("cluster-wide: watch delta for resource=%q = %v, want 1", k, got)
		}
	}
	assertExporterLogContains(t, "watch mode = cluster-wide")
}

// TestTopology_KindSubset verifies that watch.resources limits which GVRs get a
// WATCH: only pods and deployments open a connection; others do not.
func TestTopology_KindSubset(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	nss := makeScenarioNamespaces(t, "top-ks", 2)
	createNamespaces(t, nss...)
	t.Cleanup(func() { deleteNamespaces(t, nss...) })

	scaleExporter(t, 0)
	waitForSteadyState(t, 5*time.Second)
	baseline := watchDeltaSnapshot(t)

	setExporterConfig(t, kindSubsetClusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)
	after := watchDeltaSnapshot(t)

	delta := map[string]float64{}
	for _, k := range watchedKinds {
		delta[k] = after[k] - baseline[k]
	}
	t.Logf("kind-subset (Pod+Deployment) watch delta: %v", delta)

	for _, k := range []string{"pods", "deployments"} {
		if got := delta[k]; got != 1 {
			t.Errorf("kind-subset: watch delta for resource=%q = %v, want 1", k, got)
		}
	}
	for _, k := range []string{"replicasets", "statefulsets", "daemonsets", "nodes"} {
		if got := delta[k]; got != 0 {
			t.Errorf("kind-subset: watch delta for resource=%q = %v, want 0", k, got)
		}
	}
	assertExporterLogContains(t, "watch mode = cluster-wide")
	// topController/ownerController rules with RS/STS/DS not watched
	assertExporterLogContains(t, "not all parent kinds are watched")
}

// TestTopology_PerNamespace verifies that watches scale as N_ns × N_kinds
// when watch.resources includes per-kind namespaces, and that namespaces
// outside the list are not observed.
func TestTopology_PerNamespace(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	watched := makeScenarioNamespaces(t, "top-pn-watch", 3)
	unwatched := makeScenarioNamespaces(t, "top-pn-skip", 2)
	all := append(append([]string{}, watched...), unwatched...)
	createNamespaces(t, all...)
	t.Cleanup(func() { deleteNamespaces(t, all...) })

	// Baseline without exporter.
	scaleExporter(t, 0)
	waitForSteadyState(t, 5*time.Second)
	baseline := watchDeltaSnapshot(t)

	// Per-namespace config + a Pod in every namespace so /metrics has data
	// to distinguish watched vs. unwatched.
	setExporterConfig(t, perNamespaceConfigYAML(watched))
	scaleExporter(t, 1)
	for _, ns := range all {
		createPausePod(t, ns, "probe", map[string]string{"role": "probe"}, nil)
	}
	waitForSteadyState(t, 10*time.Second)
	after := watchDeltaSnapshot(t)

	delta := map[string]float64{}
	for _, k := range watchedKinds {
		delta[k] = after[k] - baseline[k]
	}
	t.Logf("per-namespace (3 ns) watch delta: %v", delta)

	wantPerKind := float64(len(watched))
	for _, k := range watchedKinds {
		want := wantPerKind
		if k == "nodes" {
			want = 1
		}
		if got := delta[k]; got != want {
			t.Errorf("per-namespace: watch delta for resource=%q = %v, want %v", k, got, want)
		}
	}

	// Indirect proof that no stray cluster-wide watch is running: the Pods
	// in unwatched namespaces never show up in /metrics.
	if err := waitFor(context.Background(), 2*time.Second, func(ctx context.Context) (bool, error) {
		// This condition will be rechecked below; we just want to give the
		// exporter one extra chance to ingest before we assert.
		return true, nil
	}); err != nil {
		t.Fatalf("wait: %v", err)
	}
	mfs := scrapeExporterMetrics(t)
	if podInfoMentionsAnyNamespace(mfs, unwatched) {
		t.Errorf("per-namespace exporter exposed series for unwatched namespaces %v", unwatched)
	}
	for _, ns := range watched {
		if !podInfoMentionsNamespace(mfs, ns) {
			t.Errorf("per-namespace exporter missing series for watched namespace %q", ns)
		}
	}

	assertExporterLogContains(t, "watch mode = per-namespace")
}

// TestTopology_IdleStable verifies that with no cluster changes the
// reconcile queue drains to 0 and the apiserver sees no watch churn.
func TestTopology_IdleStable(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	// Ensure the exporter is running cluster-wide and quiet.
	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 15*time.Second)

	before := scrapeExporterMetrics(t)
	apiBefore := watchDeltaSnapshot(t)
	reconcileBefore := counterValue(before, "exporter_reconcile_total", nil)

	// Idle window.
	time.Sleep(60 * time.Second)

	after := scrapeExporterMetrics(t)
	apiAfter := watchDeltaSnapshot(t)
	reconcileAfter := counterValue(after, "exporter_reconcile_total", nil)

	// Queue must be empty on both sides of the window (it should have been
	// empty throughout, but we guard the end-of-window here).
	if depth, _ := gaugeValue(after, "exporter_reconcile_queue_depth", nil); depth != 0 {
		t.Errorf("idle: expected queue depth 0, got %v", depth)
	}

	// Reconcile counter should stay near-flat. With Node watched, kubelet-driven
	// heartbeat/condition updates can trigger a tiny amount of background reconcile.
	// Extra nodes increase baseline node churn slightly (multi-node Kind).
	nodeCount := len(listNodes(t))
	if nodeCount < 1 {
		nodeCount = 1
	}
	maxIdleReconcileDelta := 5.0 + 2.0*float64(nodeCount-1)
	if diff := reconcileAfter - reconcileBefore; diff > maxIdleReconcileDelta {
		t.Errorf("idle: reconcile_total moved by %v over idle window; expected <= %v (nodes=%d)", diff, maxIdleReconcileDelta, nodeCount)
	}

	// Watch connections should be identical across the idle window.
	for _, k := range watchedKinds {
		if apiBefore[k] != apiAfter[k] {
			t.Errorf("idle: watch count for resource=%q changed from %v to %v", k, apiBefore[k], apiAfter[k])
		}
	}
}

// ---------------------------------------------------------------------------
// helpers private to topology tests
// ---------------------------------------------------------------------------

func makeScenarioNamespaces(t *testing.T, prefix string, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("e2e-%s-%d", prefix, i))
	}
	return out
}

// watchDeltaSnapshot returns the apiserver's current WATCH counts for the
// kinds we care about.
func watchDeltaSnapshot(t *testing.T) map[string]float64 {
	t.Helper()
	mfs := scrapeAPIServerMetrics(t)
	out := map[string]float64{}
	for _, k := range watchedKinds {
		out[k] = counterValue(mfs, "apiserver_longrunning_requests", withAll(map[string]string{"verb": "WATCH"}, map[string]string{"resource": k}))
	}
	return out
}

// withAll composes multiple label-filter maps into a single matcher.
func withAll(parts ...map[string]string) labelMatcher {
	return func(got map[string]string) bool {
		for _, p := range parts {
			for k, v := range p {
				if got[k] != v {
					return false
				}
			}
		}
		return true
	}
}

// waitForSteadyState sleeps a fixed duration after a rollout or scale event.
// Using a sleep (instead of polling informer caches) is fine because these
// operations depend on the apiserver eventually settling, not on our own
// bookkeeping.
func waitForSteadyState(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}

// podInfoMentionsNamespace returns true when any `it_pod_info` series has a
// `namespace` label equal to ns.
func podInfoMentionsNamespace(mfs map[string]*dto.MetricFamily, ns string) bool {
	mf, ok := mfs["it_pod_info"]
	if !ok {
		return false
	}
	for _, m := range mf.GetMetric() {
		if labelsOf(m)["namespace"] == ns {
			return true
		}
	}
	return false
}

func podInfoMentionsAnyNamespace(mfs map[string]*dto.MetricFamily, nss []string) bool {
	for _, ns := range nss {
		if podInfoMentionsNamespace(mfs, ns) {
			return true
		}
	}
	return false
}

// assertExporterLogContains reads the current exporter Pod logs and checks
// for a substring. Used to pin startup log lines like "watch mode = ...".
func assertExporterLogContains(t *testing.T, substr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cs := mustClient(t)
	pod := exporterPodName(t)
	req := cs.CoreV1().Pods(exporterNamespace).GetLogs(pod, &corev1.PodLogOptions{
		Container: exporterContainer,
	})
	body, err := req.Do(ctx).Raw()
	if err != nil {
		t.Fatalf("fetch logs: %v", err)
	}
	if !strings.Contains(string(body), substr) {
		t.Errorf("exporter logs do not contain %q\n--- logs tail ---\n%s", substr, tailString(body, 4096))
	}
}

func tailString(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[len(b)-n:])
}
