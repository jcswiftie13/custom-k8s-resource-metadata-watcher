//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestBurden_BurstDedup fires a storm of metadata patches at a Deployment
// and its Pods. The workqueue + update filter should collapse the storm
// into a dramatically smaller number of reconcile invocations, and the
// exporter must not issue any writes against the apiserver.
func TestBurden_BurstDedup(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-burst-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	// Cluster-wide topology keeps the test insensitive to namespace count.
	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	const replicas = int32(3)
	dep := createFixtureDeployment(t, ns, "burst-fixture", replicas, "initial-note")
	waitForDeploymentReady(t, ns, dep.Name, 2*time.Minute)

	// Let the exporter observe the fixture at least once so its parent
	// index has a hot entry to exercise.
	waitForPodInfoMentions(t, ns, dep.Name, int(replicas), 60*time.Second)

	before := scrapeExporterMetrics(t)
	apiBefore := countClientGoWrites(before)

	// Load generator: 200 deployment label patches + 50 annotation patches
	// per pod, run concurrently over ~30 seconds.
	const (
		depPatches = 200
		podPatches = 50
	)
	t.Logf("starting churn: %d deployment patches + %d per-pod patches (x%d pods)",
		depPatches, podPatches, replicas)
	start := time.Now()
	churnFixture(t, ns, dep.Name, depPatches, podPatches)
	t.Logf("churn finished in %s", time.Since(start))

	// Give the exporter time to drain; the workqueue should return to 0.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		depth, _ := gaugeValue(mfs, "exporter_reconcile_queue_depth", nil)
		return depth == 0, nil
	}); err != nil {
		t.Fatalf("queue did not drain after churn: %v", err)
	}

	after := scrapeExporterMetrics(t)
	apiAfter := countClientGoWrites(after)

	reconcileBefore := counterValue(before, "exporter_reconcile_total", nil)
	reconcileAfter := counterValue(after, "exporter_reconcile_total", nil)
	reconcileDelta := reconcileAfter - reconcileBefore

	hitsBefore := counterValue(before, "exporter_parent_index_hit_total", nil)
	hitsAfter := counterValue(after, "exporter_parent_index_hit_total", nil)
	fallbackBefore := counterValue(before, "exporter_parent_index_fallback_total", nil)
	fallbackAfter := counterValue(after, "exporter_parent_index_fallback_total", nil)

	// The per-rule enqueue upper-bound is the real regression signal: every
	// Deployment patch fans out through the parent index to each owned Pod
	// (replicas enqueues per rule), and every Pod patch enqueues its own
	// key once per rule. If the workqueue+update-filter pipeline is
	// healthy, reconciles per rule should stay well below this ceiling;
	// if it regresses (e.g. dedup is bypassed), reconciles approach the
	// ceiling 1:1. We budget 45% which tolerates the bursty-but-fast
	// reconcile path observed in Kind (~20-35% in practice) without being
	// so loose that a full regression would still pass.
	enqueuesPerRule := float64(int(replicas)*depPatches + int(replicas)*podPatches)
	const dedupBudget = 0.45
	// Per-rule counter keyed by reconcileTotal's "rule" label; we compare
	// the max rule against the budget because all rules share the same
	// workqueue and therefore the same dedup ratio.
	maxRuleDelta := maxReconcilePerRuleDelta(before, after)
	t.Logf("enqueuesPerRule=%.0f totalReconcileDelta=%.0f maxRuleDelta=%.0f parentHits=%.0f parentFallbacks=%.0f",
		enqueuesPerRule, reconcileDelta, maxRuleDelta, hitsAfter-hitsBefore, fallbackAfter-fallbackBefore)

	if budget := enqueuesPerRule * dedupBudget; maxRuleDelta > budget {
		t.Errorf("reconcile storm not deduplicated enough: maxRuleDelta=%.0f > %.0f (%.0f%% of %v enqueues/rule)",
			maxRuleDelta, budget, dedupBudget*100, enqueuesPerRule)
	}

	if hitsAfter <= hitsBefore {
		t.Errorf("parent index never hit: before=%.0f after=%.0f (reverse index not exercised)",
			hitsBefore, hitsAfter)
	}
	// Fallbacks should only happen during cold start; we tolerate a small
	// amount of background fallback from unrelated objects but not a spike
	// correlated with the churn.
	if fallbackAfter-fallbackBefore > 5 {
		t.Errorf("parent index fell back %.0f times during churn; expected cold path to stay quiet",
			fallbackAfter-fallbackBefore)
	}

	// Read-only regression guard: no writes should have left the exporter.
	if apiAfter.writeVerbs > apiBefore.writeVerbs {
		t.Errorf("exporter issued %v write-verb requests during churn; should be read-only",
			apiAfter.writeVerbs-apiBefore.writeVerbs)
	}
}

// TestBurden_ParentEventViaIndex patches a Deployment annotation and
// confirms the change reaches every Pod series within a short deadline, and
// that it routed through the reverse index (not the namespace rescan).
func TestBurden_ParentEventViaIndex(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-parent-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	const replicas = int32(3)
	dep := createFixtureDeployment(t, ns, "parent-fixture", replicas, "v1")
	waitForDeploymentReady(t, ns, dep.Name, 2*time.Minute)
	waitForPodInfoMentions(t, ns, dep.Name, int(replicas), 60*time.Second)

	before := scrapeExporterMetrics(t)
	hitsBefore := counterValue(before, "exporter_parent_index_hit_total", nil)
	fallbackBefore := counterValue(before, "exporter_parent_index_fallback_total", nil)

	patchDeploymentControllerNote(t, ns, dep.Name, "v2")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := waitFor(ctx, 1*time.Second, func(ctx context.Context) (bool, error) {
		return podInfoHasAnnotation(scrapeExporterMetrics(t), ns, "v2", int(replicas)), nil
	}); err != nil {
		t.Fatalf("annotation did not propagate to all %d Pod series within 15s", replicas)
	}

	after := scrapeExporterMetrics(t)
	hitsAfter := counterValue(after, "exporter_parent_index_hit_total", nil)
	fallbackAfter := counterValue(after, "exporter_parent_index_fallback_total", nil)

	if hitsAfter-hitsBefore < 1 {
		t.Errorf("parent index did not register a hit (before=%.0f after=%.0f)", hitsBefore, hitsAfter)
	}
	if fallbackAfter-fallbackBefore > 0 {
		t.Errorf("parent event fell back to namespace rescan; reverse index should have handled it")
	}
}

// ---------------------------------------------------------------------------
// burden helpers
// ---------------------------------------------------------------------------

// churnFixture performs the label/annotation patches in parallel. Errors are
// logged but not fatal — the exporter's behaviour should be robust against
// transient apiserver conflicts.
func churnFixture(t *testing.T, ns, deployName string, depPatches, podPatches int) {
	t.Helper()
	cs := mustClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pods := listFixturePods(t, ns, deployName)
	if len(pods) == 0 {
		t.Fatalf("no fixture pods found in ns=%s for deploy=%s", ns, deployName)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < depPatches; i++ {
			patch := fmt.Sprintf(`{"metadata":{"labels":{"tick":"%d"}}}`, i)
			_, err := cs.AppsV1().Deployments(ns).Patch(ctx, deployName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
			if err != nil && !apierrors.IsConflict(err) {
				t.Logf("deployment patch %d: %v", i, err)
			}
		}
	}()
	for _, pod := range pods {
		pod := pod
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < podPatches; i++ {
				patch := fmt.Sprintf(`{"metadata":{"annotations":{"tick":"%d"}}}`, i)
				_, err := cs.CoreV1().Pods(ns).Patch(ctx, pod, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
				if err != nil && !apierrors.IsConflict(err) {
					t.Logf("pod %s patch %d: %v", pod, i, err)
				}
			}
		}()
	}
	wg.Wait()
}

// listFixturePods returns the names of Pods currently owned by the given
// Deployment, resolved via the `app.kubernetes.io/name` label.
func listFixturePods(t *testing.T, ns, deployName string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app.kubernetes.io/name=%s", deployName),
	})
	if err != nil {
		t.Fatalf("list fixture pods: %v", err)
	}
	names := make([]string, 0, len(pods.Items))
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	return names
}

// patchDeploymentControllerNote patches the controller-note annotation on a
// fixture Deployment. The collector reads this via the `top` relation and
// emits it as a label on `it_pod_info`.
func patchDeploymentControllerNote(t *testing.T, ns, name, note string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	body, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				"integration.test/controller-note": note,
			},
		},
	})
	_, err := cs.AppsV1().Deployments(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("patch deployment %s: %v", name, err)
	}
}

// waitForPodInfoMentions blocks until `it_pod_info` contains at least count
// series from the given namespace and controller, giving the exporter time
// to ingest a freshly-created fixture.
func waitForPodInfoMentions(t *testing.T, ns, controller string, count int, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := waitFor(ctx, 1*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return countPodInfoSeries(mfs, ns, controller) >= count, nil
	})
	if err != nil {
		t.Fatalf("%d it_pod_info series for controller=%s not observed within %s", count, controller, timeout)
	}
}

// countPodInfoSeries counts `it_pod_info` series matching the given ns and
// controller name.
func countPodInfoSeries(mfs map[string]*dto.MetricFamily, ns, controller string) int {
	mf, ok := mfs["it_pod_info"]
	if !ok {
		return 0
	}
	n := 0
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["namespace"] == ns && l["controller_name"] == controller {
			n++
		}
	}
	return n
}

// podInfoHasAnnotation returns true when every Pod series (>= expected) has
// the controller_annotation_integration_test_controller_note label equal to
// want.
func podInfoHasAnnotation(mfs map[string]*dto.MetricFamily, ns, want string, expected int) bool {
	mf, ok := mfs["it_pod_info"]
	if !ok {
		return false
	}
	const label = "controller_annotation_integration_test_controller_note"
	matched := 0
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["namespace"] != ns {
			continue
		}
		if l[label] != want {
			return false
		}
		matched++
	}
	return matched >= expected
}

// maxReconcilePerRuleDelta returns the largest reconcile_total delta across
// all rules. Buckets with result="error" are included because they should
// still be bounded by dedup.
func maxReconcilePerRuleDelta(before, after map[string]*dto.MetricFamily) float64 {
	sum := func(mfs map[string]*dto.MetricFamily) map[string]float64 {
		out := map[string]float64{}
		mf, ok := mfs["exporter_reconcile_total"]
		if !ok {
			return out
		}
		for _, m := range mf.GetMetric() {
			rule := labelsOf(m)["rule"]
			if c := m.GetCounter(); c != nil {
				out[rule] += c.GetValue()
			}
		}
		return out
	}
	a := sum(after)
	b := sum(before)
	var max float64
	for rule, v := range a {
		diff := v - b[rule]
		if diff > max {
			max = diff
		}
	}
	return max
}

// writeVerbCounts is the projection of rest_client_requests_total we care
// about: the sum of all requests issued with write verbs.
type writeVerbCounts struct {
	writeVerbs float64
}

func countClientGoWrites(mfs map[string]*dto.MetricFamily) writeVerbCounts {
	mf, ok := mfs["rest_client_requests_total"]
	if !ok {
		return writeVerbCounts{}
	}
	out := writeVerbCounts{}
	write := map[string]struct{}{"POST": {}, "PUT": {}, "PATCH": {}, "DELETE": {}}
	for _, m := range mf.GetMetric() {
		method := labelsOf(m)["method"]
		if _, isWrite := write[strings.ToUpper(method)]; !isWrite {
			continue
		}
		if c := m.GetCounter(); c != nil {
			out.writeVerbs += c.GetValue()
		}
	}
	return out
}
