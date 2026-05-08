//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestCorrectness_FixtureFlow ports the original bash-based smoke test to
// Go. It creates a Deployment with two containers and asserts the expected
// series + labels; then deletes the Deployment and asserts the series
// disappear within 60 seconds. It also checks that exporter_anchor_count
// for pod_info reflects the cluster shrinking after the Deployment is
// deleted, which is our regression guard against stale series in the
// scrape-time custom collector.
func TestCorrectness_FixtureFlow(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-correctness-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })
	t.Cleanup(func() { printExporterMetricsSnapshotIfEnabled(t, t.Name(), snapMetricsCorrectnessFixtureFlow) })

	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	const replicas = int32(2)
	dep := createFixtureDeployment(t, ns, "fixture-web", replicas, "from-fixture-deployment")
	waitForDeploymentReady(t, ns, dep.Name, 2*time.Minute)
	waitForPodInfoMentions(t, ns, dep.Name, int(replicas), 60*time.Second)

	mfs := scrapeExporterMetrics(t)

	// Assert it_pod_info: one series per pod, carrying the controller.
	if got := countPodInfoSeries(mfs, ns, dep.Name); got < int(replicas) {
		t.Fatalf("it_pod_info series for controller=%s: got %d, want >= %d", dep.Name, got, replicas)
	}
	// Assert the controller annotation label is present.
	if !podInfoHasAnnotation(mfs, ns, "from-fixture-deployment", int(replicas)) {
		t.Errorf("controller_annotation_integration_test_controller_note label missing on it_pod_info in ns=%s", ns)
	}

	// Assert it_pod_container_info: one series per (pod, container) with
	// the expected image tag.
	if !containerInfoMatches(mfs, ns, "pause-main", "registry.k8s.io/pause:3.9", int(replicas)) {
		t.Errorf("expected %d it_pod_container_info series for container=pause-main with pause:3.9", replicas)
	}
	if !containerInfoMatches(mfs, ns, "pause-sidecar", "registry.k8s.io/pause:3.10", int(replicas)) {
		t.Errorf("expected %d it_pod_container_info series for container=pause-sidecar with pause:3.10", replicas)
	}

	// Capture anchor count before deletion so we can verify that scrape
	// output reflects the cluster shrinking after the Deployment dies.
	preAnchorCount, _ := gaugeValue(mfs, "exporter_anchor_count", withLabels(map[string]string{"rule": "pod_info", "kind": "Pod"}))

	// Delete the Deployment and wait for propagation.
	deleteFixtureDeployment(t, ns, dep.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		m := scrapeExporterMetrics(t)
		return countPodInfoSeries(m, ns, dep.Name) == 0, nil
	}); err != nil {
		t.Fatalf("it_pod_info series for deleted controller did not disappear: %v", err)
	}

	// Anchor count for Pods should drop (this controller's pods are gone)
	// or at worst stay the same; it must never grow strictly because of a
	// delete.
	m := scrapeExporterMetrics(t)
	postAnchorCount, _ := gaugeValue(m, "exporter_anchor_count", withLabels(map[string]string{"rule": "pod_info", "kind": "Pod"}))
	if postAnchorCount > preAnchorCount {
		t.Errorf("anchor count grew after delete: before=%v after=%v", preAnchorCount, postAnchorCount)
	}
	t.Logf("anchor_count pod_info/Pod: before=%v after=%v", preAnchorCount, postAnchorCount)
}

// TestCorrectness_ControllerAnnotationWithoutPodAnnotation verifies that the
// controller-note annotation can be exported from the top controller even
// when Pods do not carry that annotation themselves.
func TestCorrectness_ControllerAnnotationWithoutPodAnnotation(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})
	ns := "e2e-correctness-controller-note-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })
	t.Cleanup(func() { printExporterMetricsSnapshotIfEnabled(t, t.Name(), snapMetricsCorrectnessControllerAnnotation) })

	setExporterConfig(t, clusterWideConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	const replicas = int32(2)
	const controllerNote = "from-controller-only"
	dep := createFixtureDeployment(t, ns, "fixture-controller-only", replicas, controllerNote)
	waitForDeploymentReady(t, ns, dep.Name, 2*time.Minute)
	waitForPodInfoMentions(t, ns, dep.Name, int(replicas), 60*time.Second)

	// Assert Pods do not carry the controller-note annotation key.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=" + dep.Name,
	})
	if err != nil {
		t.Fatalf("list fixture pods for annotation assertion: %v", err)
	}
	if len(pods.Items) < int(replicas) {
		t.Fatalf("fixture pods not ready for annotation assertion: got %d, want >= %d", len(pods.Items), replicas)
	}
	for _, pod := range pods.Items {
		if _, ok := pod.Annotations["integration.test/controller-note"]; ok {
			t.Fatalf("pod %s/%s unexpectedly has integration.test/controller-note annotation", pod.Namespace, pod.Name)
		}
	}

	// Assert metrics still expose the controller annotation value on Pod series.
	mfs := scrapeExporterMetrics(t)
	if got := countPodInfoSeries(mfs, ns, dep.Name); got < int(replicas) {
		t.Fatalf("it_pod_info series for controller=%s: got %d, want >= %d", dep.Name, got, replicas)
	}
	if !podInfoHasAnnotation(mfs, ns, controllerNote, int(replicas)) {
		t.Fatalf("expected it_pod_info label controller_annotation_integration_test_controller_note=%q on >=%d Pod series", controllerNote, replicas)
	}
}

func TestCorrectness_PodDynamicLabelsExpanded(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	ns := "e2e-dynamic-pod-labels-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	setExporterConfig(t, dynamicMetadataConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	pod := createPausePod(t, ns, "dynamic-pod-labels",
		map[string]string{
			"app.kubernetes.io/name":      "web",
			"integration.test/extra-label": "blue",
		},
		nil,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		mfs := scrapeExporterMetrics(t)
		return metricHasExactLabels(mfs, "it_pod_dynamic_metadata", map[string]string{
			"namespace":                        ns,
			"pod":                              pod.Name,
			"label_app_kubernetes_io_name":     "web",
			"label_integration_test_extra_label": "blue",
		}), nil
	}); err != nil {
		t.Fatalf("dynamic pod label expansion did not converge: %v", err)
	}
}

func TestCorrectness_PodDynamicAnnotationsMutation(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	ns := "e2e-dynamic-pod-annotations-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	setExporterConfig(t, dynamicMetadataConfigYAML())
	scaleExporter(t, 1)
	waitForSteadyState(t, 10*time.Second)

	pod := createPausePod(t, ns, "dynamic-pod-annotations", nil, map[string]string{
		"integration.test/anno-a": "value-a",
	})

	ctxInit, cancelInit := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelInit()

	if err := waitFor(ctxInit, 2*time.Second, func(ctx context.Context) (bool, error) {
		body, err := fetchExporterMetricsRaw(t)
		if err != nil {
			// exporter rollout/restart 窗口：pod 存在但尚未 Ready，交給 waitFor 重試
			if strings.Contains(err.Error(), "no ready exporter pod found") {
				return false, nil
			}
			return false, err
		}
		mfs := parsePromText(t, body)
		return metricHasExactLabels(mfs, "it_pod_dynamic_metadata", map[string]string{
			"namespace":                     ns,
			"pod":                           pod.Name,
			"annotation_integration_test_anno_a": "value-a",
		}), nil
	}); err != nil {
		t.Fatalf("initial dynamic pod annotation did not appear: %v", err)
	}

	cs := mustClient(t)
	patchBody, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				"integration.test/anno-a": nil,
				"integration.test/anno-b": "value-b",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal patch body: %v", err)
	}
	ctxPatch, cancelPatch := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPatch()
	if _, err := cs.CoreV1().Pods(ns).Patch(ctxPatch, pod.Name, types.MergePatchType, patchBody, metav1.PatchOptions{}); err != nil {
		t.Fatalf("patch pod annotations: %v", err)
	}

	ctxMutate, cancelMutate := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelMutate()
	if err := waitFor(ctxMutate, 2*time.Second, func(ctx context.Context) (bool, error) {
		body, err := fetchExporterMetricsRaw(t)
		if err != nil {
			// exporter rollout/restart 窗口：pod 存在但尚未 Ready，交給 waitFor 重試
			if strings.Contains(err.Error(), "no ready exporter pod found") {
				return false, nil
			}
			return false, err
		}
		mfs := parsePromText(t, body)
		hasNew := metricHasExactLabels(mfs, "it_pod_dynamic_metadata", map[string]string{
			"namespace":                     ns,
			"pod":                           pod.Name,
			"annotation_integration_test_anno_b": "value-b",
		})
		hasOld := metricHasLabelValue(mfs, "it_pod_dynamic_metadata", map[string]string{
			"namespace": ns,
			"pod":       pod.Name,
		}, "annotation_integration_test_anno_a", "value-a")
		return hasNew && !hasOld, nil
	}); err != nil {
		t.Fatalf("dynamic pod annotation mutation did not converge: %v", err)
	}

	mfs := scrapeExporterMetrics(t)
	if errs := counterValue(mfs, "exporter_collect_total", withLabels(map[string]string{"result": "error"})); errs > 0 {
		t.Fatalf("collector errors observed after dynamic annotation mutation: %v", errs)
	}
}

// containerInfoMatches returns true when it_pod_container_info contains at
// least `expected` series matching the supplied container and image inside
// the given namespace.
func containerInfoMatches(mfs map[string]*dto.MetricFamily, ns, container, image string, expected int) bool {
	mf, ok := mfs["it_pod_container_info"]
	if !ok {
		return false
	}
	n := 0
	for _, m := range mf.GetMetric() {
		l := labelsOf(m)
		if l["namespace"] == ns && l["container"] == container && l["image"] == image {
			n++
		}
	}
	return n >= expected
}

func deleteFixtureDeployment(t *testing.T, ns, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cs := mustClient(t)
	bg := metav1.DeletePropagationBackground
	err := cs.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &bg})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete fixture deployment %s/%s: %v", ns, name, err)
	}
}

func metricHasExactLabels(mfs map[string]*dto.MetricFamily, metric string, want map[string]string) bool {
	mf, ok := mfs[metric]
	if !ok {
		return false
	}
	for _, m := range mf.GetMetric() {
		got := labelsOf(m)
		matched := true
		for k, v := range want {
			if got[k] != v {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func metricHasLabelValue(mfs map[string]*dto.MetricFamily, metric string, base map[string]string, key, value string) bool {
	mf, ok := mfs[metric]
	if !ok {
		return false
	}
	for _, m := range mf.GetMetric() {
		got := labelsOf(m)
		baseMatched := true
		for k, v := range base {
			if got[k] != v {
				baseMatched = false
				break
			}
		}
		if baseMatched && got[key] == value {
			return true
		}
	}
	return false
}
