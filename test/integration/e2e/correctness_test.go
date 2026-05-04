//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestCorrectness_FixtureFlow ports the original bash-based smoke test to
// Go. It creates a Deployment with two containers and asserts the expected
// series + labels; then deletes the Deployment and asserts the series
// disappear within 60 seconds. It also checks that
// exporter_parent_index_size drops after deletion, which is our regression
// guard against the reverse-index leak.
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
	// Assert the flattened annotation label is present.
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

	// Capture by_parent size before deletion so we can assert it drops.
	preDeleteByParent, _ := gaugeValue(mfs, "exporter_parent_index_size", withLabels(map[string]string{"direction": "by_parent"}))

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

	// parent index should have forgotten the Deployment's UID. We allow
	// a small slack because the exporter might still have live entries for
	// other namespaces/controllers, but the by_parent count must not
	// monotonically grow past the pre-delete snapshot.
	m := scrapeExporterMetrics(t)
	postDeleteByParent, _ := gaugeValue(m, "exporter_parent_index_size", withLabels(map[string]string{"direction": "by_parent"}))
	if postDeleteByParent > preDeleteByParent {
		t.Errorf("parent index grew after delete: before=%v after=%v", preDeleteByParent, postDeleteByParent)
	}
	t.Logf("parent_index_size by_parent: before=%v after=%v", preDeleteByParent, postDeleteByParent)
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
