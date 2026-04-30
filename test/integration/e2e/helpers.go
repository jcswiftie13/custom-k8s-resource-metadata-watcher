//go:build integration

// Package e2e contains integration tests that run against a live Kubernetes
// cluster (typically Kind) where the metadata-exporter has already been
// deployed by test/integration/run.sh.
//
// Tests in this package are gated behind the `integration` build tag so a
// default `go test ./...` run does not attempt to contact a cluster.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	// exporterNamespace is where the exporter Deployment/Service/ConfigMap live.
	exporterNamespace = "metadata-exporter-it"

	// exporterDeployment is the canonical name used throughout manifests.
	exporterDeployment = "metadata-exporter"

	// exporterConfigMap is the ConfigMap tests rewrite to switch topology.
	exporterConfigMap = "metadata-exporter"

	// exporterContainer is the container name inside the exporter Pod.
	exporterContainer = "metadata-exporter"

	// testLabelKey marks every object created by this test suite so cleanup
	// can be done with a single LabelSelector.
	testLabelKey   = "integration.test/owner"
	testLabelValue = "metadata-exporter-e2e"
)

// shared holds process-wide test singletons established by TestMain.
var shared struct {
	cfg    *rest.Config
	client kubernetes.Interface
	repoRoot string
}

// mustClient returns the process-wide clientset, failing the test when it
// was not initialised (TestMain should always initialise it first).
func mustClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	if shared.client == nil {
		t.Fatalf("shared clientset not initialised; did TestMain run?")
	}
	return shared.client
}

// loadKubeconfig resolves the test kubeconfig in this order:
//  1. KUBECONFIG env var (explicit file path)
//  2. $HOME/.kube/config
//  3. in-cluster config (useful if the tests themselves run as a Pod)
func loadKubeconfig() (*rest.Config, error) {
	if p := os.Getenv("KUBECONFIG"); p != "" {
		return clientcmd.BuildConfigFromFlags("", p)
	}
	if home, err := os.UserHomeDir(); err == nil {
		def := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(def); err == nil {
			return clientcmd.BuildConfigFromFlags("", def)
		}
	}
	return rest.InClusterConfig()
}

// repoRootFromEnv returns the repository root. run.sh sets E2E_REPO_ROOT, but
// as a fallback we walk up from the test file until we find go.mod.
func repoRootFromEnv() (string, error) {
	if p := os.Getenv("E2E_REPO_ROOT"); p != "" {
		return p, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found starting from %s", wd)
}

// ---------------------------------------------------------------------------
// Namespace lifecycle
// ---------------------------------------------------------------------------

// createNamespaces creates a set of namespaces tagged with the test label so
// cleanupNamespaces can purge everything at the end of a test. Existing
// namespaces are ignored (idempotent).
func createNamespaces(t *testing.T, names ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	for _, name := range names {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					testLabelKey: testLabelValue,
				},
			},
		}
		_, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create namespace %s: %v", name, err)
		}
	}
}

// deleteNamespaces removes namespaces asynchronously; we do not wait for
// their finalisation because Kind's default namespace deletion is slow and
// later tests do not reuse the same names.
func deleteNamespaces(t *testing.T, names ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	for _, name := range names {
		bg := metav1.DeletePropagationBackground
		err := cs.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{
			PropagationPolicy: &bg,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Logf("delete namespace %s: %v (ignored)", name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Exporter control: config changes + rollout
// ---------------------------------------------------------------------------

// setExporterConfig replaces the exporter ConfigMap's `config.yaml` entry
// with the supplied YAML text and then restarts the exporter Pod so the new
// config takes effect. The function blocks until the rollout is complete and
// the new Pod is reporting ready.
func setExporterConfig(t *testing.T, configYAML string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := mustClient(t)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, err := cs.CoreV1().ConfigMaps(exporterNamespace).Get(ctx, exporterConfigMap, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["config.yaml"] = configYAML
		_, err = cs.CoreV1().ConfigMaps(exporterNamespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("update exporter configmap: %v", err)
	}

	restartExporter(t)
}

// scaleExporter sets the exporter Deployment replicas and blocks until the
// observed replicas match. Used by topology tests to capture an apiserver
// baseline with no exporter running.
func scaleExporter(t *testing.T, replicas int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := mustClient(t)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dep, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, exporterDeployment, metav1.GetOptions{})
		if err != nil {
			return err
		}
		dep.Spec.Replicas = &replicas
		_, err = cs.AppsV1().Deployments(exporterNamespace).Update(ctx, dep, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("scale exporter deployment: %v", err)
	}

	waitForRollout(t, ctx)
	// Wait for observed replica count to settle too (scale down has to
	// propagate through apiserver to reflect zero watches).
	if err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, exporterDeployment, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return d.Status.Replicas == replicas && d.Status.ReadyReplicas == replicas, nil
	}); err != nil {
		t.Fatalf("exporter replica count did not settle to %d: %v", replicas, err)
	}
}

// restartExporter forces a rollout restart by bumping a timestamp annotation
// on the Deployment PodTemplate. Kubernetes then rolls the pods for us.
func restartExporter(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := mustClient(t)

	restartAt := time.Now().UTC().Format(time.RFC3339Nano)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dep, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, exporterDeployment, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		dep.Spec.Template.Annotations["integration.test/restartedAt"] = restartAt
		if dep.Spec.Replicas == nil || *dep.Spec.Replicas == 0 {
			one := int32(1)
			dep.Spec.Replicas = &one
		}
		_, err = cs.AppsV1().Deployments(exporterNamespace).Update(ctx, dep, metav1.UpdateOptions{})
		return err
	}); err != nil {
		t.Fatalf("annotate exporter deployment: %v", err)
	}
	waitForRollout(t, ctx)
}

// waitForRollout polls the Deployment until UpdatedReplicas == Replicas and
// every replica is Available.
func waitForRollout(t *testing.T, ctx context.Context) {
	t.Helper()
	cs := mustClient(t)
	err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, exporterDeployment, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		if want == 0 {
			return d.Status.Replicas == 0, nil
		}
		if d.Status.UpdatedReplicas < want {
			return false, nil
		}
		if d.Status.AvailableReplicas < want {
			return false, nil
		}
		// Ensure no old ReplicaSet still has live pods.
		if d.Status.Replicas != d.Status.UpdatedReplicas {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("exporter rollout did not complete: %v", err)
	}
}

// exporterPodName returns the name of the currently-ready exporter Pod. It
// prefers Ready pods owned by the newest ReplicaSet.
func exporterPodName(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	pods, err := cs.CoreV1().Pods(exporterNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=metadata-exporter-integration",
	})
	if err != nil {
		t.Fatalf("list exporter pods: %v", err)
	}
	for _, p := range pods.Items {
		if isPodReady(&p) {
			return p.Name
		}
	}
	t.Fatalf("no ready exporter pod found (have %d pods)", len(pods.Items))
	return ""
}

func isPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// /metrics scraping
// ---------------------------------------------------------------------------

// scrapeExporterMetrics returns the parsed MetricFamilies served by the
// exporter. We reach /metrics over the API-server's pod-proxy so the tests
// do not need to run kubectl port-forward in the background.
func scrapeExporterMetrics(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pod := exporterPodName(t)
	cs := mustClient(t)
	// Use the `http:` scheme prefix + the *numeric* container port so the
	// apiserver proxies cleartext HTTP to the exporter (:8080 serves plain
	// HTTP). Using the port *name* ("metrics") here makes the apiserver's
	// pod-proxy handler fail to negotiate the upstream scheme and the
	// stream gets reset with HTTP/2 INTERNAL_ERROR.
	req := cs.CoreV1().RESTClient().
		Get().
		Namespace(exporterNamespace).
		Resource("pods").
		Name(fmt.Sprintf("http:%s:8080", pod)).
		SubResource("proxy").
		Suffix("metrics")
	body, err := req.DoRaw(ctx)
	if err != nil {
		t.Fatalf("scrape exporter /metrics via pod proxy: %v", err)
	}
	return parsePromText(t, body)
}

// scrapeAPIServerMetrics fetches kube-apiserver /metrics via `kubectl get
// --raw`. We use kubectl because client-go's discovery client does not
// expose the raw GET, and swapping to the REST client directly requires
// more plumbing than it is worth here.
func scrapeAPIServerMetrics(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	cmd := exec.Command("kubectl", "get", "--raw", "/metrics")
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		cmd.Env = append(os.Environ(), "KUBECONFIG="+kc)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl get --raw /metrics failed: %v\nstderr: %s", err, stderr.String())
	}
	return parsePromText(t, out.Bytes())
}

func parsePromText(t *testing.T, body []byte) map[string]*dto.MetricFamily {
	t.Helper()
	var parser expfmt.TextParser
	mfs, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("parse prometheus text: %v\nfirst 4KiB:\n%s", err, truncate(body, 4096))
	}
	return mfs
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "\n…(truncated)"
}

// counterValue sums the value of every series in a counter MetricFamily
// whose labels match matcher. A nil matcher matches everything.
func counterValue(mfs map[string]*dto.MetricFamily, name string, matcher labelMatcher) float64 {
	mf, ok := mfs[name]
	if !ok {
		return 0
	}
	var total float64
	for _, m := range mf.GetMetric() {
		if matcher != nil && !matcher(labelsOf(m)) {
			continue
		}
		if c := m.GetCounter(); c != nil {
			total += c.GetValue()
		}
		if g := m.GetGauge(); g != nil {
			total += g.GetValue()
		}
	}
	return total
}

// gaugeValue returns the value of the first gauge series matching the
// supplied labels, or NaN (via the bool return) when nothing matched.
func gaugeValue(mfs map[string]*dto.MetricFamily, name string, matcher labelMatcher) (float64, bool) {
	mf, ok := mfs[name]
	if !ok {
		return 0, false
	}
	for _, m := range mf.GetMetric() {
		if matcher != nil && !matcher(labelsOf(m)) {
			continue
		}
		if g := m.GetGauge(); g != nil {
			return g.GetValue(), true
		}
	}
	return 0, false
}

type labelMatcher func(map[string]string) bool

func withLabels(want map[string]string) labelMatcher {
	return func(got map[string]string) bool {
		for k, v := range want {
			if got[k] != v {
				return false
			}
		}
		return true
	}
}

// resourceIn restricts to label sets whose `resource` label is in the
// supplied set. Used for filtering apiserver_longrunning_requests by the
// kinds the exporter actually watches.
func resourceIn(vals ...string) labelMatcher {
	set := map[string]struct{}{}
	for _, v := range vals {
		set[v] = struct{}{}
	}
	return func(got map[string]string) bool {
		_, ok := set[got["resource"]]
		return ok
	}
}

func labelsOf(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// ---------------------------------------------------------------------------
// Waiting
// ---------------------------------------------------------------------------

// waitFor polls fn until it returns true, the context expires, or fn returns
// an error. interval is the polling period.
func waitFor(ctx context.Context, interval time.Duration, fn func(context.Context) (bool, error)) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	for {
		ok, err := fn(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", time.Until(deadline))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// createPausePod creates a small Pod in the given namespace with two pause
// containers. Returns the created pod (after Create, not after Ready). Tests
// should rely on /metrics scraping to determine when informers have seen it.
func createPausePod(t *testing.T, namespace, name string, labels, annotations map[string]string) *corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[testLabelKey] = testLabelValue
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: ptr(int64(0)),
			Containers: []corev1.Container{
				{Name: "pause-main", Image: "registry.k8s.io/pause:3.9"},
				{Name: "pause-sidecar", Image: "registry.k8s.io/pause:3.10"},
			},
		},
	}
	cs := mustClient(t)
	created, err := cs.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod %s/%s: %v", namespace, name, err)
	}
	return created
}

// createFixtureDeployment deploys a small Deployment with two containers and
// a controller-note annotation. Pods scheduled by the Deployment resolve
// through ReplicaSet to Deployment via topController, exercising the
// reverse parent index.
func createFixtureDeployment(t *testing.T, namespace, name string, replicas int32, controllerNote string) *appsv1.Deployment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ls := map[string]string{
		"app.kubernetes.io/name": name,
		testLabelKey:             testLabelValue,
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    ls,
			Annotations: map[string]string{
				"integration.test/controller-note": controllerNote,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: ls},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(0)),
					Containers: []corev1.Container{
						{Name: "pause-main", Image: "registry.k8s.io/pause:3.9"},
						{Name: "pause-sidecar", Image: "registry.k8s.io/pause:3.10"},
					},
				},
			},
		},
	}
	cs := mustClient(t)
	created, err := cs.AppsV1().Deployments(namespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create deployment %s/%s: %v", namespace, name, err)
	}
	return created
}

// listNodes returns current cluster nodes for integration assertions.
func listNodes(t *testing.T) []corev1.Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs := mustClient(t)
	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	return nodes.Items
}

// waitForDeploymentReady blocks until the Deployment reports ready replicas
// >= spec replicas.
func waitForDeploymentReady(t *testing.T, namespace, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cs := mustClient(t)
	err := waitFor(ctx, 2*time.Second, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		return d.Status.ReadyReplicas >= want, nil
	})
	if err != nil {
		t.Fatalf("deployment %s/%s not ready: %v", namespace, name, err)
	}
}

func ptr[T any](v T) *T { return &v }

// dumpLogs retrieves recent exporter logs — useful when diagnosing a failed
// test. Called implicitly by t.Cleanup for tests that want it.
func dumpLogs(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs := mustClient(t)
	pods, err := cs.CoreV1().Pods(exporterNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=metadata-exporter-integration",
	})
	if err != nil {
		t.Logf("dump logs: list pods: %v", err)
		return
	}
	for _, p := range pods.Items {
		req := cs.CoreV1().Pods(exporterNamespace).GetLogs(p.Name, &corev1.PodLogOptions{
			Container: exporterContainer,
			TailLines: ptr(int64(80)),
		})
		stream, err := req.Stream(ctx)
		if err != nil {
			t.Logf("dump logs: stream %s: %v", p.Name, err)
			continue
		}
		body, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Logf("dump logs: read %s: %v", p.Name, err)
			continue
		}
		t.Logf("=== exporter logs for pod %s ===\n%s", p.Name, strings.TrimRight(string(body), "\n"))
	}
}

