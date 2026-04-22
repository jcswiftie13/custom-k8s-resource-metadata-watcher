//go:build integration

package e2e

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TestMain establishes the shared clientset and waits for the exporter
// installed by run.sh to become ready. If E2E_SKIP_SETUP=1 is set, we skip
// the rollout wait (useful when iterating locally against an already-running
// exporter).
func TestMain(m *testing.M) {
	cfg, err := loadKubeconfig()
	if err != nil {
		log.Fatalf("load kubeconfig: %v", err)
	}
	// Integration tests are bursty (many create/update in short windows);
	// bump the client-side rate limiter so our test driver does not throttle
	// itself when churning fixtures.
	cfg.QPS = 200
	cfg.Burst = 400

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("build clientset: %v", err)
	}

	root, err := repoRootFromEnv()
	if err != nil {
		log.Fatalf("locate repo root: %v", err)
	}

	shared.cfg = cfg
	shared.client = cs
	shared.repoRoot = root

	if os.Getenv("E2E_SKIP_SETUP") != "1" {
		if err := waitForBaselineRollout(cs, 5*time.Minute); err != nil {
			log.Fatalf("wait for baseline exporter rollout: %v", err)
		}
	}

	code := m.Run()
	// Best-effort cleanup of test-owned namespaces.
	cleanupOwnedNamespaces(cs)
	os.Exit(code)
}

// waitForBaselineRollout blocks until the exporter Deployment reports at
// least one ready replica, ensuring subsequent tests can scrape /metrics.
func waitForBaselineRollout(cs kubernetes.Interface, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return waitFor(ctx, 3*time.Second, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments(exporterNamespace).Get(ctx, exporterDeployment, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		if want == 0 {
			return true, nil
		}
		return d.Status.AvailableReplicas >= want && d.Status.UpdatedReplicas == want, nil
	})
}

// cleanupOwnedNamespaces deletes every namespace tagged with the test label.
// We do not wait for finalisation; the Kind cluster teardown in run.sh will
// take care of anything still in Terminating.
func cleanupOwnedNamespaces(cs kubernetes.Interface) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nss, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", testLabelKey, testLabelValue),
	})
	if err != nil {
		log.Printf("cleanup: list namespaces: %v", err)
		return
	}
	bg := metav1.DeletePropagationBackground
	var names []string
	for _, ns := range nss.Items {
		if err := cs.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil {
			log.Printf("cleanup: delete namespace %s: %v", ns.Name, err)
			continue
		}
		names = append(names, ns.Name)
	}
	if len(names) > 0 {
		log.Printf("cleanup: deleted %d test namespaces: %s", len(names), strings.Join(names, ","))
	}
}
