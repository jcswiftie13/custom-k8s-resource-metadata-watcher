package collector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/example/metadata-exporter/pkg/config"
)

// drainCollector consumes whatever the Collector emits onto ch in the
// background. It is the cheapest possible "registry" that lets us measure
// Collect() in isolation, free of registry validation overhead.
func drainCollector(t testing.TB, c *Collector) {
	t.Helper()
	ch := make(chan prometheus.Metric, 4096)
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	c.Collect(ch)
	close(ch)
	<-done
}

// benchPodWithLabels builds one Pod with `nLabels` labels and `nAnnotations`
// annotations. Owner UID/Kind is fixed to a single ReplicaSet so that the
// owner chain has work to do but does not blow up cache size.
func benchPodWithLabels(name string, nLabels, nAnnotations int) *corev1.Pod {
	ctrl := true
	labels := make(map[string]string, nLabels)
	for i := 0; i < nLabels; i++ {
		labels[fmt.Sprintf("k%02d", i)] = fmt.Sprintf("v%02d", i)
	}
	annos := make(map[string]string, nAnnotations)
	for i := 0; i < nAnnotations; i++ {
		annos[fmt.Sprintf("ann.example.com/k%02d", i)] = fmt.Sprintf("v%02d", i)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "ns",
			UID:         types.UID("uid-" + name),
			Labels:      labels,
			Annotations: annos,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "rs1", UID: "uid-rs1", Controller: &ctrl},
			},
		},
	}
}

// waitForCacheReadyAny is a benchmark-friendly twin of waitForCacheReady
// that accepts any testing.TB without depending on *testing.T methods.
func waitForCacheReadyAny(_ testing.TB, col *Collector, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, kind := range col.informers.WatchedKinds() {
			items := col.informers.ListAll(kind)
			if len(items) == 0 {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// BenchmarkCollect_FixedLabelsOnly measures scrape-time cost when no
// expandLabels are configured: this is the lower bound on Collect() cost
// for N anchors.
func BenchmarkCollect_FixedLabelsOnly(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			cfg := benchFixedLabelConfig()
			objs := benchSeed(n, 0, 0)
			col, cancel := startBenchCollectorAny(b, cfg, objs...)
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drainCollector(b, col)
			}
		})
	}
}

// BenchmarkCollect_ExpandLabels measures cost as a function of N anchors and
// K dynamic keys per anchor. Cardinality grows roughly as N (one series per
// pod) but the union label set grows with K.
func BenchmarkCollect_ExpandLabels(b *testing.B) {
	cases := []struct{ pods, labels int }{
		{10, 5},
		{10, 50},
		{100, 5},
		{100, 50},
		{1000, 10},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("pods=%d_labels=%d", tc.pods, tc.labels), func(b *testing.B) {
			cfg := benchExpandLabelConfig()
			objs := benchSeed(tc.pods, tc.labels, 0)
			col, cancel := startBenchCollectorAny(b, cfg, objs...)
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drainCollector(b, col)
			}
		})
	}
}

// BenchmarkCollect_OwnerChain measures the cost of resolving Pod ->
// ReplicaSet -> Deployment for every series.
func BenchmarkCollect_OwnerChain(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("pods=%d", n), func(b *testing.B) {
			cfg := benchOwnerChainConfig()
			objs := benchSeed(n, 0, 0)
			col, cancel := startBenchCollectorAny(b, cfg, objs...)
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drainCollector(b, col)
			}
		})
	}
}

// startBenchCollectorAny is like startBenchCollector but accepts testing.TB
// so it can be reused from any benchmark. It does NOT call any
// *testing.T-only methods.
func startBenchCollectorAny(b *testing.B, cfg *config.Config, objs ...runtime.Object) (*Collector, context.CancelFunc) {
	client := fake.NewSimpleClientset(objs...)
	col, err := New(cfg, client, discardLogger(), Options{})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = col.Start(ctx) }()
	waitForCacheReadyAny(b, col, 5*time.Second)
	return col, cancel
}

// benchFixedLabelConfig: minimum-cost rule (no expandLabels, no chain).
func benchFixedLabelConfig() *config.Config {
	return &config.Config{
		MetricPrefix: "bench_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
			},
		}},
	}
}

// benchExpandLabelConfig: flatten metadata.labels into dynamic series labels.
func benchExpandLabelConfig() *config.Config {
	return &config.Config{
		MetricPrefix: "bench_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace": {Path: "metadata.namespace"},
				"pod":       {Path: "metadata.name"},
			},
			ExpandLabels: []config.ExpandLabel{
				{Path: "metadata.labels", Prefix: "label_"},
			},
		}},
	}
}

// benchOwnerChainConfig: forces the resolver to walk Pod -> RS -> Deployment.
func benchOwnerChainConfig() *config.Config {
	return &config.Config{
		MetricPrefix: "bench_",
		Rules: []config.Rule{{
			Name:   "pod_info",
			Anchor: "Pod",
			Labels: map[string]config.Extract{
				"namespace":       {Path: "metadata.namespace"},
				"pod":             {Path: "metadata.name"},
				"controller_kind": {Source: "topController", Path: "kind"},
				"controller_name": {Source: "topController", Path: "metadata.name"},
			},
		}},
	}
}

// benchSeed builds N Pods plus a single ReplicaSet/Deployment pair the Pods
// own. nLabels/nAnnotations control per-Pod metadata size.
func benchSeed(pods, nLabels, nAnnotations int) []runtime.Object {
	out := make([]runtime.Object, 0, pods+2)
	out = append(out, depObj("dep1", "ns", nil, nil))
	out = append(out, rsObj("rs1", "ns", "dep1"))
	for i := 0; i < pods; i++ {
		out = append(out, benchPodWithLabels(fmt.Sprintf("p%05d", i), nLabels, nAnnotations))
	}
	return out
}
