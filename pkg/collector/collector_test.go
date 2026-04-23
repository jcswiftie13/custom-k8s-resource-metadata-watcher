package collector

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/sink"
)

// fakeSink records every ReplaceForAnchor call so tests can assert how many
// reconciles landed without racing against Prometheus itself.
type fakeSink struct {
	mu        sync.Mutex
	calls     int
	schemas   map[string]sink.RuleSchema
	byAnchor  map[string]map[string]map[string]string
	waitCh    chan struct{}
	waitAfter int
}

func newFakeSink() *fakeSink {
	return &fakeSink{
		schemas:  map[string]sink.RuleSchema{},
		byAnchor: map[string]map[string]map[string]string{},
	}
}

func (f *fakeSink) RegisterRule(r sink.RuleSchema) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schemas[r.Name] = r
	return nil
}

func (f *fakeSink) Upsert(_ string, _ string, _ map[string]string) {}
func (f *fakeSink) Delete(_ string, _ string)                      {}

func (f *fakeSink) ReplaceForAnchor(ruleName string, anchorKey string, seriesByKey map[string]map[string]string) {
	f.mu.Lock()
	f.calls++
	if f.byAnchor[ruleName] == nil {
		f.byAnchor[ruleName] = map[string]map[string]string{}
	}
	for k, labels := range seriesByKey {
		if k != "" {
			f.byAnchor[ruleName][k] = labels
		} else {
			f.byAnchor[ruleName][anchorKey] = labels
		}
	}
	if len(seriesByKey) == 0 {
		delete(f.byAnchor, ruleName)
	}
	ch := f.waitCh
	ready := f.waitCh != nil && f.calls >= f.waitAfter
	f.mu.Unlock()
	if ready {
		// Signal that the desired number of calls has been observed. Close
		// inside a mu-free section to avoid deadlocks.
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (f *fakeSink) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSink) WaitForCalls(n int) <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitAfter = n
	f.waitCh = make(chan struct{})
	if f.calls >= n {
		close(f.waitCh)
	}
	return f.waitCh
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func podWithOwner(name, ns, rsName string) *corev1.Pod {
	ctrl := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: rsName, UID: types.UID("uid-" + rsName), Controller: &ctrl},
			},
		},
	}
}

func rs(name, ns, depName string) *appsv1.ReplicaSet {
	ctrl := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: depName, UID: types.UID("uid-" + depName), Controller: &ctrl},
			},
		},
	}
}

func dep(name, ns string, labels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID("uid-" + name),
			Labels:    labels,
		},
	}
}

func basicConfig() *config.Config {
	return &config.Config{
		MetricPrefix: "test_",
		Rules: []config.Rule{
			{
				Name:   "pod_info",
				Anchor: "Pod",
				Labels: map[string]config.Extract{
					"namespace":       {Path: "metadata.namespace"},
					"pod":             {Path: "metadata.name"},
					"controller_name": {Source: "topController", Path: "metadata.name"},
				},
			},
		},
	}
}

func TestCollector_ParentUpdateRoutesThroughIndex(t *testing.T) {
	cfg := basicConfig()
	p := podWithOwner("p1", "ns", "rs1")
	r := rs("rs1", "ns", "dep1")
	d := dep("dep1", "ns", map[string]string{"v": "1"})

	client := fake.NewSimpleClientset(p, r, d)
	fs := newFakeSink()

	col, err := New(cfg, client, fs, discardLogger(), Options{Workers: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := fs.WaitForCalls(1)
	go func() { _ = col.Start(ctx) }()

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("initial reconcile never fired (calls=%d)", fs.Calls())
	}

	// Baseline index: one anchor (the pod) dependent on the Deployment UID.
	initialCalls := fs.Calls()
	refs, hit := col.parents.AnchorsFor(types.UID("uid-dep1"))
	if !hit || len(refs) == 0 {
		t.Fatalf("expected dep1 UID in parent index after initial reconcile (hit=%v refs=%v)", hit, refs)
	}

	// Simulate a mutation of the Deployment. We expect the parent handler
	// to hit the reverse index (not trigger a namespace-wide scan) and
	// enqueue exactly one anchor.
	want := fs.WaitForCalls(initialCalls + 1)
	d.Labels["v"] = "2"
	d.ResourceVersion = "2"
	d.Generation = 2
	if _, err := client.AppsV1().Deployments("ns").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update deployment: %v", err)
	}

	select {
	case <-want:
	case <-time.After(5 * time.Second):
		t.Fatalf("parent update did not route through index; calls=%d", fs.Calls())
	}
}

func TestCollector_DeleteAnchorForgetsIndex(t *testing.T) {
	cfg := basicConfig()
	p := podWithOwner("p1", "ns", "rs1")
	r := rs("rs1", "ns", "dep1")
	d := dep("dep1", "ns", map[string]string{"v": "1"})
	client := fake.NewSimpleClientset(p, r, d)
	fs := newFakeSink()

	col, err := New(cfg, client, fs, discardLogger(), Options{Workers: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := fs.WaitForCalls(1)
	go func() { _ = col.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("initial reconcile never fired")
	}

	// Expect at least the pod to be indexed against dep1.
	if refs, hit := col.parents.AnchorsFor("uid-dep1"); !hit || len(refs) != 1 {
		t.Fatalf("dep1 should route to the pod (hit=%v refs=%v)", hit, refs)
	}

	// Delete the pod; anchor delete handler should clear index entries.
	if err := client.CoreV1().Pods("ns").Delete(ctx, "p1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		if refs, hit := col.parents.AnchorsFor("uid-dep1"); !hit || len(refs) == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("parent index never evicted deleted anchor")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
