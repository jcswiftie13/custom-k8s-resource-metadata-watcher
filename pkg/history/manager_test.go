package history

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

// scriptedStore wraps fakeStore with fault injection and call tracing for
// manager lifecycle tests.
type scriptedStore struct {
	fakeStore
	mu        sync.Mutex
	ensureErr error // consumed by the next EnsureSchema call
	pingErr   error
	closed    bool
	trace     *[]string // shared op log, see traced()
}

func (s *scriptedStore) EnsureSchema(ctx context.Context, ts []store.TableSchema) error {
	s.mu.Lock()
	err := s.ensureErr
	s.ensureErr = nil
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.fakeStore.EnsureSchema(ctx, ts)
}

func (s *scriptedStore) OpenVersions(ctx context.Context, table string) ([]store.OpenVersion, error) {
	s.traced("recover:" + table)
	return s.fakeStore.OpenVersions(ctx, table)
}

// WriteBatch/CloseVersion take the lock: the ingester flush goroutine records
// into fakeStore while the test asserts on it.
func (s *scriptedStore) WriteBatch(ctx context.Context, table string, rows []store.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeStore.WriteBatch(ctx, table, rows)
}

func (s *scriptedStore) CloseVersion(ctx context.Context, table, namespace, name, uid string, validFrom, closeAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeStore.CloseVersion(ctx, table, namespace, name, uid, validFrom, closeAt)
}

func (s *scriptedStore) Ping(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pingErr
}

func (s *scriptedStore) setPingErr(err error) {
	s.mu.Lock()
	s.pingErr = err
	s.mu.Unlock()
}

func (s *scriptedStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *scriptedStore) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *scriptedStore) traced(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace != nil {
		*s.trace = append(*s.trace, op)
	}
}

// fakeReg is an always-synced handler registration.
type fakeReg struct{}

func (fakeReg) HasSynced() bool { return true }

// fakeInformer implements the two SharedIndexInformer methods the ingester
// touches (AddEventHandler, GetStore); everything else panics via the nil
// embedded interface.
type fakeInformer struct {
	cache.SharedIndexInformer
	objs       cache.Store
	mu         sync.Mutex
	trace      *[]string
	registered int
	removed    int
}

func newFakeInformer(trace *[]string, objs ...interface{}) *fakeInformer {
	st := cache.NewStore(cache.MetaNamespaceKeyFunc)
	for _, o := range objs {
		_ = st.Add(o)
	}
	return &fakeInformer{objs: st, trace: trace}
}

func (f *fakeInformer) AddEventHandler(_ cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered++
	if f.trace != nil {
		*f.trace = append(*f.trace, "register")
	}
	return fakeReg{}, nil
}

func (f *fakeInformer) RemoveEventHandler(_ cache.ResourceEventHandlerRegistration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed++
	if f.trace != nil {
		*f.trace = append(*f.trace, "unregister")
	}
	return nil
}

func (f *fakeInformer) counts() (registered, removed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registered, f.removed
}

func (f *fakeInformer) GetStore() cache.Store { return f.objs }

// fakeInformerSource maps every kind to the same informer.
type fakeInformerSource struct{ inf *fakeInformer }

func (f fakeInformerSource) Informers(_ string) []cache.SharedIndexInformer {
	if f.inf == nil {
		return nil
	}
	return []cache.SharedIndexInformer{f.inf}
}

// newTestManager wires a Manager whose openStore/sleep are scripted. opens is
// consumed one entry per connection attempt (nil entry = success with st).
func newTestManager(t *testing.T, st store.Store, openErrs []error, o ManagerOptions) (*Manager, *[]time.Duration) {
	t.Helper()
	var (
		mu     sync.Mutex
		sleeps []time.Duration
		i      int
	)
	m := NewManager(o, slog.Default())
	m.openStore = func(_ context.Context, _ store.Options) (store.Store, error) {
		mu.Lock()
		defer mu.Unlock()
		if i < len(openErrs) && openErrs[i] != nil {
			err := openErrs[i]
			i++
			return nil, err
		}
		i++
		return st, nil
	}
	m.sleep = func(ctx context.Context, d time.Duration) bool {
		mu.Lock()
		sleeps = append(sleeps, d)
		mu.Unlock()
		return ctx.Err() == nil
	}
	m.pingInterval = 10 * time.Millisecond
	return m, &sleeps
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// approxIn asserts d is within ±20% jitter of want.
func approxIn(t *testing.T, d, want time.Duration) {
	t.Helper()
	lo := time.Duration(float64(want) * 0.79)
	hi := time.Duration(float64(want) * 1.21)
	if d < lo || d > hi {
		t.Fatalf("backoff %v outside jitter range of %v", d, want)
	}
}

func TestManager_ConnectRetriesWithBackoffThenRuns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := &scriptedStore{}
	boom := errors.New("connection refused")
	m, sleeps := newTestManager(t, st, []error{boom, boom, boom, nil}, ManagerOptions{
		Reconnect: config.ReconnectConfig{InitialBackoffMs: 10, MaxBackoffMs: 25},
	})

	if m.Connected() {
		t.Fatal("must not report connected before Start")
	}
	m.Start(ctx)
	waitFor(t, "running state", m.Connected)

	if got := m.Status(); got.State != StateRunning || got.LastError != "" {
		t.Fatalf("status after connect = %+v", got)
	}
	if len(*sleeps) != 3 {
		t.Fatalf("expected 3 backoff sleeps, got %v", *sleeps)
	}
	// 10ms → 20ms → capped at 25ms, each ±20% jitter.
	approxIn(t, (*sleeps)[0], 10*time.Millisecond)
	approxIn(t, (*sleeps)[1], 20*time.Millisecond)
	approxIn(t, (*sleeps)[2], 25*time.Millisecond)
}

func TestManager_InitFailureClosesStoreAndRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := &scriptedStore{ensureErr: errors.New("schema drift")}
	m, sleeps := newTestManager(t, st, nil, ManagerOptions{})

	m.Start(ctx)
	waitFor(t, "recovery after init failure", m.Connected)

	if !st.wasClosed() {
		t.Fatal("store from the failed init attempt must be closed")
	}
	if len(*sleeps) != 1 {
		t.Fatalf("expected 1 backoff sleep between init attempts, got %v", *sleeps)
	}
}

func TestManager_CancelDuringBackoffStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	m := NewManager(ManagerOptions{}, slog.Default())
	m.openStore = func(_ context.Context, _ store.Options) (store.Store, error) {
		attempts++
		return nil, errors.New("down")
	}
	done := make(chan struct{})
	m.sleep = func(_ context.Context, _ time.Duration) bool {
		cancel() // user shuts down while we wait out the backoff
		return false
	}
	go func() { m.run(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancelled backoff sleep")
	}
	if attempts != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", attempts)
	}
	if m.Connected() {
		t.Fatal("must not report connected after shutdown")
	}
}

func TestManager_PingFailureDegradesAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := &scriptedStore{}
	m, _ := newTestManager(t, st, nil, ManagerOptions{})
	m.Start(ctx)
	waitFor(t, "running state", m.Connected)

	st.setPingErr(errors.New("socket closed"))
	waitFor(t, "degraded state", func() bool { return m.Status().State == StateDegraded })
	if m.Connected() {
		t.Fatal("degraded must not report connected")
	}
	if m.Status().LastError == "" {
		t.Fatal("degraded status must carry the ping error")
	}

	st.setPingErr(nil)
	waitFor(t, "recovered state", m.Connected)
	if got := m.Status(); got.LastError != "" {
		t.Fatalf("recovered status still carries error: %+v", got)
	}
}

// The bring-up order is a correctness contract: Recover must build the dedup
// baseline before Register lets any event through, and ReconcileOpens must
// close rows for objects that vanished while the exporter was away.
func TestManager_InitializeOrderAndReconcile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var trace []string
	vf := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	// The store believes two objects are open; only uid-live still exists.
	st := &scriptedStore{
		fakeStore: fakeStore{open: map[string][]store.OpenVersion{
			"svc_versions": {
				{Namespace: "prod-web", Name: "api", UID: "uid-live", Values: map[string]any{"cluster_ip": "10.0.0.1"}, ValidFrom: vf, IngestSeq: 1},
				{Namespace: "prod-web", Name: "gone", UID: "uid-gone", Values: map[string]any{"cluster_ip": "10.0.0.9"}, ValidFrom: vf, IngestSeq: 2},
			},
		}},
		trace: &trace,
	}
	live := svcObj("uid-live", "prod-web", "10.0.0.1", "1")
	live.SetName("api")
	inf := newFakeInformer(&trace, live)

	cr := mustCompile(t, config.HistoryResource{
		Kind:  "Service",
		Table: "svc_versions",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
		},
	})
	m, _ := newTestManager(t, st, nil, ManagerOptions{
		Compiled:  []*CompiledResource{cr},
		Informers: fakeInformerSource{inf: inf},
		CloseMode: config.CloseModeUpdate,
	})
	m.Start(ctx)
	waitFor(t, "running state", m.Connected)

	if len(trace) < 2 || trace[0] != "recover:svc_versions" || trace[1] != "register" {
		t.Fatalf("bring-up order must be Recover before Register, got %v", trace)
	}

	// uid-gone was deleted while we were away: ReconcileOpens must close it...
	waitFor(t, "orphan close flushed", func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.fakeStore.closes) == 1
	})
	st.mu.Lock()
	defer st.mu.Unlock()
	if c := st.fakeStore.closes[0]; c.uid != "uid-gone" || !c.validFrom.Equal(vf) {
		t.Fatalf("wrong orphan close: %+v", c)
	}
	// ...and uid-live must survive as tracked last-state.
	if _, ok := m.ing.lookup("uid-live"); !ok {
		t.Fatal("live uid lost from last-state")
	}
	if _, ok := m.ing.lookup("uid-gone"); ok {
		t.Fatal("orphaned uid must be forgotten after reconcile")
	}
}

// A recovered outage with data loss must rebuild the pipeline (teardown old
// handlers, reconnect, re-run the initializing path) — recovery must be
// semantically identical to a restart.
func TestManager_LossTriggersRebuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := &scriptedStore{}
	inf := newFakeInformer(nil)
	cr := mustCompile(t, config.HistoryResource{
		Kind:  "Service",
		Table: "svc_versions",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
		},
	})

	var (
		mu       sync.Mutex
		attempts int
	)
	m := NewManager(ManagerOptions{
		Compiled:  []*CompiledResource{cr},
		Informers: fakeInformerSource{inf: inf},
		CloseMode: config.CloseModeUpdate,
	}, slog.Default())
	m.openStore = func(_ context.Context, _ store.Options) (store.Store, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return st, nil
	}
	m.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }
	m.pingInterval = 5 * time.Millisecond

	m.Start(ctx)
	waitFor(t, "first running state", m.Connected)

	m.mu.Lock()
	ing := m.ing
	m.mu.Unlock()
	ing.losses.Add(1) // simulate a lost write during an outage window

	waitFor(t, "rebuild and second running state", func() bool {
		mu.Lock()
		n := attempts
		mu.Unlock()
		return n == 2 && m.Connected()
	})

	registered, removed := inf.counts()
	if registered != 2 || removed != 1 {
		t.Fatalf("handlers: registered=%d removed=%d, want 2/1 (old detached, new attached)", registered, removed)
	}
	// The rebuilt ingester starts with a fresh loss counter.
	m.mu.Lock()
	fresh := m.ing
	m.mu.Unlock()
	if fresh == ing {
		t.Fatal("rebuild must create a new ingester")
	}
	if fresh.lossCount() != 0 {
		t.Fatalf("fresh ingester lossCount = %d, want 0", fresh.lossCount())
	}
}

// A ping blip with zero data loss must NOT rebuild: the manager resumes in
// place with the same connection, ingester, and handlers.
func TestManager_NoLossBlipResumesInPlace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := &scriptedStore{}
	inf := newFakeInformer(nil)
	cr := mustCompile(t, config.HistoryResource{
		Kind:  "Service",
		Table: "svc_versions",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
		},
	})

	var (
		mu       sync.Mutex
		attempts int
	)
	m := NewManager(ManagerOptions{
		Compiled:  []*CompiledResource{cr},
		Informers: fakeInformerSource{inf: inf},
		CloseMode: config.CloseModeUpdate,
	}, slog.Default())
	m.openStore = func(_ context.Context, _ store.Options) (store.Store, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		return st, nil
	}
	m.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }
	m.pingInterval = 5 * time.Millisecond

	m.Start(ctx)
	waitFor(t, "running state", m.Connected)

	st.setPingErr(errors.New("blip"))
	waitFor(t, "degraded state", func() bool { return m.Status().State == StateDegraded })
	st.setPingErr(nil)
	waitFor(t, "resumed state", m.Connected)

	mu.Lock()
	n := attempts
	mu.Unlock()
	if n != 1 {
		t.Fatalf("openStore attempts = %d, want 1 (no rebuild on lossless blip)", n)
	}
	_, removed := inf.counts()
	if removed != 0 {
		t.Fatalf("handlers removed = %d, want 0", removed)
	}
}
