package history

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

// ManagerState is the history subsystem's lifecycle state as reported on
// /readyz and by the exporter_history_store_connected gauge.
type ManagerState string

const (
	// StateConnecting: no usable store connection yet; retrying with backoff.
	StateConnecting ManagerState = "connecting"
	// StateInitializing: connected, running EnsureSchema/Recover/replay.
	StateInitializing ManagerState = "initializing"
	// StateRunning: ingester live, last ping healthy.
	StateRunning ManagerState = "running"
	// StateDegraded: ingester live but the last ping failed; writes are
	// fail-soft (batches drop, counted) until the store recovers.
	StateDegraded ManagerState = "degraded"
)

// defaultPingInterval is how often the running manager re-checks store health.
const defaultPingInterval = 15 * time.Second

// defaultReinitInterval is the minimum spacing between loss-triggered pipeline
// rebuilds (see Manager.reinitInterval).
const defaultReinitInterval = time.Minute

// ManagerOptions wires a Manager. Credentials inside StoreOptions must
// already be resolved — config errors stay fail-fast in main; only the
// connection itself is retried here.
type ManagerOptions struct {
	StoreOptions store.Options
	Compiled     []*CompiledResource
	Informers    InformerSource
	Batch        config.BatchConfig
	CloseMode    string
	Reconnect    config.ReconnectConfig
	// Registerer receives the history self-metrics; nil disables them.
	Registerer prometheus.Registerer
}

// ManagerStatus is the point-in-time status snapshot served on /readyz.
type ManagerStatus struct {
	Enabled       bool         `json:"enabled"`
	State         ManagerState `json:"state"`
	LastError     string       `json:"lastError,omitempty"`
	DroppedEvents uint64       `json:"droppedEvents"`
}

// Manager owns the history subsystem's lifecycle so that ClickHouse being
// unreachable never takes the exporter down: it connects in the background
// with exponential backoff, initializes the ingest pipeline once a connection
// succeeds (EnsureSchema → Recover → Start → Register → ReconcileOpens), and
// then only monitors health — a mid-run outage degrades ingest (fail-soft
// writes) instead of tearing anything down.
type Manager struct {
	o       ManagerOptions
	log     *slog.Logger
	metrics *Metrics

	// openStore, sleep, pingInterval and reinitInterval are seams for unit
	// tests; sleep returns false when ctx was cancelled before the delay
	// elapsed. reinitInterval is the minimum spacing between loss-triggered
	// pipeline rebuilds, so a flapping store cannot cause rebuild storms.
	openStore      func(ctx context.Context, o store.Options) (store.Store, error)
	sleep          func(ctx context.Context, d time.Duration) bool
	pingInterval   time.Duration
	reinitInterval time.Duration

	mu      sync.Mutex
	state   ManagerState
	lastErr error
	st      store.Store
	ing     *Ingester
}

// NewManager builds a Manager; call Start to begin connecting.
func NewManager(o ManagerOptions, log *slog.Logger) *Manager {
	return &Manager{
		o:              o,
		log:            log,
		metrics:        NewMetrics(o.Registerer),
		openStore:      store.Open,
		sleep:          sleepCtx,
		pingInterval:   defaultPingInterval,
		reinitInterval: defaultReinitInterval,
		state:          StateConnecting,
	}
}

// Start launches the manager's background loop and returns immediately. The
// loop exits when ctx is cancelled; the ingester's flush loop shares ctx.
func (m *Manager) Start(ctx context.Context) {
	go m.run(ctx)
}

// Status returns the snapshot served on /readyz.
func (m *Manager) Status() ManagerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := ManagerStatus{Enabled: true, State: m.state}
	if m.lastErr != nil {
		s.LastError = m.lastErr.Error()
	}
	if m.ing != nil {
		s.DroppedEvents = m.ing.droppedTotal.Load()
	}
	return s
}

// Connected reports whether the store connection is established and healthy.
func (m *Manager) Connected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state == StateRunning
}

func (m *Manager) setState(s ManagerState, err error) {
	m.mu.Lock()
	m.state = s
	m.lastErr = err
	m.mu.Unlock()
}

func (m *Manager) run(ctx context.Context) {
	rc := m.o.Reconnect
	initial := time.Duration(rc.InitialBackoffMsOrDefault()) * time.Millisecond
	maxB := time.Duration(rc.MaxBackoffMsOrDefault()) * time.Millisecond
	pingTimeout := time.Duration(rc.PingTimeoutMsOrDefault()) * time.Millisecond

	m.metrics.setConnected(false)
	backoff := initial
	for ctx.Err() == nil {
		m.metrics.incReconnectAttempt()
		octx, cancel := context.WithTimeout(ctx, pingTimeout)
		st, err := m.openStore(octx, m.o.StoreOptions)
		cancel()
		if err != nil {
			m.setState(StateConnecting, err)
			m.log.Warn("history store connect failed; retrying",
				"backoff", backoff.Round(time.Millisecond), "err", err)
			if !m.sleep(ctx, jitter(backoff)) {
				return
			}
			backoff = nextBackoff(backoff, maxB)
			continue
		}

		m.setState(StateInitializing, nil)
		m.metrics.setConnected(true)
		// The ingester's flush loop gets its own cancellable context so a
		// loss-triggered rebuild can stop it without touching the manager ctx.
		ingCtx, ingCancel := context.WithCancel(ctx)
		ing, err := m.initialize(ingCtx, st)
		if err != nil {
			if ing != nil {
				ing.Unregister()
			}
			ingCancel()
			if ing != nil && ing.loopDone != nil {
				<-ing.loopDone
			}
			_ = st.Close()
			m.setState(StateConnecting, err)
			m.metrics.setConnected(false)
			if ctx.Err() != nil {
				return
			}
			m.log.Error("history store init failed; reconnecting",
				"backoff", backoff.Round(time.Millisecond), "err", err)
			if !m.sleep(ctx, jitter(backoff)) {
				return
			}
			backoff = nextBackoff(backoff, maxB)
			continue
		}

		m.mu.Lock()
		m.st, m.ing = st, ing
		m.state = StateRunning
		m.lastErr = nil
		m.mu.Unlock()
		m.log.Info("history ingest enabled", "resources", len(m.o.Compiled),
			"createSchema", m.o.StoreOptions.CreateSchema, "closeMode", m.o.CloseMode)

		runningSince := time.Now()
		reinit := m.watch(ctx, st, ing, pingTimeout)
		m.teardown(ing, ingCancel)
		_ = st.Close()
		if !reinit {
			return // ctx cancelled: process shutdown
		}
		// Losses occurred (outage or sustained write failures): the store no
		// longer matches in-memory state, and in-memory dedup would suppress
		// the corrective writes forever. Rebuild the whole pipeline — the
		// initializing path (Recover → replay → ReconcileOpens) makes
		// reconnection semantically identical to a process restart. Space
		// rebuilds out so a flapping store cannot cause a rebuild storm.
		m.setState(StateConnecting, nil)
		m.metrics.setConnected(false)
		m.log.Warn("history store diverged during outage; rebuilding ingest pipeline",
			"losses", ing.lossCount())
		if wait := m.reinitInterval - time.Since(runningSince); wait > 0 {
			if !m.sleep(ctx, wait) {
				return
			}
		}
		backoff = initial
	}
}

// teardown detaches and drains an ingester: handlers are removed first so no
// new events are produced, then the flush loop is cancelled and awaited (its
// exit flush runs with a short grace context, so buffered rows still land
// when the store is reachable).
func (m *Manager) teardown(ing *Ingester, cancel context.CancelFunc) {
	m.mu.Lock()
	m.ing = nil
	m.st = nil
	m.mu.Unlock()
	ing.Unregister()
	cancel()
	<-ing.loopDone
}

// initialize runs the connected-store bring-up. Order matters: Recover must
// see the store before any event is processed (dedup baseline), the flush
// loop must run before Register so the queue drains during the replay storm,
// and ReconcileOpens must wait for the replay to finish (HasSynced).
func (m *Manager) initialize(ctx context.Context, st store.Store) (*Ingester, error) {
	if err := st.EnsureSchema(ctx, TableSchemas(m.o.Compiled)); err != nil {
		return nil, err
	}
	ing := NewIngester(st, m.o.Informers, m.o.Compiled, m.o.Batch, m.o.CloseMode, m.log, m.metrics)
	if err := ing.Recover(ctx); err != nil {
		return nil, err
	}
	ing.Start(ctx)
	// Past this point the ingester is returned even on error, so the caller
	// can drain its flush loop before discarding it.
	if err := ing.Register(); err != nil {
		return ing, err
	}
	if !cache.WaitForCacheSync(ctx.Done(), ing.HasSynced) {
		ing.Unregister()
		return ing, ctx.Err()
	}
	ing.ReconcileOpens()
	return ing, nil
}

// watch polls store health until ctx is cancelled (returns false) or a
// rebuild is needed (returns true), flipping between running and degraded in
// the meantime. The rebuild trigger is data loss, not the outage itself: any
// operation the ingester lost (failed flush, evicted insert, overflowed
// close) means the store diverged from in-memory state and dedup will
// suppress the fix forever — but the rebuild has to wait until the store is
// reachable again, so the trigger is "loss occurred AND ping succeeds". A
// blip with zero losses resumes in place with no rebuild.
func (m *Manager) watch(ctx context.Context, st store.Store, ing *Ingester, pingTimeout time.Duration) bool {
	base := ing.lossCount()
	ticker := time.NewTicker(m.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := st.Ping(pctx)
			cancel()
			m.mu.Lock()
			prev := m.state
			m.mu.Unlock()
			if err != nil {
				m.setState(StateDegraded, err)
				m.metrics.setConnected(false)
				if prev == StateRunning {
					m.log.Warn("history store unreachable; ingest degraded until it recovers", "err", err)
				}
				continue
			}
			if ing.lossCount() != base {
				return true
			}
			m.setState(StateRunning, nil)
			m.metrics.setConnected(true)
			if prev == StateDegraded {
				m.log.Info("history store reachable again; no writes were lost, ingest resumed in place")
			}
		}
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if cur >= max/2 {
		return max
	}
	return cur * 2
}

// jitter spreads a delay by ±20% so restarting replicas don't reconnect in
// lockstep.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()*0.4-0.2)*float64(d))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
