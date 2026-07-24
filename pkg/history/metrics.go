package history

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Dropped-event kinds for Metrics.incDropped.
const (
	droppedKindInsert = "insert"
	droppedKindClose  = "close"
)

// Close reasons for Metrics.incClose — one per code path that materializes a
// valid_to, so a spurious close is attributable in the field.
const (
	closeReasonSupersede       = "supersede"           // live Update replaced the open version
	closeReasonRelistChange    = "relist_change"       // change observed via re-LIST/replay Add
	closeReasonDelete          = "delete"              // informer Delete event
	closeReasonReconcileOrphan = "reconcile_orphan"    // ReconcileOpens: object absent from informer cache
	closeReasonRecoverSweep    = "recover_stale_sweep" // Recover: stale open left by a crash window
)

var closeReasons = []string{
	closeReasonSupersede, closeReasonRelistChange, closeReasonDelete,
	closeReasonReconcileOrphan, closeReasonRecoverSweep,
}

// Metrics holds the history subsystem's own observability metrics, shared by
// the Manager (connection state) and the Ingester (queue/write losses). All
// methods are nil-safe so tests can pass a nil *Metrics.
type Metrics struct {
	connected         prometheus.Gauge
	dropped           *prometheus.CounterVec
	closes            *prometheus.CounterVec
	writeFailures     prometheus.Counter
	reconnectAttempts prometheus.Counter
}

// NewMetrics registers the history self-metrics against the supplied
// registerer. A nil registerer disables registration (useful in tests).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "exporter_history_store_connected",
			Help: "1 while the history store connection is established and healthy, 0 while connecting or degraded.",
		}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "exporter_history_dropped_events_total",
			Help: "History queue operations discarded by drop-oldest eviction, partitioned by kind (insert loses one version row; close only overflows in extreme backlog).",
		}, []string{"kind"}),
		closes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "exporter_history_closes_total",
			Help: "History versions closed (valid_to materialized), partitioned by reason: supersede/relist_change (a newer version), delete (informer Delete), reconcile_orphan (object gone from cache at startup), recover_stale_sweep (crash-window repair).",
		}, []string{"reason"}),
		writeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_history_write_failures_total",
			Help: "Failed history store flush operations (WriteBatch/CloseVersion); the affected batch is dropped.",
		}),
		reconnectAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_history_reconnect_attempts_total",
			Help: "History store connection attempts, successful or not.",
		}),
	}
	m.dropped.WithLabelValues(droppedKindInsert).Add(0)
	m.dropped.WithLabelValues(droppedKindClose).Add(0)
	for _, r := range closeReasons {
		m.closes.WithLabelValues(r).Add(0)
	}
	if reg != nil {
		reg.MustRegister(m.connected, m.dropped, m.closes, m.writeFailures, m.reconnectAttempts)
	}
	return m
}

func (m *Metrics) setConnected(up bool) {
	if m == nil {
		return
	}
	if up {
		m.connected.Set(1)
	} else {
		m.connected.Set(0)
	}
}

func (m *Metrics) incClose(reason string) {
	if m == nil {
		return
	}
	m.closes.WithLabelValues(reason).Inc()
}

func (m *Metrics) incDropped(kind string) {
	if m == nil {
		return
	}
	m.dropped.WithLabelValues(kind).Inc()
}

func (m *Metrics) incWriteFailure() {
	if m == nil {
		return
	}
	m.writeFailures.Inc()
}

func (m *Metrics) incReconnectAttempt() {
	if m == nil {
		return
	}
	m.reconnectAttempts.Inc()
}
