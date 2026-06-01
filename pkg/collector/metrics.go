package collector

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// selfMetrics holds the collector's own observability metrics. They are
// registered against the main Prometheus registry so that scraping /metrics
// exposes them alongside the rule-defined `_info` gauges.
//
// Naming aligns with the new scrape-time architecture:
//   - exporter_collect_total{rule, result}        — collect attempts per rule
//   - exporter_collect_duration_seconds{rule}     — per-rule collect latency
//   - exporter_anchor_count{rule, kind}           — anchors observed at last scrape
//
// Anchor count is gauged at collect time for ops dashboards (cardinality
// estimation, drift detection between scrapes).
type selfMetrics struct {
	collectTotal    *prometheus.CounterVec
	collectDuration *prometheus.HistogramVec
	anchorCount     *prometheus.GaugeVec
}

// newSelfMetrics registers the collector self-metrics against the supplied
// registerer. A nil registerer disables metric registration (useful in tests).
// ruleAnchors is the canonical (rule -> anchor kind) map so dimensions are
// pre-initialised to zero, allowing rate() to behave correctly from the very
// first scrape and exposing every label combination in the very first
// /metrics response.
func newSelfMetrics(reg prometheus.Registerer, ruleAnchors map[string]string) *selfMetrics {
	m := &selfMetrics{
		collectTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "exporter_collect_total",
			Help: "Total number of /metrics collect attempts, partitioned by rule and result.",
		}, []string{"rule", "result"}),
		collectDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "exporter_collect_duration_seconds",
			Help:    "Per-rule duration of a single Prometheus scrape collect cycle.",
			Buckets: prometheus.DefBuckets,
		}, []string{"rule"}),
		anchorCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "exporter_anchor_count",
			Help: "Anchor object count observed by the most recent collect for the rule.",
		}, []string{"rule", "kind"}),
	}
	for rule, kind := range ruleAnchors {
		m.collectTotal.WithLabelValues(rule, "ok").Add(0)
		m.collectTotal.WithLabelValues(rule, "error").Add(0)
		m.anchorCount.WithLabelValues(rule, kind).Set(0)
	}
	if reg != nil {
		reg.MustRegister(m.collectTotal, m.collectDuration, m.anchorCount)
	}
	return m
}

func (m *selfMetrics) incCollect(rule, result string) {
	if m == nil {
		return
	}
	m.collectTotal.WithLabelValues(rule, result).Inc()
}

func (m *selfMetrics) observeCollectDuration(rule string, d time.Duration) {
	if m == nil {
		return
	}
	m.collectDuration.WithLabelValues(rule).Observe(d.Seconds())
}

func (m *selfMetrics) observeAnchorCount(rule, kind string, n int) {
	if m == nil {
		return
	}
	m.anchorCount.WithLabelValues(rule, kind).Set(float64(n))
}
