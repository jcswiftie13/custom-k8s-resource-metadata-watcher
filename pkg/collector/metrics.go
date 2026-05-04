package collector

import (
	"github.com/prometheus/client_golang/prometheus"
)

// selfMetrics holds the collector's own observability metrics. They are
// registered against the main Prometheus registry so that scraping /metrics
// exposes them alongside the rule-defined `_info` gauges.
type selfMetrics struct {
	queueDepth       prometheus.GaugeFunc
	reconcileTotal   *prometheus.CounterVec
	reconcileDur     *prometheus.HistogramVec
	parentFallback   prometheus.Counter
	parentIndexed    prometheus.Counter
	parentIndexSize  prometheus.Collector
}

// sizeProviders bundles the "current map size" callbacks the gauges need.
// Splitting them out keeps newSelfMetrics independent of the collector
// struct's internal layout.
type sizeProviders struct {
	queueDepth     func() float64
	parentByParent func() float64
	parentByAnchor func() float64
}

// newSelfMetrics registers the collector self-metrics against the supplied
// registerer. A nil registerer disables metric registration (useful in tests).
// The callbacks in sizeProviders are polled on every scrape to expose current
// cache/queue sizes.
func newSelfMetrics(reg prometheus.Registerer, sp sizeProviders) *selfMetrics {
	m := &selfMetrics{
		queueDepth: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "exporter_reconcile_queue_depth",
			Help: "Current number of anchorRefs waiting in the reconcile workqueue.",
		}, sp.queueDepth),
		reconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "exporter_reconcile_total",
			Help: "Total number of reconcile attempts, partitioned by rule and result.",
		}, []string{"rule", "result"}),
		reconcileDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "exporter_reconcile_duration_seconds",
			Help:    "Duration of a full anchor reconcile (covers all rules for the anchor).",
			Buckets: prometheus.DefBuckets,
		}, []string{"anchor_kind"}),
		parentFallback: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_parent_index_fallback_total",
			Help: "Parent events that fell back to a namespace-wide rescan because the reverse index was cold.",
		}),
		parentIndexed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_parent_index_hit_total",
			Help: "Parent events that resolved to a bounded set of anchors via the reverse index.",
		}),
		parentIndexSize: newLabeledGaugeCollector(
			"exporter_parent_index_size",
			"Current number of entries in the reverse parent index, by direction.",
			"direction",
			map[string]func() float64{
				"by_parent": sp.parentByParent,
				"by_anchor": sp.parentByAnchor,
			},
		),
	}
	if reg != nil {
		reg.MustRegister(
			m.queueDepth,
			m.reconcileTotal,
			m.reconcileDur,
			m.parentFallback,
			m.parentIndexed,
			m.parentIndexSize,
		)
	}
	return m
}

// labeledGaugeCollector is a prometheus.Collector that emits one gauge per
// label value, computing each value lazily on every scrape. Values must not
// move in and out of the map across scrapes (the set of labels is fixed).
type labeledGaugeCollector struct {
	desc      *prometheus.Desc
	labelName string
	providers map[string]func() float64
}

func newLabeledGaugeCollector(name, help, labelName string, providers map[string]func() float64) *labeledGaugeCollector {
	return &labeledGaugeCollector{
		desc:      prometheus.NewDesc(name, help, []string{labelName}, nil),
		labelName: labelName,
		providers: providers,
	}
}

func (c *labeledGaugeCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *labeledGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	for label, fn := range c.providers {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, fn(), label)
	}
}
