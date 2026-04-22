package main

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientgometrics "k8s.io/client-go/tools/metrics"
)

// registerClientGoMetrics wires client-go's pluggable metrics interface to a
// Prometheus registry. Without this, the `rest_client_*` series users expect
// from kube-state-metrics or kube-controller-manager are missing. We keep the
// surface minimal: request count + latency + rate-limiter wait latency are
// enough to diagnose apiserver pressure.
func registerClientGoMetrics(reg prometheus.Registerer) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rest_client_requests_total",
		Help: "Number of HTTP requests, partitioned by status code, method, and host.",
	}, []string{"code", "method", "host"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rest_client_request_duration_seconds",
		Help:    "Request latency in seconds, partitioned by verb and host.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"verb", "host"})
	rateLimiter := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rest_client_rate_limiter_duration_seconds",
		Help:    "Client-side rate-limiter wait time in seconds, partitioned by verb and host.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"verb", "host"})

	reg.MustRegister(requests, latency, rateLimiter)

	clientgometrics.Register(clientgometrics.RegisterOpts{
		RequestResult:      &counterAdapter{v: requests},
		RequestLatency:     &latencyAdapter{v: latency},
		RateLimiterLatency: &latencyAdapter{v: rateLimiter},
	})
}

type counterAdapter struct{ v *prometheus.CounterVec }

func (c *counterAdapter) Increment(_ context.Context, code, method, host string) {
	// code can be a blank string when the transport errored before receiving
	// a response; normalise to a "0" bucket so the label set stays small.
	if code == "" {
		code = "0"
	}
	if _, err := strconv.Atoi(code); err != nil {
		code = "invalid"
	}
	c.v.WithLabelValues(code, method, host).Inc()
}

type latencyAdapter struct{ v *prometheus.HistogramVec }

func (l *latencyAdapter) Observe(_ context.Context, verb string, u url.URL, latency time.Duration) {
	l.v.WithLabelValues(verb, u.Host).Observe(latency.Seconds())
}
