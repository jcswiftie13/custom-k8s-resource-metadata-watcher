// Package sink defines the MetadataSink interface for emitting per-series
// metadata and includes a Prometheus GaugeVec implementation.
package sink

// RuleSchema describes a metric that a sink must register at startup so the
// label set is fixed up-front (mandatory for Prometheus consistency).
type RuleSchema struct {
	// Name is the fully-qualified metric name (prefix already applied).
	Name string
	// Help is the metric help string.
	Help string
	// Labels is the ordered list of label names emitted for this metric.
	Labels []string
}

// MetadataSink is the backend-agnostic abstraction over "set one gauge series
// at value 1 with these labels". Implementations must be safe for concurrent
// use from any number of collector goroutines.
type MetadataSink interface {
	// RegisterRule prepares the backend to accept upserts for the given rule.
	// It must be called for every rule before any Upsert / ReplaceForAnchor.
	RegisterRule(rule RuleSchema) error

	// Upsert sets a single series identified by seriesKey. The labels map
	// must contain exactly the label names declared in RegisterRule.
	Upsert(ruleName string, seriesKey string, labels map[string]string)

	// Delete removes a single series.
	Delete(ruleName string, seriesKey string)

	// ReplaceForAnchor atomically replaces the set of series owned by a given
	// anchor object. Stale seriesKeys previously owned by anchorKey but absent
	// from seriesByKey are removed. Passing a nil or empty map removes all
	// series for anchorKey (equivalent to handling a DELETE event).
	ReplaceForAnchor(ruleName string, anchorKey string, seriesByKey map[string]map[string]string)
}
