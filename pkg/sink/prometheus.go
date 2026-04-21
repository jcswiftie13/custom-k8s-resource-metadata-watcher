package sink

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusSink implements MetadataSink by maintaining one *prometheus.GaugeVec
// per rule. Every series is emitted with the constant value 1, following the
// `_info` pattern popularised by kube-state-metrics.
type PrometheusSink struct {
	reg prometheus.Registerer

	mu    sync.Mutex
	rules map[string]*ruleState
}

type ruleState struct {
	schema RuleSchema
	vec    *prometheus.GaugeVec

	// Maps seriesKey -> ordered label values, used for Delete and for
	// pruning stale series during ReplaceForAnchor.
	series map[string][]string

	// anchorKey -> set of seriesKeys emitted by that anchor.
	byAnchor map[string]map[string]struct{}
}

// NewPrometheusSink constructs a sink bound to the supplied registry.
// Passing prometheus.DefaultRegisterer wires it into the global registry used
// by promhttp.Handler().
func NewPrometheusSink(reg prometheus.Registerer) *PrometheusSink {
	return &PrometheusSink{
		reg:   reg,
		rules: map[string]*ruleState{},
	}
}

// RegisterRule creates a GaugeVec for the rule.
func (p *PrometheusSink) RegisterRule(rule RuleSchema) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.rules[rule.Name]; exists {
		return fmt.Errorf("rule %q already registered", rule.Name)
	}
	labels := append([]string(nil), rule.Labels...)
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: rule.Name,
		Help: rule.Help,
	}, labels)
	if err := p.reg.Register(vec); err != nil {
		return fmt.Errorf("register metric %q: %w", rule.Name, err)
	}
	p.rules[rule.Name] = &ruleState{
		schema:   RuleSchema{Name: rule.Name, Help: rule.Help, Labels: labels},
		vec:      vec,
		series:   map[string][]string{},
		byAnchor: map[string]map[string]struct{}{},
	}
	return nil
}

// Upsert sets a single series. anchorKey is derived as empty here; callers
// that need ReplaceForAnchor semantics should use ReplaceForAnchor instead.
func (p *PrometheusSink) Upsert(ruleName string, seriesKey string, labels map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.rules[ruleName]
	if !ok {
		return
	}
	values := make([]string, len(state.schema.Labels))
	for i, name := range state.schema.Labels {
		values[i] = labels[name]
	}
	state.series[seriesKey] = values
	state.vec.WithLabelValues(values...).Set(1)
}

// Delete removes a single series.
func (p *PrometheusSink) Delete(ruleName string, seriesKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.rules[ruleName]
	if !ok {
		return
	}
	values, ok := state.series[seriesKey]
	if !ok {
		return
	}
	state.vec.DeleteLabelValues(values...)
	delete(state.series, seriesKey)
	for anchor, keys := range state.byAnchor {
		if _, has := keys[seriesKey]; has {
			delete(keys, seriesKey)
			if len(keys) == 0 {
				delete(state.byAnchor, anchor)
			}
			break
		}
	}
}

// ReplaceForAnchor atomically replaces the series that belong to anchorKey.
// Series that were previously emitted by anchorKey but are not present in
// seriesByKey are removed from the GaugeVec.
func (p *PrometheusSink) ReplaceForAnchor(ruleName string, anchorKey string, seriesByKey map[string]map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.rules[ruleName]
	if !ok {
		return
	}

	previous := state.byAnchor[anchorKey]
	next := map[string]struct{}{}

	for seriesKey, labels := range seriesByKey {
		values := make([]string, len(state.schema.Labels))
		for i, name := range state.schema.Labels {
			values[i] = labels[name]
		}
		if old, had := state.series[seriesKey]; had && !slicesEqual(old, values) {
			state.vec.DeleteLabelValues(old...)
		}
		state.series[seriesKey] = values
		state.vec.WithLabelValues(values...).Set(1)
		next[seriesKey] = struct{}{}
	}

	for seriesKey := range previous {
		if _, keep := next[seriesKey]; keep {
			continue
		}
		if values, ok := state.series[seriesKey]; ok {
			state.vec.DeleteLabelValues(values...)
			delete(state.series, seriesKey)
		}
	}

	if len(next) == 0 {
		delete(state.byAnchor, anchorKey)
	} else {
		state.byAnchor[anchorKey] = next
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
