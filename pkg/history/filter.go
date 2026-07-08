package history

import (
	"strings"

	"github.com/example/metadata-exporter/pkg/collector"
)

// srcLookup resolves a source name to its object map. The ingest path supports
// the anchor object only (single changed object per event).
type srcLookup func(source string) map[string]interface{}

// Passes reports whether every filter accepts the object. Filters are ordered
// cheap-first (regex last); the first rejecting filter short-circuits.
func (cr *CompiledResource) Passes(ev *collector.Evaluator, lookup srcLookup) bool {
	for i := range cr.Filters {
		if !cr.Filters[i].matches(ev, lookup) {
			return false
		}
	}
	return true
}

func (f *CompiledFilter) matches(ev *collector.Evaluator, lookup srcLookup) bool {
	res := f.eval(ev, lookup)
	if f.Negate {
		return !res
	}
	return res
}

func (f *CompiledFilter) eval(ev *collector.Evaluator, lookup srcLookup) bool {
	vals := ev.EvaluateExtractAll(f.Extract, lookup)
	switch f.Op {
	case "exists":
		return len(vals) > 0
	case "equals":
		return anyMatch(vals, func(s string) bool { return s == f.Value })
	case "notEquals":
		// True when no extracted value equals the operand.
		return !anyMatch(vals, func(s string) bool { return s == f.Value })
	case "prefix":
		return anyMatch(vals, func(s string) bool { return strings.HasPrefix(s, f.Value) })
	case "suffix":
		return anyMatch(vals, func(s string) bool { return strings.HasSuffix(s, f.Value) })
	case "contains":
		return anyMatch(vals, func(s string) bool { return strings.Contains(s, f.Value) })
	case "in":
		return anyMatch(vals, func(s string) bool { _, ok := f.Values[s]; return ok })
	case "regex":
		return anyMatch(vals, func(s string) bool { return f.Regex.MatchString(s) })
	}
	return false
}

// anyMatch reports whether any value satisfies pred (match-any semantics for
// wildcard/array paths).
func anyMatch(vals []string, pred func(string) bool) bool {
	for _, v := range vals {
		if pred(v) {
			return true
		}
	}
	return false
}
