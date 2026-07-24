package collector

import (
	"fmt"

	"github.com/example/metadata-exporter/pkg/config"
)

// CompileExtract compiles a standalone config.Extract into a CompiledExtract.
// It is the entry point used by non-metric consumers (e.g. the history ingest
// path) that need the same source+json-path extraction engine as metric
// labels but outside the context of a Rule.
func CompileExtract(e config.Extract) (CompiledExtract, error) {
	p, err := parsePath(e.Path)
	if err != nil {
		return CompiledExtract{}, fmt.Errorf("invalid path %q: %w", e.Path, err)
	}
	return CompiledExtract{Source: e.EffectiveSource(), Path: p}, nil
}

// EvaluateExtractRaw returns every raw (non-stringified) value the compiled
// path resolves to against its source object. nil is returned when the source
// is unknown/absent. Callers that need the underlying map (encode=kv) or an
// arbitrary subtree (encode=json) use this instead of the string form.
func (e *Evaluator) EvaluateExtractRaw(ce CompiledExtract, srcLookup func(source string) map[string]interface{}) []interface{} {
	input := srcLookup(ce.Source)
	if input == nil {
		return nil
	}
	return ce.Path.evaluate(input)
}

// EvaluateExtractAll returns every non-empty stringified value the compiled
// path resolves to. For a scalar path this is 0 or 1 element; for a wildcard
// path (e.g. spec.hosts[*]) it is one element per match. Empty and nil results
// are skipped, matching EvaluateLabel "miss" semantics.
func (e *Evaluator) EvaluateExtractAll(ce CompiledExtract, srcLookup func(source string) map[string]interface{}) []string {
	raw := e.EvaluateExtractRaw(ce, srcLookup)
	if len(raw) == 0 {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if v == nil {
			continue
		}
		s := stringifyValue(v)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

// StringifyValue exposes the scalar-to-string conversion used internally so
// history encoders can render map values consistently with metric labels.
func StringifyValue(raw interface{}) string { return stringifyValue(raw) }
