package collector

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	clientscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/example/metadata-exporter/pkg/config"
)

// CompiledRule pre-compiles every path expression in a rule so that
// evaluation per scrape touches only the cache and does zero parsing work.
type CompiledRule struct {
	Rule *config.Rule

	// ForEach is nil when the rule does not expand into sub-items.
	ForEach *parsedPath

	// LabelOrder is the canonical order of fixed label names (sorted) used
	// when assembling the per-series label vector. ExpandLabels add
	// additional, dynamic label names that are computed at scrape time.
	LabelOrder []string

	// Labels is LabelOrder-aligned; each entry holds the compiled extracts
	// for its fixed label.
	Labels []CompiledLabel

	// Expands holds compiled expandLabels entries.
	Expands []CompiledExpand
}

// CompiledLabel holds the resolved source + compiled path + fallbacks for a
// single fixed label.
type CompiledLabel struct {
	Name      string
	Primary   CompiledExtract
	Fallbacks []CompiledExtract
	OnMissing string
}

// CompiledExtract is a single evaluated source + path.
type CompiledExtract struct {
	Source string // resolved source (anchor, item, ownerController, topController, or a Kind name)
	Path   *parsedPath
}

// CompiledExpand is a compiled expandLabels entry. Path resolves to a
// map[string]interface{} at evaluation time; AllowSet/DenySet make
// allow/deny lookups O(1); MaxKeys=0 means unlimited.
type CompiledExpand struct {
	Source   string
	Path     *parsedPath
	Prefix   string
	AllowSet map[string]struct{} // nil when allow is empty
	DenySet  map[string]struct{} // nil when deny is empty
	MaxKeys  int
}

// Compile transforms a config.Rule into a CompiledRule. Path syntax errors
// are returned so the process can exit early at startup.
func Compile(rule *config.Rule) (*CompiledRule, error) {
	cr := &CompiledRule{Rule: rule}

	if rule.ForEach != "" {
		p, err := parsePath(rule.ForEach)
		if err != nil {
			return nil, fmt.Errorf("rule %q forEach %q: %w", rule.Name, rule.ForEach, err)
		}
		cr.ForEach = p
	}

	names := make([]string, 0, len(rule.Labels))
	for name := range rule.Labels {
		names = append(names, name)
	}
	sort.Strings(names)
	cr.LabelOrder = names

	cr.Labels = make([]CompiledLabel, len(names))
	for i, name := range names {
		ext := rule.Labels[name]
		primary, err := compileExtract(rule, name, "primary", ext)
		if err != nil {
			return nil, err
		}
		fb := make([]CompiledExtract, 0, len(ext.Fallbacks))
		for fi, f := range ext.Fallbacks {
			ce, err := compileExtract(rule, name, fmt.Sprintf("fallbacks[%d]", fi), f)
			if err != nil {
				return nil, err
			}
			fb = append(fb, ce)
		}
		cr.Labels[i] = CompiledLabel{
			Name:      name,
			Primary:   primary,
			Fallbacks: fb,
			OnMissing: ext.OnMissingValue(),
		}
	}

	cr.Expands = make([]CompiledExpand, 0, len(rule.ExpandLabels))
	for i, ex := range rule.ExpandLabels {
		ce, err := compileExpand(rule, i, ex)
		if err != nil {
			return nil, err
		}
		cr.Expands = append(cr.Expands, ce)
	}

	return cr, nil
}

func compileExtract(rule *config.Rule, labelName, slot string, e config.Extract) (CompiledExtract, error) {
	src := e.EffectiveSource()
	p, err := parsePath(e.Path)
	if err != nil {
		return CompiledExtract{}, fmt.Errorf("rule %q label %q %s: invalid path %q: %w", rule.Name, labelName, slot, e.Path, err)
	}
	return CompiledExtract{
		Source: src,
		Path:   p,
	}, nil
}

func compileExpand(rule *config.Rule, idx int, e config.ExpandLabel) (CompiledExpand, error) {
	p, err := parsePath(e.Path)
	if err != nil {
		return CompiledExpand{}, fmt.Errorf("rule %q expandLabels[%d]: invalid path %q: %w", rule.Name, idx, e.Path, err)
	}
	var allow, deny map[string]struct{}
	if len(e.Allow) > 0 {
		allow = make(map[string]struct{}, len(e.Allow))
		for _, k := range e.Allow {
			allow[k] = struct{}{}
		}
	}
	if len(e.Deny) > 0 {
		deny = make(map[string]struct{}, len(e.Deny))
		for _, k := range e.Deny {
			deny[k] = struct{}{}
		}
	}
	return CompiledExpand{
		Source:   e.EffectiveSource(),
		Path:     p,
		Prefix:   e.Prefix,
		AllowSet: allow,
		DenySet:  deny,
		MaxKeys:  e.MaxKeys,
	}, nil
}

// Evaluator converts runtime.Objects to unstructured maps (once) and runs
// compiled paths against them. Safe for concurrent use.
type Evaluator struct{}

// NewEvaluator returns an Evaluator.
func NewEvaluator() *Evaluator { return &Evaluator{} }

// ToUnstructured converts a typed runtime.Object to the untyped map form.
// A nil input yields a nil map so callers can cheaply detect misses.
func (e *Evaluator) ToUnstructured(obj runtime.Object) (map[string]interface{}, error) {
	if obj == nil {
		return nil, nil
	}
	if v := reflect.ValueOf(obj); v.Kind() == reflect.Ptr && v.IsNil() {
		return nil, nil
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("to unstructured: %w", err)
	}
	enrichTypeMetaFromScheme(m, obj)
	return m, nil
}

// enrichTypeMetaFromScheme fills missing kind/apiVersion from the global
// client scheme. Lister/informer objects often have empty embedded TypeMeta
// while retaining their concrete Go type, so JSONPath "kind" would otherwise
// evaluate to empty.
func enrichTypeMetaFromScheme(m map[string]interface{}, obj runtime.Object) {
	if m == nil || obj == nil {
		return
	}
	k, _ := m["kind"].(string)
	v, _ := m["apiVersion"].(string)
	if k != "" && v != "" {
		return
	}
	gvks, _, err := clientscheme.Scheme.ObjectKinds(obj)
	if err != nil || len(gvks) == 0 {
		return
	}
	gvk := gvks[0]
	if k == "" && gvk.Kind != "" {
		m["kind"] = gvk.Kind
	}
	if v == "" {
		if gv := gvk.GroupVersion(); gv.String() != "" {
			m["apiVersion"] = gv.String()
		}
	}
}

// EvaluateForEach returns the list of items produced by the rule's forEach
// path. A nil forEach yields a single-element slice containing nil so callers
// can iterate uniformly.
func (e *Evaluator) EvaluateForEach(cr *CompiledRule, anchor map[string]interface{}) []map[string]interface{} {
	if cr.ForEach == nil {
		return []map[string]interface{}{nil}
	}
	results := cr.ForEach.evaluate(anchor)
	items := make([]map[string]interface{}, 0, len(results))
	for _, v := range results {
		if m, ok := v.(map[string]interface{}); ok {
			items = append(items, m)
			continue
		}
		// Non-map items (e.g. plain strings): wrap so path "_" can reach them.
		items = append(items, map[string]interface{}{"_": v})
	}
	return items
}

// EvaluateLabel runs the primary extract, then fallbacks, then OnMissing.
// srcLookup must return nil for unknown sources without erroring.
func (e *Evaluator) EvaluateLabel(label CompiledLabel, srcLookup func(source string) map[string]interface{}) string {
	if v, ok := e.tryExtract(label.Primary, srcLookup); ok {
		return v
	}
	for _, f := range label.Fallbacks {
		if v, ok := e.tryExtract(f, srcLookup); ok {
			return v
		}
	}
	return label.OnMissing
}

func (e *Evaluator) tryExtract(ce CompiledExtract, srcLookup func(source string) map[string]interface{}) (string, bool) {
	input := srcLookup(ce.Source)
	if input == nil {
		return "", false
	}
	results := ce.Path.evaluate(input)
	for _, raw := range results {
		if raw == nil {
			continue
		}
		s := stringifyValue(raw)
		if s == "" {
			continue
		}
		return s, true
	}
	return "", false
}

// EvaluateExpand resolves the configured map and returns the (sanitised,
// prefixed) -> value pairs that should appear as dynamic labels for this
// series. Allow/Deny/MaxKeys are applied. The output map's keys are already
// the final Prometheus label names. An empty result is returned when the
// path does not resolve to a map (consistent with EvaluateLabel "miss"
// semantics: callers must treat absent labels as the empty string).
func (e *Evaluator) EvaluateExpand(ce CompiledExpand, srcLookup func(source string) map[string]interface{}) map[string]string {
	input := srcLookup(ce.Source)
	if input == nil {
		return nil
	}
	results := ce.Path.evaluate(input)
	if len(results) == 0 {
		return nil
	}
	// We take the first map result; ambiguity (multiple results) is not
	// expected for metadata.labels / metadata.annotations.
	m, ok := results[0].(map[string]interface{})
	if !ok || len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		if ce.DenySet != nil {
			if _, denied := ce.DenySet[k]; denied {
				continue
			}
		}
		if ce.AllowSet != nil {
			if _, allowed := ce.AllowSet[k]; !allowed {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if ce.MaxKeys > 0 && len(keys) > ce.MaxKeys {
		keys = keys[:ce.MaxKeys]
	}

	out := make(map[string]string, len(keys))
	for _, k := range keys {
		raw, has := m[k]
		if !has || raw == nil {
			continue
		}
		v := stringifyValue(raw)
		if v == "" {
			continue
		}
		labelName := ce.Prefix + sanitizeLabelKey(k)
		out[labelName] = v
	}
	return out
}

// sanitizeLabelKey converts a Kubernetes map key (e.g. label or annotation)
// into a Prometheus-safe label name suffix. Per the Prometheus label name
// regex `[a-zA-Z_][a-zA-Z0-9_]*`, every character outside [A-Za-z0-9_] is
// replaced with '_'. Leading characters are not specially handled here — the
// caller's prefix is responsible for ensuring the final name starts with a
// valid character (every supported prefix ends with `_`).
func sanitizeLabelKey(k string) string {
	if k == "" {
		return "_"
	}
	var b strings.Builder
	b.Grow(len(k))
	for _, r := range k {
		switch {
		case r == '_':
			b.WriteByte('_')
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// stringifyValue converts common scalar values to strings; composite values
// are rendered with %v as a best-effort fallback.
func stringifyValue(raw interface{}) string {
	switch v := raw.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return ""
	}
	rv := reflect.ValueOf(raw)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", rv.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%g", rv.Float())
	}
	return fmt.Sprintf("%v", raw)
}
