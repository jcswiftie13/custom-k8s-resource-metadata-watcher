// Package history implements the event-driven ClickHouse ingest path: it
// compiles the history config, evaluates client-side filters, and turns
// informer events into append-only version rows written via pkg/store.
package history

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

// CompiledColumn is a declared column plus either a typed constant or a
// compiled extraction path (primary → fallbacks → onMissing).
type CompiledColumn struct {
	Name   string
	Type   string // ClickHouse type
	Encode string // "", "json", "kv"
	Index  string // "", "bloom_filter"

	// Constant mode (IsConstant): fixed value resolved at compile time.
	IsConstant bool
	Constant   any

	// Path mode: EvaluateLabel-equivalent extraction.
	Primary      collector.CompiledExtract
	Fallbacks    []collector.CompiledExtract
	HasOnMissing bool
	OnMissing    string
}

// CompiledFilter is one compiled predicate. Regex is pre-compiled once so the
// per-event hot path never recompiles.
type CompiledFilter struct {
	Op      string
	Value   string
	Values  map[string]struct{} // for op=in
	Negate  bool
	Regex   *regexp.Regexp // for op=regex
	isRegex bool
	Extract collector.CompiledExtract
}

// CompiledResource holds the compiled columns and filters for one Kind. Filters
// are ordered cheap-first (non-regex before regex) so short-circuiting avoids
// regex cost whenever a cheaper predicate already fails.
type CompiledResource struct {
	Kind    string
	Table   string
	Columns []CompiledColumn
	Filters []CompiledFilter
}

// Compile builds a CompiledResource from one resource config (without injecting
// top-level history.constants). The config is assumed already validated.
func Compile(r config.HistoryResource) (*CompiledResource, error) {
	cr := &CompiledResource{Kind: r.Kind, Table: r.TableName()}

	for _, c := range r.Columns {
		cc, err := compileColumn(c)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}
		cr.Columns = append(cr.Columns, cc)
	}

	var regexFilters []CompiledFilter
	for _, f := range r.Filters {
		ce, err := collector.CompileExtract(f.Extract)
		if err != nil {
			return nil, fmt.Errorf("filter (op=%s): %w", f.Op, err)
		}
		cf := CompiledFilter{Op: f.Op, Value: f.Value, Negate: f.Negate, Extract: ce}
		switch f.Op {
		case "in":
			cf.Values = make(map[string]struct{}, len(f.Values))
			for _, v := range f.Values {
				cf.Values[v] = struct{}{}
			}
		case "regex":
			re, err := regexp.Compile(f.Value)
			if err != nil {
				return nil, fmt.Errorf("filter (op=regex): %w", err)
			}
			cf.Regex = re
			cf.isRegex = true
		}
		if cf.isRegex {
			regexFilters = append(regexFilters, cf)
		} else {
			cr.Filters = append(cr.Filters, cf)
		}
	}
	// Regex filters run last so a failing cheap predicate short-circuits first.
	cr.Filters = append(cr.Filters, regexFilters...)
	return cr, nil
}

// CompileAll compiles every resource in the history config, prepending any
// top-level constants onto each resource's column list.
func CompileAll(h config.History) ([]*CompiledResource, error) {
	constants := make([]CompiledColumn, 0, len(h.Constants))
	for i, c := range h.Constants {
		cc, err := compileConstant(c)
		if err != nil {
			return nil, fmt.Errorf("constants[%d] (name=%q): %w", i, c.Name, err)
		}
		constants = append(constants, cc)
	}

	out := make([]*CompiledResource, 0, len(h.Resources))
	for i := range h.Resources {
		cr, err := Compile(h.Resources[i])
		if err != nil {
			return nil, fmt.Errorf("resource %q: %w", h.Resources[i].Kind, err)
		}
		if len(constants) > 0 {
			merged := make([]CompiledColumn, 0, len(constants)+len(cr.Columns))
			merged = append(merged, constants...)
			merged = append(merged, cr.Columns...)
			cr.Columns = merged
		}
		out = append(out, cr)
	}
	return out, nil
}

func compileColumn(c config.HistoryColumn) (CompiledColumn, error) {
	cc := CompiledColumn{Name: c.Name, Type: c.Type, Encode: c.Encode, Index: c.Index}
	if c.IsConstant() {
		v, err := resolveConstant(c.Type, c.Value, c.ValueEnv)
		if err != nil {
			return CompiledColumn{}, err
		}
		cc.IsConstant = true
		cc.Constant = v
		return cc, nil
	}

	primary, err := collector.CompileExtract(c.Extract)
	if err != nil {
		return CompiledColumn{}, err
	}
	cc.Primary = primary
	for i, f := range c.Fallbacks {
		fe, err := collector.CompileExtract(f)
		if err != nil {
			return CompiledColumn{}, fmt.Errorf("fallback[%d]: %w", i, err)
		}
		cc.Fallbacks = append(cc.Fallbacks, fe)
	}
	if c.OnMissing != nil {
		cc.HasOnMissing = true
		cc.OnMissing = *c.OnMissing
	}
	return cc, nil
}

func compileConstant(c config.HistoryConstant) (CompiledColumn, error) {
	v, err := resolveConstant(c.Type, c.Value, c.ValueEnv)
	if err != nil {
		return CompiledColumn{}, err
	}
	return CompiledColumn{
		Name:       c.Name,
		Type:       c.Type,
		Index:      c.Index,
		IsConstant: true,
		Constant:   v,
	}, nil
}

func resolveConstant(typ, value, valueEnv string) (any, error) {
	raw, err := resolveValueSource(value, valueEnv)
	if err != nil {
		return nil, err
	}
	return parseScalar(typ, raw)
}

func resolveValueSource(value, valueEnv string) (string, error) {
	if valueEnv != "" {
		v := os.Getenv(valueEnv)
		if v == "" {
			return "", fmt.Errorf("valueEnv references empty environment variable %q", valueEnv)
		}
		return v, nil
	}
	return value, nil
}

// parseScalar converts a string into the Go value matching a ClickHouse scalar
// column type. Array(String) is rejected by validation before this is called.
func parseScalar(typ, s string) (any, error) {
	switch typ {
	case "String":
		return s, nil
	case "Int64":
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse Int64 %q: %w", s, err)
		}
		return n, nil
	case "UInt64":
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse UInt64 %q: %w", s, err)
		}
		return n, nil
	case "Float64":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("parse Float64 %q: %w", s, err)
		}
		return f, nil
	case "Bool":
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("parse Bool %q: %w", s, err)
		}
		return b, nil
	case "DateTime64(3)":
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, fmt.Errorf("parse DateTime64(3) %q: %w", s, err)
		}
		return t.UTC(), nil
	default:
		return nil, fmt.Errorf("unsupported constant type %q", typ)
	}
}

// TableSchema converts the compiled resource to the store's schema type.
func (cr *CompiledResource) TableSchema() store.TableSchema {
	cols := make([]store.ColumnSchema, len(cr.Columns))
	for i, c := range cr.Columns {
		cols[i] = store.ColumnSchema{Name: c.Name, Type: c.Type, Index: c.Index}
	}
	return store.TableSchema{Table: cr.Table, Columns: cols}
}

// TableSchemas converts a slice of compiled resources.
func TableSchemas(rs []*CompiledResource) []store.TableSchema {
	out := make([]store.TableSchema, len(rs))
	for i, r := range rs {
		out[i] = r.TableSchema()
	}
	return out
}
