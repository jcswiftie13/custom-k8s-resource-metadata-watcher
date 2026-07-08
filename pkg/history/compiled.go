// Package history implements the event-driven ClickHouse ingest path: it
// compiles the history config, evaluates client-side filters, and turns
// informer events into append-only version rows written via pkg/store.
package history

import (
	"fmt"
	"regexp"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

// CompiledColumn is a declared column plus its compiled extraction path.
type CompiledColumn struct {
	Name    string
	Type    string // ClickHouse type
	Encode  string // "", "json", "kv"
	Index   string // "", "bloom_filter"
	Extract collector.CompiledExtract
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

// Compile builds a CompiledResource from config. The config is assumed already
// validated (see config.HistoryResource.validate).
func Compile(r config.HistoryResource) (*CompiledResource, error) {
	cr := &CompiledResource{Kind: r.Kind, Table: r.TableName()}

	for _, c := range r.Columns {
		ce, err := collector.CompileExtract(c.Extract)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}
		cr.Columns = append(cr.Columns, CompiledColumn{
			Name:    c.Name,
			Type:    c.Type,
			Encode:  c.Encode,
			Index:   c.Index,
			Extract: ce,
		})
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

// CompileAll compiles every resource in the history config.
func CompileAll(h config.History) ([]*CompiledResource, error) {
	out := make([]*CompiledResource, 0, len(h.Resources))
	for i := range h.Resources {
		cr, err := Compile(h.Resources[i])
		if err != nil {
			return nil, fmt.Errorf("resource %q: %w", h.Resources[i].Kind, err)
		}
		out = append(out, cr)
	}
	return out, nil
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
