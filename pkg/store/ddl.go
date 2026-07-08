package store

import (
	"context"
	"fmt"
	"strings"
)

// envelopeColumn is an implicit column present on every history table.
type envelopeColumn struct {
	name string
	typ  string
}

// envelopeColumns are prepended to every table, in insert order. valid_to is
// intentionally absent — it is derived at query time.
var envelopeColumns = []envelopeColumn{
	{"namespace", "LowCardinality(String)"},
	{"name", "String"},
	{"uid", "String"},
	{"resource_version", "String"},
	{"valid_from", "DateTime64(3)"},
	{"deleted", "UInt8"},
	{"spec_hash", "String"},
	{"ingest_seq", "UInt64"},
}

// allColumns returns envelope + declared columns as (name, type) pairs in
// insert order.
func allColumns(t TableSchema) []envelopeColumn {
	out := make([]envelopeColumn, 0, len(envelopeColumns)+len(t.Columns))
	out = append(out, envelopeColumns...)
	for _, c := range t.Columns {
		out = append(out, envelopeColumn{c.Name, c.Type})
	}
	return out
}

// createTableSQL renders the CREATE TABLE IF NOT EXISTS statement.
func createTableSQL(t TableSchema) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", t.Table)
	for _, c := range allColumns(t) {
		fmt.Fprintf(&b, "  %s %s,\n", c.name, c.typ)
	}
	for _, c := range t.Columns {
		if c.Index == "bloom_filter" {
			fmt.Fprintf(&b, "  INDEX idx_%s %s TYPE bloom_filter GRANULARITY 1,\n", c.Name, c.Name)
		}
	}
	// Trim trailing comma+newline.
	s := strings.TrimRight(b.String(), ",\n")
	// ORDER BY doubles as the ReplacingMergeTree dedup key. resource_version
	// keeps genuinely distinct versions apart (k8s bumps it on every change,
	// and it is stable across a restart re-LIST so the same version dedups),
	// and deleted keeps a tombstone from collapsing into its last live twin
	// when they share a valid_from millisecond.
	s += "\n) ENGINE = ReplacingMergeTree(ingest_seq)\nORDER BY (namespace, name, valid_from, resource_version, deleted)"
	return s
}

func (s *chStore) createTable(ctx context.Context, t TableSchema) error {
	if err := s.conn.Exec(ctx, createTableSQL(t)); err != nil {
		return err
	}
	// Additive migration: add any declared column missing from an existing
	// table. Never drop or retype.
	for _, c := range t.Columns {
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", t.Table, c.Name, c.Type)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("add column %q: %w", c.Name, err)
		}
	}
	return nil
}

// validateTable asserts every configured column (envelope + declared) exists in
// the live table with a matching type. It mutates nothing and fails fast on
// drift.
func (s *chStore) validateTable(ctx context.Context, t TableSchema) error {
	rows, err := s.conn.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ?", t.Table)
	if err != nil {
		return err
	}
	defer rows.Close()

	live := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return err
		}
		live[name] = typ
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(live) == 0 {
		return fmt.Errorf("table does not exist (createSchema is disabled; create it out-of-band or enable createSchema in dev)")
	}
	for _, c := range allColumns(t) {
		got, ok := live[c.name]
		if !ok {
			return fmt.Errorf("column %q is missing (declared type %s)", c.name, c.typ)
		}
		if got != c.typ {
			return fmt.Errorf("column %q type drift: config=%s live=%s", c.name, c.typ, got)
		}
	}
	return nil
}
