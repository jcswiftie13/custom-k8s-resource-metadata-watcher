package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// FarFuture is the valid_to sentinel for an open (still-current) version, so
// overlap predicates need no null handling.
var FarFuture = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)

// envelopeColumn is an implicit column present on every history table.
type envelopeColumn struct {
	name string
	typ  string
}

// envelopeColumns are prepended to every table, in insert order. valid_to is
// materialized at write time: the open version carries FarFuture, and a later
// version (or a delete) re-inserts the prior row with valid_to filled in.
var envelopeColumns = []envelopeColumn{
	{"namespace", "LowCardinality(String)"},
	{"name", "String"},
	{"uid", "String"},
	{"resource_version", "String"},
	{"valid_from", "DateTime64(3)"},
	{"valid_to", "DateTime64(3)"},
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
	// and it is stable across a restart re-LIST so the same version dedups).
	// deleted is retained for schema stability but is always 0: a deletion is
	// recorded by closing valid_to, not by a tombstone row.
	//
	// Invariant: valid_to MUST stay out of ORDER BY. Closing a version re-inserts
	// the same row with valid_to filled in and a higher ingest_seq; if valid_to
	// were part of the key the open row and its closing row would sort apart and
	// both survive the merge, so the close would never take effect.
	s += "\n) ENGINE = ReplacingMergeTree(ingest_seq)\nORDER BY (namespace, name, valid_from, resource_version, deleted)"
	return s
}

func (s *chStore) createTable(ctx context.Context, t TableSchema) error {
	if err := s.conn.Exec(ctx, createTableSQL(t)); err != nil {
		return err
	}
	// Additive migration: add any column — envelope or declared — missing from an
	// existing table, so a table created before valid_to existed picks it up.
	// Never drop or retype.
	for _, c := range allColumns(t) {
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", t.Table, c.name, c.typ)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("add column %q: %w", c.name, err)
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
