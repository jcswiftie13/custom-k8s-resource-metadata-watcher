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
	{"valid_from", "DateTime64(3)"},
	{"valid_to", "DateTime64(3)"},
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

// patchPartSettings are the MergeTree table settings ClickHouse lightweight
// UPDATEs require (they materialize the _block_number/_block_offset columns
// patch parts are addressed by). Only applied under closeMode=update.
const patchPartSettings = "enable_block_number_column = 1, enable_block_offset_column = 1"

// createTableSQL renders the CREATE TABLE IF NOT EXISTS statement.
func createTableSQL(t TableSchema, updateClose bool) string {
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
	// ORDER BY doubles as the ReplacingMergeTree dedup key. uid is in the key so
	// a same-name delete/re-create (a fresh uid) forms its own version timeline
	// instead of colliding with the old object's. valid_from distinguishes an
	// object's successive versions; the ingester guarantees it is strictly
	// increasing per uid (bumping by 1ms on a same-millisecond change), which is
	// what lets us key on it without resource_version.
	//
	// Invariant: valid_to MUST stay out of ORDER BY. Closing a version re-inserts
	// the same row with valid_to filled in and a higher ingest_seq; if valid_to
	// were part of the key the open row and its closing row would sort apart and
	// both survive the merge, so the close would never take effect.
	s += "\n) ENGINE = ReplacingMergeTree(ingest_seq)\nORDER BY (namespace, name, uid, valid_from)"
	if updateClose {
		s += "\nSETTINGS " + patchPartSettings
	}
	return s
}

func (s *chStore) createTable(ctx context.Context, t TableSchema) error {
	if err := s.conn.Exec(ctx, createTableSQL(t, s.updateClose)); err != nil {
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
	// Additive migration for closeMode=update: a table created before the mode
	// switch lacks the patch-part settings lightweight UPDATE needs. MODIFY
	// SETTING is idempotent and (verified on 25.8) makes pre-existing parts
	// updatable too.
	if s.updateClose {
		stmt := fmt.Sprintf("ALTER TABLE %s MODIFY SETTING %s", t.Table, patchPartSettings)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("enable patch-part settings: %w", err)
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
	if s.updateClose {
		if err := s.validatePatchPartSettings(ctx, t.Table); err != nil {
			return err
		}
	}
	return nil
}

// validatePatchPartSettings fails fast when closeMode=update but the live
// table lacks the settings lightweight UPDATE needs — otherwise the first
// close would fail at runtime with a less actionable ClickHouse error.
func (s *chStore) validatePatchPartSettings(ctx context.Context, table string) error {
	var engineFull string
	if err := s.conn.QueryRow(ctx,
		"SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = ?",
		table).Scan(&engineFull); err != nil {
		return fmt.Errorf("read engine_full: %w", err)
	}
	for _, setting := range []string{"enable_block_number_column = 1", "enable_block_offset_column = 1"} {
		if !strings.Contains(engineFull, setting) {
			return fmt.Errorf("closeMode=update requires table setting %s; run: ALTER TABLE %s MODIFY SETTING %s",
				setting, table, patchPartSettings)
		}
	}
	return nil
}
