package store

import (
	"strings"
	"testing"
)

func TestCreateTableSQL(t *testing.T) {
	ts := TableSchema{
		Table: "svc_versions",
		Columns: []ColumnSchema{
			{Name: "cluster_ip", Type: "String"},
			{Name: "selector_kv", Type: "Array(String)", Index: "bloom_filter"},
			{Name: "port", Type: "Int64"},
		},
	}
	got := createTableSQL(ts, false)

	want := `CREATE TABLE IF NOT EXISTS svc_versions (
  namespace LowCardinality(String),
  name String,
  uid String,
  valid_from DateTime64(3),
  valid_to DateTime64(3),
  ingest_seq UInt64,
  cluster_ip String,
  selector_kv Array(String),
  port Int64,
  INDEX idx_selector_kv selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq)
ORDER BY (namespace, name, uid, valid_from)`

	if got != want {
		t.Fatalf("DDL mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestCreateTableSQL_NoIndex(t *testing.T) {
	ts := TableSchema{Table: "t", Columns: []ColumnSchema{{Name: "c", Type: "String"}}}
	got := createTableSQL(ts, false)
	if strings.Contains(got, "INDEX") {
		t.Fatalf("did not expect an INDEX clause:\n%s", got)
	}
	if !strings.HasSuffix(got, "ORDER BY (namespace, name, uid, valid_from)") {
		t.Fatalf("unexpected suffix:\n%s", got)
	}
}

// closeMode=update tables carry the patch-part settings lightweight UPDATE
// needs; rewrite-mode tables must not (no ClickHouse version requirement).
func TestCreateTableSQL_UpdateClosePatchSettings(t *testing.T) {
	ts := TableSchema{Table: "t", Columns: []ColumnSchema{{Name: "c", Type: "String"}}}
	got := createTableSQL(ts, true)
	if !strings.HasSuffix(got, "SETTINGS "+patchPartSettings) {
		t.Fatalf("update-close DDL must end with patch-part SETTINGS:\n%s", got)
	}
	if strings.Contains(createTableSQL(ts, false), "enable_block_number_column") {
		t.Fatalf("rewrite-mode DDL must not carry patch-part settings")
	}
}

// valid_to must never join the sort key: the closing row re-inserts the open
// row with valid_to set, and only an identical sort key lets ReplacingMergeTree
// collapse the two.
func TestCreateTableSQL_ValidToNotInOrderBy(t *testing.T) {
	got := createTableSQL(TableSchema{Table: "t", Columns: []ColumnSchema{{Name: "c", Type: "String"}}}, false)
	if !strings.Contains(got, "valid_to DateTime64(3)") {
		t.Fatalf("valid_to column missing:\n%s", got)
	}
	_, orderBy, ok := strings.Cut(got, "ORDER BY ")
	if !ok {
		t.Fatalf("no ORDER BY clause:\n%s", got)
	}
	if strings.Contains(orderBy, "valid_to") {
		t.Fatalf("valid_to must stay out of ORDER BY, got %q", orderBy)
	}
}

func TestAllColumns_EnvelopeFirst(t *testing.T) {
	cols := allColumns(TableSchema{Table: "t", Columns: []ColumnSchema{{Name: "x", Type: "String"}}})
	if len(cols) != len(envelopeColumns)+1 {
		t.Fatalf("column count = %d", len(cols))
	}
	if cols[0].name != "namespace" || cols[len(cols)-1].name != "x" {
		t.Fatalf("ordering wrong: first=%s last=%s", cols[0].name, cols[len(cols)-1].name)
	}
}
