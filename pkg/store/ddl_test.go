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
	got := createTableSQL(ts)

	want := `CREATE TABLE IF NOT EXISTS svc_versions (
  namespace LowCardinality(String),
  name String,
  uid String,
  resource_version String,
  valid_from DateTime64(3),
  deleted UInt8,
  spec_hash String,
  ingest_seq UInt64,
  cluster_ip String,
  selector_kv Array(String),
  port Int64,
  INDEX idx_selector_kv selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq)
ORDER BY (namespace, name, valid_from, resource_version, deleted)`

	if got != want {
		t.Fatalf("DDL mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestCreateTableSQL_NoIndex(t *testing.T) {
	ts := TableSchema{Table: "t", Columns: []ColumnSchema{{Name: "c", Type: "String"}}}
	got := createTableSQL(ts)
	if strings.Contains(got, "INDEX") {
		t.Fatalf("did not expect an INDEX clause:\n%s", got)
	}
	if !strings.HasSuffix(got, "ORDER BY (namespace, name, valid_from, resource_version, deleted)") {
		t.Fatalf("unexpected suffix:\n%s", got)
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
