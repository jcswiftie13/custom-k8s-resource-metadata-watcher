-- POC route2a version tables (poc/route2a/internal/chstore/chstore.go).
-- Runs from /docker-entrypoint-initdb.d on first start when the data dir is empty.
-- IF NOT EXISTS keeps the script safe if CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS is set.

CREATE TABLE IF NOT EXISTS service_versions (
  namespace   LowCardinality(String),
  name        String,
  valid_from  DateTime64(3),
  valid_to    DateTime64(3),
  rev         UInt32,
  ingress_ips Array(String),
  selector_kv Array(String),
  spec_json   String,
  ingest_seq  UInt64,
  INDEX idx_ips ingress_ips TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);

CREATE TABLE IF NOT EXISTS deploy_versions (
  namespace     LowCardinality(String),
  name          String,
  valid_from    DateTime64(3),
  valid_to      DateTime64(3),
  rev           UInt32,
  pod_labels_kv Array(String),
  ingest_seq    UInt64,
  INDEX idx_pod pod_labels_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);

CREATE TABLE IF NOT EXISTS gw_versions (
  namespace    LowCardinality(String),
  name         String,
  valid_from   DateTime64(3),
  valid_to     DateTime64(3),
  rev          UInt32,
  selector_kv  Array(String),
  server_hosts Array(String),
  spec_json    String,
  ingest_seq   UInt64,
  INDEX idx_sel selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);

CREATE TABLE IF NOT EXISTS vs_versions (
  namespace      LowCardinality(String),
  name           String,
  valid_from     DateTime64(3),
  valid_to       DateTime64(3),
  rev            UInt32,
  bound_gateways Array(String),
  spec_json      String,
  ingest_seq     UInt64,
  INDEX idx_bg bound_gateways TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);
