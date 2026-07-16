-- Routing-history version tables, pre-seeded for a createSchema=false exporter
-- deployment. The envelope matches the exporter's production write schema
-- (pkg/store/ddl.go): namespace, name, uid, valid_from, valid_to, ingest_seq,
-- ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, uid, valid_from).
-- Only the declared columns the routing reader's queries SELECT are included
-- (test/integration/routesim/internal/chstore/chstore.go).
--
-- Runs from /docker-entrypoint-initdb.d on first start when the data dir is empty.
-- IF NOT EXISTS keeps the script safe if CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS is set.
--
-- Each table carries SETTINGS enable_block_number_column = 1,
-- enable_block_offset_column = 1: the patch-part columns ClickHouse lightweight
-- UPDATE addresses, required by the exporter's closeMode=update (validated fast
-- when createSchema=false). They are harmless under the default rewrite mode.

CREATE TABLE IF NOT EXISTS service_versions (
  namespace   LowCardinality(String),
  name        String,
  uid         String,
  valid_from  DateTime64(3),
  valid_to    DateTime64(3),
  ingest_seq  UInt64,
  cluster     String,
  ingress_ips Array(String),
  selector_kv Array(String),
  spec_json   String,
  INDEX idx_ips ingress_ips TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (cluster, namespace, name, valid_from)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE IF NOT EXISTS deploy_versions (
  namespace     LowCardinality(String),
  name          String,
  uid           String,
  valid_from    DateTime64(3),
  valid_to      DateTime64(3),
  ingest_seq    UInt64,
  cluster       String,
  pod_labels_kv Array(String),
  INDEX idx_pod pod_labels_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (cluster, namespace, name, valid_from)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE IF NOT EXISTS gw_versions (
  namespace    LowCardinality(String),
  name         String,
  uid          String,
  valid_from   DateTime64(3),
  valid_to     DateTime64(3),
  ingest_seq   UInt64,
  cluster      String,
  selector_kv  Array(String),
  server_hosts Array(String),
  spec_json    String,
  INDEX idx_sel selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (cluster, namespace, name, valid_from)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE IF NOT EXISTS vs_versions (
  namespace      LowCardinality(String),
  name           String,
  uid            String,
  valid_from     DateTime64(3),
  valid_to       DateTime64(3),
  ingest_seq     UInt64,
  cluster        String,
  bound_gateways Array(String),
  spec_json      String,
  INDEX idx_bg bound_gateways TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (cluster, namespace, name, valid_from)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
