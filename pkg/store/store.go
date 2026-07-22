// Package store implements the append-only ClickHouse version store for the
// history ingest path. Each informer event appends one or two Rows: the new
// open version (valid_to = FarFuture) and, when it supersedes an earlier
// version, that earlier row re-inserted with valid_to materialized and a higher
// ingest_seq. ReplacingMergeTree(ingest_seq) collapses the pair, so valid_to is
// stored rather than derived at query time.
//
// Schema ownership: the exporter config is the source of truth for columns and
// types. EnsureSchema either creates tables (createSchema=true, dev) or only
// validates the live schema against config and fails fast on drift
// (createSchema=false, prod-safe). It never drops or retypes columns.
package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ColumnSchema is one declared (non-envelope) ClickHouse column.
type ColumnSchema struct {
	Name  string
	Type  string // ClickHouse type, e.g. "String", "Array(String)"
	Index string // "" or "bloom_filter"
}

// TableSchema describes one history table's declared columns. Envelope columns
// (see envelopeColumns) are implicit and prepended by the store.
type TableSchema struct {
	Table   string
	Columns []ColumnSchema
}

// Row is one version record. Envelope fields are explicit; Values holds every
// declared column value keyed by column name. Callers MUST populate Values for
// every declared column with a Go value matching its ClickHouse type
// (string, []string, int64, uint64, float64, bool, time.Time).
type Row struct {
	Namespace string
	Name      string
	UID       string
	ValidFrom time.Time
	ValidTo   time.Time
	IngestSeq uint64
	Values    map[string]any
}

// OpenVersion is one open (valid_to = FarFuture) row as seen by recovery: the
// identity, ordering keys, and declared column Values the ingester needs to
// rebuild its last-state — including recomputing the dedup hash — after a
// restart.
type OpenVersion struct {
	Namespace string
	Name      string
	UID       string
	ValidFrom time.Time
	IngestSeq uint64
	Values    map[string]any
}

// Store is the write surface used by the ingest path.
type Store interface {
	EnsureSchema(ctx context.Context, tables []TableSchema) error
	WriteBatch(ctx context.Context, table string, rows []Row) error
	// CloseVersion closes one open version in place with a lightweight UPDATE
	// (closeMode=update only): it sets valid_to = closeAt on the row matching
	// (namespace, name, uid, validFrom) that still carries the FarFuture
	// sentinel. Matching zero rows is not an error — the close is idempotent
	// by construction (a replay finds no sentinel row left to patch).
	// Requires ClickHouse >= 25.8 with the block-number table settings (see
	// EnsureSchema / docs/lightweight-update-upgrade-plan.md).
	CloseVersion(ctx context.Context, table, namespace, name, uid string, validFrom, closeAt time.Time) error
	// OpenVersions lists every row still carrying the FarFuture sentinel, for
	// the ingester's restart recovery. Raw rows: pre-migration duplicates
	// (same uid+valid_from, different ingest_seq) are NOT collapsed here — the
	// caller dedups.
	OpenVersions(ctx context.Context, table string) ([]OpenVersion, error)
	// MaxIngestSeq returns the largest ingest_seq above floor across ALL rows
	// of table — open AND closed, since a closing row can hold the maximum —
	// or 0 when no row exceeds floor. The ingester calls it once per recovery
	// with its time-derived counter base as floor: a non-zero result means
	// rows from an earlier process outrank the base (clock stepped backwards)
	// and the counter must be re-seeded above them, or post-restart rows
	// would lose ReplacingMergeTree(ingest_seq) merges to stale ones.
	MaxIngestSeq(ctx context.Context, table string, floor uint64) (uint64, error)
	// Ping verifies the connection is still usable. The history manager polls
	// it to drive the connected/degraded state it reports on /readyz.
	Ping(ctx context.Context) error
	Close() error
}

// Scope restricts a writer's restart-recovery reads (OpenVersions,
// MaxIngestSeq) — and, defensively, its CloseVersion patches — to rows it
// wrote itself, identified by a declared constant column equal to Value.
// Required when multiple writers share one table (one exporter per cluster):
// without it, one writer's orphan reconciliation closes every other writer's
// live rows. The zero value disables filtering (single-writer deployments).
type Scope struct {
	Column string
	Value  string
}

// chStore is the ClickHouse-backed Store.
type chStore struct {
	conn         driver.Conn
	createSchema bool
	updateClose  bool                   // closeMode=update: DDL adds patch-part table settings
	scope        Scope                  // zero value = no recovery-read filtering
	schemas      map[string]TableSchema // table name -> schema, set by EnsureSchema
}

// hasColumn reports whether the table declares the named column.
func hasColumn(t TableSchema, name string) bool {
	for _, c := range t.Columns {
		if c.Name == name {
			return true
		}
	}
	return false
}

// Options configures the ClickHouse connection, including authentication. DSN
// is the base connection string — native (`clickhouse://host:9000/db`) or HTTP
// (`http://host:8123/db`, `https://host/db`). The remaining fields, when set,
// override any credentials embedded in it. Token (JWT / access token) replaces
// basic auth when non-empty.
type Options struct {
	DSN           string
	Username      string
	Password      string
	Database      string
	Token         string
	Secure        bool
	TLSSkipVerify bool
	CreateSchema  bool
	// UpdateClose (closeMode=update) makes EnsureSchema add/require the
	// enable_block_number_column / enable_block_offset_column table settings
	// that ClickHouse lightweight UPDATEs need.
	UpdateClose bool
	// Scope, when non-zero, filters recovery reads to this writer's own rows
	// (see Scope). The column must be declared on every table.
	Scope Scope
}

// buildClickHouseOptions resolves the DSN and applies auth/TLS overrides. It
// performs no I/O so it can be unit-tested without a live ClickHouse.
func buildClickHouseOptions(o Options) (*clickhouse.Options, error) {
	dsn, err := prepareDSN(o.DSN, o.Secure)
	if err != nil {
		return nil, err
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if o.Token != "" {
		// JWT replaces basic auth in clickhouse-go; do not also set user/pass.
		token := o.Token
		opts.GetJWT = func(context.Context) (string, error) { return token, nil }
	} else {
		if o.Username != "" {
			opts.Auth.Username = o.Username
		}
		if o.Password != "" {
			opts.Auth.Password = o.Password
		}
	}
	if o.Database != "" {
		opts.Auth.Database = o.Database
	}
	if o.Secure {
		if opts.TLS == nil {
			opts.TLS = &tls.Config{}
		}
		opts.TLS.InsecureSkipVerify = o.TLSSkipVerify
	}
	// HTTP batch inserts benefit from ClickHouse native block compression
	// (lz4 over the HTTP body). Leave an explicit DSN compress= setting alone.
	if opts.Protocol == clickhouse.HTTP && opts.Compression == nil {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}
	return opts, nil
}

// prepareDSN makes config Secure compose with clickhouse-go's ParseDSN rules:
// an https:// DSN is rejected unless ?secure=true is present at parse time, so
// we inject it when Secure is set. http:// + Secure keeps the scheme (TLS is
// applied after parse via Options.TLS) — clickhouse-go forbids http+secure in
// the query string.
func prepareDSN(dsn string, secure bool) (string, error) {
	if !secure {
		return dsn, nil
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "https", "clickhouse":
		q := u.Query()
		if q.Get("secure") == "" {
			q.Set("secure", "true")
			u.RawQuery = q.Encode()
		}
		return u.String(), nil
	default:
		return dsn, nil
	}
}

// Open connects to ClickHouse using the given Options.
func Open(ctx context.Context, o Options) (Store, error) {
	opts, err := buildClickHouseOptions(o)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &chStore{conn: conn, createSchema: o.CreateSchema, updateClose: o.UpdateClose, scope: o.Scope}, nil
}

func (s *chStore) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

func (s *chStore) Close() error { return s.conn.Close() }

// EnsureSchema creates or validates every table depending on createSchema. It
// also records each schema so WriteBatch knows the declared column order.
func (s *chStore) EnsureSchema(ctx context.Context, tables []TableSchema) error {
	s.schemas = make(map[string]TableSchema, len(tables))
	for _, t := range tables {
		if s.scope.Column != "" && !hasColumn(t, s.scope.Column) {
			return fmt.Errorf("table %q: scope column %q is not declared (scopeColumn must be a constant present in every table)", t.Table, s.scope.Column)
		}
		s.schemas[t.Table] = t
		if s.createSchema {
			if err := s.createTable(ctx, t); err != nil {
				return fmt.Errorf("create table %q: %w", t.Table, err)
			}
		} else {
			if err := s.validateTable(ctx, t); err != nil {
				return fmt.Errorf("validate table %q: %w", t.Table, err)
			}
		}
	}
	return nil
}
