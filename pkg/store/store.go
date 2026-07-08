// Package store implements the append-only ClickHouse version store for the
// history ingest path. Each informer event appends one Row; valid_to is not
// stored (it is derived at query time as the next version's valid_from).
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
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	ValidFrom       time.Time
	Deleted         bool
	SpecHash        string
	IngestSeq       uint64
	Values          map[string]any
}

// Store is the write surface used by the ingest path.
type Store interface {
	EnsureSchema(ctx context.Context, tables []TableSchema) error
	WriteBatch(ctx context.Context, table string, rows []Row) error
	Close() error
}

// chStore is the ClickHouse-backed Store.
type chStore struct {
	conn         driver.Conn
	createSchema bool
	schemas      map[string]TableSchema // table name -> schema, set by EnsureSchema
}

// Options configures the ClickHouse connection, including authentication. DSN
// is the base clickhouse:// connection string; the remaining fields, when set,
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
}

// buildClickHouseOptions resolves the DSN and applies auth/TLS overrides. It
// performs no I/O so it can be unit-tested without a live ClickHouse.
func buildClickHouseOptions(o Options) (*clickhouse.Options, error) {
	opts, err := clickhouse.ParseDSN(o.DSN)
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
	return opts, nil
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
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &chStore{conn: conn, createSchema: o.CreateSchema}, nil
}

func (s *chStore) Close() error { return s.conn.Close() }

// EnsureSchema creates or validates every table depending on createSchema. It
// also records each schema so WriteBatch knows the declared column order.
func (s *chStore) EnsureSchema(ctx context.Context, tables []TableSchema) error {
	s.schemas = make(map[string]TableSchema, len(tables))
	for _, t := range tables {
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
