// Package storeopen is the factory that dials one config-store backend by name.
// It is the single place that imports every backend, keeping internal/store a
// leaf; callers (the benchmark harness, ipflow CLI) select a backend with the
// POC_DB env var and get a backend-agnostic store.Store back.
package storeopen

import (
	"context"
	"fmt"
	"os"

	"github.com/example/metadata-exporter/poc/route2a/internal/chstore"
	"github.com/example/metadata-exporter/poc/route2a/internal/mariastore"
	"github.com/example/metadata-exporter/poc/route2a/internal/pgstore"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
)

// defaultAddr is the per-backend connection string used when the matching env var
// is unset (local single-node containers from `make <db>-up`).
var defaultAddr = map[store.Backend]string{
	store.BackendClickHouse: "127.0.0.1:9000",
	store.BackendPostgres:   "postgres://postgres:postgres@127.0.0.1:5432/route2a?sslmode=disable",
	store.BackendMariaDB:    "root:root@tcp(127.0.0.1:3306)/route2a?parseTime=true&loc=UTC",
}

// addrEnv names the env var that overrides each backend's connection string.
var addrEnv = map[store.Backend]string{
	store.BackendClickHouse: "POC_CH_ADDR",
	store.BackendPostgres:   "POC_PG_ADDR",
	store.BackendMariaDB:    "POC_MARIA_ADDR",
}

// Backend returns the backend selected by POC_DB (default clickhouse). The
// returned error names the valid choices when POC_DB is unrecognized.
func Backend() (store.Backend, error) {
	v := os.Getenv("POC_DB")
	if v == "" {
		return store.BackendClickHouse, nil
	}
	switch store.Backend(v) {
	case store.BackendClickHouse, store.BackendPostgres, store.BackendMariaDB:
		return store.Backend(v), nil
	default:
		return "", fmt.Errorf("unknown POC_DB %q (want clickhouse|postgres|mariadb)", v)
	}
}

// Addr returns the connection string for a backend: its POC_<DB>_ADDR override,
// else the local default.
func Addr(b store.Backend) string {
	if env := addrEnv[b]; env != "" {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	return defaultAddr[b]
}

// Open dials the given backend at addr and returns a backend-agnostic Store.
func Open(ctx context.Context, b store.Backend, addr string) (store.Store, error) {
	switch b {
	case store.BackendClickHouse:
		return chstore.Open(ctx, addr)
	case store.BackendPostgres:
		return pgstore.Open(ctx, addr)
	case store.BackendMariaDB:
		return mariastore.Open(ctx, addr)
	default:
		return nil, fmt.Errorf("unknown backend %q", b)
	}
}
