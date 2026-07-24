// Package storeopen opens the ClickHouse config-store backend. It is the single
// place that imports the backend, keeping internal/store a leaf; callers (the
// benchmark harness, ipflow CLI) get a backend-agnostic store.Store back and can
// override the connection string with POC_CH_ADDR.
//
// The PoC targets ClickHouse only — the same engine production uses — so the code
// under poc/ stays portable to production.
package storeopen

import (
	"context"
	"os"

	"github.com/example/metadata-exporter/poc/route2a/internal/chstore"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
)

// defaultAddr is the ClickHouse connection string used when POC_CH_ADDR is unset
// (local single-node container from `make ch-up`).
const defaultAddr = "127.0.0.1:9000"

// Addr returns the ClickHouse connection string: the POC_CH_ADDR override, else
// the local default.
func Addr() string {
	if v := os.Getenv("POC_CH_ADDR"); v != "" {
		return v
	}
	return defaultAddr
}

// Open dials ClickHouse at addr (empty => Addr()) and returns a store.Store.
// POC_CH_UNIQUE_ROWS=1 opens the store in pruned read mode (valid_to predicate
// back in SQL) — only valid when the writer guarantees one row per version
// (exporter closeMode=update; the PoC loader also qualifies, it writes each
// version exactly once). See chstore.WithUniqueRows.
func Open(ctx context.Context, addr string) (store.Store, error) {
	if addr == "" {
		addr = Addr()
	}
	var opts []chstore.Option
	if os.Getenv("POC_CH_UNIQUE_ROWS") == "1" {
		opts = append(opts, chstore.WithUniqueRows())
	}
	return chstore.Open(ctx, addr, opts...)
}
