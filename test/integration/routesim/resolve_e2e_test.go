// Package routesim is the routing-history resolution half of the e2e scenario
// (see test/integration/e2e/history_routing_resolution_test.go for the host
// half). The host-side test provisions the fixtures, lets the real exporter
// write the four *_versions tables, extracts the version timestamps, then runs
// THIS test — compiled with `go test -c` for linux — inside the Envoy tools
// image (which carries the native router_check_tool) on the kind docker
// network, wired up entirely through ROUTESIM_* env vars.
package routesim

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/example/metadata-exporter/test/integration/routesim/internal/chstore"
	"github.com/example/metadata-exporter/test/integration/routesim/internal/matchcheck"
	"github.com/example/metadata-exporter/test/integration/routesim/internal/rangequery"
	"github.com/example/metadata-exporter/test/integration/routesim/internal/translate"
)

// expectation is one expected VersionResolution span, ordered. FromMS/ToMS are
// unix milliseconds (matching the store's DateTime64(3) precision); nil means
// "don't assert this bound".
type expectation struct {
	Gateway string `json:"gateway"`
	Cluster string `json:"cluster"`
	FromMS  *int64 `json:"from_ms"`
	ToMS    *int64 `json:"to_ms"`
}

// TestResolveRoutingHistory runs the full range-resolution pipeline
// (LoadTrafficWindow -> memwindow segments -> in-memory 3-hop -> gwresolve ->
// in-process istiod translate -> native router_check_tool) against the
// exporter-written ClickHouse tables and asserts the per-version outcomes.
//
// Env contract (all set by the host-side e2e test):
//
//	ROUTESIM_CH_ADDR          native ClickHouse addr; UNSET => Skip (bare `go
//	                          test ./...` runs outside the harness). Once set,
//	                          every other problem is Fatal.
//	ROUTESIM_ROUTERCHECK_BIN  native router_check_tool path ("" => PATH lookup);
//	                          missing/not runnable => Fatal, NEVER Skip.
//	ROUTESIM_HOST/PATH/IP     the request to resolve (authority, path, dst IP).
//	ROUTESIM_PORT             listener port (default 80).
//	ROUTESIM_T0_MS/T1_MS      window bounds, unix milliseconds UTC.
//	ROUTESIM_EXPECT           JSON []expectation, ordered.
//	ROUTESIM_UNIQUE_ROWS      "1" => the writer guarantees one row per version
//	                          (exporter closeMode=update): open the store in
//	                          pruned read mode and ASSERT the dedup safety net
//	                          collapsed zero rows.
func TestResolveRoutingHistory(t *testing.T) {
	addr := os.Getenv("ROUTESIM_CH_ADDR")
	if addr == "" {
		t.Skip("ROUTESIM_CH_ADDR not set; this test only runs under the e2e harness")
	}

	// Native router_check_tool is a hard requirement: without the resolution
	// engine the scenario is meaningless, so a missing tool must fail loudly.
	runner, err := matchcheck.New(os.Getenv("ROUTESIM_ROUTERCHECK_BIN"))
	if err != nil {
		t.Fatalf("native router_check_tool required: %v", err)
	}

	host := mustEnv(t, "ROUTESIM_HOST")
	path := mustEnv(t, "ROUTESIM_PATH")
	ip := mustEnv(t, "ROUTESIM_IP")
	port := 80
	if p := os.Getenv("ROUTESIM_PORT"); p != "" {
		port = mustInt(t, "ROUTESIM_PORT", p)
	}
	t0 := time.UnixMilli(int64(mustInt(t, "ROUTESIM_T0_MS", mustEnv(t, "ROUTESIM_T0_MS")))).UTC()
	t1 := time.UnixMilli(int64(mustInt(t, "ROUTESIM_T1_MS", mustEnv(t, "ROUTESIM_T1_MS")))).UTC()

	var expect []expectation
	if err := json.Unmarshal([]byte(mustEnv(t, "ROUTESIM_EXPECT")), &expect); err != nil {
		t.Fatalf("ROUTESIM_EXPECT: %v", err)
	}
	if len(expect) == 0 {
		t.Fatal("ROUTESIM_EXPECT is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	uniqueRows := os.Getenv("ROUTESIM_UNIQUE_ROWS") == "1"
	var opts []chstore.Option
	if uniqueRows {
		opts = append(opts, chstore.WithUniqueRows())
	}
	st, err := chstore.Open(ctx, addr, opts...)
	if err != nil {
		t.Fatalf("open clickhouse %s: %v", addr, err)
	}
	defer st.Close()

	deps := rangequery.Deps{Translator: translate.NewTranslator(), Runner: runner}
	out, m, err := deps.ResolveTimed(ctx, st, host, path, ip, port, t0, t1)
	if err != nil {
		t.Fatalf("range resolve: %v", err)
	}

	t.Logf("query host=%s path=%s ip=%s port=%d window=[%s, %s)", host, path, ip, port, t0, t1)
	t.Logf("metrics: %+v", *m)
	for i, vr := range out {
		t.Logf("span[%d]: [%s, %s) gateway=%q cluster=%q", i, vr.From.UTC(), vr.To.UTC(), vr.Gateway, vr.Cluster)
	}

	if len(out) != len(expect) {
		t.Fatalf("got %d version spans, want %d", len(out), len(expect))
	}
	for i, want := range expect {
		got := out[i]
		if got.Gateway != want.Gateway {
			t.Errorf("span[%d] gateway = %q, want %q", i, got.Gateway, want.Gateway)
		}
		if got.Cluster != want.Cluster {
			t.Errorf("span[%d] cluster = %q, want %q", i, got.Cluster, want.Cluster)
		}
		if want.FromMS != nil && got.From.UnixMilli() != *want.FromMS {
			t.Errorf("span[%d] from = %d ms, want %d ms", i, got.From.UnixMilli(), *want.FromMS)
		}
		if want.ToMS != nil && got.To.UnixMilli() != *want.ToMS {
			t.Errorf("span[%d] to = %d ms, want %d ms", i, got.To.UnixMilli(), *want.ToMS)
		}
	}

	// The scenario's whole point is per-version resolution over a config change:
	// prove the multi-version path actually ran (>=2 segments, and the change
	// forced >=2 distinct configs through translate+router_check_tool).
	if len(expect) >= 2 {
		if m.Segments < 2 {
			t.Errorf("Segments = %d, want >= 2 (window must straddle the version boundary)", m.Segments)
		}
		if m.DistinctCfgs < 2 {
			t.Errorf("DistinctCfgs = %d, want >= 2 (each version must translate+check separately)", m.DistinctCfgs)
		}
	}

	// Pruned mode: the dedup safety net must have had nothing to do — any
	// collapse means duplicate version slots reached the reader, i.e. the
	// writer's one-row-per-version guarantee (closeMode=update + restart
	// recovery) is broken.
	if uniqueRows {
		if n := st.CollapsedRows(); n != 0 {
			t.Errorf("dedup collapsed %d rows under ROUTESIM_UNIQUE_ROWS — writer uniqueness guarantee broken", n)
		}
	}
}

func mustEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s must be set", key)
	}
	return v
}

func mustInt(t *testing.T, key, v string) int {
	t.Helper()
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s = %q: %v", key, v, err)
	}
	return n
}
