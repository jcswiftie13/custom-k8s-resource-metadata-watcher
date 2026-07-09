package route2a

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
	"github.com/example/metadata-exporter/poc/route2a/internal/ingload"
	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/rccache"
	"github.com/example/metadata-exporter/poc/route2a/internal/report"
	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
	"github.com/example/metadata-exporter/poc/route2a/internal/simulate"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
	"github.com/example/metadata-exporter/poc/route2a/internal/storeopen"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// benchScale is the corpus size (default full: 600 gateways x 100 VS = 60k VS).
// Override with POC_GATEWAYS / POC_VS to ramp 10->100->600.
func benchScale() scalegen.Config {
	return scalegen.Config{NumGateways: envInt("POC_GATEWAYS", 600), VSPerGW: envInt("POC_VS", 100)}
}

// requireRouterCheck returns the resolution runner (native binary preferred,
// docker fallback) or skips when neither is available.
func requireRouterCheck(t *testing.T) matchcheck.Runner {
	t.Helper()
	r, kind, ok := matchcheck.Detect()
	if !ok {
		t.Skip("router_check_tool unavailable: set POC_ROUTERCHECK_BIN, or start docker and pull the tools image")
	}
	t.Logf("router_check_tool runner: %s", kind)
	return r
}

// buildEngine wires a ClickHouse-backed simulate.Engine over the corpus with the
// given cache mode: the request IP resolves candidate gateways via the ClickHouse
// 3-hop (Lookup), gwresolve disambiguates among them, and the translate stage
// pulls its scoped config from ClickHouse too (chScopedForBench). The whole chain
// therefore runs off the store, at a fixed query time `now`.
func buildEngine(ctx context.Context, g *scalegen.Gen, mode rccache.Mode, runner matchcheck.Runner, st store.Store, now time.Time) *simulate.Engine {
	gws := g.Gateways()
	rgws := make([]gwresolve.Gateway, len(gws))
	deps := rccache.NewDepIndex()
	for i, gw := range gws {
		rgws[i] = gwresolve.Gateway{Name: gw.Name, Hosts: gw.Hosts}
		deps.Own(gw.Name, gw.Name) // a gateway's own CR is a dependency of its RC
	}
	lookup := func(host string) ([]string, error) {
		ip, ok := g.IPForHost(host)
		if !ok {
			return nil, nil // broad / unknown host has no traffic IP -> no candidates -> miss
		}
		cands, err := st.ResolveIPToGateways(ctx, ip, now)
		if err != nil {
			return nil, err
		}
		return candNames(cands), nil
	}
	return simulate.New(simulate.Config{
		Resolver:   gwresolve.New(rgws),
		Cache:      rccache.New(mode, deps),
		Translator: translate.NewTranslator(),
		ScopedFor:  chScopedForBench(ctx, st, now),
		Lookup:     lookup,
		Runner:     runner,
	})
}

// chScopedForBench adapts the store's ScopedFor to simulate.ScopedSource at a
// fixed time, so the translate stage's config comes from the store.
func chScopedForBench(ctx context.Context, st store.Store, now time.Time) simulate.ScopedSource {
	return func(gw string) (translate.ScopedInput, bool) {
		in, ok, err := st.ScopedFor(ctx, gw, now)
		if err != nil {
			log.Printf("scopedFor %s: %v", gw, err)
			return translate.ScopedInput{}, false
		}
		return in, ok
	}
}

// requireStore opens the store for the backend selected by POC_DB (default
// clickhouse) or skips when it is unreachable (same "skip when a dependency is
// absent" style as requireRouterCheck).
func requireStore(ctx context.Context, t *testing.T) store.Store {
	t.Helper()
	backend, err := storeopen.Backend()
	if err != nil {
		t.Fatal(err)
	}
	addr := storeopen.Addr(backend)
	st, err := storeopen.Open(ctx, backend, addr)
	if err != nil {
		t.Skipf("%s not reachable at %s (%v); start it with `make %s-up`", backend, addr, err, dbUpTarget(backend))
	}
	return st
}

// dbUpTarget maps a backend to its `make <x>-up` container target (for skip hints).
func dbUpTarget(b store.Backend) string {
	switch b {
	case store.BackendPostgres:
		return "pg"
	case store.BackendMariaDB:
		return "maria"
	default:
		return "ch"
	}
}

// benchVersions is the bitemporal depth the benchmark loads into ClickHouse.
// Single version by default (fastest load; the benchmark only ever queries "now"),
// overridable per resource type with POC_VER_*.
func benchVersions() ingload.Versions {
	return ingload.Versions{
		Deploy: envInt("POC_VER_DEPLOY", 1),
		Svc:    envInt("POC_VER_SVC", 1),
		Gw:     envInt("POC_VER_GW", 1),
		VS:     envInt("POC_VER_VS", 10),
		KSvc:   envInt("POC_VER_KSVC", 10),
	}
}

// ensureLoaded makes sure ClickHouse holds the current benchmark corpus before
// timing begins. It compares gw_versions' row count against the expected
// NumGateways × Gw-versions and only regenerates+loads on mismatch, so re-running
// a benchmark against an already-loaded store skips the (untimed) load.
func ensureLoaded(ctx context.Context, t *testing.T, st store.Store, g *scalegen.Gen, vers ingload.Versions) {
	t.Helper()
	want := uint64(g.NumGateways() * vers.Gw)
	if got, err := st.CountRows(ctx, "gw_versions"); err == nil && got == want {
		t.Logf("store already holds this corpus (gw_versions=%d); skipping load", got)
		return
	}
	t.Logf("loading corpus into store: %d gateways, versions %+v ...", g.NumGateways(), vers)
	start := time.Now()
	if err := ingload.Load(ctx, st, g, vers, nil); err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	t.Logf("corpus loaded in %s (not counted in report Wall)", time.Since(start).Round(time.Millisecond))
}

func toQueries(cases []scalegen.Case) []simulate.Query {
	qs := make([]simulate.Query, len(cases))
	for i, c := range cases {
		qs[i] = simulate.Query{Host: c.Host, Path: c.Path}
	}
	return qs
}

// checkOracle counts resolutions whose cluster disagrees with the by-construction
// oracle, returning the count and up to 10 sample descriptions.
func checkOracle(cases []scalegen.Case, res []simulate.Resolution) (int, []string) {
	mismatches := 0
	var samples []string
	for i, c := range cases {
		if res[i].Cluster != c.Expected {
			mismatches++
			if len(samples) < 10 {
				samples = append(samples, fmt.Sprintf("host=%s path=%s gw=%s got=%q want=%q",
					c.Host, c.Path, res[i].Gateway, res[i].Cluster, c.Expected))
			}
		}
	}
	return mismatches, samples
}

// batchNote warns that batch/bulk results amortize router_check_tool's
// per-invocation startup over a whole gateway's queries — good for offline route
// simulation over a large corpus, but NOT the single-request latency. For the
// worst-case online cost see TestResolveSingleWorst.
const batchNote = "BULK/BATCH throughput: one router_check_tool invocation per gateway amortizes tool startup over that gateway's queries. This is the offline route-simulation number, NOT single-request latency — for worst-case online cost run TestResolveSingleWorst."

// modeLabel prefixes a benchmark mode name with the active backend (POC_DB,
// default clickhouse) so out/report.md rows from different DBs are comparable.
func modeLabel(mode string) string {
	db := os.Getenv("POC_DB")
	if db == "" {
		db = string(store.BackendClickHouse)
	}
	return "[" + db + "] " + mode
}

// buildResult folds engine metrics into a report.Result.
func buildResult(modeName string, cases []scalegen.Case, res []simulate.Resolution, m *simulate.Metrics, wall time.Duration) *report.Result {
	mismatches, samples := checkOracle(cases, res)
	return &report.Result{
		Mode:            modeName,
		Queries:         len(cases),
		Mismatches:      mismatches,
		MismatchSamples: samples,
		CacheHits:       m.CacheHits,
		CacheMisses:     m.CacheMisses,
		Wall:            wall,
		Stages:          m.Stages,
		PeakRSSKB:       peakRSSKB(),
		Notes:           batchNote,
	}
}

// appendReport writes the result to out/report.md (append) and logs it.
func appendReport(t *testing.T, res *report.Result) {
	t.Helper()
	if err := os.MkdirAll("out", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join("out", "report.md"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "\n%s\n", res.Markdown())
	t.Logf("\n%s", res.Markdown())
}

// prewarm populates the cache by resolving the corpus once (results discarded),
// so a subsequent measured run sees WARM cache hits.
func prewarm(ctx context.Context, t *testing.T, eng *simulate.Engine, queries []simulate.Query) {
	t.Helper()
	if _, _, err := eng.ResolveAll(ctx, queries); err != nil {
		t.Fatalf("prewarm: %v", err)
	}
}

// peakRSSKB reads VmHWM (peak resident set) from /proc/self/status. Returns 0 on
// non-Linux (e.g. macOS), where the metric is unavailable.
func peakRSSKB() int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range bytes.Split(b, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("VmHWM:")) {
			var kb int64
			fmt.Sscanf(string(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("VmHWM:")))), "%d", &kb)
			return kb
		}
	}
	return 0
}

// logResolutions writes one line per query — input host+path and the resolved
// destination cluster (service) — when POC_LOG_QUERIES is set. Off by default
// because the full corpus is 60k+ lines; enable it (ideally at a small scale):
//
//	POC_LOG_QUERIES=1 POC_GATEWAYS=10 POC_VS=10 make bench-warm
func logResolutions(t *testing.T, res []simulate.Resolution) {
	t.Helper()
	if os.Getenv("POC_LOG_QUERIES") == "" {
		return
	}
	for _, r := range res {
		cluster := r.Cluster
		if cluster == "" {
			cluster = "<no route / miss>"
		}
		t.Logf("host=%s path=%s -> cluster=%s (gateway=%s)", r.Host, r.Path, cluster, r.Gateway)
	}
}

// TestResolveWarm runs the full corpus in WARM mode (dependency-epoch cache, the
// production steady state): the cache is pre-warmed, so the measured pass pays
// only router_check_tool resolution cost. Asserts 0 mismatches vs the oracle.
func TestResolveWarm(t *testing.T) {
	runner := requireRouterCheck(t)
	g := scalegen.New(benchScale())
	cases := g.Cases(1)
	queries := toQueries(cases)
	t.Logf("WARM: %d queries over %d gateways", len(cases), benchScale().NumGateways)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	st := requireStore(ctx, t)
	defer st.Close()
	ensureLoaded(ctx, t, st, g, benchVersions())
	now := time.Now().UTC()

	eng := buildEngine(ctx, g, rccache.WarmLazy, runner, st, now)
	prewarm(ctx, t, eng, queries)

	start := time.Now()
	res, m, err := eng.ResolveAll(ctx, queries)
	if err != nil {
		t.Fatalf("ResolveAll: %v", err)
	}
	logResolutions(t, res)
	result := buildResult(modeLabel("WARM (cache, steady state)"), cases, res, m, time.Since(start))
	appendReport(t, result)
	if result.Mismatches != 0 {
		t.Fatalf("%d mismatches (want 0): %v", result.Mismatches, result.MismatchSamples)
	}
}

// TestResolveSingleWorst is the WORST-CASE, single-request benchmark: each
// host+path runs the FULL pipeline on its own (batch size 1, cold cache) —
// resolve gateway + translate that gateway's RC + a dedicated router_check_tool
// invocation for that one query. This is the honest online-serving latency a
// caller sees when it hands the engine exactly one request; the batch tests
// amortize the tool's per-invocation startup over ~100 queries/gateway and thus
// UNDER-report single-request cost. Sampled (POC_WORST_SAMPLE) because each query
// pays a full tool spawn (~0.2s on docker), so this is O(sample) spawns.
func TestResolveSingleWorst(t *testing.T) {
	runner := requireRouterCheck(t)
	g := scalegen.New(benchScale())
	cases := g.Cases(4)
	if sample := envInt("POC_WORST_SAMPLE", 200); sample < len(cases) {
		cases = cases[:sample]
	}
	t.Logf("SINGLE-WORST: %d single-query full-pipeline runs", len(cases))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	st := requireStore(ctx, t)
	defer st.Close()
	ensureLoaded(ctx, t, st, g, benchVersions())
	now := time.Now().UTC()

	// ColdAlways => every query re-fetches (ScopedFor) and re-translates its RC (true
	// worst case). Get always misses, so a single shared engine is fine.
	eng := buildEngine(ctx, g, rccache.ColdAlways, runner, st, now)

	agg := &report.Result{Mode: modeLabel("SINGLE-WORST (per-request, cold, batch=1)"), Queries: len(cases)}
	agg.Notes = "Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost."

	start := time.Now()
	for _, c := range cases {
		q := simulate.Query{Host: c.Host, Path: c.Path}
		t0 := time.Now()
		res, m, err := eng.ResolveAll(ctx, []simulate.Query{q})
		perQuery := time.Since(t0)
		if err != nil {
			t.Fatalf("ResolveAll single: %v", err)
		}
		logResolutions(t, res)
		agg.Stages.Total.Add(perQuery)
		agg.Stages.Lookup.Add(m.Stages.Lookup.Mean())
		agg.Stages.Resolve.Add(m.Stages.Resolve.Mean())
		if m.CacheMisses > 0 {
			agg.Stages.ScopedFetch.Add(m.Stages.ScopedFetch.Mean())
			agg.Stages.Translate.Add(m.Stages.Translate.Mean())
		}
		agg.Stages.Check.Add(m.Stages.Check.Mean())
		agg.CacheHits += m.CacheHits
		agg.CacheMisses += m.CacheMisses
		if res[0].Cluster != c.Expected {
			agg.Mismatches++
			if len(agg.MismatchSamples) < 10 {
				agg.MismatchSamples = append(agg.MismatchSamples,
					fmt.Sprintf("host=%s path=%s got=%q want=%q", c.Host, c.Path, res[0].Cluster, c.Expected))
			}
		}
	}
	agg.Wall = time.Since(start)
	agg.PeakRSSKB = peakRSSKB()
	appendReport(t, agg)
	if agg.Mismatches != 0 {
		t.Fatalf("%d mismatches (want 0): %v", agg.Mismatches, agg.MismatchSamples)
	}
}
