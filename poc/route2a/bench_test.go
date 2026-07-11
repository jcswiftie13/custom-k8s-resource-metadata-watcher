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
	"github.com/example/metadata-exporter/poc/route2a/internal/rangequery"
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

// requireStore opens the ClickHouse store or skips when it is unreachable (same
// "skip when a dependency is absent" style as requireRouterCheck).
func requireStore(ctx context.Context, t *testing.T) store.Store {
	t.Helper()
	addr := storeopen.Addr()
	st, err := storeopen.Open(ctx, addr)
	if err != nil {
		t.Skipf("clickhouse not reachable at %s (%v); start it with `make ch-up`", addr, err)
	}
	return st
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

// benchRange is the [t0,t1) the interval benchmark resolves over: the whole
// version timeline by default (store.BenchWindow), overridable via
// POC_BENCH_FROM / POC_BENCH_TO (RFC3339).
func benchRange(t *testing.T) (time.Time, time.Time) {
	t.Helper()
	t0, t1 := store.BenchWindow()
	if v := os.Getenv("POC_BENCH_FROM"); v != "" {
		p, err := time.Parse(time.RFC3339, v)
		if err != nil {
			t.Fatalf("POC_BENCH_FROM: %v", err)
		}
		t0 = p.UTC()
	}
	if v := os.Getenv("POC_BENCH_TO"); v != "" {
		p, err := time.Parse(time.RFC3339, v)
		if err != nil {
			t.Fatalf("POC_BENCH_TO: %v", err)
		}
		t1 = p.UTC()
	}
	return t0, t1
}

// rangeMismatch checks a range result against the oracle. The corpus writes an
// identical spec to every version, so the destination is constant across the
// window: every returned span must resolve to the expected cluster. Returns "" on
// agreement, else a sample description.
func rangeMismatch(versions []rangequery.VersionResolution, c scalegen.Case) string {
	if len(versions) == 0 {
		return fmt.Sprintf("host=%s path=%s got=<empty range> want=%q", c.Host, c.Path, c.Expected)
	}
	for _, v := range versions {
		if v.Cluster != c.Expected {
			return fmt.Sprintf("host=%s path=%s span=[%s,%s) got=%q want=%q",
				c.Host, c.Path, v.From.Format(time.RFC3339), v.To.Format(time.RFC3339), v.Cluster, c.Expected)
		}
	}
	return ""
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

// modeLabel prefixes a benchmark mode name with the store engine so out/report.md
// rows stay self-describing.
func modeLabel(mode string) string {
	return "[" + string(store.BackendClickHouse) + "] " + mode
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
//
// This is the bulk/steady-state throughput path and stays single-point (resolved
// at `now`, the open version): its whole point is amortizing translation across a
// warm per-gateway batch, which the cold per-segment interval path deliberately
// does not do. The interval (range) path is benchmarked by TestResolveSingleWorst
// and exercised end-to-end by TestIPFlowClickHouse.
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
	result := buildResult(modeLabel("WARM (cache, steady state, single-point)"), cases, res, m, time.Since(start))
	appendReport(t, result)
	if result.Mismatches != 0 {
		t.Fatalf("%d mismatches (want 0): %v", result.Mismatches, result.MismatchSamples)
	}
}

// TestResolveSingleWorst is the WORST-CASE, single-request benchmark and the
// default interval path: each host+path is resolved over a time RANGE [from,to)
// via rangequery — one scoped Overlap load (LoadTrafficWindow), in-memory slicing
// into per-version segments, then the full pipeline (translate + one
// router_check_tool) per segment, all cold. This is the honest online-serving
// latency for a range query; cost scales with the number of version segments in
// the window. Sampled (POC_WORST_SAMPLE) because each segment pays a full tool
// spawn. Override the window with POC_BENCH_FROM / POC_BENCH_TO.
func TestResolveSingleWorst(t *testing.T) {
	runner := requireRouterCheck(t)
	g := scalegen.New(benchScale())
	cases := g.Cases(4)
	if sample := envInt("POC_WORST_SAMPLE", 200); sample < len(cases) {
		cases = cases[:sample]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	st := requireStore(ctx, t)
	defer st.Close()
	ensureLoaded(ctx, t, st, g, benchVersions())
	t0, t1 := benchRange(t)
	t.Logf("SINGLE-WORST (range): %d single-request range queries over [%s, %s)",
		len(cases), t0.Format(time.RFC3339), t1.Format(time.RFC3339))

	deps := rangequery.Deps{Translator: translate.NewTranslator(), Runner: runner}

	agg := &report.Result{Mode: modeLabel("SINGLE-WORST (per-request, cold, batch=1, range)"), Queries: len(cases)}
	agg.Notes = "Worst-case ONLINE latency over an interval: one scoped Overlap load (LoadTrafficWindow) + in-memory slicing + translate + router_check_tool per DISTINCT config. Lookup stage = the single DB Overlap load. Cost scales with the number of distinct configs in [from,to); identical-spec version bumps are deduped, so translate/check are counted per distinct config, not per raw segment."

	start := time.Now()
	totalSegments, totalDistinct := 0, 0
	for _, c := range cases {
		// Broad/unknown hosts (direct-*, nope-*) have no traffic IP by construction;
		// like the single-point path, an empty IP yields no candidates -> a miss
		// (Expected ""), not an error.
		ip, _ := g.IPForHost(c.Host)
		tq := time.Now()
		versions, m, err := deps.ResolveTimed(ctx, st, c.Host, c.Path, ip, t0, t1)
		perQuery := time.Since(tq)
		if err != nil {
			t.Fatalf("range resolve: %v", err)
		}
		totalSegments += m.Segments
		totalDistinct += m.DistinctCfgs
		agg.Stages.Total.Add(perQuery)
		agg.Stages.Lookup.Add(m.Load) // the single DB Overlap load for the whole window
		agg.Stages.Resolve.Add(m.Resolve)
		agg.Stages.ScopedFetch.Add(m.ScopedFor)
		agg.Stages.Translate.Add(m.Translate)
		agg.Stages.Check.Add(m.Check)
		agg.CacheHits += m.CacheHits
		agg.CacheMisses += m.CacheMisses
		if mism := rangeMismatch(versions, c); mism != "" {
			agg.Mismatches++
			if len(agg.MismatchSamples) < 10 {
				agg.MismatchSamples = append(agg.MismatchSamples, mism)
			}
		}
	}
	agg.Wall = time.Since(start)
	agg.PeakRSSKB = peakRSSKB()
	t.Logf("range slicing: %d cases, %d total segments (avg %.1f/case), %d distinct configs translated+checked (identical-spec versions dedup)",
		len(cases), totalSegments, float64(totalSegments)/float64(len(cases)), totalDistinct)
	appendReport(t, agg)
	if agg.Mismatches != 0 {
		t.Fatalf("%d mismatches (want 0): %v", agg.Mismatches, agg.MismatchSamples)
	}
}
