// Command ipflow drives the ingress-traffic slice of the PoC end to end against
// ClickHouse:
//
//	-mode=load    generate the multi-version corpus (program-generated, never
//	              hand-written SQL) and stream it into ClickHouse.
//	-mode=query   given a request host+path (+ optional dst IP, + optional
//	              listener port), resolve the destination cluster via the
//	              gwresolve -> translate(from ClickHouse) -> router_check_tool
//	              pipeline. With -ip it runs a traffic simulation (ClickHouse 3-hop
//	              IP -> Gateway narrows candidates); without -ip it is config-only
//	              (resolve the host across ALL gateways, no 3-hop). -port selects
//	              the listener RC (80 => http.80, 443 => https.443, TLS-terminated).
//	-mode=verify  sample the 3-hop against the by-construction oracle and check
//	              multi-version AsOf(T) selection (no router_check_tool needed).
//
// The IP stands in for "host + path + DNS lookup landing IP"; real deployments get
// it from the OTEL span. Omit -ip to exercise the config-only path.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
	"github.com/example/metadata-exporter/poc/route2a/internal/ingload"
	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/rangequery"
	"github.com/example/metadata-exporter/poc/route2a/internal/rccache"
	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
	"github.com/example/metadata-exporter/poc/route2a/internal/simulate"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
	"github.com/example/metadata-exporter/poc/route2a/internal/storeopen"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

func main() {
	log.SetFlags(0)
	var (
		addr = flag.String("addr", "", "ClickHouse connection string; empty => POC_CH_ADDR or default 127.0.0.1:9000")
		mode = flag.String("mode", "query", "load | query | verify")
		ip   = flag.String("ip", "", "destination IP (post-DNS); empty => config-only (resolve host over all gateways, no 3-hop)")
		host = flag.String("host", "svc00.gw000.example.com", "request :authority host (required)")
		path = flag.String("path", "/", "request :path")
		port = flag.Int("port", 80, "ingress listener port; 80 => http.80 RC, 443 => https.443 RC (TLS-terminated)")
		from = flag.String("from", "", "range start (RFC3339); with -to, resolve the request over [from,to) per version")
		to   = flag.String("to", "", "range end (RFC3339); with -from, resolve the request over [from,to) per version")
	)
	flag.Parse()

	ctx := context.Background()
	st, err := storeopen.Open(ctx, *addr)
	if err != nil {
		log.Fatalf("open clickhouse: %v", err)
	}
	defer st.Close()

	gen := scalegen.New(scalegen.Config{NumGateways: envInt("POC_GATEWAYS", 600), VSPerGW: envInt("POC_VS", 100)})

	switch *mode {
	case "load":
		if err := load(ctx, st, gen); err != nil {
			log.Fatalf("load: %v", err)
		}
	case "query":
		// -from and -to (both set) switch to the interval path (per-version result);
		// otherwise resolve at "now" (single point).
		if *from != "" || *to != "" {
			t0, t1, err := parseRange(*from, *to)
			if err != nil {
				log.Fatalf("range: %v", err)
			}
			if err := queryRange(ctx, st, gen, *ip, *host, *path, *port, t0, t1); err != nil {
				log.Fatalf("query range: %v", err)
			}
		} else if err := query(ctx, st, gen, *ip, *host, *path, *port); err != nil {
			log.Fatalf("query: %v", err)
		}
	case "verify":
		if err := verify(ctx, st, gen); err != nil {
			log.Fatalf("verify: %v", err)
		}
	default:
		log.Fatalf("unknown -mode %q (load|query|verify)", *mode)
	}
}

// parseRange parses the -from/-to RFC3339 bounds; both must be set and ordered.
func parseRange(from, to string) (time.Time, time.Time, error) {
	if from == "" || to == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("both -from and -to are required for a range query")
	}
	t0, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse -from: %w", err)
	}
	t1, err := time.Parse(time.RFC3339, to)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse -to: %w", err)
	}
	if !t0.Before(t1) {
		return time.Time{}, time.Time{}, fmt.Errorf("-from %s must be before -to %s", from, to)
	}
	return t0.UTC(), t1.UTC(), nil
}

// envVersions reads the per-resource-type version counts (bitemporal depth).
func envVersions() ingload.Versions {
	return ingload.Versions{
		Deploy: envInt("POC_VER_DEPLOY", 2),
		Svc:    envInt("POC_VER_SVC", 2),
		Gw:     envInt("POC_VER_GW", 2),
		VS:     envInt("POC_VER_VS", 10),
		KSvc:   envInt("POC_VER_KSVC", 100),
	}
}

// load generates the corpus and streams it into the store as multi-version rows.
func load(ctx context.Context, st store.Store, gen *scalegen.Gen) error {
	start := time.Now()
	prog := func(done, total int) {
		if done%100 == 0 || done == total {
			log.Printf("  loaded %d/%d gateways...", done, total)
		}
	}
	if err := ingload.Load(ctx, st, gen, envVersions(), prog); err != nil {
		return err
	}
	log.Printf("load done in %s. row counts:", time.Since(start).Round(time.Millisecond))
	for _, tbl := range []string{"service_versions", "deploy_versions", "gw_versions", "vs_versions"} {
		cnt, err := st.CountRows(ctx, tbl)
		if err != nil {
			return err
		}
		log.Printf("  %-18s %d", tbl, cnt)
	}
	return nil
}

// query runs the ingress-traffic pipeline for one request at "now". With an IP it
// is a traffic simulation (ClickHouse 3-hop narrows candidate gateways); without
// an IP it is config-only (resolve the host across ALL gateways, no 3-hop). port
// selects the listener RC (80 => http.80, 443 => https.443..., TLS-terminated).
func query(ctx context.Context, st store.Store, gen *scalegen.Gen, ip, host, path string, port int) error {
	if host == "" {
		return fmt.Errorf("-host is required")
	}
	t := time.Now().UTC()

	var resolver *gwresolve.Resolver
	var gw string
	var ok bool
	configOnly := ip == ""

	if configOnly {
		log.Printf("request: host=%s path=%s port=%d (config-only, no IP)  @T=%s", host, path, port, t.Format(time.RFC3339))
		// Config-only: disambiguate the host across every live gateway (no 3-hop).
		cands, err := st.AllGatewaysLiveAt(ctx, t)
		if err != nil {
			return err
		}
		resolver = gwresolve.New(candsToGateways(cands))
		gw, ok = resolver.Resolve(host)
		if !ok {
			log.Printf("RESULT: no gateway serves host %s", host)
			return nil
		}
		log.Printf("gwresolve (all %d gateways): host %s -> gateway %s", len(cands), host, gw)
	} else {
		log.Printf("request: host=%s path=%s port=%d dst_ip=%s  @T=%s", host, path, port, ip, t.Format(time.RFC3339))
		// Stage 1: ClickHouse 3-hop IP -> candidate gateways.
		cands, err := st.ResolveIPToGateways(ctx, ip, t)
		if err != nil {
			return err
		}
		names := make([]string, len(cands))
		for i, c := range cands {
			names[i] = c.Name
		}
		log.Printf("3-hop: %s -> candidate gateways %v", ip, names)
		if len(cands) == 0 {
			log.Printf("RESULT: traffic miss (no ingress serves this IP)")
			return nil
		}
		// Stage 2: host reverse-lookup among the IP-narrowed candidates.
		resolver = gwresolve.New(candsToGateways(cands))
		gw, ok = resolver.Resolve(host)
		if !ok {
			log.Printf("RESULT: no candidate gateway serves host %s", host)
			return nil
		}
		log.Printf("gwresolve: host %s -> gateway %s", host, gw)
	}

	// Oracle check for the gateway (IP-based; skipped in config-only).
	wantGW, gwOK := "", true
	if !configOnly {
		wantGW, _ = gen.GatewayForIP(ip)
		gwOK = gw == wantGW
	}

	// Stage 3: translate (config from ClickHouse) + router_check_tool -> cluster.
	runner, kind, ok := matchcheck.Detect()
	if !ok {
		log.Printf("router_check_tool unavailable (no native binary, no tools image) — skipping cluster resolution")
		if configOnly {
			log.Printf("RESULT: gateway=%s", gw)
		} else {
			log.Printf("RESULT: gateway=%s (oracle expects %s) %s", gw, wantGW, pass(gwOK))
		}
		return nil
	}
	log.Printf("router_check_tool: %s", kind)
	engine := simulate.New(simulate.Config{
		Resolver:   resolver,
		Cache:      rccache.New(rccache.ColdAlways, rccache.NewDepIndex()),
		Translator: translate.NewTranslator(),
		ScopedFor:  chScopedFor(ctx, st, t),
		Runner:     runner,
		Port:       port,
	})
	res, _, err := engine.ResolveAll(ctx, []simulate.Query{{Host: host, Path: path}})
	if err != nil {
		return err
	}
	cluster := res[0].Cluster
	log.Printf("cluster: %s", cluster)

	if configOnly {
		// No IP oracle in config-only; still verify the config-derived cluster.
		wantCluster := gen.ExpectedCluster(host, path)
		log.Printf("RESULT: gateway=%s cluster=%s  %s", gw, cluster, pass(cluster == wantCluster))
		if cluster != wantCluster {
			log.Printf("  oracle: cluster=%s", wantCluster)
			os.Exit(1)
		}
		return nil
	}

	wantCluster := gen.ExpectedCluster(host, path)
	log.Printf("RESULT: gateway=%s cluster=%s  %s",
		gw, cluster, pass(gwOK && cluster == wantCluster))
	if !gwOK || cluster != wantCluster {
		log.Printf("  oracle: gateway=%s cluster=%s", wantGW, wantCluster)
		os.Exit(1)
	}
	return nil
}

// queryRange runs the interval pipeline for one request over [t0,t1): load the
// traffic window once, slice it at version boundaries, and resolve each segment,
// printing one line per version (time span + gateway + cluster).
func queryRange(ctx context.Context, st store.Store, gen *scalegen.Gen, ip, host, path string, port int, t0, t1 time.Time) error {
	if host == "" {
		return fmt.Errorf("-host is required")
	}
	if ip == "" {
		// The interval path loads an IP-scoped traffic window (LoadTrafficWindow);
		// a config-only interval would need a non-IP-scoped load (out of scope).
		return fmt.Errorf("range query (-from/-to) requires -ip (config-only interval not supported)")
	}
	log.Printf("request: host=%s path=%s port=%d dst_ip=%s  over [%s, %s)",
		host, path, port, ip, t0.Format(time.RFC3339), t1.Format(time.RFC3339))

	runner, kind, ok := matchcheck.Detect()
	if !ok {
		return fmt.Errorf("router_check_tool unavailable (no native binary, no tools image) — cannot resolve clusters")
	}
	log.Printf("router_check_tool: %s", kind)

	deps := rangequery.Deps{Translator: translate.NewTranslator(), Runner: runner}
	versions, err := deps.Resolve(ctx, st, host, path, ip, port, t0, t1)
	if err != nil {
		return err
	}
	if len(versions) == 0 {
		log.Printf("RESULT: no version in range (empty traffic window)")
		return nil
	}
	for _, v := range versions {
		cluster := v.Cluster
		if cluster == "" {
			cluster = "<no route / miss>"
		}
		gw := v.Gateway
		if gw == "" {
			gw = "<no gateway>"
		}
		log.Printf("  [%s, %s) gateway=%s cluster=%s",
			v.From.Format(time.RFC3339), v.To.Format(time.RFC3339), gw, cluster)
	}
	return nil
}

// verify samples the 3-hop against the oracle and checks multi-version selection.
func verify(ctx context.Context, st store.Store, gen *scalegen.Gen) error {
	t := time.Now().UTC()
	n := gen.NumGateways()
	step := n/20 + 1
	fail := 0
	for i := 0; i < n; i += step {
		ip := scalegen.IPForGateway(i)
		cands, err := st.ResolveIPToGateways(ctx, ip, t)
		if err != nil {
			return err
		}
		want := scalegen.GatewayName(i)
		got := ""
		if len(cands) == 1 {
			got = cands[0].Name
		}
		ok := got == want
		if !ok {
			fail++
		}
		log.Printf("3-hop i=%d ip=%s -> %v (want [%s]) %s", i, ip, names(cands), want, pass(ok))
	}

	// Multi-version AsOf: a resource's version live at VersionMidTime(v) has rev==v.
	vc := envVersions()
	gw0 := scalegen.GatewayName(0)
	for v := 0; v < vc.Gw; v++ {
		rev, ok, err := st.AsOfRev(ctx, "gw_versions", scalegen.GatewayNamespace(), gw0, store.VersionMidTime(v))
		if err != nil {
			return err
		}
		hit := ok && int(rev) == v
		if !hit {
			fail++
		}
		log.Printf("AsOf gw_versions %s @rev-window %d -> rev=%d (ok=%v) %s", gw0, v, rev, ok, pass(hit))
	}

	if fail != 0 {
		return fmt.Errorf("%d verification checks failed", fail)
	}
	log.Printf("verify: all checks passed")
	return nil
}

// chScopedFor adapts the store's ScopedFor to simulate.ScopedSource (fixed ctx+T).
func chScopedFor(ctx context.Context, st store.Store, t time.Time) simulate.ScopedSource {
	return func(gw string) (translate.ScopedInput, bool) {
		in, ok, err := st.ScopedFor(ctx, gw, t)
		if err != nil {
			log.Printf("scopedFor %s: %v", gw, err)
			return translate.ScopedInput{}, false
		}
		return in, ok
	}
}

func candsToGateways(cands []store.GatewayCand) []gwresolve.Gateway {
	out := make([]gwresolve.Gateway, len(cands))
	for i, c := range cands {
		out[i] = gwresolve.Gateway{Name: c.Name, Hosts: c.ServerHosts}
	}
	return out
}

func names(cands []store.GatewayCand) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.Name
	}
	return out
}

func pass(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
