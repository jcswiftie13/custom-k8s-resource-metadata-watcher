// Package simulate is the directly-usable route-simulation engine: given a
// request host+path it resolves the destination service (Envoy cluster) the way
// production would, using istiod translation + Envoy's router_check_tool as the
// matching engine (the live-Envoy path was removed):
//
//	host --gwresolve--> gateway --translate(rccache)--> RouteConfiguration
//	(host,path)... --router_check_tool (one batch per gateway)--> cluster
//
// router_check_tool is a batch tool, so queries are grouped by gateway: each
// gateway's RouteConfiguration is translated at most once (cache permitting) and
// the tool is invoked once per gateway over that gateway's whole batch.
package simulate

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/rccache"
	"github.com/example/metadata-exporter/poc/route2a/internal/report"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// Query is one host+path to resolve.
type Query struct {
	Host string
	Path string
}

// Resolution is the outcome for one query.
type Resolution struct {
	Query
	Gateway string // "" = no gateway serves this host
	Cluster string // "" = a gateway served it but no route matched (miss)
}

// ScopedSource yields the scoped translate input for one gateway (its Gateway CR
// + bound VirtualServices + referenced Services). In production this is backed by
// the config store; in the perf test by scalegen.
type ScopedSource func(gateway string) (translate.ScopedInput, bool)

// Config wires the engine's collaborators.
type Config struct {
	Resolver   *gwresolve.Resolver
	Cache      *rccache.Cache
	Translator *translate.Translator
	ScopedFor  ScopedSource
	// Lookup, when set, narrows a request to the candidate gateways served by its
	// traffic IP (the ClickHouse 3-hop) before host disambiguation; Resolver then
	// picks the most-specific match among those candidates. nil => resolve the host
	// over all gateways (the original in-memory path).
	Lookup func(host string) ([]string, error)
	Runner matchcheck.Runner
}

// Engine resolves host+path to a destination service cluster.
type Engine struct{ cfg Config }

// New builds an engine from its wiring.
func New(cfg Config) *Engine { return &Engine{cfg: cfg} }

// Metrics captures per-stage timing and cache behaviour for one ResolveAll run.
type Metrics struct {
	Stages      report.Stages
	CacheHits   uint64
	CacheMisses uint64
	Gateways    int // distinct gateways touched (== router_check_tool invocations)
}

// ResolveAll resolves every query and returns one Resolution per query (aligned
// by index) plus timing/cache metrics.
func (e *Engine) ResolveAll(ctx context.Context, queries []Query) ([]Resolution, *Metrics, error) {
	res := make([]Resolution, len(queries))
	m := &Metrics{}

	// Stage 1: resolve host -> gateway (per query); bucket query indices by gateway.
	// With Lookup wired, the IP 3-hop narrows the candidate gateways first (timed
	// separately) and ResolveAmong disambiguates within them; otherwise Resolve
	// scans all gateways.
	byGW := map[string][]int{}
	for i, q := range queries {
		res[i].Query = q
		var gw string
		var ok bool
		if e.cfg.Lookup != nil {
			t0 := time.Now()
			cands, err := e.cfg.Lookup(q.Host)
			m.Stages.Lookup.Add(time.Since(t0))
			if err != nil {
				return nil, nil, fmt.Errorf("lookup %s: %w", q.Host, err)
			}
			t1 := time.Now()
			gw, ok = e.cfg.Resolver.ResolveAmong(q.Host, cands)
			m.Stages.Resolve.Add(time.Since(t1))
		} else {
			t0 := time.Now()
			gw, ok = e.cfg.Resolver.Resolve(q.Host)
			m.Stages.Resolve.Add(time.Since(t0))
		}
		if !ok {
			continue // no gateway -> miss (Gateway "", Cluster "")
		}
		res[i].Gateway = gw
		byGW[gw] = append(byGW[gw], i)
	}

	// Process gateways in a stable order for reproducible timings/reports.
	gws := make([]string, 0, len(byGW))
	for gw := range byGW {
		gws = append(gws, gw)
	}
	sort.Strings(gws)
	m.Gateways = len(gws)

	for _, gw := range gws {
		idxs := byGW[gw]
		gwStart := time.Now()

		// Stage 2: obtain the gateway's RC (a cache hit skips translation).
		rc, hit := e.cfg.Cache.Get(gw)
		if hit {
			m.CacheHits++
		} else {
			m.CacheMisses++
			// ScopedFor may be ClickHouse-backed (a config-fetch round-trip), so time
			// it as its own stage; Translate then measures only the istiod translation.
			t0 := time.Now()
			scoped, found := e.cfg.ScopedFor(gw)
			m.Stages.ScopedFetch.Add(time.Since(t0))
			if !found {
				return nil, nil, fmt.Errorf("no scoped config for gateway %q", gw)
			}
			t0 = time.Now()
			translated, err := e.cfg.Translator.Translate(scoped)
			if err != nil {
				return nil, nil, fmt.Errorf("translate %s: %w", gw, err)
			}
			m.Stages.Translate.Add(time.Since(t0))
			e.cfg.Cache.Put(gw, translated)
			rc = translated
		}

		// Stage 3: batch-resolve this gateway's queries via router_check_tool.
		batch := make([]matchcheck.Query, len(idxs))
		for k, i := range idxs {
			batch[k] = matchcheck.Query{Host: queries[i].Host, Path: queries[i].Path}
		}
		t0 := time.Now()
		clusters, err := e.cfg.Runner.Resolve(ctx, rc, batch)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve %s: %w", gw, err)
		}
		m.Stages.Check.Add(time.Since(t0))

		for k, i := range idxs {
			res[i].Cluster = clusters[k]
		}
		m.Stages.Total.Add(time.Since(gwStart))
	}
	return res, m, nil
}
