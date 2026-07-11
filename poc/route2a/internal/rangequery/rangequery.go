// Package rangequery answers "which destination did this request resolve to over
// [t0,t1)?" — the interval counterpart of the point-in-time query. It loads the
// traffic window once (store.LoadTrafficWindow), slices it at every version
// boundary (internal/memwindow), and runs the per-gateway pipeline (3-hop ->
// gwresolve -> translate -> router_check_tool) for each distinct config in the
// window, returning one VersionResolution per distinct outcome (adjacent equal
// segments are merged).
//
// Cost scales with the number of DISTINCT configs in the window, not the raw
// number of version boundaries: segments whose scoped config is byte-identical
// (a version bump that doesn't change routing — the common case) share one
// translate+check via a per-query signature cache.
package rangequery

import (
	"context"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/memwindow"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// VersionResolution is the destination a request resolved to over one time span.
type VersionResolution struct {
	From, To time.Time
	Gateway  string // "" = no gateway served the host in this span
	Cluster  string // "" = a gateway served it but no route matched (miss)
}

// Metrics is the cost of one range Resolve: a single scoped Overlap load plus the
// per-segment resolution. translate/check are counted only on a signature-cache
// MISS (a distinct config), so the totals reflect distinct-config cost, not raw
// segment count.
type Metrics struct {
	Load         time.Duration // the one LoadTrafficWindow scoped Overlap query
	Segments     int           // version segments the window sliced into
	DistinctCfgs int           // distinct configs that paid translate+check
	Resolve      time.Duration // Σ per-segment in-memory 3-hop + host->gateway
	ScopedFor    time.Duration // Σ per-distinct-config in-memory ScopedFor
	Translate    time.Duration // Σ per-distinct-config istiod translation
	Check        time.Duration // Σ per-distinct-config router_check_tool
	CacheHits    uint64        // segments served from the signature cache
	CacheMisses  uint64        // distinct configs (== DistinctCfgs)
}

// Deps are the stateless collaborators the per-segment pipeline needs.
type Deps struct {
	Translator *translate.Translator
	Runner     matchcheck.Runner
}

// outcome caches a resolved (gateway, cluster) for one config signature.
type outcome struct {
	gateway string
	cluster string
}

// Resolve returns the gateway+cluster a request (host, path, dst ip) resolved to
// across [t0,t1), one entry per distinct outcome.
func (d Deps) Resolve(ctx context.Context, st store.Store, host, path, ip string, t0, t1 time.Time) ([]VersionResolution, error) {
	out, _, err := d.ResolveTimed(ctx, st, host, path, ip, t0, t1)
	return out, err
}

// ResolveTimed is Resolve with per-stage timing folded into Metrics (for the
// benchmark). The window is loaded once; each distinct config resolves once.
func (d Deps) ResolveTimed(ctx context.Context, st store.Store, host, path, ip string, t0, t1 time.Time) ([]VersionResolution, *Metrics, error) {
	m := &Metrics{}

	loadStart := time.Now()
	w, err := st.LoadTrafficWindow(ctx, ip, t0, t1)
	m.Load = time.Since(loadStart)
	if err != nil {
		return nil, m, err
	}
	mw := memwindow.New(w, t0, t1)

	// Per-query signature cache: a distinct config is translated + checked once,
	// then reused for every other segment with the same config. The key is a
	// 64-bit content signature (memwindow.ConfigSigAt) — the cache is per-query and
	// tiny, so 64 bits is collision-safe here.
	cache := map[uint64]outcome{}

	var out []VersionResolution
	for _, seg := range mw.Segments() {
		t := seg.From
		m.Segments++

		// Cheap in-memory stages: 3-hop IP->candidates, then host disambiguation.
		rt := time.Now()
		cands := mw.ResolveIPToGateways(ip, t)
		gw, ok := gwresolve.New(candsToGateways(cands)).ResolveAmong(host, candNames(cands))
		m.Resolve += time.Since(rt)

		var res outcome
		if ok {
			sig, sok := mw.ConfigSigAt(gw, t)
			if !sok {
				// Gateway resolved by the 3-hop but has no live config -> miss.
				res = outcome{}
			} else if cached, hit := cache[sig]; hit {
				res = cached
				m.CacheHits++
			} else {
				r, err := d.resolveConfig(ctx, mw, gw, host, path, t, m)
				if err != nil {
					return nil, m, err
				}
				cache[sig] = r
				m.CacheMisses++
				m.DistinctCfgs++
				res = r
			}
		}

		vr := VersionResolution{From: seg.From, To: seg.To, Gateway: res.gateway, Cluster: res.cluster}
		// Merge with the previous span if the outcome is identical.
		if n := len(out); n > 0 && out[n-1].Gateway == vr.Gateway && out[n-1].Cluster == vr.Cluster {
			out[n-1].To = vr.To
			continue
		}
		out = append(out, vr)
	}
	return out, m, nil
}

// resolveConfig runs the expensive stages for one gateway as-of t: in-memory
// ScopedFor -> istiod translate -> router_check_tool. Timings fold into m.
func (d Deps) resolveConfig(ctx context.Context, mw *memwindow.Window, gw, host, path string, t time.Time, m *Metrics) (outcome, error) {
	sf := time.Now()
	scoped, found, err := mw.ScopedFor(gw, t)
	m.ScopedFor += time.Since(sf)
	if err != nil {
		return outcome{}, err
	}
	if !found {
		return outcome{gateway: gw}, nil // gateway present but no config -> miss cluster
	}

	tt := time.Now()
	rc, err := d.Translator.Translate(scoped)
	m.Translate += time.Since(tt)
	if err != nil {
		return outcome{}, err
	}

	ct := time.Now()
	clusters, err := d.Runner.Resolve(ctx, rc, []matchcheck.Query{{Host: host, Path: path}})
	m.Check += time.Since(ct)
	if err != nil {
		return outcome{}, err
	}
	return outcome{gateway: gw, cluster: clusters[0]}, nil
}

func candsToGateways(cands []store.GatewayCand) []gwresolve.Gateway {
	out := make([]gwresolve.Gateway, len(cands))
	for i, c := range cands {
		out[i] = gwresolve.Gateway{Name: c.Name, Hosts: c.ServerHosts}
	}
	return out
}

func candNames(cands []store.GatewayCand) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.Name
	}
	return out
}
