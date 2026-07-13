// Package gwresolve is a PRODUCTION component of the "host+path -> service"
// query engine (not test scaffolding): given a request host, it finds the
// ingress gateway that serves it by matching the host against every gateway's
// server hosts, using Istio's own wildcard/most-specific semantics — exactly
// like real Istio gateway selection. The system under test must resolve the
// gateway this way; it must NOT decode any ordinal embedded in the hostname.
package gwresolve

import (
	"sort"

	"istio.io/istio/pkg/config/host"
)

// Gateway is one ingress gateway's identity + its server host patterns.
type Gateway struct {
	Name  string
	Hosts []string // exact / "*.suffix" / "*"
}

type pat struct {
	gw      string
	pattern host.Name
	score   int // higher = more specific
}

// Resolver indexes gateway host patterns for host->gateway lookup.
type Resolver struct {
	pats []pat
}

// specificity ranks a pattern: exact always beats any wildcard; among wildcards
// a longer suffix (longer pattern) is more specific; "*" is least specific.
func specificity(p host.Name) int {
	if p.IsWildCarded() {
		return len(p) // "*.gw000.example.com" (19) > "*.example.com" (13) > "*" (1)
	}
	return len(p) + 1_000_000 // exact host wins outright
}

// New builds a resolver over all gateways. Patterns are pre-sorted most-specific
// first so Resolve returns on the first match.
func New(gws []Gateway) *Resolver {
	r := &Resolver{}
	for _, gw := range gws {
		for _, h := range gw.Hosts {
			r.pats = append(r.pats, pat{gw: gw.Name, pattern: host.Name(h), score: specificity(host.Name(h))})
		}
	}
	sort.SliceStable(r.pats, func(a, b int) bool {
		if r.pats[a].score != r.pats[b].score {
			return r.pats[a].score > r.pats[b].score
		}
		return r.pats[a].pattern < r.pats[b].pattern // deterministic tie-break
	})
	return r
}

// Resolve returns the gateway whose server hosts most-specifically match the
// request host ("", false = no gateway serves it). This is a hot path.
func (r *Resolver) Resolve(reqHost string) (string, bool) {
	h := host.Name(reqHost)
	for _, p := range r.pats {
		// pats are sorted most-specific first; the first match is the answer.
		if p.pattern.Matches(h) {
			return p.gw, true
		}
	}
	return "", false
}

// ResolveAmong is Resolve limited to a candidate gateway set — the IP-narrowed
// candidates from the ClickHouse 3-hop. It scans the same most-specific-first
// patterns but only considers those whose gateway is in candidates, so the first
// match is the most-specific gateway serving reqHost among the candidates. An
// empty candidate set yields ("", false) — a traffic miss.
func (r *Resolver) ResolveAmong(reqHost string, candidates []string) (string, bool) {
	if len(candidates) == 0 {
		return "", false
	}
	allow := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		allow[c] = struct{}{}
	}
	h := host.Name(reqHost)
	for _, p := range r.pats {
		if _, ok := allow[p.gw]; !ok {
			continue
		}
		if p.pattern.Matches(h) {
			return p.gw, true
		}
	}
	return "", false
}
