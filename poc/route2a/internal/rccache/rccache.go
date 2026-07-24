// Package rccache is the per-gateway RouteConfiguration cache of the query
// engine. WARM mode reuses a cached RC only when the gateway's dependencies
// (its Gateway CR, bound VirtualServices, referenced Services) are unchanged
// since generation — a dependency-aware O(1) epoch check (mirroring istiod's
// push-context / resourceVersion), NOT a per-request re-hash of all inputs.
//
// A WARM cache HIT means the engine SKIPS the (expensive) scoped translation;
// the RC is still fed to Envoy every query, because queries keep hopping across
// gateways. So COLD-vs-WARM isolates translation cost.
package rccache

import (
	"sync"
	"time"

	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
)

// Mode selects cache behavior.
type Mode int

const (
	// ColdAlways forces every Get to miss -> the engine re-translates every query.
	ColdAlways Mode = iota
	// WarmLazy reuses a cached RC while the gateway's dep-epoch is unchanged.
	WarmLazy
)

// Epoch is a monotonically increasing per-gateway dependency revision.
type Epoch uint64

// DepIndex tracks, per gateway, a dependency revision that bumps whenever any
// resource that gateway's RC depends on changes. Bump is O(#gateways-depending),
// Rev is O(1). Built once from the known topology.
type DepIndex struct {
	mu      sync.RWMutex
	gwEpoch map[string]Epoch
	ownedBy map[string][]string // dependency key -> gateways depending on it
}

func NewDepIndex() *DepIndex {
	return &DepIndex{gwEpoch: map[string]Epoch{}, ownedBy: map[string][]string{}}
}

// Own records that gateway gw depends on dependency key dep (its Gateway CR,
// each bound VS, each referenced Service).
func (d *DepIndex) Own(dep, gw string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ownedBy[dep] = append(d.ownedBy[dep], gw)
	if _, ok := d.gwEpoch[gw]; !ok {
		d.gwEpoch[gw] = 0
	}
}

// Bump advances the epoch of every gateway depending on dep (a config change).
func (d *DepIndex) Bump(dep string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, gw := range d.ownedBy[dep] {
		d.gwEpoch[gw]++
	}
}

// Rev returns the current dependency revision of gateway gw.
func (d *DepIndex) Rev(gw string) Epoch {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.gwEpoch[gw]
}

type entry struct {
	rc     *route.RouteConfiguration
	depRev Epoch
	genAt  time.Time
}

// Cache is the per-gateway RC cache with dependency-aware invalidation.
type Cache struct {
	mu   sync.RWMutex
	byGW map[string]*entry
	deps *DepIndex
	mode Mode

	hits, misses uint64
}

func New(mode Mode, deps *DepIndex) *Cache {
	return &Cache{byGW: map[string]*entry{}, deps: deps, mode: mode}
}

// Get returns the cached RC for gw iff WARM and the dep-epoch is unchanged.
func (c *Cache) Get(gw string) (*route.RouteConfiguration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == ColdAlways {
		c.misses++
		return nil, false
	}
	e := c.byGW[gw]
	if e != nil && e.depRev == c.deps.Rev(gw) {
		c.hits++
		return e.rc, true
	}
	c.misses++
	return nil, false
}

// Put stores a freshly generated RC, stamped with the current dep-epoch.
func (c *Cache) Put(gw string, rc *route.RouteConfiguration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byGW[gw] = &entry{rc: rc, depRev: c.deps.Rev(gw), genAt: time.Now()}
}

// Stats returns cumulative hit/miss counts (hit rate = hits/(hits+misses)).
func (c *Cache) Stats() (hits, misses uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses
}
