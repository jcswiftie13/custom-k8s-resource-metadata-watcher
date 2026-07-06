// Package report collects per-query latency samples and renders the stress-test
// results. The headline is the honest single-replica serialized throughput and
// per-stage p50/p99 — no optimization hides the real cost.
package report

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Hist is a simple growable sample set with percentile queries.
type Hist struct {
	ns []int64
}

func (h *Hist) Add(d time.Duration) { h.ns = append(h.ns, int64(d)) }
func (h *Hist) Len() int            { return len(h.ns) }

// Pct returns the p-th percentile (0..100) as a Duration.
func (h *Hist) Pct(p float64) time.Duration {
	if len(h.ns) == 0 {
		return 0
	}
	s := append([]int64(nil), h.ns...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(p / 100 * float64(len(s)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return time.Duration(s[idx])
}

func (h *Hist) Mean() time.Duration {
	if len(h.ns) == 0 {
		return 0
	}
	var sum int64
	for _, v := range h.ns {
		sum += v
	}
	return time.Duration(sum / int64(len(h.ns)))
}

// Stages holds one histogram per pipeline stage. Resolve is sampled per query;
// Translate and Check are sampled per gateway batch (router_check_tool runs once
// per gateway over all that gateway's queries).
type Stages struct {
	Resolve   Hist // host -> gateway (per query)
	Translate Hist // scoped istiod translation (per gateway; only on cache miss)
	Check     Hist // router_check_tool batch resolution (per gateway)
	Total     Hist // whole run, per gateway batch
}

// Result is the full benchmark outcome for one mode/run.
type Result struct {
	Mode        string
	Queries     int
	Mismatches  int
	CacheHits   uint64
	CacheMisses uint64
	Wall        time.Duration
	Stages      Stages
	// MismatchSamples holds up to the first few mismatch descriptions (was the
	// driver's t.Logf output; captured here so the driver needs no test.Failer).
	MismatchSamples []string
	// Setup / scale metrics.
	PeakRSSKB int64
	Notes     string
}

// Throughput returns queries per second over the timed request loop.
func (r *Result) Throughput() float64 {
	if r.Wall <= 0 {
		return 0
	}
	return float64(r.Queries) / r.Wall.Seconds()
}

// CacheHitRate is hits/(hits+misses).
func (r *Result) CacheHitRate() float64 {
	tot := r.CacheHits + r.CacheMisses
	if tot == 0 {
		return 0
	}
	return float64(r.CacheHits) / float64(tot)
}

// Markdown renders one result as a table row block.
func (r *Result) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\n", r.Mode)
	fmt.Fprintf(&b, "- queries: **%d**, mismatches: **%d**, wall: **%s**, throughput: **%.0f q/s** (serialized single replica)\n",
		r.Queries, r.Mismatches, r.Wall.Round(time.Millisecond), r.Throughput())
	fmt.Fprintf(&b, "- cache hit rate: **%.1f%%** (%d hits / %d misses)\n",
		r.CacheHitRate()*100, r.CacheHits, r.CacheMisses)
	if r.PeakRSSKB > 0 {
		fmt.Fprintf(&b, "- peak RSS: **%d MB**\n", r.PeakRSSKB/1024)
	}
	fmt.Fprintf(&b, "\n| stage | p50 | p99 | mean |\n|---|---|---|---|\n")
	row := func(name string, h *Hist) {
		if h.Len() == 0 {
			return
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", name,
			h.Pct(50).Round(time.Microsecond), h.Pct(99).Round(time.Microsecond), h.Mean().Round(time.Microsecond))
	}
	row("resolve (host→gw, per query)", &r.Stages.Resolve)
	row("translate (scoped, per gw)", &r.Stages.Translate)
	row("check (router_check_tool, per gw)", &r.Stages.Check)
	row("total (per gw batch)", &r.Stages.Total)
	if r.Notes != "" {
		fmt.Fprintf(&b, "\n%s\n", r.Notes)
	}
	return b.String()
}
