// Package chstore is the read-only ClickHouse client of the routing-history
// range query. It answers exactly one question — LoadTrafficWindow: every
// resource version overlapping [t0,t1) reachable from a destination IP — and the
// in-memory window (internal/memwindow) does all further slicing/resolution.
//
// The tables it reads are written by the metadata-exporter's history ingest
// (pkg/history + pkg/store): ReplacingMergeTree(ingest_seq) with ORDER BY
// (namespace, name, valid_from, resource_version, deleted) — see the main
// module's pkg/store/ddl.go. That writer closes a version by REWRITING the
// previous open row (same ORDER BY key, higher ingest_seq, valid_to pulled in),
// so until background merges collapse the pair, both the stale open row
// (valid_to = far-future sentinel) and its closing rewrite are visible. The
// same duplicate shape appears on writer restart/replay (the initial re-LIST
// re-inserts rows already written). Something must therefore apply the
// ReplacingMergeTree rule (max ingest_seq per key) at read time.
//
// FINAL does that server-side but pays the part-merge machinery on every query
// (~10x lookup latency on the poc bench corpus). Queries here follow the
// no-FINAL pattern instead:
//
//  1. SQL WHERE carries only IMMUTABLE predicates (join keys, valid_from < t1).
//     valid_to must NEVER be filtered in SQL: the closing rewrite (small
//     valid_to) would be dropped pre-dedup while its stale twin (sentinel
//     valid_to) passes — the stale row would then win dedup unopposed and a
//     dead version would appear live throughout the window.
//  2. The client dedups rows per version slot (namespace, name, valid_from),
//     keeping the highest ingest_seq (dedupLatest) — the exact collapse
//     ReplacingMergeTree performs at merge time. Distinct versions keep
//     distinct valid_from so they all survive; two versions landing in the
//     same millisecond leave the earlier one an empty [t,t) interval, which
//     no asOf/overlap predicate can select anyway.
//  3. The Overlap predicate on valid_to (t0 < valid_to) is applied AFTER
//     dedup, in Go (dedupOverlap).
//
// Trade-off: step 1 also fetches versions that ended before t0 — bounded by
// per-key version counts (and table TTL), still scoped by the same join
// predicates. In exchange, scans run at plain-MergeTree speed with skip indexes
// fully effective.
//
// # Pruned mode (WithUniqueRows — update-close writers only)
//
// When the writer guarantees one physical row per version (exporter
// closeMode=update, after historical convergence — see
// docs/lightweight-update-upgrade-plan.md §4/§5), open the store with
// WithUniqueRows: queries then restore the valid_to Overlap predicate in SQL
// (prune) and skip closed versions server-side. The client dedup stays as a
// zero-cost safety net; CollapsedRows MUST read 0 — a positive count means the
// writer's uniqueness guarantee is broken. Do NOT enable against a
// rewrite-close writer: pruning would drop the closing rewrite before dedup
// and the stale sentinel twin would win unopposed.
//
// Time operands are rendered as toDateTime64 literals (dt64Lit), never `?`
// binds — clickhouse-go interpolates time.Time at second precision
// (toDateTime), which drops milliseconds and saturates beyond 2106.
package chstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"google.golang.org/protobuf/encoding/protojson"
	networking "istio.io/api/networking/v1alpha3"

	"github.com/example/metadata-exporter/test/integration/routesim/internal/store"
)

// pjUnmarshal tolerates unknown fields: production spec_json is the API server's
// CR JSON as observed by the exporter, so its field set follows the CLUSTER's CRD
// version, not this binary's compiled istio.io/api version. Discarding (rather
// than failing on) fields this proto doesn't know keeps historical queries
// answerable across version skew — the same choice istiod makes when parsing CRs.
var pjUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// Store wraps a ClickHouse connection.
type Store struct {
	conn       driver.Conn
	uniqueRows bool // see package doc: pruned mode for update-close writers
	collapsed  atomic.Uint64
}

// Option configures Open.
type Option func(*Store)

// WithUniqueRows enables the pruned read mode for update-close writers. See
// the package doc and docs/lightweight-update-upgrade-plan.md §4.
func WithUniqueRows() Option { return func(s *Store) { s.uniqueRows = true } }

// Open dials ClickHouse (native protocol, e.g. "127.0.0.1:9000") and pings it.
func Open(ctx context.Context, addr string, opts ...Option) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: "default", Username: "default"},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse %s: %w", addr, err)
	}
	st := &Store{conn: conn}
	for _, o := range opts {
		o(st)
	}
	return st, nil
}

// CollapsedRows reports how many rows the client-side dedup has discarded.
// Under WithUniqueRows any value > 0 means the writer's one-row-per-version
// guarantee is broken (the e2e asserts it stays 0); without it, collapses are
// expected (rewrite-close duplicates pre-merge).
func (s *Store) CollapsedRows() uint64 { return s.collapsed.Load() }

// dt64Lit renders t as a DateTime64(3) literal in UTC, truncated to the same
// millisecond precision the columns store. Never bind time operands with `?`.
func dt64Lit(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", t.UTC().Format("2006-01-02 15:04:05.000"))
}

// prune returns the optional SQL fragment restoring the valid_to Overlap
// predicate under uniqueRows, or "" in the superset+dedup mode.
func (s *Store) prune(t0 time.Time) string {
	if !s.uniqueRows {
		return ""
	}
	return " AND " + dt64Lit(t0) + " < valid_to"
}

func (s *Store) Close() error { return s.conn.Close() }

// versionRow is the dedup/liveness envelope every read scans alongside its
// payload columns: the version-slot identity (namespace, name, valid_from), the
// writer sequence that picks the authoritative row within a slot, and the
// materialized valid_to that overlap is checked against AFTER dedup.
type versionRow struct {
	ns, name string
	vf, vt   time.Time
	seq      uint64
}

func (r versionRow) slot() string {
	return r.ns + "\x00" + r.name + "\x00" + strconv.FormatInt(r.vf.UnixMilli(), 10)
}

// overlapsWindow reports vf < t1 && t0 < vt (checked post-dedup).
func (r versionRow) overlapsWindow(t0, t1 time.Time) bool {
	return r.vf.Before(t1) && t0.Before(r.vt)
}

// dedupLatest keeps, per version slot (namespace, name, valid_from), only the
// row with the highest ingest_seq — the reader-side equivalent of the
// ReplacingMergeTree(ingest_seq) collapse FINAL would apply server-side.
// First-seen slot order is preserved.
func dedupLatest[T any](rows []T, ver func(T) versionRow) []T {
	if len(rows) < 2 {
		return rows
	}
	idx := make(map[string]int, len(rows))
	out := rows[:0]
	for _, r := range rows {
		k := ver(r).slot()
		if i, seen := idx[k]; seen {
			if ver(r).seq > ver(out[i]).seq {
				out[i] = r
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, r)
	}
	return out
}

// dedupOverlap applies the no-FINAL read pattern to one batch of version rows:
// dedup per version slot (max ingest_seq) with collapse counting, then keep
// only versions overlapping [t0,t1) — the valid_to half of the Overlap
// predicate, which must not run in SQL unless uniqueRows holds (see package
// doc).
func dedupOverlap[T any](s *Store, rows []T, ver func(T) versionRow, t0, t1 time.Time) []T {
	before := len(rows)
	rows = dedupLatest(rows, ver)
	if n := before - len(rows); n > 0 {
		s.collapsed.Add(uint64(n))
	}
	out := rows[:0]
	for _, r := range rows {
		if ver(r).overlapsWindow(t0, t1) {
			out = append(out, r)
		}
	}
	return out
}

// closeRows drains errors and closes (Err then Close, first error wins).
func closeRows(rows driver.Rows) error {
	err := rows.Err()
	if cerr := rows.Close(); err == nil {
		err = cerr
	}
	return err
}

// LoadTrafficWindow fetches every resource version overlapping [t0,t1) reachable
// from destination IP, in one scoped query per resource kind. Per the no-FINAL
// pattern (package doc), SQL filters only on immutable columns
// (valid_from < t1 + the join predicates); rows are then deduped per version
// slot (max ingest_seq) and the Overlap predicate on the materialized valid_to
// (t0 < valid_to) is applied client-side AFTER dedup. The returned rows carry
// their valid_to so internal/memwindow can slice and resolve in memory.
//
// Each hop loads a correct SUPERSET of what any single-instant point query in the
// window would touch (e.g. gateways whose selector ⊆ the union of the ingress
// deployment's pod labels across versions); memwindow re-applies the exact
// per-instant predicate, so extra rows are harmless. The hops stay scoped like a
// point query (deploys are still selector-filtered) so the superset does not blow
// up on a shared ingress namespace.
func (s *Store) LoadTrafficWindow(ctx context.Context, ip string, t0, t1 time.Time) (store.TrafficWindow, error) {
	var w store.TrafficWindow

	svcVer := func(r store.ServiceRow) versionRow {
		return versionRow{ns: r.Namespace, name: r.Name, vf: r.ValidFrom, vt: r.ValidTo, seq: r.IngestSeq}
	}

	// 1. Ingress Service versions serving this IP.
	svcRows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingress_ips, selector_kv, spec_json, ingest_seq
		 FROM service_versions
		 WHERE has(ingress_ips, ?) AND valid_from < %s%s`, dt64Lit(t1), s.prune(t0)),
		ip)
	if err != nil {
		return w, fmt.Errorf("window ingress services: %w", err)
	}
	var ingSvcs []store.ServiceRow
	for svcRows.Next() {
		r, err := scanServiceRow(svcRows)
		if err != nil {
			svcRows.Close()
			return w, err
		}
		ingSvcs = append(ingSvcs, r)
	}
	if err := closeRows(svcRows); err != nil {
		return w, fmt.Errorf("window ingress services: %w", err)
	}
	ingSvcs = dedupOverlap(s, ingSvcs, svcVer, t0, t1)
	nsSeen := map[string]bool{}
	var nsList []string
	selSeen := map[string]bool{}
	var selectors [][]string // distinct ingress-service selectors across versions
	for _, r := range ingSvcs {
		w.Services = append(w.Services, r)
		if !nsSeen[r.Namespace] {
			nsSeen[r.Namespace] = true
			nsList = append(nsList, r.Namespace)
		}
		// An empty selector selects every pod — skip it, or hop 2 would load every
		// deployment in the (shared) ingress namespace and blow up the label union.
		if k := strings.Join(r.Selector, "\x00"); len(r.Selector) > 0 && !selSeen[k] {
			selSeen[k] = true
			selectors = append(selectors, r.Selector)
		}
	}
	if len(nsList) == 0 || len(selectors) == 0 {
		return w, nil // no ingress serves this IP in the window -> empty
	}

	// 2. Ingress Deployment versions: those in the ingress namespace(s) whose pod
	// labels ⊇ the ingress service selector (mirrors a point query's hop 2).
	// Filtering by selector keeps the label union to just the ingress
	// deployment(s), so hop 3 resolves to the right gateway(s) instead of every
	// gateway in the namespace. Rows are collected across all selector queries
	// first, then deduped once (the same deploy version can match several
	// selectors).
	var deps []store.DeployRow
	for _, sel := range selectors {
		depRows, err := s.conn.Query(ctx, fmt.Sprintf(
			`SELECT namespace, name, valid_from, valid_to, pod_labels_kv, ingest_seq
			 FROM deploy_versions
			 WHERE has(?, namespace) AND hasAll(pod_labels_kv, ?) AND valid_from < %s%s`, dt64Lit(t1), s.prune(t0)),
			nsList, sel)
		if err != nil {
			return w, fmt.Errorf("window deploys: %w", err)
		}
		for depRows.Next() {
			var r store.DeployRow
			if err := depRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.PodLabels, &r.IngestSeq); err != nil {
				depRows.Close()
				return w, err
			}
			deps = append(deps, r)
		}
		if err := closeRows(depRows); err != nil {
			return w, fmt.Errorf("window deploys: %w", err)
		}
	}
	deps = dedupOverlap(s, deps, func(r store.DeployRow) versionRow {
		return versionRow{ns: r.Namespace, name: r.Name, vf: r.ValidFrom, vt: r.ValidTo, seq: r.IngestSeq}
	}, t0, t1)
	labelSeen := map[string]bool{}
	var labelUnion []string
	for _, r := range deps {
		w.Deploys = append(w.Deploys, r)
		for _, l := range r.PodLabels {
			if !labelSeen[l] {
				labelSeen[l] = true
				labelUnion = append(labelUnion, l)
			}
		}
	}

	// 3. Gateway versions whose selector ⊆ the pod-label union.
	var gwRefs []string
	if len(labelUnion) > 0 {
		gwRows, err := s.conn.Query(ctx, fmt.Sprintf(
			`SELECT namespace, name, valid_from, valid_to, selector_kv, server_hosts, spec_json, ingest_seq
			 FROM gw_versions
			 WHERE hasAll(?, selector_kv) AND valid_from < %s%s`, dt64Lit(t1), s.prune(t0)),
			labelUnion)
		if err != nil {
			return w, fmt.Errorf("window gateways: %w", err)
		}
		var gws []store.GatewayRow
		for gwRows.Next() {
			var r store.GatewayRow
			if err := gwRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo,
				&r.SelectorKV, &r.ServerHosts, &r.SpecJSON, &r.IngestSeq); err != nil {
				gwRows.Close()
				return w, err
			}
			gws = append(gws, r)
		}
		if err := closeRows(gwRows); err != nil {
			return w, fmt.Errorf("window gateways: %w", err)
		}
		gws = dedupOverlap(s, gws, func(r store.GatewayRow) versionRow {
			return versionRow{ns: r.Namespace, name: r.Name, vf: r.ValidFrom, vt: r.ValidTo, seq: r.IngestSeq}
		}, t0, t1)
		refSeen := map[string]bool{}
		for _, r := range gws {
			w.Gateways = append(w.Gateways, r)
			// bound_gateways holds RAW spec.gateways strings; a VS may reference this
			// gateway either qualified ("ns/name") or bare ("name", same-namespace
			// binding). Load the superset by matching both forms — memwindow re-applies
			// the exact per-instant predicate (boundTo) with the namespace check.
			for _, ref := range []string{r.Namespace + "/" + r.Name, r.Name} {
				if !refSeen[ref] {
					refSeen[ref] = true
					gwRefs = append(gwRefs, ref)
				}
			}
		}
	}

	// 4. VirtualService versions bound to any candidate gateway.
	var destHosts []string
	if len(gwRefs) > 0 {
		vsRows, err := s.conn.Query(ctx, fmt.Sprintf(
			`SELECT namespace, name, valid_from, valid_to, bound_gateways, spec_json, ingest_seq
			 FROM vs_versions
			 WHERE hasAny(bound_gateways, ?) AND valid_from < %s%s`, dt64Lit(t1), s.prune(t0)),
			gwRefs)
		if err != nil {
			return w, fmt.Errorf("window virtualservices: %w", err)
		}
		var vses []store.VSRow
		for vsRows.Next() {
			var r store.VSRow
			if err := vsRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo,
				&r.BoundGateways, &r.SpecJSON, &r.IngestSeq); err != nil {
				vsRows.Close()
				return w, err
			}
			vses = append(vses, r)
		}
		if err := closeRows(vsRows); err != nil {
			return w, fmt.Errorf("window virtualservices: %w", err)
		}
		vses = dedupOverlap(s, vses, func(r store.VSRow) versionRow {
			return versionRow{ns: r.Namespace, name: r.Name, vf: r.ValidFrom, vt: r.ValidTo, seq: r.IngestSeq}
		}, t0, t1)
		hostSeen := map[string]bool{}
		for _, r := range vses {
			w.VSes = append(w.VSes, r)
			var vsSpec networking.VirtualService
			if err := pjUnmarshal.Unmarshal([]byte(r.SpecJSON), &vsSpec); err != nil {
				return w, fmt.Errorf("window unmarshal vs %s/%s: %w", r.Namespace, r.Name, err)
			}
			for _, h := range vsDestHosts(&vsSpec) {
				if !hostSeen[h] {
					hostSeen[h] = true
					destHosts = append(destHosts, h)
				}
			}
		}
	}

	// 5. Backend Service versions those VS route to (identity = (namespace, name)
	// parsed from each route's destination.host FQDN). Chunked so a gateway with
	// very many routes can't inline a key list past ClickHouse's max_query_size.
	var backends []store.ServiceRow
	for _, chunk := range chunkStrings(backendKeys(destHosts), hostChunk) {
		bsRows, err := s.conn.Query(ctx, fmt.Sprintf(
			`SELECT namespace, name, valid_from, valid_to, ingress_ips, selector_kv, spec_json, ingest_seq
			 FROM service_versions
			 WHERE has(?, concat(namespace, '/', name)) AND valid_from < %s%s`, dt64Lit(t1), s.prune(t0)),
			chunk)
		if err != nil {
			return w, fmt.Errorf("window backend services: %w", err)
		}
		for bsRows.Next() {
			r, err := scanServiceRow(bsRows)
			if err != nil {
				bsRows.Close()
				return w, err
			}
			backends = append(backends, r)
		}
		if err := closeRows(bsRows); err != nil {
			return w, fmt.Errorf("window backend services: %w", err)
		}
	}
	w.Services = append(w.Services, dedupOverlap(s, backends, svcVer, t0, t1)...)

	return w, nil
}

// scanServiceRow scans one full service_versions row, decoding spec_json into
// Ports. Shared by the ingress (step 1) and backend (step 5) window loads.
func scanServiceRow(rows driver.Rows) (store.ServiceRow, error) {
	var r store.ServiceRow
	var specJSON string
	if err := rows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo,
		&r.IngressIPs, &r.Selector, &specJSON, &r.IngestSeq); err != nil {
		return r, err
	}
	ports, err := store.ParsePorts(specJSON)
	if err != nil {
		return r, fmt.Errorf("parse ports %s/%s: %w", r.Namespace, r.Name, err)
	}
	r.Ports = ports
	return r, nil
}

// backendKeys maps VS destination.host FQDNs to the "namespace/name" identity
// keys used to look up backend Service rows (has(?, concat(namespace,'/',name))).
// Hosts that aren't resolvable in-cluster FQDNs are skipped.
func backendKeys(hosts []string) []string {
	keys := make([]string, 0, len(hosts))
	for _, h := range hosts {
		if name, ns, ok := store.ParseBackendHost(h); ok {
			keys = append(keys, ns+"/"+name)
		}
	}
	return keys
}

// vsDestHosts collects the destination host of every route in a VirtualService
// (HTTP/TLS/TCP). Each host is the target Service's identity — an FQDN like
// svc.ns.svc.cluster.local — from which the backend lookup derives (name, ns).
func vsDestHosts(vs *networking.VirtualService) []string {
	var hosts []string
	add := func(d *networking.Destination) {
		if d != nil && d.GetHost() != "" {
			hosts = append(hosts, d.GetHost())
		}
	}
	for _, r := range vs.GetHttp() {
		for _, rd := range r.GetRoute() {
			add(rd.GetDestination())
		}
		if m := r.GetMirror(); m != nil {
			add(m)
		}
	}
	for _, r := range vs.GetTls() {
		for _, rd := range r.GetRoute() {
			add(rd.GetDestination())
		}
	}
	for _, r := range vs.GetTcp() {
		for _, rd := range r.GetRoute() {
			add(rd.GetDestination())
		}
	}
	return hosts
}

// hostChunk bounds how many host FQDNs go into one IN-list, keeping the inlined
// query text well under ClickHouse's default max_query_size (256 KB).
const hostChunk = 2000

// chunkStrings splits s into consecutive slices of at most n (n<=0 => one chunk).
func chunkStrings(s []string, n int) [][]string {
	if n <= 0 || len(s) <= n {
		if len(s) == 0 {
			return nil
		}
		return [][]string{s}
	}
	var out [][]string
	for i := 0; i < len(s); i += n {
		end := i + n
		if end > len(s) {
			end = len(s)
		}
		out = append(out, s[i:end])
	}
	return out
}
