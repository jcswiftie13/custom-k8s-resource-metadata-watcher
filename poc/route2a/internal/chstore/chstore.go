// Package chstore is the ClickHouse-backed versioned config store for the
// ingress-traffic slice of the PoC. It stores the Istio/K8s resources as
// bitemporal version rows and answers two things the real "host+path -> service"
// engine needs:
//
//   - ResolveIPToGateways: the 3-hop `IP -> Gateway` selector join
//     (has(ingress_ips) -> hasAll(pod_labels ⊇ svc.selector) -> hasAll(L ⊇ gw.selector)),
//     each hop a narrow, version-filtered (`valid_from <= T < valid_to`) query.
//   - ScopedFor: rebuilds one gateway's translate input (its Gateway CR + bound
//     VirtualServices + destination Services) FROM ClickHouse at time T, so the
//     translate stage runs off the store exactly like production would.
//
// Every table is ReplacingMergeTree(ingest_seq) ORDER BY (namespace,name,valid_from):
// distinct versions (distinct valid_from) coexist as separate rows; the
// `valid_from <= T < valid_to` predicate selects the one live at T.
//
// # Duplicate-row handling (why queries do NOT use FINAL)
//
// The production writer (the exporter's history ingest) closes a version by
// REWRITING the previous open row — same ORDER BY key, higher ingest_seq,
// valid_to pulled in — so until background merges collapse the pair, both the
// stale open row (valid_to = far-future sentinel) and its closing rewrite are
// visible. The same duplicate shape appears on writer restart/replay (the
// initial re-LIST re-inserts rows already written). Something must therefore
// apply the ReplacingMergeTree rule (max ingest_seq per key) at read time.
//
// FINAL does that server-side but pays the part-merge machinery on every query
// (~10x lookup latency on the bench corpus). Every reader here follows one
// pattern instead:
//
//  1. SQL WHERE carries only IMMUTABLE predicates (join keys, valid_from).
//     valid_to must NEVER be filtered in SQL: the closing rewrite (small
//     valid_to) would be dropped pre-dedup while its stale twin (sentinel
//     valid_to) passes — the stale row would then win dedup unopposed and a
//     dead version would appear live. Same trap as putting valid_to in WHERE
//     instead of HAVING with an argMax rewrite.
//  2. The client dedups rows per version slot (namespace, name, valid_from),
//     keeping the highest ingest_seq (dedupLatest) — the exact collapse
//     ReplacingMergeTree performs at merge time. Distinct versions keep
//     distinct valid_from, so they all survive; two versions landing in the
//     same millisecond leave the earlier one an empty [t,t) interval, which
//     no asOf/overlap predicate can select anyway.
//  3. Liveness/overlap predicates on valid_to (`t < valid_to`,
//     `t0 < valid_to`) are applied AFTER dedup, in Go.
//
// Trade-off: step 1 fetches every version of the scoped keys with
// valid_from <= t (point) / < t1 (range), including versions that ended before
// the query time — bounded by per-key version counts (and table TTL), still
// scoped by the same join predicates as before. In exchange, scans run at
// plain-MergeTree speed with skip indexes fully effective.
//
// # Pruned mode (WithUniqueRows — update-close writers only)
//
// When the writer guarantees one physical row per version (exporter
// closeMode=update, after the historical OPTIMIZE FINAL convergence — see
// docs/lightweight-update-upgrade-plan.md §4/§5), open the store with
// WithUniqueRows: every query then restores the valid_to predicate in SQL
// (prune), so versions that ended at or before the query time are skipped
// server-side instead of fetched. Steps 2 and 3 of the pattern stay in place
// as a zero-cost safety net; CollapsedRows MUST read 0 — a positive count
// means duplicate version slots reached the reader and the writer's
// uniqueness guarantee is broken (alert on it). Do NOT enable against a
// rewrite-close writer: pruning would drop the closing rewrite before dedup
// and the stale sentinel twin would win unopposed.
//
// Time operands in every query are rendered as toDateTime64 literals
// (dt64Lit), never `?` binds — see dt64Lit for the driver trap.
package chstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"google.golang.org/protobuf/encoding/protojson"
	networking "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/host"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/gvk"

	"github.com/example/metadata-exporter/poc/route2a/internal/store"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// insertChunk bounds how many rows buffer client-side before a Send (the backend
// service table is ~24M rows, so streaming in chunks keeps memory flat).
const insertChunk = 100_000

// pjUnmarshal tolerates unknown fields: production spec_json is the API server's
// CR JSON as observed by the exporter, so its field set follows the CLUSTER's CRD
// version, not this binary's compiled istio.io/api version. Discarding (rather
// than failing on) fields this proto doesn't know keeps historical queries
// answerable across version skew — the same choice istiod makes when parsing CRs.
var pjUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// Store wraps a ClickHouse connection.
type Store struct {
	conn driver.Conn
	// uniqueRows asserts the writer guarantees one physical row per version
	// (exporter closeMode=update, after historical convergence). Readers then
	// restore the valid_to predicate in SQL — closed versions are pruned
	// server-side instead of being fetched and filtered post-dedup — and the
	// client dedup degrades to a safety net whose collapse counter
	// (CollapsedRows) MUST stay zero; >0 means the uniqueness guarantee broke.
	// NEVER enable against a rewrite-close writer: its closing rewrite would be
	// pruned by the SQL predicate before dedup and the stale sentinel twin
	// would win unopposed (the exact failure the no-FINAL pattern exists to
	// avoid).
	uniqueRows bool
	collapsed  atomic.Uint64
}

// Option configures Open.
type Option func(*Store)

// WithUniqueRows enables the pruned read mode for update-close writers. See
// Store.uniqueRows and docs/lightweight-update-upgrade-plan.md §4.
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

// CollapsedRows reports how many rows the client-side dedup has discarded over
// this store's lifetime. Under uniqueRows it is the writer-uniqueness alarm:
// any value > 0 means duplicate version slots reached the reader (wire it to a
// metric/alert in production). Without uniqueRows collapses are expected
// (rewrite-close duplicates pre-merge) and the counter is merely descriptive.
func (s *Store) CollapsedRows() uint64 { return s.collapsed.Load() }

// dt64Lit renders t as a DateTime64(3) literal in UTC, truncated to the same
// millisecond precision the columns store. Time operands must NOT be `?`
// binds: clickhouse-go interpolates time.Time at second precision
// (toDateTime), which drops milliseconds and SATURATES on the far-future
// sentinel (2200 > 32-bit DateTime's 2106 ceiling), silently breaking
// equality/range predicates.
func dt64Lit(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 3, 'UTC')", t.UTC().Format("2006-01-02 15:04:05.000"))
}

// prune returns the optional SQL fragment restoring the valid_to predicate
// ("no version that ended at or before t") under uniqueRows, or "" in the
// superset+dedup mode.
func (s *Store) prune(t time.Time) string {
	if !s.uniqueRows {
		return ""
	}
	return " AND " + dt64Lit(t) + " < valid_to"
}

// dedupCounted is dedupLatest with the number of collapsed rows folded into
// the store's CollapsedRows counter.
func dedupCounted[T any](s *Store, rows []T, ver func(T) versionRow) []T {
	before := len(rows)
	out := dedupLatest(rows, ver)
	if n := before - len(out); n > 0 {
		s.collapsed.Add(uint64(n))
	}
	return out
}

// dedupOverlapCounted is dedupOverlap with collapse counting (overlap
// filtering itself is not a collapse — only slot dedup counts).
func dedupOverlapCounted[T any](s *Store, rows []T, ver func(T) versionRow, t0, t1 time.Time) []T {
	rows = dedupCounted(s, rows, ver)
	out := rows[:0]
	for _, r := range rows {
		if ver(r).overlapsWindow(t0, t1) {
			out = append(out, r)
		}
	}
	return out
}

func (s *Store) Close() error { return s.conn.Close() }

// ddl is the four version tables. Selector/label/IP join keys are Array(String)
// so `has`/`hasAll` compare them directly; spec_json carries the full proto for
// gateways/VS reconstruction.
//
// Scale-out direction (not built here — this PoC benchmarks single node): swap
// ReplacingMergeTree for ReplicatedReplacingMergeTree and front the tables with a
// Distributed engine sharded by e.g. cityHash64(namespace, name); every read is
// already namespace/name-scoped so the 3-hop stays shard-local.
var ddl = []string{
	`DROP TABLE IF EXISTS service_versions`,
	`DROP TABLE IF EXISTS deploy_versions`,
	`DROP TABLE IF EXISTS gw_versions`,
	`DROP TABLE IF EXISTS vs_versions`,
	`CREATE TABLE service_versions (
		namespace   LowCardinality(String),
		name        String,
		valid_from  DateTime64(3),
		valid_to    DateTime64(3),
		rev         UInt32,
		ingress_ips Array(String),
		selector_kv Array(String),
		spec_json   String,
		ingest_seq  UInt64,
		INDEX idx_ips ingress_ips TYPE bloom_filter GRANULARITY 1
	) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)`,
	`CREATE TABLE deploy_versions (
		namespace     LowCardinality(String),
		name          String,
		valid_from    DateTime64(3),
		valid_to      DateTime64(3),
		rev           UInt32,
		pod_labels_kv Array(String),
		ingest_seq    UInt64,
		INDEX idx_pod pod_labels_kv TYPE bloom_filter GRANULARITY 1
	) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)`,
	`CREATE TABLE gw_versions (
		namespace    LowCardinality(String),
		name         String,
		valid_from   DateTime64(3),
		valid_to     DateTime64(3),
		rev          UInt32,
		selector_kv  Array(String),
		server_hosts Array(String),
		spec_json    String,
		ingest_seq   UInt64,
		INDEX idx_sel selector_kv TYPE bloom_filter GRANULARITY 1
	) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)`,
	`CREATE TABLE vs_versions (
		namespace      LowCardinality(String),
		name           String,
		valid_from     DateTime64(3),
		valid_to       DateTime64(3),
		rev            UInt32,
		bound_gateways Array(String),
		spec_json      String,
		ingest_seq     UInt64,
		INDEX idx_bg bound_gateways TYPE bloom_filter GRANULARITY 1
	) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)`,
}

// CreateSchema drops and recreates all four tables (idempotent reload).
func (s *Store) CreateSchema(ctx context.Context) error {
	for _, stmt := range ddl {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ddl: %w", err)
		}
	}
	return nil
}

// ---- streaming inserters (auto-chunked) ----

// batch wraps a driver.Batch with periodic Send so huge tables don't buffer
// entirely in memory.
type batch struct {
	ctx  context.Context
	conn driver.Conn
	sql  string
	b    driver.Batch
	n    int
}

func (s *Store) openBatch(ctx context.Context, table string) (*batch, error) {
	sqlStr := "INSERT INTO " + table
	b, err := s.conn.PrepareBatch(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	return &batch{ctx: ctx, conn: s.conn, sql: sqlStr, b: b}, nil
}

func (bt *batch) append(vals ...any) error {
	if err := bt.b.Append(vals...); err != nil {
		return err
	}
	bt.n++
	if bt.n >= insertChunk {
		return bt.flush()
	}
	return nil
}

func (bt *batch) flush() error {
	if bt.n == 0 {
		return nil
	}
	if err := bt.b.Send(); err != nil {
		return err
	}
	nb, err := bt.conn.PrepareBatch(bt.ctx, bt.sql)
	if err != nil {
		return err
	}
	bt.b = nb
	bt.n = 0
	return nil
}

// ServiceBatch / DeployBatch / GwBatch / VSBatch are typed, auto-chunked
// inserters satisfying the store.*Batch interfaces.
type ServiceBatch struct{ *batch }
type DeployBatch struct{ *batch }
type GwBatch struct{ *batch }
type VSBatch struct{ *batch }

func (s *Store) NewServiceBatch(ctx context.Context) (store.ServiceBatch, error) {
	b, err := s.openBatch(ctx, "service_versions")
	return &ServiceBatch{b}, err
}
func (b *ServiceBatch) Append(r store.ServiceRow) error {
	specJSON, err := store.MarshalPorts(r.Ports)
	if err != nil {
		return fmt.Errorf("marshal service ports %s/%s: %w", r.Namespace, r.Name, err)
	}
	return b.append(r.Namespace, r.Name, r.ValidFrom, r.ValidTo, r.Rev,
		r.IngressIPs, r.Selector, specJSON, r.IngestSeq)
}
func (b *ServiceBatch) Close() error { return b.flush() }

func (s *Store) NewDeployBatch(ctx context.Context) (store.DeployBatch, error) {
	b, err := s.openBatch(ctx, "deploy_versions")
	return &DeployBatch{b}, err
}

// AppendDeploy inserts one deploy_versions row.
func (b *DeployBatch) Append(ns, name string, from, to time.Time, rev uint32, podLabels []string, seq uint64) error {
	return b.append(ns, name, from, to, rev, podLabels, seq)
}
func (b *DeployBatch) Close() error { return b.flush() }

func (s *Store) NewGwBatch(ctx context.Context) (store.GwBatch, error) {
	b, err := s.openBatch(ctx, "gw_versions")
	return &GwBatch{b}, err
}
func (b *GwBatch) Append(ns, name string, from, to time.Time, rev uint32, selectorKV, serverHosts []string, specJSON string, seq uint64) error {
	return b.append(ns, name, from, to, rev, selectorKV, serverHosts, specJSON, seq)
}
func (b *GwBatch) Close() error { return b.flush() }

func (s *Store) NewVSBatch(ctx context.Context) (store.VSBatch, error) {
	b, err := s.openBatch(ctx, "vs_versions")
	return &VSBatch{b}, err
}
func (b *VSBatch) Append(ns, name string, from, to time.Time, rev uint32, boundGateways []string, specJSON string, seq uint64) error {
	return b.append(ns, name, from, to, rev, boundGateways, specJSON, seq)
}
func (b *VSBatch) Close() error { return b.flush() }

// ---- queries ----

// versionRow is the dedup/liveness envelope every no-FINAL read scans alongside
// its payload columns: the version-slot identity (namespace, name, valid_from),
// the writer sequence that picks the authoritative row within a slot, and the
// materialized valid_to that liveness is checked against AFTER dedup.
type versionRow struct {
	ns, name string
	vf, vt   time.Time
	seq      uint64
}

func (r versionRow) slot() string {
	return r.ns + "\x00" + r.name + "\x00" + strconv.FormatInt(r.vf.UnixMilli(), 10)
}

// liveAt reports vf <= t < vt (checked post-dedup — see package doc).
func (r versionRow) liveAt(t time.Time) bool { return !r.vf.After(t) && t.Before(r.vt) }

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

// ResolveIPToGateways runs the 3-hop selector join for a destination IP at time
// t: IP -> ingress Service (selector) -> ingress Deployment pod labels L ->
// gateways whose selector ⊆ L. Returns the candidate gateways ("" candidates =>
// traffic miss). Each hop is a narrow, version-scoped query; liveness is
// resolved client-side per the no-FINAL pattern (see package doc).
func (s *Store) ResolveIPToGateways(ctx context.Context, ip string, t time.Time) ([]store.GatewayCand, error) {
	// Hop 1: IP -> ingress Service (its namespace + selector).
	type svcRow struct {
		versionRow
		sel []string
	}
	var svcs []svcRow
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, selector_kv
		 FROM service_versions
		 WHERE has(ingress_ips, ?) AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		ip)
	if err != nil {
		return nil, fmt.Errorf("hop1 (ip->service): %w", err)
	}
	for rows.Next() {
		var r svcRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.sel); err != nil {
			rows.Close()
			return nil, err
		}
		svcs = append(svcs, r)
	}
	if err := closeRows(rows); err != nil {
		return nil, fmt.Errorf("hop1 (ip->service): %w", err)
	}
	svcs = dedupCounted(s, svcs, func(r svcRow) versionRow { return r.versionRow })
	var svcNS string
	var svcSel []string
	found := false
	for _, r := range svcs {
		if r.liveAt(t) {
			svcNS, svcSel, found = r.ns, r.sel, true
			break
		}
	}
	if !found {
		return nil, nil // no ingress serves this IP -> traffic miss
	}

	// Hop 2: Service selector -> ingress Deployment pod labels L (svc.selector ⊆ L).
	type depRow struct {
		versionRow
		labels []string
	}
	var deps []depRow
	rows, err = s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, pod_labels_kv
		 FROM deploy_versions
		 WHERE namespace = ? AND hasAll(pod_labels_kv, ?) AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		svcNS, svcSel)
	if err != nil {
		return nil, fmt.Errorf("hop2 (service->deployment L): %w", err)
	}
	for rows.Next() {
		var r depRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.labels); err != nil {
			rows.Close()
			return nil, err
		}
		deps = append(deps, r)
	}
	if err := closeRows(rows); err != nil {
		return nil, fmt.Errorf("hop2 (service->deployment L): %w", err)
	}
	deps = dedupCounted(s, deps, func(r depRow) versionRow { return r.versionRow })
	var podLabels []string
	found = false
	for _, r := range deps {
		if r.liveAt(t) {
			podLabels, found = r.labels, true
			break
		}
	}
	if !found {
		return nil, nil // ingress workload not found -> miss
	}

	// Hop 3: L -> candidate gateways (gateway.selector ⊆ L).
	gws, err := s.gatewayCandsAt(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, server_hosts
		 FROM gw_versions
		 WHERE hasAll(?, selector_kv) AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		t, podLabels)
	if err != nil {
		return nil, fmt.Errorf("hop3 (L->gateways): %w", err)
	}
	return gws, nil
}

// AllGatewaysLiveAt returns every gateway version live at time t (name + server
// hosts), for the config-only path (no IP 3-hop) that disambiguates a host
// across all gateways. Mirrors hop3's scan shape without the selector join.
func (s *Store) AllGatewaysLiveAt(ctx context.Context, t time.Time) ([]store.GatewayCand, error) {
	gws, err := s.gatewayCandsAt(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, server_hosts
		 FROM gw_versions
		 WHERE valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		t)
	if err != nil {
		return nil, fmt.Errorf("all gateways live at t: %w", err)
	}
	return gws, nil
}

// gatewayCandsAt runs one gw_versions query (dedup envelope + server_hosts),
// dedups, and returns the candidates live at t.
func (s *Store) gatewayCandsAt(ctx context.Context, query string, t time.Time, args ...any) ([]store.GatewayCand, error) {
	type gwRow struct {
		versionRow
		hosts []string
	}
	var gws []gwRow
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r gwRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.hosts); err != nil {
			rows.Close()
			return nil, err
		}
		gws = append(gws, r)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	gws = dedupCounted(s, gws, func(r gwRow) versionRow { return r.versionRow })
	var cands []store.GatewayCand
	for _, r := range gws {
		if r.liveAt(t) {
			cands = append(cands, store.GatewayCand{Namespace: r.ns, Name: r.name, ServerHosts: r.hosts})
		}
	}
	return cands, nil
}

// closeRows drains errors and closes (Err then Close, first error wins).
func closeRows(rows driver.Rows) error {
	err := rows.Err()
	if cerr := rows.Close(); err == nil {
		err = cerr
	}
	return err
}

// ScopedFor rebuilds one gateway's translate input from ClickHouse at time t: its
// Gateway CR + bound VirtualServices + the destination Services those VS route
// to (all as-of-t versions). This is the store-backed replacement for the
// in-memory scalegen.ScopedFor — the translate stage runs entirely off CH.
func (s *Store) ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error) {
	// Gateway CR (1 current version): fetch this gateway's version slots, dedup,
	// pick the one live at t.
	type gwRow struct {
		versionRow
		spec string
	}
	var gwVers []gwRow
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, spec_json
		 FROM gw_versions
		 WHERE name = ? AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		gwName)
	if err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor gateway: %w", err)
	}
	for rows.Next() {
		var r gwRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.spec); err != nil {
			rows.Close()
			return translate.ScopedInput{}, false, err
		}
		gwVers = append(gwVers, r)
	}
	if err := closeRows(rows); err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor gateway: %w", err)
	}
	gwVers = dedupCounted(s, gwVers, func(r gwRow) versionRow { return r.versionRow })
	var gwNS, gwJSON string
	found := false
	for _, r := range gwVers {
		if r.liveAt(t) {
			gwNS, gwJSON, found = r.ns, r.spec, true
			break
		}
	}
	if !found {
		return translate.ScopedInput{}, false, nil
	}
	var gwSpec networking.Gateway
	if err := pjUnmarshal.Unmarshal([]byte(gwJSON), &gwSpec); err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("unmarshal gateway: %w", err)
	}
	cfgs := []config.Config{{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: gwName, Namespace: gwNS},
		Spec: &gwSpec,
	}}

	// Bound VirtualServices (each 1 current version). bound_gateways carries the
	// RAW spec.gateways strings, and Istio accepts two forms: the qualified
	// "<ns>/<name>", and the bare "<name>" which binds a VS to a gateway in its
	// OWN namespace — hence the second disjunct scoped to the gateway's namespace.
	type vsRow struct {
		versionRow
		spec string
	}
	var vsVers []vsRow
	rows, err = s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, spec_json
		 FROM vs_versions
		 WHERE (has(bound_gateways, ?) OR (namespace = ? AND has(bound_gateways, ?)))
		   AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		gwNS+"/"+gwName, gwNS, gwName)
	if err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor vs: %w", err)
	}
	for rows.Next() {
		var r vsRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.spec); err != nil {
			rows.Close()
			return translate.ScopedInput{}, false, err
		}
		vsVers = append(vsVers, r)
	}
	if err := closeRows(rows); err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor vs: %w", err)
	}
	vsVers = dedupCounted(s, vsVers, func(r vsRow) versionRow { return r.versionRow })
	var destHosts []string
	for _, r := range vsVers {
		if !r.liveAt(t) {
			continue
		}
		var vsSpec networking.VirtualService
		if err := pjUnmarshal.Unmarshal([]byte(r.spec), &vsSpec); err != nil {
			return translate.ScopedInput{}, false, fmt.Errorf("unmarshal vs %s/%s: %w", r.ns, r.name, err)
		}
		destHosts = append(destHosts, vsDestHosts(&vsSpec)...)
		cfgs = append(cfgs, config.Config{
			Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: r.name, Namespace: r.ns},
			Spec: &vsSpec,
		})
	}

	// Destination Services: the backend Services the bound VS route to, identified
	// by the (namespace, name) parsed from each route's destination.host FQDN. This
	// is namespace-portable — in production backend Service ns == VS ns != gateway
	// ns, so we must NOT scope by the gateway's namespace.
	svcs, err := s.backendServices(ctx, destHosts, t)
	if err != nil {
		return translate.ScopedInput{}, false, err
	}

	return translate.ScopedInput{
		Configs:  cfgs,
		Services: svcs,
		Proxy:    translate.GatewayProxy{Name: gwName, Namespace: gwNS, Labels: gwSpec.GetSelector()},
	}, true, nil
}

// backendServices reads the destination Services the bound VS route to (identity
// = the (namespace, name) parsed from each route's destination.host FQDN) as-of
// time t and rebuilds them as istiod model.Services so the config generator can
// build clusters for them. Each row is one Service carrying all its ports in
// spec_json, so a multi-port Service becomes one model.Service with a multi-port
// PortList. Keying by (namespace, name) is namespace-portable — the FQDN encodes
// the Service's own namespace, which need not equal the gateway's — and hits the
// ORDER BY (namespace, name, ...) prefix. The FQDN<->identity mapping is Istio
// derivation, so it lives here in the reader (see store.ParseBackendHost).
func (s *Store) backendServices(ctx context.Context, hosts []string, t time.Time) ([]*model.Service, error) {
	keys := backendKeys(hosts)
	if len(keys) == 0 {
		return nil, nil
	}
	type bsRow struct {
		versionRow
		spec string
	}
	var vers []bsRow
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT namespace, name, valid_from, valid_to, ingest_seq, spec_json
		 FROM service_versions
		 WHERE has(?, concat(namespace, '/', name)) AND valid_from <= %s%s`, dt64Lit(t), s.prune(t)),
		keys)
	if err != nil {
		return nil, fmt.Errorf("scopedfor backend services: %w", err)
	}
	for rows.Next() {
		var r bsRow
		if err := rows.Scan(&r.ns, &r.name, &r.vf, &r.vt, &r.seq, &r.spec); err != nil {
			rows.Close()
			return nil, err
		}
		vers = append(vers, r)
	}
	if err := closeRows(rows); err != nil {
		return nil, fmt.Errorf("scopedfor backend services: %w", err)
	}
	vers = dedupCounted(s, vers, func(r bsRow) versionRow { return r.versionRow })
	var out []*model.Service
	for _, r := range vers {
		if !r.liveAt(t) {
			continue
		}
		ports, err := store.ParsePorts(r.spec)
		if err != nil {
			return nil, fmt.Errorf("parse ports %s/%s: %w", r.ns, r.name, err)
		}
		out = append(out, backendModel(r.ns, r.name, ports))
	}
	return out, nil
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

// backendModel rebuilds one backend Service as an istiod model.Service: FQDN
// hostname derived from (name, namespace), one cluster-eligible Port per port.
func backendModel(ns, name string, ports []store.SvcPort) *model.Service {
	pl := make(model.PortList, 0, len(ports))
	for _, p := range ports {
		pl = append(pl, &model.Port{Name: p.Name, Port: int(p.Port), Protocol: protocol.Parse(p.Name)})
	}
	return &model.Service{
		Hostname:       host.Name(store.BackendFQDN(name, ns)),
		DefaultAddress: "0.0.0.0",
		Ports:          pl,
		Attributes:     model.ServiceAttributes{Namespace: ns},
	}
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
// per-instant predicate, so extra rows are harmless. The hops stay scoped like the
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
	ingSvcs = dedupOverlapCounted(s, ingSvcs, svcVer, t0, t1)
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
	// labels ⊇ the ingress service selector (mirrors single-point hop 2). Filtering
	// by selector keeps the label union to just the ingress deployment(s), so hop 3
	// resolves to the right gateway(s) instead of every gateway in the namespace.
	// Rows are collected across all selector queries first, then deduped once
	// (the same deploy version can match several selectors).
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
	deps = dedupOverlapCounted(s, deps, func(r store.DeployRow) versionRow {
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
		gws = dedupOverlapCounted(s, gws, func(r store.GatewayRow) versionRow {
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
		vses = dedupOverlapCounted(s, vses, func(r store.VSRow) versionRow {
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
	w.Services = append(w.Services, dedupOverlapCounted(s, backends, svcVer, t0, t1)...)

	return w, nil
}

// dedupOverlap applies the no-FINAL read pattern to one batch of version rows:
// dedup per version slot (max ingest_seq), then keep only versions overlapping
// [t0,t1) — the valid_to half of the Overlap predicate, which must not run in
// SQL (see package doc).
func dedupOverlap[T any](rows []T, ver func(T) versionRow, t0, t1 time.Time) []T {
	rows = dedupLatest(rows, ver)
	out := rows[:0]
	for _, r := range rows {
		if ver(r).overlapsWindow(t0, t1) {
			out = append(out, r)
		}
	}
	return out
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

// CountRows returns the row count of a table (all versions).
func (s *Store) CountRows(ctx context.Context, table string) (uint64, error) {
	var n uint64
	err := s.conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n)
	return n, err
}

// AsOfRev returns the version rev live at time t for one resource (for the
// multi-version selection check). ok=false if none is live at t.
//
// PoC-oracle only: it reads the synthetic `rev` column the PoC loader writes.
// Production tables (written by the exporter's history ingest) have no rev
// column — versions there are identified by resource_version/ingest_seq — so
// this must never move into the production reader path.
func (s *Store) AsOfRev(ctx context.Context, table, ns, name string, t time.Time) (uint32, bool, error) {
	var rev uint32
	err := s.conn.QueryRow(ctx,
		"SELECT rev FROM "+table+" WHERE namespace = ? AND name = ? AND valid_from <= ? AND ? < valid_to LIMIT 1",
		ns, name, t, t).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return rev, err == nil, err
}
