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
// `valid_from <= T < valid_to` predicate selects the one live at T. The PoC writes
// each version exactly once (valid_to is computed at load time), so there are no
// same-version rewrites to collapse and queries run without FINAL. Range queries
// (LoadTrafficWindow) reuse the same materialized valid_to for the Overlap
// predicate valid_from < t1 AND t0 < valid_to.
package chstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// Store wraps a ClickHouse connection.
type Store struct{ conn driver.Conn }

// Open dials ClickHouse (native protocol, e.g. "127.0.0.1:9000") and pings it.
func Open(ctx context.Context, addr string) (*Store, error) {
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
	return &Store{conn: conn}, nil
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
		hostname    String,
		port        UInt32,
		port_name   String,
		protocol    String,
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
	return b.append(r.Namespace, r.Name, r.ValidFrom, r.ValidTo, r.Rev,
		r.IngressIPs, r.Selector, r.Hostname, r.Port, r.PortName, r.Protocol, r.IngestSeq)
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

// ResolveIPToGateways runs the 3-hop selector join for a destination IP at time
// t: IP -> ingress Service (selector) -> ingress Deployment pod labels L ->
// gateways whose selector ⊆ L. Returns the candidate gateways ("" candidates =>
// traffic miss). Each hop is a narrow, version-filtered query (no cross product).
func (s *Store) ResolveIPToGateways(ctx context.Context, ip string, t time.Time) ([]store.GatewayCand, error) {
	// Hop 1: IP -> ingress Service (its namespace + selector).
	var svcNS string
	var svcSel []string
	err := s.conn.QueryRow(ctx,
		`SELECT namespace, selector_kv FROM service_versions
		 WHERE has(ingress_ips, ?) AND valid_from <= ? AND ? < valid_to LIMIT 1`,
		ip, t, t).Scan(&svcNS, &svcSel)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // no ingress serves this IP -> traffic miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop1 (ip->service): %w", err)
	}

	// Hop 2: Service selector -> ingress Deployment pod labels L (svc.selector ⊆ L).
	var podLabels []string
	err = s.conn.QueryRow(ctx,
		`SELECT pod_labels_kv FROM deploy_versions
		 WHERE namespace = ? AND hasAll(pod_labels_kv, ?) AND valid_from <= ? AND ? < valid_to LIMIT 1`,
		svcNS, svcSel, t, t).Scan(&podLabels)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // ingress workload not found -> miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop2 (service->deployment L): %w", err)
	}

	// Hop 3: L -> candidate gateways (gateway.selector ⊆ L).
	rows, err := s.conn.Query(ctx,
		`SELECT namespace, name, server_hosts FROM gw_versions
		 WHERE hasAll(?, selector_kv) AND valid_from <= ? AND ? < valid_to`,
		podLabels, t, t)
	if err != nil {
		return nil, fmt.Errorf("hop3 (L->gateways): %w", err)
	}
	defer rows.Close()

	var cands []store.GatewayCand
	for rows.Next() {
		var c store.GatewayCand
		if err := rows.Scan(&c.Namespace, &c.Name, &c.ServerHosts); err != nil {
			return nil, err
		}
		cands = append(cands, c)
	}
	return cands, rows.Err()
}

// ScopedFor rebuilds one gateway's translate input from ClickHouse at time t: its
// Gateway CR + bound VirtualServices + the destination Services those VS route
// to (all as-of-t versions). This is the store-backed replacement for the
// in-memory scalegen.ScopedFor — the translate stage runs entirely off CH.
func (s *Store) ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error) {
	// Gateway CR (1 current version).
	var gwNS, gwJSON string
	err := s.conn.QueryRow(ctx,
		`SELECT namespace, spec_json FROM gw_versions 
		 WHERE name = ? AND valid_from <= ? AND ? < valid_to LIMIT 1`,
		gwName, t, t).Scan(&gwNS, &gwJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return translate.ScopedInput{}, false, nil
	}
	if err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor gateway: %w", err)
	}
	var gwSpec networking.Gateway
	if err := protojson.Unmarshal([]byte(gwJSON), &gwSpec); err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("unmarshal gateway: %w", err)
	}
	cfgs := []config.Config{{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: gwName, Namespace: gwNS},
		Spec: &gwSpec,
	}}

	// Bound VirtualServices (each 1 current version).
	vsRows, err := s.conn.Query(ctx,
		`SELECT namespace, name, spec_json FROM vs_versions 
		 WHERE has(bound_gateways, ?) AND valid_from <= ? AND ? < valid_to`,
		gwNS+"/"+gwName, t, t)
	if err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor vs: %w", err)
	}
	defer vsRows.Close()
	var destHosts []string
	for vsRows.Next() {
		var ns, name, js string
		if err := vsRows.Scan(&ns, &name, &js); err != nil {
			return translate.ScopedInput{}, false, err
		}
		var vsSpec networking.VirtualService
		if err := protojson.Unmarshal([]byte(js), &vsSpec); err != nil {
			return translate.ScopedInput{}, false, fmt.Errorf("unmarshal vs %s/%s: %w", ns, name, err)
		}
		destHosts = append(destHosts, vsDestHosts(&vsSpec)...)
		cfgs = append(cfgs, config.Config{
			Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: name, Namespace: ns},
			Spec: &vsSpec,
		})
	}
	if err := vsRows.Err(); err != nil {
		return translate.ScopedInput{}, false, err
	}

	// Destination Services: the backend Services the bound VS route to, keyed by
	// the destination host FQDN in each VS's routes (the Service's identity). This
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

// backendServices reads the destination Services whose hostname matches one of
// the given VS route destination hosts (the Service's identity FQDN) as-of time t
// and rebuilds them as istiod model.Services so the config generator can build
// clusters for them. Keying by hostname (not namespace) is namespace-portable:
// the FQDN encodes the Service's own namespace, which need not equal the gateway's.
func (s *Store) backendServices(ctx context.Context, hosts []string, t time.Time) ([]*model.Service, error) {
	if len(hosts) == 0 {
		return nil, nil
	}
	rows, err := s.conn.Query(ctx,
		`SELECT hostname, namespace, port, port_name FROM service_versions
		 WHERE has(?, hostname) AND valid_from <= ? AND ? < valid_to`,
		hosts, t, t)
	if err != nil {
		return nil, fmt.Errorf("scopedfor backend services: %w", err)
	}
	defer rows.Close()
	var out []*model.Service
	for rows.Next() {
		var hostname, ns, portName string
		var port uint32
		if err := rows.Scan(&hostname, &ns, &port, &portName); err != nil {
			return nil, err
		}
		out = append(out, &model.Service{
			Hostname:       host.Name(hostname),
			DefaultAddress: "0.0.0.0",
			Ports:          model.PortList{{Name: portName, Port: int(port), Protocol: protocol.Parse(portName)}},
			Attributes:     model.ServiceAttributes{Namespace: ns},
		})
	}
	return out, rows.Err()
}

// vsDestHosts collects the destination host of every route in a VirtualService
// (HTTP/TLS/TCP). Each host is the target Service's identity — an FQDN like
// svc.ns.svc.cluster.local — matched against service_versions.hostname.
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
// from destination IP, in one scoped Overlap query per resource kind. Overlap uses
// the materialized valid_to directly (valid_from < t1 AND t0 < valid_to) — no
// window/lead derivation — and no FINAL (each version is written once, so there
// are no same-version duplicates to collapse). The returned rows carry their
// materialized valid_to so internal/memwindow can slice and resolve in memory.
//
// Each hop loads a correct SUPERSET of what any single-instant point query in the
// window would touch (e.g. gateways whose selector ⊆ the union of the ingress
// deployment's pod labels across versions); memwindow re-applies the exact
// per-instant predicate, so extra rows are harmless. The hops stay scoped like the
// point query (deploys are still selector-filtered) so the superset does not blow
// up on a shared ingress namespace.
func (s *Store) LoadTrafficWindow(ctx context.Context, ip string, t0, t1 time.Time) (store.TrafficWindow, error) {
	var w store.TrafficWindow

	// 1. Ingress Service versions serving this IP.
	svcRows, err := s.conn.Query(ctx,
		`SELECT namespace, name, valid_from, valid_to, rev, ingress_ips, selector_kv, hostname, port, port_name, protocol, ingest_seq
		 FROM service_versions
		 WHERE has(ingress_ips, ?) AND valid_from < ? AND ? < valid_to`,
		ip, t1, t0)
	if err != nil {
		return w, fmt.Errorf("window ingress services: %w", err)
	}
	nsSeen := map[string]bool{}
	var nsList []string
	selSeen := map[string]bool{}
	var selectors [][]string // distinct ingress-service selectors across versions
	for svcRows.Next() {
		var r store.ServiceRow
		if err := svcRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.Rev,
			&r.IngressIPs, &r.Selector, &r.Hostname, &r.Port, &r.PortName, &r.Protocol, &r.IngestSeq); err != nil {
			svcRows.Close()
			return w, err
		}
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
	if err := svcRows.Err(); err != nil {
		svcRows.Close()
		return w, err
	}
	svcRows.Close()
	if len(nsList) == 0 || len(selectors) == 0 {
		return w, nil // no ingress serves this IP in the window -> empty
	}

	// 2. Ingress Deployment versions: those in the ingress namespace(s) whose pod
	// labels ⊇ the ingress service selector (mirrors single-point hop 2). Filtering
	// by selector keeps the label union to just the ingress deployment(s), so hop 3
	// resolves to the right gateway(s) instead of every gateway in the namespace.
	labelSeen := map[string]bool{}
	var labelUnion []string
	depSeen := map[string]bool{}
	for _, sel := range selectors {
		depRows, err := s.conn.Query(ctx,
			`SELECT namespace, name, valid_from, valid_to, rev, pod_labels_kv, ingest_seq
			 FROM deploy_versions
			 WHERE has(?, namespace) AND hasAll(pod_labels_kv, ?) AND valid_from < ? AND ? < valid_to`,
			nsList, sel, t1, t0)
		if err != nil {
			return w, fmt.Errorf("window deploys: %w", err)
		}
		for depRows.Next() {
			var r store.DeployRow
			if err := depRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.Rev, &r.PodLabels, &r.IngestSeq); err != nil {
				depRows.Close()
				return w, err
			}
			key := r.Namespace + "\x00" + r.Name + "\x00" + r.ValidFrom.String()
			if depSeen[key] {
				continue
			}
			depSeen[key] = true
			w.Deploys = append(w.Deploys, r)
			for _, l := range r.PodLabels {
				if !labelSeen[l] {
					labelSeen[l] = true
					labelUnion = append(labelUnion, l)
				}
			}
		}
		if err := depRows.Err(); err != nil {
			depRows.Close()
			return w, err
		}
		depRows.Close()
	}

	// 3. Gateway versions whose selector ⊆ the pod-label union.
	var gwRefs []string
	if len(labelUnion) > 0 {
		gwRows, err := s.conn.Query(ctx,
			`SELECT namespace, name, valid_from, valid_to, rev, selector_kv, server_hosts, spec_json, ingest_seq
			 FROM gw_versions
			 WHERE hasAll(?, selector_kv) AND valid_from < ? AND ? < valid_to`,
			labelUnion, t1, t0)
		if err != nil {
			return w, fmt.Errorf("window gateways: %w", err)
		}
		refSeen := map[string]bool{}
		for gwRows.Next() {
			var r store.GatewayRow
			if err := gwRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.Rev,
				&r.SelectorKV, &r.ServerHosts, &r.SpecJSON, &r.IngestSeq); err != nil {
				gwRows.Close()
				return w, err
			}
			w.Gateways = append(w.Gateways, r)
			ref := r.Namespace + "/" + r.Name
			if !refSeen[ref] {
				refSeen[ref] = true
				gwRefs = append(gwRefs, ref)
			}
		}
		if err := gwRows.Err(); err != nil {
			gwRows.Close()
			return w, err
		}
		gwRows.Close()
	}

	// 4. VirtualService versions bound to any candidate gateway.
	var destHosts []string
	if len(gwRefs) > 0 {
		vsRows, err := s.conn.Query(ctx,
			`SELECT namespace, name, valid_from, valid_to, rev, bound_gateways, spec_json, ingest_seq
			 FROM vs_versions
			 WHERE hasAny(bound_gateways, ?) AND valid_from < ? AND ? < valid_to`,
			gwRefs, t1, t0)
		if err != nil {
			return w, fmt.Errorf("window virtualservices: %w", err)
		}
		hostSeen := map[string]bool{}
		for vsRows.Next() {
			var r store.VSRow
			if err := vsRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.Rev,
				&r.BoundGateways, &r.SpecJSON, &r.IngestSeq); err != nil {
				vsRows.Close()
				return w, err
			}
			w.VSes = append(w.VSes, r)
			var vsSpec networking.VirtualService
			if err := protojson.Unmarshal([]byte(r.SpecJSON), &vsSpec); err != nil {
				vsRows.Close()
				return w, fmt.Errorf("window unmarshal vs %s/%s: %w", r.Namespace, r.Name, err)
			}
			for _, h := range vsDestHosts(&vsSpec) {
				if !hostSeen[h] {
					hostSeen[h] = true
					destHosts = append(destHosts, h)
				}
			}
		}
		if err := vsRows.Err(); err != nil {
			vsRows.Close()
			return w, err
		}
		vsRows.Close()
	}

	// 5. Backend Service versions those VS route to (keyed by destination host
	// FQDN). Chunked so a gateway with very many routes can't inline a host list
	// past ClickHouse's max_query_size.
	for _, chunk := range chunkStrings(destHosts, hostChunk) {
		bsRows, err := s.conn.Query(ctx,
			`SELECT namespace, name, valid_from, valid_to, rev, ingress_ips, selector_kv, hostname, port, port_name, protocol, ingest_seq
			 FROM service_versions
			 WHERE has(?, hostname) AND valid_from < ? AND ? < valid_to`,
			chunk, t1, t0)
		if err != nil {
			return w, fmt.Errorf("window backend services: %w", err)
		}
		for bsRows.Next() {
			var r store.ServiceRow
			if err := bsRows.Scan(&r.Namespace, &r.Name, &r.ValidFrom, &r.ValidTo, &r.Rev,
				&r.IngressIPs, &r.Selector, &r.Hostname, &r.Port, &r.PortName, &r.Protocol, &r.IngestSeq); err != nil {
				bsRows.Close()
				return w, err
			}
			w.Services = append(w.Services, r)
		}
		if err := bsRows.Err(); err != nil {
			bsRows.Close()
			return w, err
		}
		bsRows.Close()
	}

	return w, nil
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
