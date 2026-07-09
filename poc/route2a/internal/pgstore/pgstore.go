// Package pgstore is the PostgreSQL-backed implementation of store.Store. It is
// the "native array" point of the storage comparison: selector/label/IP join
// keys are Postgres `text[]` columns and the 3-hop set-containment join uses the
// array operators `@>` (contains) / `<@` (is-contained-by), accelerated by GIN
// indexes — the direct analog of ClickHouse's has/hasAll.
//
// Bitemporal versions coexist as rows with distinct valid_from; the
// `valid_from <= T < valid_to` predicate selects the one live at T. The corpus is
// loaded into freshly (re)created tables so no ReplacingMergeTree-style FINAL
// collapse is needed; single-row reads still ORDER BY ingest_seq DESC to prefer
// the newest write of a given version.
//
// Scale-out direction (not built here — this PoC benchmarks single node):
// declarative PARTITION BY HASH(namespace) with a GIN index per partition, or
// Citus (coordinator + workers) distributing on namespace; every read is already
// namespace/name-scoped so it stays partition-local.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

// insertChunk bounds how many rows buffer before a CopyFrom flush (the backend
// service table is ~24M rows, so streaming in chunks keeps memory flat).
const insertChunk = 100_000

// Store wraps a PostgreSQL connection pool.
type Store struct{ pool *pgxpool.Pool }

// Open dials PostgreSQL (dsn e.g. "postgres://user:pass@host:5432/db?sslmode=disable")
// and pings it.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres %s: %w", dsn, err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() error { s.pool.Close(); return nil }

// ddl (re)creates the four version tables with native text[] join-key columns and
// GIN indexes over them (for @>/<@), plus btree indexes for the version predicate
// and backend-service lookup.
var ddl = []string{
	`DROP TABLE IF EXISTS service_versions`,
	`DROP TABLE IF EXISTS deploy_versions`,
	`DROP TABLE IF EXISTS gw_versions`,
	`DROP TABLE IF EXISTS vs_versions`,
	`CREATE TABLE service_versions (
		namespace   text        NOT NULL,
		name        text        NOT NULL,
		valid_from  timestamptz NOT NULL,
		valid_to    timestamptz NOT NULL,
		rev         int         NOT NULL,
		ingress_ips text[]      NOT NULL DEFAULT '{}',
		selector_kv text[]      NOT NULL DEFAULT '{}',
		hostname    text        NOT NULL DEFAULT '',
		port        int         NOT NULL DEFAULT 0,
		port_name   text        NOT NULL DEFAULT '',
		protocol    text        NOT NULL DEFAULT '',
		ingest_seq  bigint      NOT NULL
	)`,
	`CREATE INDEX ON service_versions USING gin (ingress_ips)`,
	`CREATE INDEX ON service_versions (namespace, hostname, valid_from)`,
	`CREATE INDEX ON service_versions (namespace, name, valid_from)`,
	`CREATE TABLE deploy_versions (
		namespace     text        NOT NULL,
		name          text        NOT NULL,
		valid_from    timestamptz NOT NULL,
		valid_to      timestamptz NOT NULL,
		rev           int         NOT NULL,
		pod_labels_kv text[]      NOT NULL DEFAULT '{}',
		ingest_seq    bigint      NOT NULL
	)`,
	`CREATE INDEX ON deploy_versions USING gin (pod_labels_kv)`,
	`CREATE INDEX ON deploy_versions (namespace, name, valid_from)`,
	`CREATE TABLE gw_versions (
		namespace    text        NOT NULL,
		name         text        NOT NULL,
		valid_from   timestamptz NOT NULL,
		valid_to     timestamptz NOT NULL,
		rev          int         NOT NULL,
		selector_kv  text[]      NOT NULL DEFAULT '{}',
		server_hosts text[]      NOT NULL DEFAULT '{}',
		spec_json    text        NOT NULL DEFAULT '',
		ingest_seq   bigint      NOT NULL
	)`,
	`CREATE INDEX ON gw_versions USING gin (selector_kv)`,
	`CREATE INDEX ON gw_versions (name, valid_from)`,
	`CREATE TABLE vs_versions (
		namespace      text        NOT NULL,
		name           text        NOT NULL,
		valid_from     timestamptz NOT NULL,
		valid_to       timestamptz NOT NULL,
		rev            int         NOT NULL,
		bound_gateways text[]      NOT NULL DEFAULT '{}',
		spec_json      text        NOT NULL DEFAULT '',
		ingest_seq     bigint      NOT NULL
	)`,
	`CREATE INDEX ON vs_versions USING gin (bound_gateways)`,
	`CREATE INDEX ON vs_versions (namespace, name, valid_from)`,
}

// CreateSchema drops and recreates all four tables (idempotent reload).
func (s *Store) CreateSchema(ctx context.Context) error {
	for _, stmt := range ddl {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ddl: %w", err)
		}
	}
	return nil
}

// ---- streaming inserters (auto-chunked via CopyFrom) ----

// batch buffers rows for one table and flushes them with CopyFrom in chunks so
// huge tables don't buffer entirely in memory.
type batch struct {
	ctx   context.Context
	pool  *pgxpool.Pool
	table string
	cols  []string
	rows  [][]any
}

func (s *Store) newBatch(ctx context.Context, table string, cols []string) *batch {
	return &batch{ctx: ctx, pool: s.pool, table: table, cols: cols}
}

func (b *batch) append(vals ...any) error {
	b.rows = append(b.rows, vals)
	if len(b.rows) >= insertChunk {
		return b.flush()
	}
	return nil
}

func (b *batch) flush() error {
	if len(b.rows) == 0 {
		return nil
	}
	_, err := b.pool.CopyFrom(b.ctx, pgx.Identifier{b.table}, b.cols, pgx.CopyFromRows(b.rows))
	if err != nil {
		return err
	}
	b.rows = b.rows[:0]
	return nil
}

type serviceBatch struct{ *batch }
type deployBatch struct{ *batch }
type gwBatch struct{ *batch }
type vsBatch struct{ *batch }

func (s *Store) NewServiceBatch(ctx context.Context) (store.ServiceBatch, error) {
	return &serviceBatch{s.newBatch(ctx, "service_versions", []string{
		"namespace", "name", "valid_from", "valid_to", "rev",
		"ingress_ips", "selector_kv", "hostname", "port", "port_name", "protocol", "ingest_seq"})}, nil
}
func (b *serviceBatch) Append(r store.ServiceRow) error {
	return b.append(r.Namespace, r.Name, r.ValidFrom, r.ValidTo, int32(r.Rev),
		r.IngressIPs, r.Selector, r.Hostname, int32(r.Port), r.PortName, r.Protocol, int64(r.IngestSeq))
}
func (b *serviceBatch) Close() error { return b.flush() }

func (s *Store) NewDeployBatch(ctx context.Context) (store.DeployBatch, error) {
	return &deployBatch{s.newBatch(ctx, "deploy_versions", []string{
		"namespace", "name", "valid_from", "valid_to", "rev", "pod_labels_kv", "ingest_seq"})}, nil
}
func (b *deployBatch) Append(ns, name string, from, to time.Time, rev uint32, podLabels []string, seq uint64) error {
	return b.append(ns, name, from, to, int32(rev), podLabels, int64(seq))
}
func (b *deployBatch) Close() error { return b.flush() }

func (s *Store) NewGwBatch(ctx context.Context) (store.GwBatch, error) {
	return &gwBatch{s.newBatch(ctx, "gw_versions", []string{
		"namespace", "name", "valid_from", "valid_to", "rev", "selector_kv", "server_hosts", "spec_json", "ingest_seq"})}, nil
}
func (b *gwBatch) Append(ns, name string, from, to time.Time, rev uint32, selectorKV, serverHosts []string, specJSON string, seq uint64) error {
	return b.append(ns, name, from, to, int32(rev), selectorKV, serverHosts, specJSON, int64(seq))
}
func (b *gwBatch) Close() error { return b.flush() }

func (s *Store) NewVSBatch(ctx context.Context) (store.VSBatch, error) {
	return &vsBatch{s.newBatch(ctx, "vs_versions", []string{
		"namespace", "name", "valid_from", "valid_to", "rev", "bound_gateways", "spec_json", "ingest_seq"})}, nil
}
func (b *vsBatch) Append(ns, name string, from, to time.Time, rev uint32, boundGateways []string, specJSON string, seq uint64) error {
	return b.append(ns, name, from, to, int32(rev), boundGateways, specJSON, int64(seq))
}
func (b *vsBatch) Close() error { return b.flush() }

// ---- queries ----

// ResolveIPToGateways runs the 3-hop selector join for a destination IP at time t
// (see store.Store). Hop1 IP membership and hop2 svc.selector ⊆ pod_labels use
// `@>` on the GIN-indexed array; hop3 gw.selector ⊆ L uses `<@` (selector_kv
// contained by L) so the gw_versions GIN index applies.
func (s *Store) ResolveIPToGateways(ctx context.Context, ip string, t time.Time) ([]store.GatewayCand, error) {
	// Hop 1: IP -> ingress Service (its namespace + selector).
	var svcNS string
	var svcSel []string
	err := s.pool.QueryRow(ctx,
		`SELECT namespace, selector_kv FROM service_versions
		 WHERE ingress_ips @> $1 AND valid_from <= $2 AND $2 < valid_to
		 ORDER BY ingest_seq DESC LIMIT 1`,
		[]string{ip}, t).Scan(&svcNS, &svcSel)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no ingress serves this IP -> traffic miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop1 (ip->service): %w", err)
	}

	// Hop 2: Service selector -> ingress Deployment pod labels L (svc.selector ⊆ L).
	var podLabels []string
	err = s.pool.QueryRow(ctx,
		`SELECT pod_labels_kv FROM deploy_versions
		 WHERE namespace = $1 AND pod_labels_kv @> $2 AND valid_from <= $3 AND $3 < valid_to
		 ORDER BY ingest_seq DESC LIMIT 1`,
		svcNS, svcSel, t).Scan(&podLabels)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // ingress workload not found -> miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop2 (service->deployment L): %w", err)
	}

	// Hop 3: L -> candidate gateways (gateway.selector ⊆ L).
	rows, err := s.pool.Query(ctx,
		`SELECT namespace, name, server_hosts FROM gw_versions
		 WHERE selector_kv <@ $1 AND valid_from <= $2 AND $2 < valid_to`,
		podLabels, t)
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

// ScopedFor rebuilds one gateway's translate input from PostgreSQL at time t: its
// Gateway CR + bound VirtualServices + the destination Services those VS route to
// (all as-of-t versions).
func (s *Store) ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error) {
	// Gateway CR (1 current version).
	var gwNS, gwJSON string
	err := s.pool.QueryRow(ctx,
		`SELECT namespace, spec_json FROM gw_versions
		 WHERE name = $1 AND valid_from <= $2 AND $2 < valid_to
		 ORDER BY ingest_seq DESC LIMIT 1`,
		gwName, t).Scan(&gwNS, &gwJSON)
	if errors.Is(err, pgx.ErrNoRows) {
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
	vsRows, err := s.pool.Query(ctx,
		`SELECT namespace, name, spec_json FROM vs_versions
		 WHERE bound_gateways @> $1 AND valid_from <= $2 AND $2 < valid_to`,
		[]string{gwNS + "/" + gwName}, t)
	if err != nil {
		return translate.ScopedInput{}, false, fmt.Errorf("scopedfor vs: %w", err)
	}
	defer vsRows.Close()
	for vsRows.Next() {
		var ns, name, js string
		if err := vsRows.Scan(&ns, &name, &js); err != nil {
			return translate.ScopedInput{}, false, err
		}
		var vsSpec networking.VirtualService
		if err := protojson.Unmarshal([]byte(js), &vsSpec); err != nil {
			return translate.ScopedInput{}, false, fmt.Errorf("unmarshal vs %s/%s: %w", ns, name, err)
		}
		cfgs = append(cfgs, config.Config{
			Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: name, Namespace: ns},
			Spec: &vsSpec,
		})
	}
	if err := vsRows.Err(); err != nil {
		return translate.ScopedInput{}, false, err
	}

	// Destination Services (backend rows: hostname set, in the gateway's app namespace).
	svcs, err := s.backendServices(ctx, gwName, t)
	if err != nil {
		return translate.ScopedInput{}, false, err
	}

	return translate.ScopedInput{
		Configs:  cfgs,
		Services: svcs,
		Proxy:    translate.GatewayProxy{Name: gwName, Namespace: gwNS, Labels: gwSpec.GetSelector()},
	}, true, nil
}

// backendServices reads the destination Services (hostname != ”) in namespace ns
// at time t and rebuilds them as istiod model.Services.
func (s *Store) backendServices(ctx context.Context, ns string, t time.Time) ([]*model.Service, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT hostname, port, port_name FROM service_versions
		 WHERE namespace = $1 AND hostname <> '' AND valid_from <= $2 AND $2 < valid_to`,
		ns, t)
	if err != nil {
		return nil, fmt.Errorf("scopedfor backend services: %w", err)
	}
	defer rows.Close()
	var out []*model.Service
	for rows.Next() {
		var hostname, portName string
		var port int32
		if err := rows.Scan(&hostname, &port, &portName); err != nil {
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

// CountRows returns the row count of a table (all versions).
func (s *Store) CountRows(ctx context.Context, table string) (uint64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n)
	return uint64(n), err
}

// AsOfRev returns the version rev live at time t for one resource. ok=false if
// none is live at t.
func (s *Store) AsOfRev(ctx context.Context, table, ns, name string, t time.Time) (uint32, bool, error) {
	var rev int32
	err := s.pool.QueryRow(ctx,
		"SELECT rev FROM "+table+" WHERE namespace = $1 AND name = $2 AND valid_from <= $3 AND $3 < valid_to ORDER BY ingest_seq DESC LIMIT 1",
		ns, name, t).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return uint32(rev), err == nil, err
}
