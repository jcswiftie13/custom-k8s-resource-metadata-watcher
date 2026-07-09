// Package mariastore is the MariaDB-backed implementation of store.Store. MariaDB
// has no array type, so it is the "normalized relational" point of the storage
// comparison: set-containment (ClickHouse has/hasAll) becomes an indexed JOIN +
// GROUP BY ... HAVING COUNT(DISTINCT ...) over a junction (child) table keyed by
// the version row's globally-unique ingest_seq.
//
//   - membership  has(col, x)      -> JOIN child ON seq WHERE child.val = x
//   - subset      hasAll(col, sub) -> JOIN child ON seq WHERE child.val IN (sub)
//     GROUP BY seq HAVING COUNT(DISTINCT val) = |sub|
//     (|sub| is a query constant; for hop3 the
//     constant is the gateway's own selector size,
//     stored as gw_versions.sel_count)
//
// Hybrid layout — a junction table is an inverted index, worth its cost only for
// a set that is SEARCHED by containment; a set that is merely RETRIEVED by a
// known key is cheaper as a JSON column on the main row (one read, no join):
//
//   - ingress_ip, deploy pod_labels, gw selector, vs bound_gateways -> SEARCHED
//     (hop1/hop2/hop3/ScopedFor) => junction child table (indexed).
//   - service selector -> only RETRIEVED (as hop2's query) => JSON on the main
//     row; folded into hop1's join, so hop1 needs no second lookup.
//   - deploy pod_labels -> SEARCHED (hop2) AND RETRIEVED (feeds hop3) => BOTH: a
//     junction table for the subset search plus a JSON copy on the main row,
//     returned by the hop2 GROUP BY so hop2 needs no second lookup either.
//
// Bitemporal versions coexist as rows with distinct valid_from; the
// `valid_from <= T < valid_to` predicate selects the one live at T. The corpus is
// loaded into freshly (re)created tables so no dedup is needed; single-row reads
// still ORDER BY ingest_seq DESC to prefer the newest write of a given version.
//
// Note: an empty selector (⊆ anything) would not be matched by the JOIN form; the
// PoC corpus always has non-empty gateway/service selectors, matching ClickHouse.
//
// Scale-out direction (not built here — this PoC benchmarks single node):
// PARTITION BY KEY(namespace) on each table, or the Spider storage engine /
// Galera cluster; every read is namespace/name-scoped so it stays shard-local.
package mariastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

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

// maxPlaceholders keeps a multi-row INSERT under MariaDB's 65535 placeholder cap;
// each buffer derives its per-flush row count as maxPlaceholders/len(cols).
const maxPlaceholders = 60_000

// Store wraps a MariaDB (database/sql) connection.
type Store struct{ db *sql.DB }

// Open dials MariaDB (dsn e.g. "user:pass@tcp(host:3306)/db?parseTime=true&loc=UTC")
// and pings it.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mariadb: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ddl (re)creates the four version tables plus their junction child tables. Each
// child references the parent by ingest_seq (globally unique per row) and is
// indexed on both the join key (seq) and the value (kv/ip/gw) for the containment
// joins. Version tables carry a (namespace,name,valid_from) btree for the
// bitemporal predicate.
var ddl = []string{
	`DROP TABLE IF EXISTS service_ingress_ip`,
	`DROP TABLE IF EXISTS service_versions`,
	`DROP TABLE IF EXISTS deploy_pod_labels_kv`,
	`DROP TABLE IF EXISTS deploy_versions`,
	`DROP TABLE IF EXISTS gw_selector_kv`,
	`DROP TABLE IF EXISTS gw_versions`,
	`DROP TABLE IF EXISTS vs_bound_gateway`,
	`DROP TABLE IF EXISTS vs_versions`,
	`DROP TABLE IF EXISTS service_selector_kv`, // legacy (pre-hybrid): now a JSON column

	// service selector is retrieve-only -> JSON column on the main row (no child).
	`CREATE TABLE service_versions (
		ingest_seq  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		namespace   VARCHAR(255) NOT NULL,
		name        VARCHAR(255) NOT NULL,
		valid_from  DATETIME(3)  NOT NULL,
		valid_to    DATETIME(3)  NOT NULL,
		rev         INT          NOT NULL,
		selector_kv JSON         NOT NULL,
		hostname    VARCHAR(255) NOT NULL DEFAULT '',
		port        INT          NOT NULL DEFAULT 0,
		port_name   VARCHAR(64)  NOT NULL DEFAULT '',
		protocol    VARCHAR(32)  NOT NULL DEFAULT '',
		KEY k_svc_nnv (namespace, name, valid_from),
		KEY k_svc_host (namespace, hostname, valid_from)
	)`,
	`CREATE TABLE service_ingress_ip (
		seq BIGINT UNSIGNED NOT NULL,
		ip  VARCHAR(64) NOT NULL,
		KEY k_sii_ip (ip), KEY k_sii_seq (seq)
	)`,

	// deploy pod_labels is searched (hop2) AND retrieved (feeds hop3): junction
	// child for the subset search + a JSON copy on the main row for retrieval.
	`CREATE TABLE deploy_versions (
		ingest_seq    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		namespace     VARCHAR(255) NOT NULL,
		name          VARCHAR(255) NOT NULL,
		valid_from    DATETIME(3)  NOT NULL,
		valid_to      DATETIME(3)  NOT NULL,
		rev           INT          NOT NULL,
		pod_labels_kv JSON         NOT NULL,
		KEY k_dep_nnv (namespace, name, valid_from)
	)`,
	`CREATE TABLE deploy_pod_labels_kv (
		seq BIGINT UNSIGNED NOT NULL,
		kv  VARCHAR(255) NOT NULL,
		KEY k_dpl_seq (seq), KEY k_dpl_kv (kv)
	)`,

	`CREATE TABLE gw_versions (
		ingest_seq   BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		namespace    VARCHAR(255) NOT NULL,
		name         VARCHAR(255) NOT NULL,
		valid_from   DATETIME(3)  NOT NULL,
		valid_to     DATETIME(3)  NOT NULL,
		rev          INT          NOT NULL,
		sel_count    INT          NOT NULL DEFAULT 0,
		server_hosts JSON         NOT NULL,
		spec_json    LONGTEXT     NOT NULL,
		KEY k_gw_nv (name, valid_from),
		KEY k_gw_nnv (namespace, name, valid_from)
	)`,
	`CREATE TABLE gw_selector_kv (
		seq BIGINT UNSIGNED NOT NULL,
		kv  VARCHAR(255) NOT NULL,
		KEY k_gsk_seq (seq), KEY k_gsk_kv (kv)
	)`,

	`CREATE TABLE vs_versions (
		ingest_seq BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		namespace  VARCHAR(255) NOT NULL,
		name       VARCHAR(255) NOT NULL,
		valid_from DATETIME(3)  NOT NULL,
		valid_to   DATETIME(3)  NOT NULL,
		rev        INT          NOT NULL,
		spec_json  LONGTEXT     NOT NULL,
		KEY k_vs_nnv (namespace, name, valid_from)
	)`,
	`CREATE TABLE vs_bound_gateway (
		seq BIGINT UNSIGNED NOT NULL,
		gw  VARCHAR(255) NOT NULL,
		KEY k_vbg_gw (gw), KEY k_vbg_seq (seq)
	)`,
}

// CreateSchema drops and recreates all tables (idempotent reload).
func (s *Store) CreateSchema(ctx context.Context) error {
	for _, stmt := range ddl {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ddl: %w", err)
		}
	}
	return nil
}

// ---- streaming inserters (auto-chunked multi-row INSERT) ----

// insertBuf accumulates value tuples for one table and flushes them as one
// multi-row INSERT when the placeholder budget is reached.
type insertBuf struct {
	ctx     context.Context
	db      *sql.DB
	prefix  string // "INSERT INTO t (a,b) VALUES "
	tuple   string // "(?,?)"
	cols    int
	maxRows int
	args    []any
	nRows   int
}

func (s *Store) newInsertBuf(ctx context.Context, table string, cols []string) *insertBuf {
	ph := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	maxRows := maxPlaceholders / len(cols)
	if maxRows < 1 {
		maxRows = 1
	}
	return &insertBuf{
		ctx: ctx, db: s.db,
		prefix:  "INSERT INTO " + table + " (" + strings.Join(cols, ",") + ") VALUES ",
		tuple:   ph,
		cols:    len(cols),
		maxRows: maxRows,
	}
}

func (b *insertBuf) add(vals ...any) error {
	b.args = append(b.args, vals...)
	b.nRows++
	if b.nRows >= b.maxRows {
		return b.flush()
	}
	return nil
}

func (b *insertBuf) flush() error {
	if b.nRows == 0 {
		return nil
	}
	q := b.prefix + strings.TrimSuffix(strings.Repeat(b.tuple+",", b.nRows), ",")
	if _, err := b.db.ExecContext(b.ctx, q, b.args...); err != nil {
		return err
	}
	b.args = b.args[:0]
	b.nRows = 0
	return nil
}

type serviceBatch struct{ main, ips *insertBuf }
type deployBatch struct{ main, pod *insertBuf }
type gwBatch struct{ main, sel *insertBuf }
type vsBatch struct{ main, bound *insertBuf }

func (s *Store) NewServiceBatch(ctx context.Context) (store.ServiceBatch, error) {
	return &serviceBatch{
		main: s.newInsertBuf(ctx, "service_versions", []string{"ingest_seq", "namespace", "name", "valid_from", "valid_to", "rev", "selector_kv", "hostname", "port", "port_name", "protocol"}),
		ips:  s.newInsertBuf(ctx, "service_ingress_ip", []string{"seq", "ip"}),
	}, nil
}
func (b *serviceBatch) Append(r store.ServiceRow) error {
	// selector is retrieve-only -> JSON column on the main row (no child table).
	selJSON, err := marshalKV(r.Selector)
	if err != nil {
		return err
	}
	if err := b.main.add(r.IngestSeq, r.Namespace, r.Name, r.ValidFrom, r.ValidTo, r.Rev, selJSON, r.Hostname, r.Port, r.PortName, r.Protocol); err != nil {
		return err
	}
	for _, ip := range r.IngressIPs {
		if err := b.ips.add(r.IngestSeq, ip); err != nil {
			return err
		}
	}
	return nil
}
func (b *serviceBatch) Close() error { return flushAll(b.main, b.ips) }

func (s *Store) NewDeployBatch(ctx context.Context) (store.DeployBatch, error) {
	return &deployBatch{
		main: s.newInsertBuf(ctx, "deploy_versions", []string{"ingest_seq", "namespace", "name", "valid_from", "valid_to", "rev", "pod_labels_kv"}),
		pod:  s.newInsertBuf(ctx, "deploy_pod_labels_kv", []string{"seq", "kv"}),
	}, nil
}
func (b *deployBatch) Append(ns, name string, from, to time.Time, rev uint32, podLabels []string, seq uint64) error {
	// pod_labels is searched (hop2, junction) AND retrieved (hop3): write BOTH a
	// JSON copy on the main row and the child rows for the subset search.
	podJSON, err := marshalKV(podLabels)
	if err != nil {
		return err
	}
	if err := b.main.add(seq, ns, name, from, to, rev, podJSON); err != nil {
		return err
	}
	for _, kv := range podLabels {
		if err := b.pod.add(seq, kv); err != nil {
			return err
		}
	}
	return nil
}
func (b *deployBatch) Close() error { return flushAll(b.main, b.pod) }

// marshalKV encodes a kv set as a JSON array string for a retrieve-only column
// (nil marshals to "[]", never SQL NULL, so the JSON NOT NULL column is happy).
func marshalKV(kv []string) (string, error) {
	if kv == nil {
		kv = []string{}
	}
	b, err := json.Marshal(kv)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *Store) NewGwBatch(ctx context.Context) (store.GwBatch, error) {
	return &gwBatch{
		main: s.newInsertBuf(ctx, "gw_versions", []string{"ingest_seq", "namespace", "name", "valid_from", "valid_to", "rev", "sel_count", "server_hosts", "spec_json"}),
		sel:  s.newInsertBuf(ctx, "gw_selector_kv", []string{"seq", "kv"}),
	}, nil
}
func (b *gwBatch) Append(ns, name string, from, to time.Time, rev uint32, selectorKV, serverHosts []string, specJSON string, seq uint64) error {
	hostsJSON, err := json.Marshal(serverHosts)
	if err != nil {
		return err
	}
	if err := b.main.add(seq, ns, name, from, to, rev, len(selectorKV), string(hostsJSON), specJSON); err != nil {
		return err
	}
	for _, kv := range selectorKV {
		if err := b.sel.add(seq, kv); err != nil {
			return err
		}
	}
	return nil
}
func (b *gwBatch) Close() error { return flushAll(b.main, b.sel) }

func (s *Store) NewVSBatch(ctx context.Context) (store.VSBatch, error) {
	return &vsBatch{
		main:  s.newInsertBuf(ctx, "vs_versions", []string{"ingest_seq", "namespace", "name", "valid_from", "valid_to", "rev", "spec_json"}),
		bound: s.newInsertBuf(ctx, "vs_bound_gateway", []string{"seq", "gw"}),
	}, nil
}
func (b *vsBatch) Append(ns, name string, from, to time.Time, rev uint32, boundGateways []string, specJSON string, seq uint64) error {
	if err := b.main.add(seq, ns, name, from, to, rev, specJSON); err != nil {
		return err
	}
	for _, gw := range boundGateways {
		if err := b.bound.add(seq, gw); err != nil {
			return err
		}
	}
	return nil
}
func (b *vsBatch) Close() error { return flushAll(b.main, b.bound) }

func flushAll(bufs ...*insertBuf) error {
	for _, b := range bufs {
		if err := b.flush(); err != nil {
			return err
		}
	}
	return nil
}

// inClause returns "(?,?,...)" plus the []any args for an IN list.
func inClause(vals []string) (string, []any) {
	if len(vals) == 0 {
		return "(NULL)", nil
	}
	args := make([]any, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	return "(" + strings.TrimSuffix(strings.Repeat("?,", len(vals)), ",") + ")", args
}

// ---- queries ----

// ResolveIPToGateways runs the 3-hop selector join for a destination IP at time t
// (see store.Store), using junction-table joins with GROUP BY ... HAVING COUNT to
// evaluate the set-containment hops.
func (s *Store) ResolveIPToGateways(ctx context.Context, ip string, t time.Time) ([]store.GatewayCand, error) {
	// Hop 1: IP -> ingress Service. The selector is a JSON column on the main row,
	// so the ip join returns it inline (no second lookup).
	var svcNS string
	var svcSelJSON []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT s.namespace, s.selector_kv
		 FROM service_versions s JOIN service_ingress_ip i ON i.seq = s.ingest_seq
		 WHERE i.ip = ? AND s.valid_from <= ? AND ? < s.valid_to
		 ORDER BY s.ingest_seq DESC LIMIT 1`,
		ip, t, t).Scan(&svcNS, &svcSelJSON)
	if err == sql.ErrNoRows {
		return nil, nil // no ingress serves this IP -> traffic miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop1 (ip->service): %w", err)
	}
	svcSel, err := unmarshalKV(svcSelJSON)
	if err != nil {
		return nil, fmt.Errorf("hop1 selector: %w", err)
	}

	// Hop 2: Service selector -> ingress Deployment (svc.selector ⊆ pod_labels L).
	// The junction table drives the subset search; the deployment's full pod_labels
	// (needed for hop3) come back inline from the main row's JSON column.
	inSel, selArgs := inClause(svcSel)
	args := append([]any{svcNS, t, t}, selArgs...)
	args = append(args, len(svcSel))
	var podJSON []byte
	err = s.db.QueryRowContext(ctx,
		`SELECT d.pod_labels_kv
		 FROM deploy_versions d JOIN deploy_pod_labels_kv l ON l.seq = d.ingest_seq
		 WHERE d.namespace = ? AND d.valid_from <= ? AND ? < d.valid_to AND l.kv IN `+inSel+`
		 GROUP BY d.ingest_seq HAVING COUNT(DISTINCT l.kv) = ? LIMIT 1`,
		args...).Scan(&podJSON)
	if err == sql.ErrNoRows {
		return nil, nil // ingress workload not found -> miss
	}
	if err != nil {
		return nil, fmt.Errorf("hop2 (service->deployment L): %w", err)
	}
	podLabels, err := unmarshalKV(podJSON)
	if err != nil {
		return nil, fmt.Errorf("hop2 pod labels: %w", err)
	}

	// Hop 3: L -> candidate gateways (gateway.selector ⊆ L; HAVING against each
	// gateway's own sel_count).
	inL, lArgs := inClause(podLabels)
	// MIN(g.sel_count) (constant within a gateway's group) lets HAVING compare the
	// matched-selector count against each gateway's own selector cardinality; a bare
	// g.sel_count is rejected in HAVING under GROUP BY.
	q := `SELECT g.ingest_seq, g.namespace, g.name, g.server_hosts
		 FROM gw_versions g JOIN gw_selector_kv gs ON gs.seq = g.ingest_seq
		 WHERE g.valid_from <= ? AND ? < g.valid_to AND gs.kv IN ` + inL + `
		 GROUP BY g.ingest_seq HAVING COUNT(DISTINCT gs.kv) = MIN(g.sel_count)`
	rows, err := s.db.QueryContext(ctx, q, append([]any{t, t}, lArgs...)...)
	if err != nil {
		return nil, fmt.Errorf("hop3 (L->gateways): %w", err)
	}
	defer rows.Close()

	var cands []store.GatewayCand
	for rows.Next() {
		var seq uint64
		var c store.GatewayCand
		var hostsJSON []byte
		if err := rows.Scan(&seq, &c.Namespace, &c.Name, &hostsJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(hostsJSON, &c.ServerHosts); err != nil {
			return nil, fmt.Errorf("unmarshal server_hosts: %w", err)
		}
		cands = append(cands, c)
	}
	return cands, rows.Err()
}

// unmarshalKV decodes a JSON-array kv column (selector_kv / pod_labels_kv) into a
// slice. A NULL/empty column decodes to an empty slice.
func unmarshalKV(js []byte) ([]string, error) {
	if len(js) == 0 {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal(js, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ScopedFor rebuilds one gateway's translate input from MariaDB at time t: its
// Gateway CR + bound VirtualServices + the destination Services those VS route to.
func (s *Store) ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error) {
	var gwNS, gwJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT namespace, spec_json FROM gw_versions
		 WHERE name = ? AND valid_from <= ? AND ? < valid_to ORDER BY ingest_seq DESC LIMIT 1`,
		gwName, t, t).Scan(&gwNS, &gwJSON)
	if err == sql.ErrNoRows {
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

	// Bound VirtualServices.
	vsRows, err := s.db.QueryContext(ctx,
		`SELECT v.namespace, v.name, v.spec_json
		 FROM vs_versions v JOIN vs_bound_gateway b ON b.seq = v.ingest_seq
		 WHERE b.gw = ? AND v.valid_from <= ? AND ? < v.valid_to`,
		gwNS+"/"+gwName, t, t)
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
	rows, err := s.db.QueryContext(ctx,
		`SELECT hostname, port, port_name FROM service_versions
		 WHERE namespace = ? AND hostname <> '' AND valid_from <= ? AND ? < valid_to`,
		ns, t, t)
	if err != nil {
		return nil, fmt.Errorf("scopedfor backend services: %w", err)
	}
	defer rows.Close()
	var out []*model.Service
	for rows.Next() {
		var hostname, portName string
		var port int
		if err := rows.Scan(&hostname, &port, &portName); err != nil {
			return nil, err
		}
		out = append(out, &model.Service{
			Hostname:       host.Name(hostname),
			DefaultAddress: "0.0.0.0",
			Ports:          model.PortList{{Name: portName, Port: port, Protocol: protocol.Parse(portName)}},
			Attributes:     model.ServiceAttributes{Namespace: ns},
		})
	}
	return out, rows.Err()
}

// CountRows returns the row count of a table (all versions).
func (s *Store) CountRows(ctx context.Context, table string) (uint64, error) {
	var n uint64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n)
	return n, err
}

// AsOfRev returns the version rev live at time t for one resource. ok=false if
// none is live at t.
func (s *Store) AsOfRev(ctx context.Context, table, ns, name string, t time.Time) (uint32, bool, error) {
	var rev uint32
	err := s.db.QueryRowContext(ctx,
		"SELECT rev FROM "+table+" WHERE namespace = ? AND name = ? AND valid_from <= ? AND ? < valid_to ORDER BY ingest_seq DESC LIMIT 1",
		ns, name, t, t).Scan(&rev)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return rev, err == nil, err
}
