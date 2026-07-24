// Package store is the backend-agnostic contract for the ingress-traffic config
// store. It holds the value types and the Store/batch interfaces the pipeline
// depends on, so the loader (internal/ingload), the benchmark harness, and the
// ipflow CLI can run against the store without knowing the engine.
//
// The engine is ClickHouse (internal/chstore) — the same store production uses,
// so the PoC stays portable. The factory in internal/storeopen dials it. This
// package imports no backend, so it stays a leaf (the backend imports it).
package store

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// Backend names the config-store engine (ClickHouse). Kept as a named value so
// report rows and log lines can label the store.
type Backend string

const BackendClickHouse Backend = "clickhouse"

// farFuture is the open-ended `valid_to` sentinel for the current (open) version.
// It stays within ClickHouse DateTime64 range (max year 2299) while being
// comfortably after any query time.
var farFuture = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)

// versionBase/versionStep lay the K versions on a fixed early timeline so the
// open (last) version is the only one live at "now".
var versionBase = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

const versionStep = time.Hour

// Version is one bitemporal slice of a resource.
type Version struct {
	Rev  int
	From time.Time
	To   time.Time
}

// Versions returns k contiguous, non-overlapping versions; the last is open
// (To = farFuture = current).
func Versions(k int) []Version {
	if k < 1 {
		k = 1
	}
	out := make([]Version, k)
	for v := 0; v < k; v++ {
		from := versionBase.Add(time.Duration(v) * versionStep)
		to := versionBase.Add(time.Duration(v+1) * versionStep)
		if v == k-1 {
			to = farFuture
		}
		out[v] = Version{Rev: v, From: from, To: to}
	}
	return out
}

// VersionMidTime returns a timestamp inside version v's interval — used by the
// multi-version AsOf verification to target a specific past version.
func VersionMidTime(v int) time.Time {
	return versionBase.Add(time.Duration(v)*versionStep + versionStep/2)
}

// BenchWindow is the default [t0,t1) for a range query over the whole corpus: the
// first version's start through the far-future sentinel, so every version on the
// timeline falls inside it (segments >= 1).
func BenchWindow() (time.Time, time.Time) { return versionBase, farFuture }

// ServiceRow is one service_versions row (ingress LB: ingress_ips/selector set,
// Ports empty; backend: Ports set, ingress_ips empty). One row per Service
// version — a backend Service's ports all live in Ports (serialized to the
// spec_json column), so a multi-port Service is a single row, not one row per
// port. The backend Service's identity is (Namespace, Name); its FQDN (the
// destination.host a VS routes to) is derived — see BackendFQDN / ParseBackendHost.
type ServiceRow struct {
	Namespace, Name    string
	ValidFrom, ValidTo time.Time
	// Rev is write-side only: the PoC loader materializes it into the `rev`
	// oracle column (production tables have no such column, so readers never
	// select it — LoadTrafficWindow leaves it zero).
	Rev                  uint32
	IngressIPs, Selector []string
	Ports                []SvcPort
	IngestSeq            uint64
}

// SvcPort is one Service port. JSON tags match the corev1 ServiceSpec.ports
// shape so the reader unmarshals the same struct from the PoC's synthetic
// spec_json and from a real metadata-exporter `spec` blob (extra fields ignored).
type SvcPort struct {
	Name     string `json:"name"`
	Port     uint32 `json:"port"`
	Protocol string `json:"protocol"`
}

// serviceDomain is the standard Kubernetes Service FQDN suffix. The PoC assumes
// the default cluster domain; the exporter stores only (namespace, name), so the
// FQDN mapping is Istio-side derivation that lives here in the reader layer.
const serviceDomain = ".svc.cluster.local"

// BackendFQDN reconstructs a backend Service's FQDN (its destination.host
// identity) from its (name, namespace) — the inverse of ParseBackendHost.
func BackendFQDN(name, ns string) string { return name + "." + ns + serviceDomain }

// ParseBackendHost derives (name, namespace) from a VirtualService route's
// destination.host FQDN (name.namespace.svc.cluster.local). ok=false for a host
// that isn't a resolvable in-cluster FQDN (e.g. a bare short name or external
// host), which the caller skips. Short-name support (default to the VS namespace)
// can be added later; the corpus emits FQDNs.
func ParseBackendHost(host string) (name, ns string, ok bool) {
	rest, found := strings.CutSuffix(host, serviceDomain)
	if !found {
		return "", "", false
	}
	labels := strings.SplitN(rest, ".", 3)
	if len(labels) < 2 || labels[0] == "" || labels[1] == "" {
		return "", "", false
	}
	return labels[0], labels[1], true
}

// serviceSpec is the minimal shape carried in the spec_json column: just the
// ports subtree of a Service spec. It mirrors the field a real exporter `spec`
// blob exposes, so one decoder works for both.
type serviceSpec struct {
	Ports []SvcPort `json:"ports"`
}

// MarshalPorts encodes a Service's ports as the spec_json column value
// (`{"ports":[...]}`), or "" for a portless (ingress LB) row.
func MarshalPorts(ports []SvcPort) (string, error) {
	if len(ports) == 0 {
		return "", nil
	}
	b, err := json.Marshal(serviceSpec{Ports: ports})
	return string(b), err
}

// ParsePorts decodes the ports out of a spec_json column value ("" => none).
// Unknown fields are ignored, so it also reads a full exporter `spec` blob.
func ParsePorts(specJSON string) ([]SvcPort, error) {
	if specJSON == "" {
		return nil, nil
	}
	var s serviceSpec
	if err := json.Unmarshal([]byte(specJSON), &s); err != nil {
		return nil, err
	}
	return s.Ports, nil
}

// GatewayCand is one candidate gateway from the 3-hop (name + server host
// patterns, enough to build a gwresolve.Gateway).
type GatewayCand struct {
	Namespace   string
	Name        string
	ServerHosts []string
}

// DeployRow / GatewayRow / VSRow are full version rows for the range path (the
// point-in-time path takes only what each hop needs). They carry the materialized
// ValidFrom/ValidTo so the in-memory window can do AsOf without re-hitting the DB;
// GatewayRow/VSRow keep spec_json so ScopedFor can rebuild off the loaded rows.
type DeployRow struct {
	Namespace, Name    string
	ValidFrom, ValidTo time.Time
	PodLabels          []string
	IngestSeq          uint64
}

type GatewayRow struct {
	Namespace, Name    string
	ValidFrom, ValidTo time.Time
	SelectorKV         []string
	ServerHosts        []string
	SpecJSON           string
	IngestSeq          uint64
}

type VSRow struct {
	Namespace, Name    string
	ValidFrom, ValidTo time.Time
	BoundGateways      []string
	SpecJSON           string
	IngestSeq          uint64
}

// TrafficWindow is every resource version overlapping [t0,t1) that is reachable
// from one destination IP — the ingress Service, its Deployment, the candidate
// Gateways, their bound VirtualServices, and the backend Services those VS route
// to. It is loaded once (LoadTrafficWindow), then sliced/resolved in memory
// (internal/memwindow) with no further DB round-trips. Each row's ValidTo is the
// materialized column value, so AsOf(t) is `ValidFrom <= t < ValidTo`.
type TrafficWindow struct {
	Services []ServiceRow // ingress LB + backend destination Service versions
	Deploys  []DeployRow
	Gateways []GatewayRow
	VSes     []VSRow
}

// Store is the versioned config store one backend implements. Reads answer the
// two things the "host+path -> service" engine needs: the 3-hop IP->Gateway
// selector join (ResolveIPToGateways) and one gateway's translate input at time
// T (ScopedFor). Writes stream the corpus through the typed batch inserters.
type Store interface {
	Close() error
	CreateSchema(ctx context.Context) error

	NewServiceBatch(ctx context.Context) (ServiceBatch, error)
	NewDeployBatch(ctx context.Context) (DeployBatch, error)
	NewGwBatch(ctx context.Context) (GwBatch, error)
	NewVSBatch(ctx context.Context) (VSBatch, error)

	// ResolveIPToGateways runs the 3-hop selector join for a destination IP at
	// time t: IP -> ingress Service (selector) -> ingress Deployment pod labels
	// L -> gateways whose selector ⊆ L. Empty result => traffic miss.
	ResolveIPToGateways(ctx context.Context, ip string, t time.Time) ([]GatewayCand, error)
	// AllGatewaysLiveAt returns every gateway version live at time t (name +
	// server hosts), for the config-only path that disambiguates a host across
	// all gateways without an IP 3-hop.
	AllGatewaysLiveAt(ctx context.Context, t time.Time) ([]GatewayCand, error)
	// ScopedFor rebuilds one gateway's translate input (its Gateway CR + bound
	// VirtualServices + destination Services) as-of time t. ok=false if the
	// gateway has no version live at t.
	ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error)

	// LoadTrafficWindow fetches every resource version overlapping [t0,t1) that is
	// reachable from destination IP (one scoped Overlap load per resource kind,
	// using the materialized valid_to: valid_from < t1 AND t0 < valid_to). The
	// range query slices and resolves the returned window in memory.
	LoadTrafficWindow(ctx context.Context, ip string, t0, t1 time.Time) (TrafficWindow, error)

	// CountRows returns the row count of a table (all versions).
	CountRows(ctx context.Context, table string) (uint64, error)
	// AsOfRev returns the version rev live at time t for one resource (multi-
	// version selection check). ok=false if none is live at t.
	AsOfRev(ctx context.Context, table, ns, name string, t time.Time) (uint32, bool, error)
}

// ServiceBatch / DeployBatch / GwBatch / VSBatch are typed, auto-chunked
// inserters. Append buffers one row (fanning out to child tables where a backend
// normalizes array columns); Close flushes the final chunk.
type ServiceBatch interface {
	Append(r ServiceRow) error
	Close() error
}

type DeployBatch interface {
	Append(ns, name string, from, to time.Time, rev uint32, podLabels []string, seq uint64) error
	Close() error
}

type GwBatch interface {
	Append(ns, name string, from, to time.Time, rev uint32, selectorKV, serverHosts []string, specJSON string, seq uint64) error
	Close() error
}

type VSBatch interface {
	Append(ns, name string, from, to time.Time, rev uint32, boundGateways []string, specJSON string, seq uint64) error
	Close() error
}
