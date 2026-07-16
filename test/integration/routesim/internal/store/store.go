// Package store holds the value types of the versioned routing-config store as
// the reader side sees them. It is a read-only trim of poc/route2a/internal/store
// adapted to the PRODUCTION schema — the four *_versions tables written by the
// metadata-exporter's history ingest (pkg/history + pkg/store), whose envelope
// is (namespace, name, uid, valid_from, valid_to, ingest_seq) — no synthetic
// `rev` oracle column like the PoC. This package imports no backend, so it
// stays a leaf (the backend imports it).
package store

import (
	"encoding/json"
	"strings"
	"time"
)

// ServiceRow is one service_versions row (ingress LB: ingress_ips/selector set;
// backend: Ports set). One row per Service version — a backend Service's ports
// all live in Ports (decoded from the spec_json column), so a multi-port Service
// is a single row, not one row per port. The backend Service's identity is
// (Namespace, Name); its FQDN (the destination.host a VS routes to) is derived —
// see BackendFQDN / ParseBackendHost.
type ServiceRow struct {
	Namespace, Name      string
	UID                  string
	ValidFrom, ValidTo   time.Time
	IngressIPs, Selector []string
	Ports                []SvcPort
	IngestSeq            uint64
}

// SvcPort is one Service port. JSON tags match the corev1 ServiceSpec.ports
// shape so the reader unmarshals it straight from the exporter's `spec` blob
// (extra fields ignored).
type SvcPort struct {
	Name     string `json:"name"`
	Port     uint32 `json:"port"`
	Protocol string `json:"protocol"`
}

// serviceDomain is the standard Kubernetes Service FQDN suffix. The exporter
// stores only (namespace, name), so the FQDN mapping is Istio-side derivation
// that lives here in the reader layer.
const serviceDomain = ".svc.cluster.local"

// BackendFQDN reconstructs a backend Service's FQDN (its destination.host
// identity) from its (name, namespace) — the inverse of ParseBackendHost.
func BackendFQDN(name, ns string) string { return name + "." + ns + serviceDomain }

// ParseBackendHost derives (name, namespace) from a VirtualService route's
// destination.host FQDN (name.namespace.svc.cluster.local). ok=false for a host
// that isn't a resolvable in-cluster FQDN (e.g. a bare short name or external
// host), which the caller skips.
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

// serviceSpec is the subtree of a Service spec the reader needs out of the
// spec_json column: just the ports.
type serviceSpec struct {
	Ports []SvcPort `json:"ports"`
}

// ParsePorts decodes the ports out of a spec_json column value ("" => none).
// Unknown fields are ignored, so it reads a full exporter `spec` blob.
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

// DeployRow / GatewayRow / VSRow are full version rows for the range path. They
// carry the materialized ValidFrom/ValidTo so the in-memory window can do AsOf
// without re-hitting the DB; GatewayRow/VSRow keep spec_json so ScopedFor can
// rebuild off the loaded rows.
type DeployRow struct {
	Namespace, Name    string
	UID                string
	ValidFrom, ValidTo time.Time
	PodLabels          []string
	IngestSeq          uint64
}

type GatewayRow struct {
	Namespace, Name    string
	UID                string
	ValidFrom, ValidTo time.Time
	SelectorKV         []string
	ServerHosts        []string
	SpecJSON           string
	IngestSeq          uint64
}

type VSRow struct {
	Namespace, Name    string
	UID                string
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
