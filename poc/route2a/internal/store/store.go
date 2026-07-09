// Package store is the backend-agnostic contract for the ingress-traffic config
// store. It holds the value types and the Store/batch interfaces the pipeline
// depends on, so the loader (internal/ingload), the benchmark harness, and the
// ipflow CLI can run against ClickHouse, PostgreSQL, or MariaDB interchangeably.
//
// Each backend (internal/chstore, internal/pgstore, internal/mariastore)
// implements Store with the fastest single-node access path for its engine; the
// factory in internal/storeopen dials one by Backend name. This package imports
// no backend, so it stays a leaf (backends import it; the factory imports both).
package store

import (
	"context"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// Backend names a config-store engine. The value is what POC_DB / -db select.
type Backend string

const (
	BackendClickHouse Backend = "clickhouse"
	BackendPostgres   Backend = "postgres"
	BackendMariaDB    Backend = "mariadb"
)

// farFuture is the open-ended `valid_to` sentinel for the current version. It
// stays within ClickHouse DateTime64 range (max year 2299) while being
// comfortably after any query time; MariaDB DATETIME (max 9999) and PostgreSQL
// timestamp both accommodate it too.
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

// ServiceRow is one service_versions row (ingress LB: ingress_ips/selector set,
// hostname empty; backend: hostname/port set, ingress_ips empty).
type ServiceRow struct {
	Namespace, Name      string
	ValidFrom, ValidTo   time.Time
	Rev                  uint32
	IngressIPs, Selector []string
	Hostname             string
	Port                 uint32
	PortName, Protocol   string
	IngestSeq            uint64
}

// GatewayCand is one candidate gateway from the 3-hop (name + server host
// patterns, enough to build a gwresolve.Gateway).
type GatewayCand struct {
	Namespace   string
	Name        string
	ServerHosts []string
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
	// ScopedFor rebuilds one gateway's translate input (its Gateway CR + bound
	// VirtualServices + destination Services) as-of time t. ok=false if the
	// gateway has no version live at t.
	ScopedFor(ctx context.Context, gwName string, t time.Time) (translate.ScopedInput, bool, error)

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
