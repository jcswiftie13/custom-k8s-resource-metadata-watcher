// Package memwindow slices and resolves a store.TrafficWindow in memory. Given
// the versions overlapping [t0,t1) loaded once by store.LoadTrafficWindow, it
// splits the interval at every version boundary (Segments) and answers the same
// 3-hop IP->Gateway and per-gateway ScopedFor the point-in-time store does — but
// against the already-loaded rows, with no further DB round-trips.
//
// The set/AsOf logic mirrors internal/chstore exactly: has -> Contains, hasAll ->
// containsAll, and the materialized valid_to gives AsOf(t) = ValidFrom <= t < ValidTo.
package memwindow

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"

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

// Window is a loaded TrafficWindow scoped to [t0,t1), ready for in-memory queries.
type Window struct {
	w      store.TrafficWindow
	t0, t1 time.Time
}

// New wraps a loaded window over the half-open interval [t0,t1).
func New(w store.TrafficWindow, t0, t1 time.Time) *Window {
	return &Window{w: w, t0: t0, t1: t1}
}

// Segment is a half-open [From,To) sub-interval during which every resource
// version in the window is constant (no version boundary falls strictly inside).
type Segment struct {
	From, To time.Time
}

// Segments splits [t0,t1) at every version boundary (valid_from) that falls
// strictly inside it, yielding contiguous half-open segments. Resolving at each
// segment's From gives the answer for the whole segment (the live version set
// does not change until the next boundary).
func (m *Window) Segments() []Segment {
	bset := map[time.Time]bool{m.t0: true}
	add := func(vf time.Time) {
		if vf.After(m.t0) && vf.Before(m.t1) {
			bset[vf] = true
		}
	}
	for _, r := range m.w.Services {
		add(r.ValidFrom)
	}
	for _, r := range m.w.Deploys {
		add(r.ValidFrom)
	}
	for _, r := range m.w.Gateways {
		add(r.ValidFrom)
	}
	for _, r := range m.w.VSes {
		add(r.ValidFrom)
	}
	bounds := make([]time.Time, 0, len(bset))
	for b := range bset {
		bounds = append(bounds, b)
	}
	sort.Slice(bounds, func(i, j int) bool { return bounds[i].Before(bounds[j]) })

	segs := make([]Segment, 0, len(bounds))
	for i, b := range bounds {
		to := m.t1
		if i+1 < len(bounds) {
			to = bounds[i+1]
		}
		segs = append(segs, Segment{From: b, To: to})
	}
	return segs
}

// ResolveIPToGateways runs the 3-hop selector join for a destination IP as-of t
// over the loaded rows: IP -> ingress Service (selector) -> ingress Deployment
// pod labels L -> gateways whose selector ⊆ L. Empty result => traffic miss.
func (m *Window) ResolveIPToGateways(ip string, t time.Time) []store.GatewayCand {
	// Hop 1: IP -> ingress Service (its namespace + selector).
	var svcNS string
	var svcSel []string
	found := false
	for i := range m.w.Services {
		r := &m.w.Services[i]
		if asOf(r.ValidFrom, r.ValidTo, t) && contains(r.IngressIPs, ip) {
			svcNS, svcSel, found = r.Namespace, r.Selector, true
			break
		}
	}
	if !found {
		return nil
	}

	// Hop 2: Service selector -> ingress Deployment pod labels L (svc.selector ⊆ L).
	var podLabels []string
	found = false
	for i := range m.w.Deploys {
		r := &m.w.Deploys[i]
		if asOf(r.ValidFrom, r.ValidTo, t) && r.Namespace == svcNS && containsAll(r.PodLabels, svcSel) {
			podLabels, found = r.PodLabels, true
			break
		}
	}
	if !found {
		return nil
	}

	// Hop 3: L -> candidate gateways (gateway.selector ⊆ L).
	var cands []store.GatewayCand
	for i := range m.w.Gateways {
		r := &m.w.Gateways[i]
		if asOf(r.ValidFrom, r.ValidTo, t) && containsAll(podLabels, r.SelectorKV) {
			cands = append(cands, store.GatewayCand{Namespace: r.Namespace, Name: r.Name, ServerHosts: r.ServerHosts})
		}
	}
	return cands
}

// GatewaysLiveAt returns every gateway version live at t (name + server hosts),
// enough to build a gwresolve.Resolver for the segment.
func (m *Window) GatewaysLiveAt(t time.Time) []store.GatewayCand {
	var out []store.GatewayCand
	for i := range m.w.Gateways {
		r := &m.w.Gateways[i]
		if asOf(r.ValidFrom, r.ValidTo, t) {
			out = append(out, store.GatewayCand{Namespace: r.Namespace, Name: r.Name, ServerHosts: r.ServerHosts})
		}
	}
	return out
}

// ConfigSigAt returns a content signature of the scoped config that feeds
// translation for gwName at t: the live Gateway spec, the live VirtualService
// specs bound to it, and the live backend Service identities. It is derived from
// the raw rows WITHOUT unmarshaling protos, so it is cheap enough to call per
// segment. Two times with the same signature translate to the same
// RouteConfiguration and resolve identically, so rangequery runs the expensive
// translate+router_check once per distinct signature instead of once per version
// segment. ok=false if no gateway version is live at t.
//
// INVARIANT (the dedup is only correct if this holds): the signature must cover
// EVERY input ScopedFor feeds to the translator — it must be a SUPERSET of
// ScopedFor's inputs. If it misses one, two genuinely-different configs could
// share a signature and the range query would return a stale cluster. So: when you
// add a new routing input to ScopedFor (e.g. DestinationRule, ServiceEntry, a new
// spec field), add it here too. TestConfigSigCoversScopedInputs enforces this — it
// perturbs every resource ScopedFor emits and fails if the signature is unmoved.
//
// Notes:
//   - Keys on spec_json CONTENT, not ingest_seq, so identical specs at different
//     versions collapse (the common case: a version bump that doesn't change
//     routing).
//   - Deployments are intentionally excluded: they only affect WHICH gateway the
//     3-hop selects, and rangequery re-runs the 3-hop per segment (it is not
//     deduped), so the resolved gateway already reflects deploy state.
//   - Uses a 64-bit hash. The cache that keys on this signature is PER-QUERY and
//     holds only a handful of distinct configs (bounded by the version boundaries
//     in the window — tens at most), so the collision probability (~n²/2⁶⁴ with
//     n≈tens) is astronomically small. If you ever widen the cache to a long-lived
//     or cross-query scope, upgrade to a wider/cryptographic hash here.
func (m *Window) ConfigSigAt(gwName string, t time.Time) (uint64, bool) {
	var gw *store.GatewayRow
	for i := range m.w.Gateways {
		r := &m.w.Gateways[i]
		if r.Name == gwName && asOf(r.ValidFrom, r.ValidTo, t) {
			gw = r
			break
		}
	}
	if gw == nil {
		return 0, false
	}

	h := fnv.New64a()
	fmt.Fprintf(h, "g:%s/%s\x00%s\x01", gw.Namespace, gw.Name, gw.SpecJSON)

	ref := gw.Namespace + "/" + gwName
	var vsParts []string
	for i := range m.w.VSes {
		r := &m.w.VSes[i]
		if asOf(r.ValidFrom, r.ValidTo, t) && contains(r.BoundGateways, ref) {
			vsParts = append(vsParts, r.Namespace+"/"+r.Name+"\x00"+r.SpecJSON)
		}
	}
	sort.Strings(vsParts)
	for _, p := range vsParts {
		fmt.Fprintf(h, "v:%s\x01", p)
	}

	var svcParts []string
	for i := range m.w.Services {
		r := &m.w.Services[i]
		// Backend Services (Ports set) feed ScopedFor; fold identity + the whole
		// port set so the signature stays a superset of the translate input.
		if len(r.Ports) == 0 || !asOf(r.ValidFrom, r.ValidTo, t) {
			continue
		}
		part := r.Namespace + "\x00" + r.Name
		for _, p := range r.Ports {
			part += fmt.Sprintf("\x00%s\x00%d", p.Name, p.Port)
		}
		svcParts = append(svcParts, part)
	}
	sort.Strings(svcParts)
	for _, p := range svcParts {
		fmt.Fprintf(h, "s:%s\x01", p)
	}

	return h.Sum64(), true
}

// ScopedFor rebuilds one gateway's translate input as-of t from the loaded rows:
// its Gateway CR + the VirtualServices bound to it + the backend Services those
// VS route to. ok=false if the gateway has no version live at t.
func (m *Window) ScopedFor(gwName string, t time.Time) (translate.ScopedInput, bool, error) {
	var gw *store.GatewayRow
	for i := range m.w.Gateways {
		r := &m.w.Gateways[i]
		if r.Name == gwName && asOf(r.ValidFrom, r.ValidTo, t) {
			gw = r
			break
		}
	}
	if gw == nil {
		return translate.ScopedInput{}, false, nil
	}
	var gwSpec networking.Gateway
	if err := protojson.Unmarshal([]byte(gw.SpecJSON), &gwSpec); err != nil {
		return translate.ScopedInput{}, false, err
	}
	cfgs := []config.Config{{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: gwName, Namespace: gw.Namespace},
		Spec: &gwSpec,
	}}

	ref := gw.Namespace + "/" + gwName
	var destHosts []string
	for i := range m.w.VSes {
		r := &m.w.VSes[i]
		if !asOf(r.ValidFrom, r.ValidTo, t) || !contains(r.BoundGateways, ref) {
			continue
		}
		var vsSpec networking.VirtualService
		if err := protojson.Unmarshal([]byte(r.SpecJSON), &vsSpec); err != nil {
			return translate.ScopedInput{}, false, err
		}
		destHosts = append(destHosts, vsDestHosts(&vsSpec)...)
		cfgs = append(cfgs, config.Config{
			Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: r.Name, Namespace: r.Namespace},
			Spec: &vsSpec,
		})
	}

	return translate.ScopedInput{
		Configs:  cfgs,
		Services: m.backendServices(destHosts, t),
		Proxy:    translate.GatewayProxy{Name: gwName, Namespace: gw.Namespace, Labels: gwSpec.GetSelector()},
	}, true, nil
}

// backendServices rebuilds the destination Services matching hosts as-of t.
// Identity is the (namespace, name) parsed from each destination.host FQDN — not
// scoped by the gateway namespace, so it's portable to production where backend
// Service ns == VS ns != gateway ns. Each matched row is one Service carrying all
// its ports.
func (m *Window) backendServices(hosts []string, t time.Time) []*model.Service {
	if len(hosts) == 0 {
		return nil
	}
	want := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		if name, ns, ok := store.ParseBackendHost(h); ok {
			want[ns+"/"+name] = true
		}
	}
	var out []*model.Service
	for i := range m.w.Services {
		r := &m.w.Services[i]
		if len(r.Ports) == 0 || !want[r.Namespace+"/"+r.Name] || !asOf(r.ValidFrom, r.ValidTo, t) {
			continue
		}
		pl := make(model.PortList, 0, len(r.Ports))
		for _, p := range r.Ports {
			pl = append(pl, &model.Port{Name: p.Name, Port: int(p.Port), Protocol: protocol.Parse(p.Name)})
		}
		out = append(out, &model.Service{
			Hostname:       host.Name(store.BackendFQDN(r.Name, r.Namespace)),
			DefaultAddress: "0.0.0.0",
			Ports:          pl,
			Attributes:     model.ServiceAttributes{Namespace: r.Namespace},
		})
	}
	return out
}

// asOf reports whether the version [vf,vt) is live at t: vf <= t < vt.
func asOf(vf, vt, t time.Time) bool { return !vf.After(t) && t.Before(vt) }

// contains reports whether set has x (mirrors ClickHouse has()).
func contains(set []string, x string) bool {
	for _, s := range set {
		if s == x {
			return true
		}
	}
	return false
}

// containsAll reports whether sub ⊆ super (mirrors ClickHouse hasAll(super, sub)).
func containsAll(super, sub []string) bool {
	for _, x := range sub {
		if !contains(super, x) {
			return false
		}
	}
	return true
}

// vsDestHosts collects the destination host (target Service identity FQDN) of
// every route in a VirtualService. Kept in sync with chstore.vsDestHosts.
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
		if mr := r.GetMirror(); mr != nil {
			add(mr)
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
