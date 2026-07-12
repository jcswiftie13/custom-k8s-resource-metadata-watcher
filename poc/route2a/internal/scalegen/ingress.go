package scalegen

// ingress.go adds the ingress-traffic view on top of the routing corpus: the
// per-gateway ingress infrastructure (LoadBalancer Service + workload Deployment)
// and a deterministic IP<->gateway oracle. This is what the ClickHouse 3-hop
// `IP -> Gateway` resolution (see internal/chstore) is validated against.
//
// Model (per ingress gateway i, mirroring the gw-NNN naming):
//
//	ingress Service     ing-gw-NNN  @istio-system  selector {istio: gw-NNN}
//	                    LB IP       10.0.<i/256>.<i%256>   (600 distinct)
//	ingress Deployment  ing-gw-NNN  @istio-system  podLabels {app: istio-ingressgateway, istio: gw-NNN}
//	Gateway CR          gw-NNN      @istio-system  selector {istio: gw-NNN}
//	backend Services    svc-NNN-JJ-<rule>.gw-NNN.svc.cluster.local:8080  (route destinations)
//
// The selector join that maps IP->Gateway: the Deployment's pod labels L is a
// SUPERSET of both the Service selector and the Gateway selector (it carries an
// extra `app` label), so `Gateway.selector ⊆ L` picks exactly gw-NNN (each
// gateway's `istio: gw-NNN` label is unique) — the by-construction uniqueness the
// 3-hop relies on.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"istio.io/istio/pkg/config"

	"github.com/example/metadata-exporter/poc/route2a/internal/store"
)

// ingressWorkloadApp is the extra pod-template label the ingress Deployment
// carries beyond the Gateway selector, so L is a strict superset (exercises the
// "use Deployment labels as L, not just Service.selector" design point).
const ingressWorkloadApp = "istio-ingressgateway"

// IngressServiceInfo is one ingress LoadBalancer Service (the IP entry point).
type IngressServiceInfo struct {
	Name      string
	Namespace string
	IP        string            // LB IP (Service.status.loadBalancer / spec.externalIPs)
	Selector  map[string]string // selects the ingress workload pods
}

// IngressDeploymentInfo is one ingress workload Deployment; its pod-template
// labels are the canonical label set L for the Gateway.selector ⊆ L test.
type IngressDeploymentInfo struct {
	Name      string
	Namespace string
	PodLabels map[string]string
}

// BackendSvc is one route-destination Service (what istiod turns into a cluster).
// Ports holds every port the Service serves (one row / one spec_json); its VS
// routes always target the primary svcPort.
type BackendSvc struct {
	Name      string
	Namespace string
	Hostname  string // FQDN (destination.host)
	Ports     []store.SvcPort
}

// ingressName is the ingress Service/Deployment name for gateway i.
func ingressName(i int) string { return "ing-" + gwName(i) }

// IPForGateway maps gateway ordinal i to its (distinct) ingress LB IP. Encodes i
// so GatewayForIP can invert it — the by-construction oracle, NOT parsed by the
// system under test (which resolves via the selector join in ClickHouse).
func IPForGateway(i int) string { return fmt.Sprintf("10.0.%d.%d", i/256, i%256) }

// GatewayForIP inverts IPForGateway: the correct gateway an ingress IP belongs
// to ("", false if the IP is not one of the generated ingress IPs).
func (g *Gen) GatewayForIP(ip string) (string, bool) {
	var a, b int
	if _, err := fmt.Sscanf(ip, "10.0.%d.%d", &a, &b); err != nil {
		return "", false
	}
	i := a*256 + b
	if i < 0 || i >= g.cfg.NumGateways {
		return "", false
	}
	return gwName(i), true
}

// IPForHost simulates the collector-side DNS lookup: the ingress IP a request
// host lands on. Deterministic (host svcJJ.gwNNN.example.com -> gw-NNN's IP);
// real deployments get this IP from the OTEL span / a resolver.
func (g *Gen) IPForHost(host string) (string, bool) {
	i, _, ok := g.parseHost(host)
	if !ok {
		return "", false
	}
	return IPForGateway(i), true
}

// IngressService returns gateway i's ingress LoadBalancer Service.
func (g *Gen) IngressService(i int) IngressServiceInfo {
	return IngressServiceInfo{
		Name:      ingressName(i),
		Namespace: gwNS,
		IP:        IPForGateway(i),
		Selector:  gwSelector(i), // {istio: gw-NNN}
	}
}

// IngressDeployment returns gateway i's ingress workload Deployment. Its pod
// labels are L = {app: istio-ingressgateway, istio: gw-NNN} (superset of both
// the Service and Gateway selectors).
func (g *Gen) IngressDeployment(i int) IngressDeploymentInfo {
	return IngressDeploymentInfo{
		Name:      ingressName(i),
		Namespace: gwNS,
		PodLabels: map[string]string{"app": ingressWorkloadApp, "istio": gwName(i)},
	}
}

// BackendServices returns the 4*VSPerGW route-destination Services of gateway i
// (one per VS rule), which translate needs in the registry to build clusters.
func (g *Gen) BackendServices(i int) []BackendSvc {
	out := make([]BackendSvc, 0, g.cfg.VSPerGW*len(ruleOrder))
	for j := 0; j < g.cfg.VSPerGW; j++ {
		for _, rule := range ruleOrder {
			out = append(out, BackendSvc{
				Name:      destShort(i, j, rule),
				Namespace: vsNS(i),
				Hostname:  destFQDN(i, j, rule),
				Ports:     backendPorts(i, j),
			})
		}
	}
	return out
}

// backendPorts is a Service's port set. Every Service serves the primary http
// port (svcPort) that its VS routes target, so the resolved cluster is always
// outbound|svcPort||fqdn; a deterministic subset also serves extra ports, so the
// multi-port build path is exercised without changing any oracle expectation.
func backendPorts(i, j int) []store.SvcPort {
	ports := []store.SvcPort{{Name: "http", Port: svcPort, Protocol: "TCP"}}
	switch (i + j) % 3 {
	case 1:
		ports = append(ports, store.SvcPort{Name: "https", Port: svcPortHTTPS, Protocol: "TCP"})
	case 2:
		ports = append(ports,
			store.SvcPort{Name: "https", Port: svcPortHTTPS, Protocol: "TCP"},
			store.SvcPort{Name: "grpc", Port: svcPortGRPC, Protocol: "TCP"})
	}
	return ports
}

// GatewayConfig / VSConfig expose the typed Istio configs (already built by the
// generator) so the loader can protojson-marshal their specs into ClickHouse.
func (g *Gen) GatewayConfig(i int) config.Config { return g.gwConfig(i) }
func (g *Gen) VSConfig(i, j int) config.Config   { return g.vsConfig(i, j) }

// NumGateways / VSPerGW expose the corpus dimensions to the loader.
func (g *Gen) NumGateways() int { return g.cfg.NumGateways }
func (g *Gen) VSPerGW() int     { return g.cfg.VSPerGW }

// BoundGatewayRef is the "<ns>/<name>" reference a VS of gateway i binds to
// (spec.gateways), used as the vs_versions filter key for translate.
func BoundGatewayRef(i int) string { return gwNS + "/" + gwName(i) }

// SortedKV flattens a label map into sorted "k=v" tokens — the canonical
// Array(String) form ClickHouse `hasAll` compares (sorting avoids format drift).
func SortedKV(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// GatewayHostPattern exposes the server-host pattern of gateway i (stored in
// gw_versions.server_hosts, later rebuilt into a gwresolve.Gateway).
func GatewayHostPattern(i int) string { return gwHostPat(i) }

// GatewayName / GatewayNamespace expose identity for the loader.
func GatewayName(i int) string                         { return gwName(i) }
func GatewayNamespace() string                         { return gwNS }
func (g *Gen) GatewaySelector(i int) map[string]string { return gwSelector(i) }

// parseGatewayOrdinal returns the ordinal of gw-NNN (-1 if not that form),
// exposed for callers that need to relate a resolved gateway name back to i.
func ParseGatewayOrdinal(name string) int {
	if !strings.HasPrefix(name, "gw-") {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimPrefix(name, "gw-"))
	if err != nil {
		return -1
	}
	return n
}
