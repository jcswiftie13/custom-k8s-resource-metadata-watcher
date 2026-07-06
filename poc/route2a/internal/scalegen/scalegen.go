// Package scalegen deterministically GENERATES the large test corpus for the
// enhanced PoC by script (never hand-written YAML): 600 distinct ingress
// gateways, each with >=100 VirtualServices (>=60k VS total), plus a few
// overlapping-wildcard "broad" gateways for host-disambiguation coverage.
//
// It is the single source of ground truth:
//   - ScopedFor(gw) yields exactly one ingress's config (1 Gateway CR + its VS
//     + the Services those VS route to) for on-demand scoped translation — the
//     whole cluster is NEVER materialized at once.
//   - ExpectedCluster(host,path) and Cases(...) give an oracle computed purely
//     by construction, independent of istiod, so the translate+Envoy pipeline
//     is validated against something it did not itself produce.
//
// Naming (the ordinal is encoded in the hostname ONLY so the generator can
// build the oracle — the system under test must NOT parse it; it resolves the
// gateway by matching host against gateway server hosts, see internal/gwresolve):
//
//	gateway  gw-NNN         ns istio-system  selector {istio: gw-NNN}  host *.gwNNN.example.com
//	vs       vs-NNN-JJ      ns gw-NNN         host svcJJ.gwNNN.example.com  -> gateway gw-NNN
//	routes   exact /healthz, prefix /api/v1, regex ^/products/[0-9]+$, catch-all
//	service  svc-NNN-JJ-<rule>.gw-NNN.svc.cluster.local : 8080 (http)
//	cluster  outbound|8080||svc-NNN-JJ-<rule>.gw-NNN.svc.cluster.local
package scalegen

import (
	"fmt"
	"strconv"
	"strings"

	networking "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/host"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/gvk"

	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

const (
	svcPort   = 8080
	gwNS      = "istio-system"
	regexRule = "^/products/[0-9]+$"
)

// Config parameterizes the corpus (ramp 10->100->600 to find the knee).
type Config struct {
	NumGateways int // e.g. 600
	VSPerGW     int // e.g. 100
}

// DefaultConfig is the target scale: 600 gateways x 100 VS = 60k VS.
func DefaultConfig() Config { return Config{NumGateways: 600, VSPerGW: 100} }

// Gen builds config/services/cases on demand from a lightweight spec.
type Gen struct{ cfg Config }

func New(cfg Config) *Gen { return &Gen{cfg: cfg} }

// GatewayInfo is the per-gateway data gwresolve needs to match host->gateway.
type GatewayInfo struct {
	Name      string
	Namespace string
	Selector  map[string]string
	Hosts     []string // server hosts (wildcard patterns)
}

// Case is one by-construction query: the correct Gateway (oracle for gwresolve)
// and Expected cluster (oracle for matching; "" = miss).
type Case struct {
	Gateway  string
	Host     string
	Path     string
	Expected string
}

// broadName is an extra overlapping-wildcard gateway serving *.example.com,
// so every gwNNN host also matches it and gwresolve must pick most-specific.
const broadName = "gw-broad-all"

func pad3(i int) string { return fmt.Sprintf("%03d", i) }
func pad2(j int) string { return fmt.Sprintf("%02d", j) }

func gwName(i int) string      { return "gw-" + pad3(i) }
func gwSelector(i int) map[string]string { return map[string]string{"istio": gwName(i)} }
func gwHostPat(i int) string   { return "*.gw" + pad3(i) + ".example.com" }
func vsHost(i, j int) string   { return "svc" + pad2(j) + ".gw" + pad3(i) + ".example.com" }
func vsNS(i int) string        { return gwName(i) }

func destShort(i, j int, rule string) string { return "svc-" + pad3(i) + "-" + pad2(j) + "-" + rule }
func destFQDN(i, j int, rule string) string  { return destShort(i, j, rule) + "." + vsNS(i) + ".svc.cluster.local" }
func clusterOf(i, j int, rule string) string { return fmt.Sprintf("outbound|%d||%s", svcPort, destFQDN(i, j, rule)) }

// Gateways returns every gateway (the 600 + broad ones) for gwresolve.
func (g *Gen) Gateways() []GatewayInfo {
	out := make([]GatewayInfo, 0, g.cfg.NumGateways+1)
	for i := 0; i < g.cfg.NumGateways; i++ {
		out = append(out, GatewayInfo{
			Name: gwName(i), Namespace: gwNS, Selector: gwSelector(i),
			Hosts: []string{gwHostPat(i)},
		})
	}
	out = append(out, GatewayInfo{
		Name: broadName, Namespace: gwNS, Selector: map[string]string{"istio": broadName},
		Hosts: []string{"*.example.com"},
	})
	return out
}

// index returns the ordinal of gw-NNN, or -1 for the broad gateway / unknown.
func gwIndex(name string) int {
	if !strings.HasPrefix(name, "gw-") {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimPrefix(name, "gw-"))
	if err != nil {
		return -1
	}
	return n
}

// httpRoute builds one VS http route (uri==nil => catch-all).
func httpRoute(uri *networking.StringMatch, destHost string) *networking.HTTPRoute {
	r := &networking.HTTPRoute{
		Route: []*networking.HTTPRouteDestination{{
			Destination: &networking.Destination{Host: destHost, Port: &networking.PortSelector{Number: svcPort}},
		}},
	}
	if uri != nil {
		r.Match = []*networking.HTTPMatchRequest{{Uri: uri}}
	}
	return r
}

func exact(s string) *networking.StringMatch  { return &networking.StringMatch{MatchType: &networking.StringMatch_Exact{Exact: s}} }
func prefix(s string) *networking.StringMatch { return &networking.StringMatch{MatchType: &networking.StringMatch_Prefix{Prefix: s}} }
func regex(s string) *networking.StringMatch  { return &networking.StringMatch{MatchType: &networking.StringMatch_Regex{Regex: s}} }

func svcModel(fqdn, ns string) *model.Service {
	return &model.Service{
		Hostname:       host.Name(fqdn),
		DefaultAddress: "0.0.0.0",
		Ports:          model.PortList{&model.Port{Name: "http", Port: svcPort, Protocol: protocol.HTTP}},
		Attributes:     model.ServiceAttributes{Namespace: ns},
	}
}

// vsConfig builds VS j of gateway i (as typed config.Config).
func (g *Gen) vsConfig(i, j int) config.Config {
	ns := vsNS(i)
	return config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: "vs-" + pad3(i) + "-" + pad2(j), Namespace: ns},
		Spec: &networking.VirtualService{
			Hosts:    []string{vsHost(i, j)},
			Gateways: []string{gwNS + "/" + gwName(i)},
			Http: []*networking.HTTPRoute{
				httpRoute(exact("/healthz"), destFQDN(i, j, "exact")),
				httpRoute(prefix("/api/v1"), destFQDN(i, j, "prefix")),
				httpRoute(regex(regexRule), destFQDN(i, j, "regex")),
				httpRoute(nil, destFQDN(i, j, "default")),
			},
		},
	}
}

func (g *Gen) gwConfig(i int) config.Config {
	return config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: gwName(i), Namespace: gwNS},
		Spec: &networking.Gateway{
			Selector: gwSelector(i),
			Servers: []*networking.Server{{
				Port:  &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"},
				Hosts: []string{gwHostPat(i)},
			}},
		},
	}
}

// ScopedFor builds the scoped translate input for ONE ingress: its single
// Gateway CR + its VSPerGW VirtualServices + the ~4*VSPerGW Services they route
// to. Broad gateways are handled too (they carry no VS in this corpus, so their
// scoped RC is empty/all-miss, which is the faithful result).
func (g *Gen) ScopedFor(gwName string) (translate.ScopedInput, bool) {
	i := gwIndex(gwName)
	if i < 0 || i >= g.cfg.NumGateways {
		if gwName == broadName {
			return translate.ScopedInput{
				Configs: []config.Config{{
					Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: broadName, Namespace: gwNS},
					Spec: &networking.Gateway{
						Selector: map[string]string{"istio": broadName},
						Servers:  []*networking.Server{{Port: &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"}, Hosts: []string{"*.example.com"}}},
					},
				}},
				Proxy: translate.GatewayProxy{Name: broadName, Namespace: gwNS, Labels: map[string]string{"istio": broadName}},
			}, true
		}
		return translate.ScopedInput{}, false
	}

	cfgs := make([]config.Config, 0, g.cfg.VSPerGW+1)
	cfgs = append(cfgs, g.gwConfig(i))
	svcs := make([]*model.Service, 0, g.cfg.VSPerGW*4)
	for j := 0; j < g.cfg.VSPerGW; j++ {
		cfgs = append(cfgs, g.vsConfig(i, j))
		for _, rule := range []string{"exact", "prefix", "regex", "default"} {
			svcs = append(svcs, svcModel(destFQDN(i, j, rule), vsNS(i)))
		}
	}
	return translate.ScopedInput{
		Configs:  cfgs,
		Services: svcs,
		Proxy:    translate.GatewayProxy{Name: gwName, Namespace: gwNS, Labels: gwSelector(i)},
	}, true
}

// ExpectedCluster is the pure oracle: given a request host+path, the cluster the
// correct gateway's RC resolves it to ("" = miss). Independent of istiod.
func (g *Gen) ExpectedCluster(hostname, path string) string {
	i, j, ok := g.parseHost(hostname)
	if !ok {
		return ""
	}
	rule, ok := ruleForPath(path)
	if !ok {
		return ""
	}
	return clusterOf(i, j, rule)
}

// parseHost decodes svcJJ.gwNNN.example.com -> (i,j). ORACLE-ONLY; the SUT never
// does this — it resolves the gateway by host matching.
func (g *Gen) parseHost(h string) (i, j int, ok bool) {
	const suffix = ".example.com"
	if !strings.HasSuffix(h, suffix) {
		return 0, 0, false
	}
	labels := strings.Split(strings.TrimSuffix(h, suffix), ".") // [svcJJ, gwNNN]
	if len(labels) != 2 || !strings.HasPrefix(labels[0], "svc") || !strings.HasPrefix(labels[1], "gw") {
		return 0, 0, false
	}
	jj, err1 := strconv.Atoi(strings.TrimPrefix(labels[0], "svc"))
	ii, err2 := strconv.Atoi(strings.TrimPrefix(labels[1], "gw"))
	if err1 != nil || err2 != nil || ii < 0 || ii >= g.cfg.NumGateways || jj < 0 || jj >= g.cfg.VSPerGW {
		return 0, 0, false
	}
	return ii, jj, true
}

// ruleForPath maps a path to the VS rule it hits (mirrors first-match order:
// exact, then prefix, then regex, then catch-all).
func ruleForPath(path string) (rule string, hit bool) {
	switch {
	case path == "/healthz":
		return "exact", true
	case strings.HasPrefix(path, "/api/v1"):
		return "prefix", true
	case isProductsN(path):
		return "regex", true
	default:
		return "default", true // catch-all always hits (for a known host)
	}
}

func isProductsN(path string) bool {
	if !strings.HasPrefix(path, "/products/") {
		return false
	}
	n := strings.TrimPrefix(path, "/products/")
	if n == "" {
		return false
	}
	for _, r := range n {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
