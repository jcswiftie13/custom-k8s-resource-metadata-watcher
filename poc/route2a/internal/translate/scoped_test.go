package translate_test

import (
	"testing"
	"time"

	envoyroute "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	networking "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/host"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/gvk"

	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// svc builds a minimal HTTP model.Service for the mem registry.
func svc(fqdn, ns string, port int) *model.Service {
	return &model.Service{
		Hostname:       host.Name(fqdn),
		DefaultAddress: "0.0.0.0",
		Ports: model.PortList{
			&model.Port{Name: "http", Port: port, Protocol: protocol.HTTP},
		},
		Attributes: model.ServiceAttributes{Namespace: ns},
	}
}

func gwConfig(name, ns string, selector map[string]string, hosts []string) config.Config {
	return config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: name, Namespace: ns},
		Spec: &networking.Gateway{
			Selector: selector,
			Servers: []*networking.Server{{
				Port:  &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"},
				Hosts: hosts,
			}},
		},
	}
}

func route(uri *networking.StringMatch, destHost string, port uint32) *networking.HTTPRoute {
	r := &networking.HTTPRoute{
		Route: []*networking.HTTPRouteDestination{{
			Destination: &networking.Destination{Host: destHost, Port: &networking.PortSelector{Number: port}},
		}},
	}
	if uri != nil {
		r.Match = []*networking.HTTPMatchRequest{{Uri: uri}}
	}
	return r
}

func matchStr(m *envoyroute.RouteMatch) string {
	switch p := m.GetPathSpecifier().(type) {
	case *envoyroute.RouteMatch_Prefix:
		return "prefix:" + p.Prefix
	case *envoyroute.RouteMatch_Path:
		return "exact:" + p.Path
	case *envoyroute.RouteMatch_SafeRegex:
		return "regex:" + p.SafeRegex.GetRegex()
	default:
		return "<other>"
	}
}

func exact(s string) *networking.StringMatch {
	return &networking.StringMatch{MatchType: &networking.StringMatch_Exact{Exact: s}}
}
func prefix(s string) *networking.StringMatch {
	return &networking.StringMatch{MatchType: &networking.StringMatch_Prefix{Prefix: s}}
}
func regex(s string) *networking.StringMatch {
	return &networking.StringMatch{MatchType: &networking.StringMatch_Regex{Regex: s}}
}

// TestRoutesForScopedSpike validates the scoped config-generator core path:
// build one ingress's config typed (as scalegen will), translate, and assert
// each route's cluster equals the by-construction expected cluster.
func TestRoutesForScopedSpike(t *testing.T) {
	const ns = "gw-000"
	gw := gwConfig("gw-000", "istio-system", map[string]string{"istio": "gw-000"}, []string{"*.gw000.example.com"})

	// destination hosts (FQDNs) + the by-construction expected clusters.
	cl := func(short string) string {
		return "outbound|8080||" + short + "." + ns + ".svc.cluster.local"
	}
	dh := func(short string) string { return short + "." + ns + ".svc.cluster.local" }

	vs := config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: "svc0", Namespace: ns},
		Spec: &networking.VirtualService{
			Hosts:    []string{"svc0.gw000.example.com"},
			Gateways: []string{"istio-system/gw-000"},
			Http: []*networking.HTTPRoute{
				route(exact("/healthz"), dh("svc-000-0-exact"), 8080),
				route(prefix("/api/v1"), dh("svc-000-0-prefix"), 8080),
				route(regex("^/products/[0-9]+$"), dh("svc-000-0-regex"), 8080),
				route(nil, dh("svc-000-0-default"), 8080), // catch-all
			},
		},
	}

	services := []*model.Service{
		svc(dh("svc-000-0-exact"), ns, 8080),
		svc(dh("svc-000-0-prefix"), ns, 8080),
		svc(dh("svc-000-0-regex"), ns, 8080),
		svc(dh("svc-000-0-default"), ns, 8080),
	}

	in := translate.ScopedInput{
		Configs:  []config.Config{gw, vs},
		Services: services,
		Proxy:    translate.GatewayProxy{Name: "gw-000", Namespace: "istio-system", Labels: map[string]string{"istio": "gw-000"}},
	}

	tSetup := time.Now()
	rc := translate.RoutesForScoped(t, in)
	elapsed := time.Since(tSetup)
	t.Logf("scoped translate (setup+build) took %s", elapsed)

	if rc.GetName() != translate.RouteConfigName {
		t.Fatalf("rc name = %q, want %q", rc.GetName(), translate.RouteConfigName)
	}

	// collect match->cluster
	got := map[string]string{}
	for _, vh := range rc.GetVirtualHosts() {
		for _, rt := range vh.GetRoutes() {
			got[matchStr(rt.GetMatch())] = rt.GetRoute().GetCluster()
		}
	}
	t.Logf("routes: %v", got)

	want := map[string]string{
		"exact:/healthz":            cl("svc-000-0-exact"),
		"prefix:/api/v1":            cl("svc-000-0-prefix"),
		"regex:^/products/[0-9]+$":  cl("svc-000-0-regex"),
		"prefix:/":                  cl("svc-000-0-default"), // catch-all becomes prefix:/
	}
	for m, wantCluster := range want {
		if got[m] != wantCluster {
			t.Errorf("match %s: cluster = %q, want %q", m, got[m], wantCluster)
		}
	}
}
