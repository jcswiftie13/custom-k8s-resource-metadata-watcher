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

	if rc.GetName() != "http.80" {
		t.Fatalf("rc name = %q, want %q", rc.GetName(), "http.80")
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
		"exact:/healthz":           cl("svc-000-0-exact"),
		"prefix:/api/v1":           cl("svc-000-0-prefix"),
		"regex:^/products/[0-9]+$": cl("svc-000-0-regex"),
		"prefix:/":                 cl("svc-000-0-default"), // catch-all becomes prefix:/
	}
	for m, wantCluster := range want {
		if got[m] != wantCluster {
			t.Errorf("match %s: cluster = %q, want %q", m, got[m], wantCluster)
		}
	}
}

// TestMultiPortBackendServiceClusters proves a single backend Service carrying
// MULTIPLE ports (the shape spec_json now stores — one row/one model.Service with
// a multi-port PortList) is usable on every port: routing to each port resolves
// to that port's own outbound|<port>||<fqdn> cluster. istiod only builds a route
// cluster for a destination port the service actually exposes, so this fails if
// the second port were dropped (the old scalar-column / ReplacingMergeTree bug).
func TestMultiPortBackendServiceClusters(t *testing.T) {
	const ns = "gw-000"
	dh := "svc-multi." + ns + ".svc.cluster.local"

	gw := gwConfig("gw-000", "istio-system", map[string]string{"istio": "gw-000"}, []string{"*.gw000.example.com"})
	vs := config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: "vs-multi", Namespace: ns},
		Spec: &networking.VirtualService{
			Hosts:    []string{"svc0.gw000.example.com"},
			Gateways: []string{"istio-system/gw-000"},
			Http: []*networking.HTTPRoute{
				route(exact("/p8080"), dh, 8080),
				route(exact("/p8443"), dh, 8443),
			},
		},
	}
	// One Service, both ports — exactly what backendModel / svcModel now build.
	multi := &model.Service{
		Hostname:       host.Name(dh),
		DefaultAddress: "0.0.0.0",
		Ports: model.PortList{
			&model.Port{Name: "http", Port: 8080, Protocol: protocol.HTTP},
			&model.Port{Name: "http-alt", Port: 8443, Protocol: protocol.HTTP},
		},
		Attributes: model.ServiceAttributes{Namespace: ns},
	}

	rc, err := translate.NewTranslator().Translate(translate.ScopedInput{
		Configs:  []config.Config{gw, vs},
		Services: []*model.Service{multi},
		Proxy:    translate.GatewayProxy{Name: "gw-000", Namespace: "istio-system", Labels: map[string]string{"istio": "gw-000"}},
	})
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}

	got := map[string]string{}
	for _, vh := range rc.GetVirtualHosts() {
		for _, rt := range vh.GetRoutes() {
			got[matchStr(rt.GetMatch())] = rt.GetRoute().GetCluster()
		}
	}
	want := map[string]string{
		"exact:/p8080": "outbound|8080||" + dh,
		"exact:/p8443": "outbound|8443||" + dh,
	}
	for m, wantCluster := range want {
		if got[m] != wantCluster {
			t.Errorf("match %s: cluster = %q, want %q (multi-port service must expose both ports)", m, got[m], wantCluster)
		}
	}
}

// gwConfigHTTPSAndHTTP builds a gateway with BOTH a plain HTTP :80 server and a
// TLS-terminated HTTPS :443 server, sharing the same host pattern (the shape
// scalegen now generates). tls controls the 443 server's TLS mode.
func gwConfigHTTPSAndHTTP(name, ns string, selector map[string]string, hosts []string, tls networking.ServerTLSSettings_TLSmode) config.Config {
	return config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: name, Namespace: ns},
		Spec: &networking.Gateway{
			Selector: selector,
			Servers: []*networking.Server{
				{Port: &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"}, Hosts: hosts},
				{
					Port:  &networking.Port{Number: 443, Name: "https", Protocol: "HTTPS"},
					Hosts: hosts,
					Tls:   &networking.ServerTLSSettings{Mode: tls, CredentialName: "cred-000"},
				},
			},
		},
	}
}

// TestTranslatePortSelectsRouteConfig proves ScopedInput.Port selects the RC:
// :80 -> "http.80", TLS-terminated :443 -> "https.443.https.<gw>.<ns>" (same
// clusters as :80, since VS routing is port-independent), and an unserved port
// (:8080) or a passthrough :443 server -> an empty RC (a miss).
func TestTranslatePortSelectsRouteConfig(t *testing.T) {
	const ns = "gw-000"
	dh := "svc-000-0-exact." + ns + ".svc.cluster.local"
	wantCluster := "outbound|8080||" + dh

	vs := config.Config{
		Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: "svc0", Namespace: ns},
		Spec: &networking.VirtualService{
			Hosts:    []string{"svc0.gw000.example.com"},
			Gateways: []string{"istio-system/gw-000"},
			Http:     []*networking.HTTPRoute{route(exact("/healthz"), dh, 8080)},
		},
	}
	services := []*model.Service{svc(dh, ns, 8080)}
	proxy := translate.GatewayProxy{Name: "gw-000", Namespace: "istio-system", Labels: map[string]string{"istio": "gw-000"}}

	clusterFor := func(rc *envoyroute.RouteConfiguration) string {
		for _, vh := range rc.GetVirtualHosts() {
			for _, rt := range vh.GetRoutes() {
				if matchStr(rt.GetMatch()) == "exact:/healthz" {
					return rt.GetRoute().GetCluster()
				}
			}
		}
		return ""
	}

	tr := translate.NewTranslator()

	cases := []struct {
		name     string
		port     int
		tls      networking.ServerTLSSettings_TLSmode
		wantName string
		wantHit  bool // true => the /healthz route resolves to wantCluster
	}{
		{"http_80", 80, networking.ServerTLSSettings_SIMPLE, "http.80", true},
		{"https_443_terminated", 443, networking.ServerTLSSettings_SIMPLE, "https.443.https.gw-000.istio-system", true},
		{"unserved_8080_miss", 8080, networking.ServerTLSSettings_SIMPLE, "http.8080", false},
		{"passthrough_443_miss", 443, networking.ServerTLSSettings_PASSTHROUGH, "http.443", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gw := gwConfigHTTPSAndHTTP("gw-000", "istio-system", map[string]string{"istio": "gw-000"}, []string{"*.gw000.example.com"}, c.tls)
			rc, err := tr.Translate(translate.ScopedInput{
				Configs:  []config.Config{gw, vs},
				Services: services,
				Proxy:    proxy,
				Port:     c.port,
			})
			if err != nil {
				t.Fatalf("Translate: %v", err)
			}
			if rc.GetName() != c.wantName {
				t.Errorf("rc name = %q, want %q", rc.GetName(), c.wantName)
			}
			got := clusterFor(rc)
			if c.wantHit {
				if got != wantCluster {
					t.Errorf("/healthz cluster = %q, want %q", got, wantCluster)
				}
			} else if got != "" {
				t.Errorf("/healthz cluster = %q, want miss (empty RC)", got)
			}
		})
	}
}
