// Package translate turns one ingress gateway's scoped Istio config (its single
// Gateway CR + the VirtualServices bound to it + the Services those VS route to)
// into the Envoy RouteConfiguration istiod would push to that gateway for a given
// listener port. It drives istio's config-generator core directly (see
// translator.go), so no FakeDiscoveryServer or whole-cluster load is needed —
// mirroring what a real "host+path -> service" query engine loads per gateway.
//
// The RouteConfiguration to build is selected by ScopedInput.Port (default 80):
// a plain HTTP server yields "http.<port>", a TLS-terminated HTTPS server yields
// "https.<port>.<portName>.<gwName>.<gwNamespace>" (see routeConfigNameFor). TLS
// passthrough servers have no HTTP RDS route and resolve to a miss.
package translate

import (
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/test"
)

// GatewayProxy identifies one ingress-gateway vantage. Labels must equal the
// Gateway resource's spec.selector so istiod attaches that Gateway to the
// synthetic proxy.
type GatewayProxy struct {
	Name      string
	Namespace string
	Labels    map[string]string
}

// ScopedInput is everything needed to translate ONE ingress gateway's
// RouteConfiguration for one listener port: its config (exactly one Gateway CR +
// the VirtualServices bound to it) and the Services those VS route to.
// Proxy.Labels must equal the Gateway's spec.selector so istiod attaches that
// Gateway to the vantage. Port selects which listener's RC to build (0 => 80).
type ScopedInput struct {
	Configs  []config.Config
	Services []*model.Service
	Proxy    GatewayProxy
	Port     int
}

// sharedTranslator backs the RoutesForScoped test helper. Translator is
// stateless/concurrency-safe, so one package-level instance is fine.
var sharedTranslator = NewTranslator()

// RoutesForScoped is a thin test-only helper over Translator.Translate: it runs
// istio's config-generator core over one ingress's scoped config and returns the
// RouteConfiguration (for in.Port, default 80) istiod would push to it, failing
// the test on error.
//
// Production code should use Translator.Translate directly (no test.Failer, no
// goroutine tied to the test lifetime). Kept so existing *_test.go call sites
// stay unchanged; t is satisfied by *testing.T / *testing.B.
func RoutesForScoped(t test.Failer, in ScopedInput) *route.RouteConfiguration {
	t.Helper()
	rc, err := sharedTranslator.Translate(in)
	if err != nil {
		t.Fatalf("scoped translate: %v", err)
	}
	return rc
}
