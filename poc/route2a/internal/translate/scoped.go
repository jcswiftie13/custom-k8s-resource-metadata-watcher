// Package translate turns one ingress gateway's scoped Istio config (its single
// Gateway CR + the VirtualServices bound to it + the Services those VS route to)
// into the Envoy http.80 RouteConfiguration istiod would push to that gateway.
// It drives istio's config-generator core directly (see translator.go), so no
// FakeDiscoveryServer or whole-cluster load is needed — mirroring what a real
// "host+path -> service" query engine loads per gateway.
package translate

import (
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/test"
)

// RouteConfigName is the fixed name istiod assigns an HTTP :80 gateway's
// RouteConfiguration (<protocol>.<port>). Confirmed against out/*.json.
const RouteConfigName = "http.80"

// GatewayProxy identifies one ingress-gateway vantage. Labels must equal the
// Gateway resource's spec.selector so istiod attaches that Gateway to the
// synthetic proxy.
type GatewayProxy struct {
	Name      string
	Namespace string
	Labels    map[string]string
}

// ScopedInput is everything needed to translate ONE ingress gateway's http.80
// RouteConfiguration: its config (exactly one Gateway CR + the VirtualServices
// bound to it) and the Services those VS route to. Proxy.Labels must equal the
// Gateway's spec.selector so istiod attaches that Gateway to the vantage.
type ScopedInput struct {
	Configs  []config.Config
	Services []*model.Service
	Proxy    GatewayProxy
}

// sharedTranslator backs the RoutesForScoped test helper. Translator is
// stateless/concurrency-safe, so one package-level instance is fine.
var sharedTranslator = NewTranslator()

// RoutesForScoped is a thin test-only helper over Translator.Translate: it runs
// istio's config-generator core over one ingress's scoped config and returns the
// http.80 RouteConfiguration istiod would push to it, failing the test on error.
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
