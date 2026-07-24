package translate_test

import (
	"runtime"
	"testing"

	networking "istio.io/api/networking/v1alpha3"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/schema/gvk"

	"google.golang.org/protobuf/proto"

	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// scopedFixture is the same one-gateway/one-VS scoped input as the spike test,
// reused here to compare the two translation paths on identical input. Helpers
// (gwConfig, svc, route, exact/prefix/regex) live in scoped_test.go.
func scopedFixture() translate.ScopedInput {
	const ns = "gw-000"
	gw := gwConfig("gw-000", "istio-system", map[string]string{"istio": "gw-000"}, []string{"*.gw000.example.com"})
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
	return translate.ScopedInput{
		Configs:  []config.Config{gw, vs},
		Services: services,
		Proxy:    translate.GatewayProxy{Name: "gw-000", Namespace: "istio-system", Labels: map[string]string{"istio": "gw-000"}},
	}
}

// TestTranslatorMatchesRoutesForScoped is the de-risk (plan Step 1): the
// controller-free Translator must produce a RouteConfiguration identical to the
// NewConfigGenTest-based RoutesForScoped path for the same scoped input.
func TestTranslatorMatchesRoutesForScoped(t *testing.T) {
	in := scopedFixture()

	old := translate.RoutesForScoped(t, in)
	got, err := translate.NewTranslator().Translate(in)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !proto.Equal(old, got) {
		t.Fatalf("Translator RC != RoutesForScoped RC\n old=%v\n new=%v", old, got)
	}
}

// TestTranslatorNoGoroutineLeak asserts repeated translation does not accumulate
// goroutines. The old path leaked 2 goroutines per call (tied to t.Cleanup),
// so 200 calls would add ~400; the controller-free path should add ~0.
func TestTranslatorNoGoroutineLeak(t *testing.T) {
	in := scopedFixture()
	tr := translate.NewTranslator()

	if _, err := tr.Translate(in); err != nil { // warm up
		t.Fatal(err)
	}
	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 200; i++ {
		if _, err := tr.Translate(in); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	runtime.GC()
	after := runtime.NumGoroutine()

	if after > before+10 {
		t.Fatalf("goroutine growth after 200 translations: before=%d after=%d", before, after)
	}
}
