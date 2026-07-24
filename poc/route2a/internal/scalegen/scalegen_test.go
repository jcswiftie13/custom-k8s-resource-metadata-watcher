package scalegen_test

import (
	"os"
	"path/filepath"
	"testing"

	envoyroute "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"istio.io/istio/pilot/pkg/config/kube/crd"

	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

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

// samplePathFor returns a representative hitting path for each rule kind.
func samplePathFor(match string) string {
	switch match {
	case "exact:/healthz":
		return "/healthz"
	case "prefix:/api/v1":
		return "/api/v1/x"
	case "regex:^/products/[0-9]+$":
		return "/products/7"
	case "prefix:/": // catch-all
		return "/whatever"
	default:
		return ""
	}
}

// TestScaleGen spot-checks the generator at full scale (600 gw / 60k VS) WITHOUT
// materializing the whole cluster: it asserts counts, then for a random sample
// of gateways translates the scoped config and checks each route's cluster
// equals the by-construction oracle. It also validates emitted YAML parses.
func TestScaleGen(t *testing.T) {
	g := scalegen.New(scalegen.DefaultConfig())

	gws := g.Gateways()
	if len(gws) != 600+1 { // 600 + broad
		t.Fatalf("Gateways() = %d, want 601", len(gws))
	}

	// Spot-check ~40 gateways spread across the range; a few VS each.
	total, checked := 0, 0
	for i := 0; i < 600; i += 15 {
		gwName := gws[i].Name
		in, ok := g.ScopedFor(gwName)
		if !ok {
			t.Fatalf("ScopedFor(%s) not found", gwName)
		}
		// exactly one Gateway CR in the scoped store (our topology invariant)
		nGW := 0
		for _, c := range in.Configs {
			if c.GroupVersionKind.Kind == "Gateway" {
				nGW++
			}
		}
		if nGW != 1 {
			t.Errorf("%s scoped store has %d Gateways, want 1", gwName, nGW)
		}

		rc := translate.RoutesForScoped(t, in)
		got := map[string]string{}
		for _, vh := range rc.GetVirtualHosts() {
			for _, rt := range vh.GetRoutes() {
				got[vh.GetName()+"|"+matchStr(rt.GetMatch())] = rt.GetRoute().GetCluster()
			}
		}
		// For a few VS of this gateway, verify each rule's cluster vs oracle.
		for _, vh := range rc.GetVirtualHosts() {
			for _, rt := range vh.GetRoutes() {
				host := ""
				if len(vh.GetDomains()) > 0 {
					host = vh.GetDomains()[0]
				}
				path := samplePathFor(matchStr(rt.GetMatch()))
				if path == "" {
					continue
				}
				want := g.ExpectedCluster(host, path)
				gotCluster := rt.GetRoute().GetCluster()
				if want != gotCluster {
					t.Errorf("%s host=%s %s: cluster=%q oracle=%q", gwName, host, matchStr(rt.GetMatch()), gotCluster, want)
				}
				checked++
			}
			break // just the first vhost per gateway to bound runtime
		}
		total++
	}
	t.Logf("spot-checked %d gateways, %d route/cluster assertions", total, checked)
	if checked == 0 {
		t.Fatal("no assertions ran")
	}

	// Cases: has hits and misses.
	cases := g.Cases(1)
	hits, misses := 0, 0
	for _, c := range cases {
		if c.Expected == "" {
			misses++
		} else {
			hits++
		}
	}
	t.Logf("cases: %d total, %d hits, %d misses", len(cases), hits, misses)
	if hits < 60000 {
		t.Errorf("want >=60000 hit cases, got %d", hits)
	}
	if misses == 0 {
		t.Error("want some miss cases")
	}

	// YAML sample validity: emit 3 gateways and parse them back.
	dir := t.TempDir()
	sg := scalegen.New(scalegen.Config{NumGateways: 3, VSPerGW: 2})
	if err := sg.WriteYAML(dir, 3); err != nil {
		t.Fatal(err)
	}
	// Istio CRDs must parse as valid Istio schema. (services.yaml is k8s core
	// v1/Service, not an Istio CRD, so it isn't parsed here — its presence and
	// non-emptiness are checked separately.)
	for _, f := range []string{"gateways.yaml", "virtualservices.yaml"} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		cfgs, _, err := crd.ParseInputs(string(b))
		if err != nil {
			t.Errorf("emitted %s does not parse: %v", f, err)
		}
		if len(cfgs) == 0 {
			t.Errorf("emitted %s parsed to 0 configs", f)
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, "services.yaml")); err != nil || len(b) == 0 {
		t.Errorf("services.yaml missing/empty: %v", err)
	}
}
