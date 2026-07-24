package route2a

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// TestMatchRouterCheckScale is matcher A: an OFFLINE cross-validation of the
// by-construction oracle using the prebuilt router_check_tool over a sample of
// hit cases across all gateways. Combined with matcher B (live Envoy, checked in
// TestResolveWarm) this gives three-way agreement Expected == B == A.
func TestMatchRouterCheckScale(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker not available")
	}
	image := envOr("POC_ROUTERCHECK_IMAGE", matchcheck.DefaultImage)
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		t.Skipf("router_check tools image %s not present (docker pull %s)", image, image)
	}

	g := scalegen.New(benchScale())
	all := g.Cases(1)
	sample := envInt("POC_A_SAMPLE", 2000)

	// take hit cases up to the sample size
	cases := make([]matchcheck.ScaleCase, 0, sample)
	for _, c := range all {
		if c.Expected == "" || c.Gateway == "" {
			continue
		}
		cases = append(cases, matchcheck.ScaleCase{Gateway: c.Gateway, Host: c.Host, Path: c.Path, Expected: c.Expected})
		if len(cases) >= sample {
			break
		}
	}

	// memoized RAW (un-echoed) RC per gateway
	rcCache := map[string]*route.RouteConfiguration{}
	rcFor := func(gw string) *route.RouteConfiguration {
		if rc, ok := rcCache[gw]; ok {
			return rc
		}
		in, ok := g.ScopedFor(gw)
		if !ok {
			return nil
		}
		rc := translate.RoutesForScoped(t, in)
		rcCache[gw] = rc
		return rc
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	agree, disagree, elapsed, err := matchcheck.RunScale(ctx, image, rcFor, cases, 8)
	if err != nil {
		t.Fatalf("RunScale: %v", err)
	}
	t.Logf("matcher A (router_check_tool): %d cases, agree=%d disagree=%d, slowest gw invocation=%s",
		len(cases), agree, disagree, elapsed.Round(time.Millisecond))
	if disagree != 0 {
		t.Fatalf("matcher A disagreed on %d cases (Expected==A must hold)", disagree)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
