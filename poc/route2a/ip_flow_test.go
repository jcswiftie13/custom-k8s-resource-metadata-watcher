package route2a

import (
	"context"
	"testing"
	"time"

	"github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
	"github.com/example/metadata-exporter/poc/route2a/internal/ingload"
	"github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
	"github.com/example/metadata-exporter/poc/route2a/internal/memwindow"
	"github.com/example/metadata-exporter/poc/route2a/internal/rangequery"
	"github.com/example/metadata-exporter/poc/route2a/internal/rccache"
	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
	"github.com/example/metadata-exporter/poc/route2a/internal/simulate"
	"github.com/example/metadata-exporter/poc/route2a/internal/store"
	"github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

// TestIPFlowClickHouse is the end-to-end ingress-traffic check: it loads a small
// program-generated multi-version corpus into ClickHouse, then verifies (1) the
// 3-hop IP->Gateway against the by-construction oracle, (2) multi-version AsOf(T)
// version selection, and (3) — when router_check_tool is available — the full
// IP -> gateway -> translate(from ClickHouse) -> cluster pipeline against the
// oracle. Skips cleanly when ClickHouse is unreachable.
func TestIPFlowClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	st := requireStore(ctx, t)
	defer st.Close()

	// Small, fast corpus (still exercises multi-version depth).
	gen := scalegen.New(scalegen.Config{NumGateways: 20, VSPerGW: 5})
	vers := ingload.Versions{Deploy: 2, Svc: 2, Gw: 2, VS: 3, KSvc: 3}
	if err := ingload.Load(ctx, st, gen, vers, nil); err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	now := time.Now().UTC()

	// (1) 3-hop IP->Gateway == oracle, and unique (single candidate).
	for i := 0; i < gen.NumGateways(); i++ {
		ip := scalegen.IPForGateway(i)
		cands, err := st.ResolveIPToGateways(ctx, ip, now)
		if err != nil {
			t.Fatalf("3-hop ip=%s: %v", ip, err)
		}
		want := scalegen.GatewayName(i)
		if len(cands) != 1 || cands[0].Name != want {
			t.Fatalf("3-hop ip=%s: got %v, want single [%s]", ip, candNames(cands), want)
		}
	}

	// (2) Multi-version AsOf: version live at VersionMidTime(v) has rev==v.
	gw0 := scalegen.GatewayName(0)
	for v := 0; v < vers.Gw; v++ {
		rev, ok, err := st.AsOfRev(ctx, "gw_versions", scalegen.GatewayNamespace(), gw0, store.VersionMidTime(v))
		if err != nil {
			t.Fatalf("AsOfRev v=%d: %v", v, err)
		}
		if !ok || int(rev) != v {
			t.Fatalf("AsOfRev gw_versions %s v=%d: got rev=%d ok=%v, want rev=%d", gw0, v, rev, ok, v)
		}
	}
	// And at now the open (last) version is selected.
	if rev, ok, _ := st.AsOfRev(ctx, "gw_versions", scalegen.GatewayNamespace(), gw0, now); !ok || int(rev) != vers.Gw-1 {
		t.Fatalf("AsOfRev now: got rev=%d ok=%v, want rev=%d (open)", rev, ok, vers.Gw-1)
	}

	// (3) Full pipeline (needs router_check_tool). Reuse the scalegen oracle cases.
	runner, kind, ok := matchcheck.Detect()
	if !ok {
		t.Log("router_check_tool unavailable (no native binary / tools image); skipping cluster stage")
		return
	}
	t.Logf("router_check_tool: %s", kind)

	cases := hitCases(gen.Cases(1), 30)
	for _, c := range cases {
		ip, ok := gen.IPForHost(c.Host)
		if !ok {
			t.Fatalf("IPForHost(%s) failed", c.Host)
		}
		cands, err := st.ResolveIPToGateways(ctx, ip, now)
		if err != nil {
			t.Fatalf("3-hop %s: %v", ip, err)
		}
		resolver := gwresolve.New(candGateways(cands))
		eng := simulate.New(simulate.Config{
			Resolver:   resolver,
			Cache:      rccache.New(rccache.ColdAlways, rccache.NewDepIndex()),
			Translator: translate.NewTranslator(),
			ScopedFor: func(gw string) (translate.ScopedInput, bool) {
				in, ok, err := st.ScopedFor(ctx, gw, now)
				if err != nil {
					t.Errorf("ScopedFor %s: %v", gw, err)
					return translate.ScopedInput{}, false
				}
				return in, ok
			},
			Runner: runner,
		})
		res, _, err := eng.ResolveAll(ctx, []simulate.Query{{Host: c.Host, Path: c.Path}})
		if err != nil {
			t.Fatalf("ResolveAll %s%s: %v", c.Host, c.Path, err)
		}
		if res[0].Gateway != c.Gateway {
			t.Errorf("host=%s: gateway=%s want %s", c.Host, res[0].Gateway, c.Gateway)
		}
		if res[0].Cluster != c.Expected {
			t.Errorf("host=%s path=%s: cluster=%s want %s", c.Host, c.Path, res[0].Cluster, c.Expected)
		}
	}

	// (4) Range query: over a window spanning >=2 version boundaries, the traffic
	// window slices into >=2 segments; every segment must resolve to the oracle
	// gateway+cluster. The corpus writes an identical spec to every version, so the
	// destination is constant and adjacent segments merge into one covering span.
	t0 := store.VersionMidTime(0)
	t1 := store.VersionMidTime(vers.VS - 1)
	deps := rangequery.Deps{Translator: translate.NewTranslator(), Runner: runner}
	for _, c := range cases[:min(5, len(cases))] {
		ip, _ := gen.IPForHost(c.Host)

		// The loaded window must slice into >=2 segments (crosses version boundaries).
		w, err := st.LoadTrafficWindow(ctx, ip, t0, t1)
		if err != nil {
			t.Fatalf("LoadTrafficWindow %s: %v", ip, err)
		}
		if segs := memwindow.New(w, t0, t1).Segments(); len(segs) < 2 {
			t.Fatalf("host=%s: want >=2 segments over [%s,%s), got %d",
				c.Host, t0.Format(time.RFC3339), t1.Format(time.RFC3339), len(segs))
		}

		versions, err := deps.Resolve(ctx, st, c.Host, c.Path, ip, t0, t1)
		if err != nil {
			t.Fatalf("range resolve %s: %v", c.Host, err)
		}
		if len(versions) == 0 {
			t.Fatalf("host=%s: empty range result", c.Host)
		}
		// Spans cover [t0,t1) contiguously.
		if !versions[0].From.Equal(t0) || !versions[len(versions)-1].To.Equal(t1) {
			t.Errorf("host=%s: range spans do not cover [%s,%s): first=%s last=%s",
				c.Host, t0.Format(time.RFC3339), t1.Format(time.RFC3339),
				versions[0].From.Format(time.RFC3339), versions[len(versions)-1].To.Format(time.RFC3339))
		}
		// Every span resolves to the oracle gateway+cluster (matches the point query).
		for _, v := range versions {
			if v.Gateway != c.Gateway {
				t.Errorf("host=%s span=[%s,%s): gateway=%s want %s",
					c.Host, v.From.Format(time.RFC3339), v.To.Format(time.RFC3339), v.Gateway, c.Gateway)
			}
			if v.Cluster != c.Expected {
				t.Errorf("host=%s path=%s span=[%s,%s): cluster=%s want %s",
					c.Host, c.Path, v.From.Format(time.RFC3339), v.To.Format(time.RFC3339), v.Cluster, c.Expected)
			}
		}
	}
}

func candNames(cands []store.GatewayCand) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.Name
	}
	return out
}

func candGateways(cands []store.GatewayCand) []gwresolve.Gateway {
	out := make([]gwresolve.Gateway, len(cands))
	for i, c := range cands {
		out[i] = gwresolve.Gateway{Name: c.Name, Hosts: c.ServerHosts}
	}
	return out
}

// hitCases takes up to n resolvable (gateway + cluster) cases.
func hitCases(all []scalegen.Case, n int) []scalegen.Case {
	out := make([]scalegen.Case, 0, n)
	for _, c := range all {
		if c.Gateway == "" || c.Expected == "" {
			continue
		}
		out = append(out, c)
		if len(out) >= n {
			break
		}
	}
	return out
}
