# route2a — Go API 參考

Resolve an ingress request (`host` + `path`) to the destination **service** (Envoy cluster):
istiod translates `Gateway`+`VirtualService`+`Service` → `RouteConfiguration`, then Envoy's
**`router_check_tool`** performs the route match. No live Envoy — offline anywhere Docker or a
native binary is available.

**文件導覽**

- 怎麼跑、測試矩陣、環境變數 → [../README.md](../README.md)
- POC 架構、ClickHouse 模型、benchmark 階段 → [../ARCHITECTURE.md](../ARCHITECTURE.md)
- 完整系統設計 → [../../docs/istio-virtualservice-routing-history-design.md](../../docs/istio-virtualservice-routing-history-design.md)

```
host + path [+ IP → 3-hop]
   │  gwresolve / lookup + ResolveAmong
   ▼
gateway
   │  ScopedFor → translate (rccache)
   ▼
RouteConfiguration
   │  router_check_tool
   ▼
cluster  e.g.  outbound|8080||reviews.prod.svc.cluster.local
```

Entry point: **`simulate.Engine.ResolveAll`** in
[internal/simulate/simulate.go](internal/simulate/simulate.go).

---

## 1. Resolve one or many host+path

```go
import (
    "context"

    "github.com/example/metadata-exporter/poc/route2a/internal/gwresolve"
    "github.com/example/metadata-exporter/poc/route2a/internal/matchcheck"
    "github.com/example/metadata-exporter/poc/route2a/internal/rccache"
    "github.com/example/metadata-exporter/poc/route2a/internal/simulate"
    "github.com/example/metadata-exporter/poc/route2a/internal/translate"
)

runner, kind, ok := matchcheck.Detect()
if !ok { /* set POC_ROUTERCHECK_BIN or docker+image */ }
_ = kind

gateways := []gwresolve.Gateway{
    {Name: "public-gw", Hosts: []string{"*.example.com"}},
}

deps := rccache.NewDepIndex()
for _, gw := range gateways {
    deps.Own(gw.Name, gw.Name)
}

eng := simulate.New(simulate.Config{
    Resolver:   gwresolve.New(gateways),
    Cache:      rccache.New(rccache.WarmLazy, deps),
    Translator: translate.NewTranslator(),
    ScopedFor:  scopedFor, // gateway name → ScopedInput (see §2)
    Runner:     runner,
})

res, metrics, err := eng.ResolveAll(context.Background(), []simulate.Query{
    {Host: "reviews.example.com", Path: "/api/v1/list"},
})
// res[i].Gateway — "" if no gateway; res[i].Cluster — "" on route miss
```

`ScopedFor` 由你的設定 store 提供；POC 用 [`scalegen.Gen.ScopedFor`](internal/scalegen/scalegen.go)，
benchmark 用 ClickHouse `chstore.ScopedFor`。

`ResolveAll` groups by gateway → **one `router_check_tool` invocation per gateway**.

---

## 2. Feeding Gateway + VirtualService + Service

[`translate.Translator.Translate`](internal/translate/translator.go) returns `http.80` RouteConfiguration.

```go
type ScopedInput struct {
    Configs  []config.Config    // Gateway CR + VirtualServices
    Services []*model.Service   // destination Services
    Proxy    GatewayProxy       // namespace + selector labels
}
```

### Gateway

```go
config.Config{
    Meta: config.Meta{GroupVersionKind: gvk.Gateway, Name: "public-gw", Namespace: "istio-system"},
    Spec: &networking.Gateway{
        Selector: map[string]string{"istio": "ingressgateway-public"},
        Servers: []*networking.Server{{
            Port:  &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"},
            Hosts: []string{"*.example.com"},
        }},
    },
}
```

### VirtualService

```go
config.Config{
    Meta: config.Meta{GroupVersionKind: gvk.VirtualService, Name: "reviews-vs", Namespace: "public-gw"},
    Spec: &networking.VirtualService{
        Hosts:    []string{"reviews.example.com"},
        Gateways: []string{"istio-system/public-gw"},
        Http: []*networking.HTTPRoute{
            {Match: []*networking.HTTPMatchRequest{{Uri: &networking.StringMatch{
                MatchType: &networking.StringMatch_Exact{Exact: "/healthz"}}}},
             Route: []*networking.HTTPRouteDestination{{Destination: &networking.Destination{
                Host: "reviews-health.public-gw.svc.cluster.local",
                Port: &networking.PortSelector{Number: 8080}}}}},
            },
            {Route: []*networking.HTTPRouteDestination{{Destination: &networking.Destination{
                Host: "reviews.public-gw.svc.cluster.local",
                Port: &networking.PortSelector{Number: 8080}}}}},
        },
    },
}
```

### Service + Proxy

```go
&model.Service{
    Hostname: host.Name("reviews.public-gw.svc.cluster.local"),
    Ports:    model.PortList{{Name: "http", Port: 8080, Protocol: protocol.HTTP}},
    Attributes: model.ServiceAttributes{Namespace: "public-gw"},
}

translate.GatewayProxy{
    Name: "public-gw", Namespace: "istio-system",
    Labels: map[string]string{"istio": "ingressgateway-public"},
}
```

Hit cluster: `outbound|<port>||<destination-fqdn>`.

---

## 3. router_check_tool contract (`matchcheck`)

[`matchcheck.Runner.Resolve`](internal/matchcheck/runner.go) writes `rc.json` + `tests.json`, runs:

```
router_check_tool -c rc.json -t tests.json --details --disable-deprecation-check
```

Sentinel trick — expected cluster set to `__routecheck_unmatched_sentinel__`, parse `actual: [...]`:

```json
{"tests":[{"test_name":"0",
  "input":{"authority":"reviews.example.com","path":"/healthz","method":"GET"},
  "validate":{"cluster_name":"__routecheck_unmatched_sentinel__"}}]}
```

`Detect()`: native `POC_ROUTERCHECK_BIN` / PATH first, else docker
(`POC_ROUTERCHECK_IMAGE`, default `envoyproxy/envoy:tools-v1.34-latest`).

---

## 4. Corpus / oracle (POC only)

[`internal/scalegen`](internal/scalegen): 600 gateways × 100 VS, by-construction `Case.Expected`.
Also provides `ScopedFor`, `Gateways()`, `IPForHost` for benchmarks.

---

## 5. ClickHouse traffic_simulation

Wire `simulate.Config.Lookup` + CH `ScopedFor` for the full chain (see
[`bench_test.go`](bench_test.go)). Quick check:

```bash
make ch-up
go test -run TestIPFlowClickHouse -v .
go run ./cmd/ipflow -mode=load|query|verify
```

Benchmarks, env vars, Dev Container → [../README.md](../README.md).
