# route2a — host+path → service resolver (router_check_tool engine)

Resolve an ingress request (`host` + `path`) to the destination **service**
(Envoy cluster) the way production does: istiod translates the
`Gateway`+`VirtualService`+`Service` config into an Envoy `RouteConfiguration`,
then Envoy's **`router_check_tool`** performs the actual route match. There is no
live Envoy — the tool is the sole matching engine, so this runs offline anywhere
Docker (or the native tool binary) is available.

```
host + path
   │  ① gwresolve : host → gateway            (internal/gwresolve)
   ▼
gateway
   │  ② translate : Gateway+VS+Service → RC   (internal/translate, cached via internal/rccache)
   ▼
RouteConfiguration
   │  ③ router_check_tool : (host,path) → cluster   (internal/matchcheck)
   ▼
cluster  e.g.  outbound|8080||reviews.prod.svc.cluster.local
```

Entry point for everything: **`simulate.Engine.ResolveAll`** in
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

// --- wire once, reuse for the process lifetime ---

// (a) how the tool runs: native router_check_tool binary if present, else docker.
runner, kind, ok := matchcheck.Detect()
if !ok { /* no native binary and no docker+image: cannot resolve */ }
_ = kind // "native" | "docker"

// (b) gateways, for host→gateway matching (Name + its server host patterns).
gateways := []gwresolve.Gateway{
    {Name: "public-gw", Hosts: []string{"*.example.com"}},
    // ...
}

// (c) dependency index + cache. WarmLazy reuses a gateway's RC until its
//     dependencies change; ColdAlways re-translates every time.
deps := rccache.NewDepIndex()
for _, gw := range gateways {
    deps.Own(gw.Name, gw.Name) // a gateway's own CR is a dependency of its RC
}

eng := simulate.New(simulate.Config{
    Resolver:   gwresolve.New(gateways),
    Cache:      rccache.New(rccache.WarmLazy, deps),
    Translator: translate.NewTranslator(),
    ScopedFor:  scopedFor, // YOU supply this: gateway name → its scoped config (see §2)
    Runner:     runner,
})

// --- resolve: pass ONE query or MANY; same call ---
res, metrics, err := eng.ResolveAll(context.Background(), []simulate.Query{
    {Host: "reviews.example.com", Path: "/api/v1/list"},
    {Host: "reviews.example.com", Path: "/healthz"},
})
// res[i].Gateway  — "" if no gateway serves the host
// res[i].Cluster  — destination cluster; "" = matched a gateway but no route (miss)
```

`ScopedFor` is the only thing you must provide from your own config store:

```go
type ScopedSource func(gateway string) (translate.ScopedInput, bool)
```

Given a gateway name it returns that gateway's **scoped** config (its one
`Gateway` CR + the `VirtualService`s bound to it + the `Service`s those VS route
to). In this POC it is backed by
[`scalegen.Gen.ScopedFor`](internal/scalegen/scalegen.go); in production, back it
with your live store (Kubernetes informer, DB, etc.).

> **Batching:** `ResolveAll` groups queries by gateway and invokes
> `router_check_tool` **once per gateway** — one query in ⇒ one invocation; N
> queries across G gateways ⇒ G invocations. See §5 for what this means for perf.

---

## 2. Feeding Gateway + VirtualService + Service (→ RouteConfiguration)

Translation is [`translate.Translator.Translate(in ScopedInput)`](internal/translate/translator.go).
It runs istiod's real config generator and returns the `http.80`
`*route.RouteConfiguration`. Input:

```go
type ScopedInput struct {
    Configs  []config.Config    // the Gateway CR + all its VirtualServices
    Services []*model.Service   // every Service the VS destinations point at
    Proxy    GatewayProxy       // the gateway "vantage" (namespace + selector labels)
}
```

Imports used below:

```go
import (
    networking "istio.io/api/networking/v1alpha3"
    "istio.io/istio/pilot/pkg/model"
    "istio.io/istio/pkg/config"
    "istio.io/istio/pkg/config/host"
    "istio.io/istio/pkg/config/protocol"
    "istio.io/istio/pkg/config/schema/gvk"
)
```

### Gateway (`config.Config`, GVK = Gateway)

```go
config.Config{
    Meta: config.Meta{
        GroupVersionKind: gvk.Gateway,
        Name:             "public-gw",
        Namespace:        "istio-system",
    },
    Spec: &networking.Gateway{
        Selector: map[string]string{"istio": "ingressgateway-public"}, // must equal Proxy.Labels
        Servers: []*networking.Server{{
            Port:  &networking.Port{Number: 80, Name: "http", Protocol: "HTTP"},
            Hosts: []string{"*.example.com"},
        }},
    },
}
```

### VirtualService (`config.Config`, GVK = VirtualService)

```go
config.Config{
    Meta: config.Meta{
        GroupVersionKind: gvk.VirtualService,
        Name:             "reviews-vs",
        Namespace:        "public-gw", // the VS's namespace
    },
    Spec: &networking.VirtualService{
        Hosts:    []string{"reviews.example.com"},
        Gateways: []string{"istio-system/public-gw"}, // <gwNamespace>/<gwName>
        Http: []*networking.HTTPRoute{
            { // first-match wins, top to bottom (exact → prefix → regex → catch-all)
                Match: []*networking.HTTPMatchRequest{{Uri: &networking.StringMatch{
                    MatchType: &networking.StringMatch_Exact{Exact: "/healthz"}}}},
                Route: []*networking.HTTPRouteDestination{{
                    Destination: &networking.Destination{
                        Host: "reviews-health.public-gw.svc.cluster.local",
                        Port: &networking.PortSelector{Number: 8080},
                    }}},
            },
            { // no Match == catch-all
                Route: []*networking.HTTPRouteDestination{{
                    Destination: &networking.Destination{
                        Host: "reviews.public-gw.svc.cluster.local",
                        Port: &networking.PortSelector{Number: 8080},
                    }}},
            },
        },
    },
}
```

`StringMatch` variants: `_Exact`, `_Prefix`, `_Regex`.

### Service (`*model.Service`) — one per destination host

Required so a VS `Destination.Host`+`Port` resolves to a real cluster name
(`outbound|<port>||<fqdn>`). Missing Service ⇒ that route produces no/blackhole
cluster.

```go
&model.Service{
    Hostname:       host.Name("reviews.public-gw.svc.cluster.local"),
    DefaultAddress: "0.0.0.0",
    Ports:          model.PortList{{Name: "http", Port: 8080, Protocol: protocol.HTTP}},
    Attributes:     model.ServiceAttributes{Namespace: "public-gw"},
}
```

### Proxy (`GatewayProxy`) — the gateway vantage

```go
translate.GatewayProxy{
    Name:      "public-gw",
    Namespace: "istio-system",
    Labels:    map[string]string{"istio": "ingressgateway-public"}, // must match the Gateway.Selector
}
```

Assemble these into `ScopedInput` and return from your `ScopedFor(gateway)`. The
resulting cluster name for a hit is `outbound|<port>||<destination-fqdn>`.

---

## 3. What router_check_tool receives (handled for you by `matchcheck`)

You normally never touch this — [`matchcheck.Runner.Resolve`](internal/matchcheck/runner.go)
writes the files, runs the tool, and parses the answer. Documented so you know the
contract.

**Config file** (`rc.json`): the `RouteConfiguration` as protojson
(`protojson.Marshal(rc)`).

**Tests file** (`tests.json`): one entry per query. `method` is **required**
(min 3 chars). `validate.cluster_name` is set to a **sentinel** that can never
match, which forces the tool to print the *real* matched cluster:

```json
{"tests":[
  {"test_name":"0",
   "input":{"authority":"reviews.example.com","path":"/healthz","method":"GET"},
   "validate":{"cluster_name":"__routecheck_unmatched_sentinel__"}}
]}
```

**Invocation:**

```
router_check_tool -c rc.json -t tests.json --details --disable-deprecation-check
```

- `--details` makes it print `actual: [<cluster>]` per case (empty `[]` = miss).
- `--disable-deprecation-check` is required: istiod RCs carry deprecated fields
  (e.g. `RouteAction.max_grpc_timeout`) that newer Envoy otherwise rejects at load.

`Runner.Resolve` parses each case's `actual: [...]` back to a cluster (empty ⇒
`""` miss). This "sentinel + read actual" trick turns the *validator* into a
*resolver*. (`matchcheck.RunScale` uses the same tool in validator mode — you give
a known expected cluster and it reports agreement — used by the oracle test.)

**Native vs docker:** `Detect()` prefers a native `router_check_tool`
(`POC_ROUTERCHECK_BIN`, or on `PATH`); otherwise it runs the prebuilt tools image
(`POC_ROUTERCHECK_IMAGE`, default `envoyproxy/envoy:tools-v1.34-latest`) via
`docker run`. **Native has no per-call container startup**, so use it for any real
latency measurement.

---

## 4. Corpus / oracle (POC only)

[`internal/scalegen`](internal/scalegen) deterministically generates 600
gateways × 100 VS with a by-construction oracle (`Case.Expected`), used to assert
0 mismatches. It also implements `ScopedFor` and `Gateways()` for the benchmarks.
Not needed in production — replace with your store.

---

## 5. Benchmarks — which number is which

| target | test | what it measures |
|---|---|---|
| `make bench-worst` | `TestResolveSingleWorst` | **worst-case single-request latency** — full pipeline per ONE host+path (batch=1, cold). p50/p99 of `total` is the real online cost. Sampled (`POC_WORST_SAMPLE`, default 200). |
| `make bench-warm` | `TestResolveWarm` | **bulk/steady-state throughput** — full corpus, cache warm (translation skipped). The scenario where the `rccache` warm path pays off; also asserts full-corpus 0 mismatches. |
| `make bench-routercheck` | `TestMatchRouterCheckScale` | offline oracle agreement (correctness, not perf). |

> ⚠️ `bench-warm` reports throughput where one `router_check_tool` invocation is
> amortized over a whole gateway's ~100 queries — the *bulk/offline route-simulation*
> number, **not** single-request latency. For online cost read `bench-worst`.

Scale knobs: `POC_GATEWAYS` (default 600), `POC_VS` (default 100); e.g.
`make bench-worst POC_GATEWAYS=100`.

Per-query trace: set `POC_LOG_QUERIES=1` to log every query's input host+path and
the resolved cluster (service), one line each. Off by default (full corpus is 60k+
lines); pair with a small scale, e.g.
`POC_LOG_QUERIES=1 POC_GATEWAYS=10 POC_VS=10 make bench-worst`.

**On macOS these run via docker**, where per-call container startup (~0.2s)
dominates and even the worst-case number is docker-bound. Cite real
throughput/latency only from a **native-binary or Linux** run; correctness
(0 mismatches) holds on either.

---

## 6. Full check

```
make verify        # gen + gwresolve + warm (full-corpus correctness) + worst, writes out/report.md
make bench-worst   # the honest single-request latency
```
