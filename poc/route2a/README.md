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

> Both `bench-worst` and `bench-warm` now run the **whole ClickHouse chain**
> (IP 3-hop → `gwresolve` → translate-from-ClickHouse → `router_check_tool`), so
> they need a running store — start it with `make ch-up` first (they auto-load the
> corpus on first run, untimed, and skip the load when it is already present). The
> report gains two ClickHouse stages: **`lookup`** (the per-query IP→candidates
> 3-hop) and **`scopedfetch`** (the per-gateway `ScopedFor` config fetch), so
> `sum(stages) ≈ total`. When ClickHouse is unreachable the benchmarks skip.

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

---

## 7. Ingress `IP → Gateway` over ClickHouse (traffic simulation)

The sections above resolve `host + path`. Real north-south traffic also carries a
**destination IP** (from a DNS lookup / OTEL span). This front half stores the
ingress resources in **ClickHouse** as bitemporal versions and resolves
`IP → Gateway` with a 3-hop selector join, then hands off to the existing
`gwresolve → translate → router_check_tool` pipeline — with `translate` now reading
its config (Gateway + VirtualServices + Services) back **from ClickHouse**. This is
the same chain the benchmarks (§5) exercise.

```
make ch-up      # start a ClickHouse container (waits until ready)
make bench-worst # (or bench-warm) run the full chain; auto-loads the corpus once
make ch-down    # stop the container
```

Scale/version knobs (shrink for dev): `POC_GATEWAYS POC_VS` and the bitemporal
depth `POC_VER_DEPLOY POC_VER_SVC POC_VER_GW POC_VER_VS POC_VER_KSVC` (the
benchmarks default each version count to **1** for a fast load; `cmd/ipflow`
defaults to the multi-version corpus). e.g. `make bench-worst POC_GATEWAYS=20 POC_VS=5`.

The 3-hop is: `has(ingress_ips, ip)` (Service) → `hasAll(pod_labels ⊇ svc.selector)`
(Deployment L) → `hasAll(L ⊇ gw.selector)` (Gateway), each filtered by
`valid_from <= T < valid_to`. Design write-up + data model:
[`docs/istio-virtualservice-routing-history-design.md`](../../docs/istio-virtualservice-routing-history-design.md)
("Ingress `IP→Gateway` 流程的 POC").

For a quick correctness check (3-hop vs oracle + multi-version AsOf, no benchmark):
`go test -run TestIPFlowClickHouse .` (needs `make ch-up`). For the manual CLI —
`go run ./cmd/ipflow -mode=load|query|verify` — see its package doc.
(skips if ClickHouse is unreachable).

---

## 8. Dev Container（Mac 上跑 native `router_check_tool` + ClickHouse）

On macOS the `router_check_tool` binary extracted from the Envoy tools image is a
**Linux ELF** — it cannot run natively on the host. Use the repo's Dev Container
to develop and benchmark inside Linux with a native binary and a pre-wired
ClickHouse service.

**Open:** Cursor / VS Code → *Dev Containers: Reopen in Container* (uses
[`.devcontainer/`](../../.devcontainer/)).

What it sets up automatically:

| Item | Value |
|------|-------|
| Go toolchain | `mcr.microsoft.com/devcontainers/go:1.25-bookworm` |
| ClickHouse | compose service `clickhouse` (no `make ch-up` needed) |
| `POC_CH_ADDR` | `clickhouse:9000` |
| `router_check_tool` | `post-create` runs `make routercheck-bin` → native mode |
| Docker CLI | `docker-outside-of-docker` (for `TestMatchRouterCheckScale` + fallback) |

**Quick checks inside the container:**

```bash
cd poc/route2a
go test -run TestMatchRouterCheckScale -v .    # translate + tool oracle (no CH)
go test -run TestIPFlowClickHouse -v .         # full IP→CH→cluster chain
make bench-worst POC_GATEWAYS=20 POC_VS=5      # meaningful latency (native binary)
```

**Mac host note:** if you previously ran `make routercheck-bin` on macOS, the
Linux ELF under `bin/router_check_tool` is ignored on the host (Makefile only
exports `POC_ROUTERCHECK_BIN` when the binary actually runs). Inside the Dev
Container the same file is used as the native binary.

Outside the Dev Container on macOS, correctness tests still work via **docker
fallback**; do not cite `bench-warm` / `bench-worst` latency numbers from macOS.
