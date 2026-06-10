# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A config-driven Kubernetes metadata exporter (Go module `github.com/example/metadata-exporter`). It watches cluster resources via dynamic `SharedInformer` caches, walks `ownerReferences` using only cached listers (zero extra API calls), and exposes per-resource metadata as Prometheus `_info` gauges, kube-state-metrics style. All metrics are declared in YAML config using kubectl-style JSONPath.

## Commands

```bash
go build ./...                                  # build
go test ./... -race -count=1                    # all unit tests
go test -v ./pkg/collector -run TestName        # single unit test
go vet ./...                                    # lint
go test -run NONE -bench BenchmarkCollect -benchmem -benchtime 3x ./pkg/collector  # collector benchmark
./test/integration/run.sh                       # full e2e suite (creates a Kind cluster); also: make e2e
go test -tags integration -v ./test/integration/e2e/... -run TestName              # single integration test (needs cluster)
```

Integration run.sh env knobs: `SKIP_KIND_CREATE`, `SKIP_CLUSTER_DELETE`, `SKIP_GO_TEST`, `GOTEST_FLAGS`, `INTEGRATION_IMAGE`, `INTEGRATION_PRINT_METRICS`, `INTEGRATION_PORT_FORWARD_METRICS`. CI (`.github/workflows/integration.yaml`) runs `make e2e` on a Kind cluster.

Run locally: `./bin/metadata-exporter --config=deploy/config-example.yaml --kubeconfig=$HOME/.kube/config` (metrics on `:8080`, default config path in-cluster is `/etc/metadata-exporter/config.yaml`).

## Architecture

Scrape-time evaluation is the core design: the custom Prometheus collector keeps **no metric state** between scrapes. Every `/metrics` request lists anchors from the informer cache, resolves owner chains, evaluates rules, and emits fresh `ConstMetric`s. Adding state (GaugeVecs, caches of series) breaks this model.

Flow in `cmd/main.go`: flags → `config.Load()` (validates YAML, builds resource registry) → dynamic client (+ optional discovery client for GVR resolution) → custom `Collector` registered with Prometheus → HTTP server (`/metrics`, `/healthz`) + collector goroutine → signal-driven shutdown.

`pkg/collector` pieces and how they interact:

- **`listers.go` (ScopedInformers)** — one `DynamicSharedInformerFactory` per (namespace, resource), with per-kind apiserver-side `labelSelector`/`fieldSelector`. Hands out `cache.GenericLister`s; nothing else touches the API.
- **`resolver.go` (Resolver)** — walks `ownerReferences` through the `ListerGetter` interface only. Produces a `Chain` map keyed by `"anchor"`, Kind names, `"ownerController"`, `"topController"`. Max depth 8.
- **`evaluator.go`** — compiles rules at startup (`CompiledRule`/`CompiledLabel`/`CompiledExpand`: path expressions, label order, expandLabels allow/deny/maxKeys). At scrape time extracts fixed labels via JSONPath and flattens `metadata.labels`/`annotations` into sanitized dynamic label names.
- **`collector.go`** — implements `prometheus.Collector`; `Collect()` ties anchors → resolver → evaluator → `ConstMetric` (value 1). `Start()` launches informers and dry-run LISTs. Self-metrics: `exporter_collect_total{rule,result}`, `exporter_collect_duration_seconds{rule}`, `exporter_anchor_count{rule,kind}`.

`pkg/config` owns the YAML schema, validation, and the resource registry mapping kinds to GVRs (built-ins plus arbitrary GVRs/CRDs declared in `watch.resources[]`). Important invariant: **no implicit watches** — if `watch.resources` is empty the exporter watches nothing, and config validation fails fast when a rule references an unwatched anchor kind.

Config reference: `docs/CONFIG.md`. Example: `deploy/config-example.yaml`. Integration test design: `docs/INTEGRATION_TESTS.md`.

## Tests

Unit tests use the dynamic fake client (`k8s.io/client-go/dynamic/fake`) and live next to the code in `pkg/`. Integration tests (`test/integration/e2e/`, build tag `integration`) run against a real Kind cluster and assert on scraped `/metrics` output, watch-connection deltas (`apiserver_longrunning_requests`), and namespace/cluster scoping; fixtures live in `test/integration/manifests/`.

## Deployment

`deploy/manifests.yaml` for raw kubectl; `charts/metadata-exporter/` is the Helm chart (RBAC, Deployment with Recreate strategy, ConfigMap mounted at `/etc/metadata-exporter/config.yaml`, Service for `/metrics`).
