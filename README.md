# metadata-exporter

A lightweight, config-driven Kubernetes metadata exporter. It watches cluster
resources through `SharedInformer` caches (no extra API calls), walks
`ownerReferences` via the cache, and publishes per-series labels as Prometheus
`_info` gauges — in the same style as `kube-state-metrics`.

- **Config-driven**: declare every Prometheus metric in YAML using standard
  kubectl-style JSONPath.
- **Zero extra API calls**: owner-chain resolution uses only cached listers.
- **Pluggable sink**: `MetadataSink` interface, with a Prometheus implementation;
  future backends (Kafka, PostgreSQL…) implement the same interface without
  touching the collector core.
- **Scoped informers**: optionally restrict watches to specific namespaces, pick
  which resource kinds to watch via `watch.kinds`, and apply per-kind
  `labelSelector` / `fieldSelector` for apiserver-side filtering.

See [docs/CONFIG.md](docs/CONFIG.md) for the full configuration reference.

## Build

```sh
# local binary
go build -o bin/metadata-exporter ./cmd

# container image
docker build -t ghcr.io/example/metadata-exporter:latest .
```

## Run

Local (against the current kubeconfig context):

```sh
./bin/metadata-exporter --config=deploy/config-example.yaml --kubeconfig=$HOME/.kube/config
```

### Runtime flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--config` | `/etc/metadata-exporter/config.yaml` | Path to the YAML config file. |
| `--metrics-addr` | `:8080` | Address the `/metrics` and `/healthz` endpoints listen on. |
| `--kubeconfig` | *(empty)* | Explicit kubeconfig path; empty falls back to in-cluster. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, `error`. |
| `--kube-api-qps` | `20` | Maximum QPS of the kubernetes client against the apiserver. |
| `--kube-api-burst` | `40` | Maximum burst of the kubernetes client against the apiserver. |
| `--reconcile-workers` | `4` | Number of goroutines draining the reconcile workqueue. |

Tune `--kube-api-qps` / `--kube-api-burst` downward on large clusters to cap
exporter pressure on the apiserver, and upward when tens of namespaces are
watched in parallel. The exporter also exposes its own self-metrics to help
you right-size the workqueue:

- `exporter_reconcile_queue_depth` — current queue depth gauge.
- `exporter_reconcile_total{rule,result}` — reconcile attempts per rule.
- `exporter_reconcile_duration_seconds` — reconcile latency histogram per anchor kind.
- `exporter_parent_index_hit_total` / `exporter_parent_index_fallback_total` — parent-event routing stats.
- `exporter_parent_index_size{direction}` — reverse-index map sizes (by_parent / by_anchor), useful for leak detection.
- `exporter_update_filter_size` — cached metadata-digest count used by the update-event filter.
- `rest_client_requests_total{code,method,host}` + `rest_client_request_duration_seconds{verb,host}` — standard client-go metrics, handy for spotting apiserver pressure.

Deploy to a cluster:

```sh
kubectl apply -f deploy/manifests.yaml
```

Then scrape `http://<service>:8080/metrics`. The shipped manifests include
`prometheus.io/scrape` annotations and a `Service` so that a Prometheus with
pod-level service discovery will find the exporter automatically.

## Integration tests (Kind)

End-to-end checks run the exporter inside a Kind cluster and drive a set of
Go-based scenarios that cover correctness, API-server watch topology, and
burst-churn behaviour. See [docs/INTEGRATION_TESTS.md](docs/INTEGRATION_TESTS.md)
for the full scenario list.

**Requirements:** Docker, `kind`, `kubectl`, Go (matching `go.mod`), and a
Bash shell.

**Full flow** (creates a dedicated Kind cluster named `metadata-exporter-it`,
applies the kustomize base, then runs `go test -tags integration` — the
cluster is deleted on success or failure):

```sh
make e2e
```

**Use an existing cluster** (current kubeconfig context; the script will not
create or delete the cluster):

```sh
export SKIP_KIND_CREATE=1
# Optional if your cluster is not the default kind name:
# export KIND_CLUSTER_NAME=kind
./test/integration/run.sh
```

**Optional environment variables:**

| Variable | Purpose |
|----------|---------|
| `SKIP_CLUSTER_DELETE` | Keep the Kind cluster after the run (only when `run.sh` created it) |
| `SKIP_GO_TEST` | Apply manifests and exit; useful for interactive debugging |
| `INTEGRATION_IMAGE` | Docker image tag to build and load (default `metadata-exporter:it`) |
| `DOCKER_BUILD_PLATFORM` | e.g. `linux/amd64` for cross-platform image builds |
| `GOTEST_FLAGS` | Extra flags forwarded to `go test` (e.g. `-run TestTopology_`) |

CI runs the same script via [.github/workflows/integration.yaml](.github/workflows/integration.yaml) after [helm/kind-action](https://github.com/helm/kind-action) provisions the cluster.

Manifests live under [test/integration/manifests/](test/integration/manifests/)
and the Go test harness under [test/integration/e2e/](test/integration/e2e/).

## Project layout

```
cmd/
  main.go                  Entry point
pkg/
  config/                  YAML schema + validation
  sink/                    MetadataSink interface + Prometheus impl
  collector/               Informer wiring, resolver, evaluator, handlers
deploy/
  manifests.yaml           SA, ClusterRole+Binding, ConfigMap, Deployment, Service
docs/
  CONFIG.md                Configuration reference
test/
  integration/             Kind e2e script + kustomize manifests
Dockerfile                 Multi-stage build ending in distroless/nonroot
```
