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
- **Scoped informers**: optionally restrict watches to specific namespaces and
  apply per-kind label/field selectors for apiserver-side filtering.

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

Deploy to a cluster:

```sh
kubectl apply -f deploy/manifests.yaml
```

Then scrape `http://<service>:8080/metrics`. The shipped manifests include
`prometheus.io/scrape` annotations and a `Service` so that a Prometheus with
pod-level service discovery will find the exporter automatically.

## Integration tests (Kind)

End-to-end checks run the exporter inside a cluster, apply a sample Deployment
(Pod → ReplicaSet → Deployment owner chain), scrape `/metrics`, then delete the
workload and assert the series disappears.

**Requirements:** Docker, `kind`, `kubectl`, `curl`, and a Bash shell.

**Full flow** (creates a dedicated Kind cluster named `metadata-exporter-it`,
then deletes it on success or failure):

```sh
./test/integration/run.sh
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
| `SKIP_CLUSTER_DELETE` | If set (with default `run.sh` behavior), skip `kind delete cluster` when the script created the cluster |
| `INTEGRATION_IMAGE` | Docker image tag to build and load (default `metadata-exporter:it`) |
| `DOCKER_BUILD_PLATFORM` | e.g. `linux/amd64` for cross-platform image builds |
| `PF_LOCAL_PORT` | Local port for `kubectl port-forward` (default `18080`) |
| `METRICS_WAIT_SEC` / `DELETE_WAIT_SEC` | Timeouts for scraping and delete propagation |

CI runs the same script via [.github/workflows/integration.yaml](.github/workflows/integration.yaml) after [helm/kind-action](https://github.com/helm/kind-action) provisions the cluster.

Manifests live under [test/integration/manifests/](test/integration/manifests/).

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
