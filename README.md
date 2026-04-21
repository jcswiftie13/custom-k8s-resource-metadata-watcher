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
Dockerfile                 Multi-stage build ending in distroless/nonroot
```
