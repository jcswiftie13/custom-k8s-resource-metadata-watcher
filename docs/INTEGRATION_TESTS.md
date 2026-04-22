# Integration tests: correctness & API-server burden

This document describes the integration test design that exercises
`metadata-exporter` against a real Kubernetes apiserver (Kind) and validates
two properties of the new reconcile pipeline:

1. **Correctness regressions** for the workqueue + reverse parent index +
   update filter refactor introduced in the optimization plan.
2. **API-server burden** — in particular:
   * The number of watch connections grows with `len(kinds)` only, not with
     namespace count, when running cluster-wide.
   * Per-namespace mode produces exactly `N_ns × N_kinds` watches with no
     hidden cluster-wide fallback.
   * Idle informers do not grow watch connections over time.
   * Burst mutations on Pods / parents collapse through workqueue dedup
     and the update filter before reaching the apiserver.

Memory / goroutine leak coverage is intentionally kept minimal and left to a
follow-up soak test (see *Out of scope* at the end).

## Test pyramid

| Layer | Status | What it covers |
|-------|--------|----------------|
| Unit (`go test ./pkg/...`) | Already in place | Parent index, update filter digests, workqueue dedup, sink mapping |
| Integration (this doc)     | New             | Live informer wiring, apiserver watch count, churn behaviour, fixture correctness |
| Soak / leak (future)       | Not in scope    | 30+ minute RSS/`go_goroutines`/index size tracking |

## Observability sources

The tests read three different metric surfaces:

| Source | Endpoint | What we read |
|--------|----------|--------------|
| kube-apiserver | `kubectl get --raw /metrics` | `apiserver_longrunning_requests{verb="WATCH", resource=...}` — **authoritative watch connection count** |
| exporter `/metrics` | `http://exporter:8080/metrics` | `exporter_reconcile_*`, `exporter_parent_index_*`, `exporter_update_filter_size`, `rest_client_requests_total`, `go_goroutines` |
| exporter logs | `kubectl logs` | Startup topology log line (`watch mode = cluster-wide` / `per-namespace`) for human-readable confirmation |

Because `apiserver_longrunning_requests` also counts watches from the
kubelet, kube-controller-manager, and any other controllers, we use a
**differential measurement**: capture the baseline `before exporter is
running`, then subtract from the steady-state reading after deployment. The
resulting delta is the exporter's contribution.

## Harness architecture

```
test/integration/
  run.sh                       # (slimmed) kind create + docker build + load + kubectl apply
  manifests/                   # kustomize base: ServiceAccount, RBAC, ConfigMap, Deployments, Service
    overlays/
      baseline/                # no exporter (used to capture apiserver baseline)
      cluster-wide/            # watch.namespaces = []
      per-namespace/           # watch.namespaces = [ns-a, ns-b, ns-c]
  e2e/                         # NEW: Go test harness, build tag `integration`
    e2e_test.go                # TestMain: connect kubeconfig, share clientset
    helpers/
      cluster.go               # kubeconfig + clientset + restcfg
      deploy.go                # apply overlay, patch ConfigMap, rollout restart
      metrics.go               # scrape + parse /metrics via expfmt
      wait.go                  # polling helpers with exponential backoff
    topology_test.go           # scenarios 1-3
    burden_test.go             # scenarios 4-5
    correctness_test.go        # scenario 6 (subsumes the bash assertions)
```

Invocation:

```sh
make e2e                       # == run.sh + go test -tags integration ./test/integration/e2e/...
```

## Scenarios

### 1. Cluster-wide watch count is O(kinds)

* **Setup**: 5 namespaces `ns-0..ns-4`, each with a pause Pod. Exporter
  configured with `watch.namespaces: []`.
* **Steps**:
  1. Capture `apiserver_longrunning_requests{verb="WATCH",resource=...}`
     baseline (exporter replicas=0).
  2. Scale exporter to 1; wait until `/healthz` is ready and caches are
     synced (proxy: `exporter_reconcile_total` non-zero for at least one
     rule).
  3. Capture watch counts; compute delta.
* **Assertions**:
  * Delta per kind ∈ `{pods, replicasets, deployments, statefulsets, daemonsets}` is **exactly 1**.
  * Sum of deltas == `len(allKinds) == 5`.
  * Exporter log contains `watch mode = cluster-wide`.
* **Scale knob**: re-run with 10 namespaces; delta must stay at 1 per kind
  (the linearity-in-kinds property we care about).

### 2. Per-namespace watch count is N_ns × N_kinds

* **Setup**: Same 5 namespaces, exporter configured with
  `watch.namespaces: [ns-0, ns-1, ns-2]`.
* **Assertions**:
  * Delta per kind == 3.
  * Total delta == 15.
  * Pods created in `ns-3`/`ns-4` do **not** appear in `/metrics` (indirect
    proof that no stray cluster-wide watch is running).
  * Exporter log: `watch mode = per-namespace`, `factoriesPerKind=3`.

### 3. Idle watches stay stable

* **Setup**: Whichever topology was last deployed (re-use scenario 2).
* **Steps**: Wait 60 seconds with no cluster changes.
* **Assertions**:
  * `exporter_reconcile_queue_depth == 0`.
  * `exporter_reconcile_total` delta over the idle window == 0 (resync
    period is 0, so nothing should be re-enqueued).
  * apiserver WATCH counts unchanged — no reconnect churn.

### 4. Burst updates collapse via workqueue dedup + update filter

* **Setup**: Cluster-wide exporter. Create one `Deployment` with
  `replicas=3` in `ns-0`.
* **Load generator** (runs for 60s):
  * 200× `kubectl patch deployment fixture --type=merge -p '{"metadata":{"labels":{"tick":"<n>"}}}'`
  * In parallel, on each of the 3 Pods: 50× annotation patches
    (`metadata.annotations.tick`).
* **Assertions**:
  * `Δ exporter_reconcile_total{result="ok"}` ≪ `observed event count`.
    Threshold: delta ≤ `25%` of the event count (generous to allow worker
    pool overlap) — tune after first run.
  * `Δ exporter_parent_index_hit_total > 0` (Deployment label patches go
    through the reverse index).
  * `Δ exporter_parent_index_fallback_total == 0` once steady state is
    reached (cold path should not be re-entered).
  * `rest_client_requests_total{verb in {POST,PUT,PATCH,DELETE}}` on the
    exporter side remains `0` — regression guard that the exporter is
    read-only.
  * 30 seconds after churn ends, `exporter_reconcile_queue_depth` returns
    to 0.

### 5. Parent event routes through reverse index

* **Setup**: Reuse scenario 4's Deployment.
* **Steps**: Patch the Deployment's
  `metadata.annotations.integration.test/controller-note` to a new value.
* **Assertions**:
  * Within 10 seconds, the corresponding Prometheus label
    `controller_annotation_integration_test_controller_note` on
    `it_pod_info{...}` reflects the new value for all three Pods.
  * `Δ exporter_parent_index_hit_total ≥ 1`.
  * `Δ exporter_parent_index_fallback_total == 0` during the window.

### 6. Correctness regression (ported from bash)

* Fixture Deployment `fixture-web` with two containers appears.
* `it_pod_info{controller_kind="Deployment",controller_name="fixture-web"}`
  series present.
* `it_pod_container_info{container="pause-main|pause-sidecar"}` each carry
  the expected image tag.
* After `kubectl delete deployment fixture-web`:
  * All `controller_name="fixture-web"` series disappear within 60 s.
  * `exporter_parent_index_size{direction="by_parent"}` drops (Deployment
    UID is removed from the index when its anchors are forgotten).

## Small production changes required

These are strictly additive and gated behind existing knobs:

1. **Register client-go rest metrics** in [cmd/main.go](../cmd/main.go) so
   `rest_client_requests_total` is exposed through the main registry. Needed
   for scenario 4's read-only regression guard.
2. **Expose parent index / update filter size** via two gauges in
   [pkg/collector/metrics.go](../pkg/collector/metrics.go):
   * `exporter_parent_index_size{direction="by_parent"|"by_anchor"}`
   * `exporter_update_filter_size`
   Backed by a new `Len()` method on
   [pkg/collector/index.go](../pkg/collector/index.go) and on
   `updateDigestCache` in [pkg/collector/collector.go](../pkg/collector/collector.go).

## RBAC delta (integration only)

The apiserver `/metrics` endpoint is behind RBAC. Because the test driver
(`go test`) reads `/metrics` via the user's kubeconfig (admin in Kind),
**no extra RBAC for the exporter ServiceAccount is required**. We keep the
existing minimal `Role` untouched. If we later decide to read apiserver
metrics from inside the cluster, we can add a scoped `ClusterRole` with:

```yaml
- nonResourceURLs: ["/metrics"]
  verbs: ["get"]
```

…under `test/integration/manifests/overlays/*` only, never in
`deploy/manifests.yaml`.

## CI integration

`.github/workflows/integration.yaml` will:

1. Checkout + set up Go.
2. `helm/kind-action` to provision Kind.
3. `make e2e` (which internally calls `run.sh` for image build/load and then
   `go test -tags integration -json | gotestsum --junitfile junit.xml`).
4. Upload `junit.xml` as an artifact.

## Out of scope (follow-up PRs)

* 30-minute churn soak test tracking RSS, `go_goroutines`,
  `exporter_parent_index_size`, `exporter_update_filter_size` for
  monotonic growth.
* Reconnect / backoff behaviour (`kubectl cordon` the control plane node
  briefly, verify informers recover without re-listing everything).
* Multi-node Kind to validate behaviour under realistic scheduling churn.
* Audit-log based verification of per-exporter watch count (needs Kind
  `--config` with audit policy).
