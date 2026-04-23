## Integration Tests (Kind)

The integration suite runs `metadata-exporter` against a real Kubernetes
apiserver provisioned by Kind, exercises it with live workloads, and asserts
both **functional correctness** and **API-server burden** properties of the
reconcile pipeline.

The full design rationale lives in
[`docs/INTEGRATION_TESTS.md`](../../docs/INTEGRATION_TESTS.md); this README
focuses on what the suite actually does today, how to run it, and what its
current limitations are.

---

### How to run

Prerequisites on the host: `kind`, `kubectl`, `docker`, `go`.

```sh
make e2e
```

That target wraps [`run.sh`](./run.sh), which:

1. Creates a Kind cluster (skipped when `SKIP_KIND_CREATE=1` is set).
2. Builds the `metadata-exporter:it` image and `kind load`s it into the node.
3. Applies [`manifests/`](./manifests) (kustomize base): namespace,
   ServiceAccount, ClusterRole/Binding, ConfigMap, Service, Deployment.
4. Waits for the exporter Deployment to roll out and a Pod to become Ready.
5. Hands off to `go test -tags integration -v -count=1 ./test/integration/e2e/...`,
   which owns **all** assertions.

| Env var | Default | Purpose |
|---|---|---|
| `KIND_CLUSTER_NAME` | `metadata-exporter-it` | Kind cluster name. |
| `INTEGRATION_IMAGE` | `metadata-exporter:it` | Image tag built and loaded. |
| `SKIP_KIND_CREATE` | unset | Use the current `kubectl` context; do **not** create or delete a cluster. |
| `SKIP_CLUSTER_DELETE` | unset | Keep the cluster after the run for debugging. Only honoured when `run.sh` created it. |
| `SKIP_GO_TEST` | unset | Apply manifests and exit; useful for manual probing. |
| `DOCKER_BUILD_PLATFORM` | unset | Forwarded as `--platform` to `docker build` (e.g. `linux/amd64` on Apple silicon when the cluster needs amd64). |
| `GOTEST_FLAGS` | empty | Extra flags appended to `go test` (e.g. `-run=TestTopology_*`). |

Runner sets `E2E_REPO_ROOT` for the Go suite. The Go tests pick up
`KUBECONFIG` from the environment, falling back to `$HOME/.kube/config` and
finally to in-cluster config.

---

### What the suite validates

All scenarios live in [`e2e/`](./e2e) and are gated behind the `integration`
build tag. Each test owns its own namespaces, fixtures, and exporter config
(by patching the ConfigMap and rolling the Deployment), so they can run in
sequence against a single Kind cluster without polluting each other.

| Test | File | Property under test |
|---|---|---|
| `TestTopology_ClusterWide` | `topology_test.go` | With `watch.namespaces: []`, the apiserver gains exactly **one** WATCH per kind in `{pods, replicasets, deployments, statefulsets, daemonsets}`, regardless of how many namespaces exist. Startup log line contains `watch mode = cluster-wide`. |
| `TestTopology_PerNamespace` | `topology_test.go` | With `watch.namespaces: [a,b,c]`, watches scale as `N_ns × N_kinds` (15 for 3 namespaces × 5 kinds). Pods in non-watched namespaces never appear in `it_pod_info` (proves no stray cluster-wide watch). Startup log line contains `watch mode = per-namespace`. |
| `TestTopology_IdleStable` | `topology_test.go` | Over a 60 s idle window with no cluster changes, `exporter_reconcile_total` does **not** advance, `exporter_reconcile_queue_depth` is `0`, and per-kind WATCH counts on the apiserver are unchanged. |
| `TestBurden_BurstDedup` | `burden_test.go` | A burst of 200 Deployment patches + 50 patches per Pod (×3 Pods) collapses through the workqueue / parent index. `maxRuleDelta(exporter_reconcile_total)` stays under 40% of the theoretical per-rule enqueue ceiling (`replicas × (depPatches + podPatches)`). Parent index registers hits but no fallback. `rest_client_requests_total{method=POST/PUT/PATCH/DELETE}` does **not** advance — the exporter must be read-only. |
| `TestBurden_ParentEventViaIndex` | `burden_test.go` | Patching the Deployment's `controller-note` annotation propagates to all owned Pod series within 15 s; resolution goes through the **reverse parent index** (`exporter_parent_index_hit_total` increases) and never falls back (`exporter_parent_index_fallback_total` stays flat). |
| `TestCorrectness_FixtureFlow` | `correctness_test.go` | Functional smoke (port of the original bash check): `it_pod_info` carries `controller_kind="Deployment"`, `controller_name`, namespace, and the flattened `controller_annotation_integration_test_controller_note` label; `it_pod_container_info` produces one series per `(pod, container)` with the expected `image` (`pause-main`/`pause:3.9`, `pause-sidecar`/`pause:3.10`); after `kubectl delete deployment …` the series disappear within 90 s and `exporter_parent_index_size{direction="by_parent"}` does not grow. |

#### Default rules under test

The base ConfigMap [`manifests/configmap.yaml`](./manifests/configmap.yaml)
defines two rules, exercised across every test:

* `it_pod_info` — one series per Pod with `namespace`, `pod`, `phase`,
  `controller_kind` (`top.kind`), `controller_name`
  (`top.metadata.name`), and a `flatten` of
  `top.metadata.annotations.integration.test/controller-note` into the
  Prometheus label `controller_annotation_integration_test_controller_note`.
* `it_pod_container_info` — uses `forEach: spec.containers[*]` to emit one
  series per `(pod, container)` carrying `container` (`item.name`) and
  `image` (`item.image`).

#### Observability surfaces consumed

| Source | How it is read | What is asserted |
|---|---|---|
| kube-apiserver `/metrics` | `kubectl get --raw /metrics` | `apiserver_longrunning_requests{verb="WATCH",resource=…}` — the **authoritative** WATCH connection count. Tests use a *differential* (baseline with exporter scaled to 0, then re-scale to 1) to isolate the exporter's contribution. |
| Exporter `/metrics` | API-server pod proxy via `pods/http:<pod>:8080/proxy/metrics` (numeric port + `http:` scheme prefix; using the named port crashes the apiserver pod-proxy with HTTP/2 `INTERNAL_ERROR`) | `exporter_reconcile_total`, `exporter_reconcile_queue_depth`, `exporter_parent_index_size{direction=…}`, `exporter_parent_index_hit_total`, `exporter_parent_index_fallback_total`, `exporter_update_filter_size`, `rest_client_requests_total`, plus the user-defined `it_pod_info` / `it_pod_container_info` series. |
| Exporter logs | `kubectl logs` | Startup line `"watch mode = cluster-wide"` / `"per-namespace"` — human-readable confirmation of the configured topology. |

---

### Current limitations / what is **NOT** covered

* **Soak / long-run stability**. The suite runs in ~3 minutes; it does not
  track RSS, `go_goroutines`, parent-index size, or update-filter size for
  monotonic growth over hours. A separate soak test is listed as future
  work in `docs/INTEGRATION_TESTS.md`.
* **Memory-leak detection**. We expose `exporter_parent_index_size` and
  `exporter_update_filter_size` and check that they don't grow after a
  delete in `TestCorrectness_FixtureFlow`, but there is no
  pprof / heap-profile assertion.
* **Reconnect / backoff behaviour**. The tests do not cordon the control
  plane or sever the apiserver connection to validate informer recovery.
* **Multi-node scheduling churn**. Kind is single-node by default; we do
  not run with multiple worker nodes.
* **Per-exporter WATCH attribution**. `apiserver_longrunning_requests` does
  not carry `userAgent`/SA labels, so we rely on the *differential*
  measurement. Any other controller spinning up watches concurrently with
  scenario 1/2 would skew the delta. In Kind the only sources of WATCH
  are the kube-apiserver components themselves (kubelet, kube-controller-manager,
  kube-scheduler), which are stable across the test window, so this
  is acceptable in practice but would not survive being moved to a busy
  shared cluster.
* **Burst dedup ratio is timing-sensitive**. The `TestBurden_BurstDedup`
  threshold (40% of theoretical enqueues) tolerates the bursty-but-fast
  reconcile path observed on Kind (~20–25% in practice). On much slower
  hardware the ratio improves; on much faster hardware it could approach
  the limit. Tune `dedupBudget` if it becomes flaky.
* **Update-filter zero-event coverage**. There is no dedicated test that
  patches a field outside any rule projection (e.g. `status` only) to
  prove the update filter suppresses the event entirely. The current
  burst test uses real label/annotation churn, so it exercises only the
  workqueue + parent-index dedup paths.
* **CRD / non-built-in resources**. Only the five built-in workload kinds
  are watched. Custom resources are out of scope.
* **Service / ServiceAccount RBAC scoping**. The integration manifests use
  a cluster-wide `ClusterRole` so the suite can switch between cluster-wide
  and per-namespace topologies. Production deployments should follow the
  scoped-Role pattern in [`deploy/manifests.yaml`](../../deploy/manifests.yaml).

---

### File layout

```
test/integration/
├── manifests/                        # kustomize base applied by run.sh
│   ├── namespace.yaml
│   ├── rbac.yaml                     # ClusterRole + binding (cluster-wide for tests)
│   ├── configmap.yaml                # default rules + cluster-wide watch
│   ├── exporter-deployment.yaml      # named port "metrics" on :8080
│   ├── exporter-service.yaml
│   └── kustomization.yaml
├── e2e/                              # Go test harness (build tag `integration`)
│   ├── e2e_test.go                   # TestMain: kubeconfig + shared clientset
│   ├── helpers.go                    # rollout/scale/scrape utilities
│   ├── config_yaml.go                # ConfigMap renderers per topology
│   ├── topology_test.go              # ClusterWide / PerNamespace / IdleStable
│   ├── burden_test.go                # BurstDedup / ParentEventViaIndex
│   └── correctness_test.go           # FixtureFlow
├── run.sh                            # provisioning + go-test launcher
└── README.md                         # this file
```
