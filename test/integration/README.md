## Integration Test (Kind) - Full Explanation

This integration test validates the metadata-exporter against a real Kubernetes API server (Kind),
instead of only package-level unit tests.

### What it validates

- informer list/watch and cache sync in a real cluster
- ownerReferences resolution (Pod -> ReplicaSet -> Deployment)
- rule evaluation and JSONPath extraction
- `forEach: spec.containers[*]` producing one series per container, with
  per-container `image` (name + tag) reaching the sink
- `flatten` expansion of a controller-level annotation into a fixed Prometheus
  label, including sanitisation of `.` and `/` into `_`
- Prometheus metric emission and stale series cleanup after delete
- exporter health endpoint readiness before metric checks

### What it does NOT validate

- multi-namespace behavior
- StatefulSet / DaemonSet specific scenarios
- advanced `forEach` / fallback combinations
- performance / load / long-run stability

### Test inputs

1. **Config input** (`test/integration/manifests/configmap.yaml`)
   - `metricPrefix: "it_"`
   - watch namespace: `metadata-exporter-it`
   - rule `pod_info` with labels:
     - namespace, pod, phase
     - controller_kind (`top.kind`)
     - controller_name (`top.metadata.name`)
     - `flatten` entry on `top.metadata.annotations` for
       `integration.test/controller-note`, producing the Prometheus label
       `controller_annotation_integration_test_controller_note`
   - rule `pod_container_info` (`forEach: spec.containers[*]`) with labels:
     - namespace, pod
     - container (`item.name`), image (`item.image`)
     - controller_name (`top.metadata.name`)

2. **Fixture workload** (`test/integration/manifests/fixture-deployment.yaml`)
   - deployment `fixture-web` (1 replica), producing RS + Pod owner chain
   - Deployment annotation
     `integration.test/controller-note: "from-fixture-deployment"` so the
     flatten rule has a value to resolve
   - Pod spec runs two containers (`pause-main` @ `registry.k8s.io/pause:3.9`
     and `pause-sidecar` @ `registry.k8s.io/pause:3.10`) to exercise both
     `forEach` fan-out and distinct image/tag values

3. **Runtime variables** (`test/integration/run.sh`)
   - `KIND_CLUSTER_NAME` (default `metadata-exporter-it`)
   - `INTEGRATION_IMAGE` (default `metadata-exporter:it`)
   - `DOCKER_BUILD_PLATFORM`
   - `PF_LOCAL_PORT` (default `18080`)
   - `METRICS_WAIT_SEC`, `DELETE_WAIT_SEC`
   - `SKIP_KIND_CREATE`, `SKIP_CLUSTER_DELETE`

### Expected outputs

1. exporter and fixture deployments roll out successfully
2. `/healthz` returns 200
3. `/metrics` contains:
   - `it_pod_info{...}` with
     - `controller_kind="Deployment"`
     - `controller_name="fixture-web"`
     - `namespace="metadata-exporter-it"`
     - `controller_annotation_integration_test_controller_note="from-fixture-deployment"`
   - `it_pod_container_info{...}` with at least two series, respectively
     - `container="pause-main"`, `image="registry.k8s.io/pause:3.9"`
     - `container="pause-sidecar"`, `image="registry.k8s.io/pause:3.10"`
4. after deleting `fixture-web`, metrics no longer contain `controller_name="fixture-web"`
   (both `it_pod_info` and `it_pod_container_info` series disappear together)

### Script flow

1. check dependencies (`kind`, `kubectl`, `docker`, `curl`)
2. create Kind cluster (unless `SKIP_KIND_CREATE=1`)
3. build image and load into Kind node
4. apply manifests via `kubectl apply -k`
5. wait for rollout and pod readiness
6. port-forward exporter service
7. check `/healthz`
8. poll `/metrics` with exponential backoff until expected series appears
9. delete fixture deployment
10. poll `/metrics` until fixture series disappears
11. cleanup (port-forward process, temp kubeconfig, optional cluster delete)

### Run commands

```sh
./test/integration/run.sh