# metadata-exporter — Configuration Reference

`metadata-exporter` is a rule-driven Kubernetes metadata collector. It watches
resources via SharedInformers and publishes per-series labels as Prometheus
`_info` gauges (value = 1).

This document is the source of truth for the YAML schema.

- [1. Top-level structure](#1-top-level-structure)
- [2. `watch` — scoping the informers](#2-watch--scoping-the-informers)
- [3. `rules` — declaring metrics](#3-rules--declaring-metrics)
- [4. Pod-level vs Container-level](#4-pod-level-vs-container-level)
- [5. Recipes](#5-recipes)
- [6. Troubleshooting](#6-troubleshooting)

---

## 1. Top-level structure

```yaml
metricPrefix: "custom_"    # optional; prepended to every rule's name
watch: { ... }             # optional; defaults to cluster-wide, no selectors
rules:
  - { ... }                # one entry per Prometheus metric
```

Rules are independent: each rule produces one Prometheus metric whose name is
`metricPrefix + rule.name` (e.g. `custom_pod_info`).

---

## 2. `watch` — scoping the informers

```yaml
watch:
  namespaces: ["prod", "staging"]   # optional; empty/omitted = cluster-wide
  selectors:
    Pod:
      labelSelector: "app.kubernetes.io/part-of=my-platform"
      fieldSelector: "status.phase!=Succeeded"
    Deployment:
      labelSelector: "managed-by=argocd"
```

### Why this matters

- `labelSelector` and `fieldSelector` are forwarded to `LIST`/`WATCH` calls.
  The apiserver's **watch cache** filters events with these predicates *before*
  they are pushed to clients, so both the initial LIST payload and subsequent
  watch traffic shrink.
- `namespaces` turns each listed namespace into a scoped informer factory,
  which queries the apiserver with namespace-restricted LIST/WATCH requests.
- The smallest possible scope is a single object:
  `fieldSelector: "metadata.name=my-pod"` in a specific namespace.

### `fieldSelector` whitelist (per resource)

| Kind        | Allowed `fieldSelector` keys                                                                                                                             |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pod         | `metadata.name`, `metadata.namespace`, `spec.nodeName`, `spec.restartPolicy`, `spec.schedulerName`, `spec.serviceAccountName`, `status.phase`, `status.nominatedNodeName` |
| ReplicaSet  | `metadata.name`, `metadata.namespace`                                                                                                                    |
| Deployment  | `metadata.name`, `metadata.namespace`                                                                                                                    |
| StatefulSet | `metadata.name`, `metadata.namespace`                                                                                                                    |
| DaemonSet   | `metadata.name`, `metadata.namespace`                                                                                                                    |

Anything outside the whitelist is rejected by the apiserver with HTTP 400.
`metadata-exporter` issues one small `LIST` per configured selector at startup
(dry-run) so misconfigurations fail fast.

### Owner-chain "dangling selector" risk

If you narrow `Pod` with a selector but leave its parents (`ReplicaSet`,
`Deployment`, etc.) unrestricted, the owner chain still resolves. But the
reverse is dangerous: narrowing a parent (e.g. `Deployment`) while leaving
`Pod` unrestricted can hide the parent from our cache, turning
`source: topController` into a miss. A startup warning is printed when the
collector detects this pattern.

**Rule of thumb**: start broad, narrow `Pod` if needed, leave parents broader
than the Pod selector.

### RBAC implications

- `namespaces: []` or omitted → cluster-wide LIST/WATCH on all supported
  resources (needs a `ClusterRoleBinding`, as shipped in `deploy/manifests.yaml`).
- `namespaces: ["foo"]` → namespace-scoped LIST/WATCH (still satisfies the
  cluster-wide RBAC). A `RoleBinding` can be substituted if you wish, but you
  must bind it in every listed namespace.

---

## 3. `rules` — declaring metrics

A rule is a single metric declaration.

```yaml
rules:
  - name: "pod_info"       # required; metric name = metricPrefix + name
    help: "..."            # optional; Prometheus HELP text
    anchor: Pod            # required; Pod|Deployment|StatefulSet|DaemonSet|ReplicaSet
    forEach: "spec.containers[*]"   # optional; expands into N series
    relations:                      # optional; local aliases for sources
      - name: top
        via: topController
    labels:                          # required
      <label_name>:
        source: <source>             # optional; default "anchor"
        path: "<jsonpath>"           # required
        fallbacks:                   # optional; tried in order after path miss
          - source: anchor
            path: "..."
        onMissing: ""                # optional; default ""
```

### `anchor`

`anchor` identifies both the **resource whose events trigger reconciliation**
and the **subject of each emitted series**. Supported values:
`Pod`, `Deployment`, `StatefulSet`, `DaemonSet`, `ReplicaSet`.

### `forEach`

Optional JSONPath on the anchor object. When set, one series is emitted per
element of the result array. Common uses:

- `spec.containers[*]` — one row per regular container
- `spec.initContainers[*]` — one row per init container
- `spec.ephemeralContainers[*]` — one row per ephemeral container

Inside a rule with `forEach`, labels can reference the current iteration
element using `source: item`.

### `source` values

| Value                                                                 | Meaning                                                                                                  |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `anchor` *(default)*                                                  | The anchor object itself.                                                                                |
| `item`                                                                | The current `forEach` iteration element. Only valid when `forEach` is set.                               |
| `ownerController`                                                     | The direct `ownerReferences` controller of the anchor, if any.                                           |
| `topController`                                                       | The deepest ancestor in the owner chain that is `Deployment`, `StatefulSet`, or `DaemonSet`.             |
| `Pod` / `Deployment` / `StatefulSet` / `DaemonSet` / `ReplicaSet`     | The first object of that Kind encountered while walking the owner chain. If the anchor is that Kind it is returned directly. |
| Any name declared under `relations`                                   | A local alias for one of the above.                                                                      |

> All "related" objects are resolved exclusively from the informer cache.
> `metadata-exporter` does **not** issue extra API calls to walk the chain.

### `path` — path expression syntax

`path` uses a small, kubectl-inspired subset designed to cover every common
Kubernetes metadata extraction use-case:

| Construct            | Example                                                             | Notes                                               |
|----------------------|---------------------------------------------------------------------|-----------------------------------------------------|
| Dotted field access  | `metadata.name` · `status.phase` · `spec.nodeName`                  | Idents match `[A-Za-z_][A-Za-z0-9_-]*`.             |
| Array wildcard       | `spec.containers[*]`                                                 | Use with `forEach` to emit one series per element.  |
| Array index          | `spec.containers[0].image`                                           | Zero-based.                                         |
| Map key with symbols | `metadata.annotations["argocd.argoproj.io/tracking-id"]`             | Single or double quotes.                            |
| Leading kubectl wrap | `{.metadata.name}`                                                   | The outer `{...}` is optional.                      |

Filter expressions (`[?(@.x==y)]`) and `range`/`end` blocks are **not**
supported — the parser is intentionally small. If you need them, open an
issue; the default form is expressive enough for annotation/label/image/phase
extraction.

### `fallbacks` + `onMissing`

For each label we evaluate:

1. The primary `path`. An empty string, missing key, or evaluation error is
   treated as a miss.
2. Each entry in `fallbacks` in declaration order, stopping at the first
   non-empty result.
3. `onMissing` (default: empty string) is used if all of the above miss.

`fallbacks` entries accept the same fields as the top-level extract except
that nested `fallbacks` are not allowed.

### Label-name rules

- Keys under `labels:` become Prometheus label names. They must match
  `[a-zA-Z_][a-zA-Z0-9_]*` and cannot start with `__`.
- The label set is **fixed at startup** from your config: every series for
  a given rule carries the same label names. Missing values render as empty
  strings rather than collapsing labels.

---

## 4. Pod-level vs Container-level

Both granularities coexist trivially — just declare two rules with different
names and different (or absent) `forEach`. They become independent metrics.

| Desired granularity                 | `anchor`     | `forEach`                         | Resulting cardinality   |
|-------------------------------------|--------------|-----------------------------------|-------------------------|
| One series per Pod                  | `Pod`        | *omit*                            | 1 Pod → 1 series        |
| One series per (Pod, container)     | `Pod`        | `spec.containers[*]`              | 1 Pod with N containers → N series |
| One series per (Pod, initContainer) | `Pod`        | `spec.initContainers[*]`          | as above                |
| One series per (Pod, ephemeral)     | `Pod`        | `spec.ephemeralContainers[*]`     | as above                |
| One series per Deployment           | `Deployment` | *omit*                            | 1 Deployment → 1 series |

Example pairing a Pod-level and container-level metric in one config:

```yaml
rules:
  - name: "pod_info"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      node:      { path: "spec.nodeName" }
      phase:     { path: "status.phase" }

  - name: "pod_container_info"
    anchor: Pod
    forEach: "spec.containers[*]"
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      container: { source: item, path: "name" }
      image:     { source: item, path: "image" }
```

Scraping `/metrics` will then expose two separate metrics:

```
custom_pod_info{namespace="prod",pod="api-abc",node="...",phase="Running"} 1
custom_pod_container_info{namespace="prod",pod="api-abc",container="api",image="..."} 1
custom_pod_container_info{namespace="prod",pod="api-abc",container="sidecar",image="..."} 1
```

---

## 5. Recipes

### 5.1 Argo CD tracking id, with fallback to the pod itself

```yaml
rules:
  - name: "pod_argocd_info"
    anchor: Pod
    relations:
      - { name: top, via: topController }
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      app:
        source: top
        path: 'metadata.annotations["argocd.argoproj.io/tracking-id"]'
        fallbacks:
          - source: anchor
            path: 'metadata.annotations["argocd.argoproj.io/tracking-id"]'
```

### 5.2 Helm release name

```yaml
rules:
  - name: "pod_helm_info"
    anchor: Pod
    relations:
      - { name: top, via: topController }
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      release:
        source: top
        path: 'metadata.annotations["meta.helm.sh/release-name"]'
      chart:
        source: top
        path: 'metadata.labels["helm.sh/chart"]'
```

### 5.3 Minimising cluster load

Watch one namespace with a narrow label selector on Pods only:

```yaml
watch:
  namespaces: ["prod"]
  selectors:
    Pod:
      labelSelector: "app.kubernetes.io/part-of=payments"
```

Parent resources (Deployment/StatefulSet/DaemonSet/ReplicaSet) stay unrestricted
so owner-chain resolution keeps working.

### 5.4 Deployment-anchored metric

```yaml
rules:
  - name: "deployment_info"
    anchor: Deployment
    labels:
      namespace:        { path: "metadata.namespace" }
      deployment:       { path: "metadata.name" }
      replicas_desired: { path: "spec.replicas" }
      argocd_app:
        path: 'metadata.annotations["argocd.argoproj.io/tracking-id"]'
```

---

## 6. Troubleshooting

| Symptom                                               | Likely cause                                                                                        | Resolution                                                                                   |
|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Label always empty                                    | `path` points to a field that doesn't exist on this object; `source` not in owner chain for this anchor. | Confirm the path with `kubectl -o jsonpath='{.your.path}'`. Add a `fallbacks` entry if needed. |
| `controller_kind`/`controller_name` empty for Static Pods | Static Pods have no owner references.                                                               | Expected; add a `fallbacks` with `source: anchor` or set `onMissing` to a sentinel.          |
| Startup error mentioning `fieldSelector`              | Field is not in the per-resource whitelist.                                                         | Move the predicate to `labelSelector` or remove it.                                          |
| Warning "pod selector combined with stricter parent selector ..." | Parents are filtered away; owner chain can't resolve them.                                          | Drop the parent selector or broaden it.                                                      |
| `metrics-addr` refuses connections                    | Container port mismatch or the process crashed at config validation.                                | Check logs; the pod's readiness probe targets `/healthz`.                                    |
