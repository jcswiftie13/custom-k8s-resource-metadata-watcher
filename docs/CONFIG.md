# metadata-exporter — 設定參考與設計

`metadata-exporter` 是以設定驅動的 Kubernetes metadata 匯出器：透過 SharedInformer 快取觀察叢集資源、在快取內走訪 `ownerReferences`（不額外打 API），並將每筆時間序列的標籤以 Prometheus `_info` 風格 Gauge（值為 1）暴露。

本文件為 **YAML 結構的權威說明**，並說明其**設計意圖**：設定如何載入、驗證、編譯成執行期結構，以及如何銜接 informer 與 Prometheus sink。

**實作對照（原始碼為準）**

- YAML 模型、`Load`、`Validate`：`pkg/config/config.go`
- CLI（`-config`，預設 `/etc/metadata-exporter/config.yaml`）：`cmd/main.go`
- Path 編譯與求值：`pkg/collector/evaluator.go`、`pkg/collector/pathexpr.go`、`pkg/collector/flatten.go`
- Collector 組裝：`pkg/collector/collector.go`

若本文件與程式碼不一致，**以原始碼為準**。

---

## 目錄

1. [總覽與設計目標](#1-總覽與設計目標)
2. [設定生命週期](#2-設定生命週期)
3. [頂層結構](#3-頂層結構)
4. [watch：限縮 informer 範圍](#4-watch限縮-informer-範圍)
5. [rules：宣告指標](#5-rules宣告指標)
6. [Pod 層級與容器層級](#6-pod-層級與容器層級)
7. [展平 labels 與 annotations](#7-展平-labels-與-annotations)
8. [範例配方](#8-範例配方)
9. [疑難排解](#9-疑難排解)
10. [執行期內部行為](#10-執行期內部行為)
11. [結構與實作對照表](#11-結構與實作對照表)

---

## 1. 總覽與設計目標

### 1.1 設定必須表達的三件事

1. **Watch 拓樸** — 哪些物件會進 SharedInformer 快取（`watch`），以縮小對 apiserver 的 LIST/WATCH 量。
2. **指標契約** — 每條 `rules[]` 對應 **一個** Prometheus metric，且在啟動時即具備 **固定標籤集合**（含 `flatten` 產生的標籤）。
3. **取值語意** — 每個標籤如何從 anchor、`forEach` 的 `item`、owner 鏈（`ownerController`、`topController` 或具體 Kind）、或 **`relations` 別名** 取出字串。

相關物件僅從 **informer 快取** 解析；exporter **不會**為走訪 owner 鏈而額外呼叫 API。

### 1.2 設計原則

| 原則 | 意義 |
|------|------|
| **宣告式** | 營運方描述指標與取值，設定內不含 Go 程式。 |
| **儘早失敗** | 結構問題在 `Validate` 攔截；**path 語法**在 collector 建構、**`Compile`** 時才解析，錯誤則啟動失敗。 |
| **時間序列形狀穩定** | 每條 rule 的標籤名稱於啟動時固定；缺值為空字串，不會省略標籤。 |
| **YAML 鍵與 `json` struct tag 一致** | Go 結構使用 `json:"..."`；YAML 鍵為 **camelCase**（如 `metricPrefix`、`watch`、`rules`），與 `sigs.k8s.io/yaml` 慣例一致。 |
| **驗證分兩階** | `Validate` **不**解析 `path` 字串；`Compile` 才對每條 `path`、`forEach`、flatten 的 `path` 做 `parsePath`。 |

### 1.3 非目標

- **非**完整 JSONPath：僅支援下文 [path 語法](#path-syntax) 所述子集。
- workqueue 深度、worker 數、client QPS/burst、監聽位址等為 **`cmd/main.go` 的 CLI 旗標**，不在 YAML 內。

---

## 2. 設定生命週期

### 2.1 設定檔來源

程式僅讀取 **一份** YAML；預設路徑 `/etc/metadata-exporter/config.yaml`，可用 `-config` 覆寫（見 `cmd/main.go`）。

### 2.2 載入 → 解析 → 驗證

`config.Load(path)` 依序：

1. `os.ReadFile` 讀取檔案。
2. `sigs.k8s.io/yaml.Unmarshal` 填入 `*config.Config`。
3. `cfg.Validate()`：至少一條 rule、`metricPrefix` 與各 rule 名稱組成之 Prometheus metric 名合法且不重複、每條 rule 的 `anchor`、`labels`、`relations`、`flatten` 與 `source`／`forEach` 搭配等。

任一步失敗則在建立 Kubernetes client 或 informer **之前**結束程式。

### 2.3 編譯規則（YAML 已通過驗證後）

`collector.New(cfg, ...)` 對每個 `cfg.Rules[i]` 呼叫 `Compile(rule)`：

- 將 `forEach`、各 `labels` 的 `path`、fallback 的 `path`、flatten 的 `path` 編成內部 path AST（`pkg/collector/pathexpr.go` 的 `parsePath`）。
- **`relations` 別名在編譯期消解**：`CompiledExtract.Source` 存解析後名稱（例如別名 `top` → `topController`），執行期 `srcLookup` 只用解析後鍵（`pkg/collector/evaluator.go` 的 `compileExtract`）。
- `flatten` 產生額外編譯後標籤，並與既有 labels 合併、重排順序。
- 向 Prometheus sink `RegisterRule` 註冊最終 metric 名（`metricPrefix + rule.name`）與**有序**標籤集合。

因此 **`path` 打錯** 可能在 `Validate` 通過後，仍於 collector 初始化時失敗。

### 2.4 執行期求值（摘要）

每次 reconcile：resolver 從快取組出 **chain**；各物件轉成 unstructured `map`；`EvaluateForEach` 展開 `forEach`（若省略則仍以一輪合成項目迭代，見 `pkg/collector/evaluator.go` 的 `EvaluateForEach`）；每個標籤依序：主要 path → fallbacks → `onMissing`；結果寫入 sink（例如 Prometheus 的 `ReplaceForAnchor`）。

---

## 3. 頂層結構

```yaml
metricPrefix: "custom_"    # 選填；加在每條 rule 的 name 之前
watch: { ... }             # 選填；預設叢集全域、五種 kind 全 watch、無 selector
rules:
  - { ... }                # 每個元素對應一個 Prometheus metric
```

各 rule **彼此獨立**；metric 全名為 `metricPrefix + rule.name`（例如 `custom_pod_info`）。

對應 `pkg/config/config.go` 的根型別 `Config`：

| YAML 欄位 | 角色 |
|-----------|------|
| `metricPrefix` | 選填前綴；與各 `rules[].name` 組成註冊用 Prometheus metric 名稱，並受 metric 命名 regex 檢查。 |
| `watch` | 選填 `WatchScope`：以 `watch.resources[]` 宣告每個 Kind 的 scope、namespace 與 selector。 |
| `rules` | **必填**、非空之 `Rule` 陣列。 |

---

## 4. watch：限縮 informer 範圍

```yaml
watch:
  resources:
    - kind: Pod
      scope: Namespaced
      namespaces: ["prod", "staging"]   # 可省略，省略代表此 kind cluster-wide
      labelSelector: "app.kubernetes.io/part-of=my-platform"
      fieldSelector: "status.phase!=Succeeded"
    - kind: Deployment
      scope: Namespaced
      namespaces: ["prod", "staging"]
      labelSelector: "managed-by=argocd"
    - kind: Node
      scope: Cluster
```

`resources[].kind` 必須是 `Pod` | `ReplicaSet` | `Deployment` | `StatefulSet` | `DaemonSet` | `Node`。若某 Kind 不在 `resources` 中，該 Kind 不會建立 SharedInformer。

若省略 `resources` 或留空，等同 watch 全部支援 kind（含 `Node`）且無 selector。`Validate()` 會驗證每條 `rule` 的 `anchor` 及顯式 Kind source 有列在有效 watch set 內。

**遷移（breaking）**：`watch.selectors`、`watch.kinds`、`watch.namespaces` 均已移除。請改用 `watch.resources[]`。

### 為何重要

- `labelSelector` 與 `fieldSelector` 會傳給 `LIST`／`WATCH`；apiserver 的 **watch cache** 會在事件下發給 client **之前**依述詞過濾，初始 LIST 與後續 watch 流量都會縮小。
- `namespaces` 非空時，每個列出的 namespace 會建立獨立的 scoped informer factory，對 apiserver 發 namespace 限定的 LIST/WATCH。
- 最小範圍可縮到單一物件：例如在特定 namespace 內使用 `fieldSelector: "metadata.name=my-pod"`。

### `labelSelector`／`fieldSelector`：單一字串與逗號 AND

- 每個 kind 下的 `labelSelector`、`fieldSelector` 在 YAML 裡各是 **一個字串**（不是陣列）；程式會原樣帶入 Kubernetes `LIST`／`WATCH` 的 query 參數，**不**在應用程式內解析或拆段。
- **`labelSelector`**：在標準 Kubernetes label selector 語法中，**同一字串內**以英文逗號 **`,`** 連接多個 requirement，語意為 **AND**（例如 `app.kubernetes.io/name=api,env=prod`）。需要 OR 或集合條件時，使用 set-based 等寫法，同樣寫在這一個字串裡。
- **`fieldSelector`**：同樣為單一字串；是否允許用 **`,`** 串接多個欄位條件，**依資源種類與叢集版本由 apiserver 決定**；不合法時啟動時的 dry-run `LIST` 會失敗並回錯。

### 各資源 `fieldSelector` 白名單

| Kind        | 允許的 `fieldSelector` 鍵 |
|-------------|---------------------------|
| Pod         | `metadata.name`、`metadata.namespace`、`spec.nodeName`、`spec.restartPolicy`、`spec.schedulerName`、`spec.serviceAccountName`、`status.phase`、`status.nominatedNodeName` |
| ReplicaSet  | `metadata.name`、`metadata.namespace` |
| Deployment  | `metadata.name`、`metadata.namespace` |
| StatefulSet | `metadata.name`、`metadata.namespace` |
| DaemonSet   | `metadata.name`、`metadata.namespace` |

超出白名單會被 apiserver 以 HTTP 400 拒絕。`metadata-exporter` 啟動時會對每組設定好的 selector 做一次小型 `LIST`（dry-run），以儘早暴露錯誤。

### Owner 鏈與「過窄的 parent selector」風險

若只縮小 `Pod` 的 selector、parent（`ReplicaSet`、`Deployment` 等）維持較寬，owner 鏈通常仍可解析。反之若 **parent** 選得很窄、**Pod** 卻很寬，可能導致 parent **不在快取中**，`source: topController` 等變成 miss；collector 偵測到此模式時會印出啟動警告。

**經驗法則**：先寬後窄；需要時縮 `Pod`；parent selector 應 **寬於** Pod selector。

### RBAC 影響

- `namespaces` 空或省略 → 各支援資源為叢集全域 LIST/WATCH（需 `ClusterRoleBinding`，見 `deploy/manifests.yaml`）。
- `namespaces: ["foo"]` → namespace 範圍 LIST/WATCH（仍可由叢集層級 RBAC 滿足）。若改 `RoleBinding`，須在每個列出的 namespace 綁定。

### Watch 拓樸：叢集全域 vs 每 namespace

令 `K = len(watch.kinds)` 的鍵（若 `kinds` 空或省略，則 `K = 5`）。

| 模式 | 觸發條件 | 開啟的 watch 量級 | 取捨 |
|------|----------|-------------------|------|
| 叢集全域 | `watch.namespaces` 省略或空 | 每 Kind 約一個，共 `K` 路 | 對大型叢集通常最省；仍可對每個 kind 用 `labelSelector`／`fieldSelector` 縮流。 |
| 每 namespace | `watch.namespaces` 非空 | 約 `len(namespaces) × K` 路 | 隔離強；或搭配 `fieldSelector: metadata.name=...` 等縮到單一 Pod。 |
| 僅部分 Kind | `kinds` 只列部分 key | 約 `K`（× namespace 段數） | 降低多餘的 GVR watch；有 `topController`／`ownerController` 但缺父 kind 時 owner 鏈易 miss。 |

啟動日誌會印出 `watch mode = cluster-wide` 或 `per-namespace` 及 factory 數量。namespace 過濾發生在 client-go cache；叢集全域模式 **不會**因此線性增加「每次事件」的 CPU，主要影響的是對 apiserver 的 watch 扇出。

**設計說明**：`watch` 只決定 **快取內有什麼**；rule 仍可引用 `topController`／`ownerController`，但若 parent 被 selector 切掉或沒有 watch 該 parent Kind，解析會 miss——這是 `watch` 與 `rules` 之間的**設定耦合**；`topController` 未強制在 schema 內，但若 rule 用到了，collector 在啟動時會在缺 `ReplicaSet` / `Deployment` / `StatefulSet` / `DaemonSet` 任一者時打 **Warn** 日誌（可能 miss owner 鏈）。

---

## 5. rules：宣告指標

一條 rule 即一個 metric 宣告。

```yaml
rules:
  - name: "pod_info"       # 必填；metric 全名 = metricPrefix + name
    help: "..."            # 選填；Prometheus HELP
    anchor: Pod            # 必填；Pod|Deployment|StatefulSet|DaemonSet|ReplicaSet
    forEach: "spec.containers[*]"   # 選填；展開為 N 筆 series
    relations:                      # 選填；來源別名
      - name: top
        via: topController
    labels:                          # 必填
      <label_name>:
        source: <source>             # 選填；預設 "anchor"
        path: "<jsonpath>"           # 必填
        fallbacks:                   # 選填；主 path miss 後依序嘗試
          - source: anchor
            path: "..."
        onMissing: ""                # 選填；預設 ""
```

### `anchor`

`anchor` 同時決定 **哪種資源事件會觸發 reconcile**，以及 **每筆輸出 series 的主體**。允許值：`Pod`、`Deployment`、`StatefulSet`、`DaemonSet`、`ReplicaSet`。

### `forEach`

選填；為相對 anchor 的 path，評估結果為陣列時，**每個元素一筆 series**。常見用法：

- `spec.containers[*]` — 每個一般容器一筆
- `spec.initContainers[*]` — 每個 init 容器一筆
- `spec.ephemeralContainers[*]` — 每個 ephemeral 容器一筆

有 `forEach` 時，可用 `source: item` 指到目前迭代元素。

**實作**：未設 `forEach` 時，`EvaluateForEach` 仍回傳單元素（含 `nil` 的 slice），讓後續管線一致（見 `pkg/collector/evaluator.go`）。

### `relations`（別名）

每筆 `{ name, via }` 在該 rule 內新增一個合法 `source` 名稱。`Validate` 限制包括：

- `name` 不得與 builtin 或其它別名衝突。
- `via` 須為 builtin（`anchor`、`ownerController`、`topController`）或支援的 Kind；**不可**為 `item`（`item` 僅能在有 `forEach` 時作為 label 的直接 `source`）。

編譯時會把 `source: <別名>` 改寫為 `via`（`pkg/config/config.go` 的 `Rule.ResolveRelation`）。

### `source` 取值

| 值 | 意義 |
|----|------|
| `anchor`（預設） | anchor 物件本身。 |
| `item` | 目前 `forEach` 元素；**僅**在設了 `forEach` 時合法。 |
| `ownerController` | anchor 在 `ownerReferences` 上的直接 controller（若有）。 |
| `topController` | owner 鏈上最深、且為 `Deployment`／`StatefulSet`／`DaemonSet` 的祖先。 |
| `Pod`／`Deployment`／… | 沿 owner 鏈走訪時**第一個**該 Kind；若 anchor 即該 Kind則直接回傳 anchor。 |
| `relations` 宣告的 `name` | 對應 `via` 所指的上述其中一種來源。 |

> 相關物件**僅**從 informer 快取取得；**不**為走訪 owner 鏈額外打 API。

<a id="path-syntax"></a>
### path 語法

類 kubectl 的小子集：

| 構造 | 範例 | 說明 |
|------|------|------|
| 點號欄位 | `metadata.name`、`status.phase`、`spec.nodeName` | 識別字樣式 `[A-Za-z_][A-Za-z0-9_-]*`。 |
| 陣列萬用字元 | `spec.containers[*]` | 常與 `forEach` 並用，一元素一 series。 |
| 陣列索引 | `spec.containers[0].image` | 零為基底。 |
| 含符號的 map 鍵 | `metadata.annotations["argocd.argoproj.io/tracking-id"]` | 單雙引號皆可。 |
| kubectl 風格外層 | `{.metadata.name}` | 外層 `{...}` 可省略。 |

**不支援** filter 表達式（如 `[?(@.x==y)]`）與 `range`／`end` 區塊——刻意維持小解析器。

### `fallbacks` 與 `onMissing`

對每個標籤：

1. 評估主要 `path`；空字串、缺鍵或求值錯誤視為 miss。
2. 依序嘗試 `fallbacks` 各項，取第一個非空結果。
3. 皆 miss 則用 `onMissing`（預設空字串）。

`fallbacks` 內每一項欄位與頂層 extract 相同，但 **不可**再嵌套 `fallbacks`。

### 標籤名稱規則

- `labels:` 的鍵即 Prometheus 標籤名，須符合 `[a-zA-Z_][a-zA-Z0-9_]*`，且不得以 `__` 開頭。
- 每條 rule 的標籤集合在 **啟動時固定**；缺值為空字串，不會動態刪除標籤。

### 啟動時不變式（摘要）

- 至少一條 rule；每條需非空 `name`、支援的 `anchor`、非空 `labels`。
- `metricPrefix + name` 全叢集設定內唯一，且符合 Prometheus metric 命名。
- `flatten` 產生的名稱不得與 `labels` 或其它 flatten 列衝突。
- `relations` 的 `via` 合法、不得以 `item` 作為 `via`（詳見 `pkg/config/config.go` 的 `Validate`）。

---

## 6. Pod 層級與容器層級

以**兩條 rule**（不同 `name`、不同或省略 `forEach`）即可同時存在兩種粒度，彼此獨立。

| 目標粒度 | `anchor` | `forEach` | 結果基数 |
|----------|----------|-----------|----------|
| 每 Pod 一筆 | `Pod` | 省略 | 1 Pod → 1 series |
| 每 (Pod, 容器) 一筆 | `Pod` | `spec.containers[*]` | N 容器 → N series |
| init 容器 | `Pod` | `spec.initContainers[*]` | 同上 |
| ephemeral 容器 | `Pod` | `spec.ephemeralContainers[*]` | 同上 |
| 每 Deployment 一筆 | `Deployment` | 省略 | 1 Deployment → 1 series |

範例：

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

`/metrics` 會暴露兩個不同 metric，例如：

```
custom_pod_info{namespace="prod",pod="api-abc",node="...",phase="Running"} 1
custom_pod_container_info{namespace="prod",pod="api-abc",container="api",image="..."} 1
custom_pod_container_info{namespace="prod",pod="api-abc",container="sidecar",image="..."} 1
```

---

## 7. 展平 labels 與 annotations

當 `metadata.annotations` 或 `metadata.labels` 上有多個鍵值，希望變成**同一 series 上多個 Prometheus 標籤**（例如數個眾所周知的 controller annotation），可用 `flatten:` 以 **allow-list** 宣告，避免在 `labels:` 手寫大量重複 path。

```yaml
rules:
  - name: "pod_info"
    anchor: Pod
    relations:
      - { name: top, via: topController }
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
    flatten:
      - namePrefix: "controller_annotation_"
        source: top                # 預設為 "anchor"
        path: "metadata.annotations"
        keys:
          - "integration.test/controller-note"
          - "integration.test/owner"
        onMissing: ""              # 選填；預設 ""
      - namePrefix: "pod_label_"
        path: "metadata.labels"
        keys: ["app.kubernetes.io/name"]
```

### 欄位說明

| 欄位 | 必填 | 預設 | 意義 |
|------|------|------|------|
| `namePrefix` | 否 | `""` | 接到每個產生之標籤名前（逐字元前綴）。 |
| `source` | 否 | `"anchor"` | 與 `labels` 的 `source` 相同語意（`item` 仍須有 `forEach`）。 |
| `path` | 是 | | 須能解析為 `map[string]interface{}`（常為 `metadata.annotations`／`metadata.labels`）。 |
| `keys` | 是 | | 非空、同條目內鍵不可重複。 |
| `onMissing` | 否 | `""` | 某鍵在 map 中不存在，或整段 `path` miss 時之值。 |

### 名稱產生與清理（sanitize）

Prometheus 標籤須符合 `[a-zA-Z_][a-zA-Z0-9_]*`。Kubernetes 的 annotation／label 鍵常含 `.`、`/`、`-`，故每個 `key` 會經決定性清理後再接 `namePrefix`：

- 不在 `[A-Za-z0-9_]` 的字元改為 `_`。
- 若首字元為數字，結果前會再加 `_`。

| 鍵 | 清理後片段 |
|----|------------|
| `integration.test/controller-note` | `integration_test_controller_note` |
| `app.kubernetes.io/name` | `app_kubernetes_io_name` |
| `123abc` | `_123abc` |

最終標籤名為 `namePrefix + sanitize(key)`，啟動時會檢查命名規則；不得以 `__` 開頭（Prometheus 保留）。

### 碰撞規則

同一 rule 內，若 `labels:` 與 `flatten:` 或不同 `flatten` 列產生相同標籤名，啟動即失敗。儀表板可假設每個 metric 的標籤集合 **固定且無衝突**。

### Miss 語意

若 `path` 解析結果**不是 map**（缺欄位、型別錯誤等），整段 flatten 視為 total miss：該列產生的**所有**標籤皆取該列的 `onMissing`。若 map 存在但某 `key` 不存在，僅該標籤 miss。

與其餘設定一致：**固定標籤集合**，miss 為空字串而非刪標籤。

**設計說明**：flatten 在編譯期為每個 key append path segment（`pkg/collector/flatten.go` 的 `compileFlatten`），避免在 path 字串內塞入任意 annotation 字元。

---

## 8. 範例配方

### 8.1 Argo CD tracking id，並 fallback 到 Pod 本體

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

### 8.2 Helm release 名稱

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

### 8.3 降低叢集負載

僅 watch 單一 namespace，並縮小 Pod：

```yaml
watch:
  namespaces: ["prod"]
  kinds:
    Pod:
      labelSelector: "app.kubernetes.io/part-of=payments"
    # 其他 kind 不列 = 不 watch；使用 topController 等時需自行權衡是否補上
```

Parent（`ReplicaSet` / `Deployment` 等）若也縮了 selector 或從 `kinds` 省略，owner 鏈易 miss，請見第 4 節「Owner 鏈」風險；通常 parent 的選擇應**寬於** Pod 或一併把需要的 kind 納入 `kinds`。

### 8.4 以 Deployment 為 anchor

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

## 9. 疑難排解

| 現象 | 可能原因 | 處理方式 |
|------|----------|----------|
| 標籤永遠空 | `path` 在該物件上不存在；或 `source` 在此 anchor 的 owner 鏈上取不到。 | 用 `kubectl -o jsonpath='{.…}'` 確認；必要時加 `fallbacks`。 |
| Static Pod 沒有 controller 類標籤 | Static Pod 無 owner references。 | 預期行為；可對 anchor 加 `fallbacks` 或設 `onMissing`。 |
| 啟動錯誤提到 `fieldSelector` | 欄位不在該資源白名單。 | 改為 `labelSelector` 或移除。 |
| 警告「pod selector combined with stricter parent selector …」 | parent 被 filter 掉，owner 鏈斷裂。 | 放寬或移除 parent 的過窄 selector。 |
| 警告「not all parent kinds are watched」 | 規則使用 `topController`／`ownerController`，但 `watch.kinds` 未納入典型 owner 鏈所要的 parent kind。 | 補上 `ReplicaSet` 等，或放寬 `kinds`；屬可預期，指標中 parent 欄位可能空。 |
| `metrics-addr` 連不上 | 容器埠不符或程式在驗證階段即退出。 | 查日誌；readiness 通常打 `/healthz`。 |

---

## 10. 執行期內部行為

以下行為**不在** YAML schema 內，但會影響對 apiserver 的負載與可觀測性。

### Workqueue 與 worker 池

每個 informer 事件會產生 `anchorRef = {kind, namespace, name}` 推入具 rate limit 的 `workqueue`；`--reconcile-workers` 支線程池 drain queue，對每個 anchor 執行一次 reconcile。相同 key 會合併，故短時間大量 parent 更新仍可能收斂為每 anchor 少量 reconcile。

自監控指標例如：

- `exporter_reconcile_queue_depth`
- `exporter_reconcile_total{rule,result}`
- `exporter_reconcile_duration_seconds`（依 `anchor_kind`）

### 反向 parent 索引

成功 reconcile 後會記錄 owner 鏈上物件 UID → 影響的 `anchorRef`；parent 事件優先走索引只 requeue 相關 anchor。索引 miss（冷啟動等）時會 fallback 為同 namespace 掃描一次，之後由 reconcile 逐漸填滿索引。

相關指標：`exporter_parent_index_hit_total`、`exporter_parent_index_fallback_total`。

### 更新事件過濾

對每 UID 快取 `{metadata.generation, labels, annotations}` 的 digest；僅 status 變動等不影響 digest 的更新可不入隊，減少大 Pod 的無謂 reconcile。

### Apiserver 限速

`--kube-api-qps`（預設 20）與 `--kube-api-burst`（預設 40）設定 client-go token bucket；informers 同步完成後對外 REST 通常很少，但可限制啟動 LIST 等尖峰。

---

## 11. 結構與實作對照表

| 概念 | 主要程式位置 |
|------|----------------|
| 根 `Config`、`WatchScope`、`Rule`、`Extract`、`FlattenExtract`、`RelationAlias` | `pkg/config/config.go`（`json` tag = YAML 鍵） |
| `Load`（回絕舊的 `watch.selectors`）、`Validate`、`WatchScope.EffectiveKinds`、`ResolveRelation` | `pkg/config/config.go` |
| CLI `-config` 與 `collector.New` 銜接 | `cmd/main.go` |
| `Compile`、`CompiledRule`、標籤求值順序 | `pkg/collector/evaluator.go` |
| Path 文法（`parsePath`、`evaluate`） | `pkg/collector/pathexpr.go` |
| Flatten 編譯 | `pkg/collector/flatten.go` |
| `cfg.Watch` → informers；每 rule `Compile` 與 `RegisterRule` | `pkg/collector/collector.go` |
| 整合測設定形狀 | `test/integration/e2e/config_yaml.go` |
| 架構長文 | `docs/METADATA_EXPORTER_DEEP_DIVE.md`、`docs/AI_REFERENCE_METADATA_EXPORTER.md` |

---

*本文件合併原「設定參考」與「結構設計」敘述；若程式變更請以原始碼為準。*
