# metadata-exporter — 設定參考與設計

`metadata-exporter` 是以設定驅動的 Kubernetes metadata 匯出器：透過 SharedInformer 快取觀察叢集資源、在快取內走訪 `ownerReferences`（不額外打 API），並以 **custom `prometheus.Collector`** 在 **scrape time** 把 cache 現況組成 `_info` 風格 Gauge（值為 1）後輸出到 Prometheus。

> **架構摘要（v2，custom collector 全面替換）**
> exporter 不再以 `GaugeVec` 維護任何 series 狀態。每次 `/metrics` 被 scrape，collector 會以 `cfg.Rules` 走訪 informer cache、解析 owner 鏈、求值固定 labels 與動態 `expandLabels`，當下產出整個 metric family。事件驅動的 workqueue／reconcile loop／reverse parent index 已移除；informers 仍然以 watch 維持 cache 新鮮度，但不再觸發指標寫入。

本文件為 **YAML 結構的權威說明**，並說明其**設計意圖**：設定如何載入、驗證、編譯成執行期結構，以及如何在 scrape time 銜接 informer 與 Prometheus output。

**實作對照（原始碼為準）**

- YAML 模型、`Load`、`Validate`：`pkg/config/config.go`
- CLI（`-config`，預設 `/etc/metadata-exporter/config.yaml`）：`cmd/main.go`
- Path 編譯與求值：`pkg/collector/evaluator.go`、`pkg/collector/pathexpr.go`
- Custom collector / scrape-time 組裝：`pkg/collector/collector.go`

若本文件與程式碼不一致，**以原始碼為準**。

---

## 目錄

1. [總覽與設計目標](#1-總覽與設計目標)
2. [設定生命週期](#2-設定生命週期)
3. [頂層結構](#3-頂層結構)
4. [watch：限縮 informer 範圍](#4-watch限縮-informer-範圍)
5. [rules：宣告指標](#5-rules宣告指標)
6. [Pod 層級與容器層級](#6-pod-層級與容器層級)
7. [以 labels 讀取特定 annotation／label 鍵](#7-以-labels-讀取特定-annotation-label-鍵)
8. [`expandLabels`：動態 label 名稱（KSM `kube_pod_labels` 風格）](#8-expandlabels動態-label-名稱ksm-kube_pod_labels-風格)
9. [從 v1（`relations`）遷移至 v2](#9-從-v1relations遷移至-v2)
10. [範例配方](#10-範例配方)
11. [疑難排解](#11-疑難排解)
12. [執行期內部行為](#12-執行期內部行為)
13. [結構與實作對照表](#13-結構與實作對照表)

---

## 1. 總覽與設計目標

### 1.1 設定必須表達的三件事

1. **Watch 拓樸** — 哪些物件會進 SharedInformer 快取（`watch`），以縮小對 apiserver 的 LIST/WATCH 量。
2. **指標契約** — 每條 `rules[]` 對應 **一個** Prometheus metric。固定 label 集合由 `labels` 宣告；可選的 **動態** label 由 `expandLabels` 在 scrape time 從 K8s map（如 `metadata.labels` / `metadata.annotations`）展開。
3. **取值語意** — 每個 label 如何從 anchor、`forEach` 的 `item`、或 owner 鏈上的角色（`ownerController`、`topController` 或具體 Kind）取出字串。

相關物件僅從 **informer 快取** 解析；exporter **不會**為走訪 owner 鏈而額外呼叫 API。

### 1.2 設計原則

| 原則 | 意義 |
|------|------|
| **宣告式** | 營運方描述指標與取值，設定內不含 Go 程式。 |
| **儘早失敗** | 結構問題在 `Validate` 攔截；**path 語法**在 collector 建構、`Compile` 時才解析，錯誤則啟動失敗。 |
| **每次 scrape 重新組裝** | 沒有任何 series 狀態跨 scrape 殘留，PromQL 上看到的就是 cache 當下的真相，避免「mutate 後沒清掉」這類 bug。 |
| **per-scrape label set 一致** | 一條 metric 在同一次 scrape 內，所有 series 共享同一組 label 名稱（`labels` 鍵 ∪ 各 series 的動態鍵），缺值補空字串。 |
| **per-pod 動態獨立** | Pod A 新增 label key 不會把 Pod B 的舊 series 寫死，但會在 union 中加上一欄（Pod B 該欄為空字串）。這是必要的 Prometheus per-scrape 對齊成本。 |
| **驗證分兩階** | `Validate` **不**解析 `path` 字串；`Compile` 才對每條 `path` 與 `forEach` 做 `parsePath`。 |

### 1.3 非目標

- **非**完整 JSONPath：僅支援下文 [path 語法](#path-syntax) 所述子集。
- 不再支援 v1 的 `relations` 別名；請改寫為直接的 `source: topController` / `source: ownerController` / Kind 名稱（見[第 9 節遷移指引](#9-從-v1relations遷移至-v2)）。
- worker 數、queue 深度、parent index 等 v1 自監控概念已停用。client QPS/burst 與監聽位址仍為 `cmd/main.go` CLI 旗標。

---

## 2. 設定生命週期

### 2.1 設定檔來源

程式僅讀取 **一份** YAML；預設路徑 `/etc/metadata-exporter/config.yaml`，可用 `-config` 覆寫（見 `cmd/main.go`）。

### 2.2 載入 → 解析 → 驗證

`config.Load(path)` 依序：

1. `os.ReadFile` 讀取檔案。
2. `sigs.k8s.io/yaml.Unmarshal` 填入 `*config.Config`。
3. `cfg.Validate()`：至少一條 rule、`metricPrefix` 與各 rule 名稱組成之 Prometheus metric 名合法且不重複；每條 rule 的 `anchor`、`labels`、`expandLabels` 與 `source`／`forEach` 搭配等。

任一步失敗則在建立 Kubernetes client 或 informer **之前**結束程式。

### 2.3 編譯規則（YAML 已通過驗證後）

`collector.New(cfg, ...)` 對每個 `cfg.Rules[i]` 呼叫 `Compile(rule)`：

- 將 `forEach`、各 `labels` 的 `path`、fallback 的 `path`、以及 `expandLabels` 的 `path` 編成內部 path AST（`pkg/collector/pathexpr.go` 的 `parsePath`）。
- `expandLabels` 的 `allow` / `deny` 轉成查表用的 set。
- 不再有別名解析步驟：`source` 直接記錄為 `anchor` / `item` / `ownerController` / `topController` / 某個 Kind 名。

因此 **`path` 打錯** 可能在 `Validate` 通過後，仍於 collector 初始化時失敗。

### 2.4 Scrape-time 求值（摘要）

每次 Prometheus 對 `/metrics` 發 GET：

1. Collector 取得排他鎖，避免兩條 scrape 互相影響統計。
2. 對每條 rule，從 informer cache 用 `ListAll(rule.Anchor)` 拉出所有 anchor 物件。
3. 對每個 anchor：
   - 透過 `Resolver` 走 `ownerReferences`，組出 chain（`anchor` / `ownerController` / `topController` / 具體 Kind 名）。
   - 把 anchor 與 chain 上的物件各自轉成 unstructured map。
   - `EvaluateForEach` 展開 `forEach`（沒有就視為單一空 item），每筆都產出一個 `pendingSeries`：
     - 固定 label：依 `Compile` 時 sort 過的順序求值，逐項試 `path`、`fallbacks`、`onMissing`。
     - 動態 label：對每條 `expandLabels` 求值；map 鍵經 [sanitize](#expandlabels-sanitize-規則) 並加上 prefix 後直接成為 Prometheus label name。
4. 把所有 `pendingSeries` 蒐集到的動態 label 名做集合聯集，固定順序（字典序）後組成本次 scrape 此 rule 的最終 label set。
5. 用一個 `prometheus.NewDesc` 建立 metric family，逐筆 `prometheus.NewConstMetric(desc, GaugeValue, 1, ...)` 送進 channel。
6. 自監控：`exporter_collect_total{rule,result}`、`exporter_collect_duration_seconds{rule}`、`exporter_anchor_count{rule,kind}` 一併更新。

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

設定檔僅支援本文件所述欄位；`watch.selectors`、`watch.kinds`、`watch.namespaces` 等舊鍵**不**在 `WatchScope` schema 內，出現時會與其它未知 YAML 鍵一併在反序列化時被忽略。請使用 `watch.resources[]`。

### 為何重要

- `labelSelector` 與 `fieldSelector` 會傳給 `LIST`／`WATCH`；apiserver 的 **watch cache** 會在事件下發給 client **之前**依述詞過濾，初始 LIST 與後續 watch 流量都會縮小。
- 縮小 watch 範圍同時也縮小每次 scrape 要走訪的 cache，scrape latency 與 cardinality 都跟著降低。
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

令 `R = len(watch.resources)`（若 `resources` 省略或空，程式會展開為**全部**支援 kind）。

| 模式 | 觸發條件 | 開啟的 watch 量級（概念） | 取捨 |
|------|----------|---------------------------|------|
| 叢集全域 | 各 `WatchResource` 為 `scope: Cluster`，或 namespaced 資源未設 `namespaces` | 每個列入的 kind 約一路 informer | 對大型叢集通常最省；可對每個 kind 用 `labelSelector`／`fieldSelector` 縮流。 |
| 每 namespace | `scope: Namespaced` 且 `namespaces` 非空 | 約 `len(namespaces) ×`（該列所涵蓋之 kind 數） | 隔離強；可搭配 `fieldSelector: metadata.name=...` 等縮到單一 Pod。 |
| 僅部分 Kind | `resources` 只列部分 kind | 路數隨列出的 kind 變少 | 降低多餘 GVR watch；有 `topController`／`ownerController` 但未 watch 父 kind 時 owner 鏈易 miss。 |

啟動日誌會印出 `watch mode = cluster-wide` 或 `per-namespace` 及 factory 數量。namespace 過濾發生在 client-go cache；叢集全域模式 **不會**因此線性增加「每次事件」的 CPU，主要影響的是對 apiserver 的 watch 扇出。

**設計說明**：`watch` 只決定 **快取內有什麼**；rule 仍可引用 `topController`／`ownerController`，但若 parent 被 selector 切掉或沒有 watch 該 parent Kind，解析會 miss——這是 `watch` 與 `rules` 之間的**設定耦合**；`topController` 未強制在 schema 內，但若 rule 用到了，collector 在啟動時會在缺 `ReplicaSet` / `Deployment` / `StatefulSet` / `DaemonSet` 任一者時打 **Warn** 日誌（可能 miss owner 鏈）。

---

## 5. rules：宣告指標

一條 rule 即一個 metric 宣告。

```yaml
rules:
  - name: "pod_info"       # 必填；metric 全名 = metricPrefix + name
    help: "..."            # 選填；Prometheus HELP
    anchor: Pod            # 必填；Pod|Deployment|StatefulSet|DaemonSet|ReplicaSet|Node
    forEach: "spec.containers[*]"   # 選填；展開為 N 筆 series
    labels:                          # 選填；至少 labels 或 expandLabels 二者擇一
      <label_name>:
        source: <source>             # 選填；預設 "anchor"
        path: "<jsonpath>"           # 必填
        fallbacks:                   # 選填；主 path miss 後依序嘗試
          - source: anchor
            path: "..."
        onMissing: ""                # 選填；預設 ""
    expandLabels:                    # 選填；動態 label 名稱
      - source: anchor               # 選填；預設 "anchor"
        path: "metadata.labels"      # 必填；解析結果須為 map
        prefix: "label_"             # 必填；以 "_" 結尾以避免邊界問題
        allow: ["app", "team"]       # 選填；只匯出 allow list 內的 key
        deny:  ["secret"]            # 選填；deny 永遠優先於 allow
        maxKeys: 0                   # 選填；0 = 不限；>0 時取字典序前 N 個
```

### `anchor`

`anchor` 同時決定 **哪些 informer cache 會被 scrape 走訪**，以及 **每筆輸出 series 的主體**。允許值：`Pod`、`Deployment`、`StatefulSet`、`DaemonSet`、`ReplicaSet`、`Node`。

### `forEach`

選填；為相對 anchor 的 path，評估結果為陣列時，**每個元素一筆 series**。常見用法：

- `spec.containers[*]` — 每個一般容器一筆
- `spec.initContainers[*]` — 每個 init 容器一筆
- `spec.ephemeralContainers[*]` — 每個 ephemeral 容器一筆

有 `forEach` 時，可用 `source: item` 指到目前迭代元素。`expandLabels` 也允許 `source: item`，可把每個容器自己的 metadata map 展平。

**實作**：未設 `forEach` 時，`EvaluateForEach` 仍回傳單元素（含 `nil` 的 slice），讓後續管線一致（見 `pkg/collector/evaluator.go`）。

### `source` 取值

| 值 | 意義 |
|----|------|
| `anchor`（預設） | anchor 物件本身。 |
| `item` | 目前 `forEach` 元素；**僅**在設了 `forEach` 時合法。 |
| `ownerController` | anchor 在 `ownerReferences` 上的直接 controller（若有）。 |
| `topController` | owner 鏈上最深、且為 `Deployment`／`StatefulSet`／`DaemonSet` 的祖先；若 anchor 本身就是這些 Kind，回傳 anchor 自己。 |
| `Pod`／`Deployment`／… | 沿 owner 鏈走訪時**第一個**該 Kind；若 anchor 即該 Kind 則直接回傳 anchor。 |

> 相關物件**僅**從 informer 快取取得；**不**為走訪 owner 鏈額外打 API。
> v1 的 `relations` 別名已移除；請直接寫 `source: topController` / `source: ownerController` / `source: Deployment` 等。

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

對每個 **固定** label：

1. 評估主要 `path`；空字串、缺鍵或求值錯誤視為 miss。
2. 依序嘗試 `fallbacks` 各項，取第一個非空結果。
3. 皆 miss 則用 `onMissing`（預設空字串）。

`fallbacks` 內每一項欄位與頂層 extract 相同，但 **不可**再嵌套 `fallbacks`。

`expandLabels` **不**支援 `fallbacks` 或 `onMissing`：對動態 label 而言「miss」就是 key 不在當下這個 anchor 的 map 中——其他有此 key 的 anchor 仍可貢獻 union，本 anchor 該欄位則為空字串（per-scrape 對齊規則）。

### 標籤名稱規則

- `labels:` 的鍵即 Prometheus 標籤名，須符合 `[a-zA-Z_][a-zA-Z0-9_]*`，且不得以 `__` 開頭。
- `expandLabels[].prefix` 同樣須能組成合法 Prometheus label name；不得以 `__` 開頭，且**不能**等同任一固定 label 名（避免歧義）。
- 同一次 scrape 內，一條 rule 的最終 label set 是「固定 labels ∪ 動態鍵聯集」。動態鍵跨 scrape 可能變化；同一條 metric 的 label name 集合只在 **同一次 scrape** 內保持一致（這是 Prometheus 對 metric family 的硬性要求）。

### 啟動時不變式（摘要）

- 至少一條 rule；每條需非空 `name`、支援的 `anchor`，且 `labels` 與 `expandLabels` 至少有其一。
- `metricPrefix + name` 全叢集設定內唯一，且符合 Prometheus metric 命名。
- 同一 rule 內 `labels` 的鍵不得重複（由 map 結構保證）；`expandLabels[].prefix` 不可與固定 label 名衝突。
- `expandLabels` 中 `source: item` 必須搭配 `forEach`；`maxKeys >= 0`。

---

## 6. Pod 層級與容器層級

以**兩條 rule**（不同 `name`、不同或省略 `forEach`）即可同時存在兩種粒度，彼此獨立。

| 目標粒度 | `anchor` | `forEach` | 結果基數 |
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

## 7. 以 labels 讀取特定 annotation／label 鍵

當要把 `metadata.annotations` 或 `metadata.labels` 裡**特定鍵**暴露成 Prometheus 標籤時，最直接的辦法是在 `labels:` 底下為每個鍵寫一條 `Extract`，並用 [path 語法](#path-syntax) 的**引號下標**指向該 K8s 鍵。

```yaml
rules:
  - name: "pod_info"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      controller_annotation_integration_test_controller_note:
        source: topController
        path: 'metadata.annotations["integration.test/controller-note"]'
      pod_label_app_kubernetes_io_name:
        path: 'metadata.labels["app.kubernetes.io/name"]'
```

Prometheus 標籤名須符合 `[a-zA-Z_][a-zA-Z0-9_]*` 且不得以 `__` 開頭；請在 YAML 裡**自行選擇**合法的 `labels` 鍵名（上例採描述性蛇形命名）。某條 `path` miss 時行為與其它 label 相同：走 `fallbacks` 與 `onMissing`，不會從 series 上移除該標籤。

> 若已知特定 key 是固定要看的維度，**強烈建議用 `labels`** 而非 `expandLabels`：
> 1. 固定 label 名穩定，PromQL 寫法不需考慮 union；
> 2. 不會被其他 anchor 的新 key 影響 metric family 結構，scrape diff 較小；
> 3. 可搭配 `fallbacks` / `onMissing` 等錯誤處理。
> `expandLabels` 適用於「事先不能列舉所有 key」的場景。

---

## 8. `expandLabels`：動態 label 名稱（KSM `kube_pod_labels` 風格）

當你想匯出 **整張 `metadata.labels` / `metadata.annotations`** 而不想為每個 key 各寫一行 `labels`，使用 `expandLabels`：

```yaml
rules:
  - name: "pod_meta"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
    expandLabels:
      - path: "metadata.labels"
        prefix: "label_"
      - path: "metadata.annotations"
        prefix: "annotation_"
        allow: ["argocd.argoproj.io/tracking-id"]
        maxKeys: 32
```

每筆 `expandLabels[]` 在 scrape time 解析 `path` 取得一個 `map[string]string`，套用 `allow`/`deny`/`maxKeys` 後，把每個 key 用[下文 sanitize 規則](#expandlabels-sanitize-規則)轉換並加上 `prefix`，做為動態 Prometheus label name；該 anchor 在輸出時即帶上對應 value。

### Per-scrape union 行為

> 重要：Prometheus 在「同一條 metric、同一次 scrape」內**要求所有 series 共享相同 label 名稱集合**。

因此 collector 在 emit 之前會：

1. 蒐集本次 scrape 內所有 anchor 對該 rule 產生的動態 key 集合（聯集）。
2. 用 `固定 labels ∪ 該聯集` 建立 `prometheus.NewDesc`。
3. 對每筆 series：擁有的動態 key 帶實際 value，缺的 key 帶空字串。

舉例：

| Pod | `metadata.labels` |
|-----|--------------------|
| `a` | `{app: shipping, team: payments, feature/flag: true}` |
| `b` | `{team: search}` |

scrape 輸出（簡化排序）：

```
custom_pod_meta{namespace="ns",pod="a",label_app="shipping",label_feature_flag="true",label_team="payments"} 1
custom_pod_meta{namespace="ns",pod="b",label_app="",label_feature_flag="",label_team="search"} 1
```

Pod `b` 的 `label_app` 是空字串，**因為 union 規則**——這是 Prometheus 強制的對齊代價，並非任何 Pod 「被改寫」。下一次 scrape 若 Pod `a` 移除了 `feature/flag`，`label_feature_flag` 會從整條 metric 的 label set 消失（直到下個 anchor 又出現它）。

> 這也代表：**新增/移除一個 K8s label 鍵，會在下一次 scrape 改變該 metric 的 label name 集合**。對 PromQL 而言，這等同於該維度的 series 被視作不同 series。如果你需要 PromQL 端維度穩定，請改用 `labels` 把該 key 釘成固定 label。

### Cardinality 控制

`expandLabels` 是高 cardinality 風險區，務必審慎使用：

- `allow`：白名單，只有列出的 K8s key 才會匯出。**強烈建議生產環境設定。**
- `deny`：黑名單，永遠優先於 `allow`（用來臨時屏蔽噪音 key）。
- `maxKeys`：每個 `expandLabels[]` 在每個 anchor 上最多產出多少動態 label。`0` 表示不限制；`>0` 時程式會把候選 key 字典序排序、保留前 N 個、其餘丟棄（行為穩定且可預期）。

> 經驗值：對 `metadata.annotations` 開放展平時，建議搭配 `allow` 列出所需 key 的白名單。若沒有把握列舉所有 key，先抓 `maxKeys: 32`，並監看 `scrape_duration_seconds`、Prometheus side 的 `prometheus_tsdb_storage_blocks_bytes` 等指標。

### `expandLabels` sanitize 規則

K8s map key 經以下變換成為 Prometheus label name 後綴：

- 任何不在 `[A-Za-z0-9_]` 範圍的字元一律替換為 `_`（含 `.`、`-`、`/`、空白）。
- 空字串會被視為 `_`（極不常見，但避免 prefix 之後仍違反 label name regex）。
- 結果與 `prefix` 串接後即為最終 Prometheus label name；`prefix` 必須以合法 label-name 起始字元開頭，慣例是 `<role>_`（如 `label_`、`annotation_`、`controller_label_`）。

範例：

| Map key | sanitized 後綴 | with `prefix: label_` |
|---------|----------------|------------------------|
| `app` | `app` | `label_app` |
| `app.kubernetes.io/name` | `app_kubernetes_io_name` | `label_app_kubernetes_io_name` |
| `with-dashes/and.dots` | `with_dashes_and_dots` | `label_with_dashes_and_dots` |
| `123` | `123` | `label_123` |

### 與 `labels` 共存

固定 `labels` 與 `expandLabels` 可在同一條 rule 同時使用：固定 label 在每筆 series 一定出現（按設定順序），動態 label 接在後面（依 union 字典序）。Prefix 與固定 label 名稱衝突會被 `Validate` 攔截。

### 何時不要用 `expandLabels`

- 你只關心 1–2 個固定 key：用 `labels` 寫死，更穩定也更便宜。
- 你的 K8s map 有大量隨機／使用者注入 key：先用 `allow` / `maxKeys` 嚴格限制；否則 cardinality 失控，影響整個 Prometheus。
- 你需要根據 anchor 的祖先 key 變化做 PromQL 比較：固定 label 才能保證 series 連續。

---

## 9. 從 v1（`relations`）遷移至 v2

v2 移除了 `relations` 別名機制以簡化心智模型。所有改寫都是純機械替換，不會改變 metric 形狀（label name 與值）。

| v1 寫法 | v2 改寫 |
|---------|---------|
| `relations: [{name: top, via: topController}]` 並在 label 用 `source: top` | 把該 label 直接寫 `source: topController`，整條 `relations` 區塊刪除 |
| `relations: [{name: owner, via: ownerController}]` + `source: owner` | `source: ownerController` |
| `relations: [{name: rs, via: ReplicaSet}]` + `source: rs` | `source: ReplicaSet`（kind 名直接合法） |

啟動時若仍寫 `relations:` 區塊，YAML 的未知欄位會被忽略；但以 `source: top` 等別名取值會在 `Validate` 失敗（`source "top" is not recognised`）。請依上表全部改寫。

整合測試 / 部署 manifest 內亦同步更新；可參考 `deploy/manifests.yaml` 與 `test/integration/e2e/config_yaml.go` 的最新示範。

---

## 10. 範例配方

### 10.1 Argo CD tracking id，並 fallback 到 Pod 本體

```yaml
rules:
  - name: "pod_argocd_info"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      app:
        source: topController
        path: 'metadata.annotations["argocd.argoproj.io/tracking-id"]'
        fallbacks:
          - source: anchor
            path: 'metadata.annotations["argocd.argoproj.io/tracking-id"]'
```

### 10.2 Helm release 名稱

```yaml
rules:
  - name: "pod_helm_info"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
      release:
        source: topController
        path: 'metadata.annotations["meta.helm.sh/release-name"]'
      chart:
        source: topController
        path: 'metadata.labels["helm.sh/chart"]'
```

### 10.3 KSM 風格 `kube_pod_labels`

```yaml
rules:
  - name: "pod_labels"
    anchor: Pod
    help: "Pod labels expanded into dynamic Prometheus labels."
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
    expandLabels:
      - path: "metadata.labels"
        prefix: "label_"
        # 強烈建議生產環境配 allow，限縮 cardinality。
        # allow: ["app.kubernetes.io/name", "team", "env"]
        maxKeys: 64
```

### 10.4 控制器層級的動態 annotations

```yaml
rules:
  - name: "pod_controller_annotations"
    anchor: Pod
    labels:
      namespace: { path: "metadata.namespace" }
      pod:       { path: "metadata.name" }
    expandLabels:
      - source: topController
        path: "metadata.annotations"
        prefix: "controller_annotation_"
        allow:
          - "argocd.argoproj.io/tracking-id"
          - "meta.helm.sh/release-name"
```

### 10.5 降低叢集負載

僅 watch 單一 namespace，並縮小 Pod：

```yaml
watch:
  resources:
    - kind: Pod
      scope: Namespaced
      namespaces: ["prod"]
      labelSelector: "app.kubernetes.io/part-of=payments"
    # 其他 kind 不列 = 不 watch；使用 topController 等時需自行權衡是否補上
```

Parent（`ReplicaSet` / `Deployment` 等）若也縮了 selector 或從 `resources` 省略，owner 鏈易 miss，請見第 4 節「Owner 鏈」風險；通常 parent 的選擇應**寬於** Pod 或一併把需要的 kind 納入 `resources`。

### 10.6 以 Deployment 為 anchor

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

## 11. 疑難排解

| 現象 | 可能原因 | 處理方式 |
|------|----------|----------|
| 標籤永遠空 | `path` 在該物件上不存在；或 `source` 在此 anchor 的 owner 鏈上取不到。 | 用 `kubectl -o jsonpath='{.…}'` 確認；必要時加 `fallbacks`。 |
| Static Pod 沒有 controller 類標籤 | Static Pod 無 owner references。 | 預期行為；可對 anchor 加 `fallbacks` 或設 `onMissing`。 |
| 啟動錯誤提到 `fieldSelector` | 欄位不在該資源白名單。 | 改為 `labelSelector` 或移除。 |
| 啟動錯誤提到 `source "top" is not recognised` | 仍在使用 v1 `relations` 別名。 | 依[第 9 節](#9-從-v1relations遷移至-v2)機械改寫；`top` → `topController` 等。 |
| `expandLabels` 輸出 cardinality 暴衝 | 沒設 `allow` 或 `maxKeys`，annotation/label key 過多／不可預期。 | 嚴格收斂 `allow`；用 `maxKeys` 上限保護；必要時改回固定 `labels`。 |
| 連續兩次 scrape 看到同一 anchor 的某 dynamic label 一下有一下沒有 | 該 key 不在所有 anchor 上出現，或 anchor 自己的 K8s metadata 在變動。 | 改用 `labels` 把該 key 釘為固定維度；或縮小 `expandLabels[].allow`。 |
| 警告「pod selector combined with stricter parent selector …」 | parent 被 filter 掉，owner 鏈斷裂。 | 放寬或移除 parent 的過窄 selector。 |
| 警告「rules reference ownerController/topController but not all parent kinds are watched」 | 規則使用 `topController`／`ownerController`，但 `watch.resources` 未納入典型 owner 鏈所要的 parent kind。 | 補上 `ReplicaSet` 等，或放寬 selector；屬可預期，指標中 parent 欄位可能空。 |
| `metrics-addr` 連不上 | 容器埠不符或程式在驗證階段即退出。 | 查日誌；readiness 通常打 `/healthz`。 |
| Scrape 變慢 | rule 數 × anchor 數 × `expandLabels` key 數 過大。 | 縮小 `watch.resources`／selector；收斂 `expandLabels` allow；觀測 `exporter_collect_duration_seconds`。 |

---

## 12. 執行期內部行為

以下行為**不在** YAML schema 內，但會影響對 apiserver 的負載與可觀測性。

### Informer 快取與 scrape time 求值

- 每個列在 `watch.resources` 的 Kind 都會建立 SharedInformer，將 LIST/WATCH 結果快取。
- `metadata-exporter` **不再** 以事件觸發 reconcile：cache 由 informer 自動更新，但 metric 完全在 `/metrics` 被 scrape 當下從 cache 重算。
- 因此 scrape latency ≈ `O(rules × anchors × (label evals + dynamic key sanitize))`。Cache 建立成本攤在 startup 與 watch 事件處理上，與 scrape 解耦。

### 自監控指標

| 名稱 | 型別 | 標籤 | 說明 |
|------|------|------|------|
| `exporter_collect_total` | counter | `rule, result` | 每條 rule 在 scrape 時的成功／錯誤次數。 |
| `exporter_collect_duration_seconds` | histogram | `rule` | 每條 rule 在單次 scrape 內 `Collect` 流程的耗時。 |
| `exporter_anchor_count` | gauge | `rule, kind` | 該 rule 上次 scrape 時看到的 anchor 物件數，用於 cardinality 估算與排錯。 |

> v1 的 `exporter_reconcile_*`、`exporter_parent_index_*`、`exporter_reconcile_queue_depth` 已停用（事件驅動架構移除）。儀表板需同步更新，建議改觀測 `exporter_collect_*` 與 `scrape_duration_seconds{job=...}`。

### 基準量測（scrape-time 成本）

`pkg/collector/collector_bench_test.go` 提供三組基準，量化 N anchors × K dynamic keys 對 `Collect()` 的影響：

- `BenchmarkCollect_FixedLabelsOnly`：無 `expandLabels`，做為下界。
- `BenchmarkCollect_ExpandLabels`：開啟 `expandLabels` 並調整 K（5 / 10 / 50）。
- `BenchmarkCollect_OwnerChain`：強制走 Pod → ReplicaSet → Deployment chain，模擬實務最常見的 `topController` 用法。

執行方式（單次跑完所有 case）：

```bash
make bench-collect
```

由於底層使用 `fake.Clientset`，得到的時間與 alloc 數**僅可作為相對比較**——不要拿這些絕對值去推測線上環境，但可以用來驗證一次配置調整或程式重構是否拉高了 scrape 成本。建議的 SLO 觀測點是 `histogram_quantile(0.95, sum(rate(exporter_collect_duration_seconds_bucket[5m])) by (le, rule))`：當 p95 開始接近 Prometheus 的 scrape timeout，先檢查 `expandLabels` cardinality 與 anchor 量，再考慮拆 rule 或縮小 watch 範圍。

### Apiserver 限速

`--kube-api-qps`（預設 20）與 `--kube-api-burst`（預設 40）設定 client-go token bucket；informers 同步完成後對外 REST 通常很少，但可限制啟動 LIST 等尖峰。

---

## 13. 結構與實作對照表

| 概念 | 主要程式位置 |
|------|----------------|
| 根 `Config`、`WatchScope`、`Rule`、`Extract`、`ExpandLabel` | `pkg/config/config.go`（`json` tag = YAML 鍵） |
| `Load`、`Validate`、`WatchScope.EffectiveKinds` | `pkg/config/config.go` |
| CLI `-config` 與 `collector.New` 銜接 | `cmd/main.go` |
| `Compile`、`CompiledRule`、`CompiledExpand`、固定 label 求值順序、map 展平 sanitize | `pkg/collector/evaluator.go` |
| Path 文法（`parsePath`、`evaluate`） | `pkg/collector/pathexpr.go` |
| Custom `prometheus.Collector`（`Describe` / `Collect`）、scrape-time 組裝、union label set | `pkg/collector/collector.go` |
| Owner 鏈解析（`Resolve`） | `pkg/collector/resolver.go` |
| Scoped informer 群（每 (kind, namespace) 一份 factory） | `pkg/collector/listers.go` |
| 整合測設定形狀 | `test/integration/e2e/config_yaml.go` |

---

*本文件合併原「設定參考」與「結構設計」敘述；若程式變更請以原始碼為準。*
