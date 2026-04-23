# metadata-exporter 深度說明（對話整理）

本文整理自專案說明與程式碼走讀，涵蓋：功能與介面、架構與各 package、降低 apiserver 負擔、整合測試情境流程、情境 6 的 label 解析鏈、以及 `pathexpr` 的 `parsedPath` / `[*]` 與 `flatten` 的 segment 鏈。

---

## 1. 專案做什麼（對照 README）

- **Config-driven**：以 YAML 宣告 Prometheus 指標；用類 kubectl 的 JSONPath 子集取值。
- **Zero extra API calls（就 owner 鏈而言）**：`ownerReferences` 只靠 **SharedInformer / Lister 快取** 解析，不為每筆 reconcile 額外 `GET` API。
- **Pluggable sink**：`MetadataSink` 介面；內建 Prometheus（`_info` 風格 Gauge=1）。
- **Scoped informers**：可限制 namespace，並對各 kind 套用 label/field selector，在 apiserver 端過濾。

入口：`cmd/main.go`（設定、`rest.Config` QPS/Burst、Kubernetes client、`collector.New`、HTTP `/metrics` 與 `/healthz`）。

---

## 2. 介面

### 2.1 使用者定義「如何取得 metadata」

並非單一 Go `interface`，而是 **YAML 規則模型**（`pkg/config`）：

- **`Rule`**：`anchor`（Pod / Deployment / StatefulSet / DaemonSet / ReplicaSet）、可選 **`forEach`**（JSONPath）、**`labels`**（標籤 → `Extract`：source + path + fallbacks）、**`relations`**（別名，例如 `top` → `topController` 或某 Kind）、**`flatten`**（從 map 路徑依 allow-list keys 展開成多個 Prometheus label）。
- **取值**：`pkg/collector/evaluator.go` 將規則 **編譯**為 `CompiledRule`（`Compile`），執行時對 unstructured map 跑已編譯 path；`pkg/collector/pathexpr.go` 實作 path 語法與 `evaluate`。

### 2.2 與 Kubernetes API Server 連接

- **`cmd/main.go`**：`buildRestConfig`（`kubeconfig` / in-cluster / default loading rules）；`restCfg.QPS`、`Burst` 對應 `--kube-api-qps`、`--kube-api-burst`；`kubernetes.NewForConfig`。
- **`pkg/collector/listers.go`**：`ScopedInformers` 使用 **`kubernetes.Interface`** 建立 **SharedInformerFactory**（可 `WithNamespace`、`WithTweakListOptions`）；讀取物件走 **Lister.Get/List（快取）**，非每事件打 REST。

### 2.3 輸出介面 `MetadataSink`

定義於 `pkg/sink/interface.go`：

- `RegisterRule(RuleSchema)`：啟動前註冊固定 label 集合。
- `Upsert` / `Delete` / `ReplaceForAnchor`：並發安全；`ReplaceForAnchor` 以 anchor 為單位原子替換 series，刪除 stale。

Prometheus 實作：`pkg/sink/prometheus.go`（`GaugeVec`、值固定為 1）。

---

## 3. 程式架構（資料流）

1. **載入設定**：`config.Load` → `Validate`。
2. **建 Collector**：`collector.New` → `NewScopedInformers`、`NewResolver(infs)`、`NewEvaluator`、編譯 rules、`sink.RegisterRule`、建立 rate-limited **workqueue**、**parentIndex**、**updateDigestCache**、自監控 metrics。
3. **Informer 事件**：對 anchor kinds 註冊 **anchor handler**；對「會被規則讀取」的 parent kinds 註冊 **parent handler**（見 `collectReadKinds`）。
4. **入隊**：anchor 變更直接 `enqueueObject`；parent 變更優先 **`parents.AnchorsFor(UID)`** 命中則只 enqueue 相關 anchor，否則 **同 namespace 掃描**（冷路徑）。
5. **Worker**：`processNext` → `reconcileRef` → **Lister.Get** anchor → `resolver.Resolve` 建 **Chain** → `evaluator` 產 labels → **`sink.ReplaceForAnchor`**。
6. **HTTP**：`/metrics` 暴露 registry；可選 client-go REST metrics（整合測情境 4 用）。

---

## 4. 各 package 職責

| 路徑 | 職責 |
|------|------|
| `cmd/` | 進入點、REST 設定、Prometheus registry、HTTP、啟動 Collector。 |
| `pkg/config/` | YAML 模型、驗證、`MetricName`、`ResolveRelation`。 |
| `pkg/sink/` | `MetadataSink` 與 Prometheus 實作。 |
| `pkg/collector/` | `ScopedInformers`、`Resolver`（`ListerGetter`）、`Evaluator`/`Compile`、`pathexpr`、`flatten`、`Collector`（handlers、queue、reconcile、`parentIndex`、`updateDigestCache`）、自監控 metrics。 |
| `deploy/` | 範例部署。 |
| `docs/` | 設定與整合測試文件。 |
| `test/integration/` | Kind、e2e Go harness、manifest overlays。 |

---

## 5. 如何降低 Kubernetes API Server 負擔

1. **SharedInformer + Lister**：同步與讀取走快取；**resolver 不發 API**（`resolver.go` 註解與 `ListerGetter`）。
2. **Scoped watches**：`watch.namespaces` 與 **per-kind selectors**（`WithTweakListOptions`）縮小 list/watch 物件集合。
3. **Client QPS/Burst**：限制 client-go 對 apiserver 的請求速率。
4. **Workqueue**：同一 `anchorRef` 合併、rate limit，burst 更新不線性放大 reconcile。
5. **Update digest filter**：對 metadata 相關欄位 digest（generation、labels、annotations）；無實質變更可不入隊；Delete 時 `Forget` UID。
6. **Parent reverse index**：多數 parent 事件只 requeue **曾記錄依賴該 parent UID 的 anchor**；避免每次 Deployment 變更掃整個 namespace 的 Pod（熱路 vs fallback）。

---

## 6. 整合測試（對照 `docs/INTEGRATION_TESTS.md` 與 `test/integration/e2e`）

觀測面：apiserver `/metrics`（`apiserver_longrunning_requests` WATCH）、exporter `/metrics`（`exporter_*`、`rest_client_requests_total`）、exporter logs。

| 情境 | 測試函式 | 程式重點 |
|------|----------|----------|
| 1 Cluster-wide O(kinds) | `TestTopology_ClusterWide` | `watch.namespaces` 空 → `ScopedInformers` 每 kind 一個 cluster-wide factory；WATCH 增量每 resource 為 1。 |
| 2 Per-namespace N×K | `TestTopology_PerNamespace` | 每 (ns, kind) 一 factory；未列 namespace 的 Pod 不會出現在 `/metrics`。 |
| 3 Idle 穩定 | `TestTopology_IdleStable` | `resyncPeriod=0`；無事件則 queue 空、`reconcile_total` 不增、WATCH 計數不變。 |
| 4 Burst 去重 | `TestBurden_BurstDedup` | workqueue + update filter；parent index hit；**寫入動詞 REST 為 0**（唯讀）。 |
| 5 Parent 經索引 | `TestBurden_ParentEventViaIndex` | Deployment patch → parent handler → index hit → Pod reconcile → label 更新。 |
| 6 正確性 | `TestCorrectness_FixtureFlow` | `it_pod_info` / `it_pod_container_info`；刪除後 series 清除、`parent_index_size` 不惡化。 |

整合測設定來源：`test/integration/e2e/config_yaml.go`（`metricPrefix: it_`、`anchor: Pod`、`relations.top` → `topController`、`forEach: spec.containers[*]`、flatten annotation key 等）。

---

## 7. 情境 6：`reconcileAnchor` 與 `evaluator` 的 label 鏈

### 7.1 呼叫順序（`pkg/collector/collector.go`）

- `chain := resolver.Resolve(obj)` → 產生含 `anchor`、`Pod`、`ReplicaSet`、`Deployment`、`topController`、`ownerController` 等鍵的 **Chain**。
- `anchorMap := ToUnstructured(anchor)`；對 `chain` 每個值 `ToUnstructured` → **`maps[source]`**。
- `parents.Record(ref, parentUIDs)`（寫 sink 前建立反向索引）。
- 對每條 `CompiledRule`：`items := EvaluateForEach(cr, anchorMap)`；對每個 `item` 與每個 `CompiledLabel`：`EvaluateLabel(..., srcLookup)`；`srcLookup("item")` 回傳當前 forEach 元素，否則 **`maps[source]`**。
- `buildSeriesKey` + `sink.ReplaceForAnchor`。

### 7.2 `Compile`（`pkg/collector/evaluator.go` + `flatten.go`）

- 有 **`forEach`** 則 `parsePath` → `cr.ForEach`。
- 每個 `labels` 條目 → `compileExtract`（**`rule.ResolveRelation`** 寫入 `CompiledExtract.Source`，故 `source: top` 變 **`topController`**）。
- **`flatten`**：`compileFlatten` 對 `path` 做 `parsePath`，再對每個 key **append** `pathSegment{kind:"field", name:key}`，合併進 `cr.Labels` 後 **`reorderLabels`**。

### 7.3 `ToUnstructured` 與 `kind`

`enrichTypeMetaFromScheme`：typed informer 物件常缺 embedded TypeMeta，從 scheme 補 **`kind` / `apiVersion`**，使 `path: kind` 在 topController（Deployment）上可靠。

### 7.4 `it_pod_info`（無 `forEach`）

- `EvaluateForEach`：`cr.ForEach == nil` → **`[]map{nil}`**，僅一輪；無 `source: item` 的 label。
- `namespace` / `pod` / `phase`：根為 **`anchor`**（Pod map）。
- `controller_kind` / `controller_name` 與 flatten 出的 `controller_annotation_*`：根為 **`topController`**（Deployment map）。

### 7.5 `it_pod_container_info`（`forEach: spec.containers[*]`）

- `EvaluateForEach`：`cr.ForEach.evaluate(anchor)` → 見下一節 **pathexpr**；得到多個 **container map** 作為 `item`。
- 每輪：`namespace`/`pod` 從 **anchor**；`container`/`image` 從 **`item`**；`controller_name` 從 **`topController`**。

### 7.6 刪除 Deployment

主要走 **Informer Delete + `ReplaceForAnchor(..., nil)`** 與 `parents.Forget`，非再跑一輪 evaluator 產新指標。

---

## 8. `pathexpr.go`：`parsedPath`、segment 鏈、`[*]` 展開

### 8.1 結構

- **`pathSegment`**：`kind` ∈ `field` | `index` | `wildcard`；`field`/`index` 使用 `name` 或 `index`。
- **`parsedPath`**：`segments []pathSegment`。
- **`parsePath`**：支援 `.ident`、`["*"]` / `[0]` / `['quoted']`；可選去掉 `{...}`、`$.` 前綴。
- **`evaluate`**：維護 **frontier**；每步對 frontier 中每個 `cur` 呼叫 `applySegment`，合併為下一 frontier。

### 8.2 `forEach`：`spec.containers[*]`

**Segments：**

1. `field` `spec`
2. `field` `containers`
3. `wildcard`（`[*]`）

**展開語意（`applySegment` wildcard）：**

- 若 `cur` 為 **`[]interface{}`**：回傳 **所有元素**（slice 展開）→ 每個 container 一個 frontier 節點。
- 若 `cur` 為 **map**：回傳所有 value（**順序不固定**）；一般不適合當 `containers` 的語意。

### 8.3 `flatten`：`metadata.annotations` + key

- `parsePath("metadata.annotations")` → `[field metadata, field annotations]`。
- 對 key `integration.test/controller-note` **編譯期 append**：`[field metadata, field annotations, field integration.test/controller-note]`。
- **runtime** `evaluate(DeploymentRoot)`：等同 `metadata.annotations["integration.test/controller-note"]` 的取值效果；Prometheus **label 名稱**另由 `namePrefix + SanitizeLabelName(key)` 決定（`pkg/config`）。

### 8.4 其它常見 `labels.path`（同套 parser）

- `metadata.namespace`：`metadata` → `namespace`。
- `status.phase`：`status` → `phase`。
- `kind`：單段 `field kind`（根為 topController map）。
- `metadata.annotations["key"]`：兩段 field + 一段由 **quoted subscript** 編譯成的 `field{name=key}`，與 flatten 第三段同形。

### 8.5 `tryExtract` 與多值

`pathexpr.evaluate` 回傳 `[]interface{}`；`evaluator.tryExtract` 取第一個 **stringify 後非空** 的值。

---

## 9. 延伸閱讀（檔案索引）

| 主題 | 檔案 |
|------|------|
| 專案概覽 | `README.md` |
| 設定欄位 | `docs/CONFIG.md` |
| 整合測設計 | `docs/INTEGRATION_TESTS.md` |
| Collector 核心 | `pkg/collector/collector.go` |
| Informer 範圍 | `pkg/collector/listers.go` |
| Owner 鏈 | `pkg/collector/resolver.go` |
| 編譯與評估 | `pkg/collector/evaluator.go` |
| Path 語法 | `pkg/collector/pathexpr.go` |
| Flatten 編譯 | `pkg/collector/flatten.go` |
| Sink | `pkg/sink/interface.go`, `pkg/sink/prometheus.go` |
| 設定模型 | `pkg/config/config.go` |
| 整合測 e2e | `test/integration/e2e/*.go` |

---

*本文件為對話整理之技術備忘，非發行版號文件；若程式變更請以原始碼為準。*
