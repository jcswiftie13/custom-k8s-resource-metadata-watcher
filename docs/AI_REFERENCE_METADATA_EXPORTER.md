# AI 協作參考：metadata-exporter（metadata-exporter / custom-k8s-resource-metadata-watcher）

> **用途**：後續對話可 `@docs/AI_REFERENCE_METADATA_EXPORTER.md` 或貼上本檔，讓 Agent 快速對齊專案語彙與檔案位置。  
> **人類可讀長文**：`docs/METADATA_EXPORTER_DEEP_DIVE.md`（同一輪對話的完整整理）。  
> **語言**：與使用者對話預設 **繁體中文**。

---

## 1. 一句話

Config-driven 的 K8s metadata → Prometheus（`_info` Gauge=1）；**SharedInformer/Lister 快取** + **ownerReferences 只走快取**；**MetadataSink** 可換後端；**ScopedInformers** 限 NS/selector。

---

## 2. 必記檔案地圖（讀碼順序）

| 路徑 | 內容 |
|------|------|
| `cmd/main.go` | kubeconfig/rest、QPS/Burst、`collector.New`、HTTP `/metrics` `/healthz` |
| `pkg/config/config.go` | YAML 模型、`Validate`、`ResolveRelation`、`WatchScope` |
| `pkg/collector/collector.go` | informer handlers、workqueue、`reconcileAnchor`、`updateDigestCache`、`parentIndex` |
| `pkg/collector/listers.go` | `ScopedInformers`、`allKinds`、factory 每 (ns, kind) 或 cluster-wide |
| `pkg/collector/resolver.go` | `ListerGetter`、`Resolve` owner 鏈、`Chain` |
| `pkg/collector/evaluator.go` | `Compile`、`EvaluateForEach`、`EvaluateLabel`、`ToUnstructured` + enrich TypeMeta |
| `pkg/collector/pathexpr.go` | `parsePath`、`parsedPath.evaluate`、`wildcard` = slice 展開 |
| `pkg/collector/flatten.go` | `compileFlatten`：base path + `field{name=key}` |
| `pkg/sink/interface.go` | `MetadataSink`、`RuleSchema` |
| `pkg/sink/prometheus.go` | `GaugeVec`、`ReplaceForAnchor` |
| `test/integration/e2e/config_yaml.go` | 整合測 exporter YAML（`it_` prefix、`top`→`topController`、`forEach`、`flatten`） |
| `docs/INTEGRATION_TESTS.md` | 情境 1–6 與觀測指標 |

---

## 3. 不變式 / 容易錯的點

- **`relations` 別名**：編譯後 `CompiledExtract.Source` 已是解析後字串（例：`top` → **`topController`**），`srcLookup` 用 **`maps[source]`**。
- **`forEach`  nil 行為**：`EvaluateForEach` 回傳 **`[]map{nil}`** 單元素；`item` 僅在有 `forEach` 的規則有意義。
- **`spec.containers[*]` segments**：`field spec` → `field containers` → **`wildcard`**；wildcard 對 **`[]interface{}`** 展開為 N 個 container map。
- **`flatten`**：`parsePath("metadata.annotations")` + 編譯期 append **`field{name: fullAnnotationKey}`**；避免在 path 字串內寫怪字元與引號。
- **唯讀**：業務邏輯不應對 apiserver PATCH/POST；整合測用 `rest_client_requests_total` 寫動詞為 0 守門。
- **負擔**：cluster-wide = **O(kinds)** watches；per-namespace = **N_ns × N_kinds**；冷路徑 parent 事件可 **namespace 掃描**。

---

## 4. 整合測試 ↔ 測試函式（速查）

| 情境 | `test/integration/e2e` |
|------|---------------------------|
| 1 cluster-wide watch | `TestTopology_ClusterWide` |
| 2 per-namespace | `TestTopology_PerNamespace` |
| 3 idle | `TestTopology_IdleStable` |
| 4 burst dedup | `TestBurden_BurstDedup` |
| 5 parent via index | `TestBurden_ParentEventViaIndex` |
| 6 correctness | `TestCorrectness_FixtureFlow` |

---

## 5. 給未來 Agent 的接續指令（可複製）

```
請先閱讀 docs/AI_REFERENCE_METADATA_EXPORTER.md 與（若需敘述細節）docs/METADATA_EXPORTER_DEEP_DIVE.md，
再回答本 repo 關於 metadata-exporter、collector、evaluator、pathexpr、整合測試的問題；
回答時用繁體中文，並用 codebase 引用格式標註檔案與行號。
```

---

## 6. 版本註記

- 整理日期脈絡：對話發生於 **2026-04** 左右；若程式與本參考衝突，**以原始碼為準**。
