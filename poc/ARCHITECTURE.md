# POC 架構（route2a）

> 模組路徑：`poc/route2a`（獨立 Go module，`istio.io/istio` 與主 repo 隔離）。
> 使用方式見 [README.md](README.md)；完整系統設計見
> [docs/istio-virtualservice-routing-history-design.md](../docs/istio-virtualservice-routing-history-design.md)。

## 目的與範圍

驗證兩條查詢路徑在離線環境可正確運作：

| 模式 | 輸入 | 流程摘要 |
|------|------|----------|
| **config_only** | `host` + `path` | host → Gateway → translate → `router_check_tool` → cluster |
| **traffic_simulation** | `host` + `path` + `dst_ip` | IP 三跳 → 候選 Gateway → host 消歧 → CH `ScopedFor` → translate → tool → cluster |

**範圍內**：Gateway / VirtualService / Service（無 DestinationRule subset）；HTTP :80 ingress；
in-process istiod 翻譯 + Envoy `router_check_tool`；ClickHouse bitemporal 四表 + 3-hop。

**範圍外（POC 未做）**：真實 K8s watch/ingest、OTEL collector DNS、DR subset、EndpointSlice、
時間區間 `[t0,t1]` 查詢 API。

---

## 端到端管線

### config_only（記憶體或 ClickHouse `ScopedFor`）

```
host + path
   │  gwresolve：host → gateway（most-specific）
   ▼
gateway
   │  ScopedFor(gw) → Gateway + VS + Service
   │  translate：istiod ConfigGenerator → RouteConfiguration（rccache 可快取）
   ▼
RouteConfiguration
   │  router_check_tool：(host, path) → cluster
   ▼
outbound|PORT||SERVICE.NS.svc.cluster.local
```

### traffic_simulation（ClickHouse 全鏈，benchmark 預設）

```
host + path + dst_ip
   │  lookup：ClickHouse 3-hop → 候選 Gateway[]
   │  gwresolve.ResolveAmong(host, candidates)
   ▼
gateway
   │  scopedfetch：CH ScopedFor(gw, T)
   │  translate → RouteConfiguration
   ▼
router_check_tool → cluster
```

入口：`simulate.Engine.ResolveAll`（[`route2a/internal/simulate/simulate.go`](route2a/internal/simulate/simulate.go)）。

---

## 套件對照

| 套件 | 職責 |
|------|------|
| `internal/gwresolve` | `Resolve` / `ResolveAmong`：host → Gateway，萬用 host 消歧 |
| `internal/translate` | in-process istiod → `http.80` RouteConfiguration |
| `internal/matchcheck` | 驅動 `router_check_tool`（sentinel 技巧當解析器） |
| `internal/rccache` | per-gateway RC 快取（相依 epoch 失效） |
| `internal/simulate` | 串接 lookup → resolve → scopedfetch → translate → check |
| `internal/chstore` | ClickHouse schema、3-hop、`ScopedFor`、多版本 `AsOf(T)` |
| `internal/ingload` | 由 `scalegen` 產生語料並 stream 插入 CH |
| `internal/scalegen` | 600×100 語料 + by-construction oracle |
| `internal/report` | benchmark 各階段 p50/p99/mean |
| `cmd/ipflow` | 手動 CLI：`load` / `query` / `verify` |

---

## 引擎 2A 落地要點

1. **不用 `FakeDiscoveryServer`**：手工組最小 `model.Environment`，避免 goroutine leak。
2. **固定 `http.80`**：直接 `BuildHTTPRoutes`，略過 `BuildListeners`（僅 HTTP :80 ingress）。
3. **`router_check_tool` sentinel**：`validate.cluster_name` 設永不匹配值，`--details` 讀 `actual:` 當答案。
4. **native 優先**：`POC_ROUTERCHECK_BIN` 或 PATH；否則 docker（延遲含 ~200ms 啟動）。
5. **按 gateway 批次**：`ResolveAll` 每 gateway 一次 tool invocation。

cluster 名格式：`outbound|<port>||<fqdn>`（POC 無 subset 段）。

---

## ClickHouse 資料模型

四張 `ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)` 表。
開放版 `valid_to` 填遠未來 sentinel；查詢 `valid_from <= T AND T < valid_to`。

| 表 | 用途 |
|----|------|
| `service_versions` | Hop1：`ingress_ips` + `selector_kv`；translate backend 列 |
| `deploy_versions` | Hop2：`pod_labels_kv`（Deployment pod-template labels = L） |
| `gw_versions` | Hop3：`selector_kv`、`server_hosts`、`spec_json` |
| `vs_versions` | `bound_gateways`、`spec_json` |

ingest 期正規化 `selector_kv` / `pod_labels_kv` / `ingress_ips` 為排序 `Array(String)`；
`spec_json` = `protojson.Marshal(Spec)`。

### 三跳 SQL（概念）

```sql
-- Hop1: has(ingress_ips, ip) → svc selector
-- Hop2: hasAll(pod_labels_kv, svc_sel) → L
-- Hop3: hasAll(L, selector_kv) → 候選 Gateway
-- 每跳 AND valid_from <= T AND T < valid_to
```

POC 以 `scalegen.IPForHost` 模擬 DNS（`host → 10.0.x.y`），語料 IP↔gateway 為 1:1。

---

## Benchmark 管線階段

`bench-worst` / `bench-warm` 走完整 ClickHouse 鏈，報表六階段：

| stage | 粒度 | 說明 |
|-------|------|------|
| `lookup` | per query | IP → 候選 gateway（3-hop） |
| `resolve` | per query | `ResolveAmong` host 消歧 |
| `scopedfetch` | per gw（cache miss） | CH `ScopedFor` |
| `translate` | per gw（cache miss） | istiod 翻譯 |
| `check` | per gw batch | `router_check_tool` |
| `total` | per gw batch | 端到端 |

`sum(前五階段) ≈ total`。`bench-worst` = 單筆線上成本；`bench-warm` = 批量吞吐量（勿當單筆延遲）。

---

## 與完整設計的差異

| 面向 | 完整設計（docs） | POC 現況 |
|------|------------------|----------|
| 設定來源 | K8s informer → CH writer | `ingload` 程式產生語料 |
| 查詢 API | `cmd/query`、時間區間 | 測試 + `cmd/ipflow` CLI |
| DR / subset | 條件式版本化 + 套用 | 未實作 |
| DNS / OTEL | collector lookup processor | `IPForHost` 確定性模擬 |
| 主專案整合 | `pkg/store`、`pkg/resolve` | 尚未移植 |

---

## 當初目標 vs 實際交付

原計畫曾包含第二比對器（真 Envoy + access log）、YAML fixtures、`matchenvoy` 等——**未實作**。
實際交付聚焦：

- 引擎 2A + `scalegen` oracle（600 gw × 100 VS）
- ClickHouse 3-hop + CH-backed 翻譯
- benchmark 六階段計時與正確性驗證（0 mismatch）
