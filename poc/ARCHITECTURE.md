# POC 架構（route2a）

> 模組路徑：`poc/route2a`（獨立 Go module，`istio.io/istio` 與主 repo 隔離）。
> 使用方式見 [README.md](README.md)；完整系統設計見
> [docs/istio-virtualservice-routing-history-design.md](../docs/istio-virtualservice-routing-history-design.md)。

## 目的與範圍

驗證兩條查詢路徑在離線環境可正確運作：

| 模式 | 輸入 | 流程摘要 |
|------|------|----------|
| **config_only** | `host`（+ 選填 `path`/`port`；**無 `dst_ip` 即走此路徑**） | host → 全 gateway 消歧 → translate（依 port 選 RC）→ `router_check_tool` → cluster |
| **traffic_simulation** | `host` + `dst_ip`（+ 選填 `path`/`port`） | IP 三跳 → 候選 Gateway → host 消歧 → CH `ScopedFor` → translate（依 port 選 RC）→ tool → cluster |

查詢輸入：`host` 必要；`path` 預設 `/`；`dst_ip` 缺省→config_only；`port` 預設 `80`
（`80`→`http.80`、`443`→`https.443…` TLS 終結；其餘 port 無對應 server 則 miss）。

**範圍內**：Gateway / VirtualService / Service（無 DestinationRule subset）；HTTP :80 與
HTTPS :443（TLS 終結）ingress；in-process istiod 翻譯 + Envoy `router_check_tool`；
ClickHouse bitemporal 四表 + 3-hop；**單點 `AsOf(T)` 與時間區間 `[t0,t1)` 兩條查詢路徑**。

**後端**：僅 ClickHouse（production 用同一引擎，POC 程式可直接移植）。

**範圍外（POC 未做）**：真實 K8s watch/ingest、OTEL collector DNS、DR subset、EndpointSlice。

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

### 區間查詢 `[t0,t1)`（benchmark 預設）

```
host + path + dst_ip + [t0,t1)
   │  LoadTrafficWindow：一次 scoped Overlap（valid_from < t1 AND t0 < valid_to）
   │      → 相關 service/deploy/gw/vs 全版本（含物化 valid_to）載入記憶體
   ▼
memwindow：依 valid_from 切段（半開段）
   │  每段以 AsOf(段起點) 做 in-memory 3-hop / ScopedFor（不再打 DB）
   │  以設定內容 signature 去重：相同設定的段共用一次 translate + check
   ▼
每個相異設定：gwresolve.ResolveAmong → translate → router_check_tool
   ▼
[]VersionResolution{From, To, Gateway, Cluster}（相鄰同結果合併）
```

入口：`rangequery.Deps.Resolve`（[`route2a/internal/rangequery`](route2a/internal/rangequery)）。
關鍵：Overlap 直接吃**物化** `valid_to`（無 window/`lead` 推導），且整個區間只打**一次** DB。

---

## 套件對照

| 套件 | 職責 |
|------|------|
| `internal/gwresolve` | `Resolve` / `ResolveAmong`：host → Gateway，萬用 host 消歧 |
| `internal/translate` | in-process istiod → RouteConfiguration（依 `Port` 選 `http.<port>` 或 `https.<port>.<name>.<gw>.<ns>`） |
| `internal/matchcheck` | 驅動 `router_check_tool`（sentinel 技巧當解析器） |
| `internal/rccache` | per-gateway RC 快取（相依 epoch 失效） |
| `internal/simulate` | 串接 lookup → resolve → scopedfetch → translate → check |
| `internal/chstore` | ClickHouse schema、3-hop、`ScopedFor`、多版本 `AsOf(T)`、`LoadTrafficWindow` |
| `internal/memwindow` | 已載入 window 的記憶體切段 + in-memory 3-hop / `ScopedFor` |
| `internal/rangequery` | 區間查詢編排：load → 切段 → 每段 pipeline → `[]VersionResolution` |
| `internal/ingload` | 由 `scalegen` 產生語料並 stream 插入 CH |
| `internal/scalegen` | 600×100 語料 + by-construction oracle |
| `internal/report` | benchmark 各階段 p50/p99/mean |
| `cmd/ipflow` | 手動 CLI：`load` / `query`（`-from`/`-to` 走區間） / `verify` |

---

## 引擎 2A 落地要點

1. **不用 `FakeDiscoveryServer`**：手工組最小 `model.Environment`，避免 goroutine leak。
2. **依 port 選 RC**：只呼叫 `BuildHTTPRoutes`、略過 `BuildListeners`。RC 名依查詢 `port` 由
   Gateway CR 動態推導（`translate.routeConfigNameFor`，複製 istiod `gatewayRDSRouteName`）：
   HTTP→`http.<port>`；TLS 終結 HTTPS→`https.<port>.<portName>.<gwName>.<gwNamespace>`。
3. **`router_check_tool` sentinel**：`validate.cluster_name` 設永不匹配值，`--details` 讀 `actual:` 當答案。
4. **native 優先**：`POC_ROUTERCHECK_BIN` 或 PATH；否則 docker（延遲含 ~200ms 啟動）。
5. **按 gateway 批次**：`ResolveAll` 每 gateway 一次 tool invocation（單一 engine 單一 port）。

cluster 名格式：`outbound|<port>||<fqdn>`（此 `<port>` 是 backend service 埠，與查詢的 ingress
listener port 無關；POC 無 subset 段）。

### :443 passthrough 與 SNI（未做／失真註記）
- **TLS passthrough（`PASSTHROUGH`/`AUTO_PASSTHROUGH`）不支援**：istiod 的 `gatewayRDSRouteName`
  對 passthrough server 回傳 `""`（無 HTTP RDS route），選路改由 listener 層 SNI→TCP filter chain
  處理，需 `BuildListeners`；本管線只 `BuildHTTPRoutes`，故 passthrough 一律 miss。
- **SNI 失真**：SNI 只在 listener/filter-chain 層強制，我們略過該層。TLS 終結的 RC 內 vhost 仍以
  server.Hosts ∩ VS.hosts 收斂、以 `:authority` 比對，對「host+path→cluster」模擬無影響（假設
  SNI==Host）；唯一不模擬的是 SNI≠`:authority` 會被 Envoy 於 filter chain 拒絕的情境。

---

## ClickHouse 資料模型

四張 `ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)` 表。
`valid_to` 為**物化欄位**（與主專案 `pkg/store` 對齊），開放版填遠未來 sentinel。
單點查詢 `valid_from <= T AND T < valid_to`；區間 Overlap `valid_from < t1 AND t0 < valid_to`
——兩者都直接吃物化 `valid_to`，無 window/`lead` 推導。POC 每版只寫一次（`valid_to`
於 load 時算好），無同版重複列，故查詢**不需 `FINAL`**。

| 表 | 用途 |
|----|------|
| `service_versions` | Hop1：`ingress_ips` + `selector_kv`；backend Service 的 ports 存 `spec_json`（一列一個 Service，含全部 port） |
| `deploy_versions` | Hop2：`pod_labels_kv`（Deployment pod-template labels = L） |
| `gw_versions` | Hop3：`selector_kv`、`server_hosts`、`spec_json` |
| `vs_versions` | `bound_gateways`、`spec_json` |

ingest 期正規化 `selector_kv` / `pod_labels_kv` / `ingress_ips` 為排序 `Array(String)`；
gw/vs 的 `spec_json` = `protojson.Marshal(Spec)`；service 的 `spec_json` = `{"ports":[...]}`
（欄位對齊 corev1 `ServiceSpec.ports`，讀取端忽略多餘欄位，故亦可讀真實 exporter 的 `spec` blob）。

### 三跳 SQL（概念）

```sql
-- Hop1: has(ingress_ips, ip) → svc selector
-- Hop2: hasAll(pod_labels_kv, svc_sel) → L
-- Hop3: hasAll(L, selector_kv) → 候選 Gateway
-- 每跳 AND valid_from <= T AND T < valid_to
```

POC 以 `scalegen.IPForHost` 模擬 DNS（`host → 10.0.x.y`），語料 IP↔gateway 為 1:1。

**命名空間拓樸（對齊 production）**：Gateway CR 在 `istio-system`；VirtualService 與其路由的
backend Service 同 namespace（≠ gateway ns）。`ScopedFor` / `LoadTrafficWindow` 取 backend
Service 時，把 VS route 的 `destination.host` FQDN 解析成 `(name, namespace)`（Istio 衍生邏輯，
`store.ParseBackendHost`），以身分比對 `service_versions`，**不以 gateway namespace 為條件**——
如此才可移植到真實環境（FQDN↔身分的映射屬 reader 權責，exporter 只出原生 `namespace`/`name`）。

---

## Benchmark 管線階段

兩支 benchmark 走完整 ClickHouse 鏈，報表六階段：

| stage | 粒度 | 說明 |
|-------|------|------|
| `lookup` | per query | 區間：一次 `LoadTrafficWindow`（scoped Overlap）／單點：3-hop |
| `resolve` | per query | `ResolveAmong` host 消歧 |
| `scopedfetch` | per gw（cache miss） | `ScopedFor`（區間為 in-memory） |
| `translate` | per gw（cache miss） | istiod 翻譯 |
| `check` | per gw batch | `router_check_tool` |
| `total` | per gw batch | 端到端 |

- **`bench-worst`（區間預設）**：每筆 host+path 走 `rangequery` 區間路徑（一次 Overlap load + 切段 +
  每個**相異設定** translate/check）。成本隨區間內**相異設定數**放大——相同 spec 的版本升版會以
  content signature 去重（例：10 個內容相同的 VS 版本 → 只 translate/check 一次）。視窗預設涵蓋整條
  時間線，可用 `POC_BENCH_FROM` / `POC_BENCH_TO`（RFC3339）覆寫。這是單筆線上成本。
- **`bench-warm`（單點）**：暖快取批量吞吐量，維持單點 `now`（其重點是 per-gateway 批次攤提翻譯，
  與冷啟動、逐段的區間路徑本質不同）；勿當單筆延遲。

---

## 與完整設計的差異

| 面向 | 完整設計（docs） | POC 現況 |
|------|------------------|----------|
| 設定來源 | K8s informer → CH writer | `ingload` 程式產生語料 |
| 查詢 API | `cmd/query`、時間區間 | `cmd/ipflow`（單點 + `-from`/`-to` 區間）+ 測試 |
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
