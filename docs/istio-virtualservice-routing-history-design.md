# Istio 路由回溯與 Ingress 流量 Gateway 解析 — 完整流程設計

> 狀態：設計 + POC。涵蓋 **（1）VirtualService 路由回溯查詢**（host+path+時間 → 各版本 destination）
> 與 **（2）Ingress 流量視角**（OTEL span / DNS / IP → 真正命中的 Gateway）。
>
> **架構定案**：ClickHouse 版本化 store（interval 模型、事件級精確）+ 引擎 2A（in-process istiod 翻譯
>
> - Envoy `router_check_tool`）+ ingress 查詢走 **query-time 三跳** `IP→Gateway`。
>
> **POC（**`poc/route2a`**）已驗證**：引擎 2A、ClickHouse 四表、3-hop、`ResolveAmong`、CH-backed `ScopedFor`。
> 細節見 [poc/ARCHITECTURE.md](../poc/ARCHITECTURE.md)；使用方式見 [poc/README.md](../poc/README.md)。
>
> **主專案尚未實作**：watch/ingest writer、時間區間查詢 API（`cmd/query`）、DR subset / EndpointSlice 層。

---



## 1. Context（為什麼要做這件事）

**核心需求**：給定 VirtualService 的 **host**、**path** 與**時間區間**（如今天 11:00–12:00），查出該區間內
符合的路由規則**依序**解析到哪些 destination（K8s Service + namespace + port）。若設定在區間內被改過，要
**回傳每個版本**；destination 要**完整解析到 Service+port**（含 DestinationRule subset）。
使用情境 = **歷史/取證查詢**（低 QPS，精準還原當時設定最重要）。

**兩種查詢語意（並存）**：


| 模式                     | 流程                                                      | 用途                |
| ---------------------- | ------------------------------------------------------- | ----------------- |
| **config_only**        | `host` → Gateway 反查 → translate → `router_check_tool`   | 設定稽核、「誰接受這個 host」 |
| **traffic_simulation** | `host` + `dst_ip` → 三跳 → 候選內 host 反查 → translate → tool | 模擬真實流量落點          |


兩者結果可能分歧（host 設定 match 某 Gateway，但 DNS/IP 指向另一顆 ingress）。API 應標明模式，或同時
回傳並標記 mismatch。

**現況起點**：本專案原是**通用 K8s metadata → Prometheus** `_info` **gauge exporter**——scrape-time 重建、
只反映當下，不保留歷史、無 Istio 語意。本設計在其上新增「事件驅動版本化 ingest」與「路由解析引擎」，
與既有 exporter **解耦**。

---



## 2. 為何不用 TSDB metric 當真實來源

路由是有序、巢狀、關聯式結構；Prometheus flat label 會遺失 `http[]` 順序（first-match-wins）、
match 細節與 route→destination 樹。path/host 當 label 造成高基數；prefix/exact/regex 比對無法在 PromQL
執行；從碎裂 series 回溯「每個版本」的完整有序設定極為脆弱。DestinationRule subset 與完整 spec 也無法
可靠塞進 label。

> Metric/TSDB 適合低基數可觀測性；**精準歷史回溯**需要 **版本化設定快照 + 解析引擎**。ingress `IP→Gateway`
> 同樣不應塞進 Prometheus label。

---



## 3. 架構定案


| 面向         | 定案                                                                   |
| ---------- | -------------------------------------------------------------------- |
| 設定儲存       | **ClickHouse**，interval 模型（`valid_from` / `valid_to`）                |
| 時間精度       | **事件級**（informer 事件寫入，不漏短命版本）                                        |
| Join 原語    | ingest 期物化 `Array(String)`；查詢用 `has` / `hasAll`                      |
| 去重         | `ReplacingMergeTree(ingest_seq)` + 查詢端 `FINAL` / `argMax`            |
| 比對引擎       | **引擎 2A**：in-process istiod `ConfigGenerator` + `router_check_tool`  |
| 查詢視角       | **Ingress Gateway**（南北向）；不做 Sidecar mesh 視角                          |
| IP→Gateway | **query-time 三跳**（Service → Deployment → Gateway），per-resource store |
| 與 exporter | TSDB export **保留**；歷史走 opt-in `history:` ingest 路徑                   |


```mermaid
graph TD
    K8s[K8s API watch] --> CH[(ClickHouse 版本化 store)]
    Q[查詢 host + path + 時間] --> M{模式}
    M -->|traffic_simulation| IP[dst_ip → 三跳 → 候選 Gateway]
    M -->|config_only| GW[host → Gateway 反查]
    IP --> GW2[候選內 most-specific host 消歧]
    GW --> SF[store.AsOf T → scoped 設定]
    GW2 --> SF
    SF --> TR[in-process istiod → RouteConfiguration]
    TR --> RC[router_check_tool → cluster]
    RC --> OUT[Service + ns + port + subset]
    CH --> SF
    CH --> IP
```



---



## 4. 資料流

**關鍵觀念**：VS/Gateway/DR/Service/Deployment 在 API server 可被 watch；Envoy `RouteConfiguration` 由
istiod 對每個 proxy 現算，故查詢端走 in-process 翻譯（引擎 2A）。

### 4.1 擷取（watch → ClickHouse）

```mermaid
graph TD
    A[K8s API server] -->|LIST + WATCH| B[Informer cache]
    B --> C{Add/Update/Delete}
    C -->|Add| D[insert 新版 valid_from=事件時間]
    C -->|Update| E[收前版 valid_to + insert 新版]
    C -->|Delete| F[收前版 valid_to]
    D --> G[(ClickHouse)]
    E --> G
    F --> G
    C -.->|resync| H{spec-hash 變了?}
    H -->|否| I[丟棄]
    H -->|是| E
```



**擷取要點**：

- 初始 LIST 的 `valid_from` 用 watcher 啟動時間；真實建立時間讀 `creationTimestamp`。
- **去重用 spec-hash**（非僅 resourceVersion）。**例外**：ingress Service 的 `status.loadBalancer` IP
變更須開新版本（IP 在 `spec.externalIPs` 則 spec-hash 已涵蓋）。
- 版本身分用 **resourceVersion**（比 generation 抗撞）；可加 `metadata.uid`。
- VS `spec.gateways` 可跨 namespace；watch scope 須含被引用的 Gateway ns。

**watch GVR**：`VirtualService`、`Gateway`、`DestinationRule`、`Service`、ingress `Deployment`、
`EndpointSlice`（選配）。

**traffic_simulation ingest 補充**：OTEL span 優先用既有連線 IP（`server.address` / `net.peer.ip`）；
缺 IP 才 DNS lookup。span 固化 IP 比即時 DNS 更適合 `AsOf(T)` 回放。

### 4.2 查詢（store → 解析）

```mermaid
graph TD
    Q[host + path + t0,t1<br/>選配 dst_ip] --> M{模式}
    M -->|traffic_simulation| P[dst_ip → 三跳 → 候選 Gateway]
    M -->|config_only| S2
    P --> S2[候選內 host 反查 Gateway.servers hosts]
    S2 --> T1[store.AsOf T 取 VS+DR+Gateway+Service 快照]
    T1 --> T2[in-process istiod → RouteConfiguration]
    T2 --> U[router_check_tool → cluster]
    U --> W[cluster → Service+ns+port+subset]
    W --> X[回傳 per-version 結果]
```



---



## 5. 元件設計

分三塊：**事件擷取**、**查詢引擎**、**（並存）Prometheus export**。

### A. 事件擷取 → ClickHouse temporal store

- **重用** `pkg/collector/listers.go` 的 `ScopedInformers` 與 `pkg/config/config.go` 的 `WatchResource`。
- **新增** informer Add/Update/Delete handler，每事件寫版本記錄：
  ```
  { gvk, namespace, name, uid, resourceVersion, generation,
    valid_from, valid_to(sentinel if open),
    spec_hash, object_json,
    ingress_ips[], selector_kv[], pod_labels_kv[], server_hosts[] }
  ```



#### 資源分層


| 資源                      | 儲存                                | 原因                      |
| ----------------------- | --------------------------------- | ----------------------- |
| **VirtualService**      | CH 全 spec                         | `http[]` 有序、path 比對     |
| **Gateway**             | CH + `selector_kv`/`server_hosts` | host 綁定、IP→Gateway join |
| **DestinationRule**     | CH（條件式）                           | subset→labels           |
| **Service（ingress LB）** | CH + `ingress_ips`/`selector_kv`  | destination + Hop1      |
| **ingress Deployment**  | CH `pod_labels_kv`                | Hop2 的 L                |
| **EndpointSlice**       | 當下 / best-effort                  | 高易變                     |




#### ClickHouse 儲存模型

`Store` interface：`Put` / `SetValidTo` / `AsOf(T)` / `Overlap(t0,t1)`。

- **interval 模型**：`valid_from <= T AND T < valid_to`；open 版 `valid_to` = 遠未來 sentinel。
- **ingest 期物化** join 欄為排序 `Array(String)`。
- **子集** = `hasAll(set, subset)`。
- **去重** = `ReplacingMergeTree(ingest_seq)` + `FINAL` / `argMax`。

**DDL（ingress 相關示意；VS/DR 同模式加** `spec` **欄）**：

```sql
CREATE TABLE svc_versions (
  namespace   LowCardinality(String),
  name        String,
  valid_from  DateTime64(3),
  valid_to    DateTime64(3),
  ingress_ips Array(String),
  selector_kv Array(String),
  ingest_seq  UInt64,
  INDEX idx_ips ingress_ips TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq)
ORDER BY (namespace, name, valid_from);

CREATE TABLE deploy_versions (
  namespace LowCardinality(String), name String,
  valid_from DateTime64(3), valid_to DateTime64(3),
  pod_labels_kv Array(String),
  ingest_seq UInt64
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);

CREATE TABLE gw_versions (
  namespace LowCardinality(String), name String,
  valid_from DateTime64(3), valid_to DateTime64(3),
  selector_kv Array(String),
  server_hosts Array(String),
  ingest_seq UInt64,
  INDEX idx_sel selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);
```



#### Config（純加法，**已實作**）

每個資源可**宣告要寫入哪些 ClickHouse 欄位、型別、來源 json path**（像 tsdb rule），並可對 watch 回來的資料
做 **client 端 filter（支援 regex）**，符合才寫入。完整語法見 [CONFIG.md §14](CONFIG.md)；範例見
`examples/history-clickhouse.yaml`。

```yaml
watch:
  resources:
    - name: VirtualService
      apiVersion: networking.istio.io/v1beta1
      kind: VirtualService
      resource: virtualservices
      scope: Namespaced
    # ... Gateway, DestinationRule, Service, EndpointSlice, Deployment
rules: [ ... ]
history:
  enabled: true
  store:
    type: clickhouse
    dsn: "clickhouse://user:pass@host:9000/routing"
    createSchema: true          # dev；prod 預設 false → 只驗證 schema、drift 即 fail
  resources:
    - kind: VirtualService
      table: vs_versions
      columns:
        - { name: spec_json, type: String, path: "spec", encode: json }
        - { name: hosts, type: "Array(String)", path: "spec.hosts[*]" }
        - { name: bound_gateways, type: "Array(String)", path: "spec.gateways[*]", index: bloom_filter }
      filters:
        - { path: "metadata.namespace", op: regex, value: "^(prod|staging)-" }
```

**Schema 由 config 定義**（json path 值無型別，型別只能由 config 指定）；`createSchema` 開關控制 dev 自動建表 vs
prod 驗證-only。**版本化為 append-only**：每事件一到兩筆 `INSERT`（`valid_from`、`valid_to`、`ingest_seq`），
`valid_to` 於寫入端物化——Update 收前版 `valid_to` + insert 新版，Delete 只收前版 `valid_to`（無 tombstone），
由 `ReplacingMergeTree(ingest_seq)` 折疊收尾列。落地於 `pkg/config`（設定）、`pkg/history`（filter + 事件
ingest）、`pkg/store`（ClickHouse writer + DDL）。



### B. 查詢／路由解析引擎

輸入：`host`、`path`、（選配 `dst_ip`）、時間區間 `[t0,t1]`。Ingress gateway 視角。

**通用流程**：

1. 撈出與 `[t0,t1]` 重疊的版本，依 `valid_from` 切段。
2. 收斂候選 Gateway/VS（見下）。
3. 引擎 2A：path → route → cluster。
4. cluster 名解析 Service+ns+port；套用 DR subset；選配 EndpointSlice。
5. 回傳 per-version 結果。



#### 視角收斂

```
1. （traffic_simulation）dst_ip → 三跳候選 Gateway；（config_only）跳過
2. Gateway.servers[].hosts 命中 host → 候選（traffic 在步驟 1 候選內找）
3. VS：spec.hosts 命中 且 spec.gateways 含候選 Gateway（省略 gateways = mesh → 排除 ingress）
4. VS http[] 依序比對 path → destination
```



#### traffic_simulation：IP → Gateway

`ingressgateway workload → Gateway CR` **無 back-reference**；靠 `Gateway.spec.selector ⊆ ingress pod labels`
的 label-selector join。用 **Deployment pod-template labels** 當 L。

**IP 來源 union** → `ingress_ips` 陣列：`spec.externalIPs` ∪ `status.loadBalancer.ingress[]` ∪（選配）NodePort+NodeIP。

**三跳 SQL**（每跳窄查詢 + `valid_from <= T AND T < valid_to`）：

```sql
-- Hop1: has(ingress_ips, ip) → svc selector
SELECT namespace AS ns, selector_kv AS svc_sel
FROM svc_versions FINAL
WHERE has(ingress_ips, {ip}) AND valid_from <= {t} AND {t} < valid_to;

-- Hop2: hasAll(pod_labels_kv, svc_sel) → L
SELECT pod_labels_kv AS L FROM deploy_versions FINAL
WHERE namespace = {ns} AND hasAll(pod_labels_kv, {svc_sel})
  AND valid_from <= {t} AND {t} < valid_to;

-- Hop3: hasAll(L, selector_kv) → 候選 Gateway
SELECT namespace, name, server_hosts FROM gw_versions FINAL
WHERE hasAll({L}, selector_kv) AND valid_from <= {t} AND {t} < valid_to;
```

候選交 `gwresolve(host, candidates)` 做 most-specific host 消歧。可合併為單一 WITH join 查詢以攤平
per-query overhead。

**效能**：ingress 相關列數極小；掃描微秒級，延遲下限主要來自 ClickHouse per-query overhead（單 join
約 1–5ms，三次 round-trip 約 5–15ms），對取證/低 QPS 足夠。

**調校**：ingest 期物化 join 欄；`valid_to` sentinel；`ORDER BY (identity, valid_from)`；
`bloom_filter` on `ingress_ips`；單一查詢 + prepared statement；必要時 `argMax` 取代 `FINAL`。

#### 引擎 2A

in-process link istiod `ConfigGenerator` → `RouteConfiguration` → `router_check_tool` 比對 `host+path`。
命中 cluster `outbound|PORT|SUBSET|SERVICE.NS.svc.cluster.local`，destination 解析幾乎免費。
代價：與 `istio.io/istio` 版本強耦合。

**步驟概要**：

1. `store.AsOf(T)` → VS+Gateway+DR+Service 快照 → `config.Config` / `*model.Service`。
2. 建 `model.Environment` + `PushContext` + synthetic `model.Proxy`（Router + Gateway selector labels）。
3. `ConfigGenerator.BuildHTTPRoutes` → RC。
4. `router_check_tool` 比對 → 切 cluster 字串 →（選配）EndpointSlice。

POC 落地差異（固定 `http.80`、無 `FakeDiscoveryServer`、sentinel 解析技巧等）見
[poc/ARCHITECTURE.md](../poc/ARCHITECTURE.md)。

### C. Prometheus export（並存）

現有 `_info` metric 續用於即時可觀測性，**不是**路由回溯真實來源。

---



## 6. POC 現況摘要

`poc/route2a`（獨立 Go module）已驗證：

- 引擎 2A 全鏈：600 gateway × 100 VS，0 mismatch oracle
- ClickHouse 四表、3-hop、`ResolveAmong`、CH-backed `ScopedFor`、多版本 `AsOf(T)`
- benchmark 六階段計時（lookup / resolve / scopedfetch / translate / check / total）

主專案待移植：`pkg/store`、`pkg/resolve`、ingest writer、`cmd/query`。
完整 POC 架構與套件對照 → [poc/ARCHITECTURE.md](../poc/ARCHITECTURE.md)。

---



## 7. 主專案整合（關鍵檔案）


| 檔案                         | 動作                                   |
| -------------------------- | ------------------------------------ |
| `pkg/collector/listers.go` | 重用 informer，掛事件 handler              |
| `pkg/config/config.go`     | 新增 Istio GVR + `history:` 區塊         |
| `pkg/store/`（新增）           | `Store` interface + ClickHouse 實作    |
| `pkg/resolve/`（新增）         | 視角收斂、destination 解析；引擎 2A 移植自 POC    |
| `pkg/ingressresolve/`（新增）  | IP→Gateway 三跳 + `ResolveAmong`       |
| `cmd/query/`（新增）           | host/path/dst_ip/區間 → per-version 結果 |


`cmd/main.go`（scrape exporter）與 ingest/query **解耦**。

---



## 8. 驗證方式

1. **引擎 2A**：POC `matchcheck` vs `scalegen` oracle（已在 `poc/route2a` 驗證）。
2. **IP→Gateway 三跳**：POC `TestIPFlowClickHouse`；主專案需 mock 多候選案例。
3. **時間回溯**：VS 變更後查橫跨區間 → 兩個版本各自 destination 正確。
4. **解析對照**：`istioctl proxy-config routes` 比對。
5. **store**：`AsOf(T)` 唯一版、`Overlap(t0,t1)` 全版本、`FINAL` 去重正確。

---



## 9. 風險與注意事項

1. **LB IP 在 status**：須觸發新版本（見 §4.1）。
2. **多 A record / CNAME**：候選 IP 取 union。
3. **Port / TLS**：POC 固定 HTTP :80。
4. **istiod 版本耦合**：叢集升級須重建 query binary。
5. **效能**：docker 下 `router_check_tool` 啟動 ~200ms 主導單筆延遲；三跳相對可忽略。

---



## 10. 待實作清單

- [x] ingest：event handler + ClickHouse writer（`pkg/history`）＋設定式欄位/型別/json path＋client 端 regex filter
- [x] `pkg/store` ClickHouse：append-only writer + DDL 產生/驗證（`EnsureSchema` / `WriteBatch`）
  - 查詢面 `AsOf(T)` / `Overlap(t0,t1)`（含 `valid_to` 推導）待查詢引擎階段實作
- [ ] watch 新增 GVR：VS/Gateway/DR/Service/EndpointSlice/ingress Deployment（設定已支援；預設清單待補）
- [x] POC：引擎 2A、`ResolveAmong`、IP→Gateway 三跳（`poc/route2a`）
- [ ] 移植 POC `Translator` / `Runner.Resolve` → `pkg/resolve/envoy`
- [ ] OTEL collector：span IP 優先、DNS lookup processor
- [ ] DestinationRule subset + EndpointSlice 解析層
- [ ] `cmd/query`：host/path/dst_ip/區間 API

---



## 11. 一句話總結

**ClickHouse 版本化快照（interval +** `hasAll` **join）+ 引擎 2A（istiod 翻譯 +** `router_check_tool`**）**，
重用 informer 基礎，精準回放 host/path → Service+ns+port；ingress `IP→Gateway` 以 query-time 三跳下推
ClickHouse，維持 per-resource 儲存。