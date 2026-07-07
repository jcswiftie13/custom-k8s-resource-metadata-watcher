# Istio 路由回溯與 Ingress 流量 Gateway 解析 — 完整流程設計

> 狀態：設計 + POC。本文件由兩份設計討論合併而成——**（1）VirtualService 路由回溯查詢**（給
> host+path+時間，回放每個版本的 destination）與 **（2）Ingress 流量視角**（由 OTEL span / DNS / IP
> 反查真正命中的 Gateway）。
>
> **已定案的選型**：版本化 store = **ClickHouse**；比對引擎 = **引擎 2A**（in-process istiod 翻譯
> + Envoy `router_check_tool`）；valid-time = **事件級精確（interval 模型）**。
>
> **已實作（POC `poc/route2a`）**：引擎 2A 的解析切片（VS+Gateway+Service → RouteConfiguration →
> `router_check_tool` → cluster）與 host→Gateway 視角收斂。
>
> **尚未實作**：版本化 temporal store（ClickHouse）、watch/ingest writer、時間回溯、ingress
> `IP→Gateway` 映射、DestinationRule subset / EndpointSlice 解析層。
>
> 原「多選項待決定」的段落已依 POC 實作與定案改寫為**現況**；被否決的替代方案移到「附錄 A / B」保留脈絡。

---

## 1. Context（為什麼要做這件事）

**核心需求**：給定 VirtualService 的 **host**（如 `product.service.com`）、**path**（如 `/api/v1/users`）
與**時間區間**（如今天 11:00–12:00），查出該區間內符合的路由規則**依序**解析到哪些 destination
（K8s Service + namespace + port）。且若設定在區間內被改過，要**回傳每個版本**；destination 要**完整解析
到 Service+port**（含 DestinationRule subset）。使用情境 = **歷史/取證查詢**（低 QPS，精準還原當時設定最重要）。

**兩種查詢語意（應並存）**：

| 模式 | 流程 | 用途 |
|------|------|------|
| **config_only**（POC 現行） | `host` → 全 cluster host 反查 Gateway → translate → `router_check_tool` | 設定稽核、「誰接受這個 host」 |
| **traffic_simulation**（ingress 流量視角） | `host` + `dst_ip`（來自 span 或 FQDN→DNS）→ `IP → []Gateway` → 候選內 host 反查 → translate → `router_check_tool` | 模擬真實流量落點 |

兩者結果可能分歧（host 設定上 match 某 Gateway，但 DNS/IP 指向另一顆 ingress）。API 應標明模式，或同時
回傳並標記 mismatch。

**現況起點**：本專案原是**通用 K8s metadata → Prometheus `_info` gauge exporter**——scrape-time 重建、
value=1、flat label、**只反映當下**（`pkg/collector/collector.go` 的 `Collect`），不追蹤
`resourceVersion`/`generation`、**不保留歷史**、無 Istio 語意。本設計在其上新增「事件驅動版本化 ingest」
與「路由解析引擎」兩條與既有 exporter 解耦的路徑。

---

## 2. 評估結論：TSDB metric export **不適合**當這個需求的真實來源

原因（針對「有序、巢狀、需回溯歷史、需完整解析」四特性）：

1. **路由是有序、巢狀、關聯式結構，flat label 會遺失語意。** 一次解析需要：host 清單、`spec.http[]` 的
   **順序**（first-match-wins）、每條的 `match[]`（uri prefix/exact/regex、method、headers、authority）、
   以及 route→destination 的權重樹。把 route/match/dest index 全塞成 label 既脆弱又難重組。
2. **基數爆炸。** path、host、header-match 當 label → 高基數 series，正是 Prometheus 最不擅長的情境。
3. **比對邏輯無法在 PromQL 做。** prefix/exact/regex + 順序優先級，PromQL 無法執行；最後一定是把 series
   全撈回程式自己比對——等於把 TSDB 當很差的 config 快照 KV store。
4. **回溯「每個版本」很難從 metric 還原。** 要從碎裂 series 在時間點 T 正確重組「當時完整有序設定」、又要
   偵測 label-set 變化切出「每個版本」，極為脆弱。
5. **完整解析需要整份巢狀 spec。** DestinationRule subset、`destination.host` → Service/ns/port，必須保留
   VS/DR/Service/EndpointSlice 的**完整物件**，無法可靠塞進 label。

> Metric/TSDB 適合「低基數的存在性與聚合可觀測性」，**不適合**「精準回溯某時刻完整有序設定並重放路由」。
> 後者需要**版本化（bitemporal）設定快照儲存 + 解析引擎**。同理，ingress `IP→Gateway` 這種高基數關聯邊
> **也不該塞進 Prometheus label**（見 §5-B）。

---

## 3. 決策總覽（決策樹 + 決策摘要）

### 樹 1：怎麼儲存資料 → **定案 ClickHouse**

```mermaid
graph TD
    A[怎麼儲存版本化設定?] --> B[定案: ClickHouse<br/>版本化設定快照 + valid 區間]
    B --> C[interval 模型<br/>valid_from/valid_to 遠未來 sentinel]
    B --> D[selector/labels/IP 抽成 Array String<br/>hasAll 做子集 join]
    B --> E[ReplacingMergeTree ingest_seq<br/>+ 查詢端 FINAL/argMax 去重<br/>取代 Postgres ACID tx 護欄]
    A -.->|考慮過,未採用,見附錄A| F[Postgres / Mongo / TSDB+NoSQL hybrid]
```

### 樹 2：視角收斂（怎麼 parse VS 和 Gateway）

```mermaid
graph TD
    A[怎麼 parse VS/Gateway<br/>並確定 route?] --> B{視角?}
    B -->|Ingress Gateway<br/>南北向,主情境| C[host 反查 Gateway.servers 中的 hosts<br/>取得候選 Gateway]
    C --> D{需要消歧?<br/>萬用 host / 多 gateway<br/>/需回報是哪個入口}
    D -->|是,或 traffic_simulation| E[加 dst_ip 縮候選<br/>IP→Service→Deployment labels→Gateway<br/>再候選內 host 反查]
    D -->|否,單一 ingress<br/>host 具體,設定正確| F[只用 VS 過濾即可拿 destination<br/>host + gateways != mesh]
    B -->|Sidecar 東西向<br/>未來選配,本需求不做| G[需指定 client namespace<br/>候選 = gateways 含 mesh<br/>且受 exportTo/Sidecar 可見性限制]
```

### 樹 3：比對引擎 → **現況 引擎 2A（POC 已實作）**

```mermaid
graph TD
    A[比對引擎] --> B[現況: 引擎2A<br/>in-process istiod ConfigGenerator 翻 RC<br/>+ Envoy router_check_tool 比對<br/>POC poc/route2a 已實作]
    A -.->|未採用: 自寫 matcher 有語意 bug 風險| C[引擎1: 自寫 uri-match 子集 + 多 VS 合併]
    A -.->|未採用: per-proxy 基數大且 scrape 有損| D[引擎2B: 定期快照 proxy-config routes]
```

### 決策摘要

| 議題 | 結論 |
|------|------|
| TSDB metric 能當真實來源？ | **不能**。有序/巢狀/需回溯/需完整解析 → 版本化快照 store + 解析引擎。 |
| 版本化 store 用什麼？ | **ClickHouse（定案）**。interval 模型 + `Array(String)` + `hasAll` + `ReplacingMergeTree`。替代方案見附錄 A。 |
| valid-time 精度？ | **事件級精確**（informer 事件寫 `valid_from`、收前版 `valid_to`），不漏短命版本。 |
| 比對引擎？ | **引擎 2A（現況）**：in-process istiod 翻譯 + `router_check_tool`；POC `poc/route2a` 已實作解析切片。 |
| DNS / IP 能否取代 host 反查？ | **不能**。IP 只縮候選；最終 Gateway 仍須 `servers[].hosts` most-specific 比對。 |
| 查詢時重算 vs 預建索引（ingress `IP→Gateway`）？ | 本案（取證/低 QPS）**query-time 重算**：三次窄查詢（IP→Service→Deployment→Gateway），非掃全 Service；線上熱路徑才升級預建索引。 |
| ingress IP 存在哪？ | **多欄位 union**：`spec.externalIPs`（本環境即此）∪ `status.loadBalancer.ingress[]`（雲端 LB/MetalLB）∪（選配）NodePort+Node ExternalIP。`status` 來源須能版本化。 |
| `IP→Gateway` 有直接欄位嗎？ | **無**。ingressgateway workload 無指向 Gateway CR 的欄位；靠 `Gateway.spec.selector ⊆ ingress Deployment pod labels` 的 selector join。 |
| 這個 join 怎麼算才有效率？ | **query-time 重算，selector 包含用 ClickHouse `hasAll` 下推**（預設）；不建 binding 表，store 維持 per-resource。 |
| 要物化 binding 表嗎？ | **選配升級**：用 ClickHouse **Refreshable MV**，區間由 source temporal join 得出（**事件級精確**，非快照 diff）；仍是 derived view、三跳仍為預設。想獨立查詢/線上加速才用。 |
| Gateway/VS scope？ | **跨 namespace**：VS `spec.gateways` 可引用 `其他ns/gw`，ingress Gateway 常在 `istio-system`；watch scope 須含被引用 ns。 |

---

## 4. 資料流：從 API server 到路由解析

**關鍵觀念**：只有設定物件（VS/Gateway/DR/Service/Deployment）在 API server 裡、可被 watch；Envoy 的
RouteConfiguration 不在 API server——它由 istiod 對每個 proxy 現算，故引擎 2A 走 in-process 翻譯。

### 4.1 階段一：擷取（watch → ClickHouse 版本化 store）

watch 對象是 **`Gateway` CRD**（不是實體 ingressgateway pod——pod 裡沒有 RouteConfiguration），與
`VirtualService` / `DestinationRule` / `Service` / **ingress `Deployment`** 走相同 list-watch。

```mermaid
graph TD
    A[K8s API server] -->|LIST 一次抓齊 scope 現況| B[Informer cache<br/>ScopedInformers]
    A -->|WATCH 之後只送增量| B
    B --> C{事件 handler Add/Update/Delete}
    C -->|Add| D[insert 新版 valid_from=事件時間 valid_to=sentinel]
    C -->|Update| E[收前版 valid_to=事件時間 + insert 新版]
    C -->|Delete| F[收前版 valid_to=事件時間 不開新版]
    D --> G[(ClickHouse 版本化 store)]
    E --> G
    F --> G
    C -.->|resync 重送 Update| H{spec-hash 真的變了?}
    H -->|否| I[丟棄,不寫假版本]
    H -->|是| E
```

擷取階段一定要處理的細節：

- **初始 LIST 的 `valid_from`** 是「watcher 啟動時間」，非資源真正建立時間；要真實時間讀 `metadata.creationTimestamp`。
- **去重用 spec-hash（不是只看 resourceVersion 變沒變）**：resourceVersion 任何一次 etcd 寫入都變（含
  istiod 寫 VS/Gateway reconciliation **status**、`type: LoadBalancer` Service 的 `status.loadBalancer`、
  server-side apply 的 `managedFields`、GitOps 改 annotation）。resourceVersion 變但 spec-hash 沒變 →
  不開新版本。**例外**：ingress Service 的 IP 在 `status.loadBalancer` 時，status 變更**須**觸發新版本
  （否則 IP↔Gateway 映射不可靠，見 §5-B）；IP 在 `spec.externalIPs` 時 spec-hash 自然涵蓋。
- **版本身分用 resourceVersion（比 generation 抗撞）**：etcd 全域 revision、嚴格遞增、刪除重建不重用；
  generation 刪除重建歸 1 會撞號。要連 etcd 備份還原都滴水不漏，record 再加 `metadata.uid`。
- **跨 namespace 綁定**：VS `spec.gateways` 可寫 `其他ns/gateway-name`，ingress Gateway 常在
  `istio-system`；watch scope 只含 app namespace 會漏掉被引用的 Gateway。

**要 watch 的 GVR**：`VirtualService`、`Gateway`（必需，host/IP 反查）、`DestinationRule`（subset→labels）、
`Service`（host→svc+port；ingress LB IP）、**ingress `Deployment`**（pod-template labels 當 `IP→Gateway`
的 L，見 §5-B）、`EndpointSlice`（endpoint 層，選配）；`Sidecar`（僅 sidecar 視角未來需要）。

**traffic_simulation 的 ingest 補充（OTEL collector，每 span 一次）**：優先用 span 既有連線
destination IP（`server.address` / `net.peer.ip` 等），寫入 span attribute（如 `dst.endpoint.ip`），
跳過 DNS；缺 IP 時才由 collector lookup processor 補 DNS（per-host cache + TTL）。span 上固化的 IP 比
即時 DNS 更適合 `AsOf(T)` 回放（TTL/Geo/failover 會使即時 DNS 與歷史連線不一致）。

### 4.2 階段二：查詢（store → 消費路線）

輸入 `host + path + [t0,t1]`（config_only）或再加 `dst_ip`（traffic_simulation）。現況引擎 2A 直接吃
階段一的 store；被否決的 2B 是獨立 poll 管道（見附錄 B）。

```mermaid
graph TD
    Q[查詢: host + path + t0,t1<br/>選配 dst_ip] --> M{模式?}
    M -->|traffic_simulation| P[dst_ip → 候選 Gateway<br/>IP→Service→Deployment labels→Gateway 三跳]
    M -->|config_only| S2
    P --> S2[候選內 host 反查 Gateway.servers hosts]
    S2 --> T1[store.AsOf T 取當時 VS+DR+Gateway+Service 快照]
    T1 --> T2[in-process istiod ConfigGenerator 翻 RouteConfiguration]
    T2 --> U[router_check_tool 比對 path → 命中 cluster]
    U --> W[cluster 名 → Service+ns+port+subset<br/>同時刻 Service 快照解析]
    W --> X[回傳 per-version 結果]
```

---

## 5. 建議架構

分三塊。**重用既有 watcher 基礎建設**，把「scrape-time 重算」改成「事件驅動持久化」。

### A. 事件式擷取 → ClickHouse 版本化快照（temporal store）

- **重用** `pkg/collector/listers.go` 的 `ScopedInformers`（可對任意 GVR 從 informer cache watch）與
  `pkg/config/config.go` 的 `WatchResource` GVR 設定。
- **新增 informer 事件 handler**（`AddFunc/UpdateFunc/DeleteFunc`）——現行 collector 純 scrape-time、
  **沒有**這層，是真正要補的核心。每個事件寫一筆版本記錄：

  ```
  { gvk, namespace, name, uid, resourceVersion, generation,
    valid_from(事件時間), valid_to(下一版到來或刪除時填入; 未關填遠未來 sentinel),
    spec_hash, object_json(完整物件),
    -- ingest 期抽好的 join 欄（見下）
    ingress_ips[], selector_kv[], pod_labels_kv[], server_hosts[] }
  ```

  append-only 的 bitemporal log；`valid_to` 在下一版本到來或刪除時補上。

#### 資源分層：哪些進版本化 store

判準是**「查詢時要不要逐一看該資源的 spec 內容 / 需不需要精確時間切片」**。

| 資源 | 放哪 | 為什麼 |
|---|---|---|
| **VirtualService** | ClickHouse 版本化（全 spec） | 逐條看 `http[]`、**有順序**（first-match）、要 path 比對 |
| **Gateway** | ClickHouse 版本化（全 spec + `selector_kv`/`server_hosts`） | `servers.hosts`（含萬用）+ `selector` + port；host 綁定/vantage + `IP→Gateway` join |
| **DestinationRule** | ClickHouse 版本化（**條件式**） | subset→pod label 只存在 DR；解析到 subset/pod 才需要 |
| **Service（含 ingress LB）** | ClickHouse 版本化（+ `ingress_ips`/`selector_kv`） | destination 解析；**且** traffic_simulation 要 `IP→Service`、須精確版本 |
| **ingress Deployment** | ClickHouse 版本化（`pod_labels_kv`） | pod-template labels 當 `IP→Gateway` 的正規 L（見 §5-B）；pod-template 穩定、可版本化 |
| **EndpointSlice / pod** | 當下 / best-effort | endpoint 層、高易變，歷史精確本就困難 |

> 註：原設計曾把 Service 留在 TSDB（scrape 解析度）。因 ingress `IP→Gateway` 需要對 Service 做
> `IP→Service` 精確版本查找，**現況把 Service 一併放進 ClickHouse 版本化 store**，消除跨時間精度接縫。

#### 儲存：ClickHouse（定案）

writer 與 query 都只依賴一個 `Store` interface（`Put(version, validFrom, spec)` / `SetValidTo` /
`AsOf(gvk,ns,name,T)` / `Overlap(gvk,ns,name,t0,t1)`），**底層實作 = ClickHouse**：

- **interval 模型**：每版一列 `[valid_from, valid_to)`，open 版 `valid_to` 填遠未來 sentinel
  （如 `'2999-01-01'`），讓 live-at-T 是乾淨兩側 range `valid_from <= T AND T < valid_to`，兩側都可用索引。
- **join key 在 ingest 期抽好、正規化成 `Array(String)`**（排序 `"k=v"` token；IP union 陣列）——查詢期
  `JSONExtract` 會主導延遲並讓 skip-index 失效，故 writer 先物化。
- **子集判斷 = `hasAll(set, subset)`**（set ⊇ subset 回 1）：Gateway/Service/Deployment 的 selector/label
  比對全走此原語（見 §5-B 三跳 SQL）。
- **原子性用引擎特性取代 ACID tx**：ClickHouse 無多語句 ACID 交易，「收前版 `valid_to` + 插新版」的
  一致性靠 `ReplacingMergeTree(ingest_seq)` + 查詢端 `FINAL` / `argMax(col, ingest_seq)` 去重達成
  （取代 Postgres 的 tx / GIST exclusion constraint 護欄）。
- **valid-time = 事件級精確（定案）**：informer 每次變更寫 `valid_from`、收前一版 `valid_to`，不漏短命
  版本、邊界精確到變更當下。（scrape 解析度做法屬被否決的 hybrid，見附錄 A。）

**DDL（示意，以 ingress 相關表為例；VS/DR 同模式加 `spec` 欄）：**

```sql
-- 只存 ingress LB Service（非全 Service 走此路徑）
CREATE TABLE svc_versions (
  namespace   LowCardinality(String),
  name        String,
  valid_from  DateTime64(3),
  valid_to    DateTime64(3),                    -- open 版填 '2999-01-01'
  ingress_ips Array(String),                    -- spec.externalIPs ∪ status.LB.ingress ∪ (選配)NodePort+NodeIP
  selector_kv Array(String),                    -- 排序 ["app=istio-ingressgateway","istio=ingressgateway"]
  ingest_seq  UInt64,
  INDEX idx_ips ingress_ips TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq)
ORDER BY (namespace, name, valid_from);

CREATE TABLE deploy_versions (
  namespace LowCardinality(String), name String,
  valid_from DateTime64(3), valid_to DateTime64(3),
  pod_labels_kv Array(String),                  -- pod-template labels, 排序 "k=v" → 即 L
  ingest_seq UInt64
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);

CREATE TABLE gw_versions (
  namespace LowCardinality(String), name String,
  valid_from DateTime64(3), valid_to DateTime64(3),
  selector_kv Array(String),                    -- 排序 "k=v"
  server_hosts Array(String),
  ingest_seq UInt64,
  INDEX idx_sel selector_kv TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from);
```

#### Config 怎麼寫（純加法，TSDB export 不移除）

informer 由 `watch.resources` 單一來源驅動（`pkg/config/config.go` 的 `Watch`），exporter 的 `rules`
與歷史快照都是同一批 informer 的不同消費者。

- `metricPrefix` / `watch.resources` / `rules` **保留不動**（TSDB export 行為不變）。
- `watch.resources` **新增** Istio CRD（完整 GVR）+ `Service` / `EndpointSlice` / ingress `Deployment`。
- **新增** 頂層 `history:` 區塊（opt-in）；`enabled:false` 或省略 → 行為與今天完全相同。

```yaml
watch:
  resources:
    - kind: Pod                # 既有,TSDB export 照用
    - name: VirtualService     # 新增
      apiVersion: networking.istio.io/v1beta1
      kind: VirtualService
      resource: virtualservices
      scope: Namespaced
    - name: DestinationRule
      apiVersion: networking.istio.io/v1beta1
      kind: DestinationRule
      resource: destinationrules
      scope: Namespaced
    - name: Gateway
      apiVersion: networking.istio.io/v1beta1
      kind: Gateway
      resource: gateways
      scope: Namespaced
    - kind: Service            # 內建
    - kind: EndpointSlice      # 內建
    - kind: Deployment         # 內建;ingress workload pod-template labels
rules: [ ... ]                 # 既有 Prometheus 規則,完全不動
history:                       # 新增
  enabled: true
  store:
    type: clickhouse
    dsn: "clickhouse://user:pass@host:9000/routing"
  resources: [VirtualService, DestinationRule, Gateway, Service, EndpointSlice, Deployment]
```

> ⚠️ 現行 `config.Validate()` 強制 `len(rules) >= 1`。保留既有 export rules 即無影響；若日後要「只跑歷史」
> 才需放寬此檢查。

### B. 查詢／路由解析引擎（使用者設想的「另一支程式」）

輸入：`host`、`path`、（選配 method/headers、`dst_ip`）、時間區間 `[t0,t1]`。**視角預設 ingress gateway，
由 host（config_only）或 dst_ip（traffic_simulation）推導**；sidecar 視角未來選配。

通用流程：

1. 撈出 validity 與 `[t0,t1]` **重疊**的相關版本（VS/DR/Service/Gateway/Deployment），依各資源 `valid_from`
   切段 → 對應「每個版本」。
2. 依**視角**收斂候選 Gateway/VS（見下）。
3. 對每個版本做 path → route 比對（引擎 2A），取出 `route[].destination`（含 weight）有序清單。
4. 對每個 destination，用**同一時刻生效**的 Service 快照解析 `destination.host` → Service+ns+port；套用
   DestinationRule subset（labels），需要 endpoint 層再對 EndpointSlice。
5. 回傳 per-version：`[valid_from, valid_to, vantage, matched_route, [{service, ns, port, subset, weight}...]]`。

#### 視角（vantage）與 config_only 流程

同一 host+path 從不同視角可能命中不同 route（istiod 逐 proxy 算 RC）。**主情境 = Ingress Gateway，
vantage 由 host/IP 推導、非使用者參數：**

```
1. （traffic_simulation）dst_ip → 候選 Gateway（三跳,見下）;（config_only）跳過此步
2. 掃 Gateway,找 servers[].hosts 命中 host 的 → 候選 Gateway（traffic 模式在步驟1候選內找）
3. 掃 VirtualService,取 spec.hosts 命中 host 且 spec.gateways 含候選 Gateway 者
   （spec.gateways 省略 = ["mesh"],不套用 ingress → 排除）
4. 這些 VS 的 http[] 依序比對 path → 命中 route → route.destination.host → Service+ns+port
```

- 一個外部 host 通常只被一個 Gateway 服務 → 單一結果；被多個服務（對外/對內、`*.service.com` 萬用）→
  每個 gateway 各回一結果（vantage 是可枚舉維度）。
- **Gateway 掃描是「正確性/消歧層」**：route→destination 規則全在 VS，單一 ingress、host 具體、設定正確的
  叢集只用 VS（host + `spec.gateways != [mesh]`）即可拿 destination；Gateway 用來（a）驗證綁定、（b）萬用
  host、（c）多 gateway 消歧、（d）回報哪顆 ingress。

#### traffic_simulation：`IP → Gateway` 映射（為何無直接欄位、怎麼有效率做）

`IP → ingressgateway workload` 容易（IP 比對 ingress Service → `.spec.selector` 選到 ingress pods），
但 **ingressgateway workload → Gateway CR 沒有 back-reference 欄位**：唯一把 Gateway CR 綁到該 ingress 的
是 `Gateway.spec.selector`（label selector，須匹配 ingress pods 的 labels）。所以 `IP → Gateway` 本質是
一個 **label-selector 包含關係 join**，不是欄位查找。

**用 Deployment pod-template labels 當正規 label 集合 L**（穩定、可版本化），測 `Gateway.spec.selector ⊆ L`。
比只用 `Gateway.selector ⊆ Service.selector` 更準：後者在「`Gateway.selector` 含一個 pod 有、
`Service.selector` 沒有的 label」時會漏。K8s label selector 是 **AND** 語意；要比的是**選到的 pod 集合**，
不是 label 個數：

| 關係 | 能否關聯 IP ↔ Gateway |
|------|----------------------|
| `Gateway.selector ⊆ pod labels`（用 Deployment L） | ✅ 正解，貼 istiod 真實綁定 |
| `Gateway.selector ⊆ Service.selector` | ✅ 安全近似（Service 後端 pod 一定符合 Gateway），但 pod 有額外 label 時可能漏 |
| `Service.selector ⊆ Gateway.selector` | ⚠️ 不保證 |
| 互不為子集 | ❌ 不能靠 selector 推論；用 Deployment/Endpoint pod labels |

同一 LB IP 上可能有多個 Gateway CR（相同 selector、不同 `servers[].hosts`）→ `IP` 無法消歧，仍靠候選內
`host 反查`。

**IP 來源是多欄位 union**（依暴露方式），反查正規化成一個 `ingress_ips` 陣列欄、query 用
`has(ingress_ips, <IP>)`：

- `spec.externalIPs`（手動指派外部 IP；bare-metal / 無雲端 LB 常見）— 在 **spec**，既有 spec-hash 版本化涵蓋
- `status.loadBalancer.ingress[].ip` / `.hostname`（type LoadBalancer：雲端 LB / MetalLB）— 在 **status**，
  須 status 版本化（見 §4.1 例外）
- （選配）NodePort + Node `ExternalIP`（本 repo 已在追 Node ExternalIP，可接）

##### ClickHouse 三跳 SQL、效能與調校

**子集判斷 = `hasAll(set, subset)`。三跳（每跳都是窄查詢、非全表掃描）：**

```sql
-- Hop 1：IP → ingress Service（as of T）→ ~1 列
SELECT namespace AS ns, selector_kv AS svc_sel
FROM svc_versions FINAL
WHERE has(ingress_ips, {ip:String})
  AND valid_from <= {t:DateTime64(3)} AND {t:DateTime64(3)} < valid_to;

-- Hop 2：Service.selector → ingress Deployment pod labels L（as of T）→ ~1 列
SELECT pod_labels_kv AS L
FROM deploy_versions FINAL
WHERE namespace = {ns:String}
  AND hasAll(pod_labels_kv, {svc_sel:Array(String)})    -- svc.selector ⊆ pod labels
  AND valid_from <= {t} AND {t} < valid_to;

-- Hop 3：L → 候選 Gateway（as of T）
SELECT namespace, name, server_hosts
FROM gw_versions FINAL
WHERE hasAll({L:Array(String)}, selector_kv)            -- Gateway.selector ⊆ L
  AND valid_from <= {t} AND {t} < valid_to;
```

之後候選 Gateway 交給 host 反查（`gwresolve(host, candidates)`）做 most-specific host 消歧（或把
`server_hosts` 比對下推 Hop 3）。

**單一 join 查詢（把 ClickHouse 固定 per-query overhead 只付一次）：**

```sql
WITH
  svc AS (SELECT namespace ns, selector_kv svc_sel FROM svc_versions FINAL
          WHERE has(ingress_ips,{ip}) AND valid_from<={t} AND {t}<valid_to),
  dep AS (SELECT pod_labels_kv L FROM deploy_versions FINAL d INNER JOIN svc ON d.namespace=svc.ns
          WHERE hasAll(d.pod_labels_kv, svc.svc_sel) AND d.valid_from<={t} AND {t}<d.valid_to)
SELECT g.namespace, g.name, g.server_hosts
FROM gw_versions FINAL g CROSS JOIN dep
WHERE hasAll(dep.L, g.selector_kv) AND g.valid_from<={t} AND {t}<g.valid_to;
```

**效能（兩面誠實講）：**

- **對 ClickHouse 而言資料極小**：ingress Service ~1–10、ingress Deployment ~1–10、Gateway ~數十–低百
  （POC 壓到 600）× 每資源數版本 → 數千至最壞低百萬列。每跳只碰幾個 granule、`hasAll` 掃數百列，**掃描本身微秒級**。
- **延遲下限是 ClickHouse 固定 per-query overhead（~1–5ms：規劃、thread、`FINAL` merge-on-read），非掃描**。
  量級：單一 join 查詢 ≈ **1–5ms**；三次 round-trip ≈ **5–15ms**。對取證/低 QPS 完全夠。
- **誠實 caveat**：ClickHouse 是大掃描 OLAP 引擎，這種小 keyed point-lookup 非其強項（無真正 PK point-get、
  per-query 下限偏高）。它「可用，因為 config store 本來就在 ClickHouse、join 留在 store 內」，**非此形狀
  的最佳引擎**。若日後變成**線上熱路徑（數百–千 QPS）**，下限 + `FINAL` 會咬人 → 退到 in-memory 索引或
  ClickHouse **dictionary**（真 O(1)）。

**Fine-tune（依槓桿由大到小）：**

1. **ingest 期就抽好並正規化 join 欄**（`selector_kv`/`pod_labels_kv`/`ingress_ips` 排序 `Array(String)`）——
   最大槓桿，去掉查詢期 JSON parse、讓 skip-index 生效；排序也避免格式漂移害 `hasAll`。
2. **`valid_to` sentinel**（interval 模型）給乾淨兩側 range。或無 mutation 的 **Model 2**：只存 `valid_from`，
   查詢用 `... WHERE valid_from<=T ORDER BY valid_from DESC LIMIT 1 BY namespace,name`（或 `ASOF JOIN`），
   免維護 `valid_to`，代價是讀稍重。
3. **ORDER BY = identity + `valid_from`**（同時是 ReplacingMergeTree dedup key），讓時間 range + namespace
   等值走 sparse primary index；`bloom_filter` skip-index 放 `ingress_ips`（Hop1）、選配 `selector_kv`。
4. **單一 join 查詢 + parameterized/prepared statement** 攤掉固定 per-query overhead 與規劃成本。
5. **管好 `FINAL` 成本**：此規模可忽略；若浮現，改 `argMax(col, ingest_seq)` + `GROUP BY identity,valid_from`
   手動 dedup，或確保 parts 已合併；Model 2 直接免 `FINAL`。
6. **`LowCardinality`** 放 `namespace`/`gvk`；**`PARTITION BY toYear(valid_from)`** 只在 retention 很長才用。
7. **變熱時的逃生口**：ClickHouse **dictionary**（key=IP）或 app in-memory 衍生索引，繞過 per-query 下限。

**正確性註記**：Hop2 要 `svc.selector ⊆ pod_labels`、Hop3 要 `gateway.selector ⊆ pod_labels`，兩者都是
**同一個 L** 的子集——這正是用 Deployment pod-template labels 當 L 為一致正確錨點的原因。若多個 ingress
Deployment 命中（canary/blue-green），對 L 取 union 或逐 Deployment 評估。

##### `IP→Gateway` 索引落地方式（與 per-resource store 的關係）

| 方案 | 做法 | 適用 |
|------|------|------|
| **1. Query-time 重算（本案推薦）** | `AsOf(T)` 三跳窄查詢（見上），selector 包含用 `hasAll` 下推 ClickHouse | 取證/低 QPS；store 維持純 per-resource、binding 即算即丟、時間一致 |
| **2. In-memory 預建索引** | Consumer 在 watch 事件時更新 RAM 中 `IP→[]Gateway` | 只加速「當下時刻」線上查詢；對任意歷史 T 無用；store 仍唯一真相、可重算 |
| **3. 版本化 binding 記錄（物化升級）** | 另存 `ip_gateway_binding{ip, gateway, valid_from, valid_to}`，用 **ClickHouse Refreshable MV**（見下）由 source 區間 temporal join 產生 | 仍是 **derived view**（source of truth 仍在 per-resource 表）；ClickHouse 代管排程/原子替換 → 移除「自寫 temporal-join writer」的反對理由；想 binding 獨立查詢 / 線上高 QPS 加速時採用 |

> **不要把 `IP → Gateway` 塞進 Prometheus label**（高基數關聯邊，重蹈 §2 TSDB 不適合路由查詢的結論）。
> POC 的 `gwresolve.New(gateways)` 已是「查詢端 in-memory 索引」先例；候選 Gateway 由三跳縮出後，
> `gwresolve(host, candidates)` 做最終 host 消歧。

##### 選項 3 落地：ClickHouse Refreshable Materialized View（事件級精確 binding）

若要把 `IP→Gateway` binding **物化成一張表**（獨立查詢 / 線上加速），ClickHouse 的 **Refreshable
Materialized View**（`REFRESH EVERY …`，「定期跑 query → 存表」）正是這個機制。它把選項 3 原本「需自寫複雜
temporal-join writer」的反對理由消掉了——**排程與原子替換由 ClickHouse 代管**。但要件是：binding 的
`valid_from`/`valid_to` **必須來自對 source 區間的 temporal join**（事件級精確），**不是**週期快照 diff。

**機制三選一（推薦 refreshable MV）：**

| 機制 | 說明 |
|------|------|
| **Refreshable MV** | `CREATE MATERIALIZED VIEW … REFRESH EVERY 10 MINUTE …`；預設全量重算 + 原子替換。CH 24.x GA（23.x 需 `allow_experimental_refreshable_materialized_view=1`）。 |
| Dictionary + `LIFETIME` | 週期 reload key→value、真 O(1) 點查，但**只當下狀態、無 valid 區間** → 適合線上當下 `IP→gateways`，非歷史需求。 |
| 外部 cron `INSERT INTO … SELECT` | 等價但要自管排程。 |
| ~~標準 incremental MV~~ | **不適用**：只在單一 source 表 insert 時對新 block 觸發，無法在 Gateway 變更時重算三表 join。 |

**事件級精確 binding 的 temporal-join SQL**（全量重算；`greatest/least` 取區間交集、`ARRAY JOIN` 展開
`ingress_ips`、非等值 `hasAll` 用 CROSS JOIN + WHERE，資料極小安全）：

```sql
CREATE MATERIALIZED VIEW ip_gateway_binding
REFRESH EVERY 10 MINUTE                          -- 預設 replace 模式(非 APPEND)
ENGINE = MergeTree ORDER BY (ip, valid_from) AS
SELECT
  ip,
  g.namespace AS gateway_ns,
  g.name      AS gateway_name,
  greatest(s.valid_from, d.valid_from, g.valid_from) AS valid_from,
  least(s.valid_to,     d.valid_to,     g.valid_to)  AS valid_to
FROM svc_versions FINAL s
ARRAY JOIN s.ingress_ips AS ip
CROSS JOIN deploy_versions FINAL d
CROSS JOIN gw_versions FINAL g
WHERE d.namespace = s.namespace
  AND hasAll(d.pod_labels_kv, s.selector_kv)     -- svc.selector ⊆ pod labels（IP→該 ingress workload）
  AND hasAll(d.pod_labels_kv, g.selector_kv)     -- gateway.selector ⊆ pod labels（該 workload 上的 Gateway）
  AND greatest(s.valid_from, d.valid_from, g.valid_from)
    < least(s.valid_to, d.valid_to, g.valid_to); -- 三區間真的重疊才成一筆 binding
```

查詢改為對 binding 表點查：`SELECT gateway_ns, gateway_name FROM ip_gateway_binding
WHERE ip={ip} AND valid_from<={t} AND {t}<valid_to`（再交 host 反查消歧）。因全量重算涵蓋**過去+現在**
所有區間，binding 表是**完整歷史**衍生視圖，AsOf(T) 對「上次 refresh 前已存在的 T」皆事件級精確。

> **可選 coalesce**：非 binding 相關欄位變更（如 Gateway `server_hosts` 改）會在 source 開新版 → binding 被
> 切成相鄰、同 `(ip,gateway)` 的區段；用 window function 合併相鄰區間去除虛分段（精修，非必需）。

**Refresh 策略（四個面向）：**

1. **觸發時機：定期為預設，必要時事件驅動。** Refreshable MV 原生**只支援定期**（`REFRESH EVERY` /
   `REFRESH AFTER`）；`DEPENDS ON` 只是「MV 接 MV」鏈接，**非** source 表變更觸發。要事件驅動 → ingest
   writer 在寫入 binding 相關 source 版本（ingress Service/Deployment/Gateway）後呼叫
   **`SYSTEM REFRESH VIEW ip_gateway_binding`**（強制立即 refresh）；這類變更罕見、refresh 不頻繁，突發時
   debounce。預設用 `REFRESH EVERY` 當安全網；新鮮度窗口不可接受時才加 `SYSTEM REFRESH VIEW`。
2. **成本：全量重算，無增量。** Refreshable MV 設計上**每次全量重算**（無增量模式）。tiny data 下
   CROSS JOIN + `hasAll` 掃全部 svc/deploy/gw 版本（數千列）→ 亞秒級可忽略。唯一成長風險 = **長 retention +
   高 churn**（gw_versions 累積數月每次 spec 變更 → 三重積變大）；緩解：SELECT 限 rolling 時間窗（只重算與
   `[now-N, now]` 重疊的區間，需 source 表 TTL 配合）或改 app 端真增量 writer。起步用全量重算。
3. **新鮮度窗口：只影響「最新」查詢。** binding 表對「上次 refresh 前的 T」皆事件級精確；只有最近一個
   refresh 間隔內的 source 變更會落後。補法：最新窗口/當下查詢走 **query-time 三跳**（永遠見最新 ingest 狀態、
   零窗口），大量歷史走 binding 表；或因重算便宜直接用短間隔（1–5 分鐘）。
4. **REPLACE（預設）非 APPEND。** SELECT 重算**全部歷史區間**，replace 模式每次原子替換 → 表恆含完整歷史
   區間集、AsOf(T) 全歷史正確、讀者不見半成品。**APPEND 會錯**：它累加 SELECT 輸出，全歷史重算下每次 refresh
   都重複所有 binding（膨脹+錯）；APPEND 只適合「每 tick 插當下快照」的被否決模式。

> **定位**：這是選項 3 的**物化升級**，非取代預設。取證/低 QPS 仍以 **query-time 三跳**為預設（維持純
> per-resource、無新鮮度窗口）；binding 表在「想獨立查詢 / 線上高 QPS 加速」時才採用，最新 <refresh-interval>
> 窗口由三跳補。binding 仍是 derived view，per-resource 表仍是唯一 source of truth。

#### 比對引擎：引擎 2A（現況，POC 已實作）

**引擎 2A = 不自己寫 matcher，借 Envoy 真正的比對碼**：in-process link istiod 的 `ConfigGenerator`
（`pilot/pkg/networking/core`）把某時刻的 VS+DR+Gateway+Service 快照對 synthetic proxy 翻成 xDS
RouteConfiguration，再用 Envoy `router_check_tool`（離線二進位、無流量、無 istiod）比對 `host+path` →
命中 cluster。**不起外部進程、不開網路連線、不跑 ADS gRPC**。命中結果是 cluster 名
`outbound|PORT|SUBSET|SERVICE.NS.svc.cluster.local`，已編碼 Service+ns+port+subset → destination 解析
幾乎免費。代價：直接 link istiod internal package，跟 istiod 版本強耦合。

> **未採用的替代方案**（脈絡見附錄 B）：**引擎 1**（自寫 uri-match 子集 + 多 VS 合併）——純 Go、無外部
> 執行檔，但須忠實複製 Istio/Envoy 比對與優先級語意，有 subtle bug 風險。**引擎 2B**（定期
> `istioctl proxy-config routes` 快照 per-proxy RC）——per-proxy 基數大、scrape 解析度有損。**2A-重**
> （外部 `pilot-discovery` + ADS gRPC）——營運/延遲成本高於 in-process link，忠實度相同，已否決。

##### 引擎 2A 詳解：VS+Gateway+DR+Service → RouteConfiguration → `router_check_tool` → Service

> ⚠️ 本小節為**原始設計方案**；實際 POC（`poc/route2a`）在幾處做了不同落地選擇（不用
> `FakeDiscoveryServer`、略過 `BuildListeners` 兩跳、`router_check_tool` sentinel 技巧等）——差異見 §6。

**名詞解釋（xDS 家族與相關元件）**

| 名詞 | 全名 / 是什麼 | 解釋 |
|---|---|---|
| **xDS** | *x Discovery Service* | Envoy 一族「動態設定 API」總稱。Envoy 連管理伺服器（Istio 裡是 istiod）動態拉設定。LDS/RDS/CDS/EDS 都是成員。 |
| **LDS** | *Listener Discovery Service* | 遞送 **Listener**：綁 port/address、定義 filter chain。HTTP Listener 內 connection manager **引用一份 RouteConfiguration**。 |
| **RDS** | *Route Discovery Service* | 遞送 **RouteConfiguration**——本案要的東西。`virtual_hosts[]`（用 `:authority` 比對）下 `routes[]`（用 path/header/method 比對），每條指向 cluster。 |
| **CDS** | *Cluster Discovery Service* | 遞送 **Cluster**。istiod 把每個 `(Service, port, subset)` 翻成一 cluster，名字 `outbound|PORT|SUBSET|SERVICE.NS.svc.cluster.local`。 |
| **EDS** | *Endpoint Discovery Service* | 遞送每 cluster 的實際 **endpoint**（pod IP 清單）。 |
| **ADS** | *Aggregated Discovery Service* | 把 LDS/RDS/CDS/EDS 合併到單一 gRPC 串流、依相依順序遞送。 |
| **synthetic proxy** | 合成 proxy 身分（`model.Proxy`） | 捏一個假 node 身分（Type=Router + 候選 Gateway 的 ns/labels），叫 istiod「當作在幫這顆 ingress gateway 算」，拿到該 gateway 視角的 RC。 |
| **PushContext** | `model.PushContext` | istiod 一次推送用的**設定＋服務快照**，由 config store（VS/Gateway/DR）與 service registry（Service）建出。 |
| **ConfigGenerator** | `pilot/pkg/networking/core` | istiod 真正做 `config + proxy + PushContext → xDS` 翻譯的元件，**VS→RouteConfiguration 邏輯就在這裡**。 |
| **router_check_tool** | Envoy 附的離線測試二進位 | 載入一份 RouteConfiguration，用 **Envoy 真正的 route matching 碼**對測試案例比對，回命中的 route/cluster；不跑 Envoy、無流量、無 istiod。 |

**步驟一：翻出 RouteConfiguration（in-process）**——備齊 config store 快照（VS+Gateway+DR+Service）、
service registry / PushContext、ConfigGenerator、synthetic proxy：

1. **取快照並轉成 istiod config。** `store.AsOf(T)` 取時刻 T 的 VS+Gateway+DR（＋條件式 Sidecar/ServiceEntry）
   與 Service 快照；把 spec 轉成 `config.Config`（帶對的 GVK/ns/name/CreationTimestamp），Service 轉成
   `*model.Service`（或灌進 fake kube client）。
2. **建 config store 與 service registry**，組成 `model.Environment`（`env.ConfigStore`/`env.ServiceDiscovery`）。
3. **建 PushContext**：`pc := model.NewPushContext(); pc.InitContext(env, nil, nil)`。
4. **建 synthetic proxy 並綁定視角**（ingress gateway）：
   ```go
   proxy := &model.Proxy{
       Type:            model.Router,                 // ingress gateway = Router
       ConfigNamespace: candidateGateway.Namespace,
       Metadata: &model.NodeMetadata{Labels: candidateGateway.Spec.Selector},
       IstioVersion:    parsedIstioVersion,
   }
   proxy.SetGatewaysForProxy(pc)   // 依 selector 綁到候選 Gateway
   proxy.SetSidecarScope(pc)       // 收斂可見的 VS/DR
   ```
5. **呼叫 ConfigGenerator 翻出 RC**（RC 常被 Listener 用 RDS 名引用，故兩跳）：
   ```go
   cg := core.NewConfigGenerator(nil)
   listeners := cg.BuildListeners(proxy, pc)
   routeNames := extractRDSNames(listeners)
   routes, _ := cg.BuildHTTPRoutes(proxy, pc, routeNames)
   ```
6. **版本對齊（強耦合，必須管）**：query binary 相依的 `istio.io/istio` 版本決定翻譯行為——要忠實還原時刻 T，
   應**釘到當時線上叢集的 istiod 版本**；叢集升級 → query binary 重新 vendor 並重建。

> DR 條件式必要：只有 route 引用 `subset` 時才需要——subset→pod label 只存在 DR；純 Service 導流可無 DR。
> 查詢前不知哪條 route 用 subset，故仍照樣 watch/存 DR、翻譯時一併餵入。

**步驟二：對 host+path 跑 `router_check_tool` 拿目標 Service**——組測試案例（`:authority`=host、`:path`=path、
選配 method/header）→ 得命中 cluster 名 → **切字串**解析出 `Service+ns+port+subset`（無 subset 時
`outbound|9080||reviews.ns.svc.cluster.local`）→（選配）對同時刻 EndpointSlice 解到實際 endpoint。

### C.（選配）保留 Prometheus 匯出

現有 `_info` metric 可續用於**即時/聚合可觀測性**，但**不是**路由回溯的真實來源。兩者並存即可。

---

## 6. 引擎 2A 的 POC 實作現況（`poc/route2a`）

> **引擎 2A 的解析切片已有可運作 POC**（`poc/route2a`，獨立 Go module）：落地了「VS+Gateway+Service →
> RouteConfiguration → `router_check_tool` → cluster(Service)」與 host→Gateway 視角收斂，並附 600 gateway
> × 100 VS 的壓力/正確性測試。**尚未**實作版本化 store、watch/ingest、時間回溯、ingress `IP→Gateway`——
> POC 直接吃記憶體內、per-gateway 的 scoped 設定，不接 store。

**POC 元件對照：**

| `poc/route2a` 元件 | 對應概念 |
|---|---|
| `internal/gwresolve`（`Resolver.Resolve`） | host→Gateway 反查，most-specific wildcard 消歧 |
| `internal/translate`（`Translator.Translate`） | 引擎 2A 步驟一：in-process ConfigGenerator 翻 RC |
| `internal/matchcheck`（`Runner.Resolve`） | 引擎 2A 步驟二：`router_check_tool` 比對 → cluster |
| `internal/simulate`（`Engine.ResolveAll`） | 串起步驟 1→4 的解析引擎（對應 §5-B） |
| `internal/rccache` | 每 gateway 的 RC 快取（見差異 3） |
| `internal/scalegen` | 測試語料 + by-construction oracle（測試鷹架） |
| `internal/report` | 延遲/吞吐量測 |

**與原設計的重要差異（實作時的修正）：**

1. **不使用 `FakeDiscoveryServer`（避免 goroutine leak）。** 它（及 `core.NewConfigGenTest`）會啟兩條長駐
   goroutine，「每次查詢翻一次」的迴圈會累積成洩漏。POC 改為**手工靜態組出最小 `model.Environment`**
   （memory config store + memory service registry，`buildScopedEnv`），翻完即丟、無 goroutine、可並發重用
   同一 `Translator`。（`RoutesForScoped` 這個 FakeDiscoveryServer 風格 helper 保留，**只當測試 oracle**。）
2. **翻譯只呼叫一次 `BuildHTTPRoutes`、用固定 route config 名 `http.80`，略過 `BuildListeners` 兩跳。**
   針對「HTTP :80 ingress gateway」固定情境，直接
   `configGen.BuildHTTPRoutes(proxy, &model.PushRequest{Push: pc}, []string{"http.80"})`。多 port / HTTPS /
   任意 listener 才需回到 LDS→RDS 動態收名。
3. **多一層 per-gateway RC 快取（`rccache`）。** 以 gateway 為 key 快取翻好的 RC，用「相依 epoch」判失效。
   翻譯 ~2ms、`router_check_tool` 一次啟動 ~200ms（docker），快取主要省重複翻譯的 CPU。
4. **`router_check_tool` 是「驗證器」，POC 用 sentinel 技巧當「解析器」。** 它原生模式是驗證（給 expected
   cluster 回 pass/fail）。POC 塞**永不匹配的 sentinel** 當 expected，逼 `--details` 印
   `actual: [<真正 cluster>]`（miss 印 `actual: []`）再解析。另：(a) 一律加 `--disable-deprecation-check`
   （istiod RC 帶棄用欄位如 `RouteAction.max_grpc_timeout`）；(b) `:method` 必填（≥3 字元），POC 固定 `GET`。
5. **執行方式：native binary 優先、docker fallback。** 先找 `POC_ROUTERCHECK_BIN`/PATH，沒有才 docker 跑
   `envoyproxy/envoy:tools-*`。只有 native 延遲數字有意義（docker 每批付 ~200ms 啟動）。
6. **批次化：一個 gateway 一次 `router_check_tool`。** `Engine.ResolveAll` 把查詢按 gateway 分組，每 gateway
   翻一次 RC、跑一次工具。單筆查詢（線上最差）= batch=1 + cold cache，延遲 ~200ms（docker）由工具啟動主導。

> destination 解析：直接切 cluster 名。POC **尚未**做 DestinationRule subset 套用與 EndpointSlice 層
> （route 目前不帶 subset），未來照步驟 4/6 補。

### Ingress `IP→Gateway` 流程的 POC（traffic_simulation 前半段，已實作）

> 狀態：**已實作並跑通**（`poc/route2a`：`cmd/ipflow`、`internal/chstore`、`internal/ingload`、
> `internal/scalegen/ingress.go`），對一個真實 ClickHouse container 驗證。這段把 §5-B 的 traffic_simulation
> 前半段（`IP → Gateway` 三跳 + CH-backed 翻譯）接到既有引擎 2A 後半段（gateway → RC → cluster）。

#### 端到端流程圖

實線＝POC 已實作；虛線＝真實輸入來源，POC 未做（以確定性 `IPForHost` 模擬 DNS，見 §4.1）。

```mermaid
graph TD
    S[OTEL span 進入] -.->|lookup processor: host→DNS lookup→IP<br/>POC 未實作,以 IPForHost 模擬| IP[dst_ip]
    Q[查詢: host + path] --> IP
    IP --> H1["Hop1: has(ingress_ips, ip)<br/>service_versions → ingress Service selector"]
    H1 --> H2["Hop2: hasAll(pod_labels_kv ⊇ svc.selector)<br/>deploy_versions → L (pod labels)"]
    H2 --> H3["Hop3: hasAll(L ⊇ selector_kv)<br/>gw_versions → 候選 Gateway(+server_hosts)"]
    H3 --> GR["gwresolve(host, candidates)<br/>most-specific host 消歧"]
    GR --> SF["ScopedFor(gw, T) from ClickHouse<br/>gw_versions.spec_json + vs_versions.spec_json<br/>+ service_versions backend → model.Service"]
    SF --> TR["translate: istiod ConfigGenerator → RouteConfiguration"]
    TR --> RC["router_check_tool (host+path) → cluster"]
    RC --> OUT["outbound|PORT|SUBSET|SERVICE.NS.svc.cluster.local"]
    H1 -.->|每跳皆 AND valid_from<=T AND T<valid_to<br/>多版本中選 T 生效版| H1
```

#### 步驟詳細說明與原因

1. **輸入 `host + path + dst_ip`。** `dst_ip` 是「落點 IP」。真實情境由 OTEL collector 的 lookup processor
   對 host 做 DNS lookup（或直接用 span 既有連線 IP）得到；**POC 未接真實 DNS**，以 `scalegen.IPForHost`
   用相同確定性方案模擬（`svcJJ.gwNNN.example.com → 10.0.<i/256>.<i%256>`）。
2. **3-hop `IP → 候選 Gateway`（ClickHouse）。** 因為 `ingressgateway workload → Gateway CR` **無 back-reference
   欄位**，只能靠 `Gateway.spec.selector ⊆ ingress pod labels` 的 **label-selector 包含 join**：
   - **Hop1**（`has(ingress_ips, ip)`）：IP 落在哪個 ingress LB Service → 取其 `selector`。
   - **Hop2**（`hasAll(pod_labels_kv, svc.selector)`）：該 Service 選到的 ingress **Deployment** pod labels
     `L`（用 Deployment pod-template labels 當 L，比只比 `⊆ Service.selector` 更準）。
   - **Hop3**（`hasAll(L, selector_kv)`）：`Gateway.selector ⊆ L` 的 Gateway（唯一標籤 `{istio: gw-NNN}` →
     每 IP 命中單一 gateway）。
   **為什麼三段循序**：每跳都是窄查詢（IP 等值 / namespace 等值 / `hasAll` 子集），避免 `service×deploy×gw`
   的 cross product；`hasAll` 子集判斷**下推 ClickHouse**。**為什麼帶 `valid_from<=T AND T<valid_to`**：多版本
   store 中選「T 時生效的那一版」（`t=now` → 開放版）。
3. **`gwresolve(host, candidates)`。** 在 IP 縮出的候選內做 most-specific host 比對（POC 語料 IP↔gateway 為
   1:1，故候選單一；仍驗 host 命中該 gateway 的 `server_hosts`）。
4. **CH-backed `ScopedFor(gw, T)`。** 從 ClickHouse 以 `AsOf(T)` 撈該 gateway 的 `Gateway`（`gw_versions.spec_json`）
   + 綁定的 `VirtualService`（`vs_versions.spec_json`，`has(bound_gateways, "ns/gw")`）+ 目的地 `Service`
   （`service_versions` backend 列，`namespace=gw` 重建 `model.Service`），`protojson.Unmarshal` 還原成
   `config.Config` → `translate.ScopedInput`。**為什麼**：讓翻譯階段完全吃 store（模擬 `store.AsOf(T) → scoped
   設定 → 翻譯`），而非記憶體語料。
5. **translate → RouteConfiguration → `router_check_tool` → cluster**：沿用引擎 2A（§5-B / §6）。cluster 名
   已編碼 Service+ns+port+subset。

**現況 vs 未做**：已做＝3-hop、CH-backed 翻譯、多版本 `AsOf(T)` 選版、與引擎 2A 對接、by-construction oracle
驗證。未做＝真實 DNS/collector、DestinationRule subset、EndpointSlice、「一個 IP 被多 Gateway 共用」的多候選
消歧（1:1 語料下候選恆單一）。

#### Data model（ClickHouse 各 resource）

四張 `ReplacingMergeTree(ingest_seq) ORDER BY (namespace, name, valid_from)` 表（對照 §5-A DDL）。共同欄位：
`namespace, name, valid_from DateTime64(3), valid_to DateTime64(3)（開放版 sentinel 遠未來、須 ≤ DateTime64
上限 2299）, rev UInt32（版本序,觀測用）, ingest_seq UInt64`。多版本＝同 `(namespace,name)` 多列（`valid_from`
相異、不被 dedup）；`AND valid_from<=T AND T<valid_to` 選 T 生效版，`FINAL` 只收同版本 rewrite。

| 表 | 專屬欄位 | 用途 |
|----|----------|------|
| `service_versions` | `ingress_ips Array(String)`（bloom index）、`selector_kv Array(String)`、`hostname/port/port_name/protocol` | **一表兩用**：ingress LB 列（`ingress_ips`/`selector_kv`）供 Hop1；backend 列（`hostname` 等）供 translate 建 cluster |
| `deploy_versions` | `pod_labels_kv Array(String)` | Hop2 的 L（ingress workload pod-template labels） |
| `gw_versions` | `selector_kv Array(String)`（bloom index）、`server_hosts Array(String)`、`spec_json String` | Hop3 的 `selector`、`server_hosts`（給 gwresolve）、`spec_json`（translate 重建 Gateway） |
| `vs_versions` | `bound_gateways Array(String)`、`spec_json String` | ScopedFor 以 `has(bound_gateways, "ns/gw")` 撈綁定 VS；`spec_json` 重建 VirtualService |

- selector/labels/IP 皆在 **ingest 期**正規化成排序 `["k=v", …]` / IP 陣列（`hasAll`/`has` 直接比對，免查詢期
  JSON parse）。`spec_json = protojson.Marshal(cfg.Spec)`（istio API proto），讀回 `protojson.Unmarshal`。
- 測試資料**程式產生**（`internal/ingload` 由 `scalegen` 展開多版本、stream 分塊插入），非手寫 SQL。

#### 驗證（實測）

`make ch-up && make ch-load && make ch-flow`（`POC_GATEWAYS=20 POC_VS=5 POC_VER_KSVC=5`）實測：列數
`service_versions=2040 / deploy=40 / gw=40 / vs=1000`；`10.0.0.0 → 候選 [gw-000] → gw-000 →
outbound|8080||svc-000-00-exact.gw-000.svc.cluster.local`（對照 oracle PASS）。`ip_flow_test.go`
再驗 3-hop 唯一性、多版本 `AsOf(T)` 回正確 `rev`、30 筆整條管線 cluster == oracle（CH 不可達則 skip）。

---

## 7. 要動到的關鍵檔案

| 檔案 | 動作 |
|---|---|
| `pkg/collector/listers.go`（`ScopedInformers`） | 重用 informer，掛上 Add/Update/Delete 事件 handler |
| `pkg/config/config.go`（`WatchResource`） | 加入 Istio GVR（virtualservices/gateways/destinationrules）+ Service/EndpointSlice/Deployment；新增 `history:` 區塊 |
| `pkg/store/`（新增） | `Store` interface + **ClickHouse 實作**（interval 模型、`Array(String)` join 欄、`hasAll`、`ReplacingMergeTree`+`FINAL`）；`Put`/`SetValidTo`/`AsOf(T)`/`Overlap(t0,t1)` |
| `pkg/resolve/`（新增） | 視角收斂、destination→Service/ns/port 解析、subset 套用；比對引擎 = 引擎 2A（`router_check_tool`），移植自 `poc/route2a` |
| `pkg/resolve/envoy/`（引擎 2A，移植 POC） | `Translator`（in-process 翻譯，無 goroutine leak）+ `Runner.Resolve`（sentinel 解析）直接沿用；補接 store `AsOf(T)` 與 DR/EndpointSlice 層 |
| `pkg/ingressresolve/`（新增，traffic_simulation） | `IP→Gateway` 三跳 `hasAll` 下推查詢（as-of-T）；`gwresolve` 加 `ResolveAmong(host, candidates)` |
| `cmd/query/`（新增）或 CLI/HTTP | 接收 host/path/dst_ip/區間，呼叫 store + resolve，輸出 per-version 結果 |

> 現行 `cmd/main.go` 是 scrape-time exporter；本案新增「ingest writer」+「query/resolve」兩個獨立路徑，
> 與既有 exporter 解耦。

---

## 8. 驗證方式（end-to-end）

1. **引擎 2A 正確性（現況可測）**：對記憶體內 scoped 設定，POC `poc/route2a` 的 `matchcheck` 對照
   `scalegen` by-construction oracle（600 gateway × 100 VS）。
2. **視角測試**：同 host+path 從 gateway 視角驗證候選 VS 收斂與命中結果。
3. **`IP→Gateway` 三跳**：mock ingress Service/Deployment/Gateway 版本，驗證 `has`/`hasAll`/valid-overlap
   三跳回正確候選；含「同 IP 多 Gateway」「Deployment 有額外 label」案例。
4. **時間回溯整合測試**：套用一份 VS → 改 destination → 查詢「橫跨變更」的區間，期望回**兩個版本**且各自
   destination 正確（沿用 `test/integration` envtest/kind 流程）。
5. **解析正確性**：對當下狀態，將輸出與 `istioctl x describe service <svc>` / `istioctl proxy-config routes`
   比對。
6. **store range 查詢**：寫入多版本後，驗證 `AsOf(T)` 回唯一版本、`Overlap(t0,t1)` 回區間內全部版本；
   驗證 `ReplacingMergeTree`+`FINAL` 去重正確（無「兩版同時開放」）。

---

## 9. 風險與注意事項

1. **LB IP 在 status**：history writer 若只對 spec-hash 開版本，`status.loadBalancer` 的 IP 分配不會留記錄
   → ingress Service 的 **status 變更應觸發新版本**（或獨立 status 時間序列）。IP 在 `spec.externalIPs` 則
   無此問題。
2. **多 A record / CNAME**：候選 IP 取 union；雲端 LB 僅 hostname 時需 resolve 並在 status 變更時更新。
3. **Port / TLS**：DNS/IP 不含 port；query 需預設 port（如 443）或要求呼叫方提供；POC 目前固定 HTTP :80。
4. **istiod 版本強耦合**：引擎 2A link istiod internal package，翻譯行為綁 `istio.io/istio` 版本；叢集升級
   須重新 vendor 並重建 query binary。
5. **POC 離線**：`scalegen` 無真實 DNS/LB → 測 traffic 路徑需 mock ingress Service/Deployment fixture。
6. **效能**：`router_check_tool` 一次啟動 ~200ms（docker）主導單筆延遲；`IP→Gateway` 三跳與翻譯相對可忽略。
7. **Refreshable MV binding（若採用）**：有 <refresh-interval> 新鮮度窗口（最新 binding 由 query-time 三跳補，
   或 ingest 事件呼叫 `SYSTEM REFRESH VIEW`）；**必用預設 replace 模式**（`APPEND` 全歷史重算會重複膨脹）；
   全量重算成本隨 retention/churn 成長，長期可對 SELECT 開 rolling 時間窗（需 source 表 TTL 配合）。

---

## 10. 待實作清單（參考）

- [ ] ingest：event handler + ClickHouse writer（interval 模型、`ReplacingMergeTree`、ingest 期物化
  `selector_kv`/`pod_labels_kv`/`ingress_ips` 排序 `Array(String)` + `bloom_filter`）
- [ ] `pkg/store` ClickHouse 實作：`Put`/`SetValidTo`/`AsOf(T)`/`Overlap(t0,t1)`
- [ ] watch 新增 GVR：VirtualService/Gateway/DestinationRule/Service/EndpointSlice/ingress Deployment
- [ ] 移植 `poc/route2a` 的 `Translator` / `Runner.Resolve` 到 `pkg/resolve/envoy`，接上 store `AsOf(T)`
- [ ] `gwresolve`：支援 `ResolveAmong(host, candidates []string)`
- [ ] traffic_simulation：`IP→Gateway` 三跳 `hasAll` 下推查詢（as-of-T）+ 候選內 host 反查
- [ ] （選配）`ip_gateway_binding` refreshable MV：source 區間 temporal join、`REFRESH EVERY`（replace 模式）、ingest 事件選配 `SYSTEM REFRESH VIEW`、可選 coalesce
- [ ] OTEL collector：span 連線 IP 優先、DNS lookup processor + cache
- [ ] DestinationRule subset 套用 + EndpointSlice 解析層
- [ ] `simulate.Engine`：可切換 `config_only` / `traffic_simulation`
- [ ] `cmd/query`：host/path/dst_ip/區間 → per-version 結果

---

## 11. 一句話總結

純 Metric→TSDB（把整張路由表塞 label）不適合；改用 **ClickHouse 版本化設定快照儲存（interval 模型 +
`Array(String)` + `hasAll` 子集 join）+ 引擎 2A 路由解析（in-process istiod 翻譯 + Envoy
`router_check_tool`）**，重用現有 informer/GVR 基礎，才能精準回放「某時間區間內每個版本的 host/path →
Service+ns+port」。ingress `IP→Gateway` 是無直接欄位的 selector join，用同一個 ClickHouse 以 query-time
三跳 `hasAll` 下推完成，不另建 binding 表、維持 per-resource 儲存。

---

## 附錄 A：考慮過但未採用的儲存替代方案（保留脈絡）

現況定案 ClickHouse（§5-A）。下列三候選是選型過程的對照，`Store` interface 抽象讓底層可換，故保留供參考。

**選項 1：PostgreSQL（jsonb + `tstzrange` + GIST）**
- 一張表 `config_versions(gvk, ns, name, generation, validity tstzrange, spec jsonb)`，GIST index on
  `validity`；query 用 `validity && tstzrange(t0,t1)`。
- 優：valid-time 精確且**交易式**（一次 tx 寫新列+收前列，無一致性縫）；可加 **GIST exclusion constraint**
  由 DB 禁止同 `(gvk,ns,name)` 重疊區間。缺：要營運 RDBMS；spec 大時 jsonb 體積/索引成本上升。
- **與 ClickHouse 對照**：Postgres 用 ACID tx + exclusion constraint 提供 schema 層防重疊護欄；ClickHouse
  無多語句交易，改用 `ReplacingMergeTree(ingest_seq)` + `FINAL`/`argMax` 在查詢端去重達到等價正確性。選
  ClickHouse 的理由是與 ingress `hasAll` selector-join 同引擎、且 OLAP 掃描/壓縮特性契合大量版本資料。

**選項 2：MongoDB / DocumentDB（文件式）**
- 文件 `{gvk, ns, name, generation, validFrom, validTo, spec}`，複合索引 `(gvk,ns,name,validFrom)`。
- 優：spec 巢狀 JSON 存取自然、水平擴展易。缺：valid-time 用兩欄自管（無 range 型別）；「收舊 doc + 插新
  doc」跨兩文件非單一原子，須 Mongo 4.0+ 多文件交易（顯式 opt-in）或讓 query 端容忍短暫重疊；無 exclusion
  constraint 護欄。

**選項 3：TSDB + NoSQL hybrid（重用現有 exporter）**
- TSDB 存低基數版本索引 `vs_version{namespace,name,uid,generation}=1`；NoSQL 以 **join key `(uid, generation)`**
  存完整 spec。query：TSDB 在 `[t0,t1]` 查出現過的 `(uid,generation)` → NoSQL 撈 spec。
- 優：重用現有 scrape-time exporter、版本時間軸可與流量指標對齊。缺：valid-time **受限 scrape interval**
  （短命版本會漏）；兩套系統一致性須對帳。
- **關鍵約束**：TSDB label 必須 == NoSQL join key，且要低 churn + 抗撞 → 正解 `(uid, generation)`
  （`resourceVersion` churn 太兇、`generation` 刪除重建撞號）；`resourceVersion` 存進 NoSQL 當 provenance。

> 三者都滿足「回傳每個版本 + 完整解析」；差別在 valid-time 精度（選項 3 有損）與運維面。現況選 ClickHouse
> 走**事件級精確**（同選項 1/2 的 valid-time 等級），非選項 3 的 scrape 解析度。

---

## 附錄 B：討論脈絡與關鍵問答（Q&A）

**Q0. tsdb metric 適合做「host+path+時間 → destination」嗎？** 不適合當真實來源；正解是把**時間索引**與
**設定內容**分開存。（§2）

**Q1. 需求界定**：使用情境 = **歷史/取證查詢**（低 QPS）；區間內設定有變 → **回傳每個版本**；destination →
**完整解析到 Service+ns+port**（含 DR subset）。

**Q2. Config 怎麼寫？不能移除既有 TSDB export。** 純加法：既有欄位全保留，新增頂層 `history:`（opt-in）。（§5-A）

**Q3. Postgres 以外的儲存？** 現況**定案 ClickHouse**（§5-A）；Postgres/Mongo/hybrid 為對照（附錄 A）。
要點：ClickHouse interval 模型 + `hasAll`，原子性用 `ReplacingMergeTree`+`FINAL` 取代 ACID tx；valid-time
走事件級精確（非 hybrid 的 scrape 解析度）。

**Q4. 「重用 Istio 邏輯」有現成工具嗎？** typed struct ✅（`istio.io/api`）；**多 VS 合併** ⚠️ istiod
internal；**path→哪條 route** ❌ 控制面沒有（istiod 只翻譯，實際比對是 Envoy runtime）。故走引擎 2A 借 Envoy。

**Q5. 能否啟動 Envoy 餵全部 VS？** 不能直接餵（Envoy 不認識 VS）。等價做法：取得 RouteConfiguration（2A
in-process 翻譯）後用 `router_check_tool` 比對，且逐 proxy/逐視角。（§5-B）

**Q6. ingress vs sidecar 視角？** Ingress = 邊緣 Envoy 處理外部進來（north-south），套 `spec.gateways` 含該
Gateway 的 VS；Sidecar = app pod 旁處理 mesh 內呼叫（east-west），套 `gateways:[mesh]` 且受
`exportTo`/`Sidecar` 可見性限制。**本需求只做 ingress**。

**Q7. 給 host+path 無法預先知道 gateway？** gateway **不用猜、由 host 反查 `Gateway.servers[].hosts`**
（確定性）。多 Gateway 服務同 host 時各回一結果。traffic_simulation 再用 dst_ip 縮候選。（§5-B）

**Q8. Envoy "cluster" 是什麼？** Envoy 對「一組後端 endpoints（pod 池）」的稱呼。istiod 把每個
`(Service,port,subset)` 翻成 cluster `outbound|PORT|SUBSET|SERVICE.NS.svc.cluster.local`，**已編碼
Service+ns+port+subset → 引擎 2A 幾乎免做 destination 解析**。

**Q9. 為何掃 Gateway 也掃 VS？** route→destination 規則全在 VS；但 VS 只「引用」Gateway 名，不知該 Gateway
是否真收此 host、萬用 host、多 gateway、綁哪顆 ingress——這些只在 Gateway。**VS 給規則、Gateway 給入口與
綁定條件**。

**Q10. OTEL span + DNS / IP 怎麼接到 Gateway？** traffic_simulation 在 host 反查前用 span 的 `dst_ip`
（優先於即時 DNS）經 `IP→Service→Deployment labels→Gateway` 三跳縮候選，仍須候選內 host 反查；三跳是
query-time 對 ClickHouse 的 `hasAll` 下推，**不取代** per-resource 版本化 store。（§5-B）

**Q11. `IP→Gateway` 為何沒有直接欄位、怎麼有效率做？** ingressgateway workload 無指向 Gateway CR 的欄位；
唯一綁定是 `Gateway.spec.selector ⊆ ingress pod labels`（selector join）。用 Deployment pod-template labels
當 L，ClickHouse 三跳 `hasAll` 下推、query-time 重算（不建 binding 表，維持 per-resource）。（§5-B）
