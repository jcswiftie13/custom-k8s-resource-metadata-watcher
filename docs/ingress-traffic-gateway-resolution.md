# Ingress 流量視角：OTEL Span → Gateway 解析

> 狀態：設計討論紀錄（design note）。彙整自架構討論，補充
> [`istio-virtualservice-routing-history-design.md`](istio-virtualservice-routing-history-design.md)
> 中「由 host 反查 Gateway」的 ingress 視角，說明如何結合 **OTEL span**、**DNS / 連線 IP**
> 與既有 **引擎 2A**（`poc/route2a`）模擬真實南北向流量。尚未實作。

## Context

查詢輸入的 `host + path` 將來自 **OTEL span**（例如 HTTP server/client span 的
`:authority` / `url` + path）。目標是在既有解析管線（`gwresolve` → istiod 翻譯 →
`router_check_tool`）之前，用**真實流量落點**縮小 Gateway CR 候選，更貼近生產環境行為。

POC 現況（`poc/route2a`）僅做 **L7 設定層** 的 host→Gateway 比對：對全 cluster
所有 `Gateway.servers[].hosts` 做 most-specific wildcard 匹配（`internal/gwresolve`），
不經 DNS、不經 LoadBalancer IP。本文件描述如何在此基礎上加入**流量視角**篩選。

---

## 決策摘要

| 議題 | 結論 |
|------|------|
| DNS / IP 能否取代 `gwresolve` host match？ | **不能**。IP 只縮候選；最終 Gateway 仍須 `servers[].hosts` most-specific 比對。 |
| 查詢時重算 vs 預建索引？ | 本案（取證/低 QPS）**query-time 重算**：三次窄查詢（IP→Service→Deployment→Gateway），非掃全 Service；線上熱路徑才升級預建索引。 |
| Service selector 與 Gateway selector 要完全相等？ | **不要**。安全關聯條件：`Gateway.selector ⊆ Service.selector`（子集，非 `DeepEqual`）。 |
| 預建索引 vs per-resource 版本化 store？ | **不衝突**。per-resource store 是 source of truth；索引是 **derived view**（可記憶體、可重算、可選持久化）。 |
| ingress IP 存在哪？ | **多欄位 union**：`spec.externalIPs`（本環境即此）∪ `status.loadBalancer.ingress[]`（雲端 LB/MetalLB）∪（選配）NodePort+Node ExternalIP。`status` 來源須能版本化，否則 IP 映射不可靠。 |
| OTEL ingest 是否一定要 DNS lookup？ | **優先使用 span 既有連線 IP**；僅在缺少 IP 時由 collector lookup processor 補 DNS。 |
| IP→Gateway 有直接欄位嗎？ | **無**。ingressgateway workload/pod 無指向 Gateway CR 的欄位；唯一綁定是 `Gateway.spec.selector ⊆ ingress Deployment pod labels` 的 selector join。 |
| 這個 join 怎麼存/算才有效率？ | **query-time 重算，selector 包含用 ClickHouse `hasAll` 下推**（三次窄查詢）；不建 binding 表，store 維持 per-resource。 |

---

## 兩種查詢語意（應並存）

| 模式 | 流程 | 用途 |
|------|------|------|
| **config_only**（POC 現行） | `host` → 全 cluster `gwresolve` → translate → `router_check_tool` | 設定稽核、「誰接受這個 host」 |
| **traffic_simulation**（本文件） | `host` + `dst_ip`（來自 span）→ `IP → []Gateway` → `gwresolve(host, candidates)` → translate → `router_check_tool` | 模擬真實流量落點 |

兩者結果可能分歧（例如 host 在設定上 match 某 Gateway，但 DNS 指向另一顆 ingress）。
API 應標明模式，或同時回傳並標記 mismatch。

---

## 建議端到端流程

### 階段 A：OTEL Collector（ingest，每 span 一次）

```
span 進入
  ├─ 已有連線 destination IP？
  │    （server.address / net.peer.ip / 等，依 span kind 選欄位）
  │    → 寫入 span attribute（例如 dst.endpoint.ip），跳過 DNS
  └─ 否 → lookup processor：host → IP(s)，寫入 span
         （per-host cache + TTL 上限，避免重複 resolver 查詢）
```

**注意**：若 span 已有**當次連線實際 IP**，應以之為準，而非查詢當下 DNS（TTL、Geo、failover
會使 DNS 與歷史連線不一致）。歷史查詢時，span 上固化的 IP 比即時 DNS 更適合 `AsOf(T)` 回放。

### 階段 B：Watch / Store（與既有 history writer 並行）

同一 informer 的多個 consumer（設計見主文件「Config 怎麼寫」）：

```
Informer
  ├─ Consumer A：既有 exporter → TSDB（observability，非路由真相來源）
  ├─ Consumer B：history writer → per-resource 版本化 store（Gateway / VS / …）
  └─ Consumer C：index builder → IngressIndex（衍生視圖，見下節）
```

**Watch 範圍（ingress 相關）**：

- `Gateway` CR（已有）
- **ingress LoadBalancer `Service` only**（勿 watch 全 cluster Service）
  - `status.loadBalancer.ingress.{ip,hostname}`；hostname 建索引時 resolve 為 IP
- （選配）`Deployment` pod template labels 作 canonical workload identity

### 階段 C：Query Engine（每筆 host + path + 時刻 T）

```
1. dst_ip  = span attribute
2. candidates = IngressIndex.Lookup(dst_ip)     // O(1)
3. gateway    = gwresolve(host, candidates)      // 候選內 most-specific，不可省略
4. RC         = translate(gateway) [rccache]
5. cluster    = router_check_tool(host, path)
```

若 `candidates` 為空 → **traffic miss**（不建議 silent fallback 到全量 `gwresolve`，
否則與 traffic_simulation 語意矛盾）。

**Server-side span（ingress 上）替代路徑**：若有 `k8s.pod.name` / workload labels，
可直接 `pod labels → []Gateway`（`Gateway.selector` 匹配），通常比 DNS 更準、更快；
DNS 路徑保留給僅有 client span、只有 host 的情境。

---

## IngressIndex：預建索引怎麼建

### 不要要求 selector 完全相等

K8s label selector 為 **AND** 語意；要比的是**選到的 pod 集合**，不是 label 個數。

安全關聯（Service `S` 的 LB IP、Gateway `G`）：

```
∀ (k,v) ∈ Gateway.selector  ⇒  Service.selector[k] == v
（即 Gateway.selector ⊆ Service.selector）
```

| 關係 | 能否關聯 IP ↔ Gateway |
|------|----------------------|
| Gateway selector ⊆ Service selector | ✅ Service 後端 pod 一定符合 Gateway |
| Service selector ⊆ Gateway selector | ⚠️ 不保證（Service 可能送到不符合 Gateway 的 pod） |
| 互不為子集 | ❌ 不能仅靠 selector 推論；改用 Deployment labels 或 Endpoint pod labels |

同一 LB IP 上可能有多個 Gateway CR（相同 selector、不同 `servers[].hosts`）→ 索引為
`IP → [gw-a, gw-b, …]`，**IP 無法消歧**，仍靠步驟 C 的 `gwresolve(host, candidates)`。

### IP→Gateway 映射：為何無直接欄位、用什麼當 L

`IP → ingressgateway workload` 容易（IP 比對 ingress Service → `.spec.selector` 選到 ingress pods），
但 **ingressgateway workload → Gateway CR 沒有 back-reference 欄位**：唯一把 Gateway CR 綁到該 ingress 的
是 `Gateway.spec.selector`（label selector，須匹配 ingress pods 的 labels）。所以 `IP → Gateway` 本質是
一個 **label-selector 包含關係 join**，不是欄位查找。

**用 Deployment pod-template labels 當正規 label 集合 L**（穩定、可版本化），測 `Gateway.spec.selector ⊆ L`。
比只用 `Gateway.selector ⊆ Service.selector`（上表）更準：後者在「`Gateway.selector` 含一個 pod 有、
`Service.selector` 沒有的 label」時會漏（上表 ❌ 列）。`⊆ Service.selector` 保留為輕量近似。

**IP 來源是多欄位 union**（依暴露方式），反查時正規化成一個 `ingress_ips` 陣列欄、query 用
`has(ingress_ips, <IP>)`：

- `spec.externalIPs`（手動指派外部 IP；bare-metal / 無雲端 LB 常見）
- `status.loadBalancer.ingress[].ip` / `.hostname`（type LoadBalancer：雲端 LB / MetalLB）
- （選配）NodePort + Node `ExternalIP`

版本化上，`spec.externalIPs` 在 spec（既有 spec-hash 版本化涵蓋）；`status.loadBalancer` 在 status
（須 status 版本化，見主文件風險 #1）。

### ClickHouse 三跳 SQL、效能與調校

**核心前提：join key 在 ingest 期就抽好、正規化成 `Array(String)` 欄**（排序後的 `"k=v"` token；
IP 抽成 union 陣列）。查詢期 `JSONExtract` 會主導延遲並讓 skip-index 失效，故 writer 先物化這些欄。

**DDL（interval 模型；`valid_to` 用遠未來 sentinel，讓 live-at-T 是乾淨兩側 range）：**

```sql
-- 只存 ingress LB Service（非全 Service）
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

**子集判斷 = `hasAll(set, subset)`（`set ⊇ subset` 回 1）。三跳：**

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

之後 `gwresolve(host, candidates)` 在 Go 做 most-specific host 消歧（或把 `server_hosts` 比對下推 Hop 3）。

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
  （POC 壓到 600）× 每資源數版本 → 數千至最壞低百萬列。每跳只碰幾個 granule、`hasAll` 掃數百列，
  **掃描本身是微秒級**。
- **延遲下限是 ClickHouse 固定 per-query overhead（~1–5ms：規劃、thread、`FINAL` merge-on-read），非掃描**。
  量級：單一 join 查詢 ≈ **1–5ms**；三次 round-trip ≈ **5–15ms**。對取證/低 QPS（Q1）完全夠。
- **誠實 caveat**：ClickHouse 是大掃描 OLAP 引擎，這種小 keyed point-lookup 非其強項（無真正 PK point-get、
  per-query 下限偏高）。它「可用，因為 config store 本來就在 ClickHouse、join 留在 store 內」，**非此形狀的最佳引擎**。
  若日後變成**線上熱路徑（數百–千 QPS）**，下限 + `FINAL` 會咬人 → 退到 in-memory 索引或 ClickHouse
  **dictionary**（真 O(1)）——即下節方案 2/3 的物化。

**Fine-tune（依槓桿由大到小）：**

1. **ingest 期就抽好並正規化 join 欄**（`selector_kv`/`pod_labels_kv`/`ingress_ips` 排序 `Array(String)`）——
   最大槓桿，去掉查詢期 JSON parse、讓 skip-index 生效；排序也避免格式漂移害 `hasAll`。
2. **`valid_to` sentinel**（interval 模型）給乾淨兩側 range。或無 mutation 的 **Model 2**：只存 `valid_from`，
   查詢用 `... WHERE valid_from<=T ORDER BY valid_from DESC LIMIT 1 BY namespace,name`（或 `ASOF JOIN`）
   還原 live-at-T，免維護 `valid_to`，代價是讀稍重。
3. **ORDER BY = identity + `valid_from`**（同時是 ReplacingMergeTree dedup key），讓時間 range + namespace
   等值走 sparse primary index；`bloom_filter` skip-index 放 `ingress_ips`（Hop1）、選配 `selector_kv`。
   小資料時 skip-index 影響小，retention 長大後才顯著。
4. **單一 join 查詢 + parameterized/prepared statement** 攤掉固定 per-query overhead 與規劃成本。
5. **管好 `FINAL` 成本**：此規模可忽略；若浮現，改用 `argMax(col, ingest_seq)` + `GROUP BY identity,valid_from`
   手動 dedup，或確保 parts 已合併；Model 2 直接免 `FINAL`。
6. **`LowCardinality`** 放 `namespace`/`gvk`；**`PARTITION BY toYear(valid_from)`** 只在 retention 很長才用
   （小資料過度分區反而害，parts 太多）。
7. **變熱時的逃生口**：ClickHouse **dictionary**（key=IP）或 app in-memory 衍生索引，繞過 per-query 下限。

**正確性註記**：Hop2 要 `svc.selector ⊆ pod_labels`、Hop3 要 `gateway.selector ⊆ pod_labels`，兩者都是
**同一個 L** 的子集——這正是用 Deployment pod-template labels 當 L 為一致正確錨點的原因。若多個 ingress
Deployment 命中（canary/blue-green），對 L 取 union 或逐 Deployment 評估。

### 三種索引落地方式（與 per-resource store 的關係）

| 方案 | 做法 | 適用 |
|------|------|------|
| **1. Query-time 重算（本案推薦）** | `AsOf(T)` 三跳窄查詢（見上），selector 包含用 `hasAll` 下推 ClickHouse | 取證/低 QPS；store 維持純 per-resource、binding 即算即丟、時間一致 |
| **2. In-memory 預建索引** | Consumer 在 watch 事件時更新 RAM 中 `IP→[]Gateway` | 只加速「當下時刻」線上查詢；對任意歷史 T 無用；store 仍唯一真相、可重算 |
| **3. 版本化 binding 記錄** | 另存 `IngressGatewayBinding{ip, gateway, valid_from, valid_to}` | **打破 per-resource 設計**（衍生關係、需 temporal-join writer + 對帳）；僅在極大規模＋高 QPS＋要 binding 獨立查詢才升級 |

**不要把 `IP → Gateway` 塞進 Prometheus label**（高基數關聯邊，重蹈 TSDB 不適合路由查詢的結論）。

POC 的 `gwresolve.New(gateways)` 已是「查詢端 in-memory 索引」先例；候選 Gateway 由上面三跳縮出後，
`gwresolve(host, candidates)` 做最終 host 消歧。

---

## 與原六步流程的對照（簡化後）

使用者最初提出的流程與簡化建議：

| 原步驟 | 簡化後 |
|--------|--------|
| 1. Collector DNS lookup | 保留；**優先**用 span 既有連線 IP |
| 2. Watch 全 Service 找 external IP | 僅 watch **ingress LB Service**；讀 `status.loadBalancer` |
| 3. Service selector → ingressgateway | **刪除**（與步驟 4 合併為 label 集合） |
| 4. Gateway selector → ingressgateway | **預建** `IP → []Gateway`（Consumer C 或 query-time 重算） |
| 5. host resolve + 生成 RC | **保留**（候選內 `gwresolve` + `rccache`） |
| 6. `router_check_tool` | **保留**（按 gateway 批次，見 `simulate.Engine`） |

---

## 何時需要 traffic 路徑、何時 config_only 就夠

**需要 IP / DNS / pod labels 路徑**：

- 多套 ingress（public / internal），DNS 分到不同 LB
- 萬用 host 重疊、多 Gateway 可能 match 同一 host（如 POC `gw-broad-all`）
- 要回答「這筆流量**實際**打到哪顆 ingress」

**config_only 即可**（加 IP 不更準、只更慢）：

- 單一 ingress deployment、單一 LB
- 無重疊 wildcard、每 host 唯一 Gateway CR

---

## 風險與注意事項

1. **LB IP 在 status**：history writer 若只對 spec-hash 開版本，IP 分配不會留下記錄 →
   ingress Service 的 **status 變更應觸發新版本**（或獨立 status 時間序列）。
2. **多 A record / CNAME**：候選 IP 取 union；雲端 LB 僅 hostname 時需 resolve 並在 status 變更時更新索引。
3. **Port / TLS**：DNS 不含 port；query 需預設 port（如 443）或要求呼叫方提供。
4. **POC 離線**：`scalegen` 無真實 DNS/LB → 需 mock `IngressIndex` fixture 才能測 traffic 路徑。
5. **效能**：`gwresolve` + `rccache` + 按 gateway 批次的 `router_check_tool` 維持不變；
   新增成本主要在 ingest DNS（可 cache）與 `IngressIndex.Lookup`（O(1)）。

---

## 與主設計文件的關係

- 主文件 **Q7**：gateway 由 host 反查 `Gateway.servers[].hosts` —— 仍為 **config_only** 的確定性流程。
- 本文件在 **traffic_simulation** 模式下，於 host 反查**之前**增加 `dst_ip → 候選 Gateway` 篩選。
- 儲存層仍採 per-resource 版本化（Postgres / Mongo / TSDB+NoSQL hybrid）；`IngressIndex` 是
  query 加速層，不取代 exporter 或 history writer。
- 引擎 2A 實作細節與 POC 差異見主文件「引擎 2A 的 POC 實作現況」及 `poc/route2a/README.md`。

---

## 待實作清單（參考）

- [ ] OTEL collector：span 連線 IP 優先、DNS lookup processor + cache
- [ ] ingest：版本化 ingress Deployment（pod-template labels 當 L 來源）+ ingress Service 的 IP 多欄位 union（`spec.externalIPs` ∪ `status.loadBalancer` ∪ 選配 NodePort+Node ExternalIP）
- [ ] ClickHouse：ingest 期物化 `selector_kv` / `pod_labels_kv` / `ingress_ips`（排序 `Array(String)`）+ `bloom_filter` skip-index
- [ ] query 端 IP→Gateway 三跳 `hasAll` 下推查詢（as-of-T）+ `gwresolve(host, candidates)`
- [ ] `gwresolve`：支援 `ResolveAmong(host, candidates []string)`
- [ ] `simulate.Engine`：可切換 `config_only` / `traffic_simulation`
- [ ] POC fixture：mock 三跳查詢結果對照 `scalegen` oracle
