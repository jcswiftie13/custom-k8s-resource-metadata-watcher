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
| 查詢時逐步 watch 比對 vs 預建索引？ | **預建索引**（watch 事件驅動），查詢時 O(1) `IP → []Gateway`；勿在 query path 掃全 Service。 |
| Service selector 與 Gateway selector 要完全相等？ | **不要**。安全關聯條件：`Gateway.selector ⊆ Service.selector`（子集，非 `DeepEqual`）。 |
| 預建索引 vs per-resource 版本化 store？ | **不衝突**。per-resource store 是 source of truth；索引是 **derived view**（可記憶體、可重算、可選持久化）。 |
| ingress LB IP 存在哪？ | `Service.status.loadBalancer`（非 `spec.externalIPs`）；status 變更須能版本化，否則 IP 映射不可靠。 |
| OTEL ingest 是否一定要 DNS lookup？ | **優先使用 span 既有連線 IP**；僅在缺少 IP 時由 collector lookup processor 補 DNS。 |

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

### 三種索引落地方式（與 per-resource store 的關係）

| 方案 | 做法 | 與 store 關係 |
|------|------|----------------|
| **1. Query-time 重算** | `AsOf(T)` 讀 Gateway + ingress Service → 記憶體建 map | 最貼 per-resource 設計；每次查詢或每時間切片重算 |
| **2. In-process 預建（推薦起步）** | Consumer C 在 watch 事件時更新 RAM 中的 `IngressIndex` | store 仍為唯一持久真相；索引可丟棄、可重算 |
| **3. 版本化 binding 記錄** | 另存 `IngressGatewayBinding{ip, gateway, valid_from, valid_to}` | 適合查詢服務獨立擴展、大量歷史區間查詢 |

**不要把 `IP → Gateway` 塞進 Prometheus label**（高基數關聯邊，重蹈 TSDB 不適合路由查詢的结论）。

POC 的 `gwresolve.New(gateways)` 已是「查詢端 in-memory 索引」先例；`IngressIndex` 同類，
只是 key 從「全 Gateway host patterns」改為「先以 IP 縮候選」。

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
- [ ] `internal/ingressindex`：`Gateway.selector ⊆ Service.selector` 建 `IP → []Gateway`
- [ ] `gwresolve`：支援 `ResolveAmong(host, candidates []string)`
- [ ] history writer：ingress LB Service 的 status.loadBalancer 版本化
- [ ] `simulate.Engine`：可切換 `config_only` / `traffic_simulation`
- [ ] POC fixture：mock IngressIndex 對照 `scalegen` oracle
