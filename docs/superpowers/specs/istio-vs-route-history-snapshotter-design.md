# K8s 資源歷史快照器（Resource Snapshotter）— 設計備忘錄

> 狀態：brainstorming 持續進行中，已收斂多輪部署/傳輸層決策、擷取機制改為事件驅動、HA/replica 採 leader election。待使用者最終審閱後進入 writing-plans。

## 1. 問題與目標

起點需求（具體案例）：給定一個 Istio `VirtualService` 的 host（例如 `product.service.com`）、一個 path（例如 `/api/v1/users`）與一個時間區間（例如「今天 11:00–12:00」），需要查出在該區間內，符合該 host/path 的路由規則「依序」會解析到哪些 destination（K8s Service + namespace + port）。

VirtualService 是這個能力的**第一個具體案例**，但同樣的問題（有序/巢狀設定、需要任意時間區間回溯查詢）未來可能出現在其他資源上（例如 Gateway API `HTTPRoute`、`Ingress` 的 path 規則）。因此本備忘錄涵蓋的是一個**通用的、獨立的能力**——保留任意 K8s 資源 spec 的歷史版本——不是對現有 `pkg/collector` 的擴充，也不是只服務 VirtualService 一種資源。

## 2. 範圍界定（這個 repo 負責什麼）

這個 repo 新增一個獨立二進位檔 **`cmd/resource-snapshotter`**（通用命名；VirtualService 是設定檔裡的第一個案例，不是寫死在程式碼裡的特例），職責**僅限於擷取與寫入**：

- 持續觀察叢集中**設定檔宣告的**資源物件（GVR 任意，不限 VirtualService）。
- 把每個物件 spec 的**歷史版本**（含有效時間區間）寫入共用的 Postgres。

**不包含**：

- 查詢 API、HTTP 介面。
- Host/path 比對邏輯（first-match-wins、URI exact/prefix/regex 比對、TCP/TLS match 屬性比對等）——這是 VirtualService 案例特有的比對邏輯，不屬於這支通用擷取程式的職責。

查詢與比對邏輯在**另一個 repo**實作；兩個 repo 之間以 Postgres schema 為公開契約（見第 5 節）。

### 為何不直接重用現有 Collector

現有 `pkg/collector.Collector` 的核心設計原則是「scrape-time 重建、無任何跨 scrape 狀態」（見 [collector.go](../../../pkg/collector/collector.go) 開頭註解）。本需求恰好相反——核心價值就是「保留歷史狀態以供回溯查詢」。把兩種模型硬塞進同一個 binary，會讓測試、RBAC 範圍、效能 SLO（現有 benchmark 假設無狀態）都變得難以推理，因此選擇新增獨立二進位檔，但重用既有的 dynamic client / informer 程式碼模式。

### 為何不是「另一個 repo 透過 `/metrics` scrape 再寫 Postgres」

這是個容易被誤判成「團隊分工偏好」的問題，但實際上是由**資料形狀**決定的，不是分工問題：

- Prometheus exposition format 的本質是扁平、無序的 key=value label 集合。VirtualService 的 `http[]` 是**有序陣列**（順序即語意：first-match-wins）且**巢狀**（每條規則內還有 `match[]`/`route[]`）。把它**展開**成多個 atomic label（用 index 當 label）等於在 `pkg/collector` 裡重新發明一套「保序陣列展開」功能，直接違反它現有的「flat labels、無序語意」設計原則。
- **已考慮並否決的替代方案**：把整段 `.spec.http`/`.spec.tls`/`.spec.tcp` **整段 JSON 字串化**塞進單一 label value（而非展開成多個 label）。這個做法**技術上可行**——字串本身保留了 JSON 的順序與巢狀結構，並不是「裝不下」。但仍不建議採用，理由是：
  1. **Cardinality/儲存反模式**：每次 spec 內容變化，label value 變成新字串，TSDB 視為全新 series；許多 remote-write 後端對單一 label value 大小有限制，且這完全繞過了本專案 `expandLabels` 功能特地設計的 cardinality 控制（`allow`/`deny`/`maxKeys`）。
  2. **不減少實作工作量，只是把它搬到錯的地方**：「另一個 repo」拿到 label 裡的 JSON 字串後，還是要自己解析、算 hash、判斷異動、維護 SCD2 版本——這些工作量跟現在設計裡這個 repo 要做的事完全一樣，只是多繞一圈序列化/反序列化，而這個 repo 明明手上已經有解析好的物件，直接寫 Postgres 是零序列化往返。
  3. **無 diff，每次 scrape 全量重傳**：不管 spec 有沒有變，每個 scrape interval 都重傳一次完整內容；直接寫 Postgres 則只有 hash 變了才落地新 row。
- 因此，不論在哪個 repo 實作，只要傳輸路徑走 `/metrics` 的「展開 label」模式，VS 的核心需求（完整保序保結構）就無法達成；而「JSON 字串塞 label」雖然技術可行，但成本（cardinality、重複實作、邊界耦合、無 diff）大於直接寫 Postgres。唯一能在不犧牲上述任何一點的情況下拿到完整結構的方式，是直接讀 K8s API/informer cache 中**未經處理的 unstructured 物件**——這件事天生就該在「已經在跟 apiserver 對話、已經有 RBAC、已經有 informer 程式碼」的這個 repo 做，而不是讓第二個 repo 重新發明一套 informer 邏輯外加一層沒必要的 HTTP polling round-trip。

## 3. 擷取機制：沿用現有 informer 模式，事件驅動寫入，config-driven 支援多種資源

延用 [pkg/collector/listers.go](../../../pkg/collector/listers.go) 的 `dynamicinformer` 模式，而非另外寫一個輪詢迴圈：

1. 啟動時讀取 YAML 設定中宣告的資源清單（沿用 metadata-exporter 既有 `watch.resources[]` 的格式：`group`/`version`/`resource`/`kind`/`scope`），對每個 GVR 建立 `dynamicinformer.NewFilteredDynamicSharedInformerFactory`，做一次初始 LIST，之後轉為常駐 WATCH 串流維持本地 cache。**不是寫死 VirtualService**——未來要支援 `HTTPRoute`、`Ingress` 等其他資源，只需要在 YAML 加一條設定，不需要改程式碼或開新二進位檔。
2. 對每個 informer 註冊 `AddFunc`/`UpdateFunc`/`DeleteFunc` 事件處理器，**收到事件即處理**，不使用定時 ticker：
   - `AddFunc`/`UpdateFunc`：計算該物件 `spec_hash`。若與 Postgres 中目前「開放中」版本（`valid_to IS NULL`）的 `uid` 相同且 `spec_hash` 相同 → 不寫入；只要 `uid` 不同（代表同名物件被刪除後又重建，即使新舊 spec 恰好相同）或 `spec_hash` 不同 → 把舊版本的 `valid_to` 設為現在，並插入一筆新版本（`valid_from = now`, `valid_to = NULL`）。比對 `uid` 是為了在 `DeleteFunc` 事件被漏接的情況下，仍能偵測出「這其實是新一代物件」而不是誤判成內容沒變。Informer 初始 LIST（cache sync）階段會對每個既有物件觸發一次 `AddFunc`，等同於各物件的起始快照，不需要額外的「初始全量寫入」邏輯。
   - `DeleteFunc`：將該物件目前「開放中」版本的 `valid_to` 設為刪除事件發生時間，**不插入新版本**——代表該資源從此刻起沒有有效版本。
3. `resyncPeriod` 設為 0（與 `pkg/collector` 既有模式一致），不依賴 informer 內建週期性 resync、也不額外加任何輪詢迴圈——純事件驅動。Informer 內部的 watch 斷線重連（relist）機制已提供「漏接事件後自我修復」的保障：reflector 偵測 `resourceVersion` 過舊時會自動全量 relist，對 store 內容差異觸發對應的 synthetic Add/Update/Delete 事件；這些事件經過上述 hash dedup 後天然冪等（無變化則不寫入），因此不需要額外維護一個獨立的安全網全量比對迴圈。

**Self-metrics（觀測面，與資料路徑分離）**：除了把歷史寫入 Postgres，這支程式額外曝光一個 `/metrics` 端點，但**只放程式自身的健康/效能指標**（例如 snapshot 寫入次數、informer 同步狀態、dedup 命中率），讓既有的 OTel collector scrape 管道可以監控它。

**Replica/HA：透過 leader election 確保單一寫入者**：K8s 的 `watch` 不會在多個觀察者之間分流事件——每個 replica 的 informer 都會獨立收到完整的事件串流。若多個 replica 同時對同一物件執行步驟 2 的「比較 hash → 寫入新版本」，會產生競爭：兩個 replica 都可能讀到「hash 不同」而各自插入一筆新版本，導致同一 `(namespace, name)` 同時存在兩筆 `valid_to IS NULL` 的「開放中」版本，違反 SCD2「同一物件僅一筆有效版本」的不變量。

解法採用 `client-go` 的 `leaderelection`（基於 K8s `coordination.k8s.io` `Lease` 物件）：所有 replica 都照常啟動 informer 維持本地 cache，但只有目前持有 leader lease 的 replica，其事件處理器才會真正執行 Postgres 寫入；非 leader replica 收到事件後直接略過寫入。這讓寫入路徑維持單寫入者語意，不需要額外的 DB constraint 或交易層併發控制。代價是 leader 切換期間（例如目前 leader pod 被驅逐或重啟）有一段空窗，這段時間發生的事件不會被處理——效果等同單 replica 重啟的資料缺口（見第 9 節）。

**Leader 接手時的一次性和解（reconciliation）**：上述空窗有個更嚴重的變體——若某物件在空窗期間變化後就不再變化，單純等待「下一次事件」永遠不會再被觸發，會導致 Postgres 永遠停留在舊版本，而不只是漏掉中繼歷史。為避免這個情況，新 leader 在 `OnStartedLeading` 回呼觸發的當下，對本地 cache（此時已透過持續運行的 informer 保持同步）中的每個物件執行一次與 `AddFunc` 相同的比對寫入邏輯（`uid`/`spec_hash` 比對，不同則關閉舊版本插入新版本）。這是由「成為 leader」這個事件觸發的一次性動作，不是週期性 ticker，因此不違反「事件驅動、無輪詢」的設計原則。

**建議預設仍維持單一 replica**：在這個設計下，多 replica 並不提升吞吐量或降低延遲（同一時間仍只有一個寫入者），主要價值僅是「故障後更快接手」；是否值得為此多付出一份運算資源與 Lease 相關的營運複雜度，留給各叢集依可用性需求自行判斷，且這純粹是 CD pipeline 部署設定（`replicas` 值）的選擇，不影響應用程式碼——應用程式碼本身已同時支援單/多 replica 兩種拓樸。

**RBAC**：對每個設定檔宣告的資源需要 `get` / `list` / `watch`（因為要建 informer）。Namespace 範圍延用本專案既有 `watch.resources[].namespaces` 的 YAML 設定風格。若啟用 leader election，額外需要對 `coordination.k8s.io` 的 `leases` 資源（建議建立在程式所在 namespace）授予 `get`/`create`/`update` 權限。

## 4. 資料模型：config-driven GVR + 各資源專屬表（混合方案）

採用「一支二進位檔、多個 GVR、但每種資源各自一張 Postgres 表」的混合設計，而非單一通用表＋`kind` 區分欄位：

```yaml
watch:
  resources:
    - kind: VirtualService
      group: networking.istio.io
      version: v1beta1
      resource: virtualservices
      scope: Namespaced
      historyTable: virtualservice_snapshots
    # 未來新增資源範例（尚未實作，僅示意擴展方式）：
    # - kind: HTTPRoute
    #   group: gateway.networking.k8s.io
    #   version: v1
    #   resource: httproutes
    #   scope: Namespaced
    #   historyTable: httproute_snapshots
```

每張表的欄位結構統一，table name 由設定中的 `historyTable` 指定（取自設定白名單參數化，非使用者輸入字串拼接，避免 SQL injection）：

```sql
CREATE TABLE virtualservice_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    namespace   TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    uid         TEXT        NOT NULL,        -- 與目前開放版本比對；不同即代表刪除後以同名重建，即使 spec_hash 相同也要起新版本
    spec_hash   TEXT        NOT NULL,        -- sha256(normalized spec json)，用於 dedup
    spec_json   JSONB       NOT NULL,        -- 整份 .spec 原樣存：hosts[]、http[]、tcp[]、tls[]，保留原始順序
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ NULL             -- NULL = 目前仍然有效
);

CREATE INDEX idx_vs_snap_identity_time
    ON virtualservice_snapshots (namespace, name, valid_from, valid_to);

CREATE INDEX idx_vs_snap_hosts_gin
    ON virtualservice_snapshots USING GIN ((spec_json -> 'hosts'));
```

要點：

- **整份 `.spec` 原樣存成 JSONB**，不挑欄位——`http[]`／`tcp[]`／`tls[]` 都涵蓋，比對邏輯（另一個 repo 的事）需要哪個欄位都能取得。
- 採用 **SCD Type 2** 模式（`valid_from`/`valid_to`）處理版本歷史。這是「結構化設定隨時間變化、依時間區間查版本」這類問題的標準解法，**不是**時間序列（time series）問題：資源變更頻率低、值是巢狀結構而非數值，TSDB（Prometheus/InfluxDB/VictoriaMetrics 等）的壓縮與索引機制是為高頻數值取樣設計的，硬塞巢狀結構進去等於要重新打平、會丟失順序語意——這正是本需求要避免的事。
- **為何選「各資源專屬表」而非「單一通用表 + `kind` 欄位」**：單一通用表需要對每種資源加 `WHERE kind = 'X'` 的 partial index，查詢端也要記得多帶這個篩選條件，容易漏寫導致撈錯資料。各資源專屬表（欄位結構統一，只是表名不同）一樣達成「加新資源零程式碼改動」的擴展性，但索引可以直接針對該表加（例如 VS 專屬的 `idx_vs_snap_hosts_gin`），查詢端語意更直接。代價是 migration 數量隨資源種類數線性增加，但每張表的 DDL 幾乎是複製貼上，維護成本低。
- 之後若資料量/spec 大小造成問題，可考慮把 `spec_json` 大欄位外移到 object storage（用 hash 當 key，天然去重複），DB 只留輕量索引列；但目前規模下評估為過度設計，先不做。

## 5. 與「另一個 repo」的邊界與部署拓樸

- 共用一個**網路可連線的 Postgres**（而非本機檔案如 SQLite）——因為兩個 repo 預期獨立部署，不保證同 Pod/node。**已確認**：各被監控叢集到 gateway 側（含 Postgres）允許直接對外連線，不需要繞道其他中介。
- **Schema 即公開契約**：上述 DDL 是雙方都要遵守的介面；schema 變更需要兩邊協調。
- **Migration 由這個 repo（寫入端）擁有**——schema 的定義方對其生命週期負責，是較自然的責任邊界。新增資源類型時，這個 repo 也擁有對應的新表 migration。
- DB 連線資訊（host/port/credentials）透過環境變數注入，不寫死在程式碼或設定檔。
- **跨叢集部署**：沿用既有 metadata-exporter 的部署模式——每個叢集一份 Deployment（單一 binary、單一 Pod），透過 GitOps（ArgoCD/Flux）+ 模板化設定（Helm/Kustomize）帶入各叢集差異化的值（namespace 範圍、DB 連線憑證等），DB 憑證的 Secret 注入機制（External Secrets Operator / Vault 或其他）與既有流程一致，不在本備忘錄重新設計。
- **觀測管道**：這支程式的自身健康/效能 self-metrics 透過 `/metrics` 曝光，沿用既有 OTel collector pull/scrape 管道送到 gateway 側監控儲存；這條路徑與「路由歷史資料寫入 Postgres」完全獨立（見第 3 節）。

## 6. 範圍外（Out of scope）

- **多個 `VirtualService` 共用同一 host 的 merge 語意**：根據查證（[istio/istio#58453](https://github.com/istio/istio/issues/58453)、[Istio Traffic Management Best Practices](https://istio.io/latest/docs/ops/best-practices/traffic-management/)），這種合併行為**只在 gateway-bound VS 才支援**，sidecar/mesh-internal 場景（本需求的情境）完全不支援；即使支援的情況，跨 VS 的規則順序也是**未定義**的——Istio 官方建議直接避免這種設定。因此本系統假設「一個 host 只對應一個 VirtualService」；偵測到違反時，寫入端只記錄 **warning log**，兩筆資料仍照樣寫入，不阻擋、不嘗試猜測合併順序。
- **Delegate VirtualService**（root + delegate 拆分路由規則）：暫不支援，留待未來擴充。
- **查詢 API 與比對演算法**（host/path/interval → destination(s) 的 first-match-wins 邏輯、URI exact/prefix/regex、TCP/TLS match 屬性比對等）：明確排除在這個 repo 之外，屬於另一個 repo 的範圍。這些邏輯是 VirtualService 案例特有的語意，即使未來新增其他資源類型，各資源的比對邏輯仍各自留在查詢端，不會被吸收進這支通用擷取程式。

## 7. 運維

- **保留期**：可設定，預設 30 天；定期清除 `valid_to` 早於保留期界線的已關閉版本。
- **告警**：偵測到「同一 host 同時被多個 VS 宣告（且時間區間重疊）」時記錄 warning log（見第 6 節）。
- **可觀測性**：透過 `/metrics` 曝光的 self-metrics（見第 3 節）讓既有 OTel/Prometheus 維運流程可以監看這支程式本身的健康狀況，不需要額外建置監控管道。

## 8. 決策歷程摘要

| 主題 | 最終決定 | 备注 |
|------|----------|------|
| 整體架構 | 這個 repo 只做擷取+寫入，新二進位檔，不含查詢 API | 查詢/比對在另一個 repo |
| 二進位檔命名 | `cmd/resource-snapshotter`（通用），非 `cmd/vs-snapshotter` | VirtualService 是設定裡的第一個案例，不是寫死的特例 |
| 儲存技術 | Postgres（關聯式 DB，SCD2 模式） | 排除 TSDB；理由見第 4 節 |
| 資料範圍 | 整份 `.spec`（含 `http`/`tcp`/`tls`） | 不只 `hosts`/`http` |
| 擷取機制 | 沿用既有 dynamicinformer 模式（LIST+WATCH+本地 cache），改為事件驅動（`AddFunc`/`UpdateFunc`/`DeleteFunc`），`resyncPeriod=0`，無 ticker | 維持本專案「啟動後零額外 API 呼叫」原則 |
| 刪除事件處理 | `DeleteFunc` 觸發時關閉目前開放版本（`valid_to=now`），不插入新版本 | 對應「物件被刪除」情境，先前 polling 設計未涵蓋 |
| 漏接事件防護 | 不額外加週期性安全網全量比對，依賴 informer relist 機制 + hash dedup 的冪等性自我修復 | relist 觸發的 synthetic 事件無變化則不寫入，不需獨立 ticker |
| Replica/HA | 支援多 replica，透過 `client-go` `leaderelection`（K8s `Lease`）確保同一時間只有一個 replica 真正寫入；預設建議仍為單一 replica | 不需要 DB constraint 或交易層併發控制；多 replica 不提升吞吐量，主要價值是故障接手；replica 數量純屬 CD pipeline 部署設定 |
| Leader 接手和解 | `OnStartedLeading` 時對本地 cache 全量跑一次與 `AddFunc` 相同的比對寫入邏輯 | 避免空窗期間變化後未再變化的物件永遠停留在舊版本，而不只是漏中繼歷史 |
| Dedup 鍵 | 比對 `uid` + `spec_hash`，`uid` 不同即視為新一代物件起新版本（即使 spec 相同） | 偵測 `DeleteFunc` 被漏接時的「同名刪除後重建」情境 |
| 多資源擴展性 | config-driven 多 GVR，一支二進位檔，各資源各自一張表（`historyTable` 設定欄位指定表名） | 避免每加一種資源就重複一套 informer/hash/dedup/部署邏輯；也避免單一通用表需要的 `kind` 區分欄位與 partial index |
| 為何不走「另一個 repo scrape `/metrics`」 | 否決；展開成 label 會破壞順序/結構，整段 JSON 塞 label 技術可行但 cardinality/重複實作/邊界耦合代價過高 | 詳見第 2 節 |
| 跨叢集網路 | 各叢集直接連線到 gateway 側 Postgres，已確認允許 | 排除了被迫繞道 `/metrics` 的限制 |
| Self-metrics | 額外曝光 `/metrics`，但只放健康/效能指標，不含路由歷史資料本身 | 與資料路徑（直連 Postgres）分離，沿用既有 OTel scrape 慣例 |
| 跨叢集部署方式 | GitOps（ArgoCD/Flux）+ 模板化設定 | 與現有 metadata-exporter 部署方式一致 |
| 多 VS 同 host | 假設一對一，違反時只記 warning，不做 merge | Istio 本身對此無保證順序 |
| 兩 repo 邊界 | 共用網路可連線 Postgres，schema 為契約，由本 repo 維護 migration | — |
| 保留期 | 預設 30 天，可設定 | — |

## 9. 待辦 / 下一步

- [x] ~~Replica/HA 尚待決定~~：已決定支援多 replica + leader election，預設建議單一 replica（見第 3、8 節）。**仍需接受的代價**：不論單或多 replica，leader 不在線期間（單 replica 重啟、或多 replica 下 leader 切換空窗）發生的事件都會漏接；重新取得 leadership 後，informer 初始 LIST 只能補上「當下最新狀態」的一筆快照，期間發生過的任何中繼版本無法復原。
- [ ] 使用者最終審閱本備忘錄（含本輪新增的部署/傳輸層決策）。
- [ ] 進入 writing-plans：拆解成可執行的實作步驟（informer 設定、hash/dedup 邏輯、Postgres schema migration、設定檔 YAML 擴充、RBAC manifest、self-metrics 端點、GitOps 部署模板、測試策略）。
