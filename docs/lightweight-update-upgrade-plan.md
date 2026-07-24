# Lightweight UPDATE 升級計劃 — history 版本關閉改用原地更新

> 狀態：**已實作**（`store.closeMode: update`，預設仍為 `rewrite`）。前置閱讀：
> `docs/istio-virtualservice-routing-history-design.md`、`docs/CONFIG.md` §14.5、
> `poc/route2a/internal/chstore/chstore.go` package doc（no-FINAL reader 模式）。
>
> **實測結論（ClickHouse 25.8.28 / 26.5.5.8，2026-07）**：
> - lightweight `UPDATE` 在 25.8+ **預設啟用**（`enable_lightweight_update = 1`、
>   `allow_experimental_lightweight_update = 1`），無需 per-query 設定。26.5.5.8 亦同；
>   整合測試／poc／devcontainer 的 users.d profile 與 `deploy/clickhouse` Helm chart
>   顯式對齊上述三項設定。
> - **需要表級設定** `enable_block_number_column = 1, enable_block_offset_column = 1`，
>   否則 UPDATE 報 `NOT_IMPLEMENTED`；對既有表 `ALTER TABLE ... MODIFY SETTING` 即可，
>   **舊 parts 也隨即可被 UPDATE**（實測通過）。
> - ReplacingMergeTree + bloom skip index 相容；UPDATE 立即可見；重放（0 匹配）冪等無副作用。
> - **driver 陷阱（e2e 實際踩到）**：clickhouse-go 對 `Exec`/`Query` 的 `?` 參數把 `time.Time`
>   插值成**秒精度** `toDateTime('...')`——(a) 毫秒被截掉；(b) FarFuture 哨兵 `2200-01-01`
>   超出 32-bit DateTime 上限（2106）被飽和，`valid_to = ?` **永遠 0 匹配且無錯誤**，關版
>   靜默失效。修法：UPDATE/OpenVersions 的時間運算元一律用明確
>   `toDateTime64('YYYY-MM-DD HH:MM:SS.mmm', 3, 'UTC')` literal（`pkg/store/close.go` 的
>   `dt64Lit`），不可用 `?` 繫結時間。批次 INSERT 不受影響（PrepareBatch 知道欄位型別）。
>
> **落地位置**：`pkg/config`（`closeMode` 驗證/預設）、`pkg/store`（`CloseVersion`/`OpenVersions`/
> DDL patch-part 設定 + validate fail-fast）、`pkg/history`（close op 佇列、flush「先 INSERT 後 close」
> 順序、`Recover()` 重啟恢復 + stale open 掃除）、`cmd/main.go`（Recover 於 informer 起動前）。
> e2e：`istioHistoryConfigYAML` 以 `closeMode: update` 全跑，`TestHistory_RoutingResolution/
> update_close_restart_idempotency` 驗證「每版本恆一列 + 重啟不重插 + 重啟後變更正確關閉恢復的版本」。

## 1. 動機與現況

**現況（rewrite-close 模型）**：exporter history ingest（`pkg/history` + `pkg/store`）以
append-only 批次 INSERT 寫入；「關版」= 重寫前一 open row（同 ORDER BY key、更高
`ingest_seq`、`valid_to` 收攏）。在背景 merge 收斂前，同 key 同時存在 stale open row
（`valid_to` = 2200 sentinel）與 closing row 兩行；writer 重啟後的 re-LIST 重插也會產生
同形狀的重複。

**Reader 已採 no-FINAL 去重模式**（本計劃的前置，已落地於 poc/route2a 與
test/integration/routesim）：SQL WHERE 只留不可變條件（join 欄、`valid_from`），client 端
依 version slot `(namespace, name, valid_from)` 取 max `ingest_seq`，`valid_to` 的
liveness/overlap 判斷在去重**之後**於 Go 端執行。代價：會多抓「在查詢窗口前已結束」的
版本（受每 key 版本數與 TTL 上限約束）。

**本計劃的目標**：writer 改用 ClickHouse lightweight `UPDATE`（patch parts）原地收攏
`valid_to`，讓**每個版本恆為唯一一行**。屆時：

- 讀取端可把 `valid_to` 條件搬回 SQL WHERE（不再多抓已結束版本）；
- client dedup 降級為無害的 safety net；
- 不再依賴 merge 時機，資料語意在寫入當下即最終。

## 2. 前提條件（升級 gate，缺一不可）

| # | 條件 | 驗證方式 |
|---|------|---------|
| 1 | ClickHouse ≥ **25.7**（lightweight `UPDATE ... SET` / patch parts） | 所有環境已升版至 **26.5.5.8**：prod Helm（`deploy/clickhouse`）、`test/integration/manifests/clickhouse-deployment.yaml`、poc `Makefile` `CH_IMAGE`、`.devcontainer/docker-compose.yml` |
| 2 | 確認該版本 lightweight UPDATE 的成熟度旗標（初期為 `allow_experimental_lightweight_update`；以部署版本的 release note 為準） | `SELECT value FROM system.settings WHERE name LIKE '%lightweight_update%'` |
| 3 | 被更新欄位不在 ORDER BY key | 已成立：`valid_to` 刻意排除在 `(namespace, name, valid_from, resource_version, deleted)` 之外（`pkg/store/ddl.go`） |
| 4 | UPDATE 後續讀立即可見（26.5+ 已內建；`apply_patches_on_read` profile 設定已移除） | 設定檢查 / e2e `valid_to` 子測試 |
| 5 | **Writer 重啟冪等**（見 §3.3）——這是拿掉 reader 去重保險的必要條件，不做則 dedup 必須永久保留 | 重啟測試 |

## 3. Writer 改動（`pkg/history` / `pkg/store`）

### 3.1 事件寫入路徑

| 事件 | 現行（rewrite-close） | 升級後（update-close） |
|------|----------------------|----------------------|
| Add | INSERT 新 open row | 不變 |
| Update | 重插前版（valid_to=新版 valid_from、seq+1）+ INSERT 新版 | `UPDATE <table> SET valid_to = {new.valid_from} WHERE namespace=? AND name=? AND valid_to = {sentinel}` + INSERT 新版 |
| Delete | 重插前版（valid_to=now、seq+1） | `UPDATE ... SET valid_to = {now} WHERE ... AND valid_to = {sentinel}` |

要點：

- UPDATE 以 `valid_to = sentinel` 當條件鎖定 open row，天然冪等（重放同一 UPDATE 匹配 0 行）。
- UPDATE 匹配 0 行不是錯誤（open row 可能已被前次關閉）；但應記 metric 供觀察。
- UPDATE 無法批次化：每次關版一個語句、一個 patch part。K8s watch 事件率低，可接受；
  仍須監控 patch parts 堆積（§6）。
- `ingest_seq` 欄位與 `ReplacingMergeTree` 引擎**保留不動**（回滾相容 + 歷史資料收斂用）。

### 3.2 批次語意調整

現行 batch（`maxRows` / `flushIntervalMs`）只 buffer INSERT。升級後同一資源的
「close(UPDATE) → insert(新版)」必須維持順序：UPDATE 不可先於它要關閉的那行 INSERT 落地。
最簡單的正確做法：flush 當前 INSERT batch 之後才發 UPDATE，或改為每事件同步
「UPDATE 後 INSERT」（事件率低時直接同步最簡單）。

### 3.3 重啟冪等（必要）

現行 ingester 的 last-state（`map[uid]{hash,row}`）在記憶體，重啟即失；re-LIST 會把既有
物件當 Add 重插——rewrite-close 模型下靠 ReplacingMergeTree + reader 去重吸收。若目標是
「行唯一、reader 免去重」，writer 啟動時必須先**從 ClickHouse 恢復 last-state**：

```sql
SELECT namespace, name, argMax(spec_hash, ingest_seq), max(valid_from)
FROM <table> WHERE valid_to = {sentinel}
GROUP BY namespace, name
```

re-LIST 時 spec_hash 相同 → 略過；不同 → 正常走 update-close 路徑。做不到這點就永久保留
reader dedup（見 §4），計劃其餘部分仍可進行。

## 4. Reader 改動（選配、最後做）——**已實作（opt-in）**

行唯一保證成立（§3 全部完成 + §5 歷史收斂完成）後，reader 以 **`WithUniqueRows()` 選項**
（顯式 opt-in，預設仍為 superset+dedup 模式）啟用剪枝讀取：

- `valid_to` 條件搬回 SQL WHERE（點查詢 `t < valid_to`、範圍 `t0 < valid_to`），不再多抓
  已結束版本。時間運算元一律 `toDateTime64(...,3,'UTC')` literal（`dt64Lit`），不可 `?`
  繫結（見前言 driver 陷阱）。
- `dedupLatest` / `dedupOverlap` **保留**為 safety net（行唯一時是 no-op），並以
  **`CollapsedRows()` 計數器**告警：pruned 模式下收斂 > 0 = writer 唯一性保證被打破
  （production 移植時接 metric/alert；e2e 直接斷言 == 0）。

落地位置：`poc/route2a/internal/chstore`（全部 reader：3-hop、`AllGatewaysLiveAt`、
`ScopedFor`、`backendServices`、`LoadTrafficWindow`）+ `internal/storeopen`
（`POC_CH_UNIQUE_ROWS=1` 開啟）；`test/integration/routesim/internal/chstore`
（`LoadTrafficWindow`）+ `resolve_e2e_test.go`（`ROUTESIM_UNIQUE_ROWS=1` 開啟並斷言零收斂，
host 側 e2e 因 config 為 `closeMode: update` 而恆傳）。

實測（50gw/20vs、每 key 20 版、窄窗口只涵蓋開放版）：lookup p50 **38ms → 18ms**（~2x），
0 mismatches——收益隨「每 key 版本數 × 窗口外已結束版本比例」放大。

**關鍵遷移性質（維持不變）**：預設（不開 `WithUniqueRows`）的 no-FINAL reader 對
rewrite-close 與 update-close 兩種 writer **都正確**（去重對唯一行是 no-op）。因此 writer
可以漸進切換、隨時回滾；剪枝模式是確認全量切換 + 歷史收斂之後才打開的效能開關，
**絕不可對 rewrite-close writer 開啟**（closing row 會被 SQL 先濾掉、stale sentinel row
無對手勝出——no-FINAL 模式要避免的那個失效原樣回歸）。

## 5. 遷移步驟（依序）

1. **升 ClickHouse**：prod / e2e manifests / poc CH_IMAGE → **26.5.5.8**（已完成）；確認 §2 旗標。
2. **歷史資料收斂（一次性）**：`OPTIMIZE TABLE <t> FINAL` 四張表，把既有 rewrite-close
   的重複對收斂掉（此後 reader 若搬回 WHERE 過濾才不會踩到舊重複）。
3. **部署新 writer**（update-close + 重啟冪等），reader 不動（去重模式相容兩者）。
4. **觀察期**：§6 指標穩定、e2e 通過。
5. **（選配）reader 開啟剪枝模式**（`WithUniqueRows` / `POC_CH_UNIQUE_ROWS=1` /
   `ROUTESIM_UNIQUE_ROWS=1`），dedup 留作 safety net，`CollapsedRows > 0` 告警。

回滾：任一步失敗，writer 退回 rewrite-close 即可——reader 去重模式對兩者都正確，
資料不需修復（混寫期的資料兩種 reader 語意一致）。

## 6. 監控與風險

| 風險 | 監控/緩解 |
|------|----------|
| patch parts 堆積（高 churn 下每次 UPDATE 一個 patch part，read 端套用成本上升） | `system.parts`（patch parts 計數）；merge 落後告警；事件率異常時退回 rewrite-close |
| 實驗旗標行為隨版本變動 | 釘住 CH 版本；升版前在 e2e 全跑一輪 |
| UPDATE 失敗/逾時（mutation 佇列壓力） | writer metric：update 失敗率、匹配 0 行率 |
| 混寫期（舊 writer 未全下線） | reader 去重模式本來就相容；§4 必須等全量切換 + §5.2 收斂後才做 |
| 重啟冪等實作缺陷 → 重複 open row | dedup safety net 的收斂計數告警（§4） |

## 7. 驗證

1. **e2e**：`TestHistory_RoutingResolution` 不改一行應照樣通過（update-close 下
   `vs_versions FINAL` 斷言等價成立；host 測試裡少數 `FINAL` 查詢在行唯一後也是 no-op）。
2. **重啟測試**：exporter 於多版本狀態下重啟 → 表中 open row 每資源恰一行、無新重複。
3. **POC bench**：`bench-worst` 的 lookup 階段在 reader 搬回 WHERE 過濾後應再降
   （不再抓已結束版本）；對照本次 A 方案基準：FINAL p50 822ms / p99 1.64s →
   no-FINAL+dedup p50 634ms / p99 815ms（600gw/100vs corpus, 2026-07）。
4. **Patch 壓力測試**：模擬高頻 VS 更新（如每秒數次、持續數分鐘），觀察 patch parts
   與查詢延遲曲線。
