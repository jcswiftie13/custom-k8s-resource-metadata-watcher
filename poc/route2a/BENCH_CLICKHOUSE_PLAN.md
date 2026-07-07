# 計劃：把 ClickHouse `IP→Gateway` 3-hop 併進「既有」benchmark

> 狀態：**計劃文件（尚未實作）**。本檔描述後續要做的改動；程式碼日後再依此執行。
> 相關：`README.md`（怎麼跑）、`docs/istio-virtualservice-routing-history-design.md`（設計 §5-B / POC 節）、
> 既有 `cmd/ipflow`＋`internal/chstore`＋`internal/ingload`（已實作的 ClickHouse `IP→Gateway` 3-hop 與
> CH-backed 翻譯）。

## 目標

**不另開 benchmark**，而是讓**既有** `make bench-warm`（跑滿 60k）與 `make bench-worst`（sampled 單筆最差）
的整條管線改成走 ClickHouse：

```
host + path + IP
   → ClickHouse 3-hop 找候選 gateway (IP→ingress Service→Deployment L→Gateway)
   → gwresolve 在候選內 most-specific host 消歧
   → translate（設定亦從 ClickHouse 取）→ RouteConfiguration
   → router_check_tool → cluster
```

並且 **每個階段都統計執行時間**（新增 `lookup` 3-hop 階段，與既有 resolve/translate/check/total 並列），
同時 **精簡 make 指令**（移除多餘的 `ch-*`）。

設計取捨（沿用先前決定）：translate 也吃 ClickHouse（`chstore.ScopedFor`），故整串走 CH；3-hop 逐 query
序列執行，符合本 repo「單複本序列化吞吐」的量測哲學。

## 改動點（逐檔）

### 1) `internal/gwresolve/gwresolve.go` — 加 `ResolveAmong`

```go
// ResolveAmong is Resolve limited to a candidate gateway set (the IP-narrowed
// candidates from the 3-hop). Most-specific match among candidates.
func (r *Resolver) ResolveAmong(reqHost string, candidates []string) (string, bool)
```

用既有 `r.pats`（建構時已依 most-specific 排序）掃描，只接受 `pat.gw ∈ candidates` 的 pattern；
第一個命中即答案。`Resolve` 不動（`ResolveAmong` 只是多一個候選過濾條件）。

### 2) `internal/report/report.go` — `Stages` 加 `Lookup`

- `Stages` 新增 `Lookup Hist`（IP→候選 gateway 的 3-hop，**per query** 取樣）。
- `Markdown()` 在 `resolve` 之前多印一列：`lookup (IP→candidates 3-hop, per query)` 的 p50/p99/mean。

### 3) `internal/simulate/simulate.go` — 加可選 `Lookup` 階段

- `Config` 新增：
  ```go
  // Lookup, when set, resolves the candidate gateways from a request's traffic IP
  // (the ClickHouse 3-hop) before host disambiguation. nil => resolve over all gateways.
  Lookup func(host string) ([]string, error)
  ```
- `Metrics.Stages`（＝ `report.Stages`）自動帶 `Lookup`。
- `ResolveAll` 的 stage-1（逐 query）：
  - `Lookup != nil`：先計時 `cands, err := cfg.Lookup(q.Host)` → 記入 `Stages.Lookup`；再計時
    `cfg.Resolver.ResolveAmong(q.Host, cands)` → 記入 `Stages.Resolve`。
  - `Lookup == nil`：維持原本 `cfg.Resolver.Resolve(q.Host)` → `Stages.Resolve`。
- **無介面重構**：`Config.Resolver` 仍是 `*gwresolve.Resolver`（§1 幫它加了 `ResolveAmong` 方法即可）。
- 相容性：`Lookup == nil` 時行為與現況完全一致，既有非 CH 呼叫端（`ip_flow_test`、`cmd/ipflow` 的 per-case
  resolver）不受影響。

### 4) `bench_test.go` — 既有兩個 benchmark 改吃 ClickHouse（不新增測試）

- `buildEngine(g, mode, runner)` → `buildEngine(g, mode, runner, store)`，多接一個 `*chstore.Store`：
  - `Lookup = func(host string) ([]string, error) {`
    `  ip, ok := g.IPForHost(host); if !ok { return nil, nil } // miss host（broad/unknown）無 traffic IP`
    `  cands, err := store.ResolveIPToGateways(ctx, ip, now); return names(cands), err }`
  - `ScopedFor = chScopedFor(store, now)`（translate 設定從 ClickHouse 取）。
  - Resolver 仍建於**全 gateway**（`g.Gateways()`）；候選過濾由 `Lookup` 提供。
- `TestResolveWarm` / `TestResolveSingleWorst`：
  - 開頭 `store, err := chstore.Open(ctx, chAddr)`；**CH 不可達 → `t.Skip`**（與「無 router_check → skip」同風格）。
  - `ensureLoaded(ctx, store, g, vers)` 確保語料已在 ClickHouse（見下）。
  - 其餘計時／oracle 驗證邏輯不動；報表自然多出 `lookup` 一列。
- `TestResolveSingleWorst` 的手動聚合補一行：`agg.Stages.Lookup.Add(m.Stages.Lookup.Mean())`。
- **正確性不變**：broad（`direct-N.example.com`）與 unknown（`nope-N`）host 的 `IPForHost` 會失敗 → 無候選 →
  miss（cluster `""`），與 config_only 的 oracle（`""`）一致，故 full-corpus 仍 0 mismatch。

`ensureLoaded`（新 helper，可放 `bench_test.go` 或小工具）：
- 查 `service_versions` 列數，與「當前 `benchScale()` × 版本數」的期望值比對；**不符才** `CreateSchema` +
  `ingload.Load`（相符則跳過，重跑 benchmark 免重載）。
- **benchmark 載入預設單版本** `ingload.Versions{Deploy:1, Svc:1, Gw:1, VS:1, KSvc:1}`（`POC_VER_*` 可調），
  縮短載入時間；**載入不計入報表 `Wall`**（計時只圈 query 迴圈）。

> 註：`Lookup` 在 stage-1 迴圈**逐 query 序列**跑 3-hop（3 次 ClickHouse round-trip），故 60k 的 `bench-warm`
> 會多出約數分鐘的序列 3-hop 時間；報表 `lookup p50/p99` 即單筆成本。dev 可 `POC_GATEWAYS=50` 縮小先驗。

### 5) `Makefile` — 精簡 `ch-*`

- **保留**：`ch-up`、`ch-down`（容器生命週期；benchmark 需要 ClickHouse 在跑）。
- **移除**：`ch-load`、`ch-flow`、`ch-verify`、`ch-demo`（benchmark 會自動載入；正確性走 `ip_flow_test` /
  `go test`）。
- **沿用既有** `bench-warm` / `bench-worst`：現在即整串 CH 鏈。
- `cmd/ipflow`、`ip_flow_test.go` 原始碼**保留**（手動 CLI / 快速正確性），只是不再有各自 make 目標。

使用者流程收斂為：`make ch-up` → `make bench-worst`（或 `make bench-warm`）。

## 報表樣貌（實作後）

`out/report.md` 的每個 mode 區塊會由四階段變五階段：

```
| stage | p50 | p99 | mean |
|---|---|---|---|
| lookup (IP→candidates 3-hop, per query) | … | … | … |   ← 新增
| resolve (host→gw, per query)            | … | … | … |
| translate (scoped, per gw)              | … | … | … |
| check (router_check_tool, per gw)       | … | … | … |
| total (per gw batch)                    | … | … | … |
```

## 驗證方式（實作後）

1. `make ch-up`。
2. `make bench-worst`（sampled，快）→ 報表該段含 **lookup / resolve / translate / check / total** 五階段
   p50/p99/mean，mismatches=0，路徑為 IP→3-hop→`ResolveAmong`→CH translate→router_check。需 native
   `router_check_tool` 或 tools image。
3. `make bench-warm`（60k，較久）→ 同樣多出 `lookup` 階段、full-corpus 0 mismatch。
4. `POC_GATEWAYS=50 make bench-worst` dev 加速先驗一遍。
5. `go test ./...`（未起 CH）→ `TestResolveWarm` / `TestResolveSingleWorst` / `TestIPFlowClickHouse` 皆 skip；
   `go build ./... && go vet ./...` clean。
6. 相容性：`Lookup==nil` 時 `ResolveAll` 行為與現況一致。

## 不做 / 範圍外

- 不另開新的 benchmark 測試或新的 make 目標。
- 不做 Resolver 介面重構（只加 `ResolveAmong` 方法）。
- 不改多版本語意；benchmark 預設單版本載入只為縮短載入時間（`POC_VER_*` 仍可調到多版本）。
- DestinationRule subset / EndpointSlice 仍未做（沿用現況）。
