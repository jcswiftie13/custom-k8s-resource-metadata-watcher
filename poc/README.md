# POC：Istio 路由解析（route2a）

獨立 Go module，位於 [`route2a/`](route2a/)。驗證 **host + path（+ IP）→ Gateway → cluster（Service）**
的離線解析管線，對應完整系統設計中的引擎 2A 與 ClickHouse traffic_simulation 前半段。

- **架構說明** → [ARCHITECTURE.md](ARCHITECTURE.md)
- **完整系統設計** → [docs/istio-virtualservice-routing-history-design.md](../docs/istio-virtualservice-routing-history-design.md)
- **Go API 整合範例** → [route2a/README.md](route2a/README.md)

---

## 前置條件

| 項目 | 用途 |
|------|------|
| Go 1.25+ | 編譯（首次 `go test` 會下載 `istio.io/istio`，編譯可能需數分鐘） |
| Docker | `router_check_tool` fallback、ClickHouse 容器 |
| 可選：Linux native `router_check_tool` | 有意義的延遲數字（`make routercheck-bin`） |

## 獨立複製

只需複製 **`route2a/`** 整個資料夾（含 `go.mod`、`Makefile`、`testdata/`），**不需**主 repo 程式碼。
在該目錄執行 `go mod download` 即可。

---

## 快速開始

```bash
cd poc/route2a

# 無外部依賴的單元測試
go test ./internal/gwresolve/ ./internal/scalegen/ ./internal/translate/

# 完整鏈（需 ClickHouse）
make ch-up
make bench-worst POC_GATEWAYS=20 POC_VS=5
make ch-down
```

---

## Make 目標

在 `route2a/` 目錄下執行：

| 目標 | 說明 |
|------|------|
| `make gen` | 產生語料 spot-check（不 materialize 全量） |
| `make gwresolve` | Gateway host→gateway 消歧正確性 |
| `make spike` | scoped istiod 翻譯路徑驗證 |
| `make bench-routercheck` | `router_check_tool` 離線 oracle 交叉驗證 |
| `make bench-warm` | 全語料暖快取吞吐量（需 CH） |
| `make bench-worst` | 單筆最差延遲（需 CH；線上成本看這個） |
| `make verify` | gen + gwresolve + warm + worst，寫入 `out/report.md` |
| `make ch-up` / `make ch-down` | 啟停 ClickHouse 容器 |
| `make routercheck-bin` | 從 Envoy tools image 抽出 native binary |

---

## 測試矩陣

| 指令 | Docker | ClickHouse | native router_check |
|------|--------|------------|---------------------|
| `go test ./internal/...` | ❌ | ❌ | ❌ |
| `make gwresolve` / `make gen` | ❌ | ❌ | ❌ |
| `make bench-routercheck` | ✅（或 native） | ❌ | 建議 |
| `make bench-worst` / `bench-warm` | ✅ | ✅ | 建議 |
| `go test -run TestIPFlowClickHouse .` | ✅ | ✅ | ✅ |
| `make verify` | ✅ | ✅ | 建議 |

CH 或 `router_check_tool` 不可達時，相關測試會 **`t.Skip`**，不會失敗。

---

## 環境變數

| 變數 | 預設 | 說明 |
|------|------|------|
| `POC_GATEWAYS` | `600` | 語料 gateway 數 |
| `POC_VS` | `100` | 每 gateway 的 VS 數 |
| `POC_CH_ADDR` | `127.0.0.1:9000` | ClickHouse native 位址 |
| `POC_ROUTERCHECK_BIN` | （自動偵測） | native `router_check_tool` 路徑 |
| `POC_ROUTERCHECK_IMAGE` | `envoyproxy/envoy:tools-v1.34-latest` | docker fallback image |
| `POC_VER_DEPLOY` … `POC_VER_KSVC` | benchmark 預設各 `1` | bitemporal 版本深度 |
| `POC_WORST_SAMPLE` | `200` | `bench-worst` 取樣數 |
| `POC_LOG_QUERIES` | off | 設 `1` 印每筆 query 與 cluster |

縮小開發規模範例：`make bench-worst POC_GATEWAYS=20 POC_VS=5`

---

## ClickHouse 流程

```bash
make ch-up
make bench-worst    # 或 bench-warm；首次自動載入語料（不計入計時）
make ch-down
```

正確性快速檢查（非 benchmark）：

```bash
go test -run TestIPFlowClickHouse -v .
go run ./cmd/ipflow -mode=load|query|verify   # 手動 CLI，見 package doc
```

---

## Dev Container（macOS 建議）

macOS 上從 Envoy image 抽出的 `router_check_tool` 是 Linux ELF，無法在 host 原生執行。
在 **repo 根目錄** 使用 [`.devcontainer/`](../.devcontainer/)：

- ClickHouse compose service（免 `make ch-up`）
- `POC_CH_ADDR=clickhouse:9000`
- `post-create` 自動 `make routercheck-bin`

容器內快速檢查：

```bash
cd poc/route2a
go test -run TestMatchRouterCheckScale -v .
go test -run TestIPFlowClickHouse -v .
make bench-worst POC_GATEWAYS=20 POC_VS=5
```

macOS host 上 correctness 仍可走 docker fallback；**勿引用 macOS 上的 bench 延遲數字**。
