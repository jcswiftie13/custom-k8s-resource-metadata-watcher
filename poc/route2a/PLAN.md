# POC：路由解析 2A（in-process istiod 翻譯 + Envoy 比對）

> 狀態：POC 計畫（尚未全部實作）。本檔為此 `poc/route2a` 模組的自帶依據；`README.md` 放精簡的「怎麼跑」。

## 為什麼做

驗證設計 note（見 repo `docs/istio-virtualservice-routing-history-design.md`）中「**引擎 2A**」的**可行性與效率**：

對真實的 **Gateway / VirtualService / Service** 設定，用 **in-process istiod 翻譯**出 Envoy `RouteConfiguration`，再用 **Envoy 的比對碼**回答「**host + path → 導到哪個 K8s Service**」，並實測 **10000 組 host+path** 的效率。

「Envoy 比對」**兩種實現都做並比較效率**：

- **比對器 A**：Envoy `router_check_tool`（離線批量、無流量）
- **比對器 B**：真 Envoy 容器 + access log（同一套 Envoy 比對碼）

## 範圍（已確認）

- 資源只做 **Gateway / VirtualService / Service**（**無 DestinationRule/subset** → cluster 名為 `outbound|PORT||SERVICE.NS.svc.cluster.local`，subset 欄位為空）。
- **多個 ingressgateway**：多個 Gateway 資源、不同 `selector` / `hosts` / `port`，仿真實叢集。
- 每種資源**不只一個**；VS 含 `prefix` / `exact` / `regex`、多 route 順序、catch-all。
- YAML **不需真跑在 kind**，但必須是**可實際套用的 valid Istio schema**。
- Istio 版本：**最新穩定相容版**（約 1.26–1.27，`go.mod` pin 確切版本）。

## 目錄結構

```
poc/route2a/
  PLAN.md            # 本檔
  README.md          # 怎麼跑（精簡）
  go.mod             # 獨立 module，pin istio.io/istio（隔離重依賴，不動主模組）
  Makefile           # translate / bench-routercheck / bench-envoy / verify
  manifests/
    gateways.yaml        # ≥2 個 ingressgateway，不同 selector/hosts/port
    virtualservices.yaml # 多個 VS，bind 不同 Gateway，prefix/exact/regex/順序/catch-all
    services.yaml        # 多個 Service，跨 namespace，named port
  internal/
    translate/       # in-process istiod → 每個 gateway 一份 RouteConfiguration
    testgen/         # 由 VS 規則產 10000 組 host+path（hit/miss + expected cluster）
    matchcheck/      # 比對器 A：router_check_tool 驅動（docker）
    matchenvoy/      # 比對器 B：真 Envoy 容器 + access log 驅動（docker）
  docker/
    routercheck.Dockerfile  # 在容器內 bazel build router_check_tool
  poc_test.go        # TestTranslate / BenchmarkMatchRouterCheck / BenchmarkMatchEnvoy / TestConsistency
  out/               # 產物：每個 gateway 的 RouteConfiguration JSON、測試資料、報告
```

> **為什麼獨立 go module**：`istio.io/istio` 依賴樹龐大且自帶特定 k8s 版本，會與主模組（k8s 0.35）衝突。放自己的 `go.mod` 隔離，**完全不動主程式**。

## YAML fixtures（可套用、多實例）

- **gateways.yaml**：≥2 個 ingressgateway，例：
  - `public-gw`（ns `istio-system`）：`selector: {istio: ingressgateway-public}`，server 443 hosts `[shop.example.com, *.public.example.com]`
  - `internal-gw`（ns `istio-system`）：`selector: {istio: ingressgateway-internal}`，server 80 hosts `[*.internal.example.com]`
  - （可加第三個與 public 萬用重疊，練多 gateway 消歧）
- **virtualservices.yaml**：多個 VS，分別 `spec.gateways` bind 不同 Gateway，`http[]` 含 prefix/exact/regex、多 route 順序、catch-all，指向不同 Service。
- **services.yaml**：多個 Service，跨 namespace，named port。

## 翻譯（internal/translate）— in-process istiod

- 用 `istio.io/istio/pilot/pkg/xds.NewFakeDiscoveryServer(failer, FakeOptions{...})`：
  - `ConfigString` 餵 Istio CRD（VS + Gateway）
  - `KubernetesObjects` / `Services` 餵 k8s Service
- 每個 ingressgateway 建一個 **synthetic proxy**：`model.Proxy{Type: model.Router, Metadata:{Namespace, Labels=該 Gateway.selector}, ConfigNamespace}` → `s.SetupProxy`。
- 取 RC：向 ADS 要 `v3.RouteType`，用 `pilot/test/xdstest.ExtractRouteConfigurations`（LDS 列 route-config 名 → RDS 依名取）。
- 輸出：每個 gateway 一份 `RouteConfiguration`（JSON）到 `out/`。
- `NewFakeDiscoveryServer` 收 `test.Failer` 介面 → 提供最小 shim，讓翻譯可在一般程式/測試都能跑（或直接放 test setup）。

## 比對器 A：router_check_tool（internal/matchcheck）

- 取得：`docker build`（Envoy build image + `bazel build //test/tools/router_check:router_check_tool`）產出帶該 binary 的 image（本機無 bazel → 在 docker 內建；多 GB / 耗時 / arm64 風險，見風險 R2）。
- 輸入：RC 包成 tool 設定；tests = 10000 case（`:authority`=host、`:path`=path）。
- **要「發現」命中 cluster 而非「驗證」**：validate 設 sentinel + `--details` / `--only-show-failures` 讀 actual，或用輸出模式解析 actual cluster（確切旗標實作時定）。
- 一次 invocation 批量跑 10000，量 wall time。

## 比對器 B：真 Envoy 容器 + access log（internal/matchenvoy）

- 寫 Envoy static bootstrap：admin + 每 gateway port 一個 HTTP connection manager，`route_config` 內嵌翻出的 RC；clusters 定義但**無 endpoints**。
- access log format 含 `%ROUTE_NAME%`；**route→cluster→Service 由我們手上的 RC 自行映射**（避免依賴無 endpoint 時不可靠的 `%UPSTREAM_CLUSTER%`，見風險 R4）。
- `docker run envoyproxy/envoy:v1.xx` 掛設定；Go client（keep-alive + 適度並發）送 10000（帶 `Host` + path）；tail access log 收集命中 route。
- 量 wall time（**含 client / 網路 overhead**，報告時註明，與 A 的批量處理本質不同）。

## 測試資料（internal/testgen）

- 由 VS 每條 `match` 產「應命中」代表 path（prefix→+suffix；exact→原值；regex→取樣命中），記 expected cluster / Service。
- 加變體、near-miss、未知 host 湊到 **10000**，標 hit/miss。
- 同一份資料同時當**效率壓測輸入**與**一致性 oracle**。

## 交付物：效率與正確性報告

- **翻譯**：每 gateway + 總時間。
- **比對器 A vs B**：10000 case wall time、每筆 µs → **並列比較**。
- **一致性**：A、B 結果彼此一致，且與 expected 一致（含未命中）。
- 產出一張對照表 + PASS/FAIL。

## 風險與退路

- **R1 in-process istiod API**：`NewFakeDiscoveryServer` / `xdstest` 簽章跨版本會變 → pin 版本、對照該版 `pilot/pkg/xds` 測試；若 in-process 太痛，退路：docker 跑 `pilot-discovery`（file registry）出 RC。
- **R2 router_check_tool 建置**（bazel / arm64）重且可能失敗 → **真 Envoy 路線（B）是保底**；A 建不出來仍交付 B 的效率數據並記錄 A 失敗原因。
- **R3 依賴衝突** → 獨立 go module 隔離。
- **R4 無 endpoint 讀不到 cluster** → 用 `%ROUTE_NAME%` + 自有 RC 映射。

## 怎麼跑（詳見 README）

- `make -C poc/route2a translate` → 產每個 gateway 的 RC JSON。
- `make -C poc/route2a bench-routercheck` / `bench-envoy` → 各自 10000 效率數據。
- `make -C poc/route2a verify` → A/B 一致性 + 對照 expected。
- 或 `go test ./... -run TestConsistency -bench BenchmarkMatch -benchtime=1x`。
