# 整合測試：詳細設計與實作說明

本文件是 `test/integration/` 整合測試的詳細設計文件。
若只需要快速執行方式，請看 [`test/integration/README.md`](../test/integration/README.md)。

## 目標

整合測試使用 Kind 上的真實 Kubernetes API Server，驗證 `metadata-exporter` 的下列行為：

1. 指標輸出的功能正確性。
2. `topController` 關聯鏈解析正確性。
3. 在高頻更新下的 `reconcile` 負載控制（dedup、佇列排空、索引命中）。
4. watch 拓樸在 **cluster-wide**、**per-namespace**、以及 **`watch.resources[]` 僅子集** 等模式下的可預期性，與 `apiserver_longrunning_requests` 的差分一致。

## 範圍與非目標

測試範圍包含：

- 從 Kubernetes 物件到 Prometheus 指標的端到端驗證。
- watch 連線數在不同拓樸下的差分（delta）驗證。
- reverse parent index 與 workqueue／高頻更新行為的回歸檢查。

不在此套件範圍：

- 長時間 soak 或記憶體洩漏認證（例如連跑數小時）。
- 多節點**排程策略**、Pod 跨節點遷移與工作負載抖動（整測預設 Kind 已為多節點，僅用於擴充 Node 指標覆蓋與較真實的節點 churn，不驗證排程器行為）。
- 網路故障 / 控制平面中斷的 chaos 驗證。

## 測試架構

### 目錄與責任分工

- [`test/integration/run.sh`](../test/integration/run.sh)：建立或銜接 Kind（[`kind_ensure_cluster.inc.sh`](../test/integration/kind_ensure_cluster.inc.sh) 比對現有同名 cluster 節點數與 [`kind-config.yaml`](../test/integration/kind-config.yaml)，一致則重用，否則刪後重建）、等待節點 Ready、[patch ExternalIP](../test/integration/patch_kind_node_external_ips.sh)、建 image、套 manifests、呼叫 `go test`；可選列印 metrics 或 port-forward（見 `test/integration/README.md`）。
- [`test/integration/kind-config.yaml`](../test/integration/kind-config.yaml)：整測用 Kind 拓樸（預設 1 control-plane + 2 worker）。
- [`test/integration/manifests/configmap.yaml`](../test/integration/manifests/configmap.yaml)：整測用規則與預設 watcher 設定。
- [`test/integration/e2e/e2e_test.go`](../test/integration/e2e/e2e_test.go)：`TestMain` 與 kube client 初始化。
- [`test/integration/e2e/helpers.go`](../test/integration/e2e/helpers.go)：部署、等待、scrape 指標、公用斷言工具。
- [`test/integration/e2e/topology_test.go`](../test/integration/e2e/topology_test.go)：watch 拓樸測試。
- [`test/integration/e2e/burden_test.go`](../test/integration/e2e/burden_test.go)：高負載與 dedup/索引行為測試。
- [`test/integration/e2e/correctness_test.go`](../test/integration/e2e/correctness_test.go)：功能正確性測試。
- [`test/integration/e2e/correctness_node_test.go`](../test/integration/e2e/correctness_node_test.go)：Node 相關指標（假 ExternalIP 由 runner patch）。

### 測試資料流

```mermaid
flowchart LR
    k8s[Kubernetes 物件事件] --> informer[Exporter Informer Cache]
    informer --> resolve[Resolver 解析 owner 鏈]
    resolve --> eval[Rule 評估與 label 計算]
    eval --> sink[Prometheus 指標輸出]
    sink --> assert[整測斷言]
```

## 觀測面與量測方式

1. `kube-apiserver /metrics`
   - 來源：`kubectl get --raw /metrics`
   - 用途：watch 連線數的權威來源（`apiserver_longrunning_requests`）。
2. exporter `/metrics`
   - 來源：helper 透過 API server proxy 讀 exporter pod 的 metrics。
   - 用途：`it_*`、`exporter_*`、`rest_client_requests_total` 等業務與自監控指標。
3. exporter logs
   - 用途：失敗時快速確認 watch 模式與啟動狀態。

4. （可選）除錯時的 stdout metrics / port-forward  
   - 由 `run.sh` 在 `go test` 之後依 `INTEGRATION_PRINT_METRICS`、`INTEGRATION_PORT_FORWARD_METRICS` 等環境變數觸發；細節見 [`test/integration/README.md`](../test/integration/README.md)。

## 目前測試案例

### Topology 類

- `TestTopology_ClusterWide`
  - `watch.namespaces: []` 時，各 kind 的 watch delta 預期為 `1`。
- `TestTopology_PerNamespace`
  - 指定 3 個 namespace 時，各 kind watch delta 預期為 `3`。
  - 非監看 namespace 的物件不應出現在 `it_pod_info`。
- `TestTopology_IdleStable`
  - 閒置區間內 `queue_depth`、`reconcile_total` 與 watch 連線不應漂移。
  - 多節點下節點 heartbeat 可能略增 `reconcile_total`；門檻依節點數調整（`5 + 2*(nodes-1)`）。
- `TestTopology_KindSubset`
  - `watch.resources[]` 僅含 `Pod`、`Deployment`（`kindSubsetClusterWideConfigYAML`）且 cluster-wide 時，`pods` 與 `deployments` 的 WATCH 差分為 `+1`；`replicasets` / `statefulsets` / `daemonsets` / `nodes` 為 `0`。
  - 規則仍使用 `topController`，未 watch `ReplicaSet` 等父 kind 時，啟動日誌應含 `not all parent kinds are watched`（不阻斷測試斷言）。

### Burden 類

- `TestBurden_BurstDedup`
  - 建立高頻 Deployment/Pod patch 負載。
  - 驗證 queue 最終可排空、`reconcile` 放大量受控、且 exporter 不做寫入請求。
- `TestBurden_ParentEventViaIndex`
  - 修改 controller annotation 後，應快速反映到所有 Pod 指標。
  - 驗證 reverse index 命中增加且 fallback 不應異常上升。

### Correctness 類

- `TestCorrectness_FixtureFlow`
  - fixture 生命週期、container label、刪除後 series 回收。
- `TestCorrectness_ControllerAnnotationWithoutPodAnnotation`
  - 確保 annotation 來源是 controller，而非 Pod 自身 metadata。
- `TestCorrectness_NodeMetrics`
  - 對**每個** Node 驗證 API 上同時具備 `InternalIP` 與 `ExternalIP`（後者由 `patch_kind_node_external_ips.sh` 寫入 [RFC 5737](https://datatracker.ietf.org/doc/html/rfc5737) 測試位址），以及 `it_node_info` / `it_node_address` / `it_node_condition`。

## 需要特別說明的「可浮動 / 可調整」機制

### 1) `TestBurden_BurstDedup` 的比例門檻是可調參數

`burden_test` 不是用固定次數，而是用比例門檻評估：

- `enqueuesPerRule = replicas * (depPatches + podPatches)`
- `dedupBudget = 0.40`（目前為 40%）
- 斷言：`maxRuleDelta <= enqueuesPerRule * dedupBudget`

此比例在不同硬體、Kind 負載、測試機共用程度下可能有小幅波動，因此 `dedupBudget` 屬於**可調整的穩定性參數**。若 CI 偶發貼近門檻，建議先蒐集多次樣本，再小幅調整（例如 0.42 或 0.45），避免掩蓋真正回歸。

### 2) `parent_index_fallback` 在 Burden 測試中有容忍值

`TestBurden_BurstDedup` 對 `exporter_parent_index_fallback_total` 採「小幅容忍」策略（目前容忍增量 `<= 5`），原因是整測期間可能有少量背景事件；重點是避免與負載同步出現異常 spike。

### 3) watch 驗證採「差分量測」，不是絕對值

Topology 測試先量 baseline（exporter scale=0），再 scale=1 量 after，最後比 `delta`。  
這是因為 apiserver 指標本身包含系統元件的 watch，直接用絕對值容易誤判。  
有 `watch.kinds` 時，WATCH 扇出大約是「namespace 段數（cluster-wide 為 1）× **啟用中的 kind 數**」，等同程式內的 `len(namespaces) × len(EffectiveKinds)`（`EffectiveKinds` 定義在 `docs/CONFIG.md` / `pkg/config`）。

### 4) 最終一致性等待（`waitFor + timeout`）是測試設計的一部分

整測大量使用 `waitFor` 與 timeout（例如 15s/45s/60s/90s）等待 rollout、queue 排空、指標收斂。  
若環境較慢，應先調整等待窗口，而非直接放寬功能斷言。

### 5) queue 排空是「可觀測的完成條件」

在負載測試中，`exporter_reconcile_queue_depth == 0` 代表該波事件已被消化完，之後再採樣 `reconcile_total` 才有可比性。若未排空就取樣，容易把進行中的工作誤判為回歸。

## 規則與 fixture 設計重點

整測預設規則主要驗證：

- `it_pod_info`：每 Pod 一條 series，含 controller 資訊。
- `it_pod_container_info`：每 `(pod, container)` 一條 series。

其中 `controller_annotation_integration_test_controller_note` 來源是  
`top.metadata.annotations.integration.test/controller-note`，可用來明確驗證資料來源是 controller 而非 Pod。

預設的 `configmap`／`renderConfigYAML` 在 `kinds` 下列出 **五種** resource，僅對 `Pod` 加 `fieldSelector`（其餘空物件），以維持與舊版「全 kind + Pod 篩選」等價的 topology 與 `topController` 行為。`kind-subset` 專用設定在 `kindSubsetClusterWideConfigYAML`（僅在 `TestTopology_KindSubset` 套用），避免干擾其他以「全量 kind」假設的測試。

## 擴充測試時的建議

1. 優先重用 `helpers.go`，避免重複等待與 scrape 邏輯。
2. 每個 test 使用獨立 namespace/fixture，降低交互干擾。
3. 同時驗證：
   - Kubernetes 物件真值（API 物件是否真的改了）
   - metrics 真值（使用者實際會看到的輸出）
4. 若是來源邊界議題，務必加入負向斷言（例如 Pod 不應有該 annotation）。
5. 若新增比例/閾值型斷言，請在文件中說明其目的、建議調整區間與調整時機。

## 已知限制

- 尚未納入長時間記憶體/協程趨勢追蹤。
- 尚未涵蓋控制平面故障復原情境。
- watch 歸因仍採差分法，不是單 exporter 身分歸因。
- Kind 預設為 [`kind-config.yaml`](../test/integration/kind-config.yaml) 的三節點拓樸；Node `ExternalIP` 為測試用 patch，若 kubelet 覆寫 `status.addresses` 可能影響穩定性（見 `patch_kind_node_external_ips.sh` 註解與 `INTEGRATION_PATCH_NODE_EXTERNAL_IP`）。

## 設定檔（schema）變更

整合測所用的 ConfigMap 已改為使用 `watch.kinds`（取代舊的 `watch.selectors`），與正式文件 [`docs/CONFIG.md`](../docs/CONFIG.md) 第 4 節 `watch` 一致。若從舊版分支切到本變更後跑 `make e2e`，請**重建 exporter image** 以確保執行檔讀得懂新欄位；執行檔不支援 `watch.selectors` 且 `Load()` 會顯式拒絕舊 yaml。

**遷移範例**：

```yaml
# 舊（已廢棄，會導致 Load 失敗）
# watch:
#   selectors:
#     Pod: { fieldSelector: "status.phase=Running" }

# 新
watch:
  kinds:
    Pod:
      fieldSelector: "status.phase=Running"
    # 其他不列的 kinds 不會建立 watch
```
