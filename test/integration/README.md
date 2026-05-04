## Integration Tests（快速執行）

此目錄提供 `metadata-exporter` 的 Kind 整合測試執行器。

完整設計、案例細節、門檻與可調參數請參考：
[`docs/INTEGRATION_TESTS.md`](../../docs/INTEGRATION_TESTS.md)。

### 前置需求

主機需安裝：

- `kind`
- `kubectl`
- `docker`
- `go`

### 執行整套測試

```sh
make e2e
```

此命令會執行 [`run.sh`](./run.sh)，內容包含：

1. 建立或銜接 Kind cluster（預設使用 [`kind-config.yaml`](./kind-config.yaml)：一個 control-plane + 兩個 worker）。若已存在同名 cluster，會比對節點數與設定檔一致則直接重用；不一致則刪除後依設定檔重建（邏輯見 [`kind_ensure_cluster.inc.sh`](./kind_ensure_cluster.inc.sh)）
2. 等待節點 Ready 後，對每個 Node 的 `status` 追加 [RFC 5737](https://datatracker.ietf.org/doc/html/rfc5737) 測試用 `ExternalIP`（可由 `INTEGRATION_PATCH_NODE_EXTERNAL_IP=0` 關閉）
3. 建置並載入 exporter image
4. 套用 integration manifests
5. 執行

```sh
go test -tags integration -v -count=1 ./test/integration/e2e/...
```

### 常用環境變數

- `KIND_CLUSTER_NAME`（預設：`metadata-exporter-it`）：Kind cluster 名稱
- `INTEGRATION_IMAGE`（預設：`metadata-exporter:it`）：測試 image tag
- `SKIP_KIND_CREATE=1`：不建立/刪除/重建 Kind，使用目前 kube context（略過與 `kind-config.yaml` 的節點數比對）
- `SKIP_CLUSTER_DELETE=1`：測試後保留 cluster（僅對 runner 建立的 cluster 生效）
- `SKIP_GO_TEST=1`：只套 manifests，不跑 `go test`
- `INTEGRATION_PATCH_NODE_EXTERNAL_IP=0`：跳過對 Node `status.addresses` 的 ExternalIP patch（`SKIP_KIND_CREATE=1` 且不想改動既有 Node 時建議）。若關閉 patch 且 Node API 沒有 `ExternalIP`，`TestCorrectness_NodeMetrics` 會失敗
- `INTEGRATION_PRINT_METRICS=1`：每支與 metrics 相關的測試結束時，在 `t.Log` 列印該測試**事先指定的 exporter 指標名稱**之樣本列（非整份 `/metrics`）。僅斷言 kube-apiserver `/metrics` 的測試不會列印 exporter 快照。需搭配 `go test -v` 才會在 stdout 看見。未設則不列印
- `INTEGRATION_PORT_FORWARD_METRICS=1`：測試結束後前景執行 `kubectl port-forward` 至 exporter Service（本機預設埠 `INTEGRATION_METRICS_LOCAL_PORT`，預設 `18080`）。若 cluster 由此腳本建立，必須同時設 `SKIP_CLUSTER_DELETE=1`，否則腳本會拒絕執行
- `DOCKER_BUILD_PLATFORM=linux/amd64`：傳給 `docker build --platform`
- `GOTEST_FLAGS='-run TestCorrectness_'`：附加到 `go test` 的額外參數

### 重要提醒（穩定性相關）

- `TestBurden_BurstDedup` 使用「比例門檻」而非固定次數，門檻由 `dedupBudget` 控制（目前 40%）。不同執行環境可能有小幅波動，必要時可依文件建議調整。
- 多數整測依賴 `waitFor + timeout` 的最終一致性等待。若環境較慢，先調整等待窗口，再判斷是否為功能回歸。
- 從只支援舊 schema 的執行檔切到本專案版本時，**請重建 integration image 後再** `make e2e`；`make e2e` 套入的 `test/integration/manifests/configmap.yaml` 使用 `watch.resources[]`（見 [`docs/CONFIG.md`](../docs/CONFIG.md)）。

### 常用除錯指令

只跑 correctness 類測試：

```sh
GOTEST_FLAGS='-run TestCorrectness_' make e2e
```

保留 cluster 以利手動檢查：

```sh
SKIP_CLUSTER_DELETE=1 make e2e
```

測試後在本機用 port-forward 看 `/metrics`（需保留 cluster，見上）：

```sh
SKIP_CLUSTER_DELETE=1 INTEGRATION_PORT_FORWARD_METRICS=1 make e2e
```
