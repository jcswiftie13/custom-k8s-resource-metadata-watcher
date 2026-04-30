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

1. 建立（或重用）Kind cluster
2. 建置並載入 exporter image
3. 套用 integration manifests
4. 執行

```sh
go test -tags integration -v -count=1 ./test/integration/e2e/...
```

### 常用環境變數

- `KIND_CLUSTER_NAME`（預設：`metadata-exporter-it`）：Kind cluster 名稱
- `INTEGRATION_IMAGE`（預設：`metadata-exporter:it`）：測試 image tag
- `SKIP_KIND_CREATE=1`：不建立/刪除 Kind，使用目前 kube context
- `SKIP_CLUSTER_DELETE=1`：測試後保留 cluster（僅對 runner 建立的 cluster 生效）
- `SKIP_GO_TEST=1`：只套 manifests，不跑 `go test`
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
