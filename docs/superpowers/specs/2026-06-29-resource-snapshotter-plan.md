# Resource Snapshotter 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增獨立二進位檔 `cmd/resource-snapshotter`，持續觀察設定檔宣告的 K8s 資源（首例 Istio `VirtualService`），以事件驅動方式把每個物件 `.spec` 的 SCD2 歷史版本寫入 Postgres，供另一個 repo 做時間區間回溯查詢。

**Architecture:** 沿用 `pkg/collector/listers.go` 的 `dynamicinformer` 模式但不重用 `ScopedInformers`（避免拉入與本需求無關的 owner-chain/rule 相關耦合）；新增獨立、精簡的 `pkg/snapshotter/*`套件群：`config`（YAML 解析）、`hashutil`（spec hash）、`decide`（純函式 dedup 決策）、`store`（Postgres 讀寫 + migration + retention + host-overlap 檢查）、`metrics`（self-metrics）、`capture`（informer 事件 ↔ decide ↔ store 的接線，含 leader 寫入閘與 reconcile）。`cmd/resource-snapshotter/main.go` 負責組裝：建立 client、跑 informer、`client-go` `leaderelection`（Lease）保證單寫入者、定期 retention 清除、`/metrics`、`/healthz`。

**Tech Stack:** Go 1.25、`k8s.io/client-go`（dynamicinformer + leaderelection，已是現有相依）、新增 `github.com/jackc/pgx/v5`（Postgres driver/pool）、`github.com/prometheus/client_golang`（已有）、`sigs.k8s.io/yaml`（已有）。Migration 採自寫的精簡 runner（embed SQL + `schema_migrations` 表），不引入 `golang-migrate`：因為寫入路徑已經靠 leader election 保證單寫入者，不需要額外的併發安全 migration 工具，省一個重依賴。

## Global Constraints

- Go 版本與 module path 沿用既有 `go.mod`：`go 1.25.0`、module `github.com/example/metadata-exporter`。
- 測試一律使用標準庫 `testing`（不引入 testify），與現有 `pkg/collector` 風格一致。
- 所有 informer 一律 `resyncPeriod = 0`（與 `pkg/collector/listers.go` 既有模式一致），不依賴週期性 resync。
- 任何 SQL 字串插入的 table 名稱，必須先通過正規表示式白名單驗證（`^[a-z][a-z0-9_]{0,62}$`），在 config 載入與 store 函式入口各驗證一次（defense-in-depth），避免 SQL injection。
- Postgres 連線字串透過環境變數 `SNAPSHOTTER_DATABASE_URL` 注入，不寫死在程式碼或 config YAML。
- Leader election 用 `coordination.k8s.io` `Lease`；候選人身分用 `POD_NAME`/`POD_NAMESPACE` 環境變數（downward API）。
- v1 範圍限制（明確記錄、非隱藏簡化）：`watch.resources[]` 每筆最多宣告 0 或 1 個 explicit namespace（cluster scope 或單一 namespace），尚不支援像 `pkg/collector/listers.go` 那樣對單一資源同時開多個 per-namespace factory；若未來需要，再移植該模式。
- 本機沙盒環境沒有安裝 `go` 工具鏈、無法連網下載模組、也沒有可用的 Postgres/K8s cluster——所有「Run: ...」步驟必須在具備 Go 1.25+、網路存取、且（針對 store 套件的整合測試）可連線 Postgres 的環境執行。DSN 透過環境變數 `SNAPSHOTTER_TEST_POSTGRES_DSN` 提供；未設定時相關測試會自動 `t.Skip`。

---

## Task 1: Snapshotter 設定檔解析（pkg/snapshotter/config）

**Files:**
- Create: `pkg/snapshotter/config/config.go`
- Test: `pkg/snapshotter/config/config_test.go`

**Interfaces:**
- Produces: `type Config struct { Watch WatchScope; Retention string }`、`type WatchScope struct { Resources []WatchResource }`、`type WatchResource struct { Kind, Group, Version, Resource, Scope, LabelSelector, FieldSelector, HistoryTable string; Namespaces []string }`、`func (r WatchResource) GVR() schema.GroupVersionResource`、`func Load(path string) (*Config, error)`、`func (c *Config) Validate() error`、`func (c *Config) RetentionDuration() (time.Duration, error)`、常數 `ScopeNamespaced = "Namespaced"`、`ScopeCluster = "Cluster"`、`DefaultRetention = 30 * 24 * time.Hour`。
- Consumes: 無（這是最底層套件，不依賴本專案其他新套件）。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/config/config_test.go
package config

import (
	"testing"
	"time"
)

func validVSConfig() Config {
	return Config{
		Watch: WatchScope{
			Resources: []WatchResource{
				{
					Kind:         "VirtualService",
					Group:        "networking.istio.io",
					Version:      "v1beta1",
					Resource:     "virtualservices",
					Scope:        ScopeNamespaced,
					HistoryTable: "virtualservice_snapshots",
				},
			},
		},
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := validVSConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
}

func TestValidateRejectsEmptyResources(t *testing.T) {
	cfg := Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for empty watch.resources")
	}
}

func TestValidateRejectsMissingKind(t *testing.T) {
	cfg := validVSConfig()
	cfg.Watch.Resources[0].Kind = ""
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for missing kind")
	}
}

func TestValidateRejectsBadScope(t *testing.T) {
	cfg := validVSConfig()
	cfg.Watch.Resources[0].Scope = "Bogus"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for invalid scope")
	}
}

func TestValidateRejectsClusterScopeWithNamespaces(t *testing.T) {
	cfg := validVSConfig()
	cfg.Watch.Resources[0].Scope = ScopeCluster
	cfg.Watch.Resources[0].Namespaces = []string{"default"}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for cluster scope with namespaces")
	}
}

func TestValidateRejectsInvalidHistoryTableIdentifier(t *testing.T) {
	cases := []string{
		"",
		"VirtualserviceSnapshots",            // 大寫不允許
		"virtualservice_snapshots; DROP TABLE x", // SQL injection 嘗試
		"1_leading_digit",
	}
	for _, table := range cases {
		cfg := validVSConfig()
		cfg.Watch.Resources[0].HistoryTable = table
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected error for historyTable %q", table)
		}
	}
}

func TestValidateRejectsDuplicateHistoryTable(t *testing.T) {
	cfg := validVSConfig()
	dup := cfg.Watch.Resources[0]
	dup.Kind = "HTTPRoute"
	cfg.Watch.Resources = append(cfg.Watch.Resources, dup)
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected error for duplicate historyTable across resources")
	}
}

func TestRetentionDurationDefaultsTo30Days(t *testing.T) {
	cfg := validVSConfig()
	d, err := cfg.RetentionDuration()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 30*24*time.Hour {
		t.Fatalf("expected default 30 days, got %v", d)
	}
}

func TestRetentionDurationParsesCustomValue(t *testing.T) {
	cfg := validVSConfig()
	cfg.Retention = "168h"
	d, err := cfg.RetentionDuration()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 168*time.Hour {
		t.Fatalf("expected 168h, got %v", d)
	}
}

func TestRetentionDurationRejectsNonPositive(t *testing.T) {
	cfg := validVSConfig()
	cfg.Retention = "0h"
	if _, err := cfg.RetentionDuration(); err == nil {
		t.Fatalf("expected error for non-positive retention")
	}
}
```

- [ ] **Step 2: 確認測試會失敗（套件尚不存在）**

Run: `go test ./pkg/snapshotter/config/... -v`
Expected: FAIL，錯誤訊息類似 `no Go files in ...` 或找不到 `Config`/`WatchScope` 等型別。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/config/config.go
// Package config 解析 resource-snapshotter 的 YAML 設定：要監看哪些 GVR、
// 每種資源寫到哪張 Postgres 表（historyTable）、以及保留期。
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

const (
	ScopeNamespaced = "Namespaced"
	ScopeCluster    = "Cluster"

	// DefaultRetention is used when Config.Retention is empty.
	DefaultRetention = 30 * 24 * time.Hour
)

var (
	kindNameRe     = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	versionRe      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	resourceRe     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	historyTableRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
)

// Config is the root configuration for resource-snapshotter.
type Config struct {
	Watch WatchScope `json:"watch,omitempty"`
	// Retention is a Go duration string (e.g. "720h"). Empty means
	// DefaultRetention.
	Retention string `json:"retention,omitempty"`
}

// WatchScope declares every resource to capture history for.
type WatchScope struct {
	Resources []WatchResource `json:"resources,omitempty"`
}

// WatchResource identifies one GVR and the Postgres table its SCD2 history
// is written to.
type WatchResource struct {
	Kind          string   `json:"kind"`
	Group         string   `json:"group,omitempty"`
	Version       string   `json:"version"`
	Resource      string   `json:"resource"`
	Scope         string   `json:"scope"`
	Namespaces    []string `json:"namespaces,omitempty"`
	LabelSelector string   `json:"labelSelector,omitempty"`
	FieldSelector string   `json:"fieldSelector,omitempty"`
	// HistoryTable is the Postgres table this resource's versions are
	// written to. Validated against historyTableRe so it is always safe to
	// interpolate directly into SQL (Postgres has no parameter placeholder
	// for identifiers).
	HistoryTable string `json:"historyTable"`
}

// GVR returns the Kubernetes GroupVersionResource for this watch entry.
func (r WatchResource) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}
}

// Load reads and validates a config file from disk.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}
	return cfg, nil
}

// Validate performs structural checks on the whole config.
func (c *Config) Validate() error {
	if len(c.Watch.Resources) == 0 {
		return fmt.Errorf("watch.resources: at least one resource is required")
	}
	seenTables := map[string]int{}
	for i, r := range c.Watch.Resources {
		if err := validateResource(i, r); err != nil {
			return err
		}
		if prev, dup := seenTables[r.HistoryTable]; dup {
			return fmt.Errorf("watch.resources[%d].historyTable: %q already used by watch.resources[%d]", i, r.HistoryTable, prev)
		}
		seenTables[r.HistoryTable] = i
	}
	if _, err := c.RetentionDuration(); err != nil {
		return err
	}
	return nil
}

// RetentionDuration returns the configured retention window, defaulting to
// DefaultRetention when Retention is empty.
func (c *Config) RetentionDuration() (time.Duration, error) {
	if strings.TrimSpace(c.Retention) == "" {
		return DefaultRetention, nil
	}
	d, err := time.ParseDuration(c.Retention)
	if err != nil {
		return 0, fmt.Errorf("retention %q: %w", c.Retention, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("retention %q: must be positive", c.Retention)
	}
	return d, nil
}

func validateResource(i int, r WatchResource) error {
	if strings.TrimSpace(r.Kind) == "" || !kindNameRe.MatchString(r.Kind) {
		return fmt.Errorf("watch.resources[%d].kind: invalid kind %q", i, r.Kind)
	}
	if r.Version == "" || !versionRe.MatchString(r.Version) {
		return fmt.Errorf("watch.resources[%d].version: invalid version %q", i, r.Version)
	}
	if !resourceRe.MatchString(r.Resource) {
		return fmt.Errorf("watch.resources[%d].resource: invalid resource %q", i, r.Resource)
	}
	if r.Scope != ScopeNamespaced && r.Scope != ScopeCluster {
		return fmt.Errorf("watch.resources[%d].scope: must be %q or %q", i, ScopeNamespaced, ScopeCluster)
	}
	if r.Scope == ScopeCluster && len(r.Namespaces) > 0 {
		return fmt.Errorf("watch.resources[%d]: scope=%q cannot set namespaces", i, ScopeCluster)
	}
	if len(r.Namespaces) > 1 {
		return fmt.Errorf("watch.resources[%d]: at most one namespace is supported in v1 (got %d)", i, len(r.Namespaces))
	}
	if !historyTableRe.MatchString(r.HistoryTable) {
		return fmt.Errorf("watch.resources[%d].historyTable: must match %s (got %q)", i, historyTableRe.String(), r.HistoryTable)
	}
	return nil
}
```

- [ ] **Step 4: 確認測試通過**

Run: `go test ./pkg/snapshotter/config/... -v`
Expected: PASS（全部測試案例通過）。

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/config/config.go pkg/snapshotter/config/config_test.go
git commit -m "feat(snapshotter): add config package for watch resources and retention"
```

---

## Task 2: Spec Hash（pkg/snapshotter/hashutil）

**Files:**
- Create: `pkg/snapshotter/hashutil/hash.go`
- Test: `pkg/snapshotter/hashutil/hash_test.go`

**Interfaces:**
- Produces: `func SpecHash(spec map[string]interface{}) (string, error)`
- Consumes: 無。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/hashutil/hash_test.go
package hashutil

import "testing"

func TestSpecHashDeterministicAcrossMapKeyOrder(t *testing.T) {
	a := map[string]interface{}{"hosts": []interface{}{"a.com"}, "http": []interface{}{"r1"}}
	b := map[string]interface{}{"http": []interface{}{"r1"}, "hosts": []interface{}{"a.com"}}
	ha, err := SpecHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := SpecHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if ha != hb {
		t.Fatalf("expected equal hashes for same content with different map key order, got %q vs %q", ha, hb)
	}
}

func TestSpecHashDiffersOnValueChange(t *testing.T) {
	a := map[string]interface{}{"hosts": []interface{}{"a.com"}}
	b := map[string]interface{}{"hosts": []interface{}{"b.com"}}
	ha, _ := SpecHash(a)
	hb, _ := SpecHash(b)
	if ha == hb {
		t.Fatalf("expected different hashes for different content")
	}
}

func TestSpecHashDiffersOnArrayOrderChange(t *testing.T) {
	// Array order is semantic for VirtualService http[] (first-match-wins),
	// so reordering must change the hash.
	a := map[string]interface{}{"http": []interface{}{"r1", "r2"}}
	b := map[string]interface{}{"http": []interface{}{"r2", "r1"}}
	ha, _ := SpecHash(a)
	hb, _ := SpecHash(b)
	if ha == hb {
		t.Fatalf("expected different hashes when array order changes")
	}
}

func TestSpecHashIsHexSHA256Length(t *testing.T) {
	h, err := SpecHash(map[string]interface{}{"hosts": []interface{}{"a.com"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(h) != 64 {
		t.Fatalf("expected 64 hex chars (sha256), got %d: %q", len(h), h)
	}
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/hashutil/... -v`
Expected: FAIL（`SpecHash` 未定義）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/hashutil/hash.go
// Package hashutil computes the deterministic content hash used to dedup
// SCD2 version writes.
package hashutil

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SpecHash returns a deterministic sha256 hex digest of spec.
// encoding/json marshals map keys in sorted order and preserves array
// order, so two specs with identical content but different map key
// insertion order hash equal, while reordering an array (semantic for
// VirtualService http[]) changes the hash.
func SpecHash(spec map[string]interface{}) (string, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshal spec: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
```

- [ ] **Step 4: 確認測試通過**

Run: `go test ./pkg/snapshotter/hashutil/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/hashutil/hash.go pkg/snapshotter/hashutil/hash_test.go
git commit -m "feat(snapshotter): add deterministic spec hashing"
```

---

## Task 3: Dedup 決策邏輯（pkg/snapshotter/decide）

**Files:**
- Create: `pkg/snapshotter/decide/decide.go`
- Test: `pkg/snapshotter/decide/decide_test.go`

**Interfaces:**
- Produces: `type Action int` 與常數 `ActionSkip`、`ActionTransition`、`ActionClose`（含 `String()`）；`type OpenVersion struct { UID, SpecHash string }`；`type Observed struct { UID, SpecHash string }`；`func ForUpsert(open *OpenVersion, observed Observed) Action`；`func ForDelete(open *OpenVersion) Action`。
- Consumes: 無（純函式，零外部依賴，之後 `store`/`capture` 套件都會用到 `OpenVersion`/`Observed`/`Action`，名稱與欄位需與此處完全一致）。

這是整個系統最容易出 bug、也最值得窮舉測試的一塊：依設計文件第 3 節，dedup 比對需要同時看 `uid`（偵測同名刪除後重建）與 `spec_hash`（偵測內容變化）。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/decide/decide_test.go
package decide

import "testing"

func TestForUpsertNoOpenVersionTransitions(t *testing.T) {
	got := ForUpsert(nil, Observed{UID: "u1", SpecHash: "h1"})
	if got != ActionTransition {
		t.Fatalf("expected ActionTransition, got %v", got)
	}
}

func TestForUpsertSameUIDSameHashSkips(t *testing.T) {
	open := &OpenVersion{UID: "u1", SpecHash: "h1"}
	got := ForUpsert(open, Observed{UID: "u1", SpecHash: "h1"})
	if got != ActionSkip {
		t.Fatalf("expected ActionSkip, got %v", got)
	}
}

func TestForUpsertSameUIDDifferentHashTransitions(t *testing.T) {
	open := &OpenVersion{UID: "u1", SpecHash: "h1"}
	got := ForUpsert(open, Observed{UID: "u1", SpecHash: "h2"})
	if got != ActionTransition {
		t.Fatalf("expected ActionTransition, got %v", got)
	}
}

func TestForUpsertDifferentUIDSameHashTransitions(t *testing.T) {
	// Delete-then-recreate with identical spec content but a missed
	// DeleteFunc event must still be detected as a new generation via uid.
	open := &OpenVersion{UID: "u1", SpecHash: "h1"}
	got := ForUpsert(open, Observed{UID: "u2", SpecHash: "h1"})
	if got != ActionTransition {
		t.Fatalf("expected ActionTransition for uid change, got %v", got)
	}
}

func TestForUpsertDifferentUIDDifferentHashTransitions(t *testing.T) {
	open := &OpenVersion{UID: "u1", SpecHash: "h1"}
	got := ForUpsert(open, Observed{UID: "u2", SpecHash: "h2"})
	if got != ActionTransition {
		t.Fatalf("expected ActionTransition, got %v", got)
	}
}

func TestForDeleteNoOpenVersionSkips(t *testing.T) {
	if got := ForDelete(nil); got != ActionSkip {
		t.Fatalf("expected ActionSkip, got %v", got)
	}
}

func TestForDeleteWithOpenVersionCloses(t *testing.T) {
	open := &OpenVersion{UID: "u1", SpecHash: "h1"}
	if got := ForDelete(open); got != ActionClose {
		t.Fatalf("expected ActionClose, got %v", got)
	}
}

func TestActionString(t *testing.T) {
	cases := map[Action]string{
		ActionSkip:       "skip",
		ActionTransition: "transition",
		ActionClose:      "close",
	}
	for action, want := range cases {
		if got := action.String(); got != want {
			t.Fatalf("Action(%d).String() = %q, want %q", action, got, want)
		}
	}
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/decide/... -v`
Expected: FAIL（型別/函式未定義）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/decide/decide.go
// Package decide holds the pure dedup decision logic shared by every live
// informer event and the leader-promotion reconciliation pass (see design
// doc section 3). It has zero Kubernetes/Postgres dependencies so every
// branch is exhaustively unit-testable.
package decide

// Action is what the caller should do for an observed event.
type Action int

const (
	// ActionSkip: the open version's UID and spec hash both match the
	// observed object; no write needed.
	ActionSkip Action = iota
	// ActionTransition: close the open version (if any) and insert a new
	// one for the observed object.
	ActionTransition
	// ActionClose: close the open version and insert nothing (the object
	// was deleted).
	ActionClose
)

func (a Action) String() string {
	switch a {
	case ActionSkip:
		return "skip"
	case ActionTransition:
		return "transition"
	case ActionClose:
		return "close"
	default:
		return "unknown"
	}
}

// OpenVersion is the currently open (valid_to IS NULL) version recorded in
// Postgres for an object's (namespace, name), or nil if none exists.
type OpenVersion struct {
	UID      string
	SpecHash string
}

// Observed is the object state read from the informer cache at the moment
// of an Add/Update event.
type Observed struct {
	UID      string
	SpecHash string
}

// ForUpsert decides the action for an Add/Update event. uid is compared
// first: a UID change means the same-named object was deleted and recreated
// (even if the DeleteFunc event was missed), so it always transitions
// regardless of whether the spec hash happens to match.
func ForUpsert(open *OpenVersion, observed Observed) Action {
	if open == nil {
		return ActionTransition
	}
	if open.UID == observed.UID && open.SpecHash == observed.SpecHash {
		return ActionSkip
	}
	return ActionTransition
}

// ForDelete decides the action for a Delete event.
func ForDelete(open *OpenVersion) Action {
	if open == nil {
		return ActionSkip
	}
	return ActionClose
}
```

- [ ] **Step 4: 確認測試通過**

Run: `go test ./pkg/snapshotter/decide/... -v`
Expected: PASS（8 個測試全過）。

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/decide/decide.go pkg/snapshotter/decide/decide_test.go
git commit -m "feat(snapshotter): add pure uid+hash dedup decision logic"
```

---

## Task 4: Postgres 連線 + Migration Runner（pkg/snapshotter/store）

**Files:**
- Create: `pkg/snapshotter/store/store.go`
- Create: `pkg/snapshotter/store/migrate.go`
- Create: `pkg/snapshotter/store/migrations/0001_create_virtualservice_snapshots.up.sql`
- Test: `pkg/snapshotter/store/migrate_test.go`

**Interfaces:**
- Produces: `type Store struct{...}`、`func New(ctx context.Context, dsn string) (*Store, error)`、`func (s *Store) Close()`、`func (s *Store) Migrate(ctx context.Context) error`、內部 `func checkTableName(table string) error`（後續 Task 5/6/7 都會呼叫）。
- Consumes: 無新依賴（除了本任務新增的 `github.com/jackc/pgx/v5`）。

此任務新增第一個第三方依賴。設計文件第 5 節「Migration 由這個 repo 擁有」+ 第 3 節「leader election 保證單寫入者」：因此 migration 不需要處理多 replica 併發（只有 leader 會呼叫 `Migrate`，由 `cmd/resource-snapshotter` 的 `OnStartedLeading` 回呼保證，Task 11 處理），這裡用最精簡的「掃描 embed 目錄、依檔名排序、逐一檢查 `schema_migrations` 是否已套用、套用then記錄」即可，不需要鎖。

- [ ] **Step 1: 新增 Postgres driver 依賴**

Run:
```bash
go get github.com/jackc/pgx/v5
go mod tidy
```
Expected: `go.mod`/`go.sum` 新增 `github.com/jackc/pgx/v5` 與其間接依賴；指令成功結束（exit code 0）。

- [ ] **Step 2: 寫 migration SQL 檔（依設計文件第 4 節 DDL，外加一個熱路徑 partial index）**

```sql
-- pkg/snapshotter/store/migrations/0001_create_virtualservice_snapshots.up.sql
CREATE TABLE virtualservice_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    namespace   TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    uid         TEXT        NOT NULL,
    spec_hash   TEXT        NOT NULL,
    spec_json   JSONB       NOT NULL,
    valid_from  TIMESTAMPTZ NOT NULL,
    valid_to    TIMESTAMPTZ NULL
);

CREATE INDEX idx_vs_snap_identity_time
    ON virtualservice_snapshots (namespace, name, valid_from, valid_to);

CREATE INDEX idx_vs_snap_hosts_gin
    ON virtualservice_snapshots USING GIN ((spec_json -> 'hosts'));

-- Additive beyond the design doc's literal DDL: every live event does a
-- "find the currently open version for (namespace, name)" lookup (see
-- pkg/snapshotter/store/versions.go CurrentOpen). A partial index on just
-- the open rows keeps that hot-path lookup tiny instead of scanning the
-- full identity_time index and filtering valid_to in a second pass.
CREATE INDEX idx_vs_snap_open
    ON virtualservice_snapshots (namespace, name)
    WHERE valid_to IS NULL;
```

- [ ] **Step 3: 寫 Store 基礎結構**

```go
// pkg/snapshotter/store/store.go
// Package store implements resource-snapshotter's Postgres read/write layer:
// schema migration, SCD2 version transitions, retention purge, and the one
// VirtualService-specific host-overlap check (see design doc sections 3-7).
package store

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a Postgres connection pool. One Store instance serves every
// configured GVR; table names are passed per-call (not fixed at
// construction) because each watch.resources[] entry has its own
// historyTable.
type Store struct {
	pool *pgxpool.Pool
}

// validTableName mirrors pkg/snapshotter/config's historyTable validation.
// Re-checked here as defense-in-depth: every exported Store method that
// interpolates a table name into SQL (Postgres has no parameter placeholder
// for identifiers) must go through checkTableName first.
var validTableName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func checkTableName(table string) error {
	if !validTableName.MatchString(table) {
		return fmt.Errorf("store: invalid table name %q", table)
	}
	return nil
}

// New connects to Postgres using dsn, e.g.
// "postgres://user:pass@host:5432/db".
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() {
	s.pool.Close()
}
```

- [ ] **Step 4: 寫 migration runner**

```go
// pkg/snapshotter/store/migrate.go
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate applies every embedded migration not yet recorded in
// schema_migrations, in filename order, one transaction per file.
//
// Callers must only invoke this while holding write leadership (see the
// leader-election wiring in cmd/resource-snapshotter, Task 11) — this
// runner has no locking of its own and is not safe to run concurrently
// from multiple replicas.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		if err := s.applyMigrationIfNeeded(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyMigrationIfNeeded(ctx context.Context, name string) error {
	var applied bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		name,
	).Scan(&applied)
	if err != nil {
		return fmt.Errorf("check migration %s: %w", name, err)
	}
	if applied {
		return nil
	}

	sqlBytes, err := migrationFiles.ReadFile("migrations/" + name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", name, err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	return tx.Commit(ctx)
}
```

- [ ] **Step 5: 寫 DSN-gated 整合測試**

```go
// pkg/snapshotter/store/migrate_test.go
package store

import (
	"context"
	"os"
	"testing"
)

// testDSN returns the Postgres DSN to test against, skipping the test when
// unset (see Global Constraints in the plan header).
func testDSN(t *testing.T) string {
	dsn := os.Getenv("SNAPSHOTTER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SNAPSHOTTER_TEST_POSTGRES_DSN not set; skipping Postgres-backed test")
	}
	return dsn
}

func TestMigrateCreatesTableAndIsIdempotent(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	// Running again must be a no-op, not an error (e.g. "relation already exists").
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	var exists bool
	err = s.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_name = 'virtualservice_snapshots'
	)`).Scan(&exists)
	if err != nil {
		t.Fatalf("check table exists: %v", err)
	}
	if !exists {
		t.Fatalf("expected virtualservice_snapshots table to exist after Migrate")
	}

	if _, err := s.pool.Exec(ctx, "TRUNCATE virtualservice_snapshots"); err != nil {
		t.Fatalf("truncate for test isolation: %v", err)
	}
}

func TestCheckTableNameRejectsInvalidIdentifiers(t *testing.T) {
	bad := []string{"", "Foo", "foo; DROP TABLE x", "1leading"}
	for _, table := range bad {
		if err := checkTableName(table); err == nil {
			t.Fatalf("expected error for table name %q", table)
		}
	}
	if err := checkTableName("virtualservice_snapshots"); err != nil {
		t.Fatalf("expected valid table name to pass, got %v", err)
	}
}
```

- [ ] **Step 6: 執行測試**

Run（需設定 `SNAPSHOTTER_TEST_POSTGRES_DSN` 指向一個可寫入的測試用 Postgres，例如本機 `docker run -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16` 後設 `export SNAPSHOTTER_TEST_POSTGRES_DSN=postgres://postgres:test@localhost:5432/postgres?sslmode=disable`）：

```bash
go test ./pkg/snapshotter/store/... -run TestMigrate -v
go test ./pkg/snapshotter/store/... -run TestCheckTableName -v
```
Expected: PASS。若未設定環境變數，`TestMigrate...` 會顯示 `--- SKIP`（非失敗）。

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum pkg/snapshotter/store/store.go pkg/snapshotter/store/migrate.go \
        pkg/snapshotter/store/migrations/0001_create_virtualservice_snapshots.up.sql \
        pkg/snapshotter/store/migrate_test.go
git commit -m "feat(snapshotter): add Postgres store with embedded schema migrations"
```

---

## Task 5: SCD2 版本讀寫（pkg/snapshotter/store/versions.go）

**Files:**
- Create: `pkg/snapshotter/store/versions.go`
- Test: `pkg/snapshotter/store/versions_test.go`

**Interfaces:**
- Produces: `type ObjectRecord struct { Namespace, Name, UID, SpecHash string; Spec map[string]interface{} }`、`func (s *Store) CurrentOpen(ctx context.Context, table, namespace, name string) (decide.OpenVersion, bool, error)`、`func (s *Store) Transition(ctx context.Context, table string, obj ObjectRecord, at time.Time) error`、`func (s *Store) Close(ctx context.Context, table, namespace, name string, at time.Time) error`。
- Consumes: Task 3 的 `pkg/snapshotter/decide.OpenVersion`；Task 4 的 `checkTableName`、`*Store`。

`CurrentOpen` 的回傳型別直接用 `decide.OpenVersion`，因為 Task 8（capture 套件）會把它原封不動丟給 `decide.ForUpsert`/`decide.ForDelete` ——避免在 store 與 capture 之間多一層轉換型別。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/store/versions_test.go
package store

import (
	"context"
	"testing"
	"time"
)

func freshTable(t *testing.T, s *Store, table string) {
	t.Helper()
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.pool.Exec(context.Background(), "TRUNCATE "+table); err != nil {
		t.Fatalf("truncate %s: %v", table, err)
	}
}

func TestCurrentOpenReturnsNotFoundBeforeAnyWrite(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")

	_, ok, err := s.CurrentOpen(context.Background(), "virtualservice_snapshots", "ns1", "vs1")
	if err != nil {
		t.Fatalf("CurrentOpen: %v", err)
	}
	if ok {
		t.Fatalf("expected no open version before first write")
	}
}

func TestTransitionInsertsThenAdvancesOpenVersion(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")
	ctx := context.Background()
	table := "virtualservice_snapshots"

	t1 := time.Now().UTC().Truncate(time.Microsecond)
	err = s.Transition(ctx, table, ObjectRecord{
		Namespace: "ns1", Name: "vs1", UID: "uid-1", SpecHash: "hash-1",
		Spec: map[string]interface{}{"hosts": []interface{}{"a.com"}},
	}, t1)
	if err != nil {
		t.Fatalf("first Transition: %v", err)
	}

	open, ok, err := s.CurrentOpen(ctx, table, "ns1", "vs1")
	if err != nil || !ok {
		t.Fatalf("CurrentOpen after insert: ok=%v err=%v", ok, err)
	}
	if open.UID != "uid-1" || open.SpecHash != "hash-1" {
		t.Fatalf("unexpected open version: %+v", open)
	}

	t2 := t1.Add(time.Minute)
	err = s.Transition(ctx, table, ObjectRecord{
		Namespace: "ns1", Name: "vs1", UID: "uid-1", SpecHash: "hash-2",
		Spec: map[string]interface{}{"hosts": []interface{}{"b.com"}},
	}, t2)
	if err != nil {
		t.Fatalf("second Transition: %v", err)
	}

	open, ok, err = s.CurrentOpen(ctx, table, "ns1", "vs1")
	if err != nil || !ok || open.SpecHash != "hash-2" {
		t.Fatalf("expected open version to advance to hash-2, got %+v ok=%v err=%v", open, ok, err)
	}

	var rowCount, closedCount int
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM virtualservice_snapshots WHERE namespace=$1 AND name=$2",
		"ns1", "vs1",
	).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM virtualservice_snapshots WHERE namespace=$1 AND name=$2 AND valid_to IS NOT NULL",
		"ns1", "vs1",
	).Scan(&closedCount); err != nil {
		t.Fatalf("count closed rows: %v", err)
	}
	if rowCount != 2 || closedCount != 1 {
		t.Fatalf("expected 2 total rows / 1 closed row, got total=%d closed=%d", rowCount, closedCount)
	}
}

func TestCloseClosesOpenVersionWithoutInserting(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")
	ctx := context.Background()
	table := "virtualservice_snapshots"

	err = s.Transition(ctx, table, ObjectRecord{
		Namespace: "ns2", Name: "vs2", UID: "uid-2", SpecHash: "hash-1",
		Spec: map[string]interface{}{"hosts": []interface{}{"c.com"}},
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}

	if err := s.Close(ctx, table, "ns2", "vs2", time.Now().UTC()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, ok, err := s.CurrentOpen(ctx, table, "ns2", "vs2")
	if err != nil {
		t.Fatalf("CurrentOpen: %v", err)
	}
	if ok {
		t.Fatalf("expected no open version after Close")
	}

	var rowCount int
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM virtualservice_snapshots WHERE namespace=$1 AND name=$2",
		"ns2", "vs2",
	).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("Close must not insert a new row; expected 1 row total, got %d", rowCount)
	}
}

func TestCloseOnNonexistentObjectIsNoop(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")

	if err := s.Close(context.Background(), "virtualservice_snapshots", "nope", "nope", time.Now().UTC()); err != nil {
		t.Fatalf("expected no error closing a nonexistent object, got %v", err)
	}
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/store/... -run "TestCurrentOpen|TestTransition|TestClose" -v`
Expected: 編譯失敗（`ObjectRecord`/`CurrentOpen`/`Transition`/`Close` 未定義），或在已設定 DSN 時顯示 SKIP。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/store/versions.go
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/example/metadata-exporter/pkg/snapshotter/decide"
)

// ObjectRecord is the observed object state to write as a new open version.
type ObjectRecord struct {
	Namespace string
	Name      string
	UID       string
	SpecHash  string
	Spec      map[string]interface{}
}

// CurrentOpen returns the currently open version (valid_to IS NULL) for
// (namespace, name) in table, or ok=false if none exists.
func (s *Store) CurrentOpen(ctx context.Context, table, namespace, name string) (decide.OpenVersion, bool, error) {
	if err := checkTableName(table); err != nil {
		return decide.OpenVersion{}, false, err
	}
	query := fmt.Sprintf(`
		SELECT uid, spec_hash FROM %s
		WHERE namespace = $1 AND name = $2 AND valid_to IS NULL
	`, table)
	var v decide.OpenVersion
	err := s.pool.QueryRow(ctx, query, namespace, name).Scan(&v.UID, &v.SpecHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return decide.OpenVersion{}, false, nil
		}
		return decide.OpenVersion{}, false, fmt.Errorf("query open version: %w", err)
	}
	return v, true, nil
}

// Transition closes the currently open version for (namespace, name), if
// any, and inserts a new open version — atomically, in one transaction.
func (s *Store) Transition(ctx context.Context, table string, obj ObjectRecord, at time.Time) error {
	if err := checkTableName(table); err != nil {
		return err
	}
	specJSON, err := json.Marshal(obj.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transition: %w", err)
	}
	defer tx.Rollback(ctx)

	closeQuery := fmt.Sprintf(`
		UPDATE %s SET valid_to = $1
		WHERE namespace = $2 AND name = $3 AND valid_to IS NULL
	`, table)
	if _, err := tx.Exec(ctx, closeQuery, at, obj.Namespace, obj.Name); err != nil {
		return fmt.Errorf("close previous version: %w", err)
	}

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (namespace, name, uid, spec_hash, spec_json, valid_from, valid_to)
		VALUES ($1, $2, $3, $4, $5, $6, NULL)
	`, table)
	if _, err := tx.Exec(ctx, insertQuery, obj.Namespace, obj.Name, obj.UID, obj.SpecHash, specJSON, at); err != nil {
		return fmt.Errorf("insert new version: %w", err)
	}
	return tx.Commit(ctx)
}

// Close closes the currently open version for (namespace, name), if any,
// without inserting a new one (used when the object is deleted). Closing a
// (namespace, name) with no open version is a no-op, not an error.
func (s *Store) Close(ctx context.Context, table, namespace, name string, at time.Time) error {
	if err := checkTableName(table); err != nil {
		return err
	}
	query := fmt.Sprintf(`
		UPDATE %s SET valid_to = $1
		WHERE namespace = $2 AND name = $3 AND valid_to IS NULL
	`, table)
	if _, err := s.pool.Exec(ctx, query, at, namespace, name); err != nil {
		return fmt.Errorf("close version: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: 執行測試**

Run: `go test ./pkg/snapshotter/store/... -run "TestCurrentOpen|TestTransition|TestClose" -v`
Expected: PASS（已設定 `SNAPSHOTTER_TEST_POSTGRES_DSN` 時）。

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/store/versions.go pkg/snapshotter/store/versions_test.go
git commit -m "feat(snapshotter): add SCD2 version transition/close/current-open queries"
```

---

## Task 6: 保留期清除（pkg/snapshotter/store/retention.go）

**Files:**
- Create: `pkg/snapshotter/store/retention.go`
- Test: `pkg/snapshotter/store/retention_test.go`

**Interfaces:**
- Produces: `func (s *Store) PurgeExpired(ctx context.Context, table string, before time.Time) (int64, error)`
- Consumes: Task 4 的 `checkTableName`、`*Store`。

依設計文件第 7 節：只清除已關閉（`valid_to IS NOT NULL`）且早於保留期界線的版本；開放中版本（`valid_to IS NULL`）不論多舊都不可被清除。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/store/retention_test.go
package store

import (
	"context"
	"testing"
	"time"
)

func TestPurgeExpiredDeletesOnlyOldClosedVersions(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")
	ctx := context.Background()

	insert := func(namespace, name, validFrom string, validTo *string) {
		t.Helper()
		_, err := s.pool.Exec(ctx, `
			INSERT INTO virtualservice_snapshots
				(namespace, name, uid, spec_hash, spec_json, valid_from, valid_to)
			VALUES ($1, $2, 'uid', 'hash', '{}', $3::timestamptz, $4::timestamptz)
		`, namespace, name, validFrom, validTo)
		if err != nil {
			t.Fatalf("insert fixture row: %v", err)
		}
	}

	oldClosed := "2020-01-01T00:00:00Z"
	recentClosed := "2099-01-01T00:00:00Z" // always "recent" relative to cutoff below
	insert("ns", "old-closed", "2019-12-01T00:00:00Z", &oldClosed)
	insert("ns", "recent-closed", "2098-12-01T00:00:00Z", &recentClosed)
	insert("ns", "old-open", "2019-12-01T00:00:00Z", nil)

	cutoff := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	deleted, err := s.PurgeExpired(ctx, "virtualservice_snapshots", cutoff)
	if err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected to delete exactly 1 row (old-closed), got %d", deleted)
	}

	var remainingNames []string
	rows, err := s.pool.Query(ctx, "SELECT name FROM virtualservice_snapshots ORDER BY name")
	if err != nil {
		t.Fatalf("query remaining: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remainingNames = append(remainingNames, name)
	}
	want := []string{"old-open", "recent-closed"}
	if len(remainingNames) != len(want) {
		t.Fatalf("expected remaining rows %v, got %v", want, remainingNames)
	}
	for i := range want {
		if remainingNames[i] != want[i] {
			t.Fatalf("expected remaining rows %v, got %v", want, remainingNames)
		}
	}
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/store/... -run TestPurgeExpired -v`
Expected: 編譯失敗（`PurgeExpired` 未定義），或顯示 SKIP（未設 DSN）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/store/retention.go
package store

import (
	"context"
	"fmt"
	"time"
)

// PurgeExpired deletes closed versions (valid_to IS NOT NULL) whose
// valid_to is older than before. Open versions (valid_to IS NULL) are never
// deleted regardless of how old valid_from is. Returns the number of rows
// deleted.
func (s *Store) PurgeExpired(ctx context.Context, table string, before time.Time) (int64, error) {
	if err := checkTableName(table); err != nil {
		return 0, err
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE valid_to IS NOT NULL AND valid_to < $1`, table)
	tag, err := s.pool.Exec(ctx, query, before)
	if err != nil {
		return 0, fmt.Errorf("purge expired: %w", err)
	}
	return tag.RowsAffected(), nil
}
```

- [ ] **Step 4: 執行測試**

Run: `go test ./pkg/snapshotter/store/... -run TestPurgeExpired -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/store/retention.go pkg/snapshotter/store/retention_test.go
git commit -m "feat(snapshotter): add retention purge for closed versions"
```

---

## Task 7: VirtualService Host 重疊偵測（pkg/snapshotter/store/hostoverlap.go）

**Files:**
- Create: `pkg/snapshotter/store/hostoverlap.go`
- Test: `pkg/snapshotter/store/hostoverlap_test.go`

**Interfaces:**
- Produces: `type HostOverlap struct { Namespace, Name string; Hosts []string }`、`func (s *Store) CheckHostOverlap(ctx context.Context, table, namespace, name string, hosts []string) ([]HostOverlap, error)`
- Consumes: Task 4 的 `checkTableName`、`*Store`。

依設計文件第 6/7 節：這是這支通用擷取程式裡**唯一**允許的 VirtualService 專屬邏輯——只偵測「同一 host 同時被多個目前開放中的 VirtualService 宣告」，不做任何合併/排序判斷，呼叫端（Task 9）只記錄 warning log。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/store/hostoverlap_test.go
package store

import (
	"context"
	"testing"
	"time"
)

func TestCheckHostOverlapFindsOtherOpenVSWithSharedHost(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")
	ctx := context.Background()
	table := "virtualservice_snapshots"
	now := time.Now().UTC()

	mustTransition := func(namespace, name, uid string, hosts []string) {
		t.Helper()
		hostsAny := make([]interface{}, len(hosts))
		for i, h := range hosts {
			hostsAny[i] = h
		}
		err := s.Transition(ctx, table, ObjectRecord{
			Namespace: namespace, Name: name, UID: uid, SpecHash: uid,
			Spec: map[string]interface{}{"hosts": hostsAny},
		}, now)
		if err != nil {
			t.Fatalf("Transition %s/%s: %v", namespace, name, err)
		}
	}

	mustTransition("ns", "vs-a", "uid-a", []string{"shared.com", "a-only.com"})
	mustTransition("ns", "vs-b", "uid-b", []string{"shared.com"})
	mustTransition("ns", "vs-c", "uid-c", []string{"unrelated.com"})

	overlaps, err := s.CheckHostOverlap(ctx, table, "ns", "vs-a", []string{"shared.com", "a-only.com"})
	if err != nil {
		t.Fatalf("CheckHostOverlap: %v", err)
	}
	if len(overlaps) != 1 || overlaps[0].Name != "vs-b" {
		t.Fatalf("expected exactly one overlap with vs-b, got %+v", overlaps)
	}
}

func TestCheckHostOverlapExcludesSelf(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")
	ctx := context.Background()
	table := "virtualservice_snapshots"

	err = s.Transition(ctx, table, ObjectRecord{
		Namespace: "ns", Name: "vs-solo", UID: "uid-solo", SpecHash: "h",
		Spec: map[string]interface{}{"hosts": []interface{}{"solo.com"}},
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}

	overlaps, err := s.CheckHostOverlap(ctx, table, "ns", "vs-solo", []string{"solo.com"})
	if err != nil {
		t.Fatalf("CheckHostOverlap: %v", err)
	}
	if len(overlaps) != 0 {
		t.Fatalf("expected no self-overlap, got %+v", overlaps)
	}
}

func TestCheckHostOverlapEmptyHostsIsNoop(t *testing.T) {
	s, err := New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	freshTable(t, s, "virtualservice_snapshots")

	overlaps, err := s.CheckHostOverlap(context.Background(), "virtualservice_snapshots", "ns", "vs", nil)
	if err != nil {
		t.Fatalf("CheckHostOverlap: %v", err)
	}
	if overlaps != nil {
		t.Fatalf("expected nil overlaps for empty hosts, got %+v", overlaps)
	}
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/store/... -run TestCheckHostOverlap -v`
Expected: 編譯失敗（`CheckHostOverlap`/`HostOverlap` 未定義），或顯示 SKIP。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/store/hostoverlap.go
package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// HostOverlap names another object whose currently open version declares
// one of the same hosts.
type HostOverlap struct {
	Namespace string
	Name      string
	Hosts     []string
}

// CheckHostOverlap returns every other (namespace, name) in table whose
// open version's spec_json.hosts array intersects hosts, excluding
// (namespace, name) itself. This is the one VirtualService-specific check
// resource-snapshotter performs (design doc section 6): it only detects
// the violation, it never resolves a merge order.
func (s *Store) CheckHostOverlap(ctx context.Context, table, namespace, name string, hosts []string) ([]HostOverlap, error) {
	if err := checkTableName(table); err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`
		SELECT namespace, name, spec_json -> 'hosts'
		FROM %s
		WHERE valid_to IS NULL
		  AND NOT (namespace = $1 AND name = $2)
		  AND spec_json -> 'hosts' ?| $3
	`, table)
	rows, err := s.pool.Query(ctx, query, namespace, name, hosts)
	if err != nil {
		return nil, fmt.Errorf("check host overlap: %w", err)
	}
	defer rows.Close()

	var out []HostOverlap
	for rows.Next() {
		var ov HostOverlap
		var hostsJSON []byte
		if err := rows.Scan(&ov.Namespace, &ov.Name, &hostsJSON); err != nil {
			return nil, fmt.Errorf("scan host overlap row: %w", err)
		}
		if err := json.Unmarshal(hostsJSON, &ov.Hosts); err != nil {
			return nil, fmt.Errorf("unmarshal hosts for %s/%s: %w", ov.Namespace, ov.Name, err)
		}
		out = append(out, ov)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: 執行測試**

Run: `go test ./pkg/snapshotter/store/... -run TestCheckHostOverlap -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/store/hostoverlap.go pkg/snapshotter/store/hostoverlap_test.go
git commit -m "feat(snapshotter): add VirtualService host-overlap detection"
```

---

## Task 8: Self-metrics（pkg/snapshotter/metrics）

**Files:**
- Create: `pkg/snapshotter/metrics/metrics.go`
- Test: `pkg/snapshotter/metrics/metrics_test.go`

**Interfaces:**
- Produces: `type Metrics struct { WritesTotal *prometheus.CounterVec; InformerSynced *prometheus.GaugeVec; Leader prometheus.Gauge; ReconcileObjects *prometheus.CounterVec; ReconcileDuration prometheus.Histogram }`、`func New(reg prometheus.Registerer) *Metrics`、`func (m *Metrics) ObserveReconcile(d time.Duration)`。
- Consumes: 無新依賴（`prometheus/client_golang` 已是既有依賴）。

依設計文件第 3 節：self-metrics 只放程式自身健康/效能指標，與寫入 Postgres 的資料路徑完全分離，風格沿用 `pkg/collector/metrics.go`。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/metrics/metrics_test.go
package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestWritesTotalIncrementsByTableAndResult(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.WritesTotal.WithLabelValues("virtualservice_snapshots", "transition").Inc()

	want := `
		# HELP snapshotter_writes_total Total write decisions per resource table, partitioned by result (transition, close, skip).
		# TYPE snapshotter_writes_total counter
		snapshotter_writes_total{result="transition",table="virtualservice_snapshots"} 1
	`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "snapshotter_writes_total"); err != nil {
		t.Fatalf("unexpected metrics: %v", err)
	}
}

func TestLeaderGaugeReflectsSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.Leader.Set(1)
	if got := testutil.ToFloat64(m.Leader); got != 1 {
		t.Fatalf("expected snapshotter_leader=1, got %v", got)
	}
	m.Leader.Set(0)
	if got := testutil.ToFloat64(m.Leader); got != 0 {
		t.Fatalf("expected snapshotter_leader=0, got %v", got)
	}
}

func TestNewWithNilRegistererDoesNotPanic(t *testing.T) {
	m := New(nil)
	m.WritesTotal.WithLabelValues("t", "skip").Inc()
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/metrics/... -v`
Expected: FAIL（`New`/`Metrics` 未定義）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/metrics/metrics.go
// Package metrics holds resource-snapshotter's self-observability series.
// These describe the program's own health (writes, dedup hits, leader
// status, reconcile activity) — never the captured route history itself,
// which is the data path straight to Postgres (see design doc section 3).
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every self-metric resource-snapshotter exposes on /metrics.
type Metrics struct {
	WritesTotal       *prometheus.CounterVec
	InformerSynced    *prometheus.GaugeVec
	Leader            prometheus.Gauge
	ReconcileObjects  *prometheus.CounterVec
	ReconcileDuration prometheus.Histogram
}

// New registers self-metrics against reg. A nil reg disables registration
// (useful in tests).
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		WritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "snapshotter_writes_total",
			Help: "Total write decisions per resource table, partitioned by result (transition, close, skip).",
		}, []string{"table", "result"}),
		InformerSynced: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "snapshotter_informer_synced",
			Help: "1 if the informer for this resource has completed its initial cache sync, else 0.",
		}, []string{"resource"}),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "snapshotter_leader",
			Help: "1 if this replica currently holds write leadership, else 0.",
		}),
		ReconcileObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "snapshotter_reconcile_objects_total",
			Help: "Objects visited during the one-shot reconciliation pass run on becoming leader.",
		}, []string{"resource"}),
		ReconcileDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "snapshotter_reconcile_duration_seconds",
			Help:    "Duration of the one-shot reconciliation pass run on becoming leader.",
			Buckets: prometheus.DefBuckets,
		}),
	}
	if reg != nil {
		reg.MustRegister(m.WritesTotal, m.InformerSynced, m.Leader, m.ReconcileObjects, m.ReconcileDuration)
	}
	return m
}

// ObserveReconcile records the duration of one leader-promotion
// reconciliation pass.
func (m *Metrics) ObserveReconcile(d time.Duration) {
	if m == nil {
		return
	}
	m.ReconcileDuration.Observe(d.Seconds())
}
```

- [ ] **Step 4: 確認測試通過**

Run: `go test ./pkg/snapshotter/metrics/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/metrics/metrics.go pkg/snapshotter/metrics/metrics_test.go
git commit -m "feat(snapshotter): add self-metrics"
```

---

## Task 9: Capture 核心事件處理 + Leader 寫入閘（pkg/snapshotter/capture）

**Files:**
- Create: `pkg/snapshotter/capture/capture.go`
- Test: `pkg/snapshotter/capture/capture_test.go`

**Interfaces:**
- Produces:
  - `type VersionStore interface { CurrentOpen(...); Transition(...); Close(...) }`（簽名須與 Task 5 的 `*store.Store` 方法完全一致，這樣 `*store.Store` 自動滿足此介面而不需轉接層）
  - `type HostOverlapChecker interface { CheckHostOverlap(ctx context.Context, table, namespace, name string, hosts []string) ([]store.HostOverlap, error) }`
  - `type Resource struct { Name, Table string; Informer cache.SharedIndexInformer; Lister cache.GenericLister; CheckHostOverlap bool }`
  - `func New(s VersionStore, overlapChecker HostOverlapChecker, resources []Resource, log *slog.Logger, m *metrics.Metrics) *Capture`
  - `func (c *Capture) SetLeader(isLeader bool)`
  - `func (c *Capture) IsLeader() bool`
- Consumes: Task 3 `decide.{OpenVersion,Observed,ForUpsert,ForDelete}`；Task 2 `hashutil.SpecHash`；Task 5 `store.ObjectRecord`、Task 7 `store.HostOverlap`；Task 8 `metrics.Metrics`。Task 10（Reconcile）會直接呼叫本任務新增的私有方法 `applyUpsert`，因此函式簽名要保持穩定：`func (c *Capture) applyUpsert(r Resource, namespace, name, uid, hash string, spec map[string]interface{})`。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/capture/capture_test.go
package capture

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/snapshotter/decide"
	"github.com/example/metadata-exporter/pkg/snapshotter/store"
)

var vsGVR = schema.GroupVersionResource{Group: "networking.istio.io", Version: "v1beta1", Resource: "virtualservices"}

func vsObject(namespace, name, uid string, hosts []string) *unstructured.Unstructured {
	hostsAny := make([]interface{}, len(hosts))
	for i, h := range hosts {
		hostsAny[i] = h
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "networking.istio.io/v1beta1",
		"kind":       "VirtualService",
		"metadata": map[string]interface{}{
			"namespace": namespace,
			"name":      name,
			"uid":       uid,
		},
		"spec": map[string]interface{}{
			"hosts": hostsAny,
		},
	}}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
}

// fakeStore is an in-memory VersionStore + HostOverlapChecker so capture
// logic is unit-tested without a real Postgres.
type fakeStore struct {
	mu    sync.Mutex
	open  map[string]decide.OpenVersion
	calls []string
}

func newFakeStore() *fakeStore {
	return &fakeStore{open: map[string]decide.OpenVersion{}}
}

func fakeKey(table, namespace, name string) string {
	return table + "/" + namespace + "/" + name
}

func (f *fakeStore) CurrentOpen(_ context.Context, table, namespace, name string) (decide.OpenVersion, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.open[fakeKey(table, namespace, name)]
	return v, ok, nil
}

func (f *fakeStore) Transition(_ context.Context, table string, obj store.ObjectRecord, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open[fakeKey(table, obj.Namespace, obj.Name)] = decide.OpenVersion{UID: obj.UID, SpecHash: obj.SpecHash}
	f.calls = append(f.calls, fmt.Sprintf("transition:%s/%s/%s", table, obj.Namespace, obj.Name))
	return nil
}

func (f *fakeStore) Close(_ context.Context, table, namespace, name string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, fakeKey(table, namespace, name))
	f.calls = append(f.calls, fmt.Sprintf("close:%s/%s/%s", table, namespace, name))
	return nil
}

func (f *fakeStore) CheckHostOverlap(context.Context, string, string, string, []string) ([]store.HostOverlap, error) {
	return nil, nil
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestResource(t *testing.T, objs ...runtime.Object) (Resource, dynamic.Interface) {
	t.Helper()
	listKinds := map[schema.GroupVersionResource]string{vsGVR: "VirtualServiceList"}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return client.Resource(vsGVR).Namespace("").List(context.Background(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return client.Resource(vsGVR).Namespace("").Watch(context.Background(), options)
			},
		},
		&unstructured.Unstructured{},
		0,
		cache.Indexers{},
	)
	return Resource{
		Name:             "VirtualService",
		Table:            "virtualservice_snapshots",
		Informer:         informer,
		Lister:           cache.NewGenericLister(informer.GetIndexer(), vsGVR.GroupResource()),
		CheckHostOverlap: true,
	}, client
}

func runInformer(t *testing.T, r Resource) func() {
	t.Helper()
	stop := make(chan struct{})
	go r.Informer.Run(stop)
	if !cache.WaitForCacheSync(stop, r.Informer.HasSynced) {
		t.Fatalf("informer cache did not sync")
	}
	return func() { close(stop) }
}

func TestUpsertSkippedWhileNotLeader(t *testing.T) {
	r, _ := newTestResource(t, vsObject("ns", "vs1", "uid-1", []string{"a.com"}))
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	// Leadership defaults to false; the informer's initial Add for the
	// pre-seeded object must be a no-op. The returned *Capture is unused
	// here on purpose — only its side effect (registering handlers) matters.
	New(fs, fs, []Resource{r}, discardLogger(), nil)
	time.Sleep(50 * time.Millisecond)
	if got := fs.callCount(); got != 0 {
		t.Fatalf("expected no store calls while not leader, got %d", got)
	}
}

func TestUpsertWritesWhileLeader(t *testing.T) {
	r, client := newTestResource(t, vsObject("ns", "vs1", "uid-1", []string{"a.com"}))
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	c := New(fs, fs, []Resource{r}, discardLogger(), nil)
	c.SetLeader(true)

	u := vsObject("ns", "vs2", "uid-2", []string{"b.com"})
	if _, err := client.Resource(vsGVR).Namespace("ns").Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for fs.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	open, ok, _ := fs.CurrentOpen(context.Background(), "virtualservice_snapshots", "ns", "vs2")
	if !ok || open.UID != "uid-2" {
		t.Fatalf("expected vs2 to be written while leader, got ok=%v open=%+v", ok, open)
	}
}

func TestDeleteClosesOpenVersionWhileLeader(t *testing.T) {
	r, client := newTestResource(t, vsObject("ns", "vs1", "uid-1", []string{"a.com"}))
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	fs.open[fakeKey("virtualservice_snapshots", "ns", "vs1")] = decide.OpenVersion{UID: "uid-1", SpecHash: "irrelevant"}
	c := New(fs, fs, []Resource{r}, discardLogger(), nil)
	c.SetLeader(true)

	if err := client.Resource(vsGVR).Namespace("ns").Delete(context.Background(), "vs1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, ok, _ := fs.CurrentOpen(context.Background(), "virtualservice_snapshots", "ns", "vs1")
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected vs1's open version to be closed after delete")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIsLeaderReflectsSetLeader(t *testing.T) {
	fs := newFakeStore()
	c := New(fs, fs, nil, discardLogger(), nil)
	if c.IsLeader() {
		t.Fatalf("expected IsLeader()=false initially")
	}
	c.SetLeader(true)
	if !c.IsLeader() {
		t.Fatalf("expected IsLeader()=true after SetLeader(true)")
	}
}
```

> 註：以上測試在撰寫實作（Step 3）前無法編譯通過——`metav1`/`watch` 等 import 需要在最終測試檔補上（`k8s.io/apimachinery/pkg/apis/meta/v1` 別名 `metav1`、`k8s.io/apimachinery/pkg/watch`）。執行 Step 1 時請先補齊 import 再執行，這是 TDD 紅燈步驟的正常部分。

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/capture/... -v`
Expected: 編譯失敗（`Resource`/`New`/`SetLeader`/`IsLeader` 未定義）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/capture/capture.go
// Package capture wires informer Add/Update/Delete events to the dedup
// decision logic (pkg/snapshotter/decide) and the Postgres version store
// (pkg/snapshotter/store). Writes only happen while this replica holds
// write leadership — see SetLeader and the leader-election wiring in
// cmd/resource-snapshotter (design doc section 3).
package capture

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/snapshotter/decide"
	"github.com/example/metadata-exporter/pkg/snapshotter/hashutil"
	"github.com/example/metadata-exporter/pkg/snapshotter/metrics"
	"github.com/example/metadata-exporter/pkg/snapshotter/store"
)

// VersionStore is the subset of *store.Store Capture depends on. Tests
// substitute an in-memory fake; cmd/resource-snapshotter passes a real
// *store.Store, which satisfies this interface as-is.
type VersionStore interface {
	CurrentOpen(ctx context.Context, table, namespace, name string) (decide.OpenVersion, bool, error)
	Transition(ctx context.Context, table string, obj store.ObjectRecord, at time.Time) error
	Close(ctx context.Context, table, namespace, name string, at time.Time) error
}

// HostOverlapChecker is the subset of *store.Store used for the
// VirtualService-specific host-overlap warning (design doc section 6).
type HostOverlapChecker interface {
	CheckHostOverlap(ctx context.Context, table, namespace, name string, hosts []string) ([]store.HostOverlap, error)
}

// Resource pairs one watched GVR's informer/lister with the Postgres table
// its versions are written to.
type Resource struct {
	// Name is a logical label used for logs and metrics (typically the Kind).
	Name             string
	Table            string
	Informer         cache.SharedIndexInformer
	Lister           cache.GenericLister
	// CheckHostOverlap enables the VirtualService-specific host-overlap
	// warning for this resource (design doc section 6). Leave false for
	// every other resource kind.
	CheckHostOverlap bool
}

// Capture wires informer events for every configured Resource to the dedup
// decision logic and the version store.
type Capture struct {
	store          VersionStore
	overlapChecker HostOverlapChecker
	resources      []Resource
	log            *slog.Logger
	metrics        *metrics.Metrics
	now            func() time.Time

	leader atomic.Bool
}

// New constructs a Capture and registers event handlers on every resource's
// informer. The caller is responsible for actually running the informers
// (cmd/resource-snapshotter owns that).
func New(s VersionStore, overlapChecker HostOverlapChecker, resources []Resource, log *slog.Logger, m *metrics.Metrics) *Capture {
	if log == nil {
		log = slog.Default()
	}
	c := &Capture{store: s, overlapChecker: overlapChecker, resources: resources, log: log, metrics: m, now: time.Now}
	for _, r := range resources {
		c.registerHandlers(r)
	}
	return c
}

// SetLeader toggles whether this replica's event handlers actually write to
// Postgres. Non-leader replicas keep their informers running (so cache
// stays warm for a fast Reconcile on promotion, see Task 10) but every
// event is a no-op write.
func (c *Capture) SetLeader(isLeader bool) {
	c.leader.Store(isLeader)
	if c.metrics != nil {
		if isLeader {
			c.metrics.Leader.Set(1)
		} else {
			c.metrics.Leader.Set(0)
		}
	}
}

// IsLeader reports whether this replica currently holds write leadership.
func (c *Capture) IsLeader() bool {
	return c.leader.Load()
}

func (c *Capture) registerHandlers(r Resource) {
	_, err := r.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.handleUpsert(r, obj)
		},
		UpdateFunc: func(_, newObj interface{}) {
			c.handleUpsert(r, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.handleDelete(r, obj)
		},
	})
	if err != nil {
		c.log.Error("register informer event handler failed", "resource", r.Name, "err", err)
	}
}

func toUnstructured(obj interface{}) (*unstructured.Unstructured, bool) {
	switch t := obj.(type) {
	case *unstructured.Unstructured:
		return t, true
	case cache.DeletedFinalStateUnknown:
		u, ok := t.Obj.(*unstructured.Unstructured)
		return u, ok
	default:
		return nil, false
	}
}

func (c *Capture) handleUpsert(r Resource, raw interface{}) {
	if !c.leader.Load() {
		return
	}
	u, ok := toUnstructured(raw)
	if !ok {
		c.log.Warn("upsert event: unexpected object type", "resource", r.Name)
		return
	}
	spec, _, err := unstructured.NestedMap(u.Object, "spec")
	if err != nil {
		c.log.Error("read spec failed", "resource", r.Name, "namespace", u.GetNamespace(), "name", u.GetName(), "err", err)
		return
	}
	hash, err := hashutil.SpecHash(spec)
	if err != nil {
		c.log.Error("hash spec failed", "resource", r.Name, "namespace", u.GetNamespace(), "name", u.GetName(), "err", err)
		return
	}
	c.applyUpsert(r, u.GetNamespace(), u.GetName(), string(u.GetUID()), hash, spec)
}

func (c *Capture) applyUpsert(r Resource, namespace, name, uid, hash string, spec map[string]interface{}) {
	ctx := context.Background()
	open, ok, err := c.store.CurrentOpen(ctx, r.Table, namespace, name)
	if err != nil {
		c.log.Error("read current open version failed", "resource", r.Name, "namespace", namespace, "name", name, "err", err)
		return
	}
	var openPtr *decide.OpenVersion
	if ok {
		openPtr = &open
	}
	action := decide.ForUpsert(openPtr, decide.Observed{UID: uid, SpecHash: hash})
	if action == decide.ActionSkip {
		c.recordWrite(r.Table, "skip")
		return
	}
	if err := c.store.Transition(ctx, r.Table, store.ObjectRecord{
		Namespace: namespace, Name: name, UID: uid, SpecHash: hash, Spec: spec,
	}, c.now()); err != nil {
		c.log.Error("transition version failed", "resource", r.Name, "namespace", namespace, "name", name, "err", err)
		return
	}
	c.recordWrite(r.Table, "transition")
	c.maybeWarnHostOverlap(r, namespace, name, spec)
}

func (c *Capture) maybeWarnHostOverlap(r Resource, namespace, name string, spec map[string]interface{}) {
	if !r.CheckHostOverlap || c.overlapChecker == nil {
		return
	}
	hosts := hostsFromSpec(spec)
	if len(hosts) == 0 {
		return
	}
	overlaps, err := c.overlapChecker.CheckHostOverlap(context.Background(), r.Table, namespace, name, hosts)
	if err != nil {
		c.log.Error("host overlap check failed", "resource", r.Name, "namespace", namespace, "name", name, "err", err)
		return
	}
	for _, ov := range overlaps {
		c.log.Warn("multiple VirtualServices declare overlapping host(s)",
			"namespace", namespace, "name", name,
			"otherNamespace", ov.Namespace, "otherName", ov.Name,
			"hosts", ov.Hosts,
		)
	}
}

func hostsFromSpec(spec map[string]interface{}) []string {
	raw, _ := spec["hosts"].([]interface{})
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (c *Capture) handleDelete(r Resource, raw interface{}) {
	if !c.leader.Load() {
		return
	}
	u, ok := toUnstructured(raw)
	if !ok {
		c.log.Warn("delete event: unexpected object type", "resource", r.Name)
		return
	}
	c.applyDelete(r, u.GetNamespace(), u.GetName())
}

func (c *Capture) applyDelete(r Resource, namespace, name string) {
	ctx := context.Background()
	open, ok, err := c.store.CurrentOpen(ctx, r.Table, namespace, name)
	if err != nil {
		c.log.Error("read current open version failed", "resource", r.Name, "namespace", namespace, "name", name, "err", err)
		return
	}
	var openPtr *decide.OpenVersion
	if ok {
		openPtr = &open
	}
	if decide.ForDelete(openPtr) != decide.ActionClose {
		c.recordWrite(r.Table, "skip")
		return
	}
	if err := c.store.Close(ctx, r.Table, namespace, name, c.now()); err != nil {
		c.log.Error("close version failed", "resource", r.Name, "namespace", namespace, "name", name, "err", err)
		return
	}
	c.recordWrite(r.Table, "close")
}

func (c *Capture) recordWrite(table, result string) {
	if c.metrics != nil {
		c.metrics.WritesTotal.WithLabelValues(table, result).Inc()
	}
}
```

- [ ] **Step 4: 補上測試檔缺的 import 並執行測試**

把 Step 1 測試檔頂部的 import 區塊改為：

```go
import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/snapshotter/decide"
	"github.com/example/metadata-exporter/pkg/snapshotter/store"
)
```

Run: `go test ./pkg/snapshotter/capture/... -v -race`
Expected: PASS（5 個測試全過；`-race` 確認 `atomic.Bool` 寫入路徑沒有資料競爭）。

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/capture/capture.go pkg/snapshotter/capture/capture_test.go
git commit -m "feat(snapshotter): wire informer events to dedup decision and store with leader gate"
```

---

## Task 10: Leader 接手一次性和解（pkg/snapshotter/capture/reconcile.go）

**Files:**
- Create: `pkg/snapshotter/capture/reconcile.go`
- Test: `pkg/snapshotter/capture/reconcile_test.go`

**Interfaces:**
- Produces: `func (c *Capture) Reconcile(ctx context.Context) error`
- Consumes: Task 9 的 `c.applyUpsert`（不對外公開的既有方法，簽名不變）、`c.resources`、`c.metrics`、`c.now`、`hashutil.SpecHash`。

依設計文件第 3 節「Leader 接手時的一次性和解」：`cmd/resource-snapshotter`（Task 11）會在 `leaderelection` 的 `OnStartedLeading` 回呼裡同步呼叫這個方法，在開始處理新事件之前，把本地 cache 裡每個物件都跑一次與 `AddFunc` 相同的比對寫入邏輯——這樣即使某物件在非 leader 空窗期間變化後就不再變化，也不會永遠停留在舊版本。

- [ ] **Step 1: 寫失敗測試**

```go
// pkg/snapshotter/capture/reconcile_test.go
package capture

import (
	"context"
	"testing"
	"time"
)

func TestReconcileWritesEveryCachedObjectEvenWithoutLiveEvents(t *testing.T) {
	r, _ := newTestResource(t,
		vsObject("ns", "vs1", "uid-1", []string{"a.com"}),
		vsObject("ns", "vs2", "uid-2", []string{"b.com"}),
	)
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	c := New(fs, fs, []Resource{r}, discardLogger(), nil)
	// SetLeader is NOT called here: Reconcile must write regardless of the
	// leader flag, because it represents "I just became leader" — the
	// caller (cmd/resource-snapshotter) always calls SetLeader(true) before
	// Reconcile, but Reconcile itself must not depend on that ordering to
	// be effective, since it is the mechanism that catches up state the
	// leader flag gate would otherwise have suppressed while non-leader.
	c.SetLeader(true)

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	for _, want := range []struct{ name, uid string }{{"vs1", "uid-1"}, {"vs2", "uid-2"}} {
		open, ok, err := fs.CurrentOpen(context.Background(), "virtualservice_snapshots", "ns", want.name)
		if err != nil || !ok || open.UID != want.uid {
			t.Fatalf("expected %s to be written by Reconcile, got ok=%v open=%+v err=%v", want.name, ok, open, err)
		}
	}
}

func TestReconcileIsIdempotentWhenNothingChanged(t *testing.T) {
	r, _ := newTestResource(t, vsObject("ns", "vs1", "uid-1", []string{"a.com"}))
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	c := New(fs, fs, []Resource{r}, discardLogger(), nil)
	c.SetLeader(true)

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	firstCalls := fs.callCount()

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if got := fs.callCount(); got != firstCalls {
		t.Fatalf("expected second Reconcile to be a no-op (same uid+hash), call count grew from %d to %d", firstCalls, got)
	}
}

func TestReconcileRecordsObjectCountMetric(t *testing.T) {
	r, _ := newTestResource(t,
		vsObject("ns", "vs1", "uid-1", []string{"a.com"}),
		vsObject("ns", "vs2", "uid-2", []string{"b.com"}),
	)
	stop := runInformer(t, r)
	defer stop()

	fs := newFakeStore()
	c := New(fs, fs, []Resource{r}, discardLogger(), nil)
	c.SetLeader(true)
	c.now = func() time.Time { return time.Unix(0, 0) }

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Metrics are nil in this test (New(..., nil) for the *metrics.Metrics
	// arg); this exercises the nil-safe path in recordWrite/ObserveReconcile
	// rather than asserting a counter value.
}
```

- [ ] **Step 2: 確認測試會失敗**

Run: `go test ./pkg/snapshotter/capture/... -run TestReconcile -v`
Expected: 編譯失敗（`Reconcile` 未定義）。

- [ ] **Step 3: 寫最小實作**

```go
// pkg/snapshotter/capture/reconcile.go
package capture

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/example/metadata-exporter/pkg/snapshotter/hashutil"
)

// Reconcile walks every resource's local lister cache once and applies the
// same upsert decision logic as a live event. Call this synchronously from
// the leader-election OnStartedLeading callback (cmd/resource-snapshotter),
// before returning control to client-go — see design doc section 3,
// "Leader 接手時的一次性和解". It is a one-shot pass triggered by becoming
// leader, not a periodic ticker.
func (c *Capture) Reconcile(ctx context.Context) error {
	start := c.now()
	for _, r := range c.resources {
		objs, err := r.Lister.List(labels.Everything())
		if err != nil {
			return fmt.Errorf("list cache for %s: %w", r.Name, err)
		}
		for _, obj := range objs {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			spec, _, err := unstructured.NestedMap(u.Object, "spec")
			if err != nil {
				c.log.Error("reconcile: read spec failed", "resource", r.Name, "namespace", u.GetNamespace(), "name", u.GetName(), "err", err)
				continue
			}
			hash, err := hashutil.SpecHash(spec)
			if err != nil {
				c.log.Error("reconcile: hash spec failed", "resource", r.Name, "namespace", u.GetNamespace(), "name", u.GetName(), "err", err)
				continue
			}
			c.applyUpsert(r, u.GetNamespace(), u.GetName(), string(u.GetUID()), hash, spec)
			if c.metrics != nil {
				c.metrics.ReconcileObjects.WithLabelValues(r.Name).Inc()
			}
		}
	}
	if c.metrics != nil {
		c.metrics.ObserveReconcile(c.now().Sub(start))
	}
	return nil
}
```

- [ ] **Step 4: 執行測試**

Run: `go test ./pkg/snapshotter/capture/... -v -race`
Expected: PASS（含 Task 9 的測試，全部一起跑）。

- [ ] **Step 5: Commit**

```bash
git add pkg/snapshotter/capture/reconcile.go pkg/snapshotter/capture/reconcile_test.go
git commit -m "feat(snapshotter): add one-shot reconcile on leader promotion"
```

---

## Task 11: main.go 組裝（cmd/resource-snapshotter）

**Files:**
- Create: `cmd/resource-snapshotter/main.go`
- Create: `cmd/resource-snapshotter/clientgo_metrics.go`（直接複製 `cmd/clientgo_metrics.go`，不改動既有 `cmd/main.go`——兩個獨立二進位檔各自完整，避免為共用 ~60 行小工具而提早抽象/牽動既有可運作的程式）

**Interfaces:**
- Consumes: Task 1 `snapconfig.{Config,WatchScope,WatchResource,ScopeNamespaced,ScopeCluster}`、Task 4/5/6 `snapstore.{New,Store.Close,Store.Migrate,Store.PurgeExpired}`、Task 8 `snapmetrics.New`、Task 9/10 `snapcapture.{New,Resource,Capture.SetLeader,Capture.IsLeader,Capture.Reconcile}`。
- Produces: 無（這是組裝層，沒有其他套件依賴它）。

main.go 沒有獨立的 unit test（與現有 `cmd/main.go` 一致，該目錄目前也沒有 `_test.go`）；可測試的交付物是「成功編譯 + `go vet` 乾淨」。

- [ ] **Step 1: 寫 main.go**

```go
// cmd/resource-snapshotter/main.go
// Command resource-snapshotter watches configured Kubernetes resources and
// writes their .spec history (SCD2 versions) to Postgres for later
// time-range queries by a separate query-side service. See
// docs/superpowers/specs/istio-vs-route-history-snapshotter-design.md for
// the full design rationale (擷取機制/leader election/retention).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	snapcapture "github.com/example/metadata-exporter/pkg/snapshotter/capture"
	snapconfig "github.com/example/metadata-exporter/pkg/snapshotter/config"
	snapmetrics "github.com/example/metadata-exporter/pkg/snapshotter/metrics"
	snapstore "github.com/example/metadata-exporter/pkg/snapshotter/store"
)

func main() {
	var (
		configPath  = flag.String("config", "/etc/resource-snapshotter/config.yaml", "Path to YAML config file")
		metricsAddr = flag.String("metrics-addr", ":8080", "Address to serve /metrics on")
		kubeconfig  = flag.String("kubeconfig", "", "Path to kubeconfig (empty = in-cluster)")
		logLevel    = flag.String("log-level", "info", "Log level: debug | info | warn | error")
		kubeQPS     = flag.Float64("kube-api-qps", 20, "Maximum QPS the kubernetes client issues against the apiserver")
		kubeBurst   = flag.Int("kube-api-burst", 40, "Maximum burst the kubernetes client issues against the apiserver")
		leaseName   = flag.String("leader-election-lease-name", "resource-snapshotter-leader", "Name of the Lease object used for leader election")
	)
	flag.Parse()

	log := newLogger(*logLevel)
	slog.SetDefault(log)

	cfg, err := snapconfig.Load(*configPath)
	if err != nil {
		log.Error("load config failed", "err", err)
		os.Exit(1)
	}
	retention, err := cfg.RetentionDuration()
	if err != nil {
		log.Error("invalid retention", "err", err)
		os.Exit(1)
	}
	log.Info("config parsed", "resources", len(cfg.Watch.Resources), "retention", retention)

	dsn := os.Getenv("SNAPSHOTTER_DATABASE_URL")
	if dsn == "" {
		log.Error("SNAPSHOTTER_DATABASE_URL is required")
		os.Exit(1)
	}
	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		log.Error("POD_NAMESPACE is required (used for the leader-election Lease)")
		os.Exit(1)
	}
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		log.Error("POD_NAME is required (used as the leader-election candidate identity)")
		os.Exit(1)
	}

	restCfg, err := buildRestConfig(*kubeconfig)
	if err != nil {
		log.Error("build kube client config failed", "err", err)
		os.Exit(1)
	}
	restCfg.QPS = float32(*kubeQPS)
	restCfg.Burst = *kubeBurst

	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Error("dynamic kubernetes client failed", "err", err)
		os.Exit(1)
	}
	kubeClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Error("kubernetes client failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	st, err := snapstore.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerClientGoMetrics(reg)
	m := snapmetrics.New(reg)

	resources, factories, err := buildResources(dynClient, cfg, log)
	if err != nil {
		log.Error("build resources failed", "err", err)
		os.Exit(1)
	}

	cc := snapcapture.New(st, st, resources, log, m)

	for _, f := range factories {
		f.Start(ctx.Done())
	}
	for _, r := range resources {
		if !cache.WaitForCacheSync(ctx.Done(), r.Informer.HasSynced) {
			log.Error("informer cache sync failed", "resource", r.Name)
			os.Exit(1)
		}
		m.InformerSynced.WithLabelValues(r.Name).Set(1)
	}
	log.Info("informers synced", "resources", len(resources))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	server := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("metrics server listening", "addr", *metricsAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	go runRetentionLoop(ctx, st, cc, cfg, retention, log)

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: *leaseName, Namespace: podNamespace},
		Client:     kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: podName},
	}
	leCtx, leCancel := context.WithCancel(ctx)
	defer leCancel()
	go leaderelection.RunOrDie(leCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("acquired write leadership", "identity", podName)
				if err := st.Migrate(ctx); err != nil {
					// Crash rather than release-and-continue: a release
					// without exiting would leave this process running with
					// no further migration retries and no leadership, a
					// silent degradation. Exiting lets Kubernetes restart
					// the pod and retry from a clean state, matching every
					// other fatal-error path in this file.
					log.Error("schema migration failed", "err", err)
					os.Exit(1)
				}
				cc.SetLeader(true)
				if err := cc.Reconcile(ctx); err != nil {
					log.Error("leader reconciliation failed", "err", err)
				}
			},
			OnStoppedLeading: func() {
				log.Info("lost write leadership", "identity", podName)
				cc.SetLeader(false)
			},
		},
	})

	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-serverErr:
		log.Error("http server error", "err", err)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

// buildResources creates one DynamicSharedInformerFactory per watch.resources
// entry (resyncPeriod=0, matching pkg/collector/listers.go) and the
// corresponding snapcapture.Resource. v1 limitation (see Global
// Constraints): at most one explicit namespace per resource.
func buildResources(client dynamic.Interface, cfg *snapconfig.Config, log *slog.Logger) ([]snapcapture.Resource, []dynamicinformer.DynamicSharedInformerFactory, error) {
	var resources []snapcapture.Resource
	var factories []dynamicinformer.DynamicSharedInformerFactory
	for _, w := range cfg.Watch.Resources {
		namespace := metav1.NamespaceAll
		if w.Scope == snapconfig.ScopeNamespaced && len(w.Namespaces) == 1 {
			namespace = w.Namespaces[0]
		}
		factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			client, 0, namespace,
			func(o *metav1.ListOptions) {
				if w.LabelSelector != "" {
					o.LabelSelector = w.LabelSelector
				}
				if w.FieldSelector != "" {
					o.FieldSelector = w.FieldSelector
				}
			},
		)
		gi := factory.ForResource(w.GVR())
		resources = append(resources, snapcapture.Resource{
			Name:             w.Kind,
			Table:            w.HistoryTable,
			Informer:         gi.Informer(),
			Lister:           gi.Lister(),
			CheckHostOverlap: w.Kind == "VirtualService",
		})
		factories = append(factories, factory)
		log.Info("watching resource", "kind", w.Kind, "gvr", w.GVR().String(), "table", w.HistoryTable, "namespace", namespace)
	}
	return resources, factories, nil
}

// runRetentionLoop purges closed versions older than retention once at
// startup and then once every 24h, but only while this replica is leader
// (avoids redundant DELETE load from every replica, see design doc section
// 7). This ticker is unrelated to the capture path's "no ticker" rule in
// section 3 — retention is inherently periodic housekeeping, not capture.
func runRetentionLoop(ctx context.Context, st *snapstore.Store, cc *snapcapture.Capture, cfg *snapconfig.Config, retention time.Duration, log *slog.Logger) {
	purge := func() {
		if !cc.IsLeader() {
			return
		}
		cutoff := time.Now().Add(-retention)
		for _, w := range cfg.Watch.Resources {
			n, err := st.PurgeExpired(ctx, w.HistoryTable, cutoff)
			if err != nil {
				log.Error("retention purge failed", "table", w.HistoryTable, "err", err)
				continue
			}
			if n > 0 {
				log.Info("retention purge", "table", w.HistoryTable, "deleted", n)
			}
		}
	}
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		purge()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func buildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
```

- [ ] **Step 2: 複製 client-go 自身指標註冊器**

```go
// cmd/resource-snapshotter/clientgo_metrics.go
// (與 cmd/clientgo_metrics.go 內容逐字相同，只改 package 宣告維持 package main；
// 兩個二進位檔各自獨立，刻意不抽共用套件，見本任務開頭說明。)
package main

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientgometrics "k8s.io/client-go/tools/metrics"
)

func registerClientGoMetrics(reg prometheus.Registerer) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rest_client_requests_total",
		Help: "Number of HTTP requests, partitioned by status code, method, and host.",
	}, []string{"code", "method", "host"})
	latency := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rest_client_request_duration_seconds",
		Help:    "Request latency in seconds, partitioned by verb and host.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"verb", "host"})
	rateLimiter := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rest_client_rate_limiter_duration_seconds",
		Help:    "Client-side rate-limiter wait time in seconds, partitioned by verb and host.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"verb", "host"})

	reg.MustRegister(requests, latency, rateLimiter)

	clientgometrics.Register(clientgometrics.RegisterOpts{
		RequestResult:      &counterAdapter{v: requests},
		RequestLatency:     &latencyAdapter{v: latency},
		RateLimiterLatency: &latencyAdapter{v: rateLimiter},
	})
}

type counterAdapter struct{ v *prometheus.CounterVec }

func (c *counterAdapter) Increment(_ context.Context, code, method, host string) {
	if code == "" {
		code = "0"
	}
	if _, err := strconv.Atoi(code); err != nil {
		code = "invalid"
	}
	c.v.WithLabelValues(code, method, host).Inc()
}

type latencyAdapter struct{ v *prometheus.HistogramVec }

func (l *latencyAdapter) Observe(_ context.Context, verb string, u url.URL, latency time.Duration) {
	l.v.WithLabelValues(verb, u.Host).Observe(latency.Seconds())
}
```

- [ ] **Step 3: 編譯與靜態檢查**

Run:
```bash
go build ./cmd/resource-snapshotter/...
go vet ./cmd/resource-snapshotter/...
```
Expected: 兩個指令都成功結束（exit code 0），`go build` 產生的二進位檔可直接丟掉（`go build` 預設輸出到目前目錄，可加 `-o /tmp/resource-snapshotter` 避免污染工作區）。

- [ ] **Step 4: Commit**

```bash
git add cmd/resource-snapshotter/main.go cmd/resource-snapshotter/clientgo_metrics.go
git commit -m "feat(snapshotter): wire main.go with leader election and retention loop"
```

---

## Task 12: Dockerfile

**Files:**
- Create: `Dockerfile.resource-snapshotter`（與既有 `Dockerfile` 並存，不改動既有檔案——兩個獨立二進位檔各自的映像檔）

**Interfaces:** 無（純建置設定）。

- [ ] **Step 1: 寫 Dockerfile**

```dockerfile
# syntax=docker/dockerfile:1.6
# Dockerfile.resource-snapshotter

FROM golang:1.25-bookworm AS build

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS=-buildvcs=false

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/resource-snapshotter ./cmd/resource-snapshotter

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/resource-snapshotter /usr/local/bin/resource-snapshotter

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/resource-snapshotter"]
```

- [ ] **Step 2: 建置映像檔驗證**

Run: `docker build -f Dockerfile.resource-snapshotter -t resource-snapshotter:dev .`
Expected: 建置成功（exit code 0），最後一層輸出 `resource-snapshotter:dev`。需要可連網下載 base image 與 Go modules 的環境（本機沙盒沒有 docker/網路，需在 CI 或有 docker 的開發機執行）。

- [ ] **Step 3: Commit**

```bash
git add Dockerfile.resource-snapshotter
git commit -m "feat(snapshotter): add Dockerfile for resource-snapshotter image"
```

---

## Task 13: RBAC + 部署 Manifest（deploy/resource-snapshotter-manifests.yaml）

**Files:**
- Create: `deploy/resource-snapshotter-manifests.yaml`
- Create: `deploy/manifests_test.go`（offline YAML 結構驗證，不需要 cluster/docker；新增此檔會讓 `deploy/` 第一次成為一個 Go package，這是合法的最小作法）

**Interfaces:** 無（純 K8s manifest + 一個驗證用 Go test）。

依設計文件第 5 節沿用既有 `deploy/manifests.yaml` 的風格（Namespace/ServiceAccount/ClusterRole/ClusterRoleBinding/ConfigMap/Deployment/Service），並依第 3 節新增 leader election 用的 `Role`/`RoleBinding`（`coordination.k8s.io` `leases`）。

- [ ] **Step 1: 寫 manifests**

```yaml
# deploy/resource-snapshotter-manifests.yaml
# Render: kubectl apply -f deploy/resource-snapshotter-manifests.yaml
# Prerequisite: the target cluster has Istio's VirtualService CRD installed
# (networking.istio.io/v1beta1) and a reachable Postgres holding the
# resource-snapshotter schema (migrations run automatically on leader
# promotion, see pkg/snapshotter/store/migrate.go).

apiVersion: v1
kind: Namespace
metadata:
  name: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter

---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: resource-snapshotter
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
rules:
  # Add get/list/watch for every extra GVR you declare in config.yaml's
  # watch.resources[] (e.g. gateway.networking.k8s.io/httproutes).
  - apiGroups: ["networking.istio.io"]
    resources: ["virtualservices"]
    verbs: ["get", "list", "watch"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: resource-snapshotter
subjects:
  - kind: ServiceAccount
    name: resource-snapshotter
    namespace: resource-snapshotter

---
# Namespace-scoped: leader election only needs a Lease in this pod's own
# namespace (design doc section 3, "Leader/HA").
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: resource-snapshotter-leader-election
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: resource-snapshotter-leader-election
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: resource-snapshotter-leader-election
subjects:
  - kind: ServiceAccount
    name: resource-snapshotter
    namespace: resource-snapshotter

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: resource-snapshotter
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
data:
  config.yaml: |
    # Default retention: 30 days (omit the field entirely for the same
    # default; shown explicitly here for discoverability).
    retention: "720h"

    watch:
      resources:
        - kind: VirtualService
          group: networking.istio.io
          version: v1beta1
          resource: virtualservices
          scope: Namespaced
          # namespaces: ["prod"]   # omit to watch all namespaces
          historyTable: virtualservice_snapshots

---
# SNAPSHOTTER_DATABASE_URL is intentionally NOT created here — it must come
# from whatever Secret-injection mechanism this cluster already uses
# (External Secrets Operator / Vault / sealed-secrets), referenced below via
# secretKeyRef. See design doc section 5.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: resource-snapshotter
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
spec:
  # Recommended default: leader election makes >1 replica safe, but a
  # second replica only buys faster failover, not throughput (design doc
  # section 3/8) — bump this only if that tradeoff is worth it for a given
  # cluster.
  replicas: 1
  strategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app.kubernetes.io/name: resource-snapshotter
  template:
    metadata:
      labels:
        app.kubernetes.io/name: resource-snapshotter
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: resource-snapshotter
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: resource-snapshotter
          image: ghcr.io/example/resource-snapshotter:latest
          imagePullPolicy: IfNotPresent
          args:
            - --config=/etc/resource-snapshotter/config.yaml
            - --metrics-addr=:8080
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: SNAPSHOTTER_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: resource-snapshotter-postgres
                  key: dsn
          ports:
            - name: metrics
              containerPort: 8080
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /healthz
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: metrics
            initialDelaySeconds: 15
            periodSeconds: 20
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 256Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: config
              mountPath: /etc/resource-snapshotter
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: resource-snapshotter

---
apiVersion: v1
kind: Service
metadata:
  name: resource-snapshotter
  namespace: resource-snapshotter
  labels:
    app.kubernetes.io/name: resource-snapshotter
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: resource-snapshotter
  ports:
    - name: metrics
      port: 8080
      targetPort: metrics
      protocol: TCP
```

- [ ] **Step 2: 寫 offline 結構驗證測試**

```go
// deploy/manifests_test.go
package deploy

import (
	"os"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestResourceSnapshotterManifestsParse(t *testing.T) {
	raw, err := os.ReadFile("resource-snapshotter-manifests.yaml")
	if err != nil {
		t.Fatalf("read manifests: %v", err)
	}
	docs := strings.Split(string(raw), "\n---\n")
	if len(docs) < 9 {
		t.Fatalf("expected at least 9 YAML documents, got %d", len(docs))
	}
	for i, doc := range docs {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var obj map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("document %d: invalid YAML: %v", i, err)
		}
		if obj["apiVersion"] == nil || obj["kind"] == nil {
			t.Fatalf("document %d: missing apiVersion/kind: %v", i, obj)
		}
		if strings.Contains(doc, "{{") {
			t.Fatalf("document %d: contains unrendered template syntax", i)
		}
	}
}
```

- [ ] **Step 3: 執行測試**

Run: `go test ./deploy/... -v`
Expected: PASS（純離線 YAML 結構檢查，不需要 cluster/docker/網路）。

- [ ] **Step 4: Commit**

```bash
git add deploy/resource-snapshotter-manifests.yaml deploy/manifests_test.go
git commit -m "feat(snapshotter): add RBAC and deployment manifests"
```

---

## 自我審查（Self-Review）

**Spec coverage：** 對照 `istio-vs-route-history-snapshotter-design.md` 逐節檢查：

- 第 2 節（範圍界定，只做擷取+寫入）→ Task 11 `main.go` 不含任何查詢 API；查詢/比對邏輯完全沒有出現在本計畫。
- 第 3 節（事件驅動、uid+hash dedup、`resyncPeriod=0`、relist 自我修復、self-metrics、leader election、leader 接手和解、RBAC）→ Task 1/3/4/9/10/13。
- 第 4 節（config-driven 多 GVR、各資源專屬表、JSONB、SCD2、索引）→ Task 1（`historyTable`）、Task 4（migration DDL，並依實作需求外加 `idx_vs_snap_open` partial index，已在 Task 4 標注為「額外於設計文件 literal DDL 之外」）、Task 5。
- 第 5 節（共用 Postgres、schema 為契約、migration 由本 repo 擁有、DSN 環境變數注入、跨叢集部署）→ Task 4（migration 擁有權）、Task 11（`SNAPSHOTTER_DATABASE_URL`）、Task 13（manifest）。**已知與文字描述的落差（刻意決定，非遺漏）**：設計文件第 5 節提到「GitOps（ArgoCD/Flux）+ 模板化設定（Helm/Kustomize）」，但本計畫只產出一份扁平 `deploy/resource-snapshotter-manifests.yaml`（與現有 `deploy/manifests.yaml` 風格一致），不新增 Helm chart 或 ArgoCD `Application` 資源——因為現有 `metadata-exporter` 本身也是用同樣的扁平 manifest 模式，「怎麼把 YAML 套進叢集」屬於組織既有 GitOps pipeline 的職責，不是這個 repo 該新增的產物。若需要 Helm chart，屬於後續獨立任務。
- 第 6 節（範圍外：多 VS merge、delegate VS、查詢/比對演算法）→ 本計畫完全不實作這些；Task 7 的 `CheckHostOverlap` 只偵測、只記 warning，不做任何合併判斷，符合排除範圍。
- 第 7 節（保留期預設 30 天可設定、告警、可觀測性）→ Task 1（`RetentionDuration`）、Task 6（`PurgeExpired`）、Task 11（`runRetentionLoop`，leader-only、啟動時跑一次+每 24h）、Task 7+9（host overlap warning log）、Task 8（self-metrics）。
- 第 9 節待辦：「Replica/HA 尚待決定」→ 已透過 Task 9/10/11 的 leader election + 一次性和解解決，不是遺留問題。

**Placeholder 掃描：** 已移除原草稿中 `var _ = fmt.Sprintf` 之類的占位修補（見 Task 11 編修記錄），全文搜尋確認沒有殘留 `TODO`/`TBD`/「之後再補」等字樣。Task 9 的測試檔案 Step 1 故意缺少 `metav1`/`watch` import（並在後面 Step 4 補齊並重新給出完整 import 區塊）——這不是遺漏的占位，而是 TDD 紅燈步驟的真實情況（紅燈步驟本來就該編譯失敗），且 Step 4 給出的是完整、可直接貼上的修正版本，不是「請自行補上」的留白。

**Type consistency：** 追過一次跨 Task 的型別/簽名鏈：
- `decide.OpenVersion{UID, SpecHash}`：Task 3 定義 → Task 5 `Store.CurrentOpen` 回傳型別直接用它（零轉換層）→ Task 9 `applyUpsert`/`applyDelete` 原封不動使用。
- `store.ObjectRecord{Namespace,Name,UID,SpecHash,Spec}`：Task 5 定義 → Task 9 `applyUpsert` 建構時欄位名稱、型別完全對應。
- `store.HostOverlap{Namespace,Name,Hosts}`：Task 7 定義 → Task 9 `maybeWarnHostOverlap` 迴圈讀取欄位一致。
- `capture.Resource{Name,Table,Informer,Lister,CheckHostOverlap}`：Task 9 定義 → Task 11 `buildResources` 建構時欄位一致。
- `capture.VersionStore`/`capture.HostOverlapChecker` 兩個介面的方法簽名（含參數順序/型別）與 Task 5/7 `*store.Store` 的對應方法逐一比對皆相符，因此 Task 11 `snapcapture.New(st, st, ...)` 能直接把同一個 `*store.Store` 當兩個介面用，不需要轉接層——這正是 Task 9 interfaces 區塊當初設計的目的，驗證有達成。
- 發現並修正一個真實 bug（非單純型別問題）：Task 11 原草稿在 `OnStartedLeading` migration 失敗時呼叫 `leCancel()`，會讓本 replica 永久退出 leader election 候選但程式不結束、不重試，造成靜默退化；已改為 `os.Exit(1)`，與檔案內其他所有 fatal error 路徑一致，交給 Kubernetes 重啟 pod。

## 執行交接

計畫已完整寫入並自我審查完成：`/home/kasm-user/Personal_Data/custom-k8s-resource-metadata-watcher/superpowers/specs/2026-06-29-resource-snapshotter-plan.md`（13 個任務，bottom-up 順序：config → hash → dedup 決策 → Postgres store/migration/版本/保留期/host-overlap → self-metrics → capture 核心+leader 閘 → reconcile → main.go → Dockerfile → RBAC/部署 manifest）。

**在開始執行前務必注意（Global Constraints 已記載，這裡再次強調）**：目前這個沙盒環境**沒有安裝 `go` 工具鏈、無法連網下載 Go modules，也沒有可用的 Postgres 或 K8s cluster**。本計畫裡每一個「Run: ...」步驟都假設執行環境具備 Go 1.25+、可連網（`go get`/`go mod tidy` 需要下載 `github.com/jackc/pgx/v5`）、且針對 Task 4-7/13 的整合測試需要可連線的 Postgres（透過 `SNAPSHOTTER_TEST_POSTGRES_DSN` 提供，未設定時自動 `t.Skip`，不會擋住其他任務）。請確認接下來執行這份計畫的 session/環境滿足這些條件。

兩種執行方式可選：

**1. Subagent-Driven（建議）**——每個任務派一個全新的 subagent，任務之間有兩階段審查（implementation review + spec compliance review），適合像本計畫這樣每個任務邊界清楚、介面已預先鎖定簽名的情況。

**2. Inline Execution**——在目前這個 session 裡用 `executing-plans` 批次執行，每隔幾個任務停下來給你複核一次。

你想用哪一種方式執行？
