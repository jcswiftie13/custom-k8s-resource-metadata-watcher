package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const historyBlockYAML = `
  store:
    type: clickhouse
    dsn: clickhouse://localhost:9000/routing
  resources:
    - kind: Service
      columns:
        - name: cluster_ip
          type: String
          path: spec.clusterIP
`

const watchYAML = `
watch:
  resources:
    - kind: Service
      scope: Namespaced
`

const rulesYAML = `
rules:
  - name: svc_info
    anchor: Service
    labels:
      namespace:
        path: metadata.namespace
`

// TestLoad_HistoryEnabledFromYAML pins the presence-implies-enabled semantics
// through a real unmarshal: the *bool / *History defaults only take effect on
// the wire, not in hand-built structs.
func TestLoad_HistoryEnabledFromYAML(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantEnabled bool
	}{
		{
			name:        "history block without enabled is enabled",
			body:        watchYAML + "history:" + historyBlockYAML,
			wantEnabled: true,
		},
		{
			name:        "history block with enabled: true is enabled",
			body:        watchYAML + "history:\n  enabled: true" + historyBlockYAML,
			wantEnabled: true,
		},
		{
			// enabled: false is the explicit opt-out, so rules must carry the
			// config on their own.
			name:        "history block with enabled: false is disabled",
			body:        watchYAML + rulesYAML + "history:\n  enabled: false" + historyBlockYAML,
			wantEnabled: false,
		},
		{
			name:        "no history block is disabled",
			body:        watchYAML + rulesYAML,
			wantEnabled: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, tc.body))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := cfg.HistoryEnabled(); got != tc.wantEnabled {
				t.Fatalf("HistoryEnabled() = %v, want %v", got, tc.wantEnabled)
			}
		})
	}
}

// TestLoad_HistoryOnly is the end-to-end shape this change exists to allow: a
// config file with a history block and no rules at all.
func TestLoad_HistoryOnly(t *testing.T) {
	cfg, err := Load(writeConfig(t, watchYAML+"history:"+historyBlockYAML))
	if err != nil {
		t.Fatalf("history-only config should load: %v", err)
	}
	if len(cfg.Rules) != 0 {
		t.Fatalf("Rules = %d, want 0", len(cfg.Rules))
	}
	if !cfg.HistoryEnabled() {
		t.Fatal("history should be enabled")
	}
}

func TestLoad_RejectsConfigWithNoOutputPath(t *testing.T) {
	_, err := Load(writeConfig(t, watchYAML))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "would do nothing") {
		t.Fatalf("error should explain the config does nothing, got: %v", err)
	}
}
