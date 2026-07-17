package config

import (
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func boolPtr(b bool) *bool { return &b }

// baseHistoryConfig returns a valid history-only config (no rules) with one
// history resource so tests can mutate a single field to assert a specific
// validation failure.
func baseHistoryConfig() *Config {
	return &Config{
		Watch: watchResources("Service"),
		History: &History{
			Store: StoreConfig{Type: "clickhouse", DSN: "clickhouse://localhost:9000/routing"},
			Resources: []HistoryResource{{
				Kind: "Service",
				Columns: []HistoryColumn{
					{Extract: Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
					{Extract: Extract{Path: "spec.selector"}, Name: "selector_kv", Type: "Array(String)", Encode: "kv", Index: "bloom_filter"},
				},
				Filters: []HistoryFilter{
					{Extract: Extract{Path: "metadata.namespace"}, Op: "regex", Value: "^prod-"},
				},
			}},
		},
	}
}

// svcRule is a minimal metric rule for tests that need the rules path present.
func svcRule() Rule {
	return Rule{
		Name:   "svc_info",
		Anchor: "Service",
		Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
	}
}

// TestHistory_OnlyConfigValidates pins the history-only mode: a config with a
// history block and no rules at all is valid.
func TestHistory_OnlyConfigValidates(t *testing.T) {
	c := baseHistoryConfig()
	if len(c.Rules) != 0 {
		t.Fatalf("baseHistoryConfig should be history-only, got %d rules", len(c.Rules))
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

// TestHistory_WithRulesValidates covers both output paths enabled at once.
func TestHistory_WithRulesValidates(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Enabled = boolPtr(true)
	c.Rules = []Rule{svcRule()}
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

// TestHistory_BlockWithoutEnabledIsEnabled pins presence-implies-enabled:
// declaring a history block turns it on, and its contents are validated rather
// than silently ignored.
func TestHistory_BlockWithoutEnabledIsEnabled(t *testing.T) {
	c := baseHistoryConfig()
	if c.History.Enabled != nil {
		t.Fatalf("baseHistoryConfig should omit enabled, got %v", *c.History.Enabled)
	}
	if !c.HistoryEnabled() {
		t.Fatal("a history block without enabled should be enabled")
	}
	c.History.Store.Type = "" // invalid; must be caught, not skipped
	if err := c.Validate(); err == nil {
		t.Fatal("a history block without enabled must still be validated")
	}
}

func TestHistory_NoBlockIsDisabled(t *testing.T) {
	c := &Config{Watch: watchResources("Service"), Rules: []Rule{svcRule()}}
	if c.HistoryEnabled() {
		t.Fatal("a config with no history block should be disabled")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("rules-only config should validate: %v", err)
	}
}

func TestHistory_DisabledSkipsValidation(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Enabled = boolPtr(false)
	c.History.Store.Type = "" // would be invalid if validated
	// enabled: false is the explicit opt-out, so rules become the only output
	// path and must be present for the config to do anything at all.
	c.Rules = []Rule{svcRule()}
	if err := c.Validate(); err != nil {
		t.Fatalf("disabled history should not be validated: %v", err)
	}
}

func TestHistory_DefaultTableName(t *testing.T) {
	r := HistoryResource{Kind: "VirtualService"}
	if got := r.TableName(); got != "virtualservice_versions" {
		t.Fatalf("TableName = %q, want virtualservice_versions", got)
	}
	r.Table = "custom"
	if got := r.TableName(); got != "custom" {
		t.Fatalf("TableName = %q, want custom", got)
	}
}

func TestHistory_CreateSchemaDefault(t *testing.T) {
	if (StoreConfig{}).CreateSchemaEnabled() {
		t.Fatal("createSchema must default to false (prod-safe)")
	}
	if !(StoreConfig{CreateSchema: boolPtr(true)}).CreateSchemaEnabled() {
		t.Fatal("createSchema=true not honoured")
	}
}

func TestStoreConfig_ResolveCredentials(t *testing.T) {
	// Inline only.
	if v, err := (StoreConfig{Username: "u"}).ResolveUsername(); err != nil || v != "u" {
		t.Fatalf("inline username = (%q,%v), want u", v, err)
	}
	if v, err := (StoreConfig{Password: "p"}).ResolvePassword(); err != nil || v != "p" {
		t.Fatalf("inline password = (%q,%v), want p", v, err)
	}
	// Env takes precedence over inline.
	t.Setenv("CH_USER", "envuser")
	t.Setenv("CH_PASS", "envpass")
	t.Setenv("CH_TOK", "envtok")
	if v, err := (StoreConfig{Username: "u", UsernameEnv: "CH_USER"}).ResolveUsername(); err != nil || v != "envuser" {
		t.Fatalf("env username = (%q,%v), want envuser", v, err)
	}
	if v, err := (StoreConfig{Password: "p", PasswordEnv: "CH_PASS"}).ResolvePassword(); err != nil || v != "envpass" {
		t.Fatalf("env password = (%q,%v), want envpass", v, err)
	}
	if v, err := (StoreConfig{TokenEnv: "CH_TOK"}).ResolveToken(); err != nil || v != "envtok" {
		t.Fatalf("env token = (%q,%v), want envtok", v, err)
	}
	// Env var set but empty/missing -> error (misconfiguration).
	if _, err := (StoreConfig{PasswordEnv: "CH_MISSING"}).ResolvePassword(); err == nil {
		t.Fatal("expected error for empty PasswordEnv target")
	}
}

func TestStoreConfig_SecureEnabled(t *testing.T) {
	if (StoreConfig{}).SecureEnabled() {
		t.Fatal("secure must default to false")
	}
	if !(StoreConfig{Secure: boolPtr(true)}).SecureEnabled() {
		t.Fatal("secure=true not honoured")
	}
}

func TestHistory_TokenBasicMutuallyExclusive(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Store.Token = "jwt"
	c.History.Store.Username = "u"
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutual-exclusion error, got %v", err)
	}

	// Token alone is fine.
	c = baseHistoryConfig()
	c.History.Store.TokenEnv = "CH_TOK"
	if err := c.Validate(); err != nil {
		t.Fatalf("token-only config should validate: %v", err)
	}
}

func TestHistory_ValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantSub string
	}{
		{"bad store type", func(c *Config) { c.History.Store.Type = "postgres" }, "store.type"},
		{"missing dsn", func(c *Config) { c.History.Store.DSN = "" }, "store.dsn"},
		{"no resources", func(c *Config) { c.History.Resources = nil }, "at least one resource"},
		{"kind not watched", func(c *Config) { c.History.Resources[0].Kind = "Node" }, "not included in watch.resources"},
		{"no columns", func(c *Config) { c.History.Resources[0].Columns = nil }, "at least one column"},
		{"bad column type", func(c *Config) { c.History.Resources[0].Columns[0].Type = "Widget" }, "unsupported"},
		{"reserved column name", func(c *Config) { c.History.Resources[0].Columns[0].Name = "valid_from" }, "reserved"},
		{"reserved column valid_to", func(c *Config) { c.History.Resources[0].Columns[0].Name = "valid_to" }, "reserved"},
		{"dup column name", func(c *Config) { c.History.Resources[0].Columns[1].Name = "cluster_ip" }, "duplicated"},
		{"encode json wrong type", func(c *Config) {
			c.History.Resources[0].Columns[0].Encode = "json"
			c.History.Resources[0].Columns[0].Type = "Array(String)"
		}, "encode=json requires type String"},
		{"encode kv wrong type", func(c *Config) {
			c.History.Resources[0].Columns[1].Encode = "kv"
			c.History.Resources[0].Columns[1].Type = "String"
		}, "encode=kv requires type Array(String)"},
		{"bad index", func(c *Config) { c.History.Resources[0].Columns[0].Index = "minmax" }, "index"},
		{"bad filter op", func(c *Config) { c.History.Resources[0].Filters[0].Op = "glob" }, "op \"glob\""},
		{"regex no value", func(c *Config) { c.History.Resources[0].Filters[0].Value = "" }, "op=regex requires value"},
		{"invalid regex", func(c *Config) { c.History.Resources[0].Filters[0].Value = "([" }, "invalid pattern"},
		{"in no values", func(c *Config) {
			c.History.Resources[0].Filters[0].Op = "in"
			c.History.Resources[0].Filters[0].Value = ""
		}, "op=in requires values"},
		{"value with path", func(c *Config) {
			c.History.Resources[0].Columns[0].Value = "fixed"
		}, "mutually exclusive"},
		{"value and valueEnv", func(c *Config) {
			c.History.Resources[0].Columns[0] = HistoryColumn{
				Name: "src", Type: "String", Value: "a", ValueEnv: "B",
			}
		}, "mutually exclusive"},
		{"value with encode", func(c *Config) {
			c.History.Resources[0].Columns[0] = HistoryColumn{
				Name: "src", Type: "String", Value: "a", Encode: "json",
			}
		}, "encode is incompatible"},
		{"value Array(String)", func(c *Config) {
			c.History.Resources[0].Columns[0] = HistoryColumn{
				Name: "src", Type: "Array(String)", Value: "a",
			}
		}, "do not support type Array(String)"},
		{"encode with onMissing", func(c *Config) {
			om := "x"
			c.History.Resources[0].Columns[1].OnMissing = &om
		}, "encode is incompatible with onMissing"},
		{"constants reserved", func(c *Config) {
			c.History.Constants = []HistoryConstant{{Name: "uid", Type: "String", Value: "x"}}
		}, "reserved"},
		{"constants duplicated", func(c *Config) {
			c.History.Constants = []HistoryConstant{
				{Name: "cluster_name", Type: "String", Value: "a"},
				{Name: "cluster_name", Type: "String", Value: "b"},
			}
		}, "duplicated"},
		{"constants conflict column", func(c *Config) {
			c.History.Constants = []HistoryConstant{{Name: "cluster_ip", Type: "String", Value: "prod"}}
		}, "conflicts with history.constants"},
		{"constants missing value", func(c *Config) {
			c.History.Constants = []HistoryConstant{{Name: "cluster_name", Type: "String"}}
		}, "value or valueEnv is required"},
		{"anyOf empty", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{AnyOf: []HistoryFilter{}}
		}, "at least one child"},
		{"anyOf with op", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{
				Op:    "equals",
				AnyOf: []HistoryFilter{{Extract: Extract{Path: "metadata.name"}, Op: "exists"}},
			}
		}, "mutually exclusive"},
		{"anyOf with path", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{
				Extract: Extract{Path: "metadata.name"},
				AnyOf:   []HistoryFilter{{Extract: Extract{Path: "metadata.name"}, Op: "exists"}},
			}
		}, "mutually exclusive"},
		{"anyOf with negate", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{
				Negate: true,
				AnyOf:  []HistoryFilter{{Extract: Extract{Path: "metadata.name"}, Op: "exists"}},
			}
		}, "mutually exclusive"},
		{"anyOf nested", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{
				AnyOf: []HistoryFilter{{
					AnyOf: []HistoryFilter{{Extract: Extract{Path: "metadata.name"}, Op: "exists"}},
				}},
			}
		}, "nested anyOf"},
		{"anyOf bad child op", func(c *Config) {
			c.History.Resources[0].Filters[0] = HistoryFilter{
				AnyOf: []HistoryFilter{{Extract: Extract{Path: "metadata.name"}, Op: "glob"}},
			}
		}, "op \"glob\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseHistoryConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// TestHistory_AnyOfValidates pins the positive path: a two-child anyOf group
// alongside a plain leaf filter is a valid config.
func TestHistory_AnyOfValidates(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Resources[0].Filters = append(c.History.Resources[0].Filters, HistoryFilter{
		AnyOf: []HistoryFilter{
			{Extract: Extract{Path: "metadata.namespace"}, Op: "prefix", Value: "prod-"},
			{Extract: Extract{Path: "metadata.labels.tier"}, Op: "equals", Value: "critical", Negate: true},
		},
	})
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

// TestHistory_AnyOfYAML guards the tag layout: anyOf children unmarshal with
// the embedded Extract fields promoted flat, same as top-level filters.
func TestHistory_AnyOfYAML(t *testing.T) {
	raw := []byte(`
filters:
  - path: spec.type
    op: in
    values: [LoadBalancer, NodePort]
  - anyOf:
      - path: metadata.namespace
        op: prefix
        value: prod-
      - path: metadata.labels.tier
        op: equals
        value: critical
`)
	var r HistoryResource
	if err := yaml.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(r.Filters) != 2 {
		t.Fatalf("filters = %d, want 2", len(r.Filters))
	}
	if r.Filters[0].AnyOf != nil || r.Filters[0].Op != "in" {
		t.Fatalf("leaf filter parsed wrong: %+v", r.Filters[0])
	}
	g := r.Filters[1]
	if len(g.AnyOf) != 2 || g.Op != "" || g.Path != "" {
		t.Fatalf("anyOf group parsed wrong: %+v", g)
	}
	if g.AnyOf[0].Path != "metadata.namespace" || g.AnyOf[0].Op != "prefix" || g.AnyOf[0].Value != "prod-" {
		t.Fatalf("anyOf child parsed wrong: %+v", g.AnyOf[0])
	}
}

func TestHistory_ConstantColumnAndTopLevel(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Constants = []HistoryConstant{
		{Name: "cluster_name", Type: "String", Value: "prod-a"},
	}
	c.History.Resources[0].Columns = append(c.History.Resources[0].Columns, HistoryColumn{
		Name: "source", Type: "String", Value: "metadata-exporter",
	})
	om := "unknown"
	c.History.Resources[0].Columns = append(c.History.Resources[0].Columns, HistoryColumn{
		Name: "team", Type: "String", Extract: Extract{Path: "metadata.labels.team", OnMissing: &om},
	})
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestHistory_ValueEnvColumn(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Resources[0].Columns = append(c.History.Resources[0].Columns, HistoryColumn{
		Name: "cluster_name", Type: "String", ValueEnv: "CLUSTER_NAME",
	})
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestReconnectConfig_Defaults(t *testing.T) {
	var rc ReconnectConfig
	if got := rc.InitialBackoffMsOrDefault(); got != DefaultReconnectInitialBackoffMs {
		t.Fatalf("initial backoff default = %d", got)
	}
	if got := rc.MaxBackoffMsOrDefault(); got != DefaultReconnectMaxBackoffMs {
		t.Fatalf("max backoff default = %d", got)
	}
	if got := rc.PingTimeoutMsOrDefault(); got != DefaultReconnectPingTimeoutMs {
		t.Fatalf("ping timeout default = %d", got)
	}
	rc = ReconnectConfig{InitialBackoffMs: 500, MaxBackoffMs: 10000, PingTimeoutMs: 2000}
	if rc.InitialBackoffMsOrDefault() != 500 || rc.MaxBackoffMsOrDefault() != 10000 || rc.PingTimeoutMsOrDefault() != 2000 {
		t.Fatalf("overrides not honoured: %+v", rc)
	}
}

func TestHistory_ReconnectValidation(t *testing.T) {
	// Negative values are rejected.
	c := baseHistoryConfig()
	c.History.Store.Reconnect = ReconnectConfig{InitialBackoffMs: -1}
	if err := c.Validate(); err == nil {
		t.Fatal("negative initialBackoffMs must fail validation")
	}
	// initial > max is rejected, including via defaults (initial beyond default max).
	c = baseHistoryConfig()
	c.History.Store.Reconnect = ReconnectConfig{InitialBackoffMs: 5000, MaxBackoffMs: 1000}
	if err := c.Validate(); err == nil {
		t.Fatal("initialBackoffMs > maxBackoffMs must fail validation")
	}
	c = baseHistoryConfig()
	c.History.Store.Reconnect = ReconnectConfig{InitialBackoffMs: DefaultReconnectMaxBackoffMs + 1}
	if err := c.Validate(); err == nil {
		t.Fatal("initialBackoffMs above the default max must fail validation")
	}
	// Sane overrides pass.
	c = baseHistoryConfig()
	c.History.Store.Reconnect = ReconnectConfig{InitialBackoffMs: 200, MaxBackoffMs: 5000, PingTimeoutMs: 1000}
	if err := c.Validate(); err != nil {
		t.Fatalf("valid reconnect config rejected: %v", err)
	}
}
