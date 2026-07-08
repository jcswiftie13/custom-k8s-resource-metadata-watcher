package config

import (
	"strings"
	"testing"
)

func boolPtr(b bool) *bool { return &b }

// baseHistoryConfig returns a valid config with one history resource so tests
// can mutate a single field to assert a specific validation failure.
func baseHistoryConfig() *Config {
	return &Config{
		Watch: watchResources("Service"),
		Rules: []Rule{{
			Name:   "svc_info",
			Anchor: "Service",
			Labels: map[string]Extract{"namespace": {Path: "metadata.namespace"}},
		}},
		History: History{
			Enabled: true,
			Store:   StoreConfig{Type: "clickhouse", DSN: "clickhouse://localhost:9000/routing"},
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

func TestHistory_ValidConfig(t *testing.T) {
	if err := baseHistoryConfig().Validate(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestHistory_DisabledSkipsValidation(t *testing.T) {
	c := baseHistoryConfig()
	c.History.Enabled = false
	c.History.Store.Type = "" // would be invalid if validated
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
