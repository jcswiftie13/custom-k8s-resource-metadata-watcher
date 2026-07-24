package store

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestBuildClickHouseOptions_BasicOverridesDSN(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN:      "clickhouse://dsnuser:dsnpass@host:9000/dsndb",
		Username: "cfguser",
		Password: "cfgpass",
		Database: "cfgdb",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Auth.Username != "cfguser" || got.Auth.Password != "cfgpass" || got.Auth.Database != "cfgdb" {
		t.Fatalf("overrides not applied: %+v", got.Auth)
	}
	if got.GetJWT != nil {
		t.Fatal("GetJWT should be nil for basic auth")
	}
}

func TestBuildClickHouseOptions_PreservesDSNWhenNoOverride(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN: "clickhouse://dsnuser:dsnpass@host:9000/dsndb",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Auth.Username != "dsnuser" || got.Auth.Password != "dsnpass" || got.Auth.Database != "dsndb" {
		t.Fatalf("DSN credentials not preserved: %+v", got.Auth)
	}
}

func TestBuildClickHouseOptions_Token(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN: "clickhouse://host:9000/db",
		// Username/Password must be ignored when a token is present.
		Username: "ignored",
		Password: "ignored",
		Token:    "jwt-abc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetJWT == nil {
		t.Fatal("GetJWT should be set when Token is provided")
	}
	tok, err := got.GetJWT(context.Background())
	if err != nil || tok != "jwt-abc" {
		t.Fatalf("GetJWT = (%q, %v), want jwt-abc", tok, err)
	}
	if got.Auth.Username == "ignored" || got.Auth.Password == "ignored" {
		t.Fatalf("basic auth must not be applied alongside a token: %+v", got.Auth)
	}
}

func TestBuildClickHouseOptions_Secure(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN:           "clickhouse://host:9000/db",
		Secure:        true,
		TLSSkipVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.TLS == nil {
		t.Fatal("TLS should be enabled when Secure is true")
	}
	if !got.TLS.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify should propagate from TLSSkipVerify")
	}
}

func TestBuildClickHouseOptions_NoTLSByDefault(t *testing.T) {
	got, err := buildClickHouseOptions(Options{DSN: "clickhouse://host:9000/db"})
	if err != nil {
		t.Fatal(err)
	}
	if got.TLS != nil {
		t.Fatal("TLS should be nil when Secure is false")
	}
}

func TestBuildClickHouseOptions_HTTPDSN(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN: "http://clickhouse:8123/default",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != clickhouse.HTTP {
		t.Fatalf("Protocol = %v, want HTTP", got.Protocol)
	}
	if len(got.Addr) != 1 || got.Addr[0] != "clickhouse:8123" {
		t.Fatalf("Addr = %v, want [clickhouse:8123]", got.Addr)
	}
	if got.Auth.Database != "default" {
		t.Fatalf("Database = %q, want default", got.Auth.Database)
	}
	if got.TLS != nil {
		t.Fatal("plain http:// DSN must not enable TLS")
	}
	// HTTP writer path enables LZ4 block compression by default.
	if got.Compression == nil || got.Compression.Method != clickhouse.CompressionLZ4 {
		t.Fatalf("HTTP Compression = %+v, want LZ4", got.Compression)
	}
}

func TestBuildClickHouseOptions_HTTPDoesNotOverrideExplicitCompress(t *testing.T) {
	got, err := buildClickHouseOptions(Options{
		DSN: "http://clickhouse:8123/default?compress=gzip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Compression == nil || got.Compression.Method != clickhouse.CompressionGZIP {
		t.Fatalf("Compression = %+v, want gzip from DSN", got.Compression)
	}
}

func TestBuildClickHouseOptions_HTTPSWithSecureConfig(t *testing.T) {
	// https:// without ?secure=true fails ParseDSN unless we inject it from
	// config Secure — this is the ingress / TLS termination config shape.
	got, err := buildClickHouseOptions(Options{
		DSN:           "https://clickhouse.example.com/default",
		Secure:        true,
		TLSSkipVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != clickhouse.HTTP {
		t.Fatalf("Protocol = %v, want HTTP", got.Protocol)
	}
	if got.TLS == nil {
		t.Fatal("TLS should be set for https + secure")
	}
	if !got.TLS.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify should propagate from TLSSkipVerify")
	}
}

func TestBuildClickHouseOptions_HTTPSWithoutSecureFails(t *testing.T) {
	if _, err := buildClickHouseOptions(Options{
		DSN: "https://clickhouse.example.com/default",
	}); err == nil {
		t.Fatal("expected error for https:// DSN without secure")
	}
}

func TestBuildClickHouseOptions_NativeNoDefaultCompression(t *testing.T) {
	got, err := buildClickHouseOptions(Options{DSN: "clickhouse://host:9000/db"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != clickhouse.Native {
		t.Fatalf("Protocol = %v, want Native", got.Protocol)
	}
	if got.Compression != nil {
		t.Fatalf("native path must not auto-enable compression (got %+v)", got.Compression)
	}
}

func TestBuildClickHouseOptions_BadDSN(t *testing.T) {
	if _, err := buildClickHouseOptions(Options{DSN: "://not a dsn"}); err == nil {
		t.Fatal("expected error for malformed DSN")
	}
}
