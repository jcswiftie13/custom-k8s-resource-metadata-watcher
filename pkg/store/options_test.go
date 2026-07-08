package store

import (
	"context"
	"testing"
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

func TestBuildClickHouseOptions_BadDSN(t *testing.T) {
	if _, err := buildClickHouseOptions(Options{DSN: "://not a dsn"}); err == nil {
		t.Fatal("expected error for malformed DSN")
	}
}
