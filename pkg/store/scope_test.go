package store

import (
	"context"
	"strings"
	"testing"
)

// Scoped recovery reads are what keep multi-writer shared tables safe: without
// the predicate, writer B's Recover adopts writer A's open rows and its orphan
// reconciliation closes them (A's objects are absent from B's informers).

func TestScopePredicate(t *testing.T) {
	s := &chStore{}
	if pred, args := s.scopePredicate(); pred != "" || args != nil {
		t.Fatalf("zero scope must not filter, got %q %v", pred, args)
	}

	s = &chStore{scope: Scope{Column: "cluster", Value: "cluster-a"}}
	pred, args := s.scopePredicate()
	if pred != " AND cluster = ?" {
		t.Fatalf("predicate = %q", pred)
	}
	if len(args) != 1 || args[0] != "cluster-a" {
		t.Fatalf("args = %v, want [cluster-a]", args)
	}
}

// EnsureSchema must refuse a table that lacks the scope column: recovery
// queries would fail (or worse, silently mis-filter) at runtime otherwise.
func TestEnsureSchema_ScopeColumnMissing(t *testing.T) {
	s := &chStore{scope: Scope{Column: "cluster", Value: "cluster-a"}}
	err := s.EnsureSchema(context.Background(), []TableSchema{{
		Table:   "svc_versions",
		Columns: []ColumnSchema{{Name: "cluster_ip", Type: "String"}},
	}})
	if err == nil || !strings.Contains(err.Error(), "scope column") {
		t.Fatalf("expected scope-column error, got %v", err)
	}
}
