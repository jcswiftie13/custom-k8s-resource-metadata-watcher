package history

import (
	"context"
	"log/slog"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

type fakeStore struct {
	tables map[string][]store.Row
}

func (f *fakeStore) EnsureSchema(_ context.Context, _ []store.TableSchema) error { return nil }
func (f *fakeStore) WriteBatch(_ context.Context, table string, rows []store.Row) error {
	if f.tables == nil {
		f.tables = map[string][]store.Row{}
	}
	f.tables[table] = append(f.tables[table], rows...)
	return nil
}
func (f *fakeStore) Close() error { return nil }

func svcObj(uid, ns, clusterIP string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"namespace":         ns,
			"name":              "api",
			"uid":               uid,
			"resourceVersion":   "1",
			"creationTimestamp": "2026-01-01T00:00:00Z",
		},
		"spec": map[string]interface{}{"clusterIP": clusterIP},
	}}
}

func newTestIngester(t *testing.T) (*Ingester, *CompiledResource) {
	t.Helper()
	cr := mustCompile(t, config.HistoryResource{
		Kind:  "Service",
		Table: "svc_versions",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
		},
		Filters: []config.HistoryFilter{
			{Extract: config.Extract{Path: "metadata.namespace"}, Op: "prefix", Value: "prod-"},
		},
	})
	in := NewIngester(&fakeStore{}, nil, []*CompiledResource{cr}, config.BatchConfig{}, slog.Default())
	return in, cr
}

func drain(in *Ingester) []queued {
	var out []queued
	for {
		select {
		case q := <-in.queue:
			out = append(out, q)
		default:
			return out
		}
	}
}

func TestIngest_AddUpdateDedupDelete(t *testing.T) {
	in, cr := newTestIngester(t)
	obj := svcObj("uid-1", "prod-web", "10.0.0.1")

	// Add
	in.onEvent(cr, obj, eventAdd)
	q := drain(in)
	if len(q) != 1 {
		t.Fatalf("Add: expected 1 row, got %d", len(q))
	}
	if q[0].table != "svc_versions" || q[0].row.Deleted || q[0].row.Values["cluster_ip"] != "10.0.0.1" {
		t.Fatalf("Add row wrong: %+v", q[0].row)
	}
	// Add uses creationTimestamp for valid_from.
	if q[0].row.ValidFrom.Format("2006-01-02") != "2026-01-01" {
		t.Fatalf("Add valid_from not from creationTimestamp: %v", q[0].row.ValidFrom)
	}

	// Resync with identical content -> deduped
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.1"), eventUpdate)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("dedup: expected 0 rows, got %d", len(q))
	}

	// Real change -> new version
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2"), eventUpdate)
	q = drain(in)
	if len(q) != 1 || q[0].row.Values["cluster_ip"] != "10.0.0.2" {
		t.Fatalf("update: expected changed row, got %+v", q)
	}

	// Delete -> tombstone
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2"), eventDelete)
	q = drain(in)
	if len(q) != 1 || !q[0].row.Deleted {
		t.Fatalf("delete: expected tombstone, got %+v", q)
	}

	// Delete again (already forgotten) -> nothing
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2"), eventDelete)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("second delete: expected 0 rows, got %d", len(q))
	}
}

func TestIngest_FilteredOutNotWritten(t *testing.T) {
	in, cr := newTestIngester(t)
	// namespace does not match prefix "prod-"
	obj := svcObj("uid-2", "staging-web", "10.0.0.3")

	in.onEvent(cr, obj, eventAdd)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("filtered add should write nothing, got %d", len(q))
	}
	// Delete of a never-written object must also write nothing (no tombstone).
	in.onEvent(cr, obj, eventDelete)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("filtered delete should write nothing, got %d", len(q))
	}
}

func TestIngest_IngestSeqMonotonic(t *testing.T) {
	in, cr := newTestIngester(t)
	in.onEvent(cr, svcObj("uid-3", "prod-a", "10.0.0.4"), eventAdd)
	in.onEvent(cr, svcObj("uid-4", "prod-b", "10.0.0.5"), eventAdd)
	q := drain(in)
	if len(q) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(q))
	}
	if q[0].row.IngestSeq >= q[1].row.IngestSeq {
		t.Fatalf("ingest_seq not monotonic: %d then %d", q[0].row.IngestSeq, q[1].row.IngestSeq)
	}
}
