package history

import (
	"context"
	"log/slog"
	"testing"
	"time"

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

func svcObj(uid, ns, clusterIP, resourceVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"namespace":         ns,
			"name":              "api",
			"uid":               uid,
			"resourceVersion":   resourceVersion,
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
	obj := svcObj("uid-1", "prod-web", "10.0.0.1", "1")

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
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.1", "1"), eventUpdate)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("dedup: expected 0 rows, got %d", len(q))
	}

	// Real change -> closing row for the prior version + the new open version.
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventUpdate)
	q = drain(in)
	if len(q) != 2 {
		t.Fatalf("update: expected 2 rows (close + new), got %d", len(q))
	}
	if q[0].row.ResourceVersion != "1" || q[0].row.ValidTo == store.FarFuture {
		t.Fatalf("update: first row should close v1, got %+v", q[0].row)
	}
	if q[1].row.Values["cluster_ip"] != "10.0.0.2" || q[1].row.ValidTo != store.FarFuture {
		t.Fatalf("update: second row should be the open v2, got %+v", q[1].row)
	}

	// Delete -> close the last version, no tombstone row.
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventDelete)
	q = drain(in)
	if len(q) != 1 {
		t.Fatalf("delete: expected 1 closing row, got %d", len(q))
	}
	if q[0].row.Deleted {
		t.Fatalf("delete: must not write a deleted=1 tombstone, got %+v", q[0].row)
	}
	if q[0].row.ValidTo == store.FarFuture || !q[0].row.ValidTo.After(q[0].row.ValidFrom) {
		t.Fatalf("delete: valid_to should close the version, got %+v", q[0].row)
	}

	// Delete again (already forgotten) -> nothing
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventDelete)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("second delete: expected 0 rows, got %d", len(q))
	}
}

// TestIngest_ValidToChain pins the contiguity invariant: each version's
// valid_to is the next version's valid_from, and the closing row shares the
// closed row's sort key while carrying a higher ingest_seq so
// ReplacingMergeTree(ingest_seq) keeps the close.
func TestIngest_ValidToChain(t *testing.T) {
	in, cr := newTestIngester(t)

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.1", "1"), eventAdd)
	q := drain(in)
	if len(q) != 1 || q[0].row.ValidTo != store.FarFuture {
		t.Fatalf("Add: expected 1 open row, got %+v", q)
	}
	v1Open := q[0].row

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventUpdate)
	q = drain(in)
	if len(q) != 2 {
		t.Fatalf("update: expected 2 rows, got %d", len(q))
	}
	v1Close, v2Open := q[0].row, q[1].row

	// The closing row is v1 with valid_to filled in: same sort key, higher seq.
	if v1Close.Namespace != v1Open.Namespace || v1Close.Name != v1Open.Name ||
		!v1Close.ValidFrom.Equal(v1Open.ValidFrom) ||
		v1Close.ResourceVersion != v1Open.ResourceVersion ||
		v1Close.Deleted != v1Open.Deleted {
		t.Fatalf("closing row must share v1's sort key:\nopen=%+v\nclose=%+v", v1Open, v1Close)
	}
	if v1Close.IngestSeq <= v1Open.IngestSeq {
		t.Fatalf("closing row ingest_seq %d must exceed open row's %d", v1Close.IngestSeq, v1Open.IngestSeq)
	}
	if !v1Close.ValidTo.Equal(v2Open.ValidFrom) {
		t.Fatalf("v1.valid_to %v != v2.valid_from %v", v1Close.ValidTo, v2Open.ValidFrom)
	}
	if v2Open.ValidTo != store.FarFuture {
		t.Fatalf("v2 should be open, got valid_to %v", v2Open.ValidTo)
	}

	// Delete closes v2; the chain is v1 -> v2 -> deletion, with no gaps.
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventDelete)
	q = drain(in)
	if len(q) != 1 {
		t.Fatalf("delete: expected 1 row, got %d", len(q))
	}
	v2Close := q[0].row
	if v2Close.ResourceVersion != v2Open.ResourceVersion || !v2Close.ValidFrom.Equal(v2Open.ValidFrom) {
		t.Fatalf("delete must close v2, got %+v", v2Close)
	}
	if v2Close.IngestSeq <= v2Open.IngestSeq {
		t.Fatalf("delete closing seq %d must exceed v2's %d", v2Close.IngestSeq, v2Open.IngestSeq)
	}
	if !v2Close.ValidTo.After(v1Close.ValidTo) {
		t.Fatalf("deletion time %v should follow v2's start %v", v2Close.ValidTo, v1Close.ValidTo)
	}
}

// A version observed by a later Add (e.g. a relist after the object changed)
// carries creationTimestamp as valid_from, which predates the row it closes.
// Clamping degenerates the interval to zero width rather than inverting it.
func TestIngest_ClosingRowClampsInvertedInterval(t *testing.T) {
	in, _ := newTestIngester(t)
	prev := store.Row{ValidFrom: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), ValidTo: store.FarFuture}

	got := in.closingRow(prev, prev.ValidFrom.Add(-time.Hour))
	if !got.ValidTo.Equal(prev.ValidFrom) {
		t.Fatalf("valid_to %v should be clamped to valid_from %v", got.ValidTo, prev.ValidFrom)
	}
}

func TestIngest_FilteredOutNotWritten(t *testing.T) {
	in, cr := newTestIngester(t)
	// namespace does not match prefix "prod-"
	obj := svcObj("uid-2", "staging-web", "10.0.0.3", "1")

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
	in.onEvent(cr, svcObj("uid-3", "prod-a", "10.0.0.4", "1"), eventAdd)
	in.onEvent(cr, svcObj("uid-4", "prod-b", "10.0.0.5", "1"), eventAdd)
	q := drain(in)
	if len(q) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(q))
	}
	if q[0].row.IngestSeq >= q[1].row.IngestSeq {
		t.Fatalf("ingest_seq not monotonic: %d then %d", q[0].row.IngestSeq, q[1].row.IngestSeq)
	}
}
