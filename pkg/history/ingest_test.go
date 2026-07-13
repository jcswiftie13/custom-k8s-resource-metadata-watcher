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

type fakeClose struct {
	table, namespace, name, uid string
	validFrom, closeAt          time.Time
}

type fakeStore struct {
	tables map[string][]store.Row
	closes []fakeClose
	open   map[string][]store.OpenVersion // OpenVersions fixture for Recover tests
	ops    []string                       // "batch:<table>" / "close:<table>" in execution order
}

func (f *fakeStore) EnsureSchema(_ context.Context, _ []store.TableSchema) error { return nil }
func (f *fakeStore) WriteBatch(_ context.Context, table string, rows []store.Row) error {
	if f.tables == nil {
		f.tables = map[string][]store.Row{}
	}
	f.tables[table] = append(f.tables[table], rows...)
	f.ops = append(f.ops, "batch:"+table)
	return nil
}
func (f *fakeStore) CloseVersion(_ context.Context, table, namespace, name, uid string, validFrom, closeAt time.Time) error {
	f.closes = append(f.closes, fakeClose{table, namespace, name, uid, validFrom, closeAt})
	f.ops = append(f.ops, "close:"+table)
	return nil
}
func (f *fakeStore) OpenVersions(_ context.Context, table string) ([]store.OpenVersion, error) {
	return f.open[table], nil
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
	return newTestIngesterMode(t, config.CloseModeRewrite, &fakeStore{})
}

func newTestIngesterMode(t *testing.T, closeMode string, fs *fakeStore) (*Ingester, *CompiledResource) {
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
	in := NewIngester(fs, nil, []*CompiledResource{cr}, config.BatchConfig{}, closeMode, slog.Default())
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

// A close whose closeAt predates the row it closes (clock skew, replayed
// events) is clamped: the interval degenerates to zero width rather than
// inverting. The clamp lives in enqueueClose so both close modes share it.
func TestIngest_ClosingRowClampsInvertedInterval(t *testing.T) {
	in, _ := newTestIngester(t)
	prev := store.Row{ValidFrom: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), ValidTo: store.FarFuture}

	in.enqueueClose("svc_versions", prev, prev.ValidFrom.Add(-time.Hour))
	q := drain(in)
	if len(q) != 1 || q[0].close != nil {
		t.Fatalf("rewrite mode should enqueue 1 closing row, got %+v", q)
	}
	if !q[0].row.ValidTo.Equal(prev.ValidFrom) {
		t.Fatalf("valid_to %v should be clamped to valid_from %v", q[0].row.ValidTo, prev.ValidFrom)
	}

	inU, _ := newTestIngesterMode(t, config.CloseModeUpdate, &fakeStore{})
	inU.enqueueClose("svc_versions", prev, prev.ValidFrom.Add(-time.Hour))
	q = drain(inU)
	if len(q) != 1 || q[0].close == nil {
		t.Fatalf("update mode should enqueue 1 close op, got %+v", q)
	}
	if !q[0].close.closeAt.Equal(prev.ValidFrom) {
		t.Fatalf("closeAt %v should be clamped to valid_from %v", q[0].close.closeAt, prev.ValidFrom)
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

// closeMode=update: a superseded version yields a close OP (not a closing row)
// pinning the prior version's exact valid_from, then the new open row; a
// delete yields only the close op. No row is ever written twice.
func TestIngest_UpdateCloseOps(t *testing.T) {
	in, cr := newTestIngesterMode(t, config.CloseModeUpdate, &fakeStore{})

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.1", "1"), eventAdd)
	q := drain(in)
	if len(q) != 1 || q[0].close != nil || q[0].row.ValidTo != store.FarFuture {
		t.Fatalf("Add: expected 1 open insert, got %+v", q)
	}
	v1 := q[0].row

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventUpdate)
	q = drain(in)
	if len(q) != 2 {
		t.Fatalf("update: expected close op + insert, got %d", len(q))
	}
	if q[0].close == nil {
		t.Fatalf("update: first item should be a close op, got row %+v", q[0].row)
	}
	v2 := q[1].row
	c := q[0].close
	if c.uid != "uid-1" || !c.validFrom.Equal(v1.ValidFrom) {
		t.Fatalf("close op must pin v1's slot: %+v (v1.ValidFrom=%v)", c, v1.ValidFrom)
	}
	if !c.closeAt.Equal(v2.ValidFrom) {
		t.Fatalf("v1 must close where v2 begins: closeAt=%v v2.ValidFrom=%v", c.closeAt, v2.ValidFrom)
	}
	if q[1].close != nil || v2.ValidTo != store.FarFuture {
		t.Fatalf("update: second item should be the open v2 insert, got %+v", q[1])
	}

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventDelete)
	q = drain(in)
	if len(q) != 1 || q[0].close == nil {
		t.Fatalf("delete: expected only a close op, got %+v", q)
	}
	if !q[0].close.validFrom.Equal(v2.ValidFrom) {
		t.Fatalf("delete must close v2's slot, got %+v", q[0].close)
	}
}

// The flush cycle must send INSERT batches before executing closes: a close
// enqueued after its target row's INSERT would otherwise race it and match
// zero rows, leaving the version open forever.
func TestIngest_UpdateClose_FlushOrder(t *testing.T) {
	fs := &fakeStore{}
	in, cr := newTestIngesterMode(t, config.CloseModeUpdate, fs)

	ctx, cancel := context.WithCancel(context.Background())
	in.ctxDone = ctx.Done()
	done := make(chan struct{})
	go func() { in.loop(ctx); close(done) }()

	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.1", "1"), eventAdd)
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.2", "2"), eventUpdate)
	for len(in.queue) > 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if got := len(fs.tables["svc_versions"]); got != 2 {
		t.Fatalf("expected v1+v2 inserts, got %d rows", got)
	}
	if len(fs.closes) != 1 {
		t.Fatalf("expected 1 close, got %d", len(fs.closes))
	}
	lastBatch, lastClose := -1, -1
	for i, op := range fs.ops {
		switch op {
		case "batch:svc_versions":
			lastBatch = i
		case "close:svc_versions":
			lastClose = i
		}
	}
	if lastBatch == -1 || lastClose == -1 || lastClose < lastBatch {
		t.Fatalf("closes must run after the insert batch, ops=%v", fs.ops)
	}
}

// Recover rebuilds last-state from the store's open rows so a restart's
// re-LIST dedups instead of re-inserting, and sweeps stale open rows left by
// a crash between an insert flush and its close.
func TestIngest_RecoverPopulatesAndSweeps(t *testing.T) {
	current := svcObj("uid-1", "prod-web", "10.0.0.2", "2")
	hash, err := SpecHash(current.Object)
	if err != nil {
		t.Fatal(err)
	}
	vf1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	vf2 := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	fs := &fakeStore{open: map[string][]store.OpenVersion{
		"svc_versions": {
			{Namespace: "prod-web", Name: "api", UID: "uid-1", SpecHash: "h-old", ValidFrom: vf1, IngestSeq: 1},
			{Namespace: "prod-web", Name: "api", UID: "uid-1", SpecHash: hash, ValidFrom: vf2, IngestSeq: 3},
		},
	}}
	in, cr := newTestIngesterMode(t, config.CloseModeUpdate, fs)
	if err := in.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The stale open row (vf1) is swept: closed at the next version's start.
	if len(fs.closes) != 1 || !fs.closes[0].validFrom.Equal(vf1) || !fs.closes[0].closeAt.Equal(vf2) {
		t.Fatalf("expected stale open row swept [vf1 -> vf2], got %+v", fs.closes)
	}

	// Re-LIST of the unchanged object dedups against the recovered hash.
	in.onEvent(cr, current, eventAdd)
	if q := drain(in); len(q) != 0 {
		t.Fatalf("unchanged re-LIST must be a no-op, got %d items", len(q))
	}

	// A change observed as a re-LIST Add closes the recovered version and opens
	// a new one at OBSERVATION time — creationTimestamp (2026-01-01) would
	// overlap every prior version.
	in.onEvent(cr, svcObj("uid-1", "prod-web", "10.0.0.3", "3"), eventAdd)
	q := drain(in)
	if len(q) != 2 || q[0].close == nil || q[1].close != nil {
		t.Fatalf("changed re-LIST: expected close op + insert, got %+v", q)
	}
	if !q[0].close.validFrom.Equal(vf2) {
		t.Fatalf("close must target the recovered open version (vf2), got %+v", q[0].close)
	}
	if !q[1].row.ValidFrom.After(vf2) {
		t.Fatalf("new version must start at observation time, not creationTimestamp: %v", q[1].row.ValidFrom)
	}

	// Rewrite mode: Recover is a no-op (memory-only last-state).
	fsR := &fakeStore{open: fs.open}
	inR, _ := newTestIngesterMode(t, config.CloseModeRewrite, fsR)
	if err := inR.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fsR.closes) != 0 {
		t.Fatalf("rewrite-mode Recover must not touch the store, got %+v", fsR.closes)
	}
	if _, hadPrior, _ := inR.priorOpen("uid-1", hash); hadPrior {
		t.Fatal("rewrite-mode Recover must not populate last-state")
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

func TestIngest_ConstantsAndOnMissing(t *testing.T) {
	om := "unknown"
	rs, err := CompileAll(config.History{
		Constants: []config.HistoryConstant{
			{Name: "cluster_name", Type: "String", Value: "prod"},
		},
		Resources: []config.HistoryResource{{
			Kind:  "Service",
			Table: "svc_versions",
			Columns: []config.HistoryColumn{
				{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
				{Name: "team", Type: "String", Extract: config.Extract{Path: `metadata.labels["team"]`, OnMissing: &om}},
			},
			Filters: []config.HistoryFilter{
				{Extract: config.Extract{Path: "metadata.namespace"}, Op: "prefix", Value: "prod-"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("CompileAll: %v", err)
	}
	cr := rs[0]
	in := NewIngester(&fakeStore{}, nil, rs, config.BatchConfig{}, config.CloseModeRewrite, slog.Default())

	in.onEvent(cr, svcObj("uid-c", "prod-web", "10.0.0.1", "1"), eventAdd)
	q := drain(in)
	if len(q) != 1 {
		t.Fatalf("expected 1 row, got %d", len(q))
	}
	row := q[0].row
	if row.Values["cluster_name"] != "prod" {
		t.Fatalf("cluster_name = %v", row.Values["cluster_name"])
	}
	if row.Values["cluster_ip"] != "10.0.0.1" {
		t.Fatalf("cluster_ip = %v", row.Values["cluster_ip"])
	}
	if row.Values["team"] != "unknown" {
		t.Fatalf("team onMissing = %v", row.Values["team"])
	}

	// Spec change opens a new version; constants stay the same.
	in.onEvent(cr, svcObj("uid-c", "prod-web", "10.0.0.2", "2"), eventUpdate)
	q = drain(in)
	if len(q) != 2 { // close prior + new open
		t.Fatalf("expected close+open, got %d", len(q))
	}
	newOpen := q[1].row
	if newOpen.Values["cluster_name"] != "prod" || newOpen.Values["cluster_ip"] != "10.0.0.2" {
		t.Fatalf("updated row = %+v", newOpen.Values)
	}
}
