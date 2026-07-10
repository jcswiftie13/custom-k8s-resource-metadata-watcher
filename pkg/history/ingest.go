package history

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/store"
)

// InformerSource is the subset of collector.ScopedInformers the ingester needs.
type InformerSource interface {
	Informers(ref string) []cache.SharedIndexInformer
}

type eventKind int

const (
	eventAdd eventKind = iota
	eventUpdate
	eventDelete
)

type queued struct {
	table string
	row   store.Row
}

// Ingester registers informer event handlers and streams version rows to the
// store. It must be constructed and Register()ed before informers start so the
// initial LIST replay is captured.
type Ingester struct {
	store     store.Store
	informers InformerSource
	ev        *collector.Evaluator
	byKind    map[string]*CompiledResource
	log       *slog.Logger
	batchMax  int
	flushIvl  time.Duration

	seq atomic.Uint64

	mu   sync.Mutex
	last map[string]lastState // uid -> last written open version

	queue   chan queued
	ctxDone <-chan struct{}
}

// lastState is the last open version written for a live uid: its spec hash (for
// dedup) and the whole row, kept so a later version or a delete can re-insert it
// with valid_to filled in. One entry per live object, not per historical
// version. row.Values is built fresh by buildRow and never mutated afterwards,
// so holding the reference is safe.
type lastState struct {
	hash string
	row  store.Row
}

// NewIngester builds an Ingester from compiled resources.
func NewIngester(st store.Store, informers InformerSource, resources []*CompiledResource, batch config.BatchConfig, log *slog.Logger) *Ingester {
	byKind := make(map[string]*CompiledResource, len(resources))
	for _, r := range resources {
		byKind[r.Kind] = r
	}
	maxRows := batch.MaxRowsOrDefault()
	return &Ingester{
		store:     st,
		informers: informers,
		ev:        collector.NewEvaluator(),
		byKind:    byKind,
		log:       log,
		batchMax:  maxRows,
		flushIvl:  time.Duration(batch.FlushIntervalMsOrDefault()) * time.Millisecond,
		last:      map[string]lastState{},
		queue:     make(chan queued, maxRows*2),
	}
}

// Register attaches event handlers on every informer for each configured Kind.
// Call before the informers are started.
func (in *Ingester) Register() error {
	for kind, cr := range in.byKind {
		infs := in.informers.Informers(kind)
		if len(infs) == 0 {
			return fmt.Errorf("history: no informer for kind %q (is it in watch.resources?)", kind)
		}
		crLocal := cr
		for _, inf := range infs {
			_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(o interface{}) { in.onEvent(crLocal, o, eventAdd) },
				UpdateFunc: func(_, o interface{}) { in.onEvent(crLocal, o, eventUpdate) },
				DeleteFunc: func(o interface{}) { in.onEvent(crLocal, o, eventDelete) },
			})
			if err != nil {
				return fmt.Errorf("history: add handler for %q: %w", kind, err)
			}
		}
	}
	return nil
}

// Start launches the batch flush loop. It returns immediately; the loop runs
// until ctx is cancelled, flushing any buffered rows on exit.
func (in *Ingester) Start(ctx context.Context) {
	in.ctxDone = ctx.Done()
	go in.loop(ctx)
}

func (in *Ingester) loop(ctx context.Context) {
	ticker := time.NewTicker(in.flushIvl)
	defer ticker.Stop()
	pending := map[string][]store.Row{}
	count := 0
	flush := func() {
		for table, rows := range pending {
			if len(rows) == 0 {
				continue
			}
			if err := in.store.WriteBatch(ctx, table, rows); err != nil {
				in.log.Error("history write batch failed", "table", table, "rows", len(rows), "err", err)
			}
		}
		pending = map[string][]store.Row{}
		count = 0
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case q := <-in.queue:
			pending[q.table] = append(pending[q.table], q.row)
			count++
			if count >= in.batchMax {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (in *Ingester) onEvent(cr *CompiledResource, o interface{}, kind eventKind) {
	u := toUnstructured(o)
	if u == nil {
		return
	}
	objMap := u.Object
	lookup := func(src string) map[string]interface{} {
		switch src {
		case "", "anchor":
			return objMap
		default:
			return nil
		}
	}
	uid := string(u.GetUID())

	if kind == eventDelete {
		// Only close out objects we actually wrote (passed filters before). The
		// close alone records the deletion — valid_to now bounds the last
		// version, so no deleted=1 tombstone row is written.
		prior, ok := in.forgetIfSeen(uid)
		if !ok {
			return
		}
		in.enqueue(cr.Table, in.closingRow(prior, time.Now().UTC()))
		return
	}

	if !cr.Passes(in.ev, lookup) {
		return
	}

	hash, err := SpecHash(objMap)
	if err != nil {
		in.log.Error("history spec hash failed", "kind", cr.Kind, "uid", uid, "err", err)
		return
	}
	prior, hadPrior, same := in.priorOpen(uid, hash)
	if same {
		return // resync / no-op: unchanged content
	}

	row, err := in.buildRow(cr, u, lookup, kind)
	if err != nil {
		in.log.Error("history build row failed", "kind", cr.Kind, "uid", uid, "err", err)
		return
	}
	row.SpecHash = hash
	in.remember(uid, hash, row)

	// The superseded version ends where the new one begins. Both rows may land
	// in any batch, in any order: ingest_seq alone decides the merge.
	if hadPrior {
		in.enqueue(cr.Table, in.closingRow(prior, row.ValidFrom))
	}
	in.enqueue(cr.Table, row)
}

// buildRow assembles the open version row for an Add/Update. Deleted stays
// false: a deletion is recorded by closing valid_to, never by a tombstone row.
func (in *Ingester) buildRow(cr *CompiledResource, u *unstructured.Unstructured, lookup srcLookup, kind eventKind) (store.Row, error) {
	row := store.Row{
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		UID:             string(u.GetUID()),
		ResourceVersion: u.GetResourceVersion(),
		ValidFrom:       validFrom(u, kind),
		ValidTo:         store.FarFuture,
		IngestSeq:       in.seq.Add(1),
		Values:          make(map[string]any, len(cr.Columns)),
	}
	for i := range cr.Columns {
		c := &cr.Columns[i]
		v, err := in.columnValue(c, lookup)
		if err != nil {
			return store.Row{}, fmt.Errorf("column %q: %w", c.Name, err)
		}
		row.Values[c.Name] = v
	}
	return row, nil
}

// columnValue produces a Go value matching the column's ClickHouse type and
// encoding. Missing values yield typed zero values so the batch always has a
// complete, correctly-typed tuple.
func (in *Ingester) columnValue(c *CompiledColumn, lookup srcLookup) (any, error) {
	switch c.Encode {
	case "json":
		raw := in.ev.EvaluateExtractRaw(c.Extract, lookup)
		if len(raw) == 0 || raw[0] == nil {
			return "", nil
		}
		b, err := json.Marshal(raw[0])
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case "kv":
		return kvTokens(in.ev.EvaluateExtractRaw(c.Extract, lookup)), nil
	}

	switch c.Type {
	case "Array(String)":
		vals := in.ev.EvaluateExtractAll(c.Extract, lookup)
		if vals == nil {
			vals = []string{}
		}
		return vals, nil
	case "String":
		return firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), ""), nil
	case "Int64":
		n, _ := strconv.ParseInt(firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), ""), 10, 64)
		return n, nil
	case "UInt64":
		n, _ := strconv.ParseUint(firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), ""), 10, 64)
		return n, nil
	case "Float64":
		f, _ := strconv.ParseFloat(firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), ""), 64)
		return f, nil
	case "Bool":
		b, _ := strconv.ParseBool(firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), "false"))
		return b, nil
	case "DateTime64(3)":
		s := firstOr(in.ev.EvaluateExtractAll(c.Extract, lookup), "")
		if s == "" {
			return time.Unix(0, 0).UTC(), nil
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Unix(0, 0).UTC(), nil
		}
		return t.UTC(), nil
	}
	return nil, fmt.Errorf("unsupported column type %q", c.Type)
}

func (in *Ingester) enqueue(table string, row store.Row) {
	select {
	case in.queue <- queued{table: table, row: row}:
	case <-in.ctxDone:
	}
}

// priorOpen returns the last open row written for uid, and whether the incoming
// hash matches it (a resync / no-op). The informer delivers events for one
// object sequentially, so the caller may build the new row between this lookup
// and the matching remember() without racing itself.
func (in *Ingester) priorOpen(uid, hash string) (prior store.Row, hadPrior, same bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	prev, ok := in.last[uid]
	if !ok {
		return store.Row{}, false, false
	}
	return prev.row, true, prev.hash == hash
}

// remember records the row just written as uid's open version.
func (in *Ingester) remember(uid, hash string, row store.Row) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.last[uid] = lastState{hash: hash, row: row}
}

// forgetIfSeen drops uid from the live set, returning its open row if present.
func (in *Ingester) forgetIfSeen(uid string) (store.Row, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	prev, ok := in.last[uid]
	if !ok {
		return store.Row{}, false
	}
	delete(in.last, uid)
	return prev.row, true
}

// closingRow re-inserts prev with valid_to materialized. It keeps prev's sort
// key and takes a fresh, higher ingest_seq, so ReplacingMergeTree(ingest_seq)
// collapses it onto the open row it closes. closeAt is clamped to prev's start
// so a version can never span a negative interval (see Add-after-Add below).
func (in *Ingester) closingRow(prev store.Row, closeAt time.Time) store.Row {
	if closeAt.Before(prev.ValidFrom) {
		closeAt = prev.ValidFrom
	}
	prev.ValidTo = closeAt
	prev.IngestSeq = in.seq.Add(1)
	return prev
}

// validFrom is the observed start of this version: creationTimestamp for an
// Add (best estimate of when the object began), else observation time.
func validFrom(u *unstructured.Unstructured, kind eventKind) time.Time {
	if kind == eventAdd {
		if ts := u.GetCreationTimestamp(); !ts.IsZero() {
			return ts.Time.UTC()
		}
	}
	return time.Now().UTC()
}

func toUnstructured(o interface{}) *unstructured.Unstructured {
	switch v := o.(type) {
	case *unstructured.Unstructured:
		return v
	case cache.DeletedFinalStateUnknown:
		if u, ok := v.Obj.(*unstructured.Unstructured); ok {
			return u
		}
	}
	return nil
}

func kvTokens(raw []interface{}) []string {
	if len(raw) == 0 {
		return []string{}
	}
	m, ok := raw[0].(map[string]interface{})
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+collector.StringifyValue(v))
	}
	sort.Strings(out)
	return out
}

func firstOr(vals []string, def string) string {
	if len(vals) == 0 {
		return def
	}
	return vals[0]
}
