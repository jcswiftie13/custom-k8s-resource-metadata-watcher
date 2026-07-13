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

// queued is one ordered write operation: an INSERT of row (close == nil) or an
// in-place close of a previously inserted open row (closeMode=update). A
// single FIFO queue keeps each object's operations in event order.
type queued struct {
	table string
	row   store.Row
	close *closeOp
}

// closeOp pins the exact open row to patch: identity + valid_from + (implicit)
// the FarFuture sentinel. Because the successor version has a different
// valid_from, running the close AFTER the successor's INSERT is safe — which
// is what allows the flush cycle to send INSERT batches first and the closes
// second.
type closeOp struct {
	namespace, name, uid string
	validFrom            time.Time
	closeAt              time.Time
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
	// updateClose selects how a superseded/deleted version's valid_to lands:
	// false (rewrite) re-inserts the prior row with valid_to set (collapsed by
	// ReplacingMergeTree); true (update) patches the open row in place with a
	// lightweight UPDATE, so every version stays exactly one row.
	updateClose bool

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

// NewIngester builds an Ingester from compiled resources. closeMode is
// config.CloseModeRewrite or config.CloseModeUpdate (see StoreConfig.CloseMode).
func NewIngester(st store.Store, informers InformerSource, resources []*CompiledResource, batch config.BatchConfig, closeMode string, log *slog.Logger) *Ingester {
	byKind := make(map[string]*CompiledResource, len(resources))
	for _, r := range resources {
		byKind[r.Kind] = r
	}
	maxRows := batch.MaxRowsOrDefault()
	return &Ingester{
		store:       st,
		informers:   informers,
		ev:          collector.NewEvaluator(),
		byKind:      byKind,
		log:         log,
		batchMax:    maxRows,
		flushIvl:    time.Duration(batch.FlushIntervalMsOrDefault()) * time.Millisecond,
		updateClose: closeMode == config.CloseModeUpdate,
		last:        map[string]lastState{},
		queue:       make(chan queued, maxRows*2),
	}
}

// Recover rebuilds the ingester's last-state from the store's open rows —
// restart idempotency for closeMode=update. Without it, the initial re-LIST
// after a restart re-inserts a row for every live object (the in-memory
// last-state is empty), breaking the "every version is exactly one row"
// guarantee update mode exists for. Call after EnsureSchema and BEFORE the
// informers start.
//
// Per uid it keeps the newest open row (max valid_from, then max ingest_seq —
// raw rows may still contain rewrite-era duplicates) as the object's open
// version: matching spec_hash on re-LIST is then a no-op, a differing hash
// closes it and opens a new version at observation time. Any OLDER open row of
// the same uid is a stale leftover from a crash between an INSERT flush and
// its close — it is closed here at the next version's valid_from, the exact
// boundary the lost close would have written.
//
// Rewrite mode keeps its memory-only last-state (re-LIST duplicates there are
// same-key rows the readers' dedup already collapses), so Recover is a no-op.
func (in *Ingester) Recover(ctx context.Context) error {
	if !in.updateClose {
		return nil
	}
	tables := map[string]struct{}{}
	for _, cr := range in.byKind {
		tables[cr.Table] = struct{}{}
	}
	recovered, sweeps := 0, 0
	for table := range tables {
		open, err := in.store.OpenVersions(ctx, table)
		if err != nil {
			return fmt.Errorf("history recover %s: %w", table, err)
		}
		// Group per uid, collapse duplicate slots (same valid_from) by max
		// ingest_seq, order by valid_from. OpenVersions returns rows already
		// ordered (uid, valid_from, ingest_seq), so a linear pass suffices.
		byUID := map[string][]store.OpenVersion{}
		for _, v := range open {
			vs := byUID[v.UID]
			if n := len(vs); n > 0 && vs[n-1].ValidFrom.Equal(v.ValidFrom) {
				vs[n-1] = v // same slot, higher seq wins
			} else {
				vs = append(vs, v)
			}
			byUID[v.UID] = vs
		}
		for uid, vs := range byUID {
			latest := vs[len(vs)-1]
			in.remember(uid, latest.SpecHash, store.Row{
				Namespace: latest.Namespace,
				Name:      latest.Name,
				UID:       uid,
				ValidFrom: latest.ValidFrom,
				ValidTo:   store.FarFuture,
			})
			recovered++
			// Stale opens: every open row before the latest lost its close in a
			// crash window; its true end is the next version's start.
			for i := 0; i < len(vs)-1; i++ {
				if err := in.store.CloseVersion(ctx, table,
					vs[i].Namespace, vs[i].Name, uid, vs[i].ValidFrom, vs[i+1].ValidFrom); err != nil {
					return fmt.Errorf("history recover %s: close stale open row: %w", table, err)
				}
				sweeps++
			}
		}
	}
	in.log.Info("history last-state recovered from store", "objects", recovered, "staleOpensClosed", sweeps)
	return nil
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
	var closes []queued
	count := 0
	// flush sends the buffered INSERT batches first and only then executes the
	// buffered closes. Queue order guarantees a close was enqueued after the
	// INSERT of the row it targets, so that row is at the latest in this
	// cycle's batch; and a close can safely run after its successor's INSERT
	// because the close predicate pins the exact valid_from (see closeOp).
	flush := func() {
		for table, rows := range pending {
			if len(rows) == 0 {
				continue
			}
			if err := in.store.WriteBatch(ctx, table, rows); err != nil {
				in.log.Error("history write batch failed", "table", table, "rows", len(rows), "err", err)
			}
		}
		for _, q := range closes {
			c := q.close
			if err := in.store.CloseVersion(ctx, q.table, c.namespace, c.name, c.uid, c.validFrom, c.closeAt); err != nil {
				in.log.Error("history close version failed", "table", q.table,
					"namespace", c.namespace, "name", c.name, "uid", c.uid, "err", err)
			}
		}
		pending = map[string][]store.Row{}
		closes = nil
		count = 0
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case q := <-in.queue:
			if q.close != nil {
				closes = append(closes, q)
			} else {
				pending[q.table] = append(pending[q.table], q.row)
			}
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
		in.enqueueClose(cr.Table, prior, time.Now().UTC())
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

	// An Add for a uid we already track is a change observed as a re-LIST
	// (restart recovery, informer replay): the change happened while we were
	// not watching, so its version starts at observation time — the object's
	// creationTimestamp would overlap every prior version.
	effKind := kind
	if hadPrior && kind == eventAdd {
		effKind = eventUpdate
	}
	row, err := in.buildRow(cr, u, lookup, effKind)
	if err != nil {
		in.log.Error("history build row failed", "kind", cr.Kind, "uid", uid, "err", err)
		return
	}
	row.SpecHash = hash
	in.remember(uid, hash, row)

	// The superseded version ends where the new one begins.
	if hadPrior {
		in.enqueueClose(cr.Table, prior, row.ValidFrom)
	}
	in.enqueue(cr.Table, row, nil)
}

// enqueueClose records that prev ends at closeAt, in whichever way the close
// mode dictates: rewrite re-inserts prev with valid_to materialized and a
// higher ingest_seq (ReplacingMergeTree collapses the pair later); update
// patches the open row in place. closeAt is clamped to prev's start so a
// version can never span a negative interval (Add-after-Add).
func (in *Ingester) enqueueClose(table string, prev store.Row, closeAt time.Time) {
	if closeAt.Before(prev.ValidFrom) {
		closeAt = prev.ValidFrom
	}
	if in.updateClose {
		in.enqueue(table, store.Row{}, &closeOp{
			namespace: prev.Namespace, name: prev.Name, uid: prev.UID,
			validFrom: prev.ValidFrom, closeAt: closeAt,
		})
		return
	}
	in.enqueue(table, in.closingRow(prev, closeAt), nil)
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
// encoding. Constant columns return their compile-time value. Path columns try
// primary then fallbacks then onMissing; when onMissing is unset, missing
// values yield typed zeros so the batch always has a complete tuple.
func (in *Ingester) columnValue(c *CompiledColumn, lookup srcLookup) (any, error) {
	if c.IsConstant {
		return c.Constant, nil
	}

	switch c.Encode {
	case "json":
		raw := in.evaluateRaw(c, lookup)
		if len(raw) == 0 || raw[0] == nil {
			return "", nil
		}
		b, err := json.Marshal(raw[0])
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case "kv":
		return kvTokens(in.evaluateRaw(c, lookup)), nil
	}

	vals, _ := in.evaluateStrings(c, lookup)
	switch c.Type {
	case "Array(String)":
		if vals == nil {
			vals = []string{}
		}
		return vals, nil
	case "String":
		return firstOr(vals, ""), nil
	case "Int64":
		n, _ := strconv.ParseInt(firstOr(vals, ""), 10, 64)
		return n, nil
	case "UInt64":
		n, _ := strconv.ParseUint(firstOr(vals, ""), 10, 64)
		return n, nil
	case "Float64":
		f, _ := strconv.ParseFloat(firstOr(vals, ""), 64)
		return f, nil
	case "Bool":
		b, _ := strconv.ParseBool(firstOr(vals, "false"))
		return b, nil
	case "DateTime64(3)":
		s := firstOr(vals, "")
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

// evaluateStrings tries primary then fallbacks; on total miss returns
// onMissing (as a single-element slice) when set, else nil.
func (in *Ingester) evaluateStrings(c *CompiledColumn, lookup srcLookup) (vals []string, fromOnMissing bool) {
	vals = in.ev.EvaluateExtractAll(c.Primary, lookup)
	if len(vals) > 0 {
		return vals, false
	}
	for _, f := range c.Fallbacks {
		vals = in.ev.EvaluateExtractAll(f, lookup)
		if len(vals) > 0 {
			return vals, false
		}
	}
	if c.HasOnMissing {
		return []string{c.OnMissing}, true
	}
	return nil, false
}

// evaluateRaw tries primary then fallbacks for encode=json/kv. onMissing does
// not apply (rejected at validate time when encode is set).
func (in *Ingester) evaluateRaw(c *CompiledColumn, lookup srcLookup) []interface{} {
	raw := in.ev.EvaluateExtractRaw(c.Primary, lookup)
	if len(raw) > 0 {
		return raw
	}
	for _, f := range c.Fallbacks {
		raw = in.ev.EvaluateExtractRaw(f, lookup)
		if len(raw) > 0 {
			return raw
		}
	}
	return nil
}

func (in *Ingester) enqueue(table string, row store.Row, close *closeOp) {
	select {
	case in.queue <- queued{table: table, row: row, close: close}:
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

// closingRow re-inserts prev with valid_to materialized (rewrite mode). It
// keeps prev's sort key and takes a fresh, higher ingest_seq, so
// ReplacingMergeTree(ingest_seq) collapses it onto the open row it closes.
// closeAt is pre-clamped by enqueueClose.
func (in *Ingester) closingRow(prev store.Row, closeAt time.Time) store.Row {
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
