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
// single FIFO queue keeps each object's operations in event order. isClose
// marks BOTH close forms — closeOp and rewrite-mode closing rows are otherwise
// indistinguishable from plain inserts — so drop-oldest eviction can preserve
// them (losing a close corrupts the open/close invariant; losing an insert
// only skips one version).
type queued struct {
	table   string
	row     store.Row
	close   *closeOp
	isClose bool
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

	queue chan queued

	// closeStash holds close ops evicted from a full queue: closes are never
	// dropped while stash capacity (closeStashMax) remains, because a lost
	// close leaves a stale open row. flush drains the stash first each cycle.
	closeMu       sync.Mutex
	closeStash    []queued
	closeStashMax int

	droppedTotal atomic.Uint64 // for rate-limited drop logging only; metrics hold the counters

	// losses counts every operation that reached the ingester but never made
	// it into the store: evicted inserts, overflowed closes, failed flush
	// calls. The manager compares it against a baseline to decide whether an
	// outage diverged the store from in-memory state (→ rebuild needed).
	losses atomic.Uint64

	// regs are the informer registrations from Register, kept so HasSynced can
	// report replay delivery and Unregister can detach on teardown.
	regs []handlerReg

	// loopDone is closed when the flush loop exits; Start sets it.
	loopDone chan struct{}

	metrics *Metrics
}

// handlerReg pairs a registration with the informer it was added to, so
// Unregister can remove it.
type handlerReg struct {
	inf cache.SharedIndexInformer
	reg cache.ResourceEventHandlerRegistration
}

// lastState is the last open version written for a live uid: its spec hash (for
// dedup) and the whole row, kept so a later version or a delete can re-insert it
// with valid_to filled in. One entry per live object, not per historical
// version. row.Values is built fresh by buildRow and never mutated afterwards,
// so holding the reference is safe. table and kind locate the uid's history
// table and informer cache for ReconcileOpens.
type lastState struct {
	hash  string
	row   store.Row
	table string
	kind  string
}

// NewIngester builds an Ingester from compiled resources. closeMode is
// config.CloseModeRewrite or config.CloseModeUpdate (see StoreConfig.CloseMode).
func NewIngester(st store.Store, informers InformerSource, resources []*CompiledResource, batch config.BatchConfig, closeMode string, log *slog.Logger, metrics *Metrics) *Ingester {
	byKind := make(map[string]*CompiledResource, len(resources))
	for _, r := range resources {
		byKind[r.Kind] = r
	}
	maxRows := batch.MaxRowsOrDefault()
	in := &Ingester{
		store:         st,
		informers:     informers,
		ev:            collector.NewEvaluator(),
		byKind:        byKind,
		log:           log,
		batchMax:      maxRows,
		flushIvl:      time.Duration(batch.FlushIntervalMsOrDefault()) * time.Millisecond,
		updateClose:   closeMode == config.CloseModeUpdate,
		last:          map[string]lastState{},
		queue:         make(chan queued, maxRows*2),
		closeStashMax: maxRows,
		metrics:       metrics,
	}
	// ingest_seq base: milliseconds in the high 44 bits, the low 20 left for
	// in-process increments (>1M rows/ms would be needed to overflow into the
	// next millisecond — impossible). ReplacingMergeTree(ingest_seq) keeps the
	// max-seq row per key, so the counter must outrank every row an earlier
	// process persisted; a wall-clock base guarantees that across restarts
	// without any store read (seq>>20 also decodes to the process start time
	// for field debugging). Recover raises it further if the store holds
	// higher seqs (clock stepped backwards since the previous process).
	in.seq.Store(uint64(time.Now().UnixMilli()) << 20)
	return in
}

// Recover rebuilds the ingester's last-state from the store's open rows so a
// post-restart re-LIST dedups instead of re-inserting a row for every live
// object. Call after EnsureSchema and BEFORE the informers start.
//
// Per uid it keeps the newest open row (max valid_from, then max ingest_seq —
// raw rows may still contain rewrite-era duplicates) as the object's open
// version, recomputing the dedup hash from its declared column Values (there is
// no persisted hash): a matching column hash on re-LIST is then a no-op, a
// differing one closes the version and opens a new one at observation time.
//
// It runs in BOTH close modes — without it, dropping resource_version from the
// key would let a re-LIST of an object last changed by an Update re-open a
// false version at a fresh valid_from. Any OLDER open row of the same uid is a
// stale leftover from a crash between an INSERT flush and its close; the
// stale-open sweep closes it at the next version's valid_from (the exact
// boundary the lost close would have written). That sweep is update-mode only:
// rewrite has no in-place close, so its crash-window stale opens are left to
// the readers' dedup.
func (in *Ingester) Recover(ctx context.Context) error {
	recovered, sweeps := 0, 0
	// Config validation rejects duplicate tables, so kind <-> table is 1:1 and
	// iterating byKind visits every table exactly once.
	for kind, cr := range in.byKind {
		table := cr.Table
		// Fuse for the time-derived counter base: any persisted seq above it
		// means the wall clock stepped backwards across the restart; re-seed
		// above the store's max or every new row sharing a key with an old one
		// would lose the ReplacingMergeTree merge. Recover runs before Start
		// and Register, so plain Load/Store on seq cannot race.
		if max, err := in.store.MaxIngestSeq(ctx, table, in.seq.Load()); err != nil {
			return fmt.Errorf("history recover %s: max ingest_seq: %w", table, err)
		} else if max > in.seq.Load() {
			in.log.Warn("history ingest_seq base below stored rows (clock stepped backwards?); re-seeding",
				"table", table, "storeMax", max)
			in.seq.Store(max)
		}
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
			hash, err := ColumnHash(latest.Values)
			if err != nil {
				return fmt.Errorf("history recover %s: hash open row uid=%s: %w", table, uid, err)
			}
			in.remember(uid, hash, store.Row{
				Namespace: latest.Namespace,
				Name:      latest.Name,
				UID:       uid,
				ValidFrom: latest.ValidFrom,
				ValidTo:   store.FarFuture,
				Values:    latest.Values,
			}, table, kind)
			recovered++
			if !in.updateClose {
				continue
			}
			// Stale opens: every open row before the latest lost its close in a
			// crash window; its true end is the next version's start. Only update
			// mode can patch them in place.
			for i := 0; i < len(vs)-1; i++ {
				if err := in.store.CloseVersion(ctx, table,
					vs[i].Namespace, vs[i].Name, uid, vs[i].ValidFrom, vs[i+1].ValidFrom); err != nil {
					return fmt.Errorf("history recover %s: close stale open row: %w", table, err)
				}
				in.metrics.incClose(closeReasonRecoverSweep)
				in.log.Debug("history close version",
					"table", table, "namespace", vs[i].Namespace, "name", vs[i].Name, "uid", uid,
					"validFrom", vs[i].ValidFrom, "closeAt", vs[i+1].ValidFrom, "reason", closeReasonRecoverSweep)
				sweeps++
			}
		}
	}
	in.log.Info("history last-state recovered from store",
		"objects", recovered, "staleOpensClosed", sweeps, "ingestSeqBase", in.seq.Load())
	return nil
}

// ReconcileOpens closes open rows whose objects no longer exist: an object
// deleted while the exporter was down (or disconnected from the store) leaves
// a single open row that Recover happily adopts as live, and the cache replay
// never delivers a Delete for it. Call once per (re)initialization, after
// Register and only once HasSynced is true — before that the caches are still
// replaying and a merely-not-yet-delivered object would be closed by mistake.
//
// The close lands at observation time, consistent with how changes observed
// via re-LIST are stamped (see onEvent). Racing a live Delete event is safe:
// forgetIfSeen removes the uid atomically, so exactly one side wins.
//
// "Absent from the cache" only implies "deleted" while the informers cover
// the object's scope: a kind whose informers are missing entirely is skipped
// (never mass-closed), but a narrowed watch scope — e.g. a namespace removed
// from watch.resources between restarts — is indistinguishable from deletion
// and WILL close those objects' rows. The mass-close Error log below flags
// such wipes; check it before trusting a burst of reconcile_orphan closes.
func (in *Ingester) ReconcileOpens() int {
	if !in.HasSynced() {
		in.log.Warn("history reconcile skipped: informer replay not yet delivered")
		return 0
	}
	type candidate struct {
		uid   string
		key   string
		table string
		kind  string
	}
	in.mu.Lock()
	cands := make([]candidate, 0, len(in.last))
	perKind := map[string]int{}
	for uid, ls := range in.last {
		key := ls.row.Name
		if ls.row.Namespace != "" {
			key = ls.row.Namespace + "/" + ls.row.Name
		}
		cands = append(cands, candidate{uid: uid, key: key, table: ls.table, kind: ls.kind})
		perKind[ls.kind]++
	}
	in.mu.Unlock()

	// A kind with no informer at all yields no aliveness signal — closing its
	// rows would wipe every tracked object of that kind, so refuse.
	informersByKind := make(map[string][]cache.SharedIndexInformer, len(perKind))
	for kind := range perKind {
		infs := in.informers.Informers(kind)
		if len(infs) == 0 {
			in.log.Warn("history reconcile: no informer for kind; refusing to close its open rows",
				"kind", kind, "tracked", perKind[kind])
			continue
		}
		informersByKind[kind] = infs
	}

	orphans := make([]candidate, 0)
	orphansPerKind := map[string]int{}
	for _, c := range cands {
		infs, ok := informersByKind[c.kind]
		if !ok {
			continue
		}
		alive := false
		for _, inf := range infs {
			obj, exists, err := inf.GetStore().GetByKey(c.key)
			if err != nil || !exists {
				continue
			}
			// Same name recreated with a new uid is a different object; the
			// old uid's row must still close.
			if u := toUnstructured(obj); u != nil && string(u.GetUID()) == c.uid {
				alive = true
				break
			}
		}
		if !alive {
			orphans = append(orphans, c)
			orphansPerKind[c.kind]++
		}
	}

	// Mass-close sanity check: most of a kind vanishing at once looks like a
	// coverage change (namespace scope narrowed, informer misconfigured), not
	// deletions. Log-only — a real mass deletion must still be recorded.
	for kind, n := range orphansPerKind {
		if n > 10 && n*2 > perKind[kind] {
			in.log.Error("history reconcile closing most tracked objects of a kind; "+
				"verify the informer scope still covers them (namespace list unchanged?)",
				"kind", kind, "closing", n, "tracked", perKind[kind])
		}
	}

	closed := 0
	now := time.Now().UTC()
	for _, c := range orphans {
		prior, ok := in.forgetIfSeen(c.uid)
		if !ok {
			continue // a concurrent Delete event beat us to it
		}
		in.log.Info("history reconcile closing orphaned open row (object absent from informer cache)",
			"kind", c.kind, "table", c.table, "namespace", prior.Namespace, "name", prior.Name, "uid", c.uid)
		in.enqueueClose(c.table, prior, now, closeReasonReconcileOrphan)
		closed++
	}
	if closed > 0 {
		in.log.Info("history reconcile closed orphaned open rows", "objects", closed)
	}
	return closed
}

// Register attaches event handlers on every informer for each configured Kind.
// Registering before the informers start captures the initial LIST as Add
// events; registering after they started (the manager's reconnect path) makes
// client-go replay the full cache as Add events instead — either way the
// ingester sees a complete snapshot, deduped against the Recover baseline.
// HasSynced reports when that replay has been delivered.
func (in *Ingester) Register() error {
	for kind, cr := range in.byKind {
		infs := in.informers.Informers(kind)
		if len(infs) == 0 {
			return fmt.Errorf("history: no informer for kind %q (is it in watch.resources?)", kind)
		}
		crLocal := cr
		for _, inf := range infs {
			reg, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(o interface{}) { in.onEvent(crLocal, o, eventAdd) },
				UpdateFunc: func(_, o interface{}) { in.onEvent(crLocal, o, eventUpdate) },
				DeleteFunc: func(o interface{}) { in.onEvent(crLocal, o, eventDelete) },
			})
			if err != nil {
				return fmt.Errorf("history: add handler for %q: %w", kind, err)
			}
			in.regs = append(in.regs, handlerReg{inf: inf, reg: reg})
		}
	}
	return nil
}

// Unregister detaches every event handler added by Register, so a torn-down
// ingester stops producing before the manager rebuilds a fresh one. Removal
// errors are logged, not returned: the ingester is being discarded anyway.
func (in *Ingester) Unregister() {
	for _, r := range in.regs {
		if err := r.inf.RemoveEventHandler(r.reg); err != nil {
			in.log.Warn("history: remove event handler failed", "err", err)
		}
	}
	in.regs = nil
}

// HasSynced reports whether every registered handler has been delivered its
// initial snapshot (LIST or late-registration cache replay). Only meaningful
// after Register.
func (in *Ingester) HasSynced() bool {
	for _, r := range in.regs {
		if !r.reg.HasSynced() {
			return false
		}
	}
	return true
}

// lossCount returns how many operations were lost since construction (evicted
// inserts, overflowed closes, failed flushes). The manager uses deltas to
// detect store/in-memory divergence.
func (in *Ingester) lossCount() uint64 { return in.losses.Load() }

// Start launches the batch flush loop. It returns immediately; the loop runs
// until ctx is cancelled, flushing any buffered rows on exit (loopDone is
// closed once that final flush finished).
func (in *Ingester) Start(ctx context.Context) {
	in.loopDone = make(chan struct{})
	go func() {
		defer close(in.loopDone)
		in.loop(ctx)
	}()
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
		// The exit flush runs after ctx was cancelled; writing with a dead
		// context would fail unconditionally and silently drop the final
		// buffer, so give it a short detached grace window instead.
		wctx := ctx
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			wctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
		}
		// Closes evicted from a full queue were stashed instead of dropped;
		// fold them back in first. They predate everything currently queued,
		// and a close is order-independent past its own INSERT (see closeOp),
		// so merging them into this cycle is safe.
		for _, q := range in.drainCloseStash() {
			if q.close != nil {
				closes = append(closes, q)
			} else {
				pending[q.table] = append(pending[q.table], q.row)
			}
		}
		for table, rows := range pending {
			if len(rows) == 0 {
				continue
			}
			if err := in.store.WriteBatch(wctx, table, rows); err != nil {
				in.losses.Add(uint64(len(rows)))
				in.metrics.incWriteFailure()
				in.log.Error("history write batch failed", "table", table, "rows", len(rows), "err", err)
			}
		}
		for _, q := range closes {
			c := q.close
			if err := in.store.CloseVersion(wctx, q.table, c.namespace, c.name, c.uid, c.validFrom, c.closeAt); err != nil {
				in.losses.Add(1)
				in.metrics.incWriteFailure()
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
		// close alone records the deletion — valid_to bounds the last version, so
		// no tombstone row is written.
		prior, ok := in.forgetIfSeen(uid)
		if !ok {
			return
		}
		in.enqueueClose(cr.Table, prior, time.Now().UTC(), closeReasonDelete)
		return
	}

	if !cr.Passes(in.ev, lookup) {
		return
	}

	prev, hadPrior := in.lookup(uid)

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
	hash, err := ColumnHash(row.Values)
	if err != nil {
		in.log.Error("history column hash failed", "kind", cr.Kind, "uid", uid, "err", err)
		return
	}
	if hadPrior && prev.hash == hash {
		return // resync / no-op: declared columns unchanged
	}

	// valid_from must strictly increase per uid: without resource_version in the
	// key, two content versions landing in the same millisecond would share the
	// (namespace, name, uid, valid_from) key and ReplacingMergeTree would keep
	// only one. Bump to prev+1ms; the prior version then closes at this same
	// (bumped) instant, so successor-start == predecessor-end still holds.
	if hadPrior && !row.ValidFrom.After(prev.row.ValidFrom) {
		row.ValidFrom = prev.row.ValidFrom.Add(time.Millisecond)
	}
	in.remember(uid, hash, row, cr.Table, cr.Kind)

	// The superseded version ends where the new one begins. An Add promoted to
	// Update (effKind) is a change first observed via re-LIST/replay, not live.
	if hadPrior {
		reason := closeReasonSupersede
		if kind == eventAdd {
			reason = closeReasonRelistChange
		}
		in.enqueueClose(cr.Table, prev.row, row.ValidFrom, reason)
	}
	in.enqueue(queued{table: cr.Table, row: row})
}

// enqueueClose records that prev ends at closeAt, in whichever way the close
// mode dictates: rewrite re-inserts prev with valid_to materialized and a
// higher ingest_seq (ReplacingMergeTree collapses the pair later); update
// patches the open row in place. closeAt is clamped to prev's start so a
// version can never span a negative interval (Add-after-Add). reason is one
// of the closeReason* constants; it stamps the metric and debug log so every
// materialized valid_to is attributable to its code path.
func (in *Ingester) enqueueClose(table string, prev store.Row, closeAt time.Time, reason string) {
	if closeAt.Before(prev.ValidFrom) {
		closeAt = prev.ValidFrom
	}
	in.metrics.incClose(reason)
	in.log.Debug("history close version",
		"table", table, "namespace", prev.Namespace, "name", prev.Name, "uid", prev.UID,
		"validFrom", prev.ValidFrom, "closeAt", closeAt, "reason", reason)
	if in.updateClose {
		in.enqueue(queued{table: table, close: &closeOp{
			namespace: prev.Namespace, name: prev.Name, uid: prev.UID,
			validFrom: prev.ValidFrom, closeAt: closeAt,
		}, isClose: true})
		return
	}
	in.enqueue(queued{table: table, row: in.closingRow(prev, closeAt), isClose: true})
}

// buildRow assembles the open version row for an Add/Update. A deletion is
// recorded by closing valid_to, never by a tombstone row.
func (in *Ingester) buildRow(cr *CompiledResource, u *unstructured.Unstructured, lookup srcLookup, kind eventKind) (store.Row, error) {
	row := store.Row{
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
		UID:       string(u.GetUID()),
		ValidFrom: validFrom(u, kind),
		ValidTo:   store.FarFuture,
		IngestSeq: in.seq.Add(1),
		Values:    make(map[string]any, len(cr.Columns)),
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
		// Truncate to the millisecond precision DateTime64(3) stores, so the
		// in-memory value equals the value read back on restart and ColumnHash
		// matches (see store.OpenVersions / derefTarget).
		return t.UTC().Truncate(time.Millisecond), nil
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

// enqueue never blocks: when the queue is full it evicts the oldest queued op
// to make room (drop-oldest). Evicted inserts are lost — one version row the
// store never sees, counted and rate-limit logged — while evicted closes are
// stashed and re-sent by the next flush, because a lost close would leave a
// stale open row (broken open/close invariant) rather than a mere gap.
func (in *Ingester) enqueue(q queued) {
	for {
		select {
		case in.queue <- q:
			return
		default:
		}
		select {
		case old := <-in.queue:
			if old.isClose {
				in.stashClose(old)
			} else {
				in.dropInsert(old)
			}
		default:
			// The flush loop drained the queue between our two selects; retry.
		}
	}
}

// stashClose preserves a close op evicted from the full queue. The stash is
// capped (rewrite-mode closes carry full rows): beyond closeStashMax the close
// is dropped for real and counted under kind="close".
func (in *Ingester) stashClose(q queued) {
	in.closeMu.Lock()
	if len(in.closeStash) < in.closeStashMax {
		in.closeStash = append(in.closeStash, q)
		in.closeMu.Unlock()
		return
	}
	in.closeMu.Unlock()
	in.losses.Add(1)
	in.metrics.incDropped(droppedKindClose)
	in.log.Error("history close op dropped: queue and close stash both full", "table", q.table)
}

func (in *Ingester) drainCloseStash() []queued {
	in.closeMu.Lock()
	defer in.closeMu.Unlock()
	out := in.closeStash
	in.closeStash = nil
	return out
}

func (in *Ingester) dropInsert(q queued) {
	in.losses.Add(1)
	in.metrics.incDropped(droppedKindInsert)
	if n := in.droppedTotal.Add(1); n == 1 || n%10000 == 0 {
		in.log.Error("history events dropped: ingest queue full (oldest inserts evicted)",
			"table", q.table, "droppedTotal", n)
	}
}

// lookup returns the last-state (open row + its column hash) written for uid.
// The informer delivers events for one object sequentially, so the caller may
// build the new row and compare hashes between this lookup and the matching
// remember() without racing itself.
func (in *Ingester) lookup(uid string) (prev lastState, hadPrior bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	prev, ok := in.last[uid]
	return prev, ok
}

// remember records the row just written as uid's open version.
func (in *Ingester) remember(uid, hash string, row store.Row, table, kind string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.last[uid] = lastState{hash: hash, row: row, table: table, kind: kind}
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
