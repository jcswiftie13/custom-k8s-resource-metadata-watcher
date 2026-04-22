package collector

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

// anchorRef uniquely identifies an anchor across the collector. The
// AnchorKind field lets a single workqueue serve every rule kind without
// namespace/name collisions (e.g. a Pod and a ReplicaSet may share a name).
type anchorRef struct {
	AnchorKind string
	Namespace  string
	Name       string
}

// parentIndex is a bidirectional map between parent-object UIDs and the
// anchorRefs whose most recent reconcile referenced them. Each successful
// reconcile records the UIDs of every object reached via the owner chain;
// parent events then consult the index to avoid a namespace-wide rescan.
//
// The index is safe for concurrent use.
type parentIndex struct {
	mu sync.RWMutex

	// byParent returns the set of anchors that depend on a given parent UID.
	byParent map[types.UID]map[anchorRef]struct{}

	// byAnchor lists parent UIDs recorded for a given anchor, so we can
	// garbage-collect entries when an anchor is deleted or its dependency
	// set shrinks on re-reconcile.
	byAnchor map[anchorRef]map[types.UID]struct{}
}

// newParentIndex constructs an empty index.
func newParentIndex() *parentIndex {
	return &parentIndex{
		byParent: map[types.UID]map[anchorRef]struct{}{},
		byAnchor: map[anchorRef]map[types.UID]struct{}{},
	}
}

// Record replaces the set of parent UIDs associated with the anchor. Callers
// pass every parent UID observed during the latest reconcile (including the
// anchor's own UID is permitted but not required; the collector does not
// dedupe special-cases).
func (p *parentIndex) Record(ref anchorRef, parents []types.UID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	old := p.byAnchor[ref]
	next := make(map[types.UID]struct{}, len(parents))
	for _, uid := range parents {
		if uid == "" {
			continue
		}
		next[uid] = struct{}{}
	}

	for uid := range old {
		if _, keep := next[uid]; keep {
			continue
		}
		if set, ok := p.byParent[uid]; ok {
			delete(set, ref)
			if len(set) == 0 {
				delete(p.byParent, uid)
			}
		}
	}

	for uid := range next {
		set, ok := p.byParent[uid]
		if !ok {
			set = map[anchorRef]struct{}{}
			p.byParent[uid] = set
		}
		set[ref] = struct{}{}
	}

	if len(next) == 0 {
		delete(p.byAnchor, ref)
	} else {
		p.byAnchor[ref] = next
	}
}

// Forget removes every trace of an anchor from the index. Used when the
// anchor's delete event fires.
func (p *parentIndex) Forget(ref anchorRef) {
	p.mu.Lock()
	defer p.mu.Unlock()

	parents := p.byAnchor[ref]
	for uid := range parents {
		if set, ok := p.byParent[uid]; ok {
			delete(set, ref)
			if len(set) == 0 {
				delete(p.byParent, uid)
			}
		}
	}
	delete(p.byAnchor, ref)
}

// AnchorsFor returns a snapshot of the anchors that depend on the given
// parent UID. The second return value is false when no entry exists (cold
// index), which the caller interprets as "fall back to a namespace-wide
// rescan".
func (p *parentIndex) AnchorsFor(uid types.UID) ([]anchorRef, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set, ok := p.byParent[uid]
	if !ok {
		return nil, false
	}
	out := make([]anchorRef, 0, len(set))
	for ref := range set {
		out = append(out, ref)
	}
	return out, true
}

// Len returns the current size of the two internal maps. It is exposed so
// the collector can publish gauges for leak-detection tests.
func (p *parentIndex) Len() (byParent, byAnchor int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.byParent), len(p.byAnchor)
}
