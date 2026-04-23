package collector

import (
	"sort"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func sortRefs(refs []anchorRef) []anchorRef {
	out := append([]anchorRef(nil), refs...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].AnchorKind != out[j].AnchorKind {
			return out[i].AnchorKind < out[j].AnchorKind
		}
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func TestParentIndex_RecordAndLookup(t *testing.T) {
	idx := newParentIndex()
	a := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p1"}
	b := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p2"}

	idx.Record(a, []types.UID{"rs-1", "dep-1"})
	idx.Record(b, []types.UID{"rs-1"})

	got, hit := idx.AnchorsFor("rs-1")
	if !hit {
		t.Fatalf("expected hit for rs-1")
	}
	want := sortRefs([]anchorRef{a, b})
	if g := sortRefs(got); len(g) != 2 || g[0] != want[0] || g[1] != want[1] {
		t.Fatalf("rs-1 anchors = %v want %v", g, want)
	}

	got, hit = idx.AnchorsFor("dep-1")
	if !hit || len(got) != 1 || got[0] != a {
		t.Fatalf("dep-1 anchors = %v", got)
	}

	_, hit = idx.AnchorsFor("unknown")
	if hit {
		t.Fatalf("expected miss for unknown parent UID")
	}
}

func TestParentIndex_ReRecordShrinksSet(t *testing.T) {
	idx := newParentIndex()
	a := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p1"}
	idx.Record(a, []types.UID{"rs-1", "dep-1"})

	// Subsequent reconcile no longer sees rs-1 (anchor moved under a new
	// owner). The old mapping must be garbage-collected.
	idx.Record(a, []types.UID{"dep-1"})

	if refs, hit := idx.AnchorsFor("rs-1"); hit {
		t.Fatalf("expected rs-1 to be evicted after re-record, got %v", refs)
	}
	refs, hit := idx.AnchorsFor("dep-1")
	if !hit || len(refs) != 1 || refs[0] != a {
		t.Fatalf("expected dep-1 to still point at anchor, got %v", refs)
	}
}

func TestParentIndex_Forget(t *testing.T) {
	idx := newParentIndex()
	a := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p1"}
	b := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p2"}
	idx.Record(a, []types.UID{"rs-1"})
	idx.Record(b, []types.UID{"rs-1"})

	idx.Forget(a)

	refs, hit := idx.AnchorsFor("rs-1")
	if !hit || len(refs) != 1 || refs[0] != b {
		t.Fatalf("after forget(a), rs-1 should only map to b; got %v", refs)
	}

	idx.Forget(b)
	if _, hit := idx.AnchorsFor("rs-1"); hit {
		t.Fatalf("after forgetting both anchors, rs-1 should miss")
	}
}

func TestParentIndex_IgnoresEmptyUID(t *testing.T) {
	idx := newParentIndex()
	a := anchorRef{AnchorKind: "Pod", Namespace: "n", Name: "p1"}
	idx.Record(a, []types.UID{"", "rs-1", ""})

	if _, hit := idx.AnchorsFor(""); hit {
		t.Fatalf("empty UID must never hit")
	}
	refs, hit := idx.AnchorsFor("rs-1")
	if !hit || len(refs) != 1 || refs[0] != a {
		t.Fatalf("rs-1 should map to a, got hit=%v refs=%v", hit, refs)
	}
}
