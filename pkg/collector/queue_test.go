package collector

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// TestWorkqueue_DedupesBursts documents the queue behavior we rely on: many
// Add calls for the same key collapse into a single Get/Done cycle. This is
// the core of the "burst updates coalesce into one reconcile" property the
// collector depends on.
func TestWorkqueue_DedupesBursts(t *testing.T) {
	q := workqueue.NewTypedRateLimitingQueue[anchorRef](
		workqueue.DefaultTypedControllerRateLimiter[anchorRef](),
	)
	defer q.ShutDown()

	ref := anchorRef{AnchorKind: "Pod", Namespace: "ns", Name: "p"}
	for i := 0; i < 100; i++ {
		q.Add(ref)
	}

	// Expect a single item to come out, even after 100 adds.
	if got := q.Len(); got != 1 {
		t.Fatalf("queue length after 100 adds = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	var popped int
	go func() {
		for {
			item, shutdown := q.Get()
			if shutdown {
				close(done)
				return
			}
			popped++
			q.Done(item)
			// Give the queue a moment to re-surface duplicates if any.
			time.Sleep(10 * time.Millisecond)
			if q.Len() == 0 {
				q.ShutDown()
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("queue drain timed out")
	}
	if popped != 1 {
		t.Fatalf("expected to pop exactly 1 item after 100 adds, got %d", popped)
	}
}
