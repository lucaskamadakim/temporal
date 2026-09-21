package quotas

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDispatchQueue_FIFO(t *testing.T) {
	q := newDispatchQueue("k", 1)
	for i := 0; i < 5; i++ {
		q.push(DispatchItem{Key: "k", Value: i, Cost: 1})
	}
	require.Equal(t, 5, q.len())
	for i := 0; i < 5; i++ {
		require.Equal(t, i, q.pop().Value)
	}
	require.True(t, q.empty())
	require.Equal(t, 0, q.len())
}

func TestDispatchQueue_PeekDoesNotConsume(t *testing.T) {
	q := newDispatchQueue("k", 1)
	q.push(DispatchItem{Key: "k", Value: "a", Cost: 1})
	q.push(DispatchItem{Key: "k", Value: "b", Cost: 1})
	require.Equal(t, "a", q.peek().Value)
	require.Equal(t, "a", q.peek().Value)
	require.Equal(t, 2, q.len())
}

func TestDispatchQueue_Compaction(t *testing.T) {
	q := newDispatchQueue("k", 1)
	total := dispatchCompactThreshold * 4 // 512
	for i := 0; i < total; i++ {
		q.push(DispatchItem{Key: "k", Value: i, Cost: 1})
	}
	// Pop past the point where the dead head prefix exceeds half the
	// slice (head=256 of len=512) so compaction runs mid-drain.
	for i := 0; i < 400; i++ {
		q.pop()
	}
	require.Equal(t, 112, q.len())
	require.Less(t, len(q.items), total) // backing array shrank
	// Order is preserved across compaction.
	require.Equal(t, 400, q.pop().Value)
}

func TestDispatchQueue_Drain(t *testing.T) {
	q := newDispatchQueue("k", 1)
	q.push(DispatchItem{Key: "k", Value: 1, Cost: 1})
	q.push(DispatchItem{Key: "k", Value: 2, Cost: 1})
	q.pop()
	drained := q.drain()
	require.Len(t, drained, 1)
	require.Equal(t, 2, drained[0].Value)
	require.True(t, q.empty())
}

func TestDispatchQueue_TotalCostTracked(t *testing.T) {
	q := newDispatchQueue("k", 1)
	q.push(DispatchItem{Key: "k", Cost: 3})
	q.push(DispatchItem{Key: "k", Cost: 2.5})
	require.InDelta(t, 5.5, q.totalCost, 1e-9)
}

func TestDispatchHeap_OrdersByVirtualFinish(t *testing.T) {
	var h dispatchHeap
	qa := newDispatchQueue("a", 1)
	qb := newDispatchQueue("b", 1)
	qc := newDispatchQueue("c", 1)
	qa.virtualFinish = 3
	qb.virtualFinish = 1
	qc.virtualFinish = 2
	heap.Push(&h, qa)
	heap.Push(&h, qb)
	heap.Push(&h, qc)
	require.Equal(t, "b", heap.Pop(&h).(*dispatchQueue).key)
	require.Equal(t, "c", heap.Pop(&h).(*dispatchQueue).key)
	require.Equal(t, "a", heap.Pop(&h).(*dispatchQueue).key)
	require.Equal(t, 0, h.Len())
}

func TestDispatchHeap_TieBreaksBySeq(t *testing.T) {
	var h dispatchHeap
	for i := 0; i < 5; i++ {
		q := newDispatchQueue(string(rune('a'+i)), 1)
		q.virtualFinish = 1 // identical finish times
		q.seq = uint64(i)
		heap.Push(&h, q)
	}
	for i := 0; i < 5; i++ {
		require.Equal(t, uint64(i), heap.Pop(&h).(*dispatchQueue).seq)
	}
}

func TestDispatchHeap_HeapIndexMaintained(t *testing.T) {
	var h dispatchHeap
	qs := make([]*dispatchQueue, 4)
	for i := range qs {
		qs[i] = newDispatchQueue(string(rune('a'+i)), 1)
		qs[i].virtualFinish = float64(i)
		heap.Push(&h, qs[i])
	}
	for i, q := range h {
		require.Equal(t, i, q.heapIndex)
	}
	// Re-ordering via Fix keeps indexes consistent.
	h[3].virtualFinish = -1
	heap.Fix(&h, 3)
	for i, q := range h {
		require.Equal(t, i, q.heapIndex)
	}
	require.Equal(t, qs[3], heap.Pop(&h).(*dispatchQueue))
}

func TestDispatchHeap_RemoveMiddle(t *testing.T) {
	var h dispatchHeap
	qs := make([]*dispatchQueue, 5)
	for i := range qs {
		qs[i] = newDispatchQueue(string(rune('a'+i)), 1)
		qs[i].virtualFinish = float64(i)
		heap.Push(&h, qs[i])
	}
	removed := heap.Remove(&h, qs[2].heapIndex).(*dispatchQueue)
	require.Equal(t, qs[2], removed)
	require.Equal(t, -1, removed.heapIndex)
	require.Equal(t, 4, h.Len())
	for i, q := range h {
		require.Equal(t, i, q.heapIndex)
	}
}

func TestDispatchQueue_Snapshot(t *testing.T) {
	q := newDispatchQueue("k", 2)
	q.push(DispatchItem{Key: "k", Cost: 1})
	q.virtualStart = 1
	q.virtualFinish = 2
	q.active = true
	s := q.snapshot()
	require.Equal(t, "k", s.Key)
	require.Equal(t, 1, s.Depth)
	require.InDelta(t, 2.0, s.Weight, 1e-9)
	require.InDelta(t, 1.0, s.VirtualStart, 1e-9)
	require.InDelta(t, 2.0, s.VirtualFinish, 1e-9)
	require.True(t, s.Active)
}
