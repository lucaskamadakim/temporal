package adaptive

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestReservation(seq uint64, grantAt time.Time) *reservation {
	return &reservation{
		seq:       seq,
		grantAt:   grantAt,
		heapIndex: -1,
		done:      make(chan struct{}),
	}
}

func TestReservationHeapOrdersByGrantAt(t *testing.T) {
	t.Parallel()

	h := &reservationHeap{}
	base := testEpoch
	for _, d := range []time.Duration{3, 1, 4, 1, 5} {
		heap.Push(h, newTestReservation(0, base.Add(d*time.Second)))
	}
	require.Equal(t, 5, h.Len())

	var prev time.Time
	for h.Len() > 0 {
		r := heap.Pop(h).(*reservation)
		require.False(t, r.grantAt.Before(prev))
		prev = r.grantAt
	}
}

func TestReservationHeapBreaksTiesBySeq(t *testing.T) {
	t.Parallel()

	h := &reservationHeap{}
	grantAt := testEpoch.Add(time.Second)
	// push out of order; pop order must still be by sequence.
	for seq := uint64(5); seq > 0; seq-- {
		heap.Push(h, newTestReservation(seq, grantAt))
	}
	for want := uint64(1); want <= 5; want++ {
		r := heap.Pop(h).(*reservation)
		require.Equal(t, want, r.seq)
	}
}

func TestReservationHeapTracksIndex(t *testing.T) {
	t.Parallel()

	h := &reservationHeap{}
	rs := make([]*reservation, 5)
	for i := range rs {
		rs[i] = newTestReservation(uint64(i), testEpoch.Add(time.Duration(i)*time.Second))
		heap.Push(h, rs[i])
	}
	for i, r := range *h {
		require.Equal(t, i, r.heapIndex)
	}

	// removing a middle element keeps indexes consistent
	heap.Remove(h, rs[2].heapIndex)
	require.Equal(t, -1, rs[2].heapIndex)
	require.Equal(t, 4, h.Len())
	for i, r := range *h {
		require.Equal(t, i, r.heapIndex)
	}

	// the heap still pops in order
	var prev time.Time
	for h.Len() > 0 {
		r := heap.Pop(h).(*reservation)
		require.False(t, r.grantAt.Before(prev))
		prev = r.grantAt
	}
}

func TestReservationHeapPopClearsIndex(t *testing.T) {
	t.Parallel()

	h := &reservationHeap{}
	r := newTestReservation(1, testEpoch)
	heap.Push(h, r)
	require.Equal(t, 0, r.heapIndex)
	heap.Pop(h)
	require.Equal(t, -1, r.heapIndex)
	require.Equal(t, 0, h.Len())
}

func TestReservationHeapFix(t *testing.T) {
	t.Parallel()

	h := &reservationHeap{}
	a := newTestReservation(1, testEpoch.Add(time.Second))
	b := newTestReservation(2, testEpoch.Add(2*time.Second))
	c := newTestReservation(3, testEpoch.Add(3*time.Second))
	heap.Push(h, a)
	heap.Push(h, b)
	heap.Push(h, c)

	// delaying the head reorders the heap
	a.grantAt = testEpoch.Add(10 * time.Second)
	heap.Fix(h, a.heapIndex)

	for _, want := range []*reservation{b, c, a} {
		require.Same(t, want, heap.Pop(h).(*reservation))
	}
}
