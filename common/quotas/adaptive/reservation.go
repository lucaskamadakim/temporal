package adaptive

import (
	"container/heap"
	"time"

	"go.temporal.io/server/common/quotas"
)

type (
	// reservation tracks a request for tokens. A reservation is either
	// immediate (granted at creation), scheduled (pending in the partition's
	// heap until its grant time), or denied (wait would exceed MaxWait).
	reservation struct {
		limiter   *Limiter
		partition *partition
		tokens    int
		weight    float64
		seq       uint64

		createdAt time.Time
		grantAt   time.Time
		heapIndex int // position in partition.pending, or -1

		ok       bool // false when denied
		granted  bool
		canceled bool
		done     chan struct{} // closed when granted or canceled
	}

	// reservationHeap is a min-heap of pending reservations ordered by grant
	// time, then by arrival sequence.
	reservationHeap []*reservation
)

var _ quotas.Reservation = (*reservation)(nil)
var _ heap.Interface = (*reservationHeap)(nil)

// OK implements quotas.Reservation. It returns false when the reservation was
// denied because its wait would exceed Config.MaxWait.
func (r *reservation) OK() bool {
	r.limiter.mu.Lock()
	defer r.limiter.mu.Unlock()
	return r.ok
}

// Cancel implements quotas.Reservation.
func (r *reservation) Cancel() {
	r.CancelAt(r.limiter.clock.Now())
}

// CancelAt implements quotas.Reservation. It returns the reservation's tokens
// to the partition bucket unless they were already consumed.
func (r *reservation) CancelAt(now time.Time) {
	r.limiter.mu.Lock()
	defer r.limiter.mu.Unlock()
	if r.canceled || !r.ok {
		return
	}
	r.canceled = true
	if r.heapIndex >= 0 {
		heap.Remove(&r.partition.pending, r.heapIndex)
		r.heapIndex = -1
	}
	if !r.granted {
		// The tokens were committed as debt and never consumed; return them
		// and pull the remaining queue earlier.
		r.partition.bucket.restore(now, float64(r.tokens))
		r.partition.retimeline(now)
	}
	r.limiter.metrics.recordCanceled()
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}

// Delay implements quotas.Reservation.
func (r *reservation) Delay() time.Duration {
	return r.DelayFrom(r.limiter.clock.Now())
}

// DelayFrom implements quotas.Reservation. Denied reservations report
// quotas.InfDuration; granted and canceled reservations report 0.
func (r *reservation) DelayFrom(now time.Time) time.Duration {
	r.limiter.mu.Lock()
	defer r.limiter.mu.Unlock()
	if !r.ok {
		return quotas.InfDuration
	}
	if r.granted || r.canceled {
		return 0
	}
	if d := r.grantAt.Sub(now); d > 0 {
		return d
	}
	return 0
}

// Len implements heap.Interface.
func (h reservationHeap) Len() int { return len(h) }

// Less implements heap.Interface. Reservations grant in grant-time order;
// ties break on arrival sequence so FIFO order is preserved.
func (h reservationHeap) Less(i, j int) bool {
	if h[i].grantAt.Equal(h[j].grantAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].grantAt.Before(h[j].grantAt)
}

// Swap implements heap.Interface.
func (h reservationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

// Push implements heap.Interface.
func (h *reservationHeap) Push(x any) {
	r, ok := x.(*reservation)
	if !ok {
		return
	}
	r.heapIndex = len(*h)
	*h = append(*h, r)
}

// Pop implements heap.Interface.
func (h *reservationHeap) Pop() any {
	old := *h
	n := len(old)
	r := old[n-1]
	old[n-1] = nil
	r.heapIndex = -1
	*h = old[:n-1]
	return r
}
