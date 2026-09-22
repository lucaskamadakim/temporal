package adaptive

import (
	"cmp"
	"container/heap"
	"slices"
	"time"

	"go.temporal.io/server/common/clock"
)

// partition is the per-tenant-key scheduling state. It owns a token bucket
// and a min-heap of scheduled reservations granted by a timer as their grant
// times arrive.
type partition struct {
	limiter *Limiter
	key     string
	bucket  *tokenBucket
	pending reservationHeap

	grantTimer clock.Timer // armed for the heap head's grant time
	idleTimer  clock.Timer // armed for idle eviction
	lastUsed   time.Time
	evicted    bool
}

func newPartition(l *Limiter, key string, now time.Time) *partition {
	p := &partition{
		limiter:  l,
		key:      key,
		bucket:   newTokenBucket(l.effectiveRate(), float64(l.cfg.PartitionBurst), now),
		lastUsed: now,
	}
	p.idleTimer = l.clock.AfterFunc(l.cfg.PartitionIdleTTL, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !l.closed {
			l.evictPartition(p)
		}
	})
	return p
}

// touch records activity on the partition and postpones idle eviction.
func (p *partition) touch(now time.Time) {
	p.lastUsed = now
	if p.idleTimer != nil {
		p.idleTimer.Reset(p.limiter.cfg.PartitionIdleTTL)
	}
}

// scheduleAt returns the earliest grant time for n tokens at the given
// priority weight, honoring debt already committed to pending reservations.
// The second return value is false when no grant time within MaxWait exists.
func (p *partition) scheduleAt(now time.Time, n int, weight float64) (time.Time, bool) {
	deficit := float64(n) - p.bucket.tokensAt(now)
	if deficit <= 0 {
		return now, true
	}
	rate := p.bucket.rate * weight
	if rate <= 0 {
		return time.Time{}, false
	}
	d := delayFromDeficit(deficit, rate)
	if d > p.limiter.cfg.MaxWait {
		return time.Time{}, false
	}
	return now.Add(d), true
}

// reserve creates a reservation for n tokens. It is called with the limiter
// lock held. The returned reservation is either granted (grant time already
// reached), scheduled (queued for a future grant time), or denied (OK() ==
// false).
func (p *partition) reserve(now time.Time, n int, weight float64, seq uint64) *reservation {
	r := &reservation{
		limiter:   p.limiter,
		partition: p,
		tokens:    n,
		weight:    weight,
		seq:       seq,
		createdAt: now,
		heapIndex: -1,
		done:      make(chan struct{}),
	}
	grantAt, ok := p.scheduleAt(now, n, weight)
	if !ok {
		r.ok = false
		close(r.done)
		return r
	}
	r.ok = true
	r.grantAt = grantAt
	p.bucket.charge(now, float64(n))
	if !grantAt.After(now) {
		r.granted = true
		close(r.done)
		return r
	}
	heap.Push(&p.pending, r)
	p.rearm(now)
	return r
}

// grantDue grants every reservation whose grant time has been reached and
// rearms the timer for the next pending grant. Called with the limiter lock
// held.
func (p *partition) grantDue(now time.Time) {
	for p.pending.Len() > 0 {
		head := p.pending[0]
		if head.grantAt.After(now) {
			break
		}
		heap.Pop(&p.pending)
		head.granted = true
		close(head.done)
		p.limiter.metrics.recordGrantLatency(now.Sub(head.createdAt))
	}
	p.rearm(now)
}

// retimeline recomputes the grant times of all pending reservations after the
// partition's effective rate changes or after debt is returned by a cancel,
// so the schedule reflects the bucket's current state while preserving the
// original FIFO grant order.
func (p *partition) retimeline(now time.Time) {
	if p.pending.Len() == 0 {
		return
	}
	// Find how far into the committed schedule the bucket has already
	// accrued: balance plus committed debt is the repaid progress, which
	// shifts the whole schedule earlier when positive.
	var committed float64
	for _, r := range p.pending {
		committed += float64(r.tokens)
	}
	cursor := now
	if repaid := p.bucket.tokensAt(now) + committed; repaid > 0 && p.bucket.rate > 0 {
		cursor = now.Add(-delayFromDeficit(repaid, p.bucket.rate))
	}
	// Recompute each reservation's grant time in grant order; the heap's
	// backing array is only partially ordered, so iterating it directly would
	// assign earlier slots to reservations that grant later.
	ordered := slices.Clone(p.pending)
	slices.SortFunc(ordered, func(a, b *reservation) int {
		if a.grantAt.Equal(b.grantAt) {
			return cmp.Compare(a.seq, b.seq)
		}
		return a.grantAt.Compare(b.grantAt)
	})
	for _, r := range ordered {
		grantAt := cursor.Add(delayFor(r.tokens, p.bucket.rate, r.weight))
		if grantAt.Before(now) {
			grantAt = now
		}
		r.grantAt = grantAt
		cursor = grantAt
	}
	heap.Init(&p.pending)
	p.grantDue(now)
	p.rearm(now)
}

// rearm sets the grant timer for the heap head's grant time, or stops it when
// no reservations are pending. Called with the limiter lock held.
func (p *partition) rearm(now time.Time) {
	if p.pending.Len() == 0 {
		if p.grantTimer != nil {
			p.grantTimer.Stop()
			p.grantTimer = nil
		}
		return
	}
	d := p.pending[0].grantAt.Sub(now)
	if d < 0 {
		d = 0
	}
	if p.grantTimer == nil {
		p.grantTimer = p.limiter.clock.AfterFunc(d, func() {
			l := p.limiter
			l.mu.Lock()
			defer l.mu.Unlock()
			if !l.closed {
				p.grantDue(l.clock.Now())
			}
		})
		return
	}
	p.grantTimer.Reset(d)
}

// evict force-cancels all pending reservations and releases the partition's
// timers. Called with the limiter lock held; the caller removes the partition
// from the map.
func (p *partition) evict() {
	p.evicted = true
	for _, r := range p.pending {
		r.canceled = true
		r.heapIndex = -1
		select {
		case <-r.done:
		default:
			close(r.done)
		}
	}
	p.pending = nil
	if p.grantTimer != nil {
		p.grantTimer.Stop()
		p.grantTimer = nil
	}
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
}
