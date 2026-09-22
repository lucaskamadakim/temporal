package adaptive

import (
	"math"
	"time"
)

// tokenBucket is a token bucket whose balance is allowed to go negative.
// A negative balance represents debt: tokens already committed to scheduled
// reservations that have not yet accrued.
type tokenBucket struct {
	rate     float64   // refill rate in tokens per second
	capacity float64   // maximum balance; bounds instantaneous burst
	tokens   float64   // current balance, negative when in debt
	last     time.Time // last time tokens was updated
}

// newTokenBucket returns a bucket starting full.
func newTokenBucket(rate float64, capacity float64, now time.Time) *tokenBucket {
	return &tokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
		last:     now,
	}
}

// tokensAt returns the balance at the given time, capped at capacity.
func (b *tokenBucket) tokensAt(now time.Time) float64 {
	if now.Before(b.last) {
		return b.tokens
	}
	if b.tokens >= b.capacity {
		return b.capacity
	}
	if b.rate <= 0 {
		return b.tokens
	}
	t := b.tokens + now.Sub(b.last).Seconds()*b.rate
	if t > b.capacity {
		t = b.capacity
	}
	return t
}

// charge deducts n tokens at the given time. The balance may go negative,
// which represents tokens committed to reservations scheduled in the future.
func (b *tokenBucket) charge(now time.Time, n float64) {
	b.tokens = b.tokensAt(now) - n
	b.last = now
}

// restore returns n tokens to the bucket, e.g. when a reservation is canceled.
// The balance never exceeds capacity.
func (b *tokenBucket) restore(now time.Time, n float64) {
	b.tokens = math.Min(b.capacity, b.tokensAt(now)+n)
	b.last = now
}

// earliest returns the earliest time at which n tokens will be available if
// they are charged now, honoring already-committed debt. It returns the zero
// time when the balance can never reach n (rate is 0 and the bucket is in
// debt beyond n).
func (b *tokenBucket) earliest(now time.Time, n float64) time.Time {
	if n <= 0 {
		return now
	}
	t := b.tokensAt(now)
	if t >= n {
		return now
	}
	if b.rate <= 0 {
		return time.Time{}
	}
	return now.Add(delayFromDeficit(n-t, b.rate))
}

// setRate updates the refill rate, snapshotting the balance first so accrued
// time is not lost.
func (b *tokenBucket) setRate(rate float64, now time.Time) {
	b.tokens = b.tokensAt(now)
	b.last = now
	b.rate = rate
}

// setCapacity updates the burst capacity, snapshotting the balance first.
func (b *tokenBucket) setCapacity(capacity float64, now time.Time) {
	b.tokens = b.tokensAt(now)
	b.last = now
	b.capacity = capacity
	if b.tokens > capacity {
		b.tokens = capacity
	}
}

// utilization returns the fraction of capacity currently committed, in [0, 1].
func (b *tokenBucket) utilization(now time.Time) float64 {
	if b.capacity <= 0 {
		return 0
	}
	u := 1 - b.tokensAt(now)/b.capacity
	return math.Max(0, math.Min(1, u))
}

// delayFromDeficit returns the time to accrue deficit tokens at the given rate.
func delayFromDeficit(deficit float64, rate float64) time.Duration {
	if rate <= 0 {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(deficit / rate * float64(time.Second))
}
