package admission

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

// Kind enumerates the outcome counters maintained per bucket.
type Kind int

const (
	// KindSuccess counts requests that completed without an error that the
	// caller classified as a failure.
	KindSuccess Kind = iota
	// KindFailure counts requests that completed with a failure outcome.
	KindFailure
	// KindRejected counts requests that were shed by the controller before
	// being admitted.
	KindRejected
	numKinds
)

var (
	ErrWindowNotPositive      = errors.New("admission: window duration must be positive")
	ErrBucketCountNotPositive = errors.New("admission: bucket count must be positive")
)

// String returns a short label for the kind, suitable for metric tags.
func (k Kind) String() string {
	switch k {
	case KindSuccess:
		return "success"
	case KindFailure:
		return "failure"
	case KindRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// bucket accumulates the counters for a single slice of the window.
type bucket struct {
	counts       [numKinds]int64
	latencyNanos int64
	latencyCount int64
}

func (b *bucket) reset() {
	*b = bucket{}
}

// SlidingWindow maintains per-kind counters over a fixed time window that is
// subdivided into a ring of equally sized buckets. Buckets are rotated lazily:
// bookkeeping happens on read and write paths rather than on a background
// timer, so an idle window costs nothing.
//
// A SlidingWindow is safe for concurrent use.
type SlidingWindow struct {
	mu          sync.Mutex
	timeSource  clock.TimeSource
	bucketWidth time.Duration
	buckets     []bucket
	head        int
	headTime    time.Time
}

// NewSlidingWindow returns a window of the given total duration split into
// numBuckets buckets. The window is aligned to bucketWidth boundaries of the
// supplied time source's clock so that bucket boundaries are stable across
// processes that share a wall clock.
func NewSlidingWindow(
	window time.Duration,
	numBuckets int,
	timeSource clock.TimeSource,
) (*SlidingWindow, error) {
	if window <= 0 {
		return nil, ErrWindowNotPositive
	}
	if numBuckets <= 0 {
		return nil, ErrBucketCountNotPositive
	}
	bucketWidth := window / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		return nil, fmt.Errorf("%w: %d buckets span %s", ErrBucketCountNotPositive, numBuckets, window)
	}
	return &SlidingWindow{
		timeSource:  timeSource,
		bucketWidth: bucketWidth,
		buckets:     make([]bucket, numBuckets),
		headTime:    timeSource.Now().Truncate(bucketWidth),
	}, nil
}

// Window returns the total duration covered by all buckets.
func (w *SlidingWindow) Window() time.Duration {
	return w.bucketWidth * time.Duration(len(w.buckets))
}

// NumBuckets returns the number of buckets in the ring.
func (w *SlidingWindow) NumBuckets() int {
	return len(w.buckets)
}

// Record adds n to the counter for kind in the current bucket.
func (w *SlidingWindow) Record(kind Kind, n int64) {
	if kind < 0 || kind >= numKinds || n == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	w.buckets[w.head].counts[kind] += n
}

// RecordLatency adds a latency observation to the current bucket. Latencies
// are tracked separately from the per-kind counters because they are
// aggregated as a mean rather than a ratio.
func (w *SlidingWindow) RecordLatency(d time.Duration) {
	if d < 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	w.buckets[w.head].latencyNanos += d.Nanoseconds()
	w.buckets[w.head].latencyCount++
}

// Count returns the number of events recorded for kind across the live
// window.
func (w *SlidingWindow) Count(kind Kind) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	var total int64
	for i := range w.buckets {
		total += w.buckets[i].counts[kind]
	}
	return total
}

// Counts returns a snapshot of all per-kind counters across the live window.
// A single pass is cheaper than calling Count once per kind.
func (w *SlidingWindow) Counts() [numKinds]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	var totals [numKinds]int64
	for i := range w.buckets {
		for k := Kind(0); k < numKinds; k++ {
			totals[k] += w.buckets[i].counts[k]
		}
	}
	return totals
}

// Requests returns the number of admitted requests (successes plus failures)
// in the live window. Rejected requests are excluded so that a controller
// cannot count its own rejections toward the minimum request gate.
func (w *SlidingWindow) Requests() int64 {
	counts := w.Counts()
	return counts[KindSuccess] + counts[KindFailure]
}

// Ratio returns count(kind) / count(base...). It returns 0 when the
// denominator is zero, rather than +Inf, so callers can compare the result
// directly against a threshold.
func (w *SlidingWindow) Ratio(kind Kind, base ...Kind) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	var num, den int64
	for i := range w.buckets {
		num += w.buckets[i].counts[kind]
		for _, b := range base {
			den += w.buckets[i].counts[b]
		}
	}
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// FailureRatio is shorthand for Ratio(KindFailure, KindSuccess, KindFailure).
func (w *SlidingWindow) FailureRatio() float64 {
	return w.Ratio(KindFailure, KindSuccess, KindFailure)
}

// MeanLatency returns the mean recorded latency across the live window. It
// returns 0 when no latency observations exist.
func (w *SlidingWindow) MeanLatency() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.advance(w.timeSource.Now())
	var nanos, count int64
	for i := range w.buckets {
		nanos += w.buckets[i].latencyNanos
		count += w.buckets[i].latencyCount
	}
	if count == 0 {
		return 0
	}
	return time.Duration(nanos / count)
}

// advance rotates the ring so that the head bucket covers the time slice
// containing now. Buckets that the head moves past are cleared: each one is
// about to become the newest bucket of the live window. Time that moves
// backwards (for example, an EventTimeSource that was rewound, or NTP skew)
// is ignored.
//
// The caller must hold w.mu.
func (w *SlidingWindow) advance(now time.Time) {
	elapsed := now.Sub(w.headTime)
	if elapsed < w.bucketWidth {
		return
	}
	steps := int(elapsed / w.bucketWidth)
	// Clear every bucket the head moves past, up to the bucket the head is
	// about to occupy. The head bucket itself is the live accumulator and
	// always carries data for the current slice, so at most
	// len(w.buckets)-1 buckets can ever need clearing.
	limit := min(steps, len(w.buckets)-1)
	for i := 1; i <= limit; i++ {
		w.buckets[(w.head+i)%len(w.buckets)].reset()
	}
	w.head = (w.head + steps) % len(w.buckets)
	w.headTime = w.headTime.Add(time.Duration(steps) * w.bucketWidth)
}
