package admission

import (
	"errors"
	"math"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

var (
	ErrAlphaRange            = errors.New("admission: EWMA alpha must be in (0, 1]")
	ErrMinRefreshNotPositive = errors.New("admission: min RTT refresh interval must be positive")
)

// LatencyTracker tracks request latencies as three complementary signals:
//
//   - an exponential moving average (EWMA) of recent latencies, which reacts
//     quickly to a rising tail;
//   - a long-term minimum RTT that estimates the unloaded service time and is
//     refreshed on an interval so that it can adapt upward when the service
//     genuinely becomes slower;
//   - an epsilon-approximate quantile summary for tail-latency queries.
//
// A LatencyTracker is safe for concurrent use.
type LatencyTracker struct {
	mu          sync.Mutex
	timeSource  clock.TimeSource
	alpha       float64
	ewma        float64 // nanoseconds
	summary     *QuantileSummary
	refresh     time.Duration
	intervalMin float64 // lowest latency seen in the current refresh interval
	minRTT      float64 // refreshed estimate of the unloaded RTT
	nextRefresh time.Time
	initialized bool
}

// NewLatencyTracker returns a tracker whose EWMA mixes each new sample with
// weight alpha, whose quantile summary carries rank error epsilon, and whose
// minimum-RTT estimate is refreshed every minRefresh.
func NewLatencyTracker(
	alpha float64,
	epsilon float64,
	minRefresh time.Duration,
	timeSource clock.TimeSource,
) (*LatencyTracker, error) {
	if alpha <= 0 || alpha > 1 {
		return nil, ErrAlphaRange
	}
	if minRefresh <= 0 {
		return nil, ErrMinRefreshNotPositive
	}
	summary, err := NewQuantileSummary(epsilon)
	if err != nil {
		return nil, err
	}
	return &LatencyTracker{
		timeSource:  timeSource,
		alpha:       alpha,
		summary:     summary,
		refresh:     minRefresh,
		intervalMin: -1,
		minRTT:      -1,
	}, nil
}

// Record adds one latency observation.
func (t *LatencyTracker) Record(d time.Duration) {
	if d < 0 {
		return
	}
	nanos := float64(d)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maybeRefresh(t.timeSource.Now())
	if !t.initialized {
		t.ewma = nanos
		t.initialized = true
	} else {
		t.ewma = t.alpha*nanos + (1-t.alpha)*t.ewma
	}
	t.summary.Insert(nanos)
	if t.intervalMin < 0 || nanos < t.intervalMin {
		t.intervalMin = nanos
	}
	if t.minRTT < 0 || nanos < t.minRTT {
		t.minRTT = nanos
	}
}

// RTT returns the current short-term latency estimate (the EWMA). It returns
// 0 before the first observation.
func (t *LatencyTracker) RTT() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return time.Duration(t.ewma)
}

// MinRTT returns the current estimate of the unloaded round-trip time. It
// returns 0 before the first observation.
func (t *LatencyTracker) MinRTT() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.minRTT < 0 {
		return 0
	}
	return time.Duration(t.minRTT)
}

// Quantile returns the q-quantile of all recorded latencies, or 0 when the
// summary is empty.
func (t *LatencyTracker) Quantile(q float64) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.summary.Query(q)
	if math.IsNaN(v) {
		return 0
	}
	return time.Duration(v)
}

// Observations returns the number of recorded latencies.
func (t *LatencyTracker) Observations() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.summary.Count()
}

// maybeRefresh rolls the refresh interval forward. The minimum seen during
// the interval that just ended becomes the new unloaded-RTT estimate unless
// the tracker has already observed a lower value. Advancing the estimate in
// this direction is what lets MinRTT adapt upward: without the refresh, a
// single fast request early in the process lifetime would pin the estimate
// forever.
//
// The caller must hold t.mu.
func (t *LatencyTracker) maybeRefresh(now time.Time) {
	if t.nextRefresh.IsZero() {
		t.nextRefresh = now.Add(t.refresh)
		return
	}
	if now.Before(t.nextRefresh) {
		return
	}
	// Align the next refresh to a boundary relative to now rather than
	// accumulating drift one interval at a time.
	missed := 1 + int(now.Sub(t.nextRefresh)/t.refresh)
	t.nextRefresh = t.nextRefresh.Add(time.Duration(missed) * t.refresh)
	if t.intervalMin >= 0 && (t.minRTT < 0 || t.intervalMin > t.minRTT) {
		// The whole interval produced nothing faster than the standing
		// estimate, so the service has genuinely slowed; adopt the
		// interval's minimum as the new baseline.
		t.minRTT = t.intervalMin
	}
	t.intervalMin = -1
}
