package admission

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

var (
	ErrLimitBounds     = errors.New("admission: require 0 < minLimit <= maxLimit")
	ErrInitialLimit    = errors.New("admission: initial limit must lie in [minLimit, maxLimit]")
	ErrSmoothingRange  = errors.New("admission: smoothing must be in (0, 1]")
	ErrMultiplierRange = errors.New("admission: RTT min multiplier must be in (0, 1]")
	ErrBackoffRange    = errors.New("admission: backoff ratio must be in (0, 1)")
	ErrQueueNegative   = errors.New("admission: queue size must be non-negative")
	ErrUpdateInterval  = errors.New("admission: limiter update interval must be positive")
)

// Outcome describes how an admitted request completed, for limiter feedback.
type Outcome int

const (
	// OutcomeSuccess is a request that completed normally.
	OutcomeSuccess Outcome = iota
	// OutcomeFailure is a request that failed in a way that indicates the
	// service or a dependency is struggling. A failure shrinks the limit
	// multiplicatively.
	OutcomeFailure
	// OutcomeIgnore is a request that should not feed back into the limit,
	// for example one rejected by the caller's own validation.
	OutcomeIgnore
)

// GradientLimiter implements a latency-gradient concurrency limiter: it
// compares the short-term latency estimate with the estimated unloaded RTT
// and adjusts the in-flight cap in the direction that keeps the gradient
// near one. When latency is flat the limit grows additively by queueSize
// per update interval (probing for headroom); when latency rises the limit
// shrinks multiplicatively down to a hard floor.
//
// A GradientLimiter is safe for concurrent use.
type GradientLimiter struct {
	mu             sync.Mutex
	minLimit       int
	maxLimit       int
	limit          float64
	inFlight       int
	smoothing      float64
	rttMinMultiple float64
	backoffRatio   float64
	queueSize      float64
	updateInterval time.Duration
	nextUpdate     time.Time
	tracker        *LatencyTracker
	timeSource     clock.TimeSource
}

// NewGradientLimiter returns a limiter that reads its latency signals from
// tracker. The initial limit is initialLimit; it then floats within
// [minLimit, maxLimit].
func NewGradientLimiter(
	minLimit int,
	maxLimit int,
	initialLimit int,
	smoothing float64,
	rttMinMultiple float64,
	backoffRatio float64,
	queueSize float64,
	updateInterval time.Duration,
	tracker *LatencyTracker,
	timeSource clock.TimeSource,
) (*GradientLimiter, error) {
	switch {
	case minLimit <= 0 || maxLimit < minLimit:
		return nil, fmt.Errorf("%w: min=%d max=%d", ErrLimitBounds, minLimit, maxLimit)
	case initialLimit < minLimit || initialLimit > maxLimit:
		return nil, fmt.Errorf("%w: initial=%d", ErrInitialLimit, initialLimit)
	case smoothing <= 0 || smoothing > 1:
		return nil, ErrSmoothingRange
	case rttMinMultiple <= 0 || rttMinMultiple > 1:
		return nil, ErrMultiplierRange
	case backoffRatio <= 0 || backoffRatio >= 1:
		return nil, ErrBackoffRange
	case queueSize < 0:
		return nil, ErrQueueNegative
	case updateInterval <= 0:
		return nil, ErrUpdateInterval
	}
	return &GradientLimiter{
		minLimit:       minLimit,
		maxLimit:       maxLimit,
		limit:          float64(initialLimit),
		smoothing:      smoothing,
		rttMinMultiple: rttMinMultiple,
		backoffRatio:   backoffRatio,
		queueSize:      queueSize,
		updateInterval: updateInterval,
		tracker:        tracker,
		timeSource:     timeSource,
	}, nil
}

// TryAcquire takes one slot if in-flight is below the current limit and
// returns a release function that must be called with the request outcome.
// It returns ok = false without a release function when the limiter is
// saturated.
func (l *GradientLimiter) TryAcquire() (release func(Outcome), ok bool) {
	l.mu.Lock()
	l.maybeUpdateLocked(l.timeSource.Now())
	if float64(l.inFlight) >= l.limit {
		l.mu.Unlock()
		return nil, false
	}
	l.inFlight++
	l.mu.Unlock()

	var once sync.Once
	return func(outcome Outcome) {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.inFlight--
			if outcome == OutcomeFailure {
				l.limit = max(float64(l.minLimit), l.limit*l.backoffRatio)
			}
		})
	}, true
}

// Limit returns the current in-flight cap.
func (l *GradientLimiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.limit)
}

// InFlight returns the number of currently held slots.
func (l *GradientLimiter) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inFlight
}

// SetBounds replaces the limit bounds and clamps the current limit into the
// new range. It exists so dynamic configuration updates can retune a live
// limiter without reconstructing it.
func (l *GradientLimiter) SetBounds(minLimit, maxLimit int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if minLimit <= 0 || maxLimit < minLimit {
		return
	}
	l.minLimit = minLimit
	l.maxLimit = maxLimit
	l.limit = clampFloat(float64(minLimit), l.limit, float64(maxLimit))
}

// maybeUpdateLocked recomputes the limit once per updateInterval:
//
//	gradient = clamp(0.5, 1, rttMinMultiple * minRTT / rtt)
//	target   = limit * gradient + queueSize
//	limit   += (target - limit) * smoothing
//
// The gradient is floored at 0.5 so a single update cannot more than halve
// the limit; the additive queueSize term is what lets the limit keep
// growing when the service is healthy (gradient -> rttMinMultiple ~ 1 gives
// target = limit + queueSize).
//
// The caller must hold l.mu.
func (l *GradientLimiter) maybeUpdateLocked(now time.Time) {
	if now.Before(l.nextUpdate) {
		return
	}
	l.nextUpdate = now.Add(l.updateInterval)

	rtt := float64(l.tracker.RTT())
	minRTT := float64(l.tracker.MinRTT())
	if rtt <= 0 || minRTT <= 0 {
		return
	}
	gradient := clampFloat(0.5, l.rttMinMultiple*minRTT/rtt, 1.0)
	target := l.limit*gradient + l.queueSize
	l.limit = clampFloat(
		float64(l.minLimit),
		l.limit+(target-l.limit)*l.smoothing,
		float64(l.maxLimit),
	)
}

func clampFloat(lo, v, hi float64) float64 {
	return min(hi, max(lo, v))
}
