package admission

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func newTestLimiter(t *testing.T, tracker *LatencyTracker, ts clock.TimeSource) *GradientLimiter {
	t.Helper()
	l, err := NewGradientLimiter(
		4,   // minLimit
		200, // maxLimit
		20,  // initialLimit
		1.0, // smoothing: adopt the target immediately for deterministic tests
		0.9, // rttMinMultiplier
		0.9, // backoffRatio
		8,   // queueSize
		time.Second,
		tracker,
		ts,
	)
	require.NoError(t, err)
	return l
}

func TestNewGradientLimiter_ValidatesArgs(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(0.5, 0.01, time.Minute, ts)
	require.NoError(t, err)

	_, err = NewGradientLimiter(0, 10, 5, 1, 0.9, 0.9, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrLimitBounds)
	_, err = NewGradientLimiter(10, 5, 7, 1, 0.9, 0.9, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrLimitBounds)
	_, err = NewGradientLimiter(4, 200, 500, 1, 0.9, 0.9, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrInitialLimit)
	_, err = NewGradientLimiter(4, 200, 20, 0, 0.9, 0.9, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrSmoothingRange)
	_, err = NewGradientLimiter(4, 200, 20, 1, 1.5, 0.9, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrMultiplierRange)
	_, err = NewGradientLimiter(4, 200, 20, 1, 0.9, 1.2, 8, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrBackoffRange)
	_, err = NewGradientLimiter(4, 200, 20, 1, 0.9, 0.9, -1, time.Second, tr, ts)
	require.ErrorIs(t, err, ErrQueueNegative)
	_, err = NewGradientLimiter(4, 200, 20, 1, 0.9, 0.9, 8, 0, tr, ts)
	require.ErrorIs(t, err, ErrUpdateInterval)
}

func TestGradientLimiter_AcquireUpToLimit(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	require.Equal(t, 20, l.Limit())
	var releases []func(Outcome)
	for i := 0; i < 20; i++ {
		release, ok := l.TryAcquire()
		require.True(t, ok, "acquire %d should succeed", i)
		releases = append(releases, release)
	}
	require.Equal(t, 20, l.InFlight())

	_, ok := l.TryAcquire()
	require.False(t, ok)

	releases[0](OutcomeSuccess)
	require.Equal(t, 19, l.InFlight())
	_, ok = l.TryAcquire()
	require.True(t, ok)
}

func TestGradientLimiter_ReleaseIsIdempotent(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	release, ok := l.TryAcquire()
	require.True(t, ok)
	release(OutcomeSuccess)
	release(OutcomeSuccess)
	require.Equal(t, 0, l.InFlight())
}

func TestGradientLimiter_GrowsWhenHealthy(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(1.0, 0.01, time.Minute, ts)
	require.NoError(t, err)
	l := newTestLimiter(t, tr, ts)

	// flat latency: rtt == minRTT so gradient = rttMinMultiplier = 0.9;
	// target = limit*0.9 + 8 -> limit creeps toward the fixed point near 80
	tr.Record(100 * time.Millisecond)
	ts.Advance(1100 * time.Millisecond)
	_, ok := l.TryAcquire()
	require.True(t, ok)
	require.InDelta(t, 20*0.9+8, float64(l.Limit()), 0.6) // ~26

	ts.Advance(1100 * time.Millisecond)
	_, ok = l.TryAcquire()
	require.True(t, ok)
	require.InDelta(t, 26*0.9+8, float64(l.Limit()), 2.0) // ~31.4
}

func TestGradientLimiter_ShrinksWhenLatencyRises(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(1.0, 0.01, time.Minute, ts)
	require.NoError(t, err)
	l := newTestLimiter(t, tr, ts)

	tr.Record(50 * time.Millisecond) // establish a low baseline
	ts.Advance(1100 * time.Millisecond)
	_, ok := l.TryAcquire()
	require.True(t, ok)

	// latency quadruples; gradient = clamp(0.5, 1, 0.9*50/200) = 0.5
	tr.Record(200 * time.Millisecond)
	ts.Advance(1100 * time.Millisecond)
	_, ok = l.TryAcquire()
	require.True(t, ok)
	// limit was ~26 after the first update; halving it and adding the
	// queue term gives ~21
	require.InDelta(t, 26*0.5+8, float64(l.Limit()), 0.6)
}

func TestGradientLimiter_UpdateIntervalGatesRecompute(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(1.0, 0.01, time.Minute, ts)
	require.NoError(t, err)
	l := newTestLimiter(t, tr, ts)

	// the first acquire seeds nextUpdate; with no samples yet the limit
	// does not move
	_, ok := l.TryAcquire()
	require.True(t, ok)
	require.Equal(t, 20, l.Limit())

	tr.Record(50 * time.Millisecond)
	// inside the same update interval the limit must not move even though
	// latency doubled
	ts.Advance(500 * time.Millisecond)
	tr.Record(100 * time.Millisecond)
	release, ok := l.TryAcquire()
	require.True(t, ok)
	release(OutcomeSuccess)
	require.Equal(t, 20, l.Limit())
}

func TestGradientLimiter_FailureBackoffImmediate(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	release, ok := l.TryAcquire()
	require.True(t, ok)
	release(OutcomeFailure)
	require.Equal(t, 18, l.Limit()) // 20 * 0.9

	release, ok = l.TryAcquire()
	require.True(t, ok)
	release(OutcomeFailure)
	require.Equal(t, 16, l.Limit()) // 18 * 0.9 = 16.2
}

func TestGradientLimiter_FailureBackoffFloorsAtMin(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	for i := 0; i < 40; i++ {
		release, ok := l.TryAcquire()
		if !ok {
			break
		}
		release(OutcomeFailure)
	}
	require.Equal(t, 4, l.Limit()) // minLimit floor
}

func TestGradientLimiter_IgnoredOutcomeOnlyReleases(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	release, ok := l.TryAcquire()
	require.True(t, ok)
	release(OutcomeIgnore)
	require.Equal(t, 0, l.InFlight())
	require.Equal(t, 20, l.Limit()) // unchanged
}

func TestGradientLimiter_SetBoundsClampsLimit(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	l := newTestLimiter(t, tr, ts)

	l.SetBounds(30, 60)
	require.Equal(t, 30, l.Limit())

	// invalid bounds are rejected
	l.SetBounds(70, 10)
	require.Equal(t, 30, l.Limit())
	l.SetBounds(5, 15)
	require.Equal(t, 15, l.Limit())
}
