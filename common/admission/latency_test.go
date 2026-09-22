package admission

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func newTestTracker(t *testing.T, ts clock.TimeSource) *LatencyTracker {
	t.Helper()
	tr, err := NewLatencyTracker(0.1, 0.01, time.Minute, ts)
	require.NoError(t, err)
	return tr
}

func TestNewLatencyTracker_ValidatesArgs(t *testing.T) {
	ts := clock.NewEventTimeSource()
	_, err := NewLatencyTracker(0, 0.01, time.Minute, ts)
	require.ErrorIs(t, err, ErrAlphaRange)
	_, err = NewLatencyTracker(1.5, 0.01, time.Minute, ts)
	require.ErrorIs(t, err, ErrAlphaRange)
	_, err = NewLatencyTracker(0.1, 0.01, 0, ts)
	require.ErrorIs(t, err, ErrMinRefreshNotPositive)
	_, err = NewLatencyTracker(0.1, 0.01, -time.Second, ts)
	require.ErrorIs(t, err, ErrMinRefreshNotPositive)
	_, err = NewLatencyTracker(0.1, 0, time.Minute, ts)
	require.ErrorIs(t, err, ErrEpsilonRange)
}

func TestLatencyTracker_EmptyBeforeFirstSample(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	require.Zero(t, tr.RTT())
	require.Zero(t, tr.MinRTT())
	require.Zero(t, tr.Quantile(0.5))
	require.Zero(t, tr.Observations())
}

func TestLatencyTracker_EWMAConverges(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(0.5, 0.01, time.Minute, ts)
	require.NoError(t, err)

	tr.Record(100 * time.Millisecond)
	require.Equal(t, 100*time.Millisecond, tr.RTT())

	// with alpha=0.5 the EWMA halves the distance to each new sample
	tr.Record(200 * time.Millisecond)
	require.Equal(t, 150*time.Millisecond, tr.RTT())
	tr.Record(200 * time.Millisecond)
	require.Equal(t, 175*time.Millisecond, tr.RTT())

	for i := 0; i < 30; i++ {
		tr.Record(200 * time.Millisecond)
	}
	require.InDelta(t, 200, tr.RTT().Milliseconds(), 1)
}

func TestLatencyTracker_MinRTTTracksLowestSample(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)

	tr.Record(50 * time.Millisecond)
	tr.Record(30 * time.Millisecond)
	tr.Record(80 * time.Millisecond)
	require.Equal(t, 30*time.Millisecond, tr.MinRTT())
}

func TestLatencyTracker_MinRTTAdaptsUpwardAfterRefresh(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(0.5, 0.01, 10*time.Second, ts)
	require.NoError(t, err)

	tr.Record(10 * time.Millisecond)
	require.Equal(t, 10*time.Millisecond, tr.MinRTT())

	// the service genuinely slows: nothing in the next interval approaches
	// the old minimum
	ts.Advance(11 * time.Second)
	for i := 0; i < 5; i++ {
		tr.Record(100 * time.Millisecond)
	}
	// the baseline is still standing: it can only be replaced once a full
	// interval of slower samples has completed
	require.Equal(t, 10*time.Millisecond, tr.MinRTT())
	ts.Advance(11 * time.Second)
	tr.Record(100 * time.Millisecond)
	require.Equal(t, 100*time.Millisecond, tr.MinRTT())
}

func TestLatencyTracker_MinRTTSurvivesWhileIntervalIsOpen(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(0.5, 0.01, 10*time.Second, ts)
	require.NoError(t, err)

	tr.Record(10 * time.Millisecond)
	ts.Advance(5 * time.Second) // inside the refresh interval
	for i := 0; i < 5; i++ {
		tr.Record(100 * time.Millisecond)
	}
	// a lower baseline still stands until the interval closes
	require.Equal(t, 10*time.Millisecond, tr.MinRTT())
}

func TestLatencyTracker_IgnoresNegativeDurations(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	tr.Record(-time.Second)
	require.Zero(t, tr.Observations())
	require.Zero(t, tr.RTT())
}

func TestLatencyTracker_QuantileTracksDistribution(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr := newTestTracker(t, ts)
	for i := 1; i <= 1000; i++ {
		tr.Record(time.Duration(i) * time.Millisecond)
	}
	require.Equal(t, 1000, tr.Observations())
	p50 := tr.Quantile(0.5)
	require.InDelta(t, 500, p50.Milliseconds(), 100)
	p99 := tr.Quantile(0.99)
	require.InDelta(t, 990, p99.Milliseconds(), 100)
}

func TestLatencyTracker_RefreshSkippedIntervals(t *testing.T) {
	ts := clock.NewEventTimeSource()
	tr, err := NewLatencyTracker(0.5, 0.01, 10*time.Second, ts)
	require.NoError(t, err)

	tr.Record(10 * time.Millisecond)
	// jump past several refresh intervals at once; the tracker must treat
	// the gap as elapsed time and adopt the new baseline
	ts.Advance(35 * time.Second)
	for i := 0; i < 3; i++ {
		tr.Record(60 * time.Millisecond)
	}
	require.Equal(t, 10*time.Millisecond, tr.MinRTT())
	ts.Advance(10 * time.Second)
	tr.Record(60 * time.Millisecond)
	require.Equal(t, 60*time.Millisecond, tr.MinRTT())
}
