package adaptive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/await"
)

func newTestPartition(t *testing.T, cfg Config) (*Limiter, *partition, *clock.EventTimeSource) {
	t.Helper()
	ts := clock.NewEventTimeSource()
	ts.UseAsyncTimers(true)
	l, err := NewLimiter(cfg, WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)
	p := l.partitionFor("ns", ts.Now())
	return l, p, ts
}

func TestScheduleAtCovered(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	grantAt, ok := p.scheduleAt(ts.Now(), 10, 1.0)
	require.True(t, ok)
	require.Equal(t, ts.Now(), grantAt)
}

func TestScheduleAtDeficit(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	grantAt, ok := p.scheduleAt(ts.Now(), 10, 1.0)
	require.True(t, ok)
	require.Equal(t, ts.Now().Add(time.Second), grantAt)
}

func TestScheduleAtWeighted(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	grantAt, ok := p.scheduleAt(ts.Now(), 10, 0.5)
	require.True(t, ok)
	require.Equal(t, ts.Now().Add(2*time.Second), grantAt)
}

func TestScheduleAtDeniedBeyondMaxWait(t *testing.T) {
	t.Parallel()

	cfg := testConfigInternal()
	cfg.MaxWait = time.Second
	_, p, ts := newTestPartition(t, cfg)
	p.bucket.charge(ts.Now(), 10)
	_, ok := p.scheduleAt(ts.Now(), 100, 1.0)
	require.False(t, ok)
}

func TestScheduleAtZeroRate(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.setRate(0, ts.Now())
	p.bucket.charge(ts.Now(), 10)
	_, ok := p.scheduleAt(ts.Now(), 5, 1.0)
	require.False(t, ok)
}

func TestRetimelineOnRateChange(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	r1 := p.reserve(ts.Now(), 10, 1.0, 1)
	r2 := p.reserve(ts.Now(), 10, 1.0, 2)
	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))
	require.Equal(t, 2*time.Second, r2.DelayFrom(ts.Now()))

	// halving the rate doubles every pending grant's delay
	p.bucket.setRate(5, ts.Now())
	p.retimeline(ts.Now())
	require.Equal(t, 2*time.Second, r1.DelayFrom(ts.Now()))
	require.Equal(t, 4*time.Second, r2.DelayFrom(ts.Now()))

	// doubling the rate halves them again
	p.bucket.setRate(10, ts.Now())
	p.retimeline(ts.Now())
	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))
	require.Equal(t, 2*time.Second, r2.DelayFrom(ts.Now()))
}

func TestRetimelineEmptyIsNoop(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.retimeline(ts.Now())
	require.Equal(t, 0, p.pending.Len())
}

func TestRetimelineGrantsCoveredReservations(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	r1 := p.reserve(ts.Now(), 10, 1.0, 1)
	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))

	// returning tokens pulls the pending grant forward to now
	p.bucket.restore(ts.Now(), 10)
	p.retimeline(ts.Now())
	require.True(t, r1.granted)
	require.Equal(t, 0, p.pending.Len())
}

func TestGrantDueGrantOrder(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	r1 := p.reserve(ts.Now(), 10, 1.0, 1)
	r2 := p.reserve(ts.Now(), 10, 1.0, 2)

	ts.Advance(time.Second)
	await.RequireTrue(t, func() bool { return isGranted(r1) }, 10*time.Second, 10*time.Millisecond)
	require.False(t, isGranted(r2))

	ts.Advance(time.Second)
	await.RequireTrue(t, func() bool { return isGranted(r2) }, 10*time.Second, 10*time.Millisecond)
}

func TestGrantDueOnlyGrantsDue(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	r1 := p.reserve(ts.Now(), 10, 1.0, 1)
	r2 := p.reserve(ts.Now(), 20, 1.0, 2)

	ts.Advance(time.Second)
	await.RequireTrue(t, func() bool { return isGranted(r1) }, 10*time.Second, 10*time.Millisecond)
	require.False(t, isGranted(r2))
	require.Equal(t, 1, p.pending.Len())
}

func TestEvictCancelsPending(t *testing.T) {
	t.Parallel()

	_, p, ts := newTestPartition(t, testConfigInternal())
	p.bucket.charge(ts.Now(), 10)
	r := p.reserve(ts.Now(), 50, 1.0, 1)

	p.evict()
	select {
	case <-r.done:
	default:
		t.Fatal("reservation was not released on evict")
	}
	require.True(t, r.canceled)
	require.Equal(t, 0, p.pending.Len())
	require.True(t, p.evicted)
}

// isGranted reports whether the reservation's done channel is closed and it
// was granted. The granted write happens-before the close, so reading it
// after observing the close is safe.
func isGranted(r *reservation) bool {
	select {
	case <-r.done:
		return r.granted
	default:
		return false
	}
}

func testConfigInternal() Config {
	cfg := DefaultConfig()
	cfg.PartitionRPS = 10
	cfg.PartitionBurst = 10
	cfg.MaxWait = 60 * time.Second
	cfg.PartitionIdleTTL = time.Hour
	return cfg
}
