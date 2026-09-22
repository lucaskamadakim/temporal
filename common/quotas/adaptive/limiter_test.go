package adaptive_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/adaptive"
	"go.temporal.io/server/common/testing/await"
)

func testConfig() adaptive.Config {
	cfg := adaptive.DefaultConfig()
	cfg.PartitionRPS = 10
	cfg.PartitionBurst = 10
	cfg.MaxWait = 60 * time.Second
	cfg.PartitionIdleTTL = 30 * time.Second
	cfg.PriorityWeights = []float64{1.0, 0.5, 0.25}
	cfg.Beta = 0.5
	cfg.Alpha = 0.1
	return cfg
}

func newTestLimiter(t *testing.T) (*adaptive.Limiter, *clock.EventTimeSource) {
	t.Helper()
	ts := newAsyncTimeSource()
	ts.UseAsyncTimers(true)
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)
	return l, ts
}

func newAsyncTimeSource() *clock.EventTimeSource {
	ts := clock.NewEventTimeSource()
	ts.UseAsyncTimers(true)
	return ts
}

func TestNewLimiterValidatesConfig(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.PartitionRPS = -1
	l, err := adaptive.NewLimiter(cfg)
	require.ErrorIs(t, err, adaptive.ErrInvalidConfig)
	require.Nil(t, l)
}

func TestAllowDrainsBurst(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	for i := 0; i < 10; i++ {
		require.True(t, l.Allow("ns"), "request %d should be allowed", i)
	}
	require.False(t, l.Allow("ns"))

	ts.Advance(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		require.True(t, l.Allow("ns"))
	}
	require.False(t, l.Allow("ns"))
}

func TestAllowN(t *testing.T) {
	t.Parallel()

	l, _ := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 7))
	require.True(t, l.AllowN("ns", 3))
	require.False(t, l.AllowN("ns", 1))
}

func TestAllowIsPerPartition(t *testing.T) {
	t.Parallel()

	l, _ := newTestLimiter(t)
	require.True(t, l.AllowN("a", 10))
	require.False(t, l.Allow("a"))
	require.True(t, l.AllowN("b", 10))
	require.True(t, l.Allow("c"))
	require.Equal(t, 3, l.NumPartitions())
}

func TestReserveImmediateGrant(t *testing.T) {
	t.Parallel()

	l, _ := newTestLimiter(t)
	r := l.Reserve("ns")
	require.True(t, r.OK())
	require.Equal(t, time.Duration(0), r.Delay())
}

func TestReserveScheduledGrant(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r := l.ReserveN("ns", 5, 0)
	require.True(t, r.OK())
	// 5 tokens at 10 tokens/second => 500ms
	require.Equal(t, 500*time.Millisecond, r.DelayFrom(ts.Now()))
}

func TestReserveDeniedBeyondMaxWait(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxWait = time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10))
	r := l.ReserveN("ns", 200, 0)
	require.False(t, r.OK())
	require.Equal(t, quotas.InfDuration, r.Delay())
}

func TestWaitImmediate(t *testing.T) {
	t.Parallel()

	l, _ := newTestLimiter(t)
	require.NoError(t, l.Wait(context.Background(), "ns"))
}

func TestWaitBlocksUntilGrant(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.WaitN(context.Background(), "ns", 10)
	}()
	await.RequireTrue(t, func() bool { return l.TokensAt("ns", ts.Now()) == -10 }, 10*time.Second, 10*time.Millisecond)

	ts.Advance(999 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("wait returned early: %v", err)
	default:
	}

	ts.Advance(time.Millisecond)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not return after grant time")
	}
}

func TestWaitDenied(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxWait = time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10))
	require.ErrorIs(t, l.WaitN(context.Background(), "ns", 200), adaptive.ErrRateLimitExceeded)
}

func TestWaitContextCancel(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- l.WaitN(ctx, "ns", 5)
	}()
	await.RequireTrue(t, func() bool { return l.TokensAt("ns", ts.Now()) == -5 }, 10*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not return after context cancel")
	}

	// the canceled reservation's tokens were returned to the bucket
	require.Equal(t, 0, l.TokensAt("ns", ts.Now()))
}

func TestCancelScheduledReservation(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r := l.ReserveN("ns", 5, 0)
	require.True(t, r.OK())
	require.Equal(t, 500*time.Millisecond, r.DelayFrom(ts.Now()))
	require.Equal(t, -5, l.TokensAt("ns", ts.Now()))

	r.Cancel()
	require.Equal(t, 0, l.TokensAt("ns", ts.Now()))
	require.Equal(t, time.Duration(0), r.DelayFrom(ts.Now()))
}

func TestCancelDeniedReservation(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxWait = time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10))
	r := l.ReserveN("ns", 200, 0)
	require.False(t, r.OK())
	r.Cancel() // must not panic
	require.False(t, r.OK())
}

func TestScheduledReservationsGrantInOrder(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 10, 0)       // +1s
	r2 := l.ReserveN("ns", 10, 0)       // +2s
	r3 := l.ReserveN("ns", 10, 0)       // +3s

	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))
	require.Equal(t, 2*time.Second, r2.DelayFrom(ts.Now()))
	require.Equal(t, 3*time.Second, r3.DelayFrom(ts.Now()))
}

func TestPriorityShortensDelay(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.PriorityWeights = []float64{1.0, 0.25}
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10)) // drain
	high := l.ReserveN("ns", 10, 0)
	low := l.ReserveN("ns", 10, 1)

	// the highest priority request accrues at the full partition rate.
	require.Equal(t, time.Second, high.DelayFrom(ts.Now()))
	// the lower priority request accrues at a quarter of the rate on top of
	// the debt already committed to the earlier reservation.
	require.Equal(t, 8*time.Second, low.DelayFrom(ts.Now()))
}

func TestReportSignalShrinksRate(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 10, 0)
	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))

	// beta 0.5 halves the rate; the pending grant moves further out.
	l.ReportSignal(0.9)
	require.InDelta(t, 0.5, l.Multiplier(), 1e-9)
	require.InDelta(t, 5.0, l.Rate(), 1e-9)
	require.Equal(t, 2*time.Second, r1.DelayFrom(ts.Now()))
}

func TestReportSignalShrinksRateMultiplePending(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 10, 0)
	r2 := l.ReserveN("ns", 10, 0)

	// at half rate each reservation takes twice as long to accrue.
	l.ReportSignal(0.9)
	require.Equal(t, 2*time.Second, r1.DelayFrom(ts.Now()))
	require.Equal(t, 4*time.Second, r2.DelayFrom(ts.Now()))
}

func TestReportSignalGrowsRate(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Alpha = 0.5
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	l.ReportSignal(0.9)
	require.InDelta(t, 0.5, l.Multiplier(), 1e-9)

	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 12, 0)
	require.Equal(t, 2400*time.Millisecond, r1.DelayFrom(ts.Now()))

	// alpha 0.5 increases the multiplier back to 1.0; the grant pulls in.
	l.ReportSignal(0.0)
	require.InDelta(t, 1.0, l.Multiplier(), 1e-9)
	require.InDelta(t, 10.0, l.Rate(), 1e-9)
	require.Equal(t, 1200*time.Millisecond, r1.DelayFrom(ts.Now()))
}

func TestReportSignalDeadBandKeepsSchedule(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 10, 0)
	l.ReportSignal(0.5)
	require.Equal(t, time.Second, r1.DelayFrom(ts.Now()))
}

func TestUpdateBaseRate(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r1 := l.ReserveN("ns", 10, 0)

	l.UpdateBaseRate(20)
	require.InDelta(t, 20.0, l.Rate(), 1e-9)
	require.Equal(t, 500*time.Millisecond, r1.DelayFrom(ts.Now()))

	l.UpdateBaseRate(-5) // ignored
	require.InDelta(t, 20.0, l.Rate(), 1e-9)
}

func TestUpdateBurst(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10))
	ts.Advance(5 * time.Second)
	require.False(t, l.AllowN("ns", 11))
	l.UpdateBurst(20)
	require.Equal(t, 20, l.Burst())
}

func TestRecycleTokenPullsGrantEarlier(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10)) // drain
	r := l.ReserveN("ns", 1, 0)
	require.Equal(t, 100*time.Millisecond, r.DelayFrom(ts.Now()))

	l.RecycleToken("ns")
	require.Equal(t, time.Duration(0), r.DelayFrom(ts.Now()))
}

func TestPartitionIdleEvictionCancelsWaiters(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.PartitionIdleTTL = 10 * time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10)) // drain
	errCh := make(chan error, 1)
	go func() {
		errCh <- l.WaitN(context.Background(), "ns", 300) // grant ~30s out
	}()
	await.RequireTrue(t, func() bool { return l.TokensAt("ns", ts.Now()) == -300 }, 10*time.Second, 10*time.Millisecond)

	ts.Advance(11 * time.Second)
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, adaptive.ErrReservationCanceled)
	case <-time.After(10 * time.Second):
		t.Fatal("waiter was not released on partition eviction")
	}
	require.Equal(t, 0, l.NumPartitions())
}

func TestPartitionIdleEvictionIsRefreshedByUse(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.PartitionIdleTTL = 10 * time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.Allow("ns"))
	ts.Advance(9 * time.Second)
	require.True(t, l.Allow("ns")) // refresh
	ts.Advance(9 * time.Second)
	require.Equal(t, 1, l.NumPartitions())
	ts.Advance(11 * time.Second)
	await.RequireTrue(t, func() bool { return l.NumPartitions() == 0 }, 10*time.Second, 10*time.Millisecond)
}

func TestMaxPartitionsEvictsLRU(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxPartitions = 2
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.Allow("a"))
	ts.Advance(time.Millisecond)
	require.True(t, l.Allow("b"))
	ts.Advance(time.Millisecond)
	require.True(t, l.Allow("c"))

	require.Equal(t, 2, l.NumPartitions())
	// "a" was least recently used and is gone: it has a fresh bucket again.
	require.True(t, l.AllowN("a", 10))
}

func TestEvictedPartitionCancelsPendingReservation(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.PartitionIdleTTL = 10 * time.Second
	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(cfg, adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10)) // drain
	r := l.ReserveN("ns", 300, 0)       // ~30s out
	require.True(t, r.OK())

	ts.Advance(11 * time.Second)
	await.RequireTrue(t, func() bool { return l.NumPartitions() == 0 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, time.Duration(0), r.DelayFrom(ts.Now()))
}

func TestClose(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)

	require.True(t, l.AllowN("ns", 10))
	errCh := make(chan error, 1)
	go func() {
		errCh <- l.WaitN(context.Background(), "ns", 50)
	}()
	await.RequireTrue(t, func() bool { return l.TokensAt("ns", ts.Now()) == -50 }, 10*time.Second, 10*time.Millisecond)

	l.Close()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, adaptive.ErrReservationCanceled)
	case <-time.After(10 * time.Second):
		t.Fatal("waiter was not released on close")
	}

	require.False(t, l.Allow("ns"))
	r := l.Reserve("ns")
	require.False(t, r.OK())
}

func TestTokensAtReflectsDebt(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.True(t, l.AllowN("ns", 10))
	r := l.ReserveN("ns", 15, 0)
	require.True(t, r.OK())
	require.Equal(t, -15, l.TokensAt("ns", ts.Now()))
	ts.Advance(time.Second)
	require.Equal(t, -5, l.TokensAt("ns", ts.Now()))
}

func TestTokensAtForMissingPartition(t *testing.T) {
	t.Parallel()

	l, ts := newTestLimiter(t)
	require.Equal(t, 10, l.TokensAt("missing", ts.Now()))
}

func TestConcurrentAllow(t *testing.T) {
	t.Parallel()

	l, err := adaptive.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(l.Close)

	var wg sync.WaitGroup
	results := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			results <- l.AllowN(fmt.Sprintf("key-%d", i), 10)
		})
	}
	wg.Wait()
	close(results)
	for ok := range results {
		require.True(t, ok)
	}
	require.Equal(t, 8, l.NumPartitions())
}

func TestConcurrentReservations(t *testing.T) {
	t.Parallel()

	l, err := adaptive.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10)) // drain

	var wg sync.WaitGroup
	rs := make(chan quotas.Reservation, 16)
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			rs <- l.Reserve("ns")
		})
	}
	wg.Wait()
	close(rs)

	var got []quotas.Reservation
	for r := range rs {
		got = append(got, r)
	}
	require.Len(t, got, 16)
	for _, r := range got {
		require.True(t, r.OK())
	}
}
