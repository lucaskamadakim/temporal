package adaptive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas/adaptive"
	"go.temporal.io/server/common/testing/await"
)

func TestTenantRateLimiterAllow(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.True(t, rl.Allow())
	require.Equal(t, 1, l.NumPartitions())
}

func TestTenantRateLimiterAllowN(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.True(t, rl.AllowN(ts.Now(), 10))
	require.False(t, rl.AllowN(ts.Now(), 1))
}

func TestTenantRateLimiterReserve(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	r := rl.Reserve()
	require.True(t, r.OK())
	require.Equal(t, time.Duration(0), r.Delay())
}

func TestTenantRateLimiterReserveN(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.True(t, rl.AllowN(ts.Now(), 10)) // drain
	r := rl.ReserveN(ts.Now(), 5)
	require.True(t, r.OK())
	require.Equal(t, 500*time.Millisecond, r.DelayFrom(ts.Now()))
}

func TestTenantRateLimiterWait(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.NoError(t, rl.Wait(context.Background()))
}

func TestTenantRateLimiterWaitN(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.True(t, rl.AllowN(ts.Now(), 10)) // drain

	errCh := make(chan error, 1)
	go func() {
		errCh <- rl.WaitN(context.Background(), 10)
	}()
	await.RequireTrue(t, func() bool { return rl.TokensAt(ts.Now()) == -10 }, 10*time.Second, 10*time.Millisecond)

	ts.Advance(time.Second)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not return after grant time")
	}
}

func TestTenantRateLimiterRateBurstTokens(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.InDelta(t, 10.0, rl.Rate(), 1e-9)
	require.Equal(t, 10, rl.Burst())
	require.Equal(t, 10, rl.TokensAt(ts.Now()))
}

func TestTenantRateLimiterRecycleToken(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := l.For("ns")
	require.True(t, rl.AllowN(ts.Now(), 10)) // drain
	r := rl.ReserveN(ts.Now(), 1)
	require.Equal(t, 100*time.Millisecond, r.DelayFrom(ts.Now()))

	rl.RecycleToken()
	require.Equal(t, time.Duration(0), r.DelayFrom(ts.Now()))
}

func TestTenantRateLimiterPriority(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	// priority 1 accrues at half the partition rate.
	high := l.ForPriority("ns", 0)
	low := l.ForPriority("ns", 1)

	require.True(t, high.AllowN(ts.Now(), 10)) // drain
	rh := high.ReserveN(ts.Now(), 10)
	rl := low.ReserveN(ts.Now(), 10)
	require.Equal(t, time.Second, rh.DelayFrom(ts.Now()))
	require.Equal(t, 4*time.Second, rl.DelayFrom(ts.Now()))
}
