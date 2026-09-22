package adaptive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/quotas/adaptive"
	"go.temporal.io/server/common/testing/await"
)

func TestLimiterEmitsMetrics(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	mh := metricstest.NewCaptureHandler()
	l, err := adaptive.NewLimiter(testConfig(),
		adaptive.WithTimeSource(ts),
		adaptive.WithMetricsHandler(mh),
	)
	require.NoError(t, err)
	t.Cleanup(l.Close)

	capture := mh.StartCapture()
	defer mh.StopCapture(capture)

	require.True(t, l.Allow("ns"))
	require.False(t, l.AllowN("ns", 100000))
	l.RecycleToken("ns")

	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.AdaptiveRateLimiterAllowed.Name()], 1)
	require.Len(t, snapshot[metrics.AdaptiveRateLimiterDenied.Name()], 1)
	require.NotEmpty(t, snapshot[metrics.AdaptiveRateLimiterPartitions.Name()])
}

func TestLimiterRecordsRateOnSignal(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	mh := metricstest.NewCaptureHandler()
	l, err := adaptive.NewLimiter(testConfig(),
		adaptive.WithTimeSource(ts),
		adaptive.WithMetricsHandler(mh),
	)
	require.NoError(t, err)
	t.Cleanup(l.Close)

	capture := mh.StartCapture()
	defer mh.StopCapture(capture)

	l.ReportSignal(0.9)

	snapshot := capture.Snapshot()
	multipliers := snapshot[metrics.AdaptiveRateLimiterMultiplier.Name()]
	require.NotEmpty(t, multipliers)
	require.InDelta(t, 0.5, multipliers[len(multipliers)-1].Value, 1e-9)

	rates := snapshot[metrics.AdaptiveRateLimiterRate.Name()]
	require.NotEmpty(t, rates)
	require.InDelta(t, 5.0, rates[len(rates)-1].Value, 1e-9)
}

func TestLimiterRecordsWaitLatency(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	mh := metricstest.NewCaptureHandler()
	l, err := adaptive.NewLimiter(testConfig(),
		adaptive.WithTimeSource(ts),
		adaptive.WithMetricsHandler(mh),
	)
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.AllowN("ns", 10)) // drain

	capture := mh.StartCapture()
	defer mh.StopCapture(capture)

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.WaitN(context.Background(), "ns", 5)
	}()
	await.RequireTrue(t, func() bool { return l.TokensAt("ns", ts.Now()) == -5 }, 10*time.Second, 10*time.Millisecond)
	ts.Advance(600 * time.Millisecond)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not return")
	}

	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.AdaptiveRateLimiterWaitLatency.Name()], 1)
}

func TestLimiterRecordsEviction(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	mh := metricstest.NewCaptureHandler()
	cfg := testConfig()
	cfg.PartitionIdleTTL = time.Second
	l, err := adaptive.NewLimiter(cfg,
		adaptive.WithTimeSource(ts),
		adaptive.WithMetricsHandler(mh),
	)
	require.NoError(t, err)
	t.Cleanup(l.Close)

	require.True(t, l.Allow("ns"))

	capture := mh.StartCapture()
	defer mh.StopCapture(capture)

	ts.Advance(2 * time.Second)

	await.RequireTrue(t, func() bool {
		return len(capture.Snapshot()[metrics.AdaptiveRateLimiterPartitionEvicted.Name()]) == 1
	}, 10*time.Second, 10*time.Millisecond)
}
