package adaptive_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/adaptive"
)

func callerKey(req quotas.Request) string {
	return req.Caller
}

func apiPriority(req quotas.Request) int {
	if req.API == "Admin" {
		return 0
	}
	return 1
}

func TestRequestRateLimiterAllow(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := adaptive.NewRequestRateLimiter(l, callerKey, nil)
	req := quotas.NewRequest("StartWorkflowExecution", 5, "ns-a", "", 0, "")
	require.True(t, rl.Allow(ts.Now(), req))
	require.True(t, rl.Allow(ts.Now(), req))
	require.False(t, rl.Allow(ts.Now(), req))
}

func TestRequestRateLimiterDefaultKeyFn(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := adaptive.NewRequestRateLimiter(l, nil, nil)
	req := quotas.NewRequest("PollWorkflowTaskQueue", 10, "ns-a", "", 0, "")
	require.True(t, rl.Allow(ts.Now(), req))
	require.False(t, rl.Allow(ts.Now(), req))
	require.Equal(t, 1, l.NumPartitions())
}

func TestRequestRateLimiterReserve(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := adaptive.NewRequestRateLimiter(l, callerKey, apiPriority)
	require.True(t, rl.Allow(ts.Now(), quotas.NewRequest("Any", 10, "ns-a", "", 0, "")))

	// admin API gets priority 0 (full rate); regular API gets priority 1.
	admin := rl.Reserve(ts.Now(), quotas.NewRequest("Admin", 10, "ns-a", "", 0, ""))
	regular := rl.Reserve(ts.Now(), quotas.NewRequest("Other", 10, "ns-a", "", 0, ""))
	require.Equal(t, time.Second, admin.DelayFrom(ts.Now()))
	require.Equal(t, 4*time.Second, regular.DelayFrom(ts.Now()))
}

func TestRequestRateLimiterWait(t *testing.T) {
	t.Parallel()

	ts := newAsyncTimeSource()
	l, err := adaptive.NewLimiter(testConfig(), adaptive.WithTimeSource(ts))
	require.NoError(t, err)
	t.Cleanup(l.Close)

	rl := adaptive.NewRequestRateLimiter(l, callerKey, nil)
	req := quotas.NewRequest("Any", 1, "ns-a", "", 0, "")
	require.NoError(t, rl.Wait(context.Background(), req))
}
