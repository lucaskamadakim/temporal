package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testConfig() Config {
	return Config{
		Enabled:               true,
		Window:                10 * time.Second,
		WindowBuckets:         10,
		MinRequests:           10,
		FailureRatioThreshold: 0.5,
		MinConcurrency:        2,
		MaxConcurrency:        100,
		InitialConcurrency:    8,
		Smoothing:             0.5,
		RTTMinMultiplier:      0.9,
		BackoffRatio:          0.9,
		QueueSize:             4,
		LimiterUpdateInterval: time.Second,
		MinRTTRefreshInterval: time.Minute,
		LatencyEWMAAlpha:      0.5,
		QuantileEpsilon:       0.01,
		MaxKeys:               4,
	}
}

func newTestController(t *testing.T, ts clock.TimeSource, cfg Config) *Controller {
	t.Helper()
	c, err := NewController(cfg, ts, metricstest.NewCaptureHandler())
	require.NoError(t, err)
	return c
}

func TestNewController_ValidatesConfig(t *testing.T) {
	ts := clock.NewEventTimeSource()
	bad := testConfig()
	bad.FailureRatioThreshold = 2
	_, err := NewController(bad, ts, metricstest.NewCaptureHandler())
	require.Error(t, err)

	bad = testConfig()
	bad.Window = 0
	_, err = NewController(bad, ts, metricstest.NewCaptureHandler())
	require.ErrorIs(t, err, ErrWindowNotPositive)

	bad = testConfig()
	bad.MaxConcurrency = bad.MinConcurrency - 1
	_, err = NewController(bad, ts, metricstest.NewCaptureHandler())
	require.ErrorIs(t, err, ErrLimitBounds)
}

func TestController_DisabledAdmitsEverything(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.Enabled = false
	c := newTestController(t, ts, cfg)

	for i := 0; i < 1000; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}
	require.Equal(t, 0, c.Partitions())
}

func TestController_FailureGateArmedAfterMinRequests(t *testing.T) {
	ts := clock.NewEventTimeSource()
	c := newTestController(t, ts, testConfig())

	// below MinRequests the gate must stay open even at 100% failures
	for i := 0; i < 9; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}

	stats := c.Stats("k")
	require.Equal(t, int64(9), stats.Requests)

	// the tenth request arms the gate; recording its failure pushes the
	// ratio to 1.0 and the next acquire is shed
	permit, err := c.Acquire(context.Background(), "k")
	require.NoError(t, err)
	permit.Done(status.Error(codes.Internal, "boom"))

	_, err = c.Acquire(context.Background(), "k")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRejected)
	var rej *RejectedError
	require.ErrorAs(t, err, &rej)
	require.Equal(t, "failure ratio above threshold", rej.Reason)
}

func TestController_RecoversWhenFailuresAgeOut(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	c := newTestController(t, ts, cfg)

	for i := 0; i < 4; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}
	_, err := c.Acquire(context.Background(), "k")
	require.ErrorIs(t, err, ErrRejected)

	// while the failed buckets are still inside the window the gate keeps
	// shedding; as they rotate out one bucket at a time the ratio must fall
	for i := 0; i < 9; i++ {
		ts.Advance(1200 * time.Millisecond)
		_, _ = c.Acquire(context.Background(), "k")
	}
	// the stale bucket has now left the ring entirely
	permit, err := c.Acquire(context.Background(), "k")
	require.NoError(t, err)
	permit.Done(nil)
}

func TestController_ConcurrencyGateShedsWhenSaturated(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.InitialConcurrency = 3
	c := newTestController(t, ts, cfg)

	permits := make([]*Permit, 0, 3)
	for i := 0; i < 3; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permits = append(permits, permit)
	}
	_, err := c.Acquire(context.Background(), "k")
	require.ErrorIs(t, err, ErrRejected)
	var rej *RejectedError
	require.ErrorAs(t, err, &rej)
	require.Equal(t, "concurrency limit reached", rej.Reason)

	permits[0].Done(nil)
	permit, err := c.Acquire(context.Background(), "k")
	require.NoError(t, err)
	permit.Done(nil)
}

func TestController_PartitionsAreIndependent(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	cfg.InitialConcurrency = 2
	c := newTestController(t, ts, cfg)

	for i := 0; i < 4; i++ {
		permit, err := c.Acquire(context.Background(), "hot")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}
	_, err := c.Acquire(context.Background(), "hot")
	require.ErrorIs(t, err, ErrRejected)

	// a cold key still has its own counters and limiter
	permit, err := c.Acquire(context.Background(), "cold")
	require.NoError(t, err)
	permit.Done(nil)
}

func TestController_LRUEvictsOldestPartition(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MaxKeys = 3
	c := newTestController(t, ts, cfg)

	for _, key := range []string{"a", "b", "c"} {
		permit, err := c.Acquire(context.Background(), key)
		require.NoError(t, err)
		permit.Done(nil)
	}
	require.Equal(t, 3, c.Partitions())

	// touch "a" so "b" becomes the oldest
	permit, err := c.Acquire(context.Background(), "a")
	require.NoError(t, err)
	permit.Done(nil)

	permit, err = c.Acquire(context.Background(), "d")
	require.NoError(t, err)
	permit.Done(nil)

	require.Equal(t, 3, c.Partitions())
	require.Equal(t, int64(0), c.Stats("b").Requests)
	require.Equal(t, int64(2), c.Stats("a").Requests)
}

func TestController_RejectedDoesNotConsumeLimiterSlot(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	c := newTestController(t, ts, cfg)

	for i := 0; i < 4; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}
	// gate is now open; repeated rejections must not exhaust the limiter
	for i := 0; i < 50; i++ {
		_, err := c.Acquire(context.Background(), "k")
		require.ErrorIs(t, err, ErrRejected)
	}
	require.Equal(t, 0, c.Stats("k").InFlight)
}

func TestController_ClientErrorsDoNotTripFailureGate(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	c := newTestController(t, ts, cfg)

	for i := 0; i < 20; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.InvalidArgument, "bad request"))
	}
	permit, err := c.Acquire(context.Background(), "k")
	require.NoError(t, err)
	permit.Done(nil)
}

func TestController_CanceledDoesNotCount(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	c := newTestController(t, ts, cfg)

	for i := 0; i < 10; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Canceled, "client went away"))
	}
	require.Equal(t, int64(0), c.Stats("k").Requests)
}

func TestController_PermitDoneIsIdempotent(t *testing.T) {
	ts := clock.NewEventTimeSource()
	c := newTestController(t, ts, testConfig())

	permit, err := c.Acquire(context.Background(), "k")
	require.NoError(t, err)
	permit.Done(nil)
	permit.Done(status.Error(codes.Internal, "boom"))
	require.Equal(t, int64(1), c.Stats("k").Requests)
}

func TestController_EmitsRejectionMetrics(t *testing.T) {
	ts := clock.NewEventTimeSource()
	cfg := testConfig()
	cfg.MinRequests = 4
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	c, err := NewController(cfg, ts, handler)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		permit, err := c.Acquire(context.Background(), "k")
		require.NoError(t, err)
		permit.Done(status.Error(codes.Internal, "boom"))
	}
	_, err = c.Acquire(context.Background(), "k")
	require.ErrorIs(t, err, ErrRejected)

	recordings := capture.Snapshot()[AdmissionRejectedTotal]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value.(int64))
}

func TestController_OutcomeClassification(t *testing.T) {
	require.Equal(t, OutcomeSuccess, outcomeFromError(nil))
	require.Equal(t, OutcomeSuccess, outcomeFromError(status.Error(codes.InvalidArgument, "x")))
	require.Equal(t, OutcomeSuccess, outcomeFromError(status.Error(codes.NotFound, "x")))
	require.Equal(t, OutcomeFailure, outcomeFromError(status.Error(codes.Internal, "x")))
	require.Equal(t, OutcomeFailure, outcomeFromError(status.Error(codes.Unavailable, "x")))
	require.Equal(t, OutcomeFailure, outcomeFromError(status.Error(codes.ResourceExhausted, "x")))
	require.Equal(t, OutcomeFailure, outcomeFromError(status.Error(codes.DeadlineExceeded, "x")))
	require.Equal(t, OutcomeIgnore, outcomeFromError(status.Error(codes.Canceled, "x")))
	require.Equal(t, OutcomeFailure, outcomeFromError(errors.New("plain")))
}

func TestRejectedError_IsAndUnwrap(t *testing.T) {
	rej := &RejectedError{Key: "k", Reason: "r", Cause: context.DeadlineExceeded}
	require.ErrorIs(t, rej, ErrRejected)
	require.ErrorIs(t, rej, context.DeadlineExceeded)
	require.Contains(t, rej.Error(), "k")
}
