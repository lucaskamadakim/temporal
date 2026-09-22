package interceptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/admission"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testAdmissionMethod = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"

func testAdmissionConfig() admission.Config {
	return admission.Config{
		Enabled:               true,
		Window:                10 * time.Second,
		WindowBuckets:         10,
		MinRequests:           4,
		FailureRatioThreshold: 0.5,
		MinConcurrency:        2,
		MaxConcurrency:        64,
		InitialConcurrency:    4,
		Smoothing:             0.5,
		RTTMinMultiplier:      0.9,
		BackoffRatio:          0.9,
		QueueSize:             4,
		LimiterUpdateInterval: time.Second,
		MinRTTRefreshInterval: time.Minute,
		LatencyEWMAAlpha:      0.5,
		QuantileEpsilon:       0.01,
		MaxKeys:               8,
	}
}

func newTestAdmissionInterceptor(t *testing.T, enabled bool, weights map[string]int) (*AdmissionInterceptor, *admission.Controller) {
	t.Helper()
	controller, err := admission.NewController(
		testAdmissionConfig(),
		clock.NewEventTimeSource(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	return NewAdmissionInterceptor(controller, func() bool { return enabled }, weights), controller
}

func okHandler(payload any) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		return payload, nil
	}
}

func errHandler(err error) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		return nil, err
	}
}

func testInfo(method string) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: method}
}

func TestAdmissionInterceptor_DisabledPassesThrough(t *testing.T) {
	i, _ := newTestAdmissionInterceptor(t, false, nil)
	resp, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("ok"))
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
}

func TestAdmissionInterceptor_ZeroWeightMethodBypasses(t *testing.T) {
	i, controller := newTestAdmissionInterceptor(t, true, map[string]int{testAdmissionMethod: 0})
	resp, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("ok"))
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
	// bypassed requests do not create a partition
	require.Equal(t, 0, controller.Partitions())
}

func TestAdmissionInterceptor_AdmitsAndCompletes(t *testing.T) {
	i, controller := newTestAdmissionInterceptor(t, true, nil)
	resp, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("done"))
	require.NoError(t, err)
	require.Equal(t, "done", resp)

	stats := controller.Stats(testAdmissionMethod)
	require.Equal(t, int64(1), stats.Requests)
	require.Equal(t, 0, stats.InFlight)
}

func TestAdmissionInterceptor_HandlerErrorIsPropagatedAndRecorded(t *testing.T) {
	i, controller := newTestAdmissionInterceptor(t, true, nil)
	_, err := i.Intercept(
		context.Background(),
		nil,
		testInfo(testAdmissionMethod),
		errHandler(status.Error(codes.Internal, "kaboom")),
	)
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	// client errors are not failures; Internal is
	_, err = i.Intercept(
		context.Background(),
		nil,
		testInfo(testAdmissionMethod),
		errHandler(status.Error(codes.InvalidArgument, "nope")),
	)
	require.Error(t, err)

	stats := controller.Stats(testAdmissionMethod)
	require.Equal(t, int64(2), stats.Requests)
	require.InDelta(t, 0.5, stats.FailureRatio, 1e-9)
}

func TestAdmissionInterceptor_ConcurrencyRejectionMapsToResourceExhausted(t *testing.T) {
	i, controller := newTestAdmissionInterceptor(t, true, nil)

	// exhaust the partition's in-flight budget
	held := make([]*admission.Permit, 0, 4)
	for n := 0; n < 4; n++ {
		permit, err := controller.Acquire(context.Background(), testAdmissionMethod)
		require.NoError(t, err)
		held = append(held, permit)
	}
	defer func() {
		for _, permit := range held {
			permit.Done(nil)
		}
	}()

	_, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("never"))
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, exhausted.Cause)
}

func TestAdmissionInterceptor_FailureGateRejectsWithCircuitOpen(t *testing.T) {
	i, _ := newTestAdmissionInterceptor(t, true, nil)

	for n := 0; n < 4; n++ {
		_, err := i.Intercept(
			context.Background(),
			nil,
			testInfo(testAdmissionMethod),
			errHandler(status.Error(codes.Internal, "boom")),
		)
		require.Error(t, err)
	}
	_, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("never"))
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	var exhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN, exhausted.Cause)
}

func TestAdmissionInterceptor_DistinctMethodsPartitionIndependently(t *testing.T) {
	i, controller := newTestAdmissionInterceptor(t, true, nil)
	const other = "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"

	for n := 0; n < 4; n++ {
		_, err := i.Intercept(
			context.Background(),
			nil,
			testInfo(testAdmissionMethod),
			errHandler(status.Error(codes.Internal, "boom")),
		)
		require.Error(t, err)
	}
	// hot method is now shedding
	_, err := i.Intercept(context.Background(), nil, testInfo(testAdmissionMethod), okHandler("never"))
	require.Error(t, err)

	// the cold method still passes
	resp, err := i.Intercept(context.Background(), nil, testInfo(other), okHandler("fine"))
	require.NoError(t, err)
	require.Equal(t, "fine", resp)
	require.Equal(t, 2, controller.Partitions())
}
