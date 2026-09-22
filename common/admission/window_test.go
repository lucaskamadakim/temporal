package admission

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func newTestWindow(t *testing.T, window time.Duration, buckets int, ts clock.TimeSource) *SlidingWindow {
	t.Helper()
	w, err := NewSlidingWindow(window, buckets, ts)
	require.NoError(t, err)
	return w
}

func TestNewSlidingWindow_ValidatesArgs(t *testing.T) {
	ts := clock.NewEventTimeSource()
	_, err := NewSlidingWindow(0, 10, ts)
	require.ErrorIs(t, err, ErrWindowNotPositive)
	_, err = NewSlidingWindow(-time.Second, 10, ts)
	require.ErrorIs(t, err, ErrWindowNotPositive)
	_, err = NewSlidingWindow(time.Second, 0, ts)
	require.ErrorIs(t, err, ErrBucketCountNotPositive)
	_, err = NewSlidingWindow(time.Second, -3, ts)
	require.ErrorIs(t, err, ErrBucketCountNotPositive)
	// bucket width below a nanosecond cannot be represented
	_, err = NewSlidingWindow(time.Nanosecond, 2, ts)
	require.Error(t, err)
}

func TestSlidingWindow_RecordAndCountSameBucket(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(KindSuccess, 3)
	w.Record(KindSuccess, 2)
	w.Record(KindFailure, 1)
	w.Record(KindRejected, 4)

	require.Equal(t, int64(5), w.Count(KindSuccess))
	require.Equal(t, int64(1), w.Count(KindFailure))
	require.Equal(t, int64(4), w.Count(KindRejected))
	require.Equal(t, int64(6), w.Requests())

	counts := w.Counts()
	require.Equal(t, int64(5), counts[KindSuccess])
	require.Equal(t, int64(1), counts[KindFailure])
	require.Equal(t, int64(4), counts[KindRejected])
}

func TestSlidingWindow_RecordIgnoresInvalidInput(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(Kind(-1), 1)
	w.Record(numKinds, 1)
	w.Record(KindSuccess, 0)

	require.Equal(t, int64(0), w.Requests())
}

func TestSlidingWindow_PartialAdvanceDropsOldestBuckets(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts) // 1s buckets

	w.Record(KindSuccess, 10)
	ts.Advance(1100 * time.Millisecond) // move into the next bucket
	w.Record(KindSuccess, 5)
	ts.Advance(1100 * time.Millisecond)
	w.Record(KindFailure, 1)

	require.Equal(t, int64(15), w.Count(KindSuccess))
	require.Equal(t, int64(1), w.Count(KindFailure))
	require.Equal(t, int64(16), w.Requests())
}

func TestSlidingWindow_BucketsExpireAsHeadRotates(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(KindSuccess, 8)

	// rotate the head halfway around the ring: the original bucket is still
	// inside the window, so its count must be retained
	ts.Advance(5 * time.Second)
	require.Equal(t, int64(8), w.Count(KindSuccess))

	// rotate far enough that the original bucket falls out of the window
	// but less than a full lap, so the window still holds newer data
	ts.Advance(6 * time.Second)
	w.Record(KindSuccess, 2)
	require.Equal(t, int64(2), w.Count(KindSuccess))
}

func TestSlidingWindow_WraparoundRetainsLiveBuckets(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	// spread one event per bucket across eight buckets (1050ms spacing lands
	// in a fresh bucket each iteration)
	for i := 0; i < 8; i++ {
		w.Record(KindSuccess, 1)
		ts.Advance(1050 * time.Millisecond)
	}
	require.Equal(t, int64(8), w.Count(KindSuccess))

	// rotating the head five steps wraps it past index 0 and clears buckets
	// 0-2; the five surviving buckets must be preserved
	ts.Advance(4 * time.Second)
	require.Equal(t, int64(5), w.Count(KindSuccess))
}

func TestSlidingWindow_BackwardsTimeIsIgnored(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(KindSuccess, 7)
	// rewinding the source must not corrupt the ring
	ts.Update(ts.Now().Add(-5 * time.Second))
	w.Record(KindSuccess, 1)
	require.Equal(t, int64(8), w.Count(KindSuccess))
}

func TestSlidingWindow_FailureRatio(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	require.InDelta(t, 0.0, w.FailureRatio(), 1e-9)

	w.Record(KindSuccess, 3)
	w.Record(KindFailure, 1)
	require.InDelta(t, 0.25, w.FailureRatio(), 1e-9)

	// rejected requests must not count toward the ratio's denominator
	w.Record(KindRejected, 100)
	require.InDelta(t, 0.25, w.FailureRatio(), 1e-9)

	// after the failures age out, the ratio recovers
	ts.Advance(3 * time.Second)
	w.Record(KindSuccess, 9)
	require.InDelta(t, 1.0/13.0, w.FailureRatio(), 1e-9)
}

func TestSlidingWindow_RatioArbitraryKinds(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(KindSuccess, 2)
	w.Record(KindFailure, 1)
	w.Record(KindRejected, 6)

	require.InDelta(t, 1.0, w.Ratio(KindRejected, KindRejected), 1e-9)
	require.InDelta(t, 2.0/3.0, w.Ratio(KindSuccess, KindSuccess, KindFailure), 1e-9)
	require.InDelta(t, 1.0/6.0, w.Ratio(KindFailure, KindRejected), 1e-9)
}

func TestSlidingWindow_MeanLatency(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	require.Zero(t, w.MeanLatency())

	w.RecordLatency(100 * time.Millisecond)
	w.RecordLatency(300 * time.Millisecond)
	require.Equal(t, 200*time.Millisecond, w.MeanLatency())

	// negative latencies are rejected
	w.RecordLatency(-time.Second)
	require.Equal(t, 200*time.Millisecond, w.MeanLatency())

	ts.Advance(1500 * time.Millisecond)
	w.RecordLatency(50 * time.Millisecond)
	require.Equal(t, 150*time.Millisecond, w.MeanLatency())
}

func TestSlidingWindow_MultipleKindsShareBucket(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, 10*time.Second, 10, ts)

	w.Record(KindSuccess, 1)
	w.Record(KindFailure, 2)
	w.Record(KindRejected, 3)
	ts.Advance(1200 * time.Millisecond)
	w.Record(KindSuccess, 4)

	counts := w.Counts()
	require.Equal(t, int64(5), counts[KindSuccess])
	require.Equal(t, int64(2), counts[KindFailure])
	require.Equal(t, int64(3), counts[KindRejected])
}

func TestSlidingWindow_HeadTimeAlignsToBucketBoundary(t *testing.T) {
	ts := clock.NewEventTimeSource()
	ts.Update(time.Unix(0, 0).Add(13 * time.Hour).Add(2500 * time.Millisecond))
	w := newTestWindow(t, 10*time.Second, 10, ts)
	require.Equal(t, 0, w.headTime.Nanosecond()%int(time.Second))
}

func TestSlidingWindow_ConcurrentRecords(t *testing.T) {
	ts := clock.NewEventTimeSource()
	w := newTestWindow(t, time.Minute, 12, ts)

	const goroutines = 16
	const perGoroutine = 250
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Go(func() {
			for i := 0; i < perGoroutine; i++ {
				kind := KindSuccess
				if i%7 == 0 {
					kind = KindFailure
				}
				w.Record(kind, 1)
				w.RecordLatency(time.Duration(i) * time.Microsecond)
			}
		})
	}
	wg.Wait()

	counts := w.Counts()
	require.Equal(t, int64(goroutines*perGoroutine), counts[KindSuccess]+counts[KindFailure])
}

func TestKindString(t *testing.T) {
	require.Equal(t, "success", KindSuccess.String())
	require.Equal(t, "failure", KindFailure.String())
	require.Equal(t, "rejected", KindRejected.String())
	require.Equal(t, "unknown", Kind(99).String())
}
