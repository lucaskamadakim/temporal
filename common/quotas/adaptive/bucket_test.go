package adaptive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testEpoch = time.Unix(0, 0)

func TestTokenBucketStartsFull(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	require.InDelta(t, 50.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketRefill(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 30)
	require.InDelta(t, 20.0, b.tokensAt(testEpoch), 1e-9)
	require.InDelta(t, 25.0, b.tokensAt(testEpoch.Add(500*time.Millisecond)), 1e-9)
	require.InDelta(t, 50.0, b.tokensAt(testEpoch.Add(10*time.Second)), 1e-9)
}

func TestTokenBucketRefillDoesNotExceedCapacity(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	require.InDelta(t, 50.0, b.tokensAt(testEpoch.Add(time.Hour)), 1e-9)
}

func TestTokenBucketChargeAllowsDebt(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 80)
	require.InDelta(t, -30.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketDebtRepaysOverTime(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 80)
	require.InDelta(t, -20.0, b.tokensAt(testEpoch.Add(time.Second)), 1e-9)
	require.InDelta(t, 0.0, b.tokensAt(testEpoch.Add(3*time.Second)), 1e-9)
}

func TestTokenBucketRestore(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 30)
	b.restore(testEpoch, 10)
	require.InDelta(t, 30.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketRestoreCapsAtCapacity(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 30)
	b.restore(testEpoch, 40)
	require.InDelta(t, 50.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketRestoreDebt(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 80)
	b.restore(testEpoch, 30)
	require.InDelta(t, 0.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketEarliestWhenCovered(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	require.Equal(t, testEpoch, b.earliest(testEpoch, 50))
	require.Equal(t, testEpoch, b.earliest(testEpoch, 1))
}

func TestTokenBucketEarliestWithDeficit(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 45)
	// deficit of 25 tokens at 10 tokens/second is 2.5 seconds out.
	require.Equal(t, testEpoch.Add(2500*time.Millisecond), b.earliest(testEpoch, 30))
}

func TestTokenBucketEarliestWithDebt(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 80) // -30 debt
	require.Equal(t, testEpoch.Add(4*time.Second), b.earliest(testEpoch, 10))
}

func TestTokenBucketEarliestNoRate(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(0, 50, testEpoch)
	b.charge(testEpoch, 80)
	require.True(t, b.earliest(testEpoch, 10).IsZero())
	require.Equal(t, testEpoch, b.earliest(testEpoch, 0))
}

func TestTokenBucketEarliestClockSkew(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 30)
	past := testEpoch.Add(-time.Second)
	require.Equal(t, past, b.earliest(past, 10))
}

func TestTokenBucketSetRateSnapshotsBalance(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.charge(testEpoch, 30)
	b.setRate(20, testEpoch.Add(time.Second))
	// 10 tokens accrued in the second before the rate change.
	require.InDelta(t, 30.0, b.tokensAt(testEpoch.Add(time.Second)), 1e-9)
	require.InDelta(t, 40.0, b.tokensAt(testEpoch.Add(1500*time.Millisecond)), 1e-9)
}

func TestTokenBucketSetCapacityShrinksBalance(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	b.setCapacity(10, testEpoch)
	require.InDelta(t, 10.0, b.tokensAt(testEpoch), 1e-9)
}

func TestTokenBucketUtilization(t *testing.T) {
	t.Parallel()

	b := newTokenBucket(10, 50, testEpoch)
	require.InDelta(t, 0.0, b.utilization(testEpoch), 1e-9)
	b.charge(testEpoch, 25)
	require.InDelta(t, 0.5, b.utilization(testEpoch), 1e-9)
	b.charge(testEpoch, 50)
	require.InDelta(t, 1.0, b.utilization(testEpoch), 1e-9)
}

func TestDelayFromDeficit(t *testing.T) {
	t.Parallel()

	require.Equal(t, time.Second, delayFromDeficit(10, 10))
	require.Equal(t, 500*time.Millisecond, delayFromDeficit(5, 10))
	require.Equal(t, time.Duration(1<<63-1), delayFromDeficit(5, 0))
}
