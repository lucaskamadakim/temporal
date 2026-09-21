package quotas

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestHierarchicalReservation_SuccessFields(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	before := ts.Now()
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 7})
	require.True(t, res.OK())
	require.Equal(t, int64(7), res.Tokens())
	require.Equal(t, []string{"ns"}, res.Path())
	require.Equal(t, before, res.CreatedAt())
	require.Zero(t, res.DelayFrom(ts.Now()))
}

func TestHierarchicalReservation_CancelIsIdempotent(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 40})
	require.True(t, res.OK())
	res.Cancel()
	res.Cancel()
	res.Cancel()
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 100.0, info.Tokens, 1e-9)
	require.InDelta(t, 0.0, info.Debt, 1e-9)
}

func TestHierarchicalReservation_CancelRestoresDebtHeadroom(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 140})
	require.True(t, res.OK()) // 40 of it borrowed
	res.Cancel()
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 0.0, info.Debt, 1e-9)
	require.InDelta(t, 100.0, info.Tokens, 1e-9)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 140}))
}

func TestHierarchicalReservation_FailedHasNoPath(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	for i := 0; i < 150; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 5})
	require.False(t, res.OK())
	require.Nil(t, res.Path())
	require.Equal(t, int64(0), res.Tokens())
	require.True(t, res.CreatedAt().IsZero())
	require.Positive(t, res.DelayFrom(ts.Now()))
	res.Cancel() // no panic, no-op
}

func TestHierarchicalReservation_DelayReflectsRefill(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 150}))
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 10})
	require.False(t, res.OK())
	d1 := res.DelayFrom(ts.Now())
	require.InDelta(t, 0.6, d1.Seconds(), 0.01)
	ts.Advance(300 * time.Millisecond)
	res2 := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 40})
	require.False(t, res2.OK())
	// After 0.3s: debt 50 -> 20, borrow headroom 30, shortfall 10 ->
	// remaining wait = (20 + 10) / 100 = 0.3s
	require.InDelta(t, 0.3, res2.DelayFrom(ts.Now()).Seconds(), 0.01)
}
