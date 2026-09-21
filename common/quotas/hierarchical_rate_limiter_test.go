package quotas

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func testHierarchicalConfig() HierarchicalLimiterConfig {
	return HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 100, Burst: 100, BorrowLimit: 50},
		},
	}
}

func newTestLimiter(t *testing.T, cfg HierarchicalLimiterConfig, ts *clock.EventTimeSource) *hierarchicalRateLimiterImpl {
	t.Helper()
	if ts == nil {
		ts = clock.NewEventTimeSource()
	}
	l, err := NewHierarchicalRateLimiter(cfg, ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	return l
}

func TestNewHierarchicalRateLimiter_InvalidConfig(t *testing.T) {
	_, err := NewHierarchicalRateLimiter(HierarchicalLimiterConfig{}, clock.NewEventTimeSource(), HierarchicalRateLimiterMetrics{})
	require.Error(t, err)
}

func TestNewHierarchicalRateLimiter_NilTimeSource(t *testing.T) {
	_, err := NewHierarchicalRateLimiter(testHierarchicalConfig(), nil, HierarchicalRateLimiterMetrics{})
	require.Error(t, err)
}

func TestHierarchicalRateLimiter_AllowWithinBurst(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	for i := 0; i < 100; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}), "request %d", i)
	}
}

func TestHierarchicalRateLimiter_DenyBeyondBurstPlusBorrow(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	// ns bucket: burst 100 + borrow limit 50 = 150 grantable.
	for i := 0; i < 150; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}), "request %d", i)
	}
	require.False(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
}

func TestHierarchicalRateLimiter_BorrowLeavesDebt(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 150}))
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 0.0, info.Tokens, 1e-9)
	require.InDelta(t, 50.0, info.Debt, 1e-9)
}

func TestHierarchicalRateLimiter_RefillRepaysDebtFirst(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 150}))
	ts.Advance(time.Second) // 100 incoming at 100/s: 50 repays debt, 50 stays
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 50.0, info.Tokens, 1e-9)
	require.InDelta(t, 0.0, info.Debt, 1e-9)

	// 50 balance + 50 borrow headroom covers exactly 100 more.
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 100}))
	require.False(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	info, ok = l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 0.0, info.Tokens, 1e-9)
	require.InDelta(t, 50.0, info.Debt, 1e-9)
}

func TestHierarchicalRateLimiter_ParentLevelEnforced(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	// Twenty namespaces; each individually under its limit, together they
	// must exhaust the root's 1000+0 capacity.
	allowed := 0
	for i := 0; i < 20; i++ {
		for j := 0; j < 80; j++ {
			if l.Allow(HierarchicalRequest{Path: []string{fmt.Sprintf("ns%d", i)}, Tokens: 1}) {
				allowed++
			}
		}
	}
	require.Equal(t, 1000, allowed)
	require.False(t, l.Allow(HierarchicalRequest{Path: []string{"ns999"}, Tokens: 1}))
}

func TestHierarchicalRateLimiter_DeepPathClampsToLastLevel(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.Equal(t, 2, l.Depth())
	// Path depth 3 clamps the extra level to level-1 config.
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns", "tq", "extra"}, Tokens: 10}))
	info, ok := l.Node([]string{"ns", "tq", "extra"})
	require.True(t, ok)
	require.Equal(t, 3, info.Level)
	require.InDelta(t, 90.0, info.Tokens, 1e-9)
}

func TestHierarchicalRateLimiter_RefillRestoresCapacity(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	for i := 0; i < 150; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	require.False(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	ts.Advance(2 * time.Second) // 200 incoming: 50 to debt, balance to 100
	for i := 0; i < 150; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}), "request %d after refill", i)
	}
	require.False(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
}

func TestHierarchicalRateLimiter_ReserveCancelRefunds(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 140})
	require.True(t, res.OK())
	require.Equal(t, int64(140), res.Tokens())
	res.Cancel()
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.InDelta(t, 100.0, info.Tokens, 1e-9)
	require.InDelta(t, 0.0, info.Debt, 1e-9)
	// Root refunded too.
	root, ok := l.Node(nil)
	require.True(t, ok)
	require.InDelta(t, 1000.0, root.Tokens, 1e-9)
}

func TestHierarchicalRateLimiter_FailedReservationReportsDelay(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	for i := 0; i < 150; i++ {
		require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 10})
	require.False(t, res.OK())
	delay := res.DelayFrom(ts.Now())
	// Debt 50 + shortfall 10 at 100/s = 0.6s
	require.InDelta(t, 0.6, delay.Seconds(), 0.001)
}

func TestHierarchicalRateLimiter_UpdateReconfigures(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 50}))
	newCfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 200, Burst: 40, BorrowLimit: 10},
		},
	}
	require.NoError(t, l.Update(newCfg))
	info, ok := l.Node([]string{"ns"})
	require.True(t, ok)
	require.Equal(t, int64(40), info.Burst)
	require.InDelta(t, 40.0, info.Tokens, 1e-9) // clamped from 50 to new burst
}

func TestHierarchicalRateLimiter_UpdateRejectsDepthChange(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	deeper := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 100, Burst: 100},
			{Rate: 10, Burst: 10},
		},
	}
	require.Error(t, l.Update(deeper))
}

func TestHierarchicalRateLimiter_PruneIdle(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"busy"}, Tokens: 1}))
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"idle"}, Tokens: 1}))
	ts.Advance(10 * time.Minute)
	// "busy" stays warm: grant activity touches lastGrant.
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"busy"}, Tokens: 1}))
	removed := l.PruneIdle(time.Minute)
	require.Equal(t, 1, removed)
	_, ok := l.Node([]string{"idle"})
	require.False(t, ok)
	_, ok = l.Node([]string{"busy"})
	require.True(t, ok)
}

func TestHierarchicalRateLimiter_PruneRefreshesDebtBeforeCheck(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 150})) // 50 in debt
	ts.Advance(10 * time.Minute)                                                     // idle past cutoff; refill retires debt
	removed := l.PruneIdle(time.Minute)
	require.Equal(t, 1, removed)
}

func TestHierarchicalRateLimiter_NodeMissing(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	_, ok := l.Node([]string{"nope"})
	require.False(t, ok)
}

func TestHierarchicalRateLimiter_ZeroTokenRequest(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	require.True(t, l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 0}))
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 0})
	require.True(t, res.OK())
}

func TestHierarchicalRateLimiter_ConcurrentAllow(t *testing.T) {
	ts := clock.NewEventTimeSource()
	l := newTestLimiter(t, testHierarchicalConfig(), ts)
	var wg sync.WaitGroup
	var granted sync.Map
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			n := 0
			for j := 0; j < 50; j++ {
				if l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}) {
					n++
				}
			}
			granted.Store(i, n)
		})
	}
	wg.Wait()
	total := 0
	granted.Range(func(_, v any) bool {
		total += v.(int)
		return true
	})
	// Aggregate grants are bounded by ns capacity regardless of goroutine
	// interleaving.
	require.Equal(t, 150, total)
}

func TestHierarchicalRateLimiter_ObserversFire(t *testing.T) {
	ts := clock.NewEventTimeSource()
	var denied, refunded int64
	cfg := testHierarchicalConfig()
	l, err := NewHierarchicalRateLimiter(cfg, ts, HierarchicalRateLimiterMetrics{
		Denied:   func(n int64) { denied += n },
		Refunded: func(n int64) { refunded += n },
	})
	require.NoError(t, err)
	for i := 0; i < 160; i++ {
		l.Allow(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	}
	require.Equal(t, int64(10), denied)
	res := l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	require.False(t, res.OK()) // bucket still empty; refill hasn't run
	ts.Advance(time.Second)
	res = l.Reserve(HierarchicalRequest{Path: []string{"ns"}, Tokens: 5})
	require.True(t, res.OK())
	res.Cancel()
	require.Equal(t, int64(1), refunded)
}

func TestHierarchicalRateLimiter_RequestKey(t *testing.T) {
	r := HierarchicalRequest{Path: []string{"a", "b"}, Tokens: 1}
	require.Equal(t, "a/b", r.Key())
	require.Equal(t, 2, r.Depth())
}
