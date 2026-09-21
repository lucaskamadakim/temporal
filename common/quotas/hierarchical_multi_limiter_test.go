package quotas

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestMultiHierarchicalRateLimiter_InvalidConfig(t *testing.T) {
	_, err := NewMultiHierarchicalRateLimiter(HierarchicalLimiterConfig{}, clock.NewEventTimeSource(), HierarchicalRateLimiterMetrics{})
	require.Error(t, err)
}

func TestMultiHierarchicalRateLimiter_GroupsIsolated(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	for i := 0; i < 150; i++ {
		require.True(t, m.Allow("g1", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	require.False(t, m.Allow("g1", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	// g2 has its own untouched capacity.
	for i := 0; i < 150; i++ {
		require.True(t, m.Allow("g2", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	require.False(t, m.Allow("g2", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
}

func TestMultiHierarchicalRateLimiter_Groups(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	require.Empty(t, m.Groups())
	m.Allow("b", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	m.Allow("a", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	m.Allow("a", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	groups := m.Groups()
	slices.Sort(groups)
	require.Equal(t, []string{"a", "b"}, groups)
}

func TestMultiHierarchicalRateLimiter_RemoveGroup(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	m.Allow("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 149})
	require.False(t, m.RemoveGroup("absent"))
	require.True(t, m.RemoveGroup("g"))
	// Fresh group starts with full capacity again.
	require.True(t, m.Allow("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 150}))
}

func TestMultiHierarchicalRateLimiter_PruneIdleGroups(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	m.Allow("old", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	ts.Advance(10 * time.Minute)
	m.Allow("fresh", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	require.Equal(t, 1, m.PruneIdleGroups(time.Minute))
	require.Equal(t, []string{"fresh"}, m.Groups())
}

func TestMultiHierarchicalRateLimiter_UpdateAppliesToGroups(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	m.Allow("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1})
	newCfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 10, Burst: 5},
		},
	}
	require.NoError(t, m.Update(newCfg))
	// Existing group sees the new burst.
	for i := 0; i < 5; i++ {
		require.True(t, m.Allow("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	require.False(t, m.Allow("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	// And a newly created group uses it too.
	for i := 0; i < 5; i++ {
		require.True(t, m.Allow("new", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
	}
	require.False(t, m.Allow("new", HierarchicalRequest{Path: []string{"ns"}, Tokens: 1}))
}

func TestMultiHierarchicalRateLimiter_UpdateInvalid(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	require.Error(t, m.Update(HierarchicalLimiterConfig{}))
}

func TestMultiHierarchicalRateLimiter_Reserve(t *testing.T) {
	ts := clock.NewEventTimeSource()
	m, err := NewMultiHierarchicalRateLimiter(testHierarchicalConfig(), ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	res := m.Reserve("g", HierarchicalRequest{Path: []string{"ns"}, Tokens: 10})
	require.True(t, res.OK())
	res.Cancel()
}
