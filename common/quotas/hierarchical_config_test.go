package quotas

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHierarchicalLimiterConfigValidate_Empty(t *testing.T) {
	cfg := HierarchicalLimiterConfig{}
	require.ErrorIs(t, cfg.Validate(), errEmptyLevels)
}

func TestHierarchicalLimiterConfigValidate_TooDeep(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: make([]HierarchicalLevelConfig, MaxHierarchicalLevels+1),
	}
	for i := range cfg.Levels {
		cfg.Levels[i] = HierarchicalLevelConfig{Rate: 1, Burst: 1}
	}
	require.ErrorIs(t, cfg.Validate(), errTooDeep)
}

func TestHierarchicalLimiterConfigValidate_NonPositiveRate(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 0, Burst: 10},
		},
	}
	require.ErrorIs(t, cfg.Validate(), errNonPositiveRate)
}

func TestHierarchicalLimiterConfigValidate_NegativeRate(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: -5, Burst: 10},
		},
	}
	require.ErrorIs(t, cfg.Validate(), errNonPositiveRate)
}

func TestHierarchicalLimiterConfigValidate_NegativeBurst(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 10, Burst: -1},
		},
	}
	require.ErrorIs(t, cfg.Validate(), errNegativeBurst)
}

func TestHierarchicalLimiterConfigValidate_RootBorrowRejected(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 10, Burst: 10, BorrowLimit: 5},
			{Rate: 10, Burst: 10},
		},
	}
	require.ErrorIs(t, cfg.Validate(), errRootBorrow)
}

func TestHierarchicalLimiterConfigValidate_Valid(t *testing.T) {
	cfg := DefaultHierarchicalLimiterConfig()
	require.NoError(t, cfg.Validate())
	require.Equal(t, 2, cfg.Depth())
}

func TestHierarchicalLimiterConfig_LevelConfigClamp(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 100, Burst: 100},
			{Rate: 10, Burst: 10, BorrowLimit: 5},
		},
	}
	require.InDelta(t, 100.0, cfg.levelConfig(0).Rate, 1e-9)
	require.InDelta(t, 10.0, cfg.levelConfig(1).Rate, 1e-9)
	// Levels deeper than configured clamp to the last level.
	require.InDelta(t, 10.0, cfg.levelConfig(5).Rate, 1e-9)
	require.Equal(t, int64(5), cfg.levelConfig(7).BorrowLimit)
}

func TestHierarchicalLimiterConfig_ZeroBurstAllowed(t *testing.T) {
	cfg := HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 10, Burst: 0},
		},
	}
	require.NoError(t, cfg.Validate())
}
