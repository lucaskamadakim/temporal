package adaptive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfigValidates(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate())
}

func TestConfigValidateDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		PartitionRPS:   10,
		PartitionBurst: 5,
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, defaultMaxPartitions, cfg.MaxPartitions)
	require.Equal(t, defaultPartitionIdleTTL, cfg.PartitionIdleTTL)
	require.Equal(t, defaultMaxWait, cfg.MaxWait)
	require.InDelta(t, defaultAlpha, cfg.Alpha, 1e-9)
	require.InDelta(t, defaultBeta, cfg.Beta, 1e-9)
	require.InDelta(t, defaultMinMultiplier, cfg.MinMultiplier, 1e-9)
	require.InDelta(t, defaultMaxMultiplier, cfg.MaxMultiplier, 1e-9)
	require.Equal(t, []float64{1.0}, cfg.PriorityWeights)
}

func TestConfigValidateRejectsBadRate(t *testing.T) {
	t.Parallel()

	for _, rps := range []float64{0, -1, -100.5} {
		cfg := DefaultConfig()
		cfg.PartitionRPS = rps
		require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)
	}
}

func TestConfigValidateRejectsBadBurst(t *testing.T) {
	t.Parallel()

	for _, burst := range []int{0, -3} {
		cfg := DefaultConfig()
		cfg.PartitionBurst = burst
		require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)
	}
}

func TestConfigValidateRejectsBadBounds(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxPartitions = -1
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	cfg.PartitionIdleTTL = -time.Second
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	cfg.MaxWait = -time.Second
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	cfg.Alpha = -0.1
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	for _, beta := range []float64{-0.5, 1.0, 1.5} {
		cfg.Beta = beta
		require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)
	}

	cfg = DefaultConfig()
	cfg.MinMultiplier = -1
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	cfg.MinMultiplier = 5
	cfg.MaxMultiplier = 1
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)

	cfg = DefaultConfig()
	cfg.SignalLowWatermark = 0.9
	require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)
}

func TestConfigWeightFor(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.PriorityWeights = []float64{1.0, 0.5, 0.25}
	require.InDelta(t, 1.0, cfg.weightFor(0), 1e-9)
	require.InDelta(t, 0.5, cfg.weightFor(1), 1e-9)
	require.InDelta(t, 0.25, cfg.weightFor(2), 1e-9)
	// out of range priorities clamp to the lowest weight
	require.InDelta(t, 0.25, cfg.weightFor(3), 1e-9)
	require.InDelta(t, 0.25, cfg.weightFor(100), 1e-9)
	// negative priorities clamp to the highest weight
	require.InDelta(t, 1.0, cfg.weightFor(-1), 1e-9)
}

func TestConfigWeightForNonPositive(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.PriorityWeights = []float64{0}
	require.InDelta(t, 1.0, cfg.weightFor(0), 1e-9)
}

func TestDelayFor(t *testing.T) {
	t.Parallel()

	require.Equal(t, time.Second, delayFor(10, 10, 1))
	require.Equal(t, 500*time.Millisecond, delayFor(10, 10, 2))
	require.Equal(t, 2*time.Second, delayFor(10, 10, 0.5))
}
