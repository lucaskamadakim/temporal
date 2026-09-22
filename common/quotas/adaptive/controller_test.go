package adaptive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestController() *aimdController {
	cfg := DefaultConfig()
	cfg.Alpha = 0.1
	cfg.Beta = 0.5
	cfg.MinMultiplier = 0.1
	cfg.MaxMultiplier = 2.0
	cfg.SignalLowWatermark = 0.25
	cfg.SignalHighWatermark = 0.75
	return newAIMDController(cfg)
}

func TestAIMDStartsAtOne(t *testing.T) {
	t.Parallel()

	c := newTestController()
	require.InDelta(t, 1.0, c.multiplier, 1e-9)
}

func TestAIMDAdditiveIncrease(t *testing.T) {
	t.Parallel()

	c := newTestController()
	m, changed := c.report(0.1)
	require.True(t, changed)
	require.InDelta(t, 1.1, m, 1e-9)
	m, _ = c.report(0.2)
	require.InDelta(t, 1.2, m, 1e-9)
}

func TestAIMDMultiplicativeDecrease(t *testing.T) {
	t.Parallel()

	c := newTestController()
	m, changed := c.report(0.9)
	require.True(t, changed)
	require.InDelta(t, 0.5, m, 1e-9)
	m, _ = c.report(1.0)
	require.InDelta(t, 0.25, m, 1e-9)
}

func TestAIMDDeadBand(t *testing.T) {
	t.Parallel()

	c := newTestController()
	m, changed := c.report(0.5)
	require.False(t, changed)
	require.InDelta(t, 1.0, m, 1e-9)
	m, changed = c.report(0.75)
	require.False(t, changed)
	require.InDelta(t, 1.0, m, 1e-9)
}

func TestAIMDFloorsAtMin(t *testing.T) {
	t.Parallel()

	c := newTestController()
	for i := 0; i < 20; i++ {
		c.report(1.0)
	}
	require.InDelta(t, 0.1, c.multiplier, 1e-9)
}

func TestAIMDCeilsAtMax(t *testing.T) {
	t.Parallel()

	c := newTestController()
	for i := 0; i < 50; i++ {
		c.report(0.0)
	}
	require.InDelta(t, 2.0, c.multiplier, 1e-9)
}

func TestAIMDClampsSignal(t *testing.T) {
	t.Parallel()

	c := newTestController()
	// Out-of-range signals clamp instead of panicking.
	m, _ := c.report(-3)
	require.InDelta(t, 1.1, m, 1e-9)
	c = newTestController()
	m, _ = c.report(42)
	require.InDelta(t, 0.5, m, 1e-9)
}

func TestAIMDReset(t *testing.T) {
	t.Parallel()

	c := newTestController()
	c.report(1.0)
	require.InDelta(t, 1.0, c.reset(), 1e-9)
}

func TestAIMDRecoveryCycle(t *testing.T) {
	t.Parallel()

	c := newTestController()
	c.report(0.9) // 0.5
	c.report(0.9) // 0.25
	m, _ := c.report(0.0)
	require.InDelta(t, 0.35, m, 1e-9)
	m, _ = c.report(0.0)
	require.InDelta(t, 0.45, m, 1e-9)
}
