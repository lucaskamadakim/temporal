package adaptive

// aimdController is an additive-increase/multiplicative-decrease controller.
// It adjusts a multiplier applied to the base partition rate based on a
// congestion signal in [0, 1]: signals below the low watermark additively
// increase the multiplier, signals above the high watermark multiplicatively
// decrease it, and signals in between leave it unchanged.
type aimdController struct {
	multiplier float64
	alpha      float64
	beta       float64
	min        float64
	max        float64
	low        float64
	high       float64
}

func newAIMDController(cfg Config) *aimdController {
	return &aimdController{
		multiplier: 1.0,
		alpha:      cfg.Alpha,
		beta:       cfg.Beta,
		min:        cfg.MinMultiplier,
		max:        cfg.MaxMultiplier,
		low:        cfg.SignalLowWatermark,
		high:       cfg.SignalHighWatermark,
	}
}

// report feeds a congestion signal sample into the controller and returns the
// resulting multiplier. The second return value reports whether the
// multiplier changed, in which case callers should re-apply effective rates.
func (c *aimdController) report(signal float64) (float64, bool) {
	if signal < 0 {
		signal = 0
	}
	if signal > 1 {
		signal = 1
	}
	old := c.multiplier
	switch {
	case signal > c.high:
		c.multiplier *= c.beta
		if c.multiplier < c.min {
			c.multiplier = c.min
		}
	case signal < c.low:
		c.multiplier += c.alpha
		if c.multiplier > c.max {
			c.multiplier = c.max
		}
	default:
		// signal inside the dead band: leave the multiplier unchanged
	}
	return c.multiplier, c.multiplier != old
}

// reset returns the multiplier to 1.0, e.g. when the signal source restarts.
func (c *aimdController) reset() float64 {
	c.multiplier = 1.0
	return c.multiplier
}
