package adaptive

import (
	"time"

	"go.temporal.io/server/common/metrics"
)

// limiterMetrics emits limiter telemetry. Partition keys are deliberately not
// used as tags to avoid unbounded metric cardinality on multi-tenant fleets.
type limiterMetrics struct {
	handler metrics.Handler
}

func newLimiterMetrics(handler metrics.Handler) *limiterMetrics {
	if handler == nil {
		handler = metrics.NoopMetricsHandler
	}
	return &limiterMetrics{handler: handler}
}

func (m *limiterMetrics) recordAllowed() {
	metrics.AdaptiveRateLimiterAllowed.With(m.handler).Record(1)
}

func (m *limiterMetrics) recordDenied() {
	metrics.AdaptiveRateLimiterDenied.With(m.handler).Record(1)
}

func (m *limiterMetrics) recordCanceled() {
	metrics.AdaptiveRateLimiterCanceled.With(m.handler).Record(1)
}

func (m *limiterMetrics) recordPartitionEvicted() {
	metrics.AdaptiveRateLimiterPartitionEvicted.With(m.handler).Record(1)
}

func (m *limiterMetrics) recordWaitLatency(d time.Duration) {
	if d < 0 {
		d = 0
	}
	metrics.AdaptiveRateLimiterWaitLatency.With(m.handler).Record(d)
}

func (m *limiterMetrics) recordGrantLatency(d time.Duration) {
	if d < 0 {
		d = 0
	}
	metrics.AdaptiveRateLimiterGrantLatency.With(m.handler).Record(d)
}

func (m *limiterMetrics) recordRate(rate float64) {
	metrics.AdaptiveRateLimiterRate.With(m.handler).Record(rate)
}

func (m *limiterMetrics) recordMultiplier(multiplier float64) {
	metrics.AdaptiveRateLimiterMultiplier.With(m.handler).Record(multiplier)
}

func (m *limiterMetrics) recordPartitions(n int) {
	metrics.AdaptiveRateLimiterPartitions.With(m.handler).Record(float64(n))
}
