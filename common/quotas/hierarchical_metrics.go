package quotas

// quotas sits below common/metrics in the dependency graph (metrics ->
// log -> quotas), so it cannot depend on metrics.Handler. Callers inject
// observers as plain counters; every field is optional.

type (
	// HierarchicalRateLimiterMetrics receives limiter events.
	HierarchicalRateLimiterMetrics struct {
		// Denied fires when a request is rejected for lack of capacity.
		Denied func(n int64)
		// Refunded fires when a reservation cancel returns tokens.
		Refunded func(n int64)
	}

	// FairDispatchMetrics receives dispatcher events.
	FairDispatchMetrics struct {
		// Enqueued fires when an item is accepted.
		Enqueued func(n int64)
		// Dropped fires when an item is rejected by a depth/key limit.
		Dropped func(n int64)
		// Served fires when an item is dispatched.
		Served func(n int64)
		// Gated fires when a key is skipped by the quota gate.
		Gated func(n int64)
		// Depth is sampled with the total queued depth after each
		// dispatch batch.
		Depth func(n int64)
	}
)

func emitCounter(f func(int64), n int64) {
	if f != nil {
		f(n)
	}
}
