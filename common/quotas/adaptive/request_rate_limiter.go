package adaptive

import (
	"context"
	"time"

	"go.temporal.io/server/common/quotas"
)

type (
	// RequestRateLimiter adapts a Limiter to quotas.RequestRateLimiter by
	// extracting the tenant key and priority from each quotas.Request.
	RequestRateLimiter struct {
		limiter    *Limiter
		keyFn      quotas.RequestRateLimiterKeyFn[string]
		priorityFn quotas.RequestPriorityFn
	}
)

var _ quotas.RequestRateLimiter = (*RequestRateLimiter)(nil)

// NewRequestRateLimiter returns a quotas.RequestRateLimiter that routes each
// request to the partition identified by keyFn at the priority returned by
// priorityFn. A nil keyFn maps every request onto the request's Caller; a nil
// priorityFn maps every request to priority 0.
func NewRequestRateLimiter(
	limiter *Limiter,
	keyFn quotas.RequestRateLimiterKeyFn[string],
	priorityFn quotas.RequestPriorityFn,
) *RequestRateLimiter {
	if keyFn == nil {
		keyFn = func(req quotas.Request) string { return req.Caller }
	}
	if priorityFn == nil {
		priorityFn = func(quotas.Request) int { return 0 }
	}
	return &RequestRateLimiter{
		limiter:    limiter,
		keyFn:      keyFn,
		priorityFn: priorityFn,
	}
}

// Allow implements quotas.RequestRateLimiter.
func (rl *RequestRateLimiter) Allow(now time.Time, request quotas.Request) bool {
	return rl.limiter.AllowNAt(rl.keyFn(request), request.Token, now)
}

// Reserve implements quotas.RequestRateLimiter.
func (rl *RequestRateLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	return rl.limiter.ReserveNAt(rl.keyFn(request), request.Token, rl.priorityFn(request), now)
}

// Wait implements quotas.RequestRateLimiter.
func (rl *RequestRateLimiter) Wait(ctx context.Context, request quotas.Request) error {
	return rl.limiter.WaitNPriority(ctx, rl.keyFn(request), request.Token, rl.priorityFn(request))
}
