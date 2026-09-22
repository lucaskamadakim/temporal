package adaptive

import (
	"context"
	"time"

	"go.temporal.io/server/common/quotas"
)

type (
	// TenantRateLimiter presents a Limiter as a quotas.RateLimiter bound to a
	// single tenant key. It is returned by Limiter.For.
	TenantRateLimiter struct {
		limiter  *Limiter
		key      string
		priority int
	}
)

var _ quotas.RateLimiter = (*TenantRateLimiter)(nil)

// For returns a quotas.RateLimiter view of the limiter bound to a single
// tenant key at priority 0.
func (l *Limiter) For(key string) *TenantRateLimiter {
	return l.ForPriority(key, 0)
}

// ForPriority returns a quotas.RateLimiter view of the limiter bound to a
// single tenant key and request priority.
func (l *Limiter) ForPriority(key string, priority int) *TenantRateLimiter {
	return &TenantRateLimiter{
		limiter:  l,
		key:      key,
		priority: priority,
	}
}

// Allow implements quotas.RateLimiter.
func (t *TenantRateLimiter) Allow() bool {
	return t.limiter.Allow(t.key)
}

// AllowN implements quotas.RateLimiter.
func (t *TenantRateLimiter) AllowN(now time.Time, numToken int) bool {
	return t.limiter.AllowNAt(t.key, numToken, now)
}

// Reserve implements quotas.RateLimiter.
func (t *TenantRateLimiter) Reserve() quotas.Reservation {
	return t.limiter.Reserve(t.key)
}

// ReserveN implements quotas.RateLimiter.
func (t *TenantRateLimiter) ReserveN(now time.Time, numToken int) quotas.Reservation {
	return t.limiter.ReserveNAt(t.key, numToken, t.priority, now)
}

// Wait implements quotas.RateLimiter.
func (t *TenantRateLimiter) Wait(ctx context.Context) error {
	return t.limiter.Wait(ctx, t.key)
}

// WaitN implements quotas.RateLimiter.
func (t *TenantRateLimiter) WaitN(ctx context.Context, numToken int) error {
	return t.limiter.WaitNPriority(ctx, t.key, numToken, t.priority)
}

// Rate implements quotas.RateLimiter.
func (t *TenantRateLimiter) Rate() float64 {
	return t.limiter.Rate()
}

// Burst implements quotas.RateLimiter.
func (t *TenantRateLimiter) Burst() int {
	return t.limiter.cfg.PartitionBurst
}

// TokensAt implements quotas.RateLimiter.
func (t *TenantRateLimiter) TokensAt(tm time.Time) int {
	return t.limiter.TokensAt(t.key, tm)
}

// RecycleToken implements quotas.RateLimiter.
func (t *TenantRateLimiter) RecycleToken() {
	t.limiter.RecycleToken(t.key)
}
