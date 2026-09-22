package adaptive

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	// Limiter is a multi-tenant rate limiter whose effective rate adapts to a
	// congestion signal via an AIMD controller. Each tenant key gets an
	// independent partition with its own token bucket and reservation queue.
	// The zero value is not usable; construct with NewLimiter.
	//
	// Limiter is safe for concurrent use.
	Limiter struct {
		cfg     Config
		clock   clock.TimeSource
		metrics *limiterMetrics
		aimd    *aimdController

		mu     sync.Mutex
		parts  map[string]*partition
		seq    uint64
		closed bool
	}

	// Option configures optional Limiter dependencies.
	Option func(*Limiter)
)

// WithTimeSource overrides the time source, which is useful in tests.
func WithTimeSource(ts clock.TimeSource) Option {
	return func(l *Limiter) {
		l.clock = ts
	}
}

// WithMetricsHandler overrides the metrics handler. The default is
// metrics.NoopMetricsHandler.
func WithMetricsHandler(h metrics.Handler) Option {
	return func(l *Limiter) {
		l.metrics = newLimiterMetrics(h)
	}
}

// NewLimiter returns a Limiter with the given config. Missing optional
// dependencies default to the real wall clock and a no-op metrics handler.
func NewLimiter(cfg Config, opts ...Option) (*Limiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	l := &Limiter{
		cfg:     cfg,
		clock:   clock.NewRealTimeSource(),
		metrics: newLimiterMetrics(nil),
		aimd:    newAIMDController(cfg),
		parts:   make(map[string]*partition),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.metrics.recordRate(l.effectiveRate())
	l.metrics.recordMultiplier(l.aimd.multiplier)
	return l, nil
}

// effectiveRate returns the per-partition rate after the AIMD multiplier.
func (l *Limiter) effectiveRate() float64 {
	return l.cfg.PartitionRPS * l.aimd.multiplier
}

// Multiplier returns the current AIMD multiplier.
func (l *Limiter) Multiplier() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.aimd.multiplier
}

// Rate returns the current effective per-partition rate in tokens per second.
func (l *Limiter) Rate() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.effectiveRate()
}

// Burst returns the per-partition burst capacity.
func (l *Limiter) Burst() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg.PartitionBurst
}

// NumPartitions returns the number of live partitions.
func (l *Limiter) NumPartitions() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.parts)
}

// TokensAt returns the number of tokens the partition for key will have at
// time t, which may be negative when reservations are scheduled ahead.
func (l *Limiter) TokensAt(key string, t time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.parts[key]
	if p == nil {
		return l.cfg.PartitionBurst
	}
	return int(p.bucket.tokensAt(t))
}

// Allow attempts to serve one token for key immediately.
func (l *Limiter) Allow(key string) bool {
	return l.AllowNAt(key, 1, l.clock.Now())
}

// AllowN attempts to serve n tokens for key immediately. It does not queue a
// reservation: the request is denied unless the partition bucket already has
// enough tokens.
func (l *Limiter) AllowN(key string, n int) bool {
	return l.AllowNAt(key, n, l.clock.Now())
}

// AllowNAt is AllowN evaluated at the given time.
func (l *Limiter) AllowNAt(key string, n int, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	p := l.partitionFor(key, now)
	if p.bucket.tokensAt(now) < float64(n) {
		l.metrics.recordDenied()
		return false
	}
	p.bucket.charge(now, float64(n))
	l.metrics.recordAllowed()
	return true
}

// Reserve returns a reservation for one token at priority 0.
func (l *Limiter) Reserve(key string) quotas.Reservation {
	return l.ReserveNAt(key, 1, 0, l.clock.Now())
}

// ReserveN returns a reservation for n tokens at the given priority.
func (l *Limiter) ReserveN(key string, n int, priority int) quotas.Reservation {
	return l.ReserveNAt(key, n, priority, l.clock.Now())
}

// ReserveNAt is ReserveN evaluated at the given time. If the request cannot
// be served immediately, it is scheduled for the earliest grant time
// consistent with the partition's committed debt, or denied when that time is
// further than Config.MaxWait out.
func (l *Limiter) ReserveNAt(key string, n int, priority int, now time.Time) quotas.Reservation {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.reserveNLocked(key, n, priority, now)
	if r.ok {
		l.metrics.recordAllowed()
	} else {
		l.metrics.recordDenied()
	}
	return r
}

func (l *Limiter) reserveNLocked(key string, n int, priority int, now time.Time) *reservation {
	if l.closed {
		return &reservation{
			limiter:   l,
			tokens:    n,
			heapIndex: -1,
			done:      closedChan(),
		}
	}
	l.seq++
	p := l.partitionFor(key, now)
	return p.reserve(now, n, l.cfg.weightFor(priority), l.seq)
}

// Wait blocks until one token is granted for key or ctx expires.
func (l *Limiter) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n tokens are granted for key at priority 0 or ctx
// expires.
func (l *Limiter) WaitN(ctx context.Context, key string, n int) error {
	return l.WaitNPriority(ctx, key, n, 0)
}

// WaitNPriority blocks until n tokens are granted for key at the given
// priority or ctx expires. On context expiry the reservation is canceled so
// its tokens are returned to the partition.
func (l *Limiter) WaitNPriority(ctx context.Context, key string, n int, priority int) error {
	now := l.clock.Now()
	r := l.ReserveNAt(key, n, priority, now)
	if !r.OK() {
		return ErrRateLimitExceeded
	}
	if d := r.DelayFrom(now); d <= 0 {
		return nil
	}
	res, ok := r.(*reservation)
	if !ok {
		return nil
	}
	select {
	case <-res.done:
		l.metrics.recordWaitLatency(l.clock.Now().Sub(now))
		l.mu.Lock()
		granted := res.granted && !res.canceled
		l.mu.Unlock()
		if granted {
			return nil
		}
		return ErrReservationCanceled
	case <-ctx.Done():
		r.Cancel()
		return ctx.Err()
	}
}

// RecycleToken returns one token to the partition for key, pulling pending
// reservations earlier. Use when a granted request was not actually
// performed.
func (l *Limiter) RecycleToken(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	now := l.clock.Now()
	p := l.parts[key]
	if p == nil {
		return
	}
	p.bucket.restore(now, 1)
	p.touch(now)
	p.retimeline(now)
}

// ReportSignal feeds a congestion signal sample into the AIMD controller. The
// signal is clamped to [0, 1]; values above the high watermark shrink the
// effective rate, values below the low watermark grow it. When the effective
// rate changes, pending reservations in every partition are re-timelined.
func (l *Limiter) ReportSignal(signal float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	multiplier, changed := l.aimd.report(signal)
	l.metrics.recordMultiplier(multiplier)
	if !changed {
		return
	}
	l.applyRateLocked()
}

// UpdateBaseRate changes the base per-partition rate, e.g. when the dynamic
// config value changes, and re-timelines pending reservations.
func (l *Limiter) UpdateBaseRate(rps float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || rps <= 0 || rps == l.cfg.PartitionRPS {
		return
	}
	l.cfg.PartitionRPS = rps
	l.applyRateLocked()
}

// UpdateBurst changes the partition burst capacity.
func (l *Limiter) UpdateBurst(burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || burst <= 0 || burst == l.cfg.PartitionBurst {
		return
	}
	now := l.clock.Now()
	l.cfg.PartitionBurst = burst
	for _, p := range l.parts {
		p.bucket.setCapacity(float64(burst), now)
	}
}

// applyRateLocked pushes the current effective rate into every partition and
// re-timelines its pending reservations. Called with the lock held.
func (l *Limiter) applyRateLocked() {
	now := l.clock.Now()
	rate := l.effectiveRate()
	for _, p := range l.parts {
		p.bucket.setRate(rate, now)
		p.retimeline(now)
	}
	l.metrics.recordRate(rate)
}

// partitionFor returns the partition for key, creating it if necessary. When
// the partition table is full the least recently used partition is evicted.
// Called with the lock held.
func (l *Limiter) partitionFor(key string, now time.Time) *partition {
	p := l.parts[key]
	if p != nil {
		p.touch(now)
		return p
	}
	if len(l.parts) >= l.cfg.MaxPartitions {
		l.evictLRU()
	}
	p = newPartition(l, key, now)
	l.parts[key] = p
	l.metrics.recordPartitions(len(l.parts))
	return p
}

// evictLRU evicts the least recently used partition. Called with the lock
// held.
func (l *Limiter) evictLRU() {
	var oldest *partition
	for _, p := range l.parts {
		if oldest == nil || p.lastUsed.Before(oldest.lastUsed) {
			oldest = p
		}
	}
	if oldest != nil {
		l.evictPartition(oldest)
	}
}

// evictPartition removes p from the table and releases its reservations.
// Called with the lock held.
func (l *Limiter) evictPartition(p *partition) {
	if p.evicted {
		return
	}
	delete(l.parts, p.key)
	p.evict()
	l.metrics.recordPartitionEvicted()
	l.metrics.recordPartitions(len(l.parts))
}

// Close stops all partition timers and cancels pending reservations. Later
// operations return ErrLimiterClosed or are denied.
func (l *Limiter) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	for _, p := range l.parts {
		p.evict()
	}
	l.parts = make(map[string]*partition)
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
