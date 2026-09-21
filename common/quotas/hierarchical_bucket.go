package quotas

import (
	"math"
	"time"
)

type (
	// hierarchicalBucket is one node of a HierarchicalRateLimiter tree.
	// Buckets are lazily refilled and may spend into debt up to their
	// borrow limit; debt is retired by future refills before the balance
	// grows again. All methods must be called with the owning limiter's
	// lock held.
	hierarchicalBucket struct {
		name        string
		level       int
		rate        float64 // tokens per second
		burst       float64
		tokens      float64
		debt        float64 // overdraft against future refill
		borrowLimit float64 // max outstanding debt
		lastRefill  time.Time
		lastGrant   time.Time // last successful grant, for idle pruning
		parent      *hierarchicalBucket
		children    map[string]*hierarchicalBucket
	}

	// bucketCredit records tokens charged to one bucket by a grant, so a
	// reservation cancel can refund exactly the nodes that paid.
	bucketCredit struct {
		bucket *hierarchicalBucket
		amount float64
	}
)

func newHierarchicalBucket(
	name string,
	level int,
	cfg HierarchicalLevelConfig,
	parent *hierarchicalBucket,
	now time.Time,
) *hierarchicalBucket {
	return &hierarchicalBucket{
		name:        name,
		level:       level,
		rate:        cfg.Rate,
		burst:       float64(cfg.Burst),
		tokens:      float64(cfg.Burst), // buckets start full
		borrowLimit: float64(cfg.BorrowLimit),
		lastRefill:  now,
		lastGrant:   now,
		parent:      parent,
	}
}

// childLocked returns the named child bucket, creating it from cfg when it
// does not yet exist.
func (n *hierarchicalBucket) childLocked(name string, cfg HierarchicalLevelConfig, now time.Time) *hierarchicalBucket {
	if n.children == nil {
		n.children = make(map[string]*hierarchicalBucket)
	}
	if c, ok := n.children[name]; ok {
		return c
	}
	c := newHierarchicalBucket(name, n.level+1, cfg, n, now)
	n.children[name] = c
	return c
}

// refillLocked applies lazy token refill up to now. Refill first retires
// outstanding debt; only the remainder grows the balance.
func (n *hierarchicalBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(n.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	n.lastRefill = now
	incoming := elapsed * n.rate
	if n.debt > 0 {
		repay := math.Min(n.debt, incoming)
		n.debt -= repay
		incoming -= repay
	}
	n.tokens = math.Min(n.tokens+incoming, n.burst)
}

// grantableLocked reports the largest single grant this bucket can cover
// right now: current balance plus remaining overdraft headroom.
func (n *hierarchicalBucket) grantableLocked(now time.Time) float64 {
	n.refillLocked(now)
	return n.tokens + math.Max(0, n.borrowLimit-n.debt)
}

// consumeLocked charges tokens against the bucket, spending into debt up
// to the borrow limit. Callers must check grantableLocked first; consume
// may exceed the limit only when feasibility was not verified.
func (n *hierarchicalBucket) consumeLocked(tokens float64, now time.Time) {
	n.refillLocked(now)
	use := math.Min(n.tokens, tokens)
	n.tokens -= use
	n.debt += tokens - use
	n.lastGrant = now
}

// refundLocked returns tokens previously charged to the bucket, retiring
// debt before restoring balance. Refunds in excess of the bucket's burst
// are dropped, matching ordinary refill clamping.
func (n *hierarchicalBucket) refundLocked(amount float64, now time.Time) {
	n.refillLocked(now)
	repay := math.Min(n.debt, amount)
	n.debt -= repay
	n.tokens = math.Min(n.tokens+amount-repay, n.burst)
}

// updateLocked applies a new level configuration. Debt and limits are
// re-expressed in the new units; existing balances are preserved but
// clamped to the new burst.
func (n *hierarchicalBucket) updateLocked(cfg HierarchicalLevelConfig, now time.Time) {
	n.refillLocked(now)
	n.rate = cfg.Rate
	n.burst = float64(cfg.Burst)
	n.borrowLimit = float64(cfg.BorrowLimit)
	n.tokens = math.Min(n.tokens, n.burst)
	if n.debt > n.borrowLimit {
		// A limit shrink strands existing debt; it still retires through
		// refill, but no new borrowing is possible until it does.
		n.borrowLimit = n.debt
	}
}

// estimateWaitLocked approximates the delay until a grant of tokens could
// succeed, ignoring sibling contention at other levels. It is advisory
// only (for reservation.DelayFrom) and may underestimate when debt
// retirement and balance accrual interleave.
func (n *hierarchicalBucket) estimateWaitLocked(tokens float64, now time.Time) time.Duration {
	n.refillLocked(now)
	headroom := n.tokens + math.Max(0, n.borrowLimit-n.debt)
	shortfall := tokens - headroom
	if shortfall <= 0 {
		return 0
	}
	if n.rate <= 0 {
		// No refill path; report a day as an effectively-infinite wait.
		return 24 * time.Hour
	}
	// The bucket must first repay debt, then accrue the shortfall.
	return time.Duration((n.debt + shortfall) / n.rate * float64(time.Second))
}

// pruneIdleLocked removes descendant buckets that have been idle (no
// grant) longer than cutoff, carry no debt, and have no live children of
// their own. Returns the number of buckets removed.
func (n *hierarchicalBucket) pruneIdleLocked(cutoff time.Duration, now time.Time) int {
	removed := 0
	for name, c := range n.children {
		removed += c.pruneIdleLocked(cutoff, now)
		c.refillLocked(now)
		if len(c.children) == 0 && c.debt == 0 && now.Sub(c.lastGrant) > cutoff {
			delete(n.children, name)
			removed++
		}
	}
	return removed
}

// describeLocked builds a snapshot of this bucket and its descendants.
func (n *hierarchicalBucket) describeLocked(now time.Time) HierarchicalNodeInfo {
	n.refillLocked(now)
	info := HierarchicalNodeInfo{
		Name:        n.name,
		Level:       n.level,
		Rate:        n.rate,
		Burst:       int64(n.burst),
		Tokens:      n.tokens,
		Debt:        n.debt,
		BorrowLimit: int64(n.borrowLimit),
		LastGrant:   n.lastGrant,
	}
	for name, c := range n.children {
		info.Children = append(info.Children, childSummary{Name: name, Node: c.describeLocked(now)})
	}
	return info
}
