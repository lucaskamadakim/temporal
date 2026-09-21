package quotas

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

var _ HierarchicalRateLimiter = (*hierarchicalRateLimiterImpl)(nil)

type (
	// HierarchicalRateLimiter enforces quotas along a path of buckets, one
	// per level of a configured tree. A request for path [a, b] consumes
	// tokens at the root bucket, at child a, and at grandchild b: every
	// level independently rate-limits the traffic passing through it.
	//
	// Buckets may overspend into debt up to their configured BorrowLimit;
	// debt is retired by subsequent refills before the balance grows. This
	// lets a leaf briefly exceed its sustained rate while ancestors still
	// bound the aggregate.
	HierarchicalRateLimiter interface {
		// Allow reports whether the request can be satisfied and consumes
		// its tokens when it can.
		Allow(request HierarchicalRequest) bool
		// Reserve consumes tokens like Allow but returns a
		// HierarchicalReservation that can be cancelled to refund the
		// charged buckets.
		Reserve(request HierarchicalRequest) HierarchicalReservation
		// Update re-applies level configuration from a dynamic config
		// change. Existing buckets keep their balances.
		Update(config HierarchicalLimiterConfig) error
		// Node returns a snapshot of the bucket addressed by path, or
		// false when no bucket exists there yet.
		Node(path []string) (HierarchicalNodeInfo, bool)
		// PruneIdle drops buckets with no grants in the last cutoff, no
		// debt, and no children. Returns the number removed.
		PruneIdle(cutoff time.Duration) int
		// Depth returns the configured tree depth including the root.
		Depth() int
	}

	// HierarchicalNodeInfo is an immutable snapshot of one bucket.
	HierarchicalNodeInfo struct {
		Name        string
		Level       int
		Rate        float64
		Burst       int64
		Tokens      float64
		Debt        float64
		BorrowLimit int64
		LastGrant   time.Time
		Children    []childSummary
	}

	childSummary struct {
		Name string
		Node HierarchicalNodeInfo
	}

	hierarchicalRateLimiterImpl struct {
		sync.Mutex
		config     HierarchicalLimiterConfig
		root       *hierarchicalBucket
		timeSource clock.TimeSource
		observers  HierarchicalRateLimiterMetrics
	}
)

// NewHierarchicalRateLimiter builds a limiter from config. The config is
// validated before use; an invalid config returns an error rather than a
// half-initialized limiter.
func NewHierarchicalRateLimiter(
	config HierarchicalLimiterConfig,
	timeSource clock.TimeSource,
	observers HierarchicalRateLimiterMetrics,
) (*hierarchicalRateLimiterImpl, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if timeSource == nil {
		return nil, errors.New("timeSource is required")
	}
	now := timeSource.Now()
	root := newHierarchicalBucket("", 0, config.Levels[0], nil, now)
	return &hierarchicalRateLimiterImpl{
		config:     config,
		root:       root,
		timeSource: timeSource,
		observers:  observers,
	}, nil
}

// pathBucketsLocked resolves the bucket chain for a request, creating any
// missing buckets. The returned slice starts at the root and ends at the
// leaf; its length is path length + 1.
func (l *hierarchicalRateLimiterImpl) pathBucketsLocked(path []string, now time.Time) []*hierarchicalBucket {
	chain := make([]*hierarchicalBucket, 0, len(path)+1)
	node := l.root
	chain = append(chain, node)
	for i, name := range path {
		node = node.childLocked(name, l.config.levelConfig(i+1), now)
		chain = append(chain, node)
	}
	return chain
}

// chargeLocked checks feasibility at every level and consumes when all
// succeed. On success it returns the per-bucket credits so callers can
// build a reservation; on failure nothing is consumed and nil is
// returned.
func (l *hierarchicalRateLimiterImpl) chargeLocked(request HierarchicalRequest, now time.Time) []bucketCredit {
	tokens := float64(request.Tokens)
	if tokens <= 0 {
		return nil
	}
	chain := l.pathBucketsLocked(request.Path, now)
	for _, b := range chain {
		if b.grantableLocked(now) < tokens {
			return nil
		}
	}
	credits := make([]bucketCredit, 0, len(chain))
	for _, b := range chain {
		b.consumeLocked(tokens, now)
		credits = append(credits, bucketCredit{bucket: b, amount: tokens})
	}
	return credits
}

func (l *hierarchicalRateLimiterImpl) Allow(request HierarchicalRequest) bool {
	l.Lock()
	defer l.Unlock()
	now := l.timeSource.Now()
	if l.chargeLocked(request, now) == nil && request.Tokens > 0 {
		emitCounter(l.observers.Denied, 1)
		return false
	}
	return true
}

func (l *hierarchicalRateLimiterImpl) Reserve(request HierarchicalRequest) HierarchicalReservation {
	l.Lock()
	defer l.Unlock()
	now := l.timeSource.Now()
	credits := l.chargeLocked(request, now)
	if credits == nil && request.Tokens > 0 {
		emitCounter(l.observers.Denied, 1)
		return newFailedReservation(l.waitLocked(request, now))
	}
	return newHierarchicalReservation(l, request, credits, now)
}

// waitLocked returns the longest per-level wait estimate along the path.
func (l *hierarchicalRateLimiterImpl) waitLocked(request HierarchicalRequest, now time.Time) time.Duration {
	chain := l.pathBucketsLocked(request.Path, now)
	var wait time.Duration
	for _, b := range chain {
		if w := b.estimateWaitLocked(float64(request.Tokens), now); w > wait {
			wait = w
		}
	}
	return wait
}

func (l *hierarchicalRateLimiterImpl) Update(config HierarchicalLimiterConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if config.Depth() != l.config.Depth() {
		return fmt.Errorf("depth change from %d to %d requires a new limiter", l.config.Depth(), config.Depth())
	}
	l.Lock()
	defer l.Unlock()
	now := l.timeSource.Now()
	l.config = config
	var update func(b *hierarchicalBucket)
	update = func(b *hierarchicalBucket) {
		b.updateLocked(config.levelConfig(b.level), now)
		for _, c := range b.children {
			update(c)
		}
	}
	update(l.root)
	return nil
}

func (l *hierarchicalRateLimiterImpl) Node(path []string) (HierarchicalNodeInfo, bool) {
	l.Lock()
	defer l.Unlock()
	now := l.timeSource.Now()
	node := l.root
	for _, name := range path {
		child, ok := node.children[name]
		if !ok {
			return HierarchicalNodeInfo{}, false
		}
		node = child
	}
	return node.describeLocked(now), true
}

func (l *hierarchicalRateLimiterImpl) PruneIdle(cutoff time.Duration) int {
	l.Lock()
	defer l.Unlock()
	return l.root.pruneIdleLocked(cutoff, l.timeSource.Now())
}

func (l *hierarchicalRateLimiterImpl) Depth() int {
	return l.config.Depth()
}

// refund applies reservation cancellation credits; invoked by
// hierarchicalReservationImpl.Cancel.
func (l *hierarchicalRateLimiterImpl) refund(credits []bucketCredit) {
	l.Lock()
	defer l.Unlock()
	now := l.timeSource.Now()
	for _, c := range credits {
		c.bucket.refundLocked(c.amount, now)
	}
	emitCounter(l.observers.Refunded, 1)
}
