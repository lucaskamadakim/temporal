package quotas

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"

	"go.temporal.io/server/common/clock"
)

var (
	_ WeightedFairDispatcher = (*weightedFairDispatcherImpl)(nil)

	errDispatcherClosed = errors.New("dispatcher is closed")
)

type (
	// WeightedFairDispatcher drains per-key FIFO queues in weighted-fair
	// order. Scheduling follows a WF2Q-style virtual-time discipline: each
	// queue carries a virtual start/finish window for its head item, the
	// heap serves the smallest virtual finish, and the dispatcher's
	// virtual time tracks the finish of the last item served.
	//
	// A queue that goes empty deactivates; when an item arrives it is
	// re-anchored to the dispatcher virtual time so a long-idle key cannot
	// claim catch-up credit for the service others consumed while it was
	// gone.
	//
	// An optional HierarchicalRateLimiter gates dispatch: a key whose
	// quota is exhausted is skipped for that round without losing its
	// ordering.
	WeightedFairDispatcher interface {
		// Enqueue adds item to its key's queue. It returns false when the
		// key or global limits are exceeded, or after Close.
		Enqueue(item DispatchItem) bool
		// Dispatch removes and returns the next item in fair order.
		Dispatch() (DispatchItem, bool)
		// DispatchN removes up to n items, stopping early when no
		// eligible queue remains.
		DispatchN(n int) []DispatchItem
		// Peek returns the head item for key without removing it.
		Peek(key string) (DispatchItem, bool)
		// Len returns the depth of one key's queue.
		Len(key string) int
		// Depth returns the total number of queued items across keys.
		Depth() int
		// SetWeight changes a key's scheduling weight. Values below
		// MinWeight are clamped.
		SetWeight(key string, weight float64)
		// Weight returns a key's current weight, or the default when the
		// key has no queue.
		Weight(key string) float64
		// Remove drops a key's queue and returns its pending items.
		Remove(key string) []DispatchItem
		// Keys lists all keys with live queues.
		Keys() []string
		// Stats snapshots one key's queue.
		Stats(key string) (QueueStats, bool)
		// DispatcherStats returns aggregate counters.
		DispatcherStats() DispatcherStats
		// Close stops accepting new items; pending items remain
		// dispatchable.
		Close()
	}

	// DispatcherStats aggregates dispatcher-level counters.
	DispatcherStats struct {
		Keys    int
		Depth   int
		Served  uint64
		Dropped uint64
		Gated   uint64
		Closed  bool
	}

	// WeightedFairDispatcherConfig configures a WeightedFairDispatcher.
	WeightedFairDispatcherConfig struct {
		// MaxDepthPerKey bounds per-key queue length; 0 means unbounded.
		MaxDepthPerKey int
		// MaxKeys bounds the number of distinct live keys; 0 means
		// unbounded.
		MaxKeys int
		// DefaultWeight is applied to keys without an explicit weight.
		DefaultWeight float64
		// MinWeight clamps SetWeight so a key can never be fully starved
		// by configuration.
		MinWeight float64
		// Gate, when non-nil, is consulted before each dispatch; a denied
		// gate skips the key for the round.
		Gate HierarchicalRateLimiter
		// GatePathFn maps a dispatch key to a limiter path; nil uses the
		// key itself as a single-element path.
		GatePathFn func(key string) []string
		// GateCost is charged to the gate per dispatched item; <=0
		// defaults to 1.
		GateCost int64
		// Metrics receives dispatch events; all fields optional.
		Metrics FairDispatchMetrics
	}

	weightedFairDispatcherImpl struct {
		sync.Mutex
		cfg         WeightedFairDispatcherConfig
		queues      map[string]*dispatchQueue
		heap        dispatchHeap
		virtualTime float64
		seq         uint64
		served      uint64
		dropped     uint64
		gated       uint64
		closed      bool
		timeSource  clock.TimeSource
		observers   FairDispatchMetrics
	}
)

// NewWeightedFairDispatcher builds a dispatcher. cfg.MinWeight,
// cfg.DefaultWeight and cfg.GateCost are defaulted when non-positive.
func NewWeightedFairDispatcher(
	cfg WeightedFairDispatcherConfig,
	timeSource clock.TimeSource,
) *weightedFairDispatcherImpl {
	if cfg.DefaultWeight <= 0 {
		cfg.DefaultWeight = 1
	}
	if cfg.MinWeight <= 0 {
		cfg.MinWeight = 0.01
	}
	if cfg.DefaultWeight < cfg.MinWeight {
		cfg.DefaultWeight = cfg.MinWeight
	}
	if cfg.GateCost <= 0 {
		cfg.GateCost = 1
	}
	if cfg.GatePathFn == nil {
		cfg.GatePathFn = func(key string) []string { return []string{key} }
	}
	if timeSource == nil {
		timeSource = clock.NewRealTimeSource()
	}
	return &weightedFairDispatcherImpl{
		cfg:        cfg,
		queues:     make(map[string]*dispatchQueue),
		timeSource: timeSource,
		observers:  cfg.Metrics,
	}
}

func (d *weightedFairDispatcherImpl) Enqueue(item DispatchItem) bool {
	d.Lock()
	defer d.Unlock()
	if d.closed {
		return false
	}
	if item.Cost <= 0 {
		// Non-positive costs would let a queue drag its virtual finish
		// backwards and monopolize dispatch; treat as minimum work.
		item.Cost = 1
	}
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = d.timeSource.Now()
	}
	q := d.queues[item.Key]
	if q == nil {
		if d.cfg.MaxKeys > 0 && len(d.queues) >= d.cfg.MaxKeys {
			d.dropped++
			emitCounter(d.observers.Dropped, 1)
			return false
		}
		q = newDispatchQueue(item.Key, d.cfg.DefaultWeight)
		d.queues[item.Key] = q
	}
	if d.cfg.MaxDepthPerKey > 0 && q.len() >= d.cfg.MaxDepthPerKey {
		d.dropped++
		emitCounter(d.observers.Dropped, 1)
		return false
	}
	q.push(item)
	emitCounter(d.observers.Enqueued, 1)
	if !q.active {
		d.activateLocked(q)
	}
	return true
}

// activateLocked anchors a newly non-empty queue in the schedule and
// pushes it into the heap. The virtual start is clamped to the dispatcher
// virtual time so idle queues cannot bank service credit.
func (d *weightedFairDispatcherImpl) activateLocked(q *dispatchQueue) {
	d.seq++
	q.seq = d.seq
	q.virtualStart = math.Min(q.virtualFinish, d.virtualTime)
	q.virtualFinish = q.virtualStart + q.peek().Cost/q.weight
	q.active = true
	heap.Push(&d.heap, q)
}

func (d *weightedFairDispatcherImpl) Dispatch() (DispatchItem, bool) {
	d.Lock()
	defer d.Unlock()
	return d.dispatchLocked()
}

func (d *weightedFairDispatcherImpl) dispatchLocked() (DispatchItem, bool) {
	// Queues skipped by the gate are collected and re-pushed so a denied
	// key yields the round without losing its place.
	var skipped []*dispatchQueue
	defer func() {
		for _, q := range skipped {
			heap.Push(&d.heap, q)
		}
	}()
	for d.heap.Len() > 0 {
		q, ok := heap.Pop(&d.heap).(*dispatchQueue)
		if !ok {
			continue
		}
		if !d.gateAllowsLocked(q.key) {
			d.gated++
			emitCounter(d.observers.Gated, 1)
			skipped = append(skipped, q)
			continue
		}
		item := q.pop()
		d.virtualTime = math.Max(d.virtualTime, q.virtualFinish)
		q.lastServed = d.timeSource.Now()
		if q.empty() {
			q.active = false
		} else {
			q.virtualStart = q.virtualFinish
			q.virtualFinish = q.virtualStart + q.peek().Cost/q.weight
			heap.Push(&d.heap, q)
		}
		d.served++
		emitCounter(d.observers.Served, 1)
		return item, true
	}
	return DispatchItem{}, false
}

// gateAllowsLocked reports whether key may dispatch under the configured
// gate; with no gate every key is eligible.
func (d *weightedFairDispatcherImpl) gateAllowsLocked(key string) bool {
	if d.cfg.Gate == nil {
		return true
	}
	return d.cfg.Gate.Allow(HierarchicalRequest{
		Path:   d.cfg.GatePathFn(key),
		Tokens: d.cfg.GateCost,
	})
}

func (d *weightedFairDispatcherImpl) DispatchN(n int) []DispatchItem {
	if n <= 0 {
		return nil
	}
	d.Lock()
	defer d.Unlock()
	items := make([]DispatchItem, 0, n)
	for i := 0; i < n; i++ {
		item, ok := d.dispatchLocked()
		if !ok {
			break
		}
		items = append(items, item)
	}
	return items
}

func (d *weightedFairDispatcherImpl) Peek(key string) (DispatchItem, bool) {
	d.Lock()
	defer d.Unlock()
	q := d.queues[key]
	if q == nil || q.empty() {
		return DispatchItem{}, false
	}
	return q.peek(), true
}

func (d *weightedFairDispatcherImpl) Len(key string) int {
	d.Lock()
	defer d.Unlock()
	if q := d.queues[key]; q != nil {
		return q.len()
	}
	return 0
}

func (d *weightedFairDispatcherImpl) Depth() int {
	d.Lock()
	defer d.Unlock()
	total := 0
	for _, q := range d.queues {
		total += q.len()
	}
	return total
}

func (d *weightedFairDispatcherImpl) SetWeight(key string, weight float64) {
	d.Lock()
	defer d.Unlock()
	weight = math.Max(weight, d.cfg.MinWeight)
	q := d.queues[key]
	if q == nil {
		q = newDispatchQueue(key, weight)
		d.queues[key] = q
		return
	}
	q.weight = weight
	if q.active && !q.empty() {
		q.virtualFinish = q.virtualStart + q.peek().Cost/q.weight
		heap.Fix(&d.heap, q.heapIndex)
	}
}

func (d *weightedFairDispatcherImpl) Weight(key string) float64 {
	d.Lock()
	defer d.Unlock()
	if q := d.queues[key]; q != nil {
		return q.weight
	}
	return d.cfg.DefaultWeight
}

func (d *weightedFairDispatcherImpl) Remove(key string) []DispatchItem {
	d.Lock()
	defer d.Unlock()
	q := d.queues[key]
	if q == nil {
		return nil
	}
	if q.active {
		heap.Remove(&d.heap, q.heapIndex)
		q.active = false
	}
	delete(d.queues, key)
	return q.drain()
}

func (d *weightedFairDispatcherImpl) Keys() []string {
	d.Lock()
	defer d.Unlock()
	keys := make([]string, 0, len(d.queues))
	for k, q := range d.queues {
		if !q.empty() {
			keys = append(keys, k)
		}
	}
	return keys
}

func (d *weightedFairDispatcherImpl) Stats(key string) (QueueStats, bool) {
	d.Lock()
	defer d.Unlock()
	q := d.queues[key]
	if q == nil {
		return QueueStats{}, false
	}
	return q.snapshot(), true
}

func (d *weightedFairDispatcherImpl) DispatcherStats() DispatcherStats {
	d.Lock()
	defer d.Unlock()
	return DispatcherStats{
		Keys:    len(d.queues),
		Depth:   d.depthLocked(),
		Served:  d.served,
		Dropped: d.dropped,
		Gated:   d.gated,
		Closed:  d.closed,
	}
}

func (d *weightedFairDispatcherImpl) depthLocked() int {
	total := 0
	for _, q := range d.queues {
		total += q.len()
	}
	return total
}

func (d *weightedFairDispatcherImpl) Close() {
	d.Lock()
	defer d.Unlock()
	d.closed = true
}

// Err returns the close error, for callers that prefer error-typed
// lifecycle checks; nil while open.
func (d *weightedFairDispatcherImpl) Err() error {
	d.Lock()
	defer d.Unlock()
	if d.closed {
		return errDispatcherClosed
	}
	return nil
}

// describeLocked formats dispatcher state for debugging; intentionally
// unexported and unused by production paths.
func (d *weightedFairDispatcherImpl) describeLocked() string {
	out := fmt.Sprintf("virtualTime=%f queues=%d\n", d.virtualTime, len(d.queues))
	for _, q := range d.queues {
		out += fmt.Sprintf("  %s depth=%d w=%.3f vs=%.4f vf=%.4f active=%v served=%d\n",
			q.key, q.len(), q.weight, q.virtualStart, q.virtualFinish, q.active, q.served)
	}
	return out
}
