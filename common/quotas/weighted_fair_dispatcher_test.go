package quotas

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func newTestDispatcher(cfg WeightedFairDispatcherConfig) *weightedFairDispatcherImpl {
	return NewWeightedFairDispatcher(cfg, clock.NewEventTimeSource())
}

func dispatchAll(d WeightedFairDispatcher) []DispatchItem {
	var out []DispatchItem
	for {
		item, ok := d.Dispatch()
		if !ok {
			return out
		}
		out = append(out, item)
	}
}

func countByKey(items []DispatchItem) map[string]int {
	counts := make(map[string]int)
	for _, it := range items {
		counts[it.Key]++
	}
	return counts
}

func TestWeightedFairDispatcher_EqualWeightsInterleave(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	for i := 0; i < 30; i++ {
		require.True(t, d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1}))
	}
	for i := 0; i < 30; i++ {
		require.True(t, d.Enqueue(DispatchItem{Key: "b", Value: i, Cost: 1}))
	}
	items := dispatchAll(d)
	require.Len(t, items, 60)
	counts := countByKey(items)
	require.Equal(t, 30, counts["a"])
	require.Equal(t, 30, counts["b"])
	// Equal weights and equal costs alternate strictly once both queues
	// are active.
	for i := 2; i < len(items); i += 2 {
		require.Equal(t, items[i-2].Key, items[i].Key, "position %d", i)
	}
}

func TestWeightedFairDispatcher_WeightedShare(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.SetWeight("a", 1)
	d.SetWeight("b", 3)
	for i := 0; i < 200; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1})
		d.Enqueue(DispatchItem{Key: "b", Value: i, Cost: 1})
	}
	items := d.DispatchN(80)
	require.Len(t, items, 80)
	counts := countByKey(items)
	// Ideal WFQ share of early dispatches: a gets 1/4, b gets 3/4.
	require.InDelta(t, 20, counts["a"], 4)
	require.InDelta(t, 60, counts["b"], 4)
}

func TestWeightedFairDispatcher_PerKeyFIFOPreserved(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	for i := 0; i < 10; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1})
		d.Enqueue(DispatchItem{Key: "b", Value: 100 + i, Cost: 1})
	}
	items := dispatchAll(d)
	var aVals, bVals []int
	for _, it := range items {
		if it.Key == "a" {
			aVals = append(aVals, it.Value.(int))
		} else {
			bVals = append(bVals, it.Value.(int))
		}
	}
	require.True(t, slices.IsSortedFunc(aVals, cmp.Compare))
	require.True(t, slices.IsSortedFunc(bVals, cmp.Compare))
}

func TestWeightedFairDispatcher_ExpensiveItemsYield(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.Enqueue(DispatchItem{Key: "a", Value: "big", Cost: 10})
	d.Enqueue(DispatchItem{Key: "b", Value: "small1", Cost: 1})
	d.Enqueue(DispatchItem{Key: "b", Value: "small2", Cost: 1})
	items := dispatchAll(d)
	require.Len(t, items, 3)
	// b's cheap head (finish 1) beats a's expensive head (finish 10).
	require.Equal(t, "small1", items[0].Value)
	require.Equal(t, "small2", items[1].Value)
	require.Equal(t, "big", items[2].Value)
}

func TestWeightedFairDispatcher_IdleKeyResumes(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	for i := 0; i < 3; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: fmt.Sprintf("a%d", i), Cost: 1})
	}
	for i := 0; i < 40; i++ {
		d.Enqueue(DispatchItem{Key: "b", Value: fmt.Sprintf("b%d", i), Cost: 1})
	}
	// Drain a; keep b busy so dispatcher virtual time advances.
	got := d.DispatchN(3)
	require.Len(t, got, 3)
	require.Len(t, d.DispatchN(20), 20)
	// a returns with more work; every item must still dispatch and both
	// keys make progress.
	for i := 0; i < 3; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: fmt.Sprintf("a-resume-%d", i), Cost: 1})
	}
	rest := dispatchAll(d)
	require.Len(t, rest, 23)
	counts := countByKey(rest)
	require.Equal(t, 3, counts["a"])
	require.Equal(t, 20, counts["b"])
	// Per-key order preserved across the idle gap.
	var aVals []string
	for _, it := range rest {
		if it.Key == "a" {
			aVals = append(aVals, it.Value.(string))
		}
	}
	require.Equal(t, []string{"a-resume-0", "a-resume-1", "a-resume-2"}, aVals)
}

func TestWeightedFairDispatcher_LateJoinerServed(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	for i := 0; i < 20; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1})
	}
	require.Len(t, d.DispatchN(10), 10)
	for i := 0; i < 5; i++ {
		d.Enqueue(DispatchItem{Key: "c", Value: i, Cost: 1})
	}
	items := dispatchAll(d)
	require.Len(t, items, 15)
	require.Equal(t, 5, countByKey(items)["c"])
}

func TestWeightedFairDispatcher_GateSkipsDeniedKeys(t *testing.T) {
	ts := clock.NewEventTimeSource()
	gate, err := NewHierarchicalRateLimiter(HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 10, Burst: 3, BorrowLimit: 0},
		},
	}, ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	d := NewWeightedFairDispatcher(WeightedFairDispatcherConfig{
		Gate:     gate,
		GateCost: 1,
	}, ts)
	for i := 0; i < 5; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1})
		d.Enqueue(DispatchItem{Key: "b", Value: i, Cost: 1})
	}
	items := dispatchAll(d)
	require.Len(t, items, 6) // each key's 3-token burst
	stats := d.DispatcherStats()
	require.Equal(t, uint64(2), stats.Gated)
	require.Equal(t, 4, stats.Depth)
	// Refill opens more capacity.
	ts.Advance(time.Second)
	items = dispatchAll(d)
	require.Len(t, items, 4)
}

func TestWeightedFairDispatcher_MaxDepthPerKey(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{MaxDepthPerKey: 3})
	for i := 0; i < 3; i++ {
		require.True(t, d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1}))
	}
	require.False(t, d.Enqueue(DispatchItem{Key: "a", Value: 3, Cost: 1}))
	// Other keys unaffected.
	require.True(t, d.Enqueue(DispatchItem{Key: "b", Value: 0, Cost: 1}))
	require.Equal(t, uint64(1), d.DispatcherStats().Dropped)
}

func TestWeightedFairDispatcher_MaxKeys(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{MaxKeys: 2})
	require.True(t, d.Enqueue(DispatchItem{Key: "a", Value: 1, Cost: 1}))
	require.True(t, d.Enqueue(DispatchItem{Key: "b", Value: 1, Cost: 1}))
	require.False(t, d.Enqueue(DispatchItem{Key: "c", Value: 1, Cost: 1}))
}

func TestWeightedFairDispatcher_SetWeightClamps(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{MinWeight: 0.5})
	d.SetWeight("a", 0.0001)
	require.InDelta(t, 0.5, d.Weight("a"), 1e-9)
	d.SetWeight("a", 7)
	require.InDelta(t, 7.0, d.Weight("a"), 1e-9)
	require.InDelta(t, 1.0, d.Weight("missing"), 1e-9) // default
}

func TestWeightedFairDispatcher_PeekAndLen(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	_, ok := d.Peek("a")
	require.False(t, ok)
	d.Enqueue(DispatchItem{Key: "a", Value: "x", Cost: 1})
	d.Enqueue(DispatchItem{Key: "a", Value: "y", Cost: 1})
	require.Equal(t, 2, d.Len("a"))
	item, ok := d.Peek("a")
	require.True(t, ok)
	require.Equal(t, "x", item.Value)
	require.Equal(t, 2, d.Len("a")) // peek does not consume
	require.Equal(t, 2, d.Depth())
}

func TestWeightedFairDispatcher_Remove(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.Enqueue(DispatchItem{Key: "a", Value: 1, Cost: 1})
	d.Enqueue(DispatchItem{Key: "a", Value: 2, Cost: 1})
	d.Enqueue(DispatchItem{Key: "b", Value: 9, Cost: 1})
	d.Dispatch() // consumes a:1 or b:9 depending on tie; seq order -> a first
	rest := d.Remove("a")
	require.Len(t, rest, 1)
	require.Equal(t, 2, rest[0].Value)
	require.Nil(t, d.Remove("a"))
	require.Equal(t, []string{"b"}, d.Keys())
}

func TestWeightedFairDispatcher_Stats(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.SetWeight("a", 2)
	d.Enqueue(DispatchItem{Key: "a", Value: 1, Cost: 4})
	stats, ok := d.Stats("a")
	require.True(t, ok)
	require.Equal(t, "a", stats.Key)
	require.Equal(t, 1, stats.Depth)
	require.InDelta(t, 2.0, stats.Weight, 1e-9)
	require.True(t, stats.Active)
	require.InDelta(t, 2.0, stats.VirtualFinish-stats.VirtualStart, 1e-9) // cost/weight
	_, ok = d.Stats("gone")
	require.False(t, ok)
}

func TestWeightedFairDispatcher_DispatchNPartial(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	for i := 0; i < 7; i++ {
		d.Enqueue(DispatchItem{Key: "a", Value: i, Cost: 1})
	}
	items := d.DispatchN(5)
	require.Len(t, items, 5)
	require.Equal(t, 2, d.Depth())
	require.Empty(t, d.DispatchN(0))
}

func TestWeightedFairDispatcher_Empty(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	_, ok := d.Dispatch()
	require.False(t, ok)
	require.Empty(t, d.DispatchN(3))
	require.Equal(t, 0, d.Depth())
	require.Empty(t, d.Keys())
}

func TestWeightedFairDispatcher_Close(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.Enqueue(DispatchItem{Key: "a", Value: 1, Cost: 1})
	d.Close()
	require.False(t, d.Enqueue(DispatchItem{Key: "a", Value: 2, Cost: 1}))
	require.Error(t, d.Err())
	// Pending items still dispatch after close.
	item, ok := d.Dispatch()
	require.True(t, ok)
	require.Equal(t, 1, item.Value)
	require.True(t, d.DispatcherStats().Closed)
}

func TestWeightedFairDispatcher_CostClamped(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	require.True(t, d.Enqueue(DispatchItem{Key: "a", Cost: -5}))
	stats, _ := d.Stats("a")
	require.InDelta(t, 1.0, stats.TotalCost, 1e-9) // negative cost became 1
}

func TestWeightedFairDispatcher_Concurrent(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Go(func() {
			for i := 0; i < 50; i++ {
				d.Enqueue(DispatchItem{Key: fmt.Sprintf("k%d", w), Value: i, Cost: 1})
				d.Dispatch()
			}
		})
	}
	wg.Wait()
	stats := d.DispatcherStats()
	require.Equal(t, uint64(200), stats.Served+uint64(stats.Depth))
}

func TestWeightedFairDispatcher_GatePathFn(t *testing.T) {
	ts := clock.NewEventTimeSource()
	gate, err := NewHierarchicalRateLimiter(HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 100, Burst: 100},
			{Rate: 1, Burst: 2, BorrowLimit: 0},
		},
	}, ts, HierarchicalRateLimiterMetrics{})
	require.NoError(t, err)
	d := NewWeightedFairDispatcher(WeightedFairDispatcherConfig{
		Gate:       gate,
		GatePathFn: func(key string) []string { return []string{"ns1", key} },
		GateCost:   1,
	}, ts)
	for i := 0; i < 5; i++ {
		d.Enqueue(DispatchItem{Key: "q1", Value: i, Cost: 1})
	}
	items := dispatchAll(d)
	require.Len(t, items, 2) // deepest level burst caps dispatch
	info, ok := gate.Node([]string{"ns1", "q1"})
	require.True(t, ok)
	require.InDelta(t, 0.0, info.Tokens, 1e-9)
}

func TestWeightedFairDispatcher_KeysOnlyNonEmpty(t *testing.T) {
	d := newTestDispatcher(WeightedFairDispatcherConfig{})
	d.Enqueue(DispatchItem{Key: "a", Value: 1, Cost: 1})
	d.SetWeight("ghost", 2) // creates an empty queue entry
	keys := d.Keys()
	require.Equal(t, []string{"a"}, keys)
}
