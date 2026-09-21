package quotas

import (
	"container/heap"
	"time"
)

const (
	// dispatchCompactThreshold triggers compaction of a dispatch queue's
	// item slice when the consumed head region grows large; without it
	// long-lived queues would retain a large dead prefix forever.
	dispatchCompactThreshold = 128
)

type (
	// DispatchItem is a unit of work submitted to a
	// WeightedFairDispatcher. Cost is the item's relative work units; an
	// item's share of dispatch bandwidth is proportional to weight/cost.
	DispatchItem struct {
		Key        string
		Value      any
		Cost       float64
		EnqueuedAt time.Time
	}

	// QueueStats is a point-in-time snapshot of one dispatch queue.
	QueueStats struct {
		Key           string
		Depth         int
		Weight        float64
		VirtualStart  float64
		VirtualFinish float64
		Active        bool
		Served        uint64
		TotalCost     float64
		LastServed    time.Time
	}

	// dispatchQueue is a per-key FIFO plus the scheduler bookkeeping used
	// by the weighted-fair dispatcher. Virtual times are expressed in
	// normalized service units (cost/weight).
	dispatchQueue struct {
		key           string
		items         []DispatchItem
		head          int
		weight        float64
		virtualStart  float64
		virtualFinish float64
		active        bool // present in the dispatch heap
		heapIndex     int
		seq           uint64 // activation order, tie-breaks equal finish times
		served        uint64
		totalCost     float64
		lastServed    time.Time
	}

	// dispatchHeap orders active queues by head item virtual finish time,
	// then by activation sequence so ties dispatch FIFO.
	dispatchHeap []*dispatchQueue
)

func newDispatchQueue(key string, weight float64) *dispatchQueue {
	return &dispatchQueue{
		key:       key,
		weight:    weight,
		heapIndex: -1,
	}
}

func (q *dispatchQueue) len() int {
	return len(q.items) - q.head
}

func (q *dispatchQueue) empty() bool {
	return q.head >= len(q.items)
}

func (q *dispatchQueue) peek() DispatchItem {
	return q.items[q.head]
}

func (q *dispatchQueue) push(item DispatchItem) {
	q.items = append(q.items, item)
	q.totalCost += item.Cost
}

func (q *dispatchQueue) pop() DispatchItem {
	item := q.items[q.head]
	q.items[q.head] = DispatchItem{} // release the value reference
	q.head++
	q.served++
	if q.empty() {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head >= dispatchCompactThreshold && q.head*2 >= len(q.items) {
		q.items = append(q.items[:0], q.items[q.head:]...)
		q.head = 0
	}
	return item
}

func (q *dispatchQueue) drain() []DispatchItem {
	rest := append([]DispatchItem(nil), q.items[q.head:]...)
	q.items = q.items[:0]
	q.head = 0
	return rest
}

func (q *dispatchQueue) snapshot() QueueStats {
	return QueueStats{
		Key:           q.key,
		Depth:         q.len(),
		Weight:        q.weight,
		VirtualStart:  q.virtualStart,
		VirtualFinish: q.virtualFinish,
		Active:        q.active,
		Served:        q.served,
		TotalCost:     q.totalCost,
		LastServed:    q.lastServed,
	}
}

func (h dispatchHeap) Len() int {
	return len(h)
}

func (h dispatchHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.virtualFinish != b.virtualFinish {
		return a.virtualFinish < b.virtualFinish
	}
	return a.seq < b.seq
}

func (h dispatchHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *dispatchHeap) Push(x any) {
	q, ok := x.(*dispatchQueue)
	if !ok {
		return
	}
	q.heapIndex = len(*h)
	*h = append(*h, q)
}

func (h *dispatchHeap) Pop() any {
	old := *h
	n := len(old)
	q := old[n-1]
	old[n-1] = nil
	q.heapIndex = -1
	*h = old[:n-1]
	return q
}

var _ heap.Interface = (*dispatchHeap)(nil)
