package admission

import (
	"errors"
	"math"
	"sort"
)

var ErrEpsilonRange = errors.New("admission: epsilon must be in (0, 1)")

// gkItem is a single tuple in the Greenwald-Khanna summary: an observed
// value, the gain in rank it contributes (g), and the maximum error in its
// rank (delta).
type gkItem struct {
	value float64
	g     int
	delta int
}

// QuantileSummary is an epsilon-approximate quantile estimator implementing
// the Greenwald-Khanna streaming algorithm. It answers quantile queries with
// a rank error bounded by epsilon times the number of observations while
// storing O(1/epsilon * log(epsilon * N)) tuples.
//
// A QuantileSummary is NOT safe for concurrent use; callers must
// synchronize externally.
type QuantileSummary struct {
	epsilon float64
	items   []gkItem
	n       int
	// compressEvery bounds how many observations are accepted between
	// compression passes. Compression is what bounds memory; running it on
	// every insert would dominate the cost.
	compressEvery int
}

// NewQuantileSummary returns an estimator with the given epsilon rank error.
// Smaller epsilon gives more accurate answers at the cost of memory: with
// epsilon = 0.01 the summary retains on the order of 100 tuples.
func NewQuantileSummary(epsilon float64) (*QuantileSummary, error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, ErrEpsilonRange
	}
	return &QuantileSummary{
		epsilon:       epsilon,
		items:         make([]gkItem, 0, int(1/(2*epsilon))+2),
		compressEvery: int(1 / (2 * epsilon)),
	}, nil
}

// Count returns the number of observations inserted so far.
func (s *QuantileSummary) Count() int {
	return s.n
}

// Len returns the number of retained tuples, i.e. the memory footprint of
// the summary.
func (s *QuantileSummary) Len() int {
	return len(s.items)
}

// Insert adds an observation.
func (s *QuantileSummary) Insert(v float64) {
	s.n++
	pos := sort.Search(len(s.items), func(i int) bool {
		return s.items[i].value >= v
	})
	// The extreme tuples (the current minimum and maximum) always get
	// delta = 0 so that Query can return exact answers for q = 0 and q = 1.
	delta := 0
	if pos > 0 && pos < len(s.items) {
		delta = int(2 * s.epsilon * float64(s.n))
	}
	s.items = append(s.items, gkItem{})
	copy(s.items[pos+1:], s.items[pos:])
	s.items[pos] = gkItem{value: v, g: 1, delta: delta}
	if s.n%s.compressEvery == 0 {
		s.compress()
	}
}

// allowable returns the maximum permitted rank error at the current count.
func (s *QuantileSummary) allowable() int {
	return int(2 * s.epsilon * float64(s.n))
}

// compress merges adjacent tuples whose combined rank gain stays within the
// allowable error. It walks right to left so that the pair (i, i+1) is always
// evaluated against the already-merged right neighbour.
func (s *QuantileSummary) compress() {
	if len(s.items) < 2 {
		return
	}
	threshold := s.allowable()
	for i := len(s.items) - 2; i >= 0; i-- {
		if s.items[i].g+s.items[i+1].g+s.items[i+1].delta <= threshold {
			s.items[i+1].g += s.items[i].g
			s.items = append(s.items[:i], s.items[i+1:]...)
		}
	}
}

// Query returns a value whose true rank in the stream is within
// epsilon * Count() of the rank of q. It returns NaN on an empty summary.
func (s *QuantileSummary) Query(q float64) float64 {
	if len(s.items) == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return s.items[0].value
	}
	if q >= 1 {
		return s.items[len(s.items)-1].value
	}
	// Desired rank, biased by half the allowable error so that the returned
	// tuple's rank interval is centered on the requested rank.
	target := int(math.Ceil(q*float64(s.n))) + s.allowable()/2
	rmin := 0
	i := 0
	for ; i < len(s.items); i++ {
		if rmin+s.items[i].g+s.items[i].delta > target {
			break
		}
		rmin += s.items[i].g
	}
	if i == 0 {
		return s.items[0].value
	}
	return s.items[i-1].value
}

// RankBounds returns the inclusive lower and upper bounds on the true rank of
// the value that Query(q) would return. It is primarily useful in tests to
// assert the epsilon guarantee without depending on the data distribution.
func (s *QuantileSummary) RankBounds(q float64) (lo, hi float64) {
	centre := q * float64(max(s.n-1, 0))
	half := s.epsilon * float64(s.n)
	return centre - half, centre + half
}

// Reset discards all observations and returns the summary to its initial
// state, keeping the allocated capacity for reuse.
func (s *QuantileSummary) Reset() {
	s.n = 0
	s.items = s.items[:0]
}
