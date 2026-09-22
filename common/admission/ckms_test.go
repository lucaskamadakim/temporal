package admission

import (
	"math"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// rankOf returns the number of values <= v, i.e. the true rank estimate used
// to check the epsilon guarantee.
func mustSummary(t *testing.T, epsilon float64) *QuantileSummary {
	t.Helper()
	s, err := NewQuantileSummary(epsilon)
	require.NoError(t, err)
	return s
}

func rankOf(sorted []float64, v float64) int {
	return sort.Search(len(sorted), func(i int) bool { return sorted[i] > v })
}

func requireWithinEpsilon(t *testing.T, sorted []float64, got float64, q float64, epsilon float64) {
	t.Helper()
	lo, hi := q*float64(len(sorted)-1)-epsilon*float64(len(sorted)),
		q*float64(len(sorted)-1)+epsilon*float64(len(sorted))
	rank := float64(rankOf(sorted, got))
	require.GreaterOrEqual(t, rank, lo-1, "q=%v got=%v rank=%v below bound %v", q, got, rank, lo)
	require.LessOrEqual(t, rank, hi+1, "q=%v got=%v rank=%v above bound %v", q, got, rank, hi)
}

func TestQuantileSummary_ValidatesEpsilon(t *testing.T) {
	_, err := NewQuantileSummary(0)
	require.ErrorIs(t, err, ErrEpsilonRange)
	_, err = NewQuantileSummary(-0.5)
	require.ErrorIs(t, err, ErrEpsilonRange)
	_, err = NewQuantileSummary(1)
	require.ErrorIs(t, err, ErrEpsilonRange)
}

func TestQuantileSummary_Empty(t *testing.T) {
	s := mustSummary(t, 0.01)
	require.Equal(t, 0, s.Count())
	require.True(t, math.IsNaN(s.Query(0.5)))
}

func TestQuantileSummary_SingleValue(t *testing.T) {
	s := mustSummary(t, 0.01)
	s.Insert(42)
	require.Equal(t, 1, s.Count())
	require.InDelta(t, 42, s.Query(0.5), 1e-9)
	require.InDelta(t, 42, s.Query(0.99), 1e-9)
}

func TestQuantileSummary_ExtremesAreExact(t *testing.T) {
	s := mustSummary(t, 0.01)
	vals := []float64{9, 1, 5, 3, 7}
	for _, v := range vals {
		s.Insert(v)
	}
	require.InDelta(t, 1, s.Query(0), 1e-9)
	require.InDelta(t, 9, s.Query(1), 1e-9)
	require.InDelta(t, 1, s.Query(-0.5), 1e-9)
	require.InDelta(t, 9, s.Query(1.5), 1e-9)
}

func TestQuantileSummary_UniformAscending(t *testing.T) {
	const n = 2000
	eps := 0.01
	s := mustSummary(t, eps)
	sorted := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		v := float64(i)
		s.Insert(v)
		sorted = append(sorted, v)
	}
	require.Equal(t, n, s.Count())
	for _, q := range []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.99} {
		requireWithinEpsilon(t, sorted, s.Query(q), q, eps)
	}
	// the summary must be substantially smaller than the input
	require.Less(t, s.Len(), n/10)
}

func TestQuantileSummary_ShuffledUniform(t *testing.T) {
	const n = 5000
	eps := 0.005
	s := mustSummary(t, eps)
	rng := rand.New(rand.NewSource(42))
	sorted := make([]float64, 0, n)
	for _, v := range rng.Perm(n) {
		s.Insert(float64(v))
		sorted = append(sorted, float64(v))
	}
	slices.Sort(sorted)
	for _, q := range []float64{0.01, 0.5, 0.9, 0.999} {
		requireWithinEpsilon(t, sorted, s.Query(q), q, eps)
	}
}

func TestQuantileSummary_Gaussian(t *testing.T) {
	const n = 4000
	eps := 0.01
	s := mustSummary(t, eps)
	rng := rand.New(rand.NewSource(7))
	sorted := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		v := rng.NormFloat64()*100 + 1000
		s.Insert(v)
		sorted = append(sorted, v)
	}
	slices.Sort(sorted)
	for _, q := range []float64{0.5, 0.9, 0.99} {
		requireWithinEpsilon(t, sorted, s.Query(q), q, eps)
	}
	// sanity: p50 of N(1000, 100) should be near 1000
	require.InDelta(t, 1000, s.Query(0.5), 50)
}

func TestQuantileSummary_CompressesMonotonically(t *testing.T) {
	eps := 0.02
	s := mustSummary(t, eps)
	peak := 0
	for i := 0; i < 10000; i++ {
		s.Insert(float64(i % 997))
		if s.Len() > peak {
			peak = s.Len()
		}
	}
	require.Less(t, peak, int(3/eps))
	require.Equal(t, 10000, s.Count())
}

func TestQuantileSummary_Duplicates(t *testing.T) {
	s := mustSummary(t, 0.01)
	for i := 0; i < 1000; i++ {
		s.Insert(7)
	}
	require.InDelta(t, 7, s.Query(0.5), 1e-9)
	require.InDelta(t, 7, s.Query(0.999), 1e-9)
}

func TestQuantileSummary_Reset(t *testing.T) {
	s := mustSummary(t, 0.01)
	for i := 0; i < 100; i++ {
		s.Insert(float64(i))
	}
	s.Reset()
	require.Equal(t, 0, s.Count())
	require.Equal(t, 0, s.Len())
	require.True(t, math.IsNaN(s.Query(0.5)))
	s.Insert(5)
	require.InDelta(t, 5, s.Query(0.5), 1e-9)
}

func TestQuantileSummary_DescendingInserts(t *testing.T) {
	const n = 1000
	eps := 0.01
	s := mustSummary(t, eps)
	sorted := make([]float64, 0, n)
	for i := n; i > 0; i-- {
		s.Insert(float64(i))
		sorted = append(sorted, float64(i))
	}
	slices.Sort(sorted)
	for _, q := range []float64{0.25, 0.5, 0.95} {
		requireWithinEpsilon(t, sorted, s.Query(q), q, eps)
	}
}
