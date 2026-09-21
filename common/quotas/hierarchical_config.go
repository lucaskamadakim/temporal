package quotas

import (
	"errors"
	"fmt"
)

const (
	// MaxHierarchicalLevels bounds the depth of a hierarchical limiter tree.
	// Trees deeper than this are rejected by validation: borrow chains walk
	// one level per hop, so very deep trees add latency without practical
	// benefit for matching-style fan-out topologies.
	MaxHierarchicalLevels = 8
)

var (
	errEmptyLevels       = errors.New("at least one level must be configured")
	errTooDeep           = fmt.Errorf("level count exceeds MaxHierarchicalLevels (%d)", MaxHierarchicalLevels)
	errNonPositiveRate   = errors.New("rate must be positive")
	errNegativeBurst     = errors.New("burst must be non-negative")
	errRootBorrow        = errors.New("root level cannot borrow (it has no parent)")
	errBurstBelowRateGap = errors.New("burst is smaller than a single-token grant granularity")
)

type (
	// HierarchicalLevelConfig configures one level of a
	// HierarchicalRateLimiter tree. Level 0 is always the root; requests
	// address leaves by path and consume capacity at every level they touch.
	HierarchicalLevelConfig struct {
		// Rate is the sustained refill rate for buckets at this level, in
		// tokens per second.
		Rate float64
		// Burst is the maximum token balance a bucket at this level may
		// hold; it bounds the largest instantaneous grant.
		Burst int
		// BorrowLimit is the maximum outstanding debt a bucket at this
		// level may accrue against its parent. Values <= 0 disable
		// borrowing at this level entirely. Ignored at the root.
		BorrowLimit int64
	}

	// HierarchicalLimiterConfig is the full configuration for a
	// HierarchicalRateLimiter. The same per-level configuration applies to
	// every bucket at that level; per-tenant overrides are intentionally not
	// supported here so that sibling buckets remain comparable.
	HierarchicalLimiterConfig struct {
		Levels []HierarchicalLevelConfig
	}
)

// DefaultHierarchicalLimiterConfig returns a conservative two-level
// configuration suitable for namespaced throttling: a global root and a
// per-namespace level allowed to borrow up to 50% of its burst.
func DefaultHierarchicalLimiterConfig() HierarchicalLimiterConfig {
	return HierarchicalLimiterConfig{
		Levels: []HierarchicalLevelConfig{
			{Rate: 1000, Burst: 1000},
			{Rate: 100, Burst: 100, BorrowLimit: 50},
		},
	}
}

// Validate reports whether the configuration is usable. It is cheap and
// safe to call on every dynamic config reload.
func (c HierarchicalLimiterConfig) Validate() error {
	if len(c.Levels) == 0 {
		return errEmptyLevels
	}
	if len(c.Levels) > MaxHierarchicalLevels {
		return errTooDeep
	}
	for i, lvl := range c.Levels {
		if lvl.Rate <= 0 {
			return fmt.Errorf("level %d: %w", i, errNonPositiveRate)
		}
		if lvl.Burst < 0 {
			return fmt.Errorf("level %d: %w", i, errNegativeBurst)
		}
		if lvl.Burst > 0 && float64(lvl.Burst) < 1 {
			return fmt.Errorf("level %d: %w", i, errBurstBelowRateGap)
		}
		if i == 0 && lvl.BorrowLimit != 0 {
			return fmt.Errorf("level %d: %w", i, errRootBorrow)
		}
	}
	return nil
}

// Depth returns the configured number of levels.
func (c HierarchicalLimiterConfig) Depth() int {
	return len(c.Levels)
}

// levelConfig returns the configuration for a level, clamped to the
// deepest configured level so callers may use paths deeper than the tree.
func (c HierarchicalLimiterConfig) levelConfig(level int) HierarchicalLevelConfig {
	if level >= len(c.Levels) {
		return c.Levels[len(c.Levels)-1]
	}
	return c.Levels[level]
}
