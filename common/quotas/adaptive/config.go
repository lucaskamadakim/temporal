package adaptive

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultPartitionRPS        = 100.0
	defaultPartitionBurst      = 100
	defaultMaxPartitions       = 1024
	defaultPartitionIdleTTL    = 10 * time.Minute
	defaultMaxWait             = 30 * time.Second
	defaultAlpha               = 0.05
	defaultBeta                = 0.7
	defaultMinMultiplier       = 0.05
	defaultMaxMultiplier       = 4.0
	defaultSignalLowWatermark  = 0.3
	defaultSignalHighWatermark = 0.7
)

var (
	// ErrInvalidConfig is returned when a Config value fails validation.
	ErrInvalidConfig = errors.New("invalid adaptive rate limiter config")
	// ErrRateLimitExceeded is returned by Wait when the reservation cannot be
	// satisfied within Config.MaxWait, or the limiter is closed.
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	// ErrReservationDenied is returned by Reservation callers when a
	// reservation was denied because its wait would exceed Config.MaxWait.
	ErrReservationDenied = errors.New("reservation denied: wait would exceed max wait")
	// ErrReservationCanceled is returned to waiters whose reservation was
	// canceled, either explicitly or because its partition was evicted.
	ErrReservationCanceled = errors.New("reservation canceled")
	// ErrLimiterClosed is returned by operations on a closed Limiter.
	ErrLimiterClosed = errors.New("adaptive rate limiter closed")
)

type (
	// Config controls the behavior of a Limiter.
	Config struct {
		// PartitionRPS is the base per-partition rate in tokens per second
		// before the AIMD multiplier is applied.
		PartitionRPS float64
		// PartitionBurst is the maximum number of tokens a partition bucket
		// can hold; it bounds the instantaneous burst a single tenant may use.
		PartitionBurst int
		// MaxPartitions bounds the number of live partitions. Creating a new
		// partition beyond the bound evicts the least recently used one.
		MaxPartitions int
		// PartitionIdleTTL is how long a partition may go without serving a
		// request before it is evicted. Pending reservations of an evicted
		// partition are canceled.
		PartitionIdleTTL time.Duration
		// MaxWait is the maximum delay a reservation may be scheduled into
		// the future. Requests whose grant time would exceed now+MaxWait are
		// denied rather than queued.
		MaxWait time.Duration
		// PriorityWeights maps a request priority to a multiplier on the
		// partition rate used to schedule that request. Index 0 is the
		// highest priority. A request with priority >= len(PriorityWeights)
		// uses the last entry. Values <= 0 are treated as 1.
		PriorityWeights []float64
		// Alpha is the additive increase applied to the AIMD multiplier per
		// report below the low watermark.
		Alpha float64
		// Beta is the multiplicative decrease applied to the AIMD multiplier
		// per report above the high watermark.
		Beta float64
		// MinMultiplier and MaxMultiplier bound the AIMD multiplier.
		MinMultiplier float64
		MaxMultiplier float64
		// SignalLowWatermark and SignalHighWatermark select between additive
		// increase and multiplicative decrease in the AIMD controller.
		SignalLowWatermark  float64
		SignalHighWatermark float64
	}
)

// DefaultConfig returns a Config with production-reasonable defaults.
func DefaultConfig() Config {
	return Config{
		PartitionRPS:        defaultPartitionRPS,
		PartitionBurst:      defaultPartitionBurst,
		MaxPartitions:       defaultMaxPartitions,
		PartitionIdleTTL:    defaultPartitionIdleTTL,
		MaxWait:             defaultMaxWait,
		PriorityWeights:     []float64{1.0, 0.75, 0.5},
		Alpha:               defaultAlpha,
		Beta:                defaultBeta,
		MinMultiplier:       defaultMinMultiplier,
		MaxMultiplier:       defaultMaxMultiplier,
		SignalLowWatermark:  defaultSignalLowWatermark,
		SignalHighWatermark: defaultSignalHighWatermark,
	}
}

// Validate returns an error if the config is unusable. Zero-valued optional
// fields are filled with defaults.
func (c *Config) Validate() error {
	if c.PartitionRPS <= 0 {
		return fmt.Errorf("%w: PartitionRPS must be > 0, got %v", ErrInvalidConfig, c.PartitionRPS)
	}
	if c.PartitionBurst <= 0 {
		return fmt.Errorf("%w: PartitionBurst must be > 0, got %v", ErrInvalidConfig, c.PartitionBurst)
	}
	if c.MaxPartitions == 0 {
		c.MaxPartitions = defaultMaxPartitions
	}
	if c.MaxPartitions < 0 {
		return fmt.Errorf("%w: MaxPartitions must be > 0, got %v", ErrInvalidConfig, c.MaxPartitions)
	}
	if c.PartitionIdleTTL == 0 {
		c.PartitionIdleTTL = defaultPartitionIdleTTL
	}
	if c.PartitionIdleTTL < 0 {
		return fmt.Errorf("%w: PartitionIdleTTL must be >= 0, got %v", ErrInvalidConfig, c.PartitionIdleTTL)
	}
	if c.MaxWait == 0 {
		c.MaxWait = defaultMaxWait
	}
	if c.MaxWait < 0 {
		return fmt.Errorf("%w: MaxWait must be >= 0, got %v", ErrInvalidConfig, c.MaxWait)
	}
	if len(c.PriorityWeights) == 0 {
		c.PriorityWeights = []float64{1.0}
	}
	if c.Alpha == 0 {
		c.Alpha = defaultAlpha
	}
	if c.Alpha < 0 {
		return fmt.Errorf("%w: Alpha must be >= 0, got %v", ErrInvalidConfig, c.Alpha)
	}
	if c.Beta == 0 {
		c.Beta = defaultBeta
	}
	if c.Beta <= 0 || c.Beta >= 1 {
		return fmt.Errorf("%w: Beta must be in (0, 1), got %v", ErrInvalidConfig, c.Beta)
	}
	if c.MinMultiplier == 0 {
		c.MinMultiplier = defaultMinMultiplier
	}
	if c.MaxMultiplier == 0 {
		c.MaxMultiplier = defaultMaxMultiplier
	}
	if c.MinMultiplier <= 0 || c.MaxMultiplier < c.MinMultiplier {
		return fmt.Errorf("%w: multiplier bounds [%v, %v] are invalid", ErrInvalidConfig, c.MinMultiplier, c.MaxMultiplier)
	}
	if c.SignalLowWatermark == 0 {
		c.SignalLowWatermark = defaultSignalLowWatermark
	}
	if c.SignalHighWatermark == 0 {
		c.SignalHighWatermark = defaultSignalHighWatermark
	}
	if c.SignalLowWatermark >= c.SignalHighWatermark {
		return fmt.Errorf("%w: signal watermarks [%v, %v] are invalid", ErrInvalidConfig, c.SignalLowWatermark, c.SignalHighWatermark)
	}
	return nil
}

// weightFor returns the scheduling weight for a request priority.
func (c *Config) weightFor(priority int) float64 {
	if priority < 0 {
		priority = 0
	}
	if priority >= len(c.PriorityWeights) {
		priority = len(c.PriorityWeights) - 1
	}
	w := c.PriorityWeights[priority]
	if w <= 0 {
		return 1.0
	}
	return w
}

// delayFor converts a token count into a scheduling delay under the given
// effective rate and priority weight.
func delayFor(tokens int, rate float64, weight float64) time.Duration {
	if rate <= 0 {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(float64(tokens) / (rate * weight) * float64(time.Second))
}
