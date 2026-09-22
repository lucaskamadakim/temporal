package admission

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metric names emitted by the controller. They are defined here rather than
// in common/metrics because the admission controller is self-contained and
// may be instantiated by more than one service.
const (
	AdmissionRequestsTotal  = "admission_requests_total"
	AdmissionRejectedTotal  = "admission_rejected_total"
	AdmissionInFlight       = "admission_in_flight"
	AdmissionConcurrencyCap = "admission_concurrency_limit"
	AdmissionLatency        = "admission_latency"
	AdmissionPartitionCount = "admission_partitions"
	AdmissionFailureRatio   = "admission_failure_ratio"
)

var (
	// ErrRejected is returned when the controller sheds a request. It wraps
	// a serviceerror so callers that return it over gRPC surface a proper
	// RESOURCE_EXHAUSTED status.
	ErrRejected = errors.New("admission: request rejected")
)

// RejectedError carries the reason a request was shed, for error mapping
// and logging.
type RejectedError struct {
	Key    string
	Reason string
	Cause  error
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("%s: %s (key %q)", ErrRejected, e.Reason, e.Key)
}

func (e *RejectedError) Unwrap() error { return e.Cause }

func (e *RejectedError) Is(target error) bool { return target == ErrRejected }

// Config controls a Controller. All fields are required unless noted.
type Config struct {
	// Enabled gates admission decisions. When false, Acquire always admits
	// and the controller records nothing.
	Enabled bool
	// Window is the total span of the sliding counter window.
	Window time.Duration
	// WindowBuckets is the number of buckets in the window ring. More
	// buckets give finer expiry granularity at the cost of a fixed array.
	WindowBuckets int
	// MinRequests is the minimum number of admitted requests that must be
	// observed in the window before the failure-ratio gate is armed. It
	// prevents a handful of early failures from tripping the gate.
	MinRequests int64
	// FailureRatioThreshold trips the gate when
	// failures / (successes + failures) >= threshold.
	FailureRatioThreshold float64
	// MinConcurrency and MaxConcurrency bound the adaptive limit.
	MinConcurrency int
	MaxConcurrency int
	// InitialConcurrency is the in-flight cap at construction.
	InitialConcurrency int
	// Smoothing is the EWMA weight applied to limit updates.
	Smoothing float64
	// RTTMinMultiplier scales the unloaded-RTT estimate in the gradient
	// formula. Values below 1 keep some slack so the limiter does not
	// react to sub-millisecond jitter.
	RTTMinMultiplier float64
	// BackoffRatio multiplies the limit down on each failed request.
	BackoffRatio float64
	// QueueSize is the additive growth term of the limit update.
	QueueSize float64
	// LimiterUpdateInterval is how often the limit is recomputed.
	LimiterUpdateInterval time.Duration
	// MinRTTRefreshInterval is how often the unloaded-RTT estimate is
	// allowed to adapt upward.
	MinRTTRefreshInterval time.Duration
	// LatencyEWMAAlpha is the weight of each new latency sample.
	LatencyEWMAAlpha float64
	// QuantileEpsilon is the rank error of the quantile summary.
	QuantileEpsilon float64
	// MaxKeys bounds the number of live partitions; the least recently
	// used partition is evicted beyond this. Non-positive means unlimited.
	MaxKeys int
}

// partition bundles the three signals for one key.
type partition struct {
	window  *SlidingWindow
	latency *LatencyTracker
	limiter *GradientLimiter
	element *list.Element // position in the LRU list, for eviction
}

// Controller decides whether requests may proceed. One Controller is shared
// by all callers of a service; requests are partitioned by key so a hot
// endpoint does not starve cold ones.
type Controller struct {
	cfg        Config
	timeSource clock.TimeSource
	handler    metrics.Handler

	mu         sync.Mutex
	partitions map[string]*partition
	lru        *list.List // front = most recently used; holds partition keys
}

// NewController validates cfg and returns a Controller. Metrics are emitted
// to handler, tagged per partition key.
func NewController(
	cfg Config,
	timeSource clock.TimeSource,
	handler metrics.Handler,
) (*Controller, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &Controller{
		cfg:        cfg,
		timeSource: timeSource,
		handler:    handler,
		partitions: make(map[string]*partition),
		lru:        list.New(),
	}, nil
}

func validateConfig(cfg Config) error {
	if !cfg.Enabled {
		return nil
	}
	switch {
	case cfg.Window <= 0:
		return ErrWindowNotPositive
	case cfg.WindowBuckets <= 0:
		return ErrBucketCountNotPositive
	case cfg.MinRequests < 0:
		return errors.New("admission: MinRequests must be non-negative")
	case cfg.FailureRatioThreshold <= 0 || cfg.FailureRatioThreshold > 1:
		return errors.New("admission: FailureRatioThreshold must be in (0, 1]")
	case cfg.MinConcurrency <= 0 || cfg.MaxConcurrency < cfg.MinConcurrency:
		return fmt.Errorf("%w: min=%d max=%d", ErrLimitBounds, cfg.MinConcurrency, cfg.MaxConcurrency)
	case cfg.InitialConcurrency < cfg.MinConcurrency || cfg.InitialConcurrency > cfg.MaxConcurrency:
		return ErrInitialLimit
	case cfg.Smoothing <= 0 || cfg.Smoothing > 1:
		return ErrSmoothingRange
	case cfg.RTTMinMultiplier <= 0 || cfg.RTTMinMultiplier > 1:
		return ErrMultiplierRange
	case cfg.BackoffRatio <= 0 || cfg.BackoffRatio >= 1:
		return ErrBackoffRange
	case cfg.QueueSize < 0:
		return ErrQueueNegative
	case cfg.LimiterUpdateInterval <= 0:
		return ErrUpdateInterval
	case cfg.MinRTTRefreshInterval <= 0:
		return ErrMinRefreshNotPositive
	case cfg.LatencyEWMAAlpha <= 0 || cfg.LatencyEWMAAlpha > 1:
		return ErrAlphaRange
	case cfg.QuantileEpsilon <= 0 || cfg.QuantileEpsilon >= 1:
		return ErrEpsilonRange
	}
	return nil
}

// Permit represents an admitted request. Exactly one of Done or DoneErr must
// be called to release the held slot.
type Permit struct {
	controller *Controller
	part       *partition
	key        string
	start      time.Time
	release    func(Outcome)
	once       sync.Once
}

// Done releases the permit, classifying err into an outcome for the
// controller's counters. Nil is a success; gRPC status codes that indicate
// service overload (Unavailable, Internal, DeadlineExceeded,
// ResourceExhausted, DataLoss, Unknown) count as failures; all other errors
// are treated as caller errors and do not trip the failure gate.
func (p *Permit) Done(err error) {
	p.finish(err)
}

// DoneOutcome releases the permit with an explicit outcome, for callers
// that classify completion themselves.
func (p *Permit) DoneOutcome(outcome Outcome) {
	p.finishOutcome(outcome)
}

func (p *Permit) finish(err error) {
	p.finishOutcome(outcomeFromError(err))
}

func (p *Permit) finishOutcome(outcome Outcome) {
	p.once.Do(func() {
		if p.part == nil {
			// disabled-mode permit: nothing was recorded, nothing to release
			return
		}
		elapsed := p.controller.timeSource.Now().Sub(p.start)
		p.part.window.RecordLatency(elapsed)
		switch outcome {
		case OutcomeFailure:
			p.part.window.Record(KindFailure, 1)
		case OutcomeIgnore:
			// only latency is fed back; the request does not count toward
			// the failure gate at all
		default:
			p.part.window.Record(KindSuccess, 1)
		}
		p.release(outcome)
		p.controller.emitMetrics(p.key, p.part)
	})
}

// Acquire attempts to admit one request under partition key. On success it
// returns a Permit; on rejection it returns a *RejectedError.
func (c *Controller) Acquire(_ context.Context, key string) (*Permit, error) {
	if !c.cfg.Enabled {
		return &Permit{controller: c, release: func(Outcome) {}}, nil
	}
	part, perr := c.partitionFor(key)
	if perr != nil {
		return nil, perr
	}

	if part.window.Requests() >= c.cfg.MinRequests &&
		part.window.FailureRatio() >= c.cfg.FailureRatioThreshold {
		part.window.Record(KindRejected, 1)
		c.emitRejectMetrics(key)
		return nil, &RejectedError{
			Key:    key,
			Reason: "failure ratio above threshold",
			Cause:  serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN, "admission control: failure ratio above threshold"),
		}
	}

	release, ok := part.limiter.TryAcquire()
	if !ok {
		part.window.Record(KindRejected, 1)
		c.emitRejectMetrics(key)
		return nil, &RejectedError{
			Key:    key,
			Reason: "concurrency limit reached",
			Cause:  serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "admission control: concurrency limit reached"),
		}
	}
	c.emitMetrics(key, part)
	return &Permit{
		controller: c,
		part:       part,
		key:        key,
		start:      c.timeSource.Now(),
		release:    release,
	}, nil
}

// partitionFor returns the partition for key, creating it if necessary and
// marking it most-recently used.
func (c *Controller) partitionFor(key string) (*partition, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if part, ok := c.partitions[key]; ok {
		c.lru.MoveToFront(part.element)
		return part, nil
	}
	part, err := c.newPartition(key)
	if err != nil {
		return nil, err
	}
	part.element = c.lru.PushFront(key)
	c.partitions[key] = part
	if c.cfg.MaxKeys > 0 {
		for c.lru.Len() > c.cfg.MaxKeys {
			c.evictOldest()
		}
	}
	c.handler.Gauge(AdmissionPartitionCount).Record(float64(c.lru.Len()))
	return part, nil
}

func (c *Controller) newPartition(_ string) (*partition, error) {
	window, err := NewSlidingWindow(c.cfg.Window, c.cfg.WindowBuckets, c.timeSource)
	if err != nil {
		return nil, err
	}
	latency, err := NewLatencyTracker(
		c.cfg.LatencyEWMAAlpha,
		c.cfg.QuantileEpsilon,
		c.cfg.MinRTTRefreshInterval,
		c.timeSource,
	)
	if err != nil {
		return nil, err
	}
	limiter, err := NewGradientLimiter(
		c.cfg.MinConcurrency,
		c.cfg.MaxConcurrency,
		c.cfg.InitialConcurrency,
		c.cfg.Smoothing,
		c.cfg.RTTMinMultiplier,
		c.cfg.BackoffRatio,
		c.cfg.QueueSize,
		c.cfg.LimiterUpdateInterval,
		latency,
		c.timeSource,
	)
	if err != nil {
		return nil, err
	}
	return &partition{window: window, latency: latency, limiter: limiter}, nil
}

// evictOldest drops the least recently used partition. The caller must hold
// c.mu.
func (c *Controller) evictOldest() {
	back := c.lru.Back()
	if back == nil {
		return
	}
	key, ok := back.Value.(string)
	if !ok {
		c.lru.Remove(back)
		return
	}
	delete(c.partitions, key)
	c.lru.Remove(back)
}

// Partitions returns the number of live partitions. Exposed for tests and
// operator introspection.
func (c *Controller) Partitions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// Stats returns a point-in-time snapshot for key, or zero values if the key
// has no partition.
func (c *Controller) Stats(key string) PartitionStats {
	c.mu.Lock()
	part, ok := c.partitions[key]
	c.mu.Unlock()
	if !ok {
		return PartitionStats{}
	}
	return PartitionStats{
		Requests:     part.window.Requests(),
		Rejected:     part.window.Count(KindRejected),
		FailureRatio: part.window.FailureRatio(),
		RTT:          part.latency.RTT(),
		MinRTT:       part.latency.MinRTT(),
		P95:          part.latency.Quantile(0.95),
		InFlight:     part.limiter.InFlight(),
		Limit:        part.limiter.Limit(),
	}
}

// PartitionStats is a point-in-time view of one partition's signals.
type PartitionStats struct {
	Requests     int64
	Rejected     int64
	FailureRatio float64
	RTT          time.Duration
	MinRTT       time.Duration
	P95          time.Duration
	InFlight     int
	Limit        int
}

func (c *Controller) emitMetrics(key string, part *partition) {
	tagged := c.handler.WithTags(metrics.OperationTag(key))
	tagged.Gauge(AdmissionInFlight).Record(float64(part.limiter.InFlight()))
	tagged.Gauge(AdmissionConcurrencyCap).Record(float64(part.limiter.Limit()))
	tagged.Gauge(AdmissionFailureRatio).Record(part.window.FailureRatio())
	tagged.Timer(AdmissionLatency).Record(part.latency.RTT())
}

func (c *Controller) emitRejectMetrics(key string) {
	c.handler.WithTags(metrics.OperationTag(key)).Counter(AdmissionRejectedTotal).Record(1)
}

// outcomeFromError classifies a request error. Only server-side and
// transport failures count against the failure gate; caller errors
// (InvalidArgument, NotFound, AlreadyExists, ...) are not the service's
// problem.
func outcomeFromError(err error) Outcome {
	if err == nil {
		return OutcomeSuccess
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable,
		codes.Internal,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.DataLoss,
		codes.Unknown:
		return OutcomeFailure
	case codes.Canceled:
		return OutcomeIgnore
	default:
		return OutcomeSuccess
	}
}
