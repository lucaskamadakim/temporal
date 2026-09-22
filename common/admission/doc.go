// Package admission implements adaptive admission control for RPC services.
//
// The controller combines three independent signals to decide whether an
// incoming request should be admitted or shed:
//
//   - a sliding-window failure counter ([SlidingWindow]) that trips when the
//     observed failure ratio over recent requests crosses a threshold, similar
//     to a circuit breaker, but expressed over a time bucketed window rather
//     than an absolute count;
//   - a latency tracker ([LatencyTracker]) that maintains an EWMA of recent
//     request latencies, a decaying estimate of the unloaded (minimum) RTT,
//     and a Greenwald-Khanna epsilon-approximate quantile summary;
//   - a gradient concurrency limiter ([GradientLimiter]) that adapts the
//     number of permitted in-flight requests by comparing the short-term
//     latency against the unloaded RTT, growing the limit additively when
//     latency is flat and shrinking it multiplicatively when latency rises.
//
// Requests are partitioned by a caller supplied key (for example, the gRPC
// method name) so that a hot endpoint does not starve unrelated endpoints.
// The number of live partitions is bounded and the least recently used
// partition is evicted when the bound is reached.
//
// The package is self-contained: it only depends on common/clock for a
// [clock.TimeSource] so that tests can drive time deterministically, and on
// common/metrics for optional metric emission.
package admission
