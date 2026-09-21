package quotas

import (
	"sync/atomic"
	"time"
)

var _ HierarchicalReservation = (*hierarchicalReservationImpl)(nil)
var _ HierarchicalReservation = (*failedReservation)(nil)

type (
	// HierarchicalReservation is the result of a Reserve call. A
	// successful reservation holds tokens until Cancel returns them;
	// unlike golang.org/x/time/rate reservations there is no expiry —
	// dispatch workloads either consume or release their charge.
	HierarchicalReservation interface {
		// OK reports whether the reservation holds its tokens.
		OK() bool
		// Tokens returns the granted token count (0 when !OK).
		Tokens() int64
		// Path returns the request path.
		Path() []string
		// Cancel refunds the charged buckets. Safe to call once; later
		// calls are no-ops.
		Cancel()
		// DelayFrom estimates how long the caller would have had to wait
		// for this request to be grantable. Always 0 for OK reservations;
		// for failed ones it is the longest per-level estimate.
		DelayFrom(now time.Time) time.Duration
		// CreatedAt is when the reservation was taken.
		CreatedAt() time.Time
	}

	hierarchicalReservationImpl struct {
		limiter   *hierarchicalRateLimiterImpl
		request   HierarchicalRequest
		credits   []bucketCredit
		createdAt time.Time
		cancelled atomic.Bool
	}

	failedReservation struct {
		delay time.Duration
	}
)

func newHierarchicalReservation(
	limiter *hierarchicalRateLimiterImpl,
	request HierarchicalRequest,
	credits []bucketCredit,
	now time.Time,
) *hierarchicalReservationImpl {
	return &hierarchicalReservationImpl{
		limiter:   limiter,
		request:   request,
		credits:   credits,
		createdAt: now,
	}
}

func newFailedReservation(delay time.Duration) *failedReservation {
	return &failedReservation{delay: delay}
}

func (r *hierarchicalReservationImpl) OK() bool {
	return true
}

func (r *hierarchicalReservationImpl) Tokens() int64 {
	return r.request.Tokens
}

func (r *hierarchicalReservationImpl) Path() []string {
	return r.request.Path
}

func (r *hierarchicalReservationImpl) Cancel() {
	if r.cancelled.CompareAndSwap(false, true) {
		r.limiter.refund(r.credits)
	}
}

func (r *hierarchicalReservationImpl) DelayFrom(now time.Time) time.Duration {
	return 0
}

func (r *hierarchicalReservationImpl) CreatedAt() time.Time {
	return r.createdAt
}

func (r *failedReservation) OK() bool {
	return false
}

func (r *failedReservation) Tokens() int64 {
	return 0
}

func (r *failedReservation) Path() []string {
	return nil
}

func (r *failedReservation) Cancel() {
}

func (r *failedReservation) DelayFrom(now time.Time) time.Duration {
	return r.delay
}

func (r *failedReservation) CreatedAt() time.Time {
	return time.Time{}
}
