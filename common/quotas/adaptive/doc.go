// Package adaptive implements a multi-tenant rate limiter whose effective rate
// adapts to a congestion signal.
//
// The limiter maintains an independent token bucket per tenant key. Requests
// that cannot be served immediately are scheduled as reservations: each
// reservation is assigned a grant time computed from the partition's committed
// debt, and a background timer grants reservations as their grant times arrive.
// Reservations further out than Config.MaxWait are denied, bounding the delay
// any request can be asked to wait.
//
// Effective partition rates are the product of the configured base rate and an
// AIMD (additive-increase/multiplicative-decrease) multiplier driven by a
// congestion signal reported via Limiter.ReportSignal. When the multiplier
// changes, pending reservations in every partition are re-timelined so that
// earlier requests keep their relative order under the new rate.
package adaptive
