package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/admission"
	"google.golang.org/grpc"
)

// AdmissionDefaultWeight is the weight applied to methods without an entry
// in the weights map.
const AdmissionDefaultWeight = 1

type (
	// AdmissionInterceptor applies adaptive admission control to unary RPCs.
	// Each method is its own partition so a hot endpoint cannot starve cold
	// endpoints of limiter budget.
	AdmissionInterceptor struct {
		controller *admission.Controller
		enabled    func() bool
		// weights maps fully qualified method names to the number of slots
		// a request consumes. A weight of 0 excludes a method from
		// admission control entirely.
		weights map[string]int
	}
)

var _ grpc.UnaryServerInterceptor = (*AdmissionInterceptor)(nil).Intercept

func NewAdmissionInterceptor(
	controller *admission.Controller,
	enabled func() bool,
	weights map[string]int,
) *AdmissionInterceptor {
	return &AdmissionInterceptor{
		controller: controller,
		enabled:    enabled,
		weights:    weights,
	}
}

// Intercept implements grpc.UnaryServerInterceptor.
func (i *AdmissionInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if !i.enabled() {
		return handler(ctx, req)
	}
	if w, ok := i.weights[info.FullMethod]; ok && w <= 0 {
		return handler(ctx, req)
	}
	permit, err := i.controller.Acquire(ctx, info.FullMethod)
	if err != nil {
		return nil, mapAdmissionError(err)
	}
	resp, handlerErr := handler(ctx, req)
	permit.Done(handlerErr)
	return resp, handlerErr
}

// mapAdmissionError converts a controller rejection into a serviceerror so
// the frontend returns the correct gRPC status. Non-rejection errors pass
// through unchanged.
func mapAdmissionError(err error) error {
	var rejected *admission.RejectedError
	if errors.As(err, &rejected) && rejected.Cause != nil {
		return rejected.Cause
	}
	var exhausted *serviceerror.ResourceExhausted
	if errors.As(err, &exhausted) {
		return exhausted
	}
	return err
}
