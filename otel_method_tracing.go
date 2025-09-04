package pubsub

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/libp2p/go-libp2p-pubsub"
)

// Global tracer instance - will be nil if tracing is not enabled
var otelTracer trace.Tracer

// InitOtelTracing initializes OpenTelemetry tracing for pubsub methods.
// Call this function to enable method-level tracing.
// If this function is never called, all tracing operations are no-ops.
func InitOtelTracing() {
	otelTracer = otel.Tracer(tracerName)
}

// Helper function to start a span with common attributes
// Returns no-op span if tracing is disabled
func startSpan(ctx context.Context, operationName string) (context.Context, trace.Span) {
	if otelTracer == nil {
		// Return no-op span if tracing not enabled
		return ctx, trace.SpanFromContext(ctx)
	}
	return otelTracer.Start(ctx, operationName)
}

// IsTracingEnabled returns whether OpenTelemetry tracing is currently enabled
func IsTracingEnabled() bool {
	return otelTracer != nil
}