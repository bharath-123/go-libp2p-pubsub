package pubsub

import (
	"fmt"
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.27.0"
)

func TestOtelTracingDisabled(t *testing.T) {
	// Test that when InitOtelTracing is never called, operations work normally
	// Reset tracer to nil to simulate fresh start
	otelTracer = nil
	
	if IsTracingEnabled() {
		t.Fatal("tracing should be disabled by default")
	}
	
	// Test that spans are no-ops
	_, span := startSpan(context.Background(), "test.operation")
	span.End()
	
	// Should not panic - test passes if we get here
}

func TestOtelTracingEnabled(t *testing.T) {
	// Setup stdout exporter for testing
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()), // Sample everything for testing
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	
	// Enable tracing
	InitOtelTracing()
	
	if !IsTracingEnabled() {
		t.Fatal("tracing should be enabled")
	}
	
	// Test that spans are created
	_, span := startSpan(context.Background(), "test.operation")
	if !span.IsRecording() {
		t.Fatal("span should be recording when tracing enabled")
	}
	span.End()
}

func TestGossipSubWithOtelTracing(t *testing.T) {
	// Setup tracing
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}
	
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("pubsub-integration-test"),
		)),
		trace.WithSampler(trace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	
	otel.SetTracerProvider(tp)
	InitOtelTracing()
	
	// Create test environment
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	hosts := getDefaultHosts(t, 3)
	psubs := getGossipsubs(ctx, hosts)
	
	// Connect peers
	connectAll(t, hosts)
	
	topic := "test-otel-tracing"
	
	// Subscribe (this will create join traces)
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	
	// Wait for mesh to form
	time.Sleep(time.Millisecond * 100)
	
	// Publish message (this will create publish and handle_rpc traces)
	testMsg := []byte("test message with otel tracing")
	err = psubs[0].Publish(topic, testMsg)
	if err != nil {
		t.Fatal(err)
	}
	
	// Receive messages (this will create delivery traces)
	for i, sub := range subs[1:] {
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(msg.Data) != string(testMsg) {
			t.Fatalf("peer %d received wrong message: expected %s, got %s", i+1, testMsg, msg.Data)
		}
	}
	
	// Wait a bit to ensure all traces are processed
	time.Sleep(time.Millisecond * 100)
	
	// Test completed successfully - traces should be visible in stdout
	t.Log("Test completed successfully. Check stdout for OpenTelemetry traces.")
}