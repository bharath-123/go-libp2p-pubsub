package pubsub

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestProcessLoopTracing(t *testing.T) {
	// Set up stdout tracer for testing
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(tp)

	// Initialize our tracing
	InitOtelTracing()

	// Create a simple pubsub instance
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 2)
	psubs := getPubsubs(ctx, hosts)

	// Create a topic and subscribe
	topic := "test-processloop-tracing"
	_, err = psubs[0].Join(topic)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	// Connect the hosts
	connectAll(t, hosts)

	// Wait a bit for connections to establish
	time.Sleep(100 * time.Millisecond)

	// Publish a message to trigger processLoop events
	testData := []byte("test message for processloop tracing")
	err = psubs[0].Publish(topic, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for message delivery
	ctx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case msg := <-sub.ch:
		if string(msg.Data) != string(testData) {
			t.Fatalf("received wrong message: %s", string(msg.Data))
		}
	case <-ctx.Done():
		t.Fatal("message not received in time")
	}

	// Force flush to ensure all spans are exported
	err = tp.ForceFlush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Test passes if we reach here without panics/errors
	t.Log("ProcessLoop tracing test completed successfully")
}

func TestProcessLoopQueueDepthMonitoring(t *testing.T) {
	// Set up stdout tracer
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		t.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(tp)

	InitOtelTracing()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHosts(t, 2)
	psubs := getPubsubs(ctx, hosts)
	connectAll(t, hosts)

	topic := "queue-depth-test"
	_, err = psubs[0].Join(topic)
	if err != nil {
		t.Fatal(err)
	}

	// Publish multiple messages rapidly to test queue depth monitoring
	for i := 0; i < 5; i++ {
		testData := []byte("rapid message " + string(rune('0'+i)))
		err = psubs[0].Publish(topic, testData)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait a bit for processing
	time.Sleep(50 * time.Millisecond)

	// Force flush spans
	err = tp.ForceFlush(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Queue depth monitoring test completed successfully")
}
