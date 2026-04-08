// Example: observability/otel-trace-propagation
//
// Demonstrates OpenTelemetry trace context propagation through KubeMQ messages.
// The watermill-kubemq integration automatically injects trace context into
// message metadata before publishing (via unexported injectTraceContext) and
// extracts it after receiving (via unexported extractTraceContext). Users only
// need to:
//
//	(a) Set up a global OTel TextMapPropagator (propagation.TraceContext{}).
//	(b) Call msg.SetContext(spanCtx) before publishing to attach a trace span.
//	(c) Use msg.Context() after receiving to get the propagated trace context.
//
// NOTE: This example requires additional OTel dependencies not in the default go.mod:
//
//	go get go.opentelemetry.io/otel/sdk@v1.42.0
//	go get go.opentelemetry.io/otel/exporters/stdout/stdouttrace@v1.42.0
//
// Channel: watermill-obs.otel-trace
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Step 1: Set up OTel TracerProvider with stdout exporter ---
	// The stdout exporter prints spans to stdout for demonstration.
	// In production, use an OTLP exporter to send spans to Jaeger, Zipkin, etc.
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("TracerProvider shutdown error: %v", err)
		}
	}()

	// Register the TracerProvider globally
	otel.SetTracerProvider(tp)

	// --- Step 2: Set up global TextMapPropagator ---
	// This is REQUIRED for trace context to flow through KubeMQ messages.
	// The integration uses otel.GetTextMapPropagator() internally.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	logger := watermill.NewStdLogger(false, false)

	// --- Create publisher and subscriber ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe
	msgs, err := sub.Subscribe(ctx, "watermill-obs.otel-trace")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	// --- Step 3: Create a trace span and publish ---
	tracer := otel.Tracer("watermill-kubemq-example")
	spanCtx, span := tracer.Start(ctx, "publish-order")
	defer span.End()

	// Set the span context on the message BEFORE publishing.
	// The integration's injectTraceContext will then propagate the trace headers
	// (traceparent, tracestate) into message metadata -> KubeMQ tags.
	msg := message.NewMessage(watermill.NewUUID(), []byte("Order created"))
	msg.SetContext(spanCtx)

	if err := pub.Publish("watermill-obs.otel-trace", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published message with trace context")

	// --- Step 4: Receive and extract trace context ---
	select {
	case received := <-msgs:
		// The integration's extractTraceContext automatically restores the trace
		// context from message metadata. Use msg.Context() to access it.
		receivedCtx := received.Context()

		// Create a child span from the propagated context
		_, childSpan := tracer.Start(receivedCtx, "process-order")
		fmt.Printf("Received: %s\n", string(received.Payload))
		fmt.Printf("Trace propagated: traceparent metadata = %s\n",
			received.Metadata.Get("traceparent"))

		// Simulate processing
		time.Sleep(50 * time.Millisecond)
		childSpan.End()
		received.Ack()

	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	// Flush spans
	if err := tp.ForceFlush(ctx); err != nil {
		log.Printf("Flush error: %v", err)
	}

	fmt.Println("Done! Trace context propagated through KubeMQ message.")
}

// Expected output:
// Published message with trace context
// Received: Order created
// Trace propagated: traceparent metadata = 00-<trace-id>-<span-id>-01
// Done! Trace context propagated through KubeMQ message.
// (stdout exporter also prints span JSON to stdout)
