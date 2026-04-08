// Example: error-handling/subscription-errors
//
// Demonstrates how to capture and handle subscription errors using a custom
// LoggerAdapter. The KubeMQ Watermill integration reports subscription errors
// (connection drops, unmarshal failures) through the configured Logger.
// This example implements a custom LoggerAdapter that captures error messages
// for programmatic handling.
//
// Internal error handling per pattern:
// - Events: errors reported via kubemqSDK.WithOnError callback -> Logger.Error
// - EventsStore: errors reported via kubemqSDK.WithOnError callback -> Logger.Error
// - Queues: poll errors trigger exponential backoff with Logger.Error
//
// Channel: watermill-err.subscription-errors
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// --- Custom LoggerAdapter for error capture ---
// ErrorCapturingLogger wraps watermill.StdLoggerAdapter and captures error
// messages for programmatic inspection. This pattern is useful for monitoring
// or alerting on subscription errors.
type ErrorCapturingLogger struct {
	inner  watermill.LoggerAdapter
	mu     sync.Mutex
	errors []string
}

func NewErrorCapturingLogger() *ErrorCapturingLogger {
	return &ErrorCapturingLogger{
		inner: watermill.NewStdLogger(false, false),
	}
}

func (l *ErrorCapturingLogger) Error(msg string, err error, fields watermill.LogFields) {
	l.mu.Lock()
	l.errors = append(l.errors, fmt.Sprintf("%s: %v", msg, err))
	l.mu.Unlock()
	l.inner.Error(msg, err, fields)
}

func (l *ErrorCapturingLogger) Info(msg string, fields watermill.LogFields) {
	l.inner.Info(msg, fields)
}

func (l *ErrorCapturingLogger) Debug(msg string, fields watermill.LogFields) {
	l.inner.Debug(msg, fields)
}

func (l *ErrorCapturingLogger) Trace(msg string, fields watermill.LogFields) {
	l.inner.Trace(msg, fields)
}

func (l *ErrorCapturingLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return l
}

func (l *ErrorCapturingLogger) GetErrors() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]string, len(l.errors))
	copy(result, l.errors)
	return result
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Use custom logger to capture errors ---
	logger := NewErrorCapturingLogger()

	// --- Create subscriber with custom logger ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// --- Demonstrate empty topic error ---
	_, err = sub.Subscribe(ctx, "")
	if err != nil {
		fmt.Printf("Subscribe error (empty topic): %v\n", err)
	}

	// --- Demonstrate closed subscriber error ---
	sub.Close()
	_, err = sub.Subscribe(ctx, "watermill-err.subscription-errors")
	if err != nil {
		fmt.Printf("Subscribe error (closed subscriber): %v\n", err)
	}

	// --- Normal subscription with publisher ---
	// Create a fresh subscriber and publisher for successful flow
	freshLogger := NewErrorCapturingLogger()
	freshSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  freshLogger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer freshSub.Close()

	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  freshLogger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	msgs, err := freshSub.Subscribe(ctx, "watermill-err.subscription-errors")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second)

	// Publish and receive a message to verify normal operation
	msg := message.NewMessage(watermill.NewUUID(), []byte("error handling test"))
	if err := pub.Publish("watermill-err.subscription-errors", msg); err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-msgs:
		fmt.Printf("Received: %s\n", string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	// --- Check captured errors ---
	capturedErrors := freshLogger.GetErrors()
	if len(capturedErrors) == 0 {
		fmt.Println("No errors captured during normal operation (expected)")
	} else {
		fmt.Printf("Captured %d errors:\n", len(capturedErrors))
		for _, e := range capturedErrors {
			fmt.Printf("  - %s\n", strings.TrimSpace(e))
		}
	}

	fmt.Println("Done! Subscription error handling demonstrated.")
}

// Expected output:
// Subscribe error (empty topic): watermill-kubemq: topic cannot be empty
// Subscribe error (closed subscriber): watermill-kubemq: subscriber is closed
// Received: error handling test
// No errors captured during normal operation (expected)
// Done! Subscription error handling demonstrated.
