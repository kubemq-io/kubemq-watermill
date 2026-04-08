// Example: observability/watermill-logging
//
// Demonstrates Watermill's structured logging with the KubeMQ integration.
// Shows two approaches:
//  1. Built-in StdLogger with debug and trace enabled.
//  2. Custom LoggerAdapter implementation for integration with external
//     logging frameworks (e.g., zap, zerolog, logrus).
//
// Channel: watermill-obs.logging
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// --- Custom LoggerAdapter ---
// PrefixLogger demonstrates implementing watermill.LoggerAdapter for use
// with external logging libraries. Each method receives structured LogFields.
type PrefixLogger struct {
	prefix string
}

func NewPrefixLogger(prefix string) *PrefixLogger {
	return &PrefixLogger{prefix: prefix}
}

func (l *PrefixLogger) Error(msg string, err error, fields watermill.LogFields) {
	log.Printf("[%s][ERROR] %s: %v %v", l.prefix, msg, err, fields)
}

func (l *PrefixLogger) Info(msg string, fields watermill.LogFields) {
	log.Printf("[%s][INFO] %s %v", l.prefix, msg, fields)
}

func (l *PrefixLogger) Debug(msg string, fields watermill.LogFields) {
	log.Printf("[%s][DEBUG] %s %v", l.prefix, msg, fields)
}

func (l *PrefixLogger) Trace(msg string, fields watermill.LogFields) {
	log.Printf("[%s][TRACE] %s %v", l.prefix, msg, fields)
}

func (l *PrefixLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	// In a production implementation, you would merge the fields into a new logger.
	// For simplicity, we return the same logger here.
	return l
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Approach 1: Built-in StdLogger ---
	// Parameters: debug=true (show Debug messages), trace=true (show Trace messages)
	// This is the simplest way to enable verbose logging for troubleshooting.
	stdLogger := watermill.NewStdLogger(true, true)
	fmt.Println("=== Using StdLogger (debug=true, trace=true) ===")

	pub1, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  stdLogger,
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("StdLogger test"))
	if err := pub1.Publish("watermill-obs.logging", msg); err != nil {
		log.Fatal(err)
	}
	pub1.Close()

	fmt.Println("\n=== Using Custom PrefixLogger ===")

	// --- Approach 2: Custom LoggerAdapter ---
	// The PrefixLogger adds a prefix to each log message.
	// Replace this with your preferred logging library (zap, zerolog, etc.)
	customLogger := NewPrefixLogger("kubemq")

	pub2, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  customLogger,
	})
	if err != nil {
		log.Fatal(err)
	}

	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  customLogger,
	})
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := sub.Subscribe(ctx, "watermill-obs.logging")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	msg2 := message.NewMessage(watermill.NewUUID(), []byte("Custom logger test"))
	if err := pub2.Publish("watermill-obs.logging", msg2); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published with custom logger")

	select {
	case received := <-msgs:
		fmt.Printf("Received: %s\n", string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	pub2.Close()
	sub.Close()

	fmt.Println("Done! Both logging approaches demonstrated.")
}

// Expected output (log prefixes/formats vary):
// === Using StdLogger (debug=true, trace=true) ===
// [watermill] ... Publisher created ...
// [watermill] ... Event published ...
//
// === Using Custom PrefixLogger ===
// [kubemq][INFO] Publisher created ...
// [kubemq][INFO] Subscriber created ...
// [kubemq][TRACE] Event published ...
// Published with custom logger
// Received: Custom logger test
// Done! Both logging approaches demonstrated.
