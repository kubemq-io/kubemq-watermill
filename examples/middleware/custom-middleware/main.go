// Example: middleware/custom-middleware
//
// Demonstrates writing a custom Watermill middleware. The timingMiddleware
// wraps any handler and logs how long each invocation takes. This pattern
// can be extended for metrics collection, authentication checks, header
// injection, or any cross-cutting concern.
//
// Channel: watermill-mw.custom
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// timingMiddleware is a custom Watermill middleware that measures and logs
// the duration of each handler invocation. It wraps the handler function
// and records the time before and after execution.
func timingMiddleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		start := time.Now()
		msgs, err := h(msg)
		log.Printf("Handler took %v", time.Since(start))
		return msgs, err
	}
}

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create subscriber
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Create router with custom middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Add our custom timing middleware -- it will wrap every handler in this router
	router.AddMiddleware(timingMiddleware)

	var processedCount int64
	done := make(chan struct{})

	// Handler that simulates varying processing times
	router.AddNoPublisherHandler(
		"timed-handler",
		"watermill-mw.custom",
		sub,
		func(msg *message.Message) error {
			// Simulate work with variable duration
			payload := string(msg.Payload)
			fmt.Printf("Processing: %s\n", payload)

			// Simulate different processing durations
			time.Sleep(50 * time.Millisecond)

			count := atomic.AddInt64(&processedCount, 1)
			if count >= 3 {
				close(done)
			}
			return nil
		},
	)

	// Start router
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()
	<-router.Running()

	// Publish test messages
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("task-%d", i)))
		if err := inputPub.Publish("watermill-mw.custom", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: task-%d\n", i)
	}

	// Wait for all messages to be processed
	select {
	case <-done:
		fmt.Printf("Done! All %d messages processed with timing logged.\n", atomic.LoadInt64(&processedCount))
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message processing")
	}
}

// Expected output:
// Published: task-1
// Published: task-2
// Published: task-3
// Processing: task-1
// Handler took ~50ms
// Processing: task-2
// Handler took ~50ms
// Processing: task-3
// Handler took ~50ms
// Done! All 3 messages processed with timing logged.
