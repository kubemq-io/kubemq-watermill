// Example: middleware/throttle
//
// Demonstrates the Watermill Throttle middleware which limits the rate of
// message processing. Configured to allow a maximum of 2 messages per second.
// 10 messages are published rapidly, and the throttled handler processes them
// at the configured rate, observable via timestamps.
//
// Channel: watermill-mw.throttle
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

	// Create router with Throttle middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Throttle: max 2 messages per second
	// This creates a rate limiter that ensures at least 500ms between handler invocations
	router.AddMiddleware(middleware.NewThrottle(2, time.Second).Middleware)

	var processedCount int64
	totalMessages := int64(10)
	done := make(chan struct{})
	startTime := time.Now()

	// Handler that logs processing time to demonstrate throttling
	router.AddNoPublisherHandler(
		"throttled-handler",
		"watermill-mw.throttle",
		sub,
		func(msg *message.Message) error {
			count := atomic.AddInt64(&processedCount, 1)
			elapsed := time.Since(startTime).Truncate(time.Millisecond)
			fmt.Printf("[%v] Processed message %d/%d: %s\n",
				elapsed, count, totalMessages, string(msg.Payload))

			if count >= totalMessages {
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

	// Publish 10 messages rapidly
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	for i := 1; i <= int(totalMessages); i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("rapid-msg-%d", i)))
		if err := inputPub.Publish("watermill-mw.throttle", msg); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Published %d messages rapidly. Observing throttled processing...\n", totalMessages)
	startTime = time.Now()

	// Wait for all messages to be processed
	select {
	case <-done:
		elapsed := time.Since(startTime).Truncate(time.Millisecond)
		fmt.Printf("Done! Processed %d messages in %v (throttled to 2/sec).\n",
			atomic.LoadInt64(&processedCount), elapsed)
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message processing")
	}
}

// Expected output:
// Published 10 messages rapidly. Observing throttled processing...
// [0ms] Processed message 1/10: rapid-msg-1
// [~500ms] Processed message 2/10: rapid-msg-2
// [~1000ms] Processed message 3/10: rapid-msg-3
// ...
// [~4500ms] Processed message 10/10: rapid-msg-10
// Done! Processed 10 messages in ~4.5s (throttled to 2/sec).
