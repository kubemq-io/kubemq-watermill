// Example: router/no-publisher-handler
//
// Demonstrates a Watermill Router with AddNoPublisherHandler for consume-only
// processing. The handler receives messages and logs them without producing
// any output messages.
//
// Channel: watermill-router.no-pub-handler
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

	// Create subscriber for the router
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Create router with Recoverer middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Track received count for exit condition (atomic for goroutine safety)
	var received int64
	done := make(chan struct{})

	// AddNoPublisherHandler: consume messages without producing output
	router.AddNoPublisherHandler(
		"logger",
		"watermill-router.no-pub-handler",
		sub,
		func(msg *message.Message) error {
			fmt.Printf("Log sink received: UUID=%s, Payload=%s\n", msg.UUID, string(msg.Payload))
			if atomic.AddInt64(&received, 1) >= 3 {
				close(done)
			}
			return nil
		},
	)

	// Start router in background
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()

	// Wait for router to start
	<-router.Running()

	// Create publisher for test messages
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Publish 3 log messages
	logMessages := []string{"user logged in", "order placed", "payment processed"}
	for _, text := range logMessages {
		msg := message.NewMessage(watermill.NewUUID(), []byte(text))
		if err := pub.Publish("watermill-router.no-pub-handler", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published log: %s\n", text)
	}

	// Wait for all messages to be consumed
	select {
	case <-done:
		fmt.Println("Done! All 3 log messages consumed (no output produced).")
	case <-ctx.Done():
		log.Fatal("Timeout waiting for messages")
	}
}

// Expected output:
// Published log: user logged in
// Published log: order placed
// Published log: payment processed
// Log sink received: UUID=<uuid>, Payload=user logged in
// Log sink received: UUID=<uuid>, Payload=order placed
// Log sink received: UUID=<uuid>, Payload=payment processed
// Done! All 3 log messages consumed (no output produced).
