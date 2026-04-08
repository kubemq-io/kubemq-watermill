// Example: middleware/retry
//
// Demonstrates the Watermill Retry middleware which automatically retries
// failed handler invocations. The handler intentionally fails the first 2
// times and succeeds on the 3rd attempt. Retry middleware is configured with
// MaxRetries: 3 and an initial interval of 100ms.
//
// Channel: watermill-mw.retry
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

	// Create publisher
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

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

	// Create router with Retry middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Retry middleware: retry up to 3 times with 100ms initial interval
	router.AddMiddleware(middleware.Retry{
		MaxRetries:      3,
		InitialInterval: 100 * time.Millisecond,
	}.Middleware)

	var attemptCount int64
	done := make(chan struct{})

	// Handler that fails the first 2 times and succeeds on the 3rd
	router.AddNoPublisherHandler(
		"flaky-handler",
		"watermill-mw.retry",
		sub,
		func(msg *message.Message) error {
			attempt := atomic.AddInt64(&attemptCount, 1)
			fmt.Printf("Attempt #%d for message: %s\n", attempt, string(msg.Payload))

			if attempt < 3 {
				return fmt.Errorf("simulated failure on attempt #%d", attempt)
			}

			fmt.Printf("Success on attempt #%d!\n", attempt)
			close(done)
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

	// Create input publisher and send one message
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("retry-me"))
	if err := inputPub.Publish("watermill-mw.retry", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: retry-me")

	// Wait for successful processing
	select {
	case <-done:
		fmt.Printf("Done! Message processed after %d attempts.\n", atomic.LoadInt64(&attemptCount))
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message processing")
	}
}

// Expected output:
// Published: retry-me
// Attempt #1 for message: retry-me
// Attempt #2 for message: retry-me
// Attempt #3 for message: retry-me
// Success on attempt #3!
// Done! Message processed after 3 attempts.
