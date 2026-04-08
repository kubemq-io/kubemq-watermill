// Example: router/multiple-handlers
//
// Demonstrates a Watermill Router with 3 handlers running concurrently:
//  1. Events handler: transforms messages from events input to events output
//  2. Queues handler: transforms messages from queues input to queues output
//  3. Events sink handler: consumes events without producing output
//
// All handlers run concurrently in a single router instance.
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
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

	// --- Create publishers ---
	eventsPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer eventsPub.Close()

	queuesPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer queuesPub.Close()

	// --- Create subscribers ---
	eventsSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer eventsSub.Close()

	queuesSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer queuesSub.Close()

	eventsSinkSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer eventsSinkSub.Close()

	// --- Create router with 3 handlers ---
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Track total processed messages across all handlers
	var totalProcessed int64

	// Handler 1: Events uppercase transform
	router.AddHandler(
		"events-uppercase",
		"watermill-router.multi.events-in",
		eventsSub,
		"watermill-router.multi.events-out",
		eventsPub,
		func(msg *message.Message) ([]*message.Message, error) {
			uppercased := strings.ToUpper(string(msg.Payload))
			fmt.Printf("[events-handler] %s -> %s\n", string(msg.Payload), uppercased)
			atomic.AddInt64(&totalProcessed, 1)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(uppercased))
			return []*message.Message{outMsg}, nil
		},
	)

	// Handler 2: Queues prefix transform
	router.AddHandler(
		"queues-prefix",
		"watermill-router.multi.queues-in",
		queuesSub,
		"watermill-router.multi.queues-out",
		queuesPub,
		func(msg *message.Message) ([]*message.Message, error) {
			prefixed := fmt.Sprintf("[PROCESSED] %s", string(msg.Payload))
			fmt.Printf("[queues-handler] %s -> %s\n", string(msg.Payload), prefixed)
			atomic.AddInt64(&totalProcessed, 1)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(prefixed))
			return []*message.Message{outMsg}, nil
		},
	)

	// Handler 3: Events sink (consume-only, no output)
	router.AddNoPublisherHandler(
		"events-sink",
		"watermill-router.multi.events-sink",
		eventsSinkSub,
		func(msg *message.Message) error {
			fmt.Printf("[events-sink] consumed: %s\n", string(msg.Payload))
			atomic.AddInt64(&totalProcessed, 1)
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

	// --- Create input publishers ---
	eventsInputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer eventsInputPub.Close()

	queuesInputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer queuesInputPub.Close()

	// Publish to events handler input
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("event-%d", i)))
		if err := eventsInputPub.Publish("watermill-router.multi.events-in", msg); err != nil {
			log.Fatal(err)
		}
	}

	// Publish to queues handler input
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("task-%d", i)))
		if err := queuesInputPub.Publish("watermill-router.multi.queues-in", msg); err != nil {
			log.Fatal(err)
		}
	}

	// Publish to events sink input
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("log-%d", i)))
		if err := eventsInputPub.Publish("watermill-router.multi.events-sink", msg); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Published 6 messages across 3 handler inputs.")

	// Wait for all 6 messages to be processed
	deadline := time.After(15 * time.Second)
	for {
		if atomic.LoadInt64(&totalProcessed) >= 6 {
			break
		}
		select {
		case <-deadline:
			log.Fatalf("Timeout: only %d/6 messages processed", atomic.LoadInt64(&totalProcessed))
		case <-time.After(100 * time.Millisecond):
			// Poll
		}
	}

	fmt.Printf("Done! All %d messages processed across 3 concurrent handlers.\n", atomic.LoadInt64(&totalProcessed))
}

// Expected output (order may vary due to concurrent handlers):
// Published 6 messages across 3 handler inputs.
// [events-handler] event-1 -> EVENT-1
// [events-handler] event-2 -> EVENT-2
// [queues-handler] task-1 -> [PROCESSED] task-1
// [queues-handler] task-2 -> [PROCESSED] task-2
// [events-sink] consumed: log-1
// [events-sink] consumed: log-2
// Done! All 6 messages processed across 3 concurrent handlers.
