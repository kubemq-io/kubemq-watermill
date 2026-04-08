// Example: middleware/poison-queue
//
// Demonstrates the Watermill PoisonQueue middleware which routes messages that
// cause handler errors to a dedicated poison (dead-letter) topic. This prevents
// permanently-failing messages from blocking the handler. The handler always
// returns an error, causing the message to land in the poison queue topic.
//
// Input channel:  watermill-mw.poison-input
// Poison channel: watermill-mw.poison
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
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

	// Create publisher for poison queue output
	poisonPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer poisonPub.Close()

	// Create subscriber for handler input
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

	// Create router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// PoisonQueue middleware: messages that fail processing are sent to the poison topic.
	// It wraps the handler and catches errors, publishing the failed message to the
	// specified topic via the provided publisher.
	poisonQueue, err := middleware.PoisonQueue(poisonPub, "watermill-mw.poison")
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(poisonQueue)

	done := make(chan struct{})

	// Handler that always fails -- message will end up in poison queue
	router.AddNoPublisherHandler(
		"always-failing",
		"watermill-mw.poison-input",
		sub,
		func(msg *message.Message) error {
			fmt.Printf("Handler received (will fail): %s\n", string(msg.Payload))
			close(done)
			return fmt.Errorf("processing failed: invalid data format")
		},
	)

	// Start router
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()
	<-router.Running()

	// Publish a message that will fail processing
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("bad-data"))
	if err := inputPub.Publish("watermill-mw.poison-input", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: bad-data")

	// Wait for handler to process (and fail)
	select {
	case <-done:
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	// Give poison queue middleware time to publish to poison topic
	time.Sleep(2 * time.Second)

	// Subscribe to the poison queue to verify the message landed there
	poisonSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer poisonSub.Close()

	poisonMsgs, err := poisonSub.Subscribe(ctx, "watermill-mw.poison")
	if err != nil {
		log.Fatal(err)
	}

	select {
	case poisonMsg := <-poisonMsgs:
		fmt.Printf("Poison queue received: %s\n", string(poisonMsg.Payload))
		poisonMsg.Ack()
	case <-time.After(10 * time.Second):
		fmt.Println("No message in poison queue (may already have been consumed)")
	}

	fmt.Println("Done! Failed message was routed to the poison queue.")
}

// Expected output:
// Published: bad-data
// Handler received (will fail): bad-data
// Poison queue received: bad-data
// Done! Failed message was routed to the poison queue.
