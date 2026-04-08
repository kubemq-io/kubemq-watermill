// Example: middleware/recoverer
//
// Demonstrates the Watermill Recoverer middleware which catches panics in
// handlers and converts them to errors. Without Recoverer, a panic in a
// handler would crash the entire router. With Recoverer, the panic is caught,
// logged, and the message is nacked for redelivery.
//
// Channel: watermill-mw.recoverer
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

	// Create output subscriber to verify processed messages
	outSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer outSub.Close()

	// Create router with Recoverer middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}

	// Recoverer catches panics, logs them, and nacks the message
	router.AddMiddleware(middleware.Recoverer)

	var panicCount int64

	// Handler that panics on "bad" messages but processes "good" ones
	router.AddHandler(
		"panic-handler",
		"watermill-mw.recoverer",
		sub,
		"watermill-mw.recoverer.out",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			payload := string(msg.Payload)
			if payload == "panic-message" {
				atomic.AddInt64(&panicCount, 1)
				// This panic is caught by Recoverer instead of crashing the router
				panic("unexpected error processing message!")
			}
			fmt.Printf("Handler processed: %s\n", payload)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("processed: %s", payload)))
			return []*message.Message{outMsg}, nil
		},
	)

	// Start router
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()
	<-router.Running()

	// Create input publisher
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	// Publish a mix of good and panic-triggering messages
	messages := []string{"good-message-1", "panic-message", "good-message-2"}
	for _, text := range messages {
		msg := message.NewMessage(watermill.NewUUID(), []byte(text))
		if err := inputPub.Publish("watermill-mw.recoverer", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", text)
	}

	// Wait for processing -- the router continues despite the panic
	time.Sleep(3 * time.Second)

	fmt.Printf("Panics recovered: %d\n", atomic.LoadInt64(&panicCount))
	fmt.Println("Done! Router survived the panic thanks to Recoverer middleware.")
}

// Expected output:
// Published: good-message-1
// Published: panic-message
// Published: good-message-2
// Handler processed: good-message-1
// Handler processed: good-message-2
// Panics recovered: 1
// Done! Router survived the panic thanks to Recoverer middleware.
