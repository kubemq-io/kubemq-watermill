// Example: events/router-handler
//
// Demonstrates using Watermill Router with AddHandler to transform events
// from an input topic to an output topic. The handler converts message
// payloads to uppercase.
//
// Channels: watermill-events.router-input -> watermill-events.router-output
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
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

	// Create publisher for router output
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber for router input
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Create a second subscriber to observe output
	outputSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer outputSub.Close()

	// Subscribe to output topic to observe transformed messages
	outputMsgs, err := outputSub.Subscribe(ctx, "watermill-events.router-output")
	if err != nil {
		log.Fatal(err)
	}

	// Set up router with Recoverer middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Add handler: read from input, transform to uppercase, publish to output
	router.AddHandler(
		"uppercase-handler",
		"watermill-events.router-input",
		sub,
		"watermill-events.router-output",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			upper := strings.ToUpper(string(msg.Payload))
			fmt.Printf("Handler: %s -> %s\n", string(msg.Payload), upper)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(upper))
			return []*message.Message{outMsg}, nil
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

	// Create a separate publisher for input messages
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	// Publish messages to input topic
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("hello world %d", i)))
		if err := inputPub.Publish("watermill-events.router-input", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Receive transformed messages from output topic
	received := 0
	for received < 3 {
		select {
		case msg := <-outputMsgs:
			fmt.Printf("Output: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 3 messages transformed through router.")
}

// Expected output:
// Published: hello world 1
// Published: hello world 2
// Published: hello world 3
// Handler: hello world 1 -> HELLO WORLD 1
// Handler: hello world 2 -> HELLO WORLD 2
// Handler: hello world 3 -> HELLO WORLD 3
// Output: HELLO WORLD 1
// Output: HELLO WORLD 2
// Output: HELLO WORLD 3
// Done! All 3 messages transformed through router.
