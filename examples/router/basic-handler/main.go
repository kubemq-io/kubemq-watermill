// Example: router/basic-handler
//
// Demonstrates a Watermill Router with AddHandler that transforms messages.
// Messages published to the input topic are uppercased and forwarded to the
// output topic. A separate goroutine publishes test messages and a subscriber
// reads the transformed output.
//
// Input channel:  watermill-router.basic-handler.input
// Output channel: watermill-router.basic-handler.output
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

	// Create a separate subscriber to read from the output topic
	outputSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer outputSub.Close()

	// Subscribe to the output topic to verify transformed messages
	outputMsgs, err := outputSub.Subscribe(ctx, "watermill-router.basic-handler.output")
	if err != nil {
		log.Fatal(err)
	}

	// Create router with Recoverer middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Add handler: reads from input, transforms to uppercase, publishes to output
	router.AddHandler(
		"uppercase",
		"watermill-router.basic-handler.input",
		sub,
		"watermill-router.basic-handler.output",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			uppercased := strings.ToUpper(string(msg.Payload))
			fmt.Printf("Handler: %q -> %q\n", string(msg.Payload), uppercased)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(uppercased))
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

	// Create a separate publisher for test messages
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	// Publish 3 test messages to the input topic
	testMessages := []string{"hello world", "watermill rocks", "kubemq events"}
	for _, text := range testMessages {
		msg := message.NewMessage(watermill.NewUUID(), []byte(text))
		if err := inputPub.Publish("watermill-router.basic-handler.input", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", text)
	}

	// Read transformed messages from the output topic
	received := 0
	for received < 3 {
		select {
		case msg := <-outputMsgs:
			fmt.Printf("Output received: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for output messages")
		}
	}

	fmt.Println("Done! All 3 messages transformed and received.")
}

// Expected output:
// Published: hello world
// Published: watermill rocks
// Published: kubemq events
// Handler: "hello world" -> "HELLO WORLD"
// Handler: "watermill rocks" -> "WATERMILL ROCKS"
// Handler: "kubemq events" -> "KUBEMQ EVENTS"
// Output received: HELLO WORLD
// Output received: WATERMILL ROCKS
// Output received: KUBEMQ EVENTS
// Done! All 3 messages transformed and received.
