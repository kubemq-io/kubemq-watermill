// Example: events/stream-publish
//
// Demonstrates the difference between streaming and non-streaming publish.
// The default publisher uses eventStream.Send() for efficient persistent connections.
// With DisableStreaming: true, it falls back to client.SendEvent() per message.
//
// Channel: watermill-events.stream-publish
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

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Streaming publisher (default) ---
	// Internally uses eventStream.Send() over a persistent gRPC stream
	streamPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer streamPub.Close()

	// --- Non-streaming publisher ---
	// Internally uses client.SendEvent() -- one gRPC call per message
	nonStreamPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:          "localhost:50000",
		Pattern:          kubemq.PatternEvents,
		DisableStreaming: true,
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer nonStreamPub.Close()

	// Create subscriber to receive messages from both publishers
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-events.stream-publish")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second) // Allow subscription to establish

	// Publish with streaming publisher
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Streaming message %d", i)))
		if err := streamPub.Publish("watermill-events.stream-publish", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (streaming): %s\n", string(msg.Payload))
	}

	// Publish with non-streaming publisher
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Non-streaming message %d", i)))
		if err := nonStreamPub.Publish("watermill-events.stream-publish", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (non-streaming): %s\n", string(msg.Payload))
	}

	// Receive all 4 messages
	received := 0
	for received < 4 {
		select {
		case msg := <-msgs:
			fmt.Printf("Received: UUID=%s, Payload=%s\n", msg.UUID, string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 4 messages received from both publishers.")
}

// Expected output:
// Published (streaming): Streaming message 1
// Published (streaming): Streaming message 2
// Published (non-streaming): Non-streaming message 1
// Published (non-streaming): Non-streaming message 2
// Received: UUID=<uuid>, Payload=Streaming message 1
// Received: UUID=<uuid>, Payload=Streaming message 2
// Received: UUID=<uuid>, Payload=Non-streaming message 1
// Received: UUID=<uuid>, Payload=Non-streaming message 2
// Done! All 4 messages received from both publishers.
