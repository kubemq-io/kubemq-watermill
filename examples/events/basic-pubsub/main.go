// Example: events/basic-pubsub
//
// Demonstrates basic fire-and-forget event publish and subscribe using
// direct Publisher/Subscriber APIs with PatternEvents.
//
// Channel: watermill-events.basic-pubsub
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

	// Create publisher
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe to topic -- must be active before publish for events (fire-and-forget)
	msgs, err := sub.Subscribe(ctx, "watermill-events.basic-pubsub")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish before publishing
	time.Sleep(time.Second)

	// Publish 3 messages
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Event message %d", i)))
		if err := pub.Publish("watermill-events.basic-pubsub", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Receive messages from subscription channel
	received := 0
	for received < 3 {
		select {
		case msg := <-msgs:
			fmt.Printf("Received: UUID=%s, Payload=%s\n", msg.UUID, string(msg.Payload))
			// Events don't require ack, but we demonstrate the pattern
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 3 messages received.")
}

// Expected output:
// Published: Event message 1
// Published: Event message 2
// Published: Event message 3
// Received: UUID=<uuid>, Payload=Event message 1
// Received: UUID=<uuid>, Payload=Event message 2
// Received: UUID=<uuid>, Payload=Event message 3
// Done! All 3 messages received.
