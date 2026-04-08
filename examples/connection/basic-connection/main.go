// Example: connection/basic-connection
//
// Demonstrates the minimal configuration needed to connect to a KubeMQ broker
// using the Watermill integration. Shows how to set an explicit ClientID and
// explains auto-generated UUID behavior when ClientID is omitted.
//
// Channel: watermill-conn.basic-connection
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Publisher with explicit ClientID ---
	// When ClientID is set, the broker identifies this publisher by the given name.
	// When ClientID is omitted, the integration generates a random UUID automatically.
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:  "localhost:50000",
		ClientID: "my-publisher-1",
		Pattern:  kubemq.PatternEvents,
		// Marshaler defaults to DefaultMarshaler{} when nil.
		// Logger defaults to watermill.NopLogger{} when nil.
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Subscriber with explicit ClientID ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:  "localhost:50000",
		ClientID: "my-subscriber-1",
		Pattern:  kubemq.PatternEvents,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe to topic
	msgs, err := sub.Subscribe(ctx, "watermill-conn.basic-connection")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Hello from basic-connection"))
	if err := pub.Publish("watermill-conn.basic-connection", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: Hello from basic-connection")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! Basic connection established and message delivered.")
}

// Expected output:
// Published: Hello from basic-connection
// Received: UUID=<uuid>, Payload=Hello from basic-connection
// Done! Basic connection established and message delivered.
