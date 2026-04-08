// Example: connection/existing-client
//
// Demonstrates reusing an existing kubemq-go Client across both a Publisher
// and a Subscriber. When ExistingClient is set, the Address, ClientID,
// AuthToken, and TLS fields are ignored -- the shared client's configuration
// is used instead. The caller is responsible for closing the shared client
// after closing the publisher and subscriber.
//
// Channel: watermill-conn.existing-client
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create a shared kubemq-go client ---
	// This client can be configured once with all connection parameters
	// and shared across multiple publishers and subscribers.
	client, err := kubemqSDK.NewClient(ctx,
		kubemqSDK.WithAddress("localhost", 50000),
		kubemqSDK.WithClientId("shared-client"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// --- Reuse the client in a publisher ---
	// When ExistingClient is set, Address/ClientID/AuthToken/TLS are ignored.
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		ExistingClient: client,
		Pattern:        kubemq.PatternEvents,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Reuse the same client in a subscriber ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		ExistingClient: client,
		Pattern:        kubemq.PatternEvents,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to topic
	msgs, err := sub.Subscribe(ctx, "watermill-conn.existing-client")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Message via shared client"))
	if err := pub.Publish("watermill-conn.existing-client", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: Message via shared client")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	// --- Cleanup order matters ---
	// Close publisher/subscriber first -- they do NOT close the shared client.
	pub.Close()
	sub.Close()

	// Close the shared client explicitly after all users are done.
	if err := client.Close(); err != nil {
		log.Printf("Error closing shared client: %v", err)
	}

	fmt.Println("Done! Shared client used by both publisher and subscriber.")
}

// Expected output:
// Published: Message via shared client
// Received: UUID=<uuid>, Payload=Message via shared client
// Done! Shared client used by both publisher and subscriber.
