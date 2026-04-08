// Example: connection/auth-token
//
// Demonstrates connecting to a KubeMQ broker that requires JWT authentication.
// The AuthToken field is passed to the underlying kubemq-go client via
// kubemqSDK.WithAuthToken(token). Replace the placeholder token with your
// actual JWT token obtained from your KubeMQ dashboard or auth provider.
//
// Channel: watermill-conn.auth-token
// Assumes KubeMQ is running on localhost:50000 with authentication enabled.
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

	// --- Publisher with AuthToken ---
	// The AuthToken maps to kubemqSDK.WithAuthToken(token) internally.
	// Replace "your-jwt-token-here" with a valid JWT from your KubeMQ instance.
	// If your KubeMQ broker does not require authentication, this field can be omitted.
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:   "localhost:50000",
		AuthToken: "your-jwt-token-here",
		Pattern:   kubemq.PatternEvents,
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Subscriber with AuthToken ---
	// Both publisher and subscriber must use a valid token if the broker requires auth.
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:   "localhost:50000",
		AuthToken: "your-jwt-token-here",
		Pattern:   kubemq.PatternEvents,
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe to topic
	msgs, err := sub.Subscribe(ctx, "watermill-conn.auth-token")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Authenticated message"))
	if err := pub.Publish("watermill-conn.auth-token", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: Authenticated message")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! Authenticated connection established.")
}

// Expected output (with valid token and auth-enabled broker):
// Published: Authenticated message
// Received: UUID=<uuid>, Payload=Authenticated message
// Done! Authenticated connection established.
