// Example: tls/insecure-skip-verify
//
// Demonstrates connecting to a TLS-enabled KubeMQ broker while skipping
// certificate verification. The TLS.InsecureSkipVerify field maps to
// kubemqSDK.WithInsecureSkipVerify() internally.
//
// WARNING: DO NOT use InsecureSkipVerify in production! It disables all
// TLS certificate verification, making the connection vulnerable to
// man-in-the-middle attacks. Use this ONLY for local development or
// testing with self-signed certificates.
//
// Channel: watermill-tls.insecure-skip-verify
// Assumes KubeMQ is running on localhost:50000 with TLS enabled.
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

	// --- TLS configuration with InsecureSkipVerify ---
	// WARNING: This disables certificate verification entirely.
	// Only use for development/testing with self-signed certs.
	// In production, use CertFile or CertData with proper certificates.
	tlsConfig := &kubemq.TLSConfig{
		InsecureSkipVerify: true,
	}

	// --- Publisher with insecure TLS ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		TLS:     tlsConfig,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Subscriber with insecure TLS ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		TLS:     tlsConfig,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe to topic
	msgs, err := sub.Subscribe(ctx, "watermill-tls.insecure-skip-verify")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Insecure TLS message"))
	if err := pub.Publish("watermill-tls.insecure-skip-verify", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: Insecure TLS message")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! InsecureSkipVerify TLS connection established.")
	fmt.Println("REMINDER: Do NOT use InsecureSkipVerify in production!")
}

// Expected output (with TLS-enabled broker):
// Published: Insecure TLS message
// Received: UUID=<uuid>, Payload=Insecure TLS message
// Done! InsecureSkipVerify TLS connection established.
// REMINDER: Do NOT use InsecureSkipVerify in production!
