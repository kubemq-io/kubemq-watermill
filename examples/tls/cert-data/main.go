// Example: tls/cert-data
//
// Demonstrates connecting to a TLS-enabled KubeMQ broker using inline PEM
// certificate data instead of a file path. The TLS.CertData field maps to
// kubemqSDK.WithCertificate(certData, serverOverrideDomain) internally.
// This approach is useful when certificates are injected via environment
// variables or Kubernetes secrets rather than mounted as files.
//
// Channel: watermill-tls.cert-data
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

	// --- TLS configuration with inline PEM certificate data ---
	// CertData: The full PEM-encoded certificate as a string.
	//           In production, load this from an environment variable or secret:
	//             certData := os.Getenv("KUBEMQ_TLS_CERT")
	// ServerOverrideDomain: The expected server name for TLS verification.
	tlsConfig := &kubemq.TLSConfig{
		CertData:             "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJALRiMLAh...(your PEM data)...\n-----END CERTIFICATE-----",
		ServerOverrideDomain: "kubemq.example.com",
	}

	// --- Publisher with TLS cert data ---
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

	// --- Subscriber with TLS cert data ---
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
	msgs, err := sub.Subscribe(ctx, "watermill-tls.cert-data")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("TLS message via cert data"))
	if err := pub.Publish("watermill-tls.cert-data", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: TLS message via cert data")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! TLS connection with inline cert data established.")
}

// Expected output (with valid cert data and TLS-enabled broker):
// Published: TLS message via cert data
// Received: UUID=<uuid>, Payload=TLS message via cert data
// Done! TLS connection with inline cert data established.
