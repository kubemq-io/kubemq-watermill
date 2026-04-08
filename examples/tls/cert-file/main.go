// Example: tls/cert-file
//
// Demonstrates connecting to a TLS-enabled KubeMQ broker using a certificate
// file path. The TLS.CertFile field maps to kubemqSDK.WithCredentials(certFile,
// serverOverrideDomain) internally. ServerOverrideDomain is used for TLS
// server name verification when the broker's certificate CN differs from
// the connection address.
//
// Channel: watermill-tls.cert-file
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

	// --- TLS configuration with certificate file ---
	// CertFile: Path to the PEM-encoded CA certificate file used to verify
	//           the KubeMQ server's TLS certificate.
	// ServerOverrideDomain: Override the expected server name in the TLS handshake.
	//                       Use this when the broker's certificate CN or SAN doesn't
	//                       match "localhost" (the connection address).
	tlsConfig := &kubemq.TLSConfig{
		CertFile:             "/path/to/cert.pem",
		ServerOverrideDomain: "kubemq.example.com",
	}

	// --- Publisher with TLS ---
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

	// --- Subscriber with TLS ---
	// Both publisher and subscriber must use the same TLS configuration
	// to connect to a TLS-enabled broker.
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
	msgs, err := sub.Subscribe(ctx, "watermill-tls.cert-file")
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscription to establish
	time.Sleep(time.Second)

	// Publish a test message
	msg := message.NewMessage(watermill.NewUUID(), []byte("TLS-encrypted message"))
	if err := pub.Publish("watermill-tls.cert-file", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published: TLS-encrypted message")

	// Receive the message
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! TLS connection with cert file established.")
}

// Expected output (with valid cert and TLS-enabled broker):
// Published: TLS-encrypted message
// Received: UUID=<uuid>, Payload=TLS-encrypted message
// Done! TLS connection with cert file established.
