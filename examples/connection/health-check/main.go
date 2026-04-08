// Example: connection/health-check
//
// Demonstrates using HealthCheck on both Publisher and Subscriber to verify
// connectivity to the KubeMQ broker. HealthCheck internally calls client.Ping(ctx)
// and returns nil if the broker is reachable, or an error if not.
//
// See also: observability/health-check-endpoint for wrapping HealthCheck in an HTTP handler
//
// Channel: watermill-conn.health-check
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create publisher ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Create subscriber ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// --- Publisher health check ---
	// HealthCheck verifies the publisher can reach the broker.
	// Internally calls client.Ping(ctx) which sends an RPC ping to KubeMQ.
	if err := pub.HealthCheck(ctx); err != nil {
		fmt.Printf("Publisher health check FAILED: %v\n", err)
	} else {
		fmt.Println("Publisher health check: OK")
	}

	// --- Subscriber health check ---
	// The subscriber's HealthCheck works identically to the publisher's.
	if err := sub.HealthCheck(ctx); err != nil {
		fmt.Printf("Subscriber health check FAILED: %v\n", err)
	} else {
		fmt.Println("Subscriber health check: OK")
	}

	// --- Demonstrate health check failure scenario ---
	// After closing the publisher, its health check should fail.
	pub.Close()
	if err := pub.HealthCheck(ctx); err != nil {
		fmt.Printf("After close - Publisher health check FAILED (expected): %v\n", err)
	} else {
		fmt.Println("After close - Publisher health check: OK (unexpected)")
	}

	fmt.Println("Done! Health checks demonstrated.")
}

// Expected output:
// Publisher health check: OK
// Subscriber health check: OK
// After close - Publisher health check FAILED (expected): watermill-kubemq: health check failed: ...
// Done! Health checks demonstrated.
