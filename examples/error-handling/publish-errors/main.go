// Example: error-handling/publish-errors
//
// Demonstrates common publish error scenarios and how to handle them:
// 1. Empty topic error -- the integration rejects empty topic strings.
// 2. Successful publish for comparison.
// 3. Closed publisher error -- publishing after Close() returns an error.
//
// Note: Publishing a nil *message.Message causes a panic in the publisher's
// internal trace injection (before marshaling). Avoid passing nil messages.
//
// Channel: watermill-err.publish-errors
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"fmt"
	"log"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	// --- Create publisher ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Error 1: Empty topic ---
	// The integration validates that topic is non-empty before publishing.
	msg := message.NewMessage(watermill.NewUUID(), []byte("test payload"))
	err = pub.Publish("", msg)
	if err != nil {
		fmt.Printf("Error 1 (empty topic): %v\n", err)
	}

	// --- Successful publish for comparison ---
	validMsg := message.NewMessage(watermill.NewUUID(), []byte("valid payload"))
	err = pub.Publish("watermill-err.publish-errors", validMsg)
	if err != nil {
		fmt.Printf("Unexpected error on valid publish: %v\n", err)
	} else {
		fmt.Println("Valid publish: OK")
	}

	// --- Error 2: Closed publisher ---
	// After Close(), all subsequent Publish calls return an error.
	pub.Close()
	closedMsg := message.NewMessage(watermill.NewUUID(), []byte("after close"))
	err = pub.Publish("watermill-err.publish-errors", closedMsg)
	if err != nil {
		fmt.Printf("Error 2 (closed publisher): %v\n", err)
	}

	fmt.Println("Done! All publish error scenarios demonstrated.")
}

// Expected output:
// Error 1 (empty topic): watermill-kubemq: topic cannot be empty
// Valid publish: OK
// Error 2 (closed publisher): watermill-kubemq: publisher is closed
// Done! All publish error scenarios demonstrated.
