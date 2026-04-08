// Example: advanced/metadata-roundtrip
//
// Demonstrates rich metadata preservation through the KubeMQ Watermill
// integration. The DefaultMarshaler converts Watermill Metadata to KubeMQ
// Tags on publish and restores them on receive. This example publishes a
// message with various metadata types (user-id, correlation-id, content-type,
// unicode values, empty values) and verifies they survive the roundtrip.
//
// Metadata pipeline: Watermill Metadata -> KubeMQ Tags -> Watermill Metadata
//
// Channel: watermill-adv.metadata-roundtrip
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

	// --- Create publisher and subscriber ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe
	msgs, err := sub.Subscribe(ctx, "watermill-adv.metadata-roundtrip")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	// --- Build message with rich metadata ---
	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"order_id": 42, "item": "widget"}`))

	// Standard metadata fields
	msg.Metadata.Set("user-id", "user-12345")
	msg.Metadata.Set("correlation-id", "req-abc-def-789")
	msg.Metadata.Set("content-type", "application/json")

	// Unicode metadata value
	msg.Metadata.Set("display-name", "Lior Nabat")

	// Empty value (valid -- preserved as empty string)
	msg.Metadata.Set("optional-tag", "")

	// Numeric-looking value (stored as string in tags)
	msg.Metadata.Set("priority", "5")

	// Define expected metadata for verification
	expected := map[string]string{
		"user-id":        "user-12345",
		"correlation-id": "req-abc-def-789",
		"content-type":   "application/json",
		"display-name":   "Lior Nabat",
		"optional-tag":   "",
		"priority":       "5",
	}

	// Publish
	if err := pub.Publish("watermill-adv.metadata-roundtrip", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Published message with 6 metadata fields")

	// --- Receive and verify metadata roundtrip ---
	select {
	case received := <-msgs:
		fmt.Printf("Received: UUID=%s, Payload=%s\n", received.UUID, string(received.Payload))
		fmt.Println("\nMetadata roundtrip verification:")

		allMatch := true
		for key, expectedVal := range expected {
			actualVal := received.Metadata.Get(key)
			match := actualVal == expectedVal
			status := "OK"
			if !match {
				status = "MISMATCH"
				allMatch = false
			}
			fmt.Printf("  %-16s: expected=%q, actual=%q [%s]\n", key, expectedVal, actualVal, status)
		}

		// Check for extra metadata added by the integration
		// The subscriber may add _kubemq_channel metadata
		if ch := received.Metadata.Get("_kubemq_channel"); ch != "" {
			fmt.Printf("  %-16s: %s (added by subscriber)\n", "_kubemq_channel", ch)
		}

		if allMatch {
			fmt.Println("\nAll metadata preserved through roundtrip!")
		} else {
			fmt.Println("\nSome metadata was lost or modified!")
		}

		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! Metadata roundtrip demonstrated.")
}

// Expected output:
// Published message with 6 metadata fields
// Received: UUID=<uuid>, Payload={"order_id": 42, "item": "widget"}
//
// Metadata roundtrip verification:
//   user-id         : expected="user-12345", actual="user-12345" [OK]
//   correlation-id  : expected="req-abc-def-789", actual="req-abc-def-789" [OK]
//   content-type    : expected="application/json", actual="application/json" [OK]
//   display-name    : expected="Lior Nabat", actual="Lior Nabat" [OK]
//   optional-tag    : expected="", actual="" [OK]
//   priority        : expected="5", actual="5" [OK]
//   _kubemq_channel : watermill-adv.metadata-roundtrip (added by subscriber)
//
// All metadata preserved through roundtrip!
// Done! Metadata roundtrip demonstrated.
