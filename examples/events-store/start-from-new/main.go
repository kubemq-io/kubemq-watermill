// Example: events-store/start-from-new
//
// Demonstrates StartFromNew subscription option. Historical messages published
// BEFORE the subscription are ignored. Only messages published AFTER the
// subscription is active are delivered.
//
// Channel: watermill-es.start-from-new
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
		Pattern: kubemq.PatternEventsStore,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Publish 3 "historical" messages BEFORE subscribing
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Historical event %d", i)))
		if err := pub.Publish("watermill-es.start-from-new", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (before subscribe): %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Subscribe with StartFromNew -- only receives events published AFTER subscription
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromNew,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-es.start-from-new")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second) // Allow subscription to establish

	// Publish 2 new messages AFTER subscribing
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("New event %d", i)))
		if err := pub.Publish("watermill-es.start-from-new", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (after subscribe): %s\n", string(msg.Payload))
	}

	// Only the 2 new messages should arrive
	received := 0
	for received < 2 {
		select {
		case msg := <-msgs:
			fmt.Printf("Received: UUID=%s, Payload=%s\n", msg.UUID, string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! Only 2 new messages received (3 historical messages were skipped).")
}

// Expected output:
// Published (before subscribe): Historical event 1
// Published (before subscribe): Historical event 2
// Published (before subscribe): Historical event 3
// Published (after subscribe): New event 1
// Published (after subscribe): New event 2
// Received: UUID=<uuid>, Payload=New event 1
// Received: UUID=<uuid>, Payload=New event 2
// Done! Only 2 new messages received (3 historical messages were skipped).
