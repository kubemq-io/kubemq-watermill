// Example: events-store/start-from-time-delta
//
// Demonstrates StartFromTimeDelta subscription option. Messages published
// within the specified duration from now are delivered. Older messages are ignored.
// This is a relative time offset (e.g., "last 5 seconds").
//
// Channel: watermill-es.start-from-time-delta
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

	// Publish 3 "old" messages
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Old event %d", i)))
		if err := pub.Publish("watermill-es.start-from-time-delta", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (old): %s\n", string(msg.Payload))
	}

	// Wait 6 seconds so the old messages are older than the 5s delta
	fmt.Println("Waiting 6 seconds...")
	time.Sleep(6 * time.Second)

	// Publish 2 "recent" messages within the 5-second window
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Recent event %d", i)))
		if err := pub.Publish("watermill-es.start-from-time-delta", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (recent): %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Subscribe with StartFromTimeDelta of 5 seconds -- only recent messages
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromTimeDelta,
		EventsStoreTimeDelta:   5 * time.Second,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-es.start-from-time-delta")
	if err != nil {
		log.Fatal(err)
	}

	// Only the 2 recent messages should arrive
	received := 0
	for received < 2 {
		select {
		case msg := <-msgs:
			seq := msg.Metadata.Get("_kubemq_sequence")
			fmt.Printf("Received: Payload=%s, Sequence=%s\n", string(msg.Payload), seq)
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! Only messages within 5-second time delta received.")
}

// Expected output:
// Published (old): Old event 1
// Published (old): Old event 2
// Published (old): Old event 3
// Waiting 6 seconds...
// Published (recent): Recent event 1
// Published (recent): Recent event 2
// Received: Payload=Recent event 1, Sequence=4
// Received: Payload=Recent event 2, Sequence=5
// Done! Only messages within 5-second time delta received.
