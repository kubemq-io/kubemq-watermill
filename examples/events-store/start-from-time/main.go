// Example: events-store/start-from-time
//
// Demonstrates StartFromTime subscription option. Messages published after
// the specified absolute timestamp are delivered. Messages published before
// the cutoff are ignored.
//
// Channel: watermill-es.start-from-time
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

	// Publish 3 messages before the cutoff
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Old event %d", i)))
		if err := pub.Publish("watermill-es.start-from-time", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (before cutoff): %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Record cutoff time
	cutoff := time.Now()
	fmt.Printf("Cutoff time: %s\n", cutoff.Format(time.RFC3339Nano))

	time.Sleep(time.Second) // Ensure time separation

	// Publish 2 messages after the cutoff
	for i := 1; i <= 2; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("New event %d", i)))
		if err := pub.Publish("watermill-es.start-from-time", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published (after cutoff): %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Subscribe with StartFromTime using the cutoff timestamp
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromTime,
		EventsStoreStartTime:   cutoff,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-es.start-from-time")
	if err != nil {
		log.Fatal(err)
	}

	// Only the 2 messages after the cutoff should arrive
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

	fmt.Println("Done! Only messages after cutoff time received.")
}

// Expected output:
// Published (before cutoff): Old event 1
// Published (before cutoff): Old event 2
// Published (before cutoff): Old event 3
// Cutoff time: <timestamp>
// Published (after cutoff): New event 1
// Published (after cutoff): New event 2
// Received: Payload=New event 1, Sequence=4
// Received: Payload=New event 2, Sequence=5
// Done! Only messages after cutoff time received.
