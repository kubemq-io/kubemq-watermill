// Example: events-store/start-from-first
//
// Demonstrates StartFromFirst subscription option. All stored messages are
// replayed from the beginning in sequence order, followed by any new messages.
//
// Channel: watermill-es.start-from-first
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

	// Publish 5 messages first
	for i := 1; i <= 5; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Stored event %d", i)))
		if err := pub.Publish("watermill-es.start-from-first", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Subscribe with StartFromFirst -- replays all stored messages from the beginning
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromFirst,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-es.start-from-first")
	if err != nil {
		log.Fatal(err)
	}

	// All 5 messages should be replayed in sequence order
	received := 0
	for received < 5 {
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

	fmt.Println("Done! All 5 stored messages replayed from first.")
}

// Expected output:
// Published: Stored event 1
// Published: Stored event 2
// Published: Stored event 3
// Published: Stored event 4
// Published: Stored event 5
// Received: Payload=Stored event 1, Sequence=1
// Received: Payload=Stored event 2, Sequence=2
// Received: Payload=Stored event 3, Sequence=3
// Received: Payload=Stored event 4, Sequence=4
// Received: Payload=Stored event 5, Sequence=5
// Done! All 5 stored messages replayed from first.
