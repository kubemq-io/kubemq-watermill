// Example: events-store/basic-pubsub
//
// Demonstrates basic persistent event publish and subscribe using
// PatternEventsStore with StartFromNew. Received messages include
// _kubemq_sequence and _kubemq_timestamp metadata populated by the subscriber.
//
// Channel: watermill-es.basic-pubsub
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

	// Create publisher for persistent events
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEventsStore,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber with StartFromNew -- only receives events published AFTER subscription
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

	// Subscribe first (StartFromNew requires active subscription before publish)
	msgs, err := sub.Subscribe(ctx, "watermill-es.basic-pubsub")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second) // Allow subscription to establish

	// Publish 3 persistent messages
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Persistent event %d", i)))
		if err := pub.Publish("watermill-es.basic-pubsub", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Receive messages with sequence and timestamp metadata
	received := 0
	for received < 3 {
		select {
		case msg := <-msgs:
			seq := msg.Metadata.Get("_kubemq_sequence")
			ts := msg.Metadata.Get("_kubemq_timestamp")
			fmt.Printf("Received: UUID=%s, Payload=%s, Sequence=%s, Timestamp=%s\n",
				msg.UUID, string(msg.Payload), seq, ts)
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 3 persistent messages received with sequence numbers.")
}

// Expected output:
// Published: Persistent event 1
// Published: Persistent event 2
// Published: Persistent event 3
// Received: UUID=<uuid>, Payload=Persistent event 1, Sequence=1, Timestamp=<time>
// Received: UUID=<uuid>, Payload=Persistent event 2, Sequence=2, Timestamp=<time>
// Received: UUID=<uuid>, Payload=Persistent event 3, Sequence=3, Timestamp=<time>
// Done! All 3 persistent messages received with sequence numbers.
