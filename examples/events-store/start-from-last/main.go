// Example: events-store/start-from-last
//
// Demonstrates StartFromLast subscription option. Only the last stored message
// is delivered, followed by any new messages published after subscription.
//
// Channel: watermill-es.start-from-last
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
		if err := pub.Publish("watermill-es.start-from-last", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Subscribe with StartFromLast -- only the last stored message, plus any new ones
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromLast,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-es.start-from-last")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second) // Allow subscription to establish

	// Publish 1 more new message
	newMsg := message.NewMessage(watermill.NewUUID(), []byte("New event after subscribe"))
	if err := pub.Publish("watermill-es.start-from-last", newMsg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published (new): %s\n", string(newMsg.Payload))

	// Expect: last stored message ("Stored event 5") + new message
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

	fmt.Println("Done! Last stored message and new message received.")
}

// Expected output:
// Published: Stored event 1
// Published: Stored event 2
// Published: Stored event 3
// Published: Stored event 4
// Published: Stored event 5
// Published (new): New event after subscribe
// Received: Payload=Stored event 5, Sequence=5
// Received: Payload=New event after subscribe, Sequence=6
// Done! Last stored message and new message received.
