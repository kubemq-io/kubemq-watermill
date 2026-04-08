// Example: events-store/consumer-group
//
// Demonstrates ConsumerGroup with PatternEventsStore. Two subscribers share the
// same consumer group and replay stored messages. Messages are distributed between
// the subscribers for load-balanced processing.
//
// Channel: watermill-es.consumer-group
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

	// Publish 6 persistent messages
	for i := 1; i <= 6; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Stored event %d", i)))
		if err := pub.Publish("watermill-es.consumer-group", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	time.Sleep(time.Second) // Allow messages to be stored

	// Create 2 subscribers with the same consumer group
	subs := make([]*kubemq.Subscriber, 2)
	channels := make([]<-chan *message.Message, 2)
	for i := 0; i < 2; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address:                "localhost:50000",
			Pattern:                kubemq.PatternEventsStore,
			ConsumerGroup:          "es-workers",
			EventsStoreStartOption: kubemq.StartFromFirst,
			Logger:                 logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()
		subs[i] = sub

		msgs, err := sub.Subscribe(ctx, "watermill-es.consumer-group")
		if err != nil {
			log.Fatal(err)
		}
		channels[i] = msgs
	}

	// Receive messages distributed between the two subscribers
	received := 0
	counts := [2]int{}
	for received < 6 {
		select {
		case msg, ok := <-channels[0]:
			if !ok {
				continue
			}
			seq := msg.Metadata.Get("_kubemq_sequence")
			fmt.Printf("Subscriber 1 received: %s (seq=%s)\n", string(msg.Payload), seq)
			msg.Ack()
			counts[0]++
			received++
		case msg, ok := <-channels[1]:
			if !ok {
				continue
			}
			seq := msg.Metadata.Get("_kubemq_sequence")
			fmt.Printf("Subscriber 2 received: %s (seq=%s)\n", string(msg.Payload), seq)
			msg.Ack()
			counts[1]++
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Printf("Distribution: Sub1=%d, Sub2=%d\n", counts[0], counts[1])
	fmt.Println("Done! All 6 stored messages distributed across consumer group.")
}

// Expected output (distribution varies):
// Published: Stored event 1
// ...
// Published: Stored event 6
// Subscriber 1 received: Stored event 1 (seq=1)
// Subscriber 2 received: Stored event 2 (seq=2)
// ...
// Distribution: Sub1=3, Sub2=3
// Done! All 6 stored messages distributed across consumer group.
