// Example: events/consumer-group
//
// Demonstrates ConsumerGroup with PatternEvents for load-balanced event delivery.
// Three subscribers share the same ConsumerGroup, and 9 messages are distributed
// among them.
//
// Note: ConsumerGroup with PatternEvents depends on KubeMQ server support for
// event load-balancing. If unsupported, all subscribers may receive all messages.
//
// See also: queues/competing-consumers and advanced/competing-consumers for queue-based work distribution
//
// Channel: watermill-events.consumer-group
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
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create 3 subscribers in the same consumer group
	subs := make([]*kubemq.Subscriber, 3)
	channels := make([]<-chan *message.Message, 3)
	for i := 0; i < 3; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address:       "localhost:50000",
			Pattern:       kubemq.PatternEvents,
			ConsumerGroup: "event-workers",
			Logger:        logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()
		subs[i] = sub

		msgs, err := sub.Subscribe(ctx, "watermill-events.consumer-group")
		if err != nil {
			log.Fatal(err)
		}
		channels[i] = msgs
	}

	time.Sleep(time.Second) // Allow subscriptions to establish

	// Publish 9 messages
	for i := 1; i <= 9; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Event %d", i)))
		if err := pub.Publish("watermill-events.consumer-group", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Receive messages distributed across subscribers
	// Nil-message guard: closed channels return nil *message.Message; ok check prevents panic
	received := 0
	counts := [3]int{}
	for received < 9 {
		select {
		case msg, ok := <-channels[0]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 1 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[0]++
			received++
		case msg, ok := <-channels[1]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 2 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[1]++
			received++
		case msg, ok := <-channels[2]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 3 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[2]++
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Printf("Distribution: Sub1=%d, Sub2=%d, Sub3=%d\n", counts[0], counts[1], counts[2])
	fmt.Println("Done! All 9 messages distributed across consumer group.")
}

// Expected output (distribution varies):
// Published: Event 1
// ...
// Published: Event 9
// Subscriber 1 received: Event 1
// Subscriber 2 received: Event 2
// Subscriber 3 received: Event 3
// ...
// Distribution: Sub1=3, Sub2=3, Sub3=3
// Done! All 9 messages distributed across consumer group.
