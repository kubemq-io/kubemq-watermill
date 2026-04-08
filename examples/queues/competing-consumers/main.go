// Example: queues/competing-consumers
//
// Demonstrates competing consumers using ConsumerGroup with PatternQueues.
// Three subscribers share the same consumer group, and 9 messages are
// distributed among them for parallel processing.
//
// See also: events/consumer-group for event-based load balancing, advanced/competing-consumers for Router-based competing consumers
//
// Channel: watermill-queues.competing-consumers
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

	// Create queue publisher
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Publish 9 queue messages
	for i := 1; i <= 9; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Task %d", i)))
		if err := pub.Publish("watermill-queues.competing-consumers", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Create 3 subscribers with the same consumer group
	channels := make([]<-chan *message.Message, 3)
	for i := 0; i < 3; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address:            "localhost:50000",
			Pattern:            kubemq.PatternQueues,
			ConsumerGroup:      "queue-workers",
			MaxItems:           1,
			WaitTimeoutSeconds: 5,
			Logger:             logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()

		msgs, err := sub.Subscribe(ctx, "watermill-queues.competing-consumers")
		if err != nil {
			log.Fatal(err)
		}
		channels[i] = msgs
	}

	// Receive messages distributed across consumers
	// Nil-message guard: closed channels return nil *message.Message; ok check prevents panic
	received := 0
	counts := [3]int{}
	for received < 9 {
		select {
		case msg, ok := <-channels[0]:
			if !ok {
				continue
			}
			fmt.Printf("Consumer 1 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[0]++
			received++
		case msg, ok := <-channels[1]:
			if !ok {
				continue
			}
			fmt.Printf("Consumer 2 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[1]++
			received++
		case msg, ok := <-channels[2]:
			if !ok {
				continue
			}
			fmt.Printf("Consumer 3 received: %s\n", string(msg.Payload))
			msg.Ack()
			counts[2]++
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Printf("Distribution: Consumer1=%d, Consumer2=%d, Consumer3=%d\n",
		counts[0], counts[1], counts[2])
	fmt.Println("Done! All 9 tasks distributed across competing consumers.")
}

// Expected output (distribution varies):
// Published: Task 1
// ...
// Published: Task 9
// Consumer 1 received: Task 1
// Consumer 2 received: Task 2
// Consumer 3 received: Task 3
// ...
// Distribution: Consumer1=3, Consumer2=3, Consumer3=3
// Done! All 9 tasks distributed across competing consumers.
