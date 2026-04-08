// Example: queues/send-receive
//
// Demonstrates basic queue message send and receive with explicit acknowledgment.
// Queue messages remain in the queue until acknowledged by the consumer.
//
// Channel: watermill-queues.send-receive
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

	// Create queue subscriber
	// MaxItems: how many messages to fetch per poll cycle
	// WaitTimeoutSeconds: how long to wait for messages before returning empty
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Publish 3 queue messages (queues are persistent -- no need to subscribe first)
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Queue message %d", i)))
		if err := pub.Publish("watermill-queues.send-receive", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: %s\n", string(msg.Payload))
	}

	// Subscribe and receive messages
	msgs, err := sub.Subscribe(ctx, "watermill-queues.send-receive")
	if err != nil {
		log.Fatal(err)
	}

	received := 0
	for received < 3 {
		select {
		case msg := <-msgs:
			fmt.Printf("Received: UUID=%s, Payload=%s\n", msg.UUID, string(msg.Payload))
			// Queue messages MUST be acknowledged to remove them from the queue.
			// The subscriber internally bridges: <-msg.Acked() -> queueMsg.Ack()
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 3 queue messages received and acknowledged.")
}

// Expected output:
// Published: Queue message 1
// Published: Queue message 2
// Published: Queue message 3
// Received: UUID=<uuid>, Payload=Queue message 1
// Received: UUID=<uuid>, Payload=Queue message 2
// Received: UUID=<uuid>, Payload=Queue message 3
// Done! All 3 queue messages received and acknowledged.
