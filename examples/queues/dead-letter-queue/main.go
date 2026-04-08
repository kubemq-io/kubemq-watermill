// Example: queues/dead-letter-queue
//
// Demonstrates dead letter queue (DLQ) using QueueMessagePolicy with
// MaxReceiveCount and MaxReceiveQueue. When a message is nacked more than
// MaxReceiveCount times, it is automatically moved to the DLQ channel.
//
// Channel: watermill-queues.dlq-source -> watermill-queues.dlq (dead letter)
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

	// Create publisher with DLQ policy
	// MaxReceiveCount: message moves to DLQ after 3 failed receive attempts
	// MaxReceiveQueue: the DLQ channel name
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		QueueMessagePolicy: &kubemq.QueueMessagePolicy{
			MaxReceiveCount: 3,
			MaxReceiveQueue: "watermill-queues.dlq",
		},
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Publish 1 message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Problematic task"))
	if err := pub.Publish("watermill-queues.dlq-source", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published: %s\n", string(msg.Payload))

	// Create subscriber for the source queue
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

	msgs, err := sub.Subscribe(ctx, "watermill-queues.dlq-source")
	if err != nil {
		log.Fatal(err)
	}

	// Nack the message 3 times to trigger DLQ
	for attempt := 1; attempt <= 3; attempt++ {
		select {
		case received := <-msgs:
			fmt.Printf("Attempt %d: Nacking message: %s\n", attempt, string(received.Payload))
			received.Nack()
		case <-ctx.Done():
			log.Fatal("Timeout waiting for message")
		}
		time.Sleep(time.Second) // Brief pause between attempts
	}

	time.Sleep(2 * time.Second) // Allow DLQ transfer

	// Subscribe to the DLQ to confirm message was moved there
	dlqSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer dlqSub.Close()

	dlqMsgs, err := dlqSub.Subscribe(ctx, "watermill-queues.dlq")
	if err != nil {
		log.Fatal(err)
	}

	select {
	case dlqMsg := <-dlqMsgs:
		fmt.Printf("DLQ received: %s\n", string(dlqMsg.Payload))
		dlqMsg.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for DLQ message")
	}

	fmt.Println("Done! Message moved to dead letter queue after 3 failed attempts.")
}

// Expected output:
// Published: Problematic task
// Attempt 1: Nacking message: Problematic task
// Attempt 2: Nacking message: Problematic task
// Attempt 3: Nacking message: Problematic task
// DLQ received: Problematic task
// Done! Message moved to dead letter queue after 3 failed attempts.
