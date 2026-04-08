// Example: queues/delayed-messages
//
// Demonstrates delayed message delivery using QueueMessagePolicy with
// DelaySeconds. The message is published immediately but only becomes
// available for consumption after the specified delay.
//
// Channel: watermill-queues.delayed-messages
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

	// Create publisher with 5-second delay policy
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		QueueMessagePolicy: &kubemq.QueueMessagePolicy{
			DelaySeconds: 5,
		},
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 10,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Record start time and publish
	start := time.Now()
	msg := message.NewMessage(watermill.NewUUID(), []byte("Delayed task"))
	if err := pub.Publish("watermill-queues.delayed-messages", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published at %s: %s (with 5s delay)\n", start.Format("15:04:05"), string(msg.Payload))

	// Subscribe and wait for the delayed message
	msgs, err := sub.Subscribe(ctx, "watermill-queues.delayed-messages")
	if err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-msgs:
		elapsed := time.Since(start)
		fmt.Printf("Received after %v: %s\n", elapsed.Round(time.Second), string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for delayed message")
	}

	fmt.Println("Done! Message delivered after ~5 second delay.")
}

// Expected output:
// Published at <time>: Delayed task (with 5s delay)
// Received after 5s: Delayed task
// Done! Message delivered after ~5 second delay.
