// Example: queues/expiration
//
// Demonstrates message expiration using QueueMessagePolicy with
// ExpirationSeconds. The message is published but expires before the
// subscriber polls for it, so no message is received.
//
// Channel: watermill-queues.expiration
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

	// Create publisher with 3-second expiration policy
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		QueueMessagePolicy: &kubemq.QueueMessagePolicy{
			ExpirationSeconds: 3,
		},
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Publish message with 3-second expiration
	msg := message.NewMessage(watermill.NewUUID(), []byte("Expiring task"))
	if err := pub.Publish("watermill-queues.expiration", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published: %s (expires in 3 seconds)\n", string(msg.Payload))

	// Wait 5 seconds -- message should expire after 3 seconds
	fmt.Println("Waiting 5 seconds for message to expire...")
	time.Sleep(5 * time.Second)

	// Create subscriber and try to receive
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 3,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	msgs, err := sub.Subscribe(ctx, "watermill-queues.expiration")
	if err != nil {
		log.Fatal(err)
	}

	// Use a short timeout to detect that no message arrives
	shortCtx, shortCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shortCancel()

	select {
	case msg := <-msgs:
		// Unexpected -- message should have expired
		fmt.Printf("Unexpected receive: %s\n", string(msg.Payload))
		msg.Ack()
	case <-shortCtx.Done():
		fmt.Println("No message received -- message expired as expected.")
	}

	fmt.Println("Done! Message expired before it could be consumed.")
}

// Expected output:
// Published: Expiring task (expires in 3 seconds)
// Waiting 5 seconds for message to expire...
// No message received -- message expired as expected.
// Done! Message expired before it could be consumed.
