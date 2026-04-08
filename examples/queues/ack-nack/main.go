// Example: queues/ack-nack
//
// Demonstrates explicit ack and nack handling with queue messages.
// A message is published, then nacked on first receive (triggering redelivery),
// and finally acked on the second receive (removing it from the queue).
//
// The subscriber internally bridges Watermill ack/nack to KubeMQ:
//
//	<-wm.Acked()  -> qm.Ack()   (removes message from queue)
//	<-wm.Nacked() -> qm.Nack()  (redelivers message)
//
// Channel: watermill-queues.ack-nack
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

	// Publish 1 message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Important task"))
	if err := pub.Publish("watermill-queues.ack-nack", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published: %s\n", string(msg.Payload))

	// Subscribe
	msgs, err := sub.Subscribe(ctx, "watermill-queues.ack-nack")
	if err != nil {
		log.Fatal(err)
	}

	// First receive: Nack the message (simulates processing failure)
	select {
	case received := <-msgs:
		fmt.Printf("First receive: %s -- Nacking (simulating failure)\n", string(received.Payload))
		received.Nack() // Message goes back to the queue for redelivery
	case <-ctx.Done():
		log.Fatal("Timeout waiting for first message")
	}

	// Brief pause for redelivery
	time.Sleep(2 * time.Second)

	// Second receive: Ack the message (successful processing)
	select {
	case received := <-msgs:
		fmt.Printf("Second receive: %s -- Acking (processing success)\n", string(received.Payload))
		received.Ack() // Message removed from queue
	case <-ctx.Done():
		log.Fatal("Timeout waiting for redelivered message")
	}

	fmt.Println("Done! Message was nacked, redelivered, and then acked.")
}

// Expected output:
// Published: Important task
// First receive: Important task -- Nacking (simulating failure)
// Second receive: Important task -- Acking (processing success)
// Done! Message was nacked, redelivered, and then acked.
