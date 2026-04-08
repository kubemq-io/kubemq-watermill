// Example: queues/batch-send
//
// Demonstrates publishing multiple queue messages in a single call using
// pub.Publish("topic", msg1, msg2, ...). Internally, all messages are sent
// in one batch via queueUpstream.Send(requestID, queueMsgs).
//
// The subscriber uses MaxItems: 5 to fetch multiple messages per poll cycle,
// demonstrating batch polling on the consumer side.
//
// Channel: watermill-queues.batch-send
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

	// Build 10 messages
	msgs := make([]*message.Message, 10)
	for i := 0; i < 10; i++ {
		msgs[i] = message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Batch message %d", i+1)))
	}

	// Publish all 10 messages in one call
	// Internally this sends them as a single batch via queueUpstream.Send(requestID, queueMsgs)
	if err := pub.Publish("watermill-queues.batch-send", msgs...); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published batch of %d messages\n", len(msgs))

	// Create subscriber with batch polling (MaxItems: 5)
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           5,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	subMsgs, err := sub.Subscribe(ctx, "watermill-queues.batch-send")
	if err != nil {
		log.Fatal(err)
	}

	// Receive all 10 messages
	received := 0
	for received < 10 {
		select {
		case msg := <-subMsgs:
			fmt.Printf("Received [%d]: %s\n", received+1, string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Printf("Done! All %d batch messages received.\n", received)
}

// Expected output:
// Published batch of 10 messages
// Received [1]: Batch message 1
// Received [2]: Batch message 2
// ...
// Received [10]: Batch message 10
// Done! All 10 batch messages received.
