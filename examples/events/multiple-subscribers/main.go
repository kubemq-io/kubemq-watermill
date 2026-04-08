// Example: events/multiple-subscribers
//
// Demonstrates fan-out delivery with multiple subscribers (no ConsumerGroup).
// When ConsumerGroup is empty, each subscriber receives every message.
// One message is published and all 3 subscribers receive it.
//
// See also: advanced/fan-out-events for a more detailed fan-out example with message counting
//
// Channel: watermill-events.multiple-subscribers
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

	// Create 3 subscribers without ConsumerGroup (fan-out)
	channels := make([]<-chan *message.Message, 3)
	for i := 0; i < 3; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address: "localhost:50000",
			Pattern: kubemq.PatternEvents,
			Logger:  logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()

		msgs, err := sub.Subscribe(ctx, "watermill-events.multiple-subscribers")
		if err != nil {
			log.Fatal(err)
		}
		channels[i] = msgs
	}

	time.Sleep(time.Second) // Allow subscriptions to establish

	// Publish 1 message -- all 3 subscribers should receive it
	msg := message.NewMessage(watermill.NewUUID(), []byte("Broadcast message"))
	if err := pub.Publish("watermill-events.multiple-subscribers", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Published: %s\n", string(msg.Payload))

	// Receive from all 3 subscribers
	// Nil-message guard: closed channels return nil *message.Message; ok check prevents panic
	received := 0
	for received < 3 {
		select {
		case msg, ok := <-channels[0]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 1 received: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case msg, ok := <-channels[1]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 2 received: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case msg, ok := <-channels[2]:
			if !ok {
				continue
			}
			fmt.Printf("Subscriber 3 received: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for messages")
		}
	}

	fmt.Println("Done! All 3 subscribers received the broadcast message.")
}

// Expected output:
// Published: Broadcast message
// Subscriber 1 received: Broadcast message
// Subscriber 2 received: Broadcast message
// Subscriber 3 received: Broadcast message
// Done! All 3 subscribers received the broadcast message.
