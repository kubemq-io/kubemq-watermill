// Example: advanced/fan-out-events
//
// Demonstrates fan-out delivery with events: 1 publisher, 3 subscribers (no
// ConsumerGroup), 5 messages. Each subscriber receives all 5 messages, for a
// total of 15 deliveries. This confirms that events without ConsumerGroup are
// broadcast to all active subscribers.
//
// See also: events/multiple-subscribers for a simpler fan-out example
//
// Channel: watermill-adv.fan-out
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create publisher ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Create 3 subscribers (no ConsumerGroup = fan-out) ---
	subscribers := make([]*kubemq.Subscriber, 3)
	channels := make([]<-chan *message.Message, 3)

	for i := 0; i < 3; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address:  "localhost:50000",
			ClientID: fmt.Sprintf("fan-out-sub-%d", i+1),
			Pattern:  kubemq.PatternEvents,
			// No ConsumerGroup -- every subscriber receives every message
			Logger: logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()
		subscribers[i] = sub

		msgs, err := sub.Subscribe(ctx, "watermill-adv.fan-out")
		if err != nil {
			log.Fatal(err)
		}
		channels[i] = msgs
	}

	// Allow subscriptions to establish
	time.Sleep(time.Second)

	// --- Publish 5 messages ---
	for i := 1; i <= 5; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Fan-out message %d", i)))
		if err := pub.Publish("watermill-adv.fan-out", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published: Fan-out message %d\n", i)
	}

	// --- Count deliveries across all subscribers ---
	var totalDeliveries int64
	var wg sync.WaitGroup

	for subIdx := 0; subIdx < 3; subIdx++ {
		wg.Add(1)
		go func(idx int, ch <-chan *message.Message) {
			defer wg.Done()
			count := 0
			for count < 5 {
				select {
				case msg, ok := <-ch:
					// Nil-message guard: when context is cancelled, the subscriber
					// closes the output channel. A receive on a closed channel returns
					// nil for *message.Message, and calling msg.UUID or msg.Ack() on
					// nil panics with nil pointer dereference.
					if !ok {
						return
					}
					count++
					atomic.AddInt64(&totalDeliveries, 1)
					fmt.Printf("  Sub-%d received: %s\n", idx+1, string(msg.Payload))
					msg.Ack()
				case <-ctx.Done():
					return
				}
			}
		}(subIdx, channels[subIdx])
	}

	wg.Wait()

	total := atomic.LoadInt64(&totalDeliveries)
	fmt.Printf("\nTotal deliveries: %d (expected 15 = 3 subscribers x 5 messages)\n", total)
	if total == 15 {
		fmt.Println("Fan-out confirmed: all subscribers received all messages.")
	} else {
		fmt.Printf("Unexpected delivery count: got %d, expected 15\n", total)
	}

	fmt.Println("Done! Fan-out events demonstrated.")
}

// Expected output:
// Published: Fan-out message 1
// Published: Fan-out message 2
// Published: Fan-out message 3
// Published: Fan-out message 4
// Published: Fan-out message 5
//   Sub-1 received: Fan-out message 1
//   Sub-2 received: Fan-out message 1
//   Sub-3 received: Fan-out message 1
//   ... (all 15 deliveries) ...
//
// Total deliveries: 15 (expected 15 = 3 subscribers x 5 messages)
// Fan-out confirmed: all subscribers received all messages.
// Done! Fan-out events demonstrated.
