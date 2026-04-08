// Example: advanced/competing-consumers
//
// Demonstrates competing consumers using KubeMQ queues with ConsumerGroup.
// 5 queue subscribers share a ConsumerGroup "workers", and 20 tasks are
// published. Each message is delivered to exactly one consumer (queue
// semantics), distributing the workload across all subscribers.
//
// See also: events/consumer-group for event-based load balancing
// See also: queues/competing-consumers for basic queue competition
//
// Channel: watermill-adv.competing-consumers
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
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Publish 20 tasks ---
	// Queue messages are persistent, so we can publish before subscribing.
	for i := 1; i <= 20; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Task-%d", i)))
		if err := pub.Publish("watermill-adv.competing-consumers", msg); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Published 20 tasks")

	// --- Create 5 competing consumer subscribers ---
	// All share the same ConsumerGroup "workers". KubeMQ will distribute
	// messages so that each message goes to exactly one consumer.
	const numConsumers = 5
	var counters [numConsumers]int64
	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
			Address:            "localhost:50000",
			ClientID:           fmt.Sprintf("worker-%d", i+1),
			Pattern:            kubemq.PatternQueues,
			ConsumerGroup:      "workers",
			MaxItems:           1,
			WaitTimeoutSeconds: 5,
			Logger:             logger,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()

		msgs, err := sub.Subscribe(ctx, "watermill-adv.competing-consumers")
		if err != nil {
			log.Fatal(err)
		}

		wg.Add(1)
		go func(consumerIdx int, ch <-chan *message.Message) {
			defer wg.Done()
			for {
				select {
				case msg, ok := <-ch:
					// Nil-message guard: when context is cancelled, the subscriber
					// closes the output channel. A receive on a closed channel
					// returns nil for *message.Message, and calling msg.UUID or
					// msg.Ack() on nil panics with nil pointer dereference.
					if !ok {
						return
					}
					atomic.AddInt64(&counters[consumerIdx], 1)
					fmt.Printf("  Worker-%d processed: %s\n", consumerIdx+1, string(msg.Payload))
					msg.Ack()
				case <-ctx.Done():
					return
				}
			}
		}(i, msgs)
	}

	// Wait for all 20 tasks to be processed (with a timeout)
	done := make(chan struct{})
	go func() {
		for {
			var total int64
			for i := 0; i < numConsumers; i++ {
				total += atomic.LoadInt64(&counters[i])
			}
			if total >= 20 {
				close(done)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		// All tasks processed
	case <-ctx.Done():
		log.Println("Timeout waiting for all tasks")
	}

	// Cancel context to stop consumers
	cancel()
	wg.Wait()

	// --- Print distribution summary ---
	fmt.Println("\nDistribution summary:")
	var total int64
	for i := 0; i < numConsumers; i++ {
		count := atomic.LoadInt64(&counters[i])
		total += count
		fmt.Printf("  Worker-%d: %d tasks\n", i+1, count)
	}
	fmt.Printf("Total: %d tasks (expected 20)\n", total)

	fmt.Println("Done! Competing consumers demonstrated.")
}

// Expected output:
// Published 20 tasks
//   Worker-1 processed: Task-1
//   Worker-2 processed: Task-2
//   Worker-3 processed: Task-3
//   ... (distributed across 5 workers) ...
//
// Distribution summary:
//   Worker-1: 4 tasks
//   Worker-2: 4 tasks
//   Worker-3: 4 tasks
//   Worker-4: 4 tasks
//   Worker-5: 4 tasks
// Total: 20 tasks (expected 20)
// Done! Competing consumers demonstrated.
