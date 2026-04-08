// Example: advanced/non-streaming-publish
//
// Demonstrates DisableStreaming: true for all three messaging patterns.
// By default, the integration uses streaming gRPC calls for publishing
// (eventStream.Send, eventStoreStream.Send, queueUpstream.Send). When
// DisableStreaming is set to true, each publish uses a unary gRPC call
// instead (client.SendEvent, client.SendEventStore, client.SendQueueMessages).
//
// Non-streaming is useful when:
// - You need per-message error feedback (streaming errors are asynchronous)
// - The publisher is short-lived and doesn't benefit from stream reuse
// - You want simpler error handling at the cost of slightly higher latency
//
// Internal path differences:
//
//	Events:      eventStream.Send(event) vs client.SendEvent(ctx, event)
//	EventsStore: eventStoreStream.Send(event) vs client.SendEventStore(ctx, event)
//	Queues:      queueUpstream.Send(id, msgs) vs client.SendQueueMessages(ctx, msgs)
//
// Satisfies: R19 (Non-streaming publish example)
//
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

	// --- 1. Non-streaming Events publisher ---
	fmt.Println("=== Events (non-streaming) ===")
	eventPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:          "localhost:50000",
		Pattern:          kubemq.PatternEvents,
		DisableStreaming: true, // Uses client.SendEvent() instead of eventStream.Send()
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	eventSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	eventMsgs, err := eventSub.Subscribe(ctx, "watermill-adv.nonstream-events")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	msg := message.NewMessage(watermill.NewUUID(), []byte("Non-streaming event"))
	if err := eventPub.Publish("watermill-adv.nonstream-events", msg); err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-eventMsgs:
		fmt.Printf("  Received event: %s\n", string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	eventPub.Close()
	eventSub.Close()

	// --- 2. Non-streaming EventsStore publisher ---
	fmt.Println("\n=== EventsStore (non-streaming) ===")
	esPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:          "localhost:50000",
		Pattern:          kubemq.PatternEventsStore,
		DisableStreaming: true, // Uses client.SendEventStore() instead of eventStoreStream.Send()
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	esSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:                "localhost:50000",
		Pattern:                kubemq.PatternEventsStore,
		EventsStoreStartOption: kubemq.StartFromNew,
		Logger:                 logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	esMsgs, err := esSub.Subscribe(ctx, "watermill-adv.nonstream-es")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	esMsg := message.NewMessage(watermill.NewUUID(), []byte("Non-streaming event store"))
	if err := esPub.Publish("watermill-adv.nonstream-es", esMsg); err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-esMsgs:
		seq := received.Metadata.Get("_kubemq_sequence")
		fmt.Printf("  Received event store: %s (seq=%s)\n", string(received.Payload), seq)
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	esPub.Close()
	esSub.Close()

	// --- 3. Non-streaming Queues publisher ---
	fmt.Println("\n=== Queues (non-streaming) ===")
	qPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:          "localhost:50000",
		Pattern:          kubemq.PatternQueues,
		DisableStreaming: true, // Uses client.SendQueueMessages() instead of queueUpstream.Send()
		Logger:           logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	qSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	qMsg := message.NewMessage(watermill.NewUUID(), []byte("Non-streaming queue message"))
	if err := qPub.Publish("watermill-adv.nonstream-queues", qMsg); err != nil {
		log.Fatal(err)
	}

	qMsgs, err := qSub.Subscribe(ctx, "watermill-adv.nonstream-queues")
	if err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-qMsgs:
		fmt.Printf("  Received queue msg: %s\n", string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	qPub.Close()
	qSub.Close()

	fmt.Println("\nDone! Non-streaming publish demonstrated for all 3 patterns.")
}

// Expected output:
// === Events (non-streaming) ===
//   Received event: Non-streaming event
//
// === EventsStore (non-streaming) ===
//   Received event store: Non-streaming event store (seq=1)
//
// === Queues (non-streaming) ===
//   Received queue msg: Non-streaming queue message
//
// Done! Non-streaming publish demonstrated for all 3 patterns.
