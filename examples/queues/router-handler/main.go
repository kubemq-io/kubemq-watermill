// Example: queues/router-handler
//
// Demonstrates Watermill Router-based queue processing. A handler reads from
// a pending queue, processes the message, and publishes the result to a
// completed queue.
//
// Channels: watermill-queues.pending -> watermill-queues.completed
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create publisher for output queue
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber for input queue
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

	// Create subscriber to observe completed queue
	completedSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:            "localhost:50000",
		Pattern:            kubemq.PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
		Logger:             logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer completedSub.Close()

	// Set up router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Add handler: process pending tasks and publish to completed queue
	router.AddHandler(
		"task-processor",
		"watermill-queues.pending",
		sub,
		"watermill-queues.completed",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			result := fmt.Sprintf("DONE: %s", strings.ToUpper(string(msg.Payload)))
			fmt.Printf("Handler processed: %s -> %s\n", string(msg.Payload), result)
			outMsg := message.NewMessage(watermill.NewUUID(), []byte(result))
			return []*message.Message{outMsg}, nil
		},
	)

	// Start router in background
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()

	<-router.Running()

	// Subscribe to completed queue to observe results
	completedMsgs, err := completedSub.Subscribe(ctx, "watermill-queues.completed")
	if err != nil {
		log.Fatal(err)
	}

	// Create a separate publisher for input messages
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	// Publish tasks to pending queue
	for i := 1; i <= 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("task-%d", i)))
		if err := inputPub.Publish("watermill-queues.pending", msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Published task: %s\n", string(msg.Payload))
	}

	// Receive completed results
	received := 0
	for received < 3 {
		select {
		case msg := <-completedMsgs:
			fmt.Printf("Completed: %s\n", string(msg.Payload))
			msg.Ack()
			received++
		case <-ctx.Done():
			log.Fatal("Timeout waiting for completed messages")
		}
	}

	fmt.Println("Done! All 3 tasks processed through router.")
}

// Expected output:
// Published task: task-1
// Published task: task-2
// Published task: task-3
// Handler processed: task-1 -> DONE: TASK-1
// Handler processed: task-2 -> DONE: TASK-2
// Handler processed: task-3 -> DONE: TASK-3
// Completed: DONE: TASK-1
// Completed: DONE: TASK-2
// Completed: DONE: TASK-3
// Done! All 3 tasks processed through router.
