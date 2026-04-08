// Example: error-handling/graceful-shutdown
//
// Demonstrates graceful shutdown of a Watermill Router with KubeMQ transport
// using OS signal handling. The shutdown sequence is:
// 1. Receive SIGINT or SIGTERM
// 2. Cancel the router context (stops accepting new messages)
// 3. Wait for in-flight handlers to complete
// 4. Close subscriber (cancels all active subscriptions, waits for goroutines)
// 5. Close publisher (closes streaming handles, releases connection)
//
// Channel: watermill-err.graceful-shutdown
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	// --- Create publisher ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Create subscriber ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Create router with recoverer middleware ---
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// --- Add a handler that processes messages ---
	router.AddNoPublisherHandler(
		"graceful-handler",
		"watermill-err.graceful-shutdown",
		sub,
		func(msg *message.Message) error {
			fmt.Printf("Processing: %s\n", string(msg.Payload))
			// Simulate work
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("Completed: %s\n", string(msg.Payload))
			return nil
		},
	)

	// --- Signal handling ---
	// Set up channel to receive OS signals for graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// --- Publish some messages before starting the router ---
	go func() {
		// Wait for router to start
		<-router.Running()
		fmt.Println("Router is running, publishing messages...")

		for i := 1; i <= 5; i++ {
			msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("Task %d", i)))
			if err := pub.Publish("watermill-err.graceful-shutdown", msg); err != nil {
				log.Printf("Publish error: %v", err)
				return
			}
		}
		fmt.Println("All 5 messages published.")

		// Allow time for processing, then trigger graceful shutdown
		time.Sleep(2 * time.Second)
		fmt.Println("\nTriggering graceful shutdown...")
		sigChan <- syscall.SIGINT
	}()

	// --- Run router in background and wait for signal ---
	go func() {
		if err := router.Run(context.Background()); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	fmt.Printf("\nReceived signal: %v\n", sig)

	// --- Ordered shutdown sequence ---
	fmt.Println("Step 1: Closing router (stops accepting new messages)...")
	if err := router.Close(); err != nil {
		log.Printf("Router close error: %v", err)
	}
	fmt.Println("Step 1: Router closed.")

	fmt.Println("Step 2: Closing subscriber (cancels subscriptions, waits for goroutines)...")
	if err := sub.Close(); err != nil {
		log.Printf("Subscriber close error: %v", err)
	}
	fmt.Println("Step 2: Subscriber closed.")

	fmt.Println("Step 3: Closing publisher (closes streaming handles)...")
	if err := pub.Close(); err != nil {
		log.Printf("Publisher close error: %v", err)
	}
	fmt.Println("Step 3: Publisher closed.")

	fmt.Println("Done! Graceful shutdown completed.")
}

// Expected output:
// Router is running, publishing messages...
// All 5 messages published.
// Processing: Task 1
// Completed: Task 1
// Processing: Task 2
// Completed: Task 2
// Processing: Task 3
// Completed: Task 3
// Processing: Task 4
// Completed: Task 4
// Processing: Task 5
// Completed: Task 5
//
// Triggering graceful shutdown...
//
// Received signal: interrupt
// Step 1: Closing router (stops accepting new messages)...
// Step 1: Router closed.
// Step 2: Closing subscriber (cancels subscriptions, waits for goroutines)...
// Step 2: Subscriber closed.
// Step 3: Closing publisher (closes streaming handles)...
// Step 3: Publisher closed.
// Done! Graceful shutdown completed.
