// Example: router/graceful-shutdown
//
// Demonstrates graceful shutdown of a Watermill Router using signal.NotifyContext.
// The router runs until SIGINT (Ctrl+C) or SIGTERM is received, then cleanly
// shuts down all handlers and closes resources.
//
// Channel: watermill-router.graceful-shutdown
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

	// Key pattern for graceful shutdown:
	// signal.NotifyContext creates a context that is cancelled on SIGINT/SIGTERM.
	// router.Run(ctx) blocks until the context is cancelled, then performs clean shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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

	// Create subscriber
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Create router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// Add a simple echo handler
	router.AddHandler(
		"echo",
		"watermill-router.graceful-shutdown",
		sub,
		"watermill-router.graceful-shutdown.out",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			fmt.Printf("Processing: %s\n", string(msg.Payload))
			// Simulate some work
			time.Sleep(100 * time.Millisecond)
			outMsg := message.NewMessage(watermill.NewUUID(), msg.Payload)
			return []*message.Message{outMsg}, nil
		},
	)

	// Publish a few test messages in background after router starts
	go func() {
		<-router.Running()
		fmt.Println("Router is running. Send SIGINT (Ctrl+C) or SIGTERM to shut down.")

		inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
			Address: "localhost:50000",
			Pattern: kubemq.PatternEvents,
			Logger:  logger,
		})
		if err != nil {
			log.Printf("Failed to create input publisher: %v", err)
			return
		}
		defer inputPub.Close()

		for i := 1; i <= 5; i++ {
			msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("message-%d", i)))
			if err := inputPub.Publish("watermill-router.graceful-shutdown", msg); err != nil {
				log.Printf("Publish error: %v", err)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}

		// After publishing, trigger shutdown for demo purposes
		fmt.Println("All messages published. Triggering shutdown...")
		cancel()
	}()

	// router.Run blocks until ctx is cancelled (SIGINT/SIGTERM or manual cancel)
	if err := router.Run(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Router shut down gracefully.")
}

// Expected output:
// Router is running. Send SIGINT (Ctrl+C) or SIGTERM to shut down.
// Processing: message-1
// Processing: message-2
// Processing: message-3
// Processing: message-4
// Processing: message-5
// All messages published. Triggering shutdown...
// Router shut down gracefully.
