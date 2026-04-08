// Example: middleware/correlation-id
//
// Demonstrates the Watermill CorrelationID middleware which automatically
// propagates a correlation ID from incoming messages to any outgoing messages
// produced by the handler. This enables end-to-end request tracing across
// multiple services and message hops.
//
// Input channel:  watermill-mw.correlation-in
// Output channel: watermill-mw.correlation-out
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
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

	// Create publisher for handler output
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// Create subscriber for handler input
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Create output subscriber to verify correlation ID propagation
	outSub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer outSub.Close()

	// Subscribe to output topic
	outMsgs, err := outSub.Subscribe(ctx, "watermill-mw.correlation-out")
	if err != nil {
		log.Fatal(err)
	}

	// Create router with CorrelationID middleware
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}
	router.AddMiddleware(middleware.Recoverer)

	// CorrelationID middleware: reads the correlation ID from incoming messages
	// and automatically sets it on all outgoing messages produced by the handler.
	// If no correlation ID exists on the incoming message, one is generated.
	router.AddMiddleware(middleware.CorrelationID)

	// Handler that reads the correlation ID and produces an output message
	router.AddHandler(
		"correlation-handler",
		"watermill-mw.correlation-in",
		sub,
		"watermill-mw.correlation-out",
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			correlationID := middleware.MessageCorrelationID(msg)
			fmt.Printf("Handler received: payload=%s, correlation_id=%s\n",
				string(msg.Payload), correlationID)

			// The output message automatically gets the same correlation ID
			// thanks to the CorrelationID middleware
			outMsg := message.NewMessage(watermill.NewUUID(), []byte("processed: "+string(msg.Payload)))
			return []*message.Message{outMsg}, nil
		},
	)

	// Start router
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()
	<-router.Running()

	// Create input publisher
	inputPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer inputPub.Close()

	// Publish a message with a known correlation ID
	msg := message.NewMessage(watermill.NewUUID(), []byte("order-123"))
	myCorrelationID := "req-abc-123"
	middleware.SetCorrelationID(myCorrelationID, msg)
	fmt.Printf("Published with correlation_id=%s\n", myCorrelationID)

	if err := inputPub.Publish("watermill-mw.correlation-in", msg); err != nil {
		log.Fatal(err)
	}

	// Read the output message and verify the correlation ID was propagated
	select {
	case outMsg := <-outMsgs:
		outCorrelationID := middleware.MessageCorrelationID(outMsg)
		fmt.Printf("Output received: payload=%s, correlation_id=%s\n",
			string(outMsg.Payload), outCorrelationID)

		if outCorrelationID == myCorrelationID {
			fmt.Println("Correlation ID propagated correctly!")
		} else {
			fmt.Printf("Mismatch: expected %s, got %s\n", myCorrelationID, outCorrelationID)
		}
		outMsg.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for output message")
	}

	fmt.Println("Done!")
}

// Expected output:
// Published with correlation_id=req-abc-123
// Handler received: payload=order-123, correlation_id=req-abc-123
// Output received: payload=processed: order-123, correlation_id=req-abc-123
// Correlation ID propagated correctly!
// Done!
