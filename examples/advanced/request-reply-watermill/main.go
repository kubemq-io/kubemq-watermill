// Example: advanced/request-reply-watermill
//
// Demonstrates the Watermill request-reply pattern using KubeMQ queue transport.
// Uses requestreply.NewPubSubBackend with requestreply.NewCommandHandlerWithResult
// to send a command and receive a typed reply through KubeMQ queues.
//
// Architecture:
//
//	Sender -> CommandBus -> [KubeMQ Queue: commands] -> CommandProcessor
//	                                                     -> Handler processes command
//	                                                     -> PubSubBackend publishes reply
//	Sender <- PubSubBackend <- [KubeMQ Queue: replies] <-
//
// Compare with: advanced/request-reply-cq for the native KubeMQ CQ approach.
//
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/requestreply"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// --- Command and Result types ---

// OrderCommand represents a command to place an order.
type OrderCommand struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"`
}

// OrderResult represents the reply from order processing.
type OrderResult struct {
	OrderID string  `json:"order_id"`
	Status  string  `json:"status"`
	Total   float64 `json:"total"`
}

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create KubeMQ publishers and subscribers for queue transport ---

	// Publisher for sending commands
	commandPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer commandPub.Close()

	// Publisher for sending replies
	replyPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer replyPub.Close()

	// --- Create the request-reply PubSubBackend ---
	// The backend coordinates sending commands and listening for replies
	// through KubeMQ queues.
	replyTimeout := 10 * time.Second
	backend, err := requestreply.NewPubSubBackend[OrderResult](
		requestreply.PubSubBackendConfig{
			Publisher: replyPub,
			SubscriberConstructor: func(params requestreply.PubSubBackendSubscribeParams) (message.Subscriber, error) {
				return kubemq.NewSubscriber(kubemq.SubscriberConfig{
					Address:            "localhost:50000",
					Pattern:            kubemq.PatternQueues,
					MaxItems:           1,
					WaitTimeoutSeconds: 5,
					Logger:             logger,
				})
			},
			GeneratePublishTopic: func(params requestreply.PubSubBackendPublishParams) (string, error) {
				return "watermill-adv.order-replies", nil
			},
			GenerateSubscribeTopic: func(params requestreply.PubSubBackendSubscribeParams) (string, error) {
				return "watermill-adv.order-replies", nil
			},
			Logger:                logger,
			ListenForReplyTimeout: &replyTimeout,
		},
		requestreply.BackendPubsubJSONMarshaler[OrderResult]{},
	)
	if err != nil {
		log.Fatal(err)
	}

	// --- Set up the Router and CommandProcessor ---
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}

	cqrsMarshaler := cqrs.JSONMarshaler{
		GenerateName: func(v interface{}) string {
			return fmt.Sprintf("%T", v)
		},
	}

	// Command processor subscribes to the command queue
	cp, err := cqrs.NewCommandProcessorWithConfig(router, cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return "watermill-adv.order-commands", nil
		},
		SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return kubemq.NewSubscriber(kubemq.SubscriberConfig{
				Address:            "localhost:50000",
				Pattern:            kubemq.PatternQueues,
				MaxItems:           1,
				WaitTimeoutSeconds: 5,
				Logger:             logger,
			})
		},
		Marshaler: cqrsMarshaler,
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Register the command handler with result ---
	// The handler processes OrderCommand and returns OrderResult.
	// The backend automatically publishes the result to the reply topic.
	err = cp.AddHandlers(
		requestreply.NewCommandHandlerWithResult[OrderCommand, OrderResult](
			"order-handler",
			backend,
			func(ctx context.Context, cmd *OrderCommand) (OrderResult, error) {
				fmt.Printf("Handler: Processing order %s for %d x %s\n", cmd.OrderID, cmd.Qty, cmd.Item)
				return OrderResult{
					OrderID: cmd.OrderID,
					Status:  "confirmed",
					Total:   float64(cmd.Qty) * 9.99,
				}, nil
			},
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// --- Start router in background ---
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()

	// Wait for router to start
	<-router.Running()
	fmt.Println("Router running, sending command...")

	// --- Create CommandBus to send commands ---
	commandBus, err := cqrs.NewCommandBusWithConfig(commandPub, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return "watermill-adv.order-commands", nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// --- Send command and wait for reply ---
	cmd := &OrderCommand{
		OrderID: "ORD-001",
		Item:    "widget",
		Qty:     3,
	}

	reply, err := requestreply.SendWithReply[OrderResult](ctx, commandBus, backend, cmd)
	if err != nil {
		log.Fatalf("SendWithReply error: %v", err)
	}

	if reply.Error != nil {
		log.Fatalf("Reply error: %v", reply.Error)
	}

	resultJSON, _ := json.MarshalIndent(reply.HandlerResult, "", "  ")
	fmt.Printf("Reply received:\n%s\n", string(resultJSON))

	// Cleanup
	if err := router.Close(); err != nil {
		log.Printf("Router close error: %v", err)
	}

	fmt.Println("Done! Watermill request-reply pattern demonstrated with KubeMQ queues.")
}

// Expected output:
// Router running, sending command...
// Handler: Processing order ORD-001 for 3 x widget
// Reply received:
// {
//   "order_id": "ORD-001",
//   "status": "confirmed",
//   "total": 29.97
// }
// Done! Watermill request-reply pattern demonstrated with KubeMQ queues.
