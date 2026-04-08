// Example: cqrs/facade
//
// Demonstrates wiring Watermill's cqrs.Facade with KubeMQ transports. Commands
// use KubeMQ queues (reliable delivery) and events use KubeMQ events-store
// (persistent events). Defines domain types CreateOrderCmd and OrderCreatedEvt.
//
// Note: cqrs.Facade is deprecated in Watermill v1.5.1. The modern approach uses
// CommandProcessor and EventProcessor directly. This example demonstrates the
// Facade pattern for reference and backward compatibility.
//
// Channels:
//   - Commands: watermill-cqrs.CreateOrderCmd (queues)
//   - Events:   watermill-cqrs.OrderCreatedEvt (events-store)
//
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// --- Domain types ---

// CreateOrderCmd is a command to create a new order.
type CreateOrderCmd struct {
	OrderID string `json:"order_id"`
	Product string `json:"product"`
	Amount  int    `json:"amount"`
}

// OrderCreatedEvt is an event emitted after an order is successfully created.
type OrderCreatedEvt struct {
	OrderID   string `json:"order_id"`
	Product   string `json:"product"`
	Amount    int    `json:"amount"`
	CreatedAt string `json:"created_at"`
}

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create KubeMQ publishers ---

	// Commands publisher: uses queues for reliable command delivery
	cmdPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternQueues,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cmdPub.Close()

	// Events publisher: uses events-store for persistent event storage
	evtPub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEventsStore,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer evtPub.Close()

	// --- Create router ---
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}

	// --- Configure CQRS Facade ---
	// Facade wires command bus, event bus, command processor, and event processor together.
	// GenerateCommandsTopic/GenerateEventsTopic map command/event names to channel names.
	facade, err := cqrs.NewFacade(cqrs.FacadeConfig{
		// Command handling via KubeMQ queues
		GenerateCommandsTopic: func(commandName string) string {
			return fmt.Sprintf("watermill-cqrs.%s", commandName)
		},
		CommandsPublisher: cmdPub,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return kubemq.NewSubscriber(kubemq.SubscriberConfig{
				Address:            "localhost:50000",
				Pattern:            kubemq.PatternQueues,
				MaxItems:           1,
				WaitTimeoutSeconds: 5,
				Logger:             logger,
			})
		},
		CommandHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{
				cqrs.NewCommandHandler[CreateOrderCmd](
					"CreateOrderHandler",
					func(ctx context.Context, cmd *CreateOrderCmd) error {
						fmt.Printf("Command handler: Creating order %s (%dx %s)\n",
							cmd.OrderID, cmd.Amount, cmd.Product)

						// After handling the command, emit an event
						evt := &OrderCreatedEvt{
							OrderID:   cmd.OrderID,
							Product:   cmd.Product,
							Amount:    cmd.Amount,
							CreatedAt: time.Now().Format(time.RFC3339),
						}

						if err := eventBus.Publish(ctx, evt); err != nil {
							return fmt.Errorf("failed to publish event: %w", err)
						}
						fmt.Printf("Command handler: Published OrderCreatedEvt for %s\n", cmd.OrderID)
						return nil
					},
				),
			}
		},

		// Event handling via KubeMQ events-store
		GenerateEventsTopic: func(eventName string) string {
			return fmt.Sprintf("watermill-cqrs.%s", eventName)
		},
		EventsPublisher: evtPub,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return kubemq.NewSubscriber(kubemq.SubscriberConfig{
				Address:                "localhost:50000",
				Pattern:                kubemq.PatternEventsStore,
				EventsStoreStartOption: kubemq.StartFromNew,
				Logger:                 logger,
			})
		},
		EventHandlers: func(commandBus *cqrs.CommandBus, eventBus *cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{
				cqrs.NewEventHandler[OrderCreatedEvt](
					"OrderCreatedHandler",
					func(ctx context.Context, evt *OrderCreatedEvt) error {
						fmt.Printf("Event handler: Order %s created (%dx %s) at %s\n",
							evt.OrderID, evt.Amount, evt.Product, evt.CreatedAt)
						return nil
					},
				),
			}
		},

		Router:                router,
		CommandEventMarshaler: cqrs.JSONMarshaler{GenerateName: cqrs.StructName},
		Logger:                logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start router in background
	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router stopped: %v", err)
		}
	}()
	<-router.Running()

	// Allow subscriptions to establish
	time.Sleep(time.Second)

	// --- Send a command via the command bus ---
	cmd := &CreateOrderCmd{
		OrderID: "ORD-001",
		Product: "Widget",
		Amount:  5,
	}

	fmt.Printf("Sending command: CreateOrderCmd{OrderID=%s}\n", cmd.OrderID)
	if err := facade.CommandBus().Send(ctx, cmd); err != nil {
		log.Fatalf("Failed to send command: %v", err)
	}

	// Wait for the event handler to process the resulting event
	time.Sleep(3 * time.Second)
	fmt.Println("Done! Command -> Event flow completed via CQRS Facade.")
}

// Expected output:
// Sending command: CreateOrderCmd{OrderID=ORD-001}
// Command handler: Creating order ORD-001 (5x Widget)
// Command handler: Published OrderCreatedEvt for ORD-001
// Event handler: Order ORD-001 created (5x Widget) at <timestamp>
// Done! Command -> Event flow completed via CQRS Facade.
