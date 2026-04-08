// Example: commands/send-command
//
// Demonstrates sending a command via CQPublisher.SendCommand and handling it
// with a kubemq-go SubscribeToCommands responder. This is the hybrid approach:
// Watermill CQPublisher sends the command, native kubemq-go SDK handles the response.
//
// Channel: watermill-cq.send-command
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Responder side: kubemq-go native SDK ---
	// The responder uses the kubemq-go SDK directly because Watermill's
	// Publisher/Subscriber interface doesn't support request-reply natively.
	responderClient, err := kubemqSDK.NewClient(ctx,
		kubemqSDK.WithAddress("localhost", 50000),
		kubemqSDK.WithClientId("command-responder"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer responderClient.Close()

	// Subscribe to commands and respond
	_, err = responderClient.SubscribeToCommands(ctx, "watermill-cq.send-command", "",
		kubemqSDK.WithOnCommandReceive(func(cmd *kubemqSDK.CommandReceive) {
			fmt.Printf("Responder received command: %s\n", string(cmd.Body))

			// Build and send the reply -- note the required SendCommandResponse wrapper
			reply := kubemqSDK.NewCommandReply().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now())

			if err := responderClient.SendCommandResponse(ctx, reply); err != nil {
				log.Printf("Failed to send command response: %v", err)
			}
		}),
		kubemqSDK.WithOnError(func(err error) {
			log.Printf("Command subscription error: %v", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second) // Allow subscription to establish

	// --- Sender side: Watermill CQPublisher ---
	cq, err := kubemq.NewCQPublisher(kubemq.CQConfig{
		Address:        "localhost:50000",
		DefaultTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cq.Close()

	// Build command message
	msg := message.NewMessage(watermill.NewUUID(), []byte("process-order-123"))

	// Send command with 10s timeout
	resp, err := cq.SendCommand(ctx, "watermill-cq.send-command", msg, 10*time.Second)
	if err != nil {
		log.Fatalf("SendCommand failed: %v", err)
	}

	fmt.Printf("Command response: Executed=%v\n", resp.Executed)
	fmt.Println("Done!")
}

// Expected output:
// Responder received command: process-order-123
// Command response: Executed=true
// Done!
