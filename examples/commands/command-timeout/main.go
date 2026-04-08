// Example: commands/command-timeout
//
// Demonstrates command timeout behavior when no responder is available.
// SendCommand returns a timeout error after the specified duration. Shows
// both DefaultTimeout (from CQConfig) and per-call timeout override.
//
// Channel: watermill-cq.command-timeout
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create CQPublisher with a 5s default timeout
	cq, err := kubemq.NewCQPublisher(kubemq.CQConfig{
		Address:        "localhost:50000",
		DefaultTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cq.Close()

	// --- Attempt 1: Use per-call timeout override (2s) ---
	// No responder is running, so the command will time out after 2 seconds.
	msg1 := message.NewMessage(watermill.NewUUID(), []byte("timeout-test-1"))

	fmt.Println("Sending command with 2s per-call timeout (no responder)...")
	start1 := time.Now()
	_, err = cq.SendCommand(ctx, "watermill-cq.command-timeout", msg1, 2*time.Second)
	elapsed1 := time.Since(start1)

	if err != nil {
		fmt.Printf("Expected timeout error (2s override): %v (elapsed: %v)\n", err, elapsed1.Truncate(time.Millisecond))
	} else {
		fmt.Println("Unexpected: command succeeded without a responder")
	}

	// --- Attempt 2: Use DefaultTimeout (5s) by passing 0 for timeout ---
	// When timeout <= 0, CQPublisher falls back to DefaultTimeout from CQConfig.
	msg2 := message.NewMessage(watermill.NewUUID(), []byte("timeout-test-2"))

	fmt.Println("Sending command with DefaultTimeout (5s, pass 0 for timeout)...")
	start2 := time.Now()
	_, err = cq.SendCommand(ctx, "watermill-cq.command-timeout", msg2, 0)
	elapsed2 := time.Since(start2)

	if err != nil {
		fmt.Printf("Expected timeout error (5s default): %v (elapsed: %v)\n", err, elapsed2.Truncate(time.Millisecond))
	} else {
		fmt.Println("Unexpected: command succeeded without a responder")
	}

	fmt.Println("Done! Both commands timed out as expected.")
}

// Expected output:
// Sending command with 2s per-call timeout (no responder)...
// Expected timeout error (2s override): watermill-kubemq: send command error: ... (elapsed: ~2s)
// Sending command with DefaultTimeout (5s, pass 0 for timeout)...
// Expected timeout error (5s default): watermill-kubemq: send command error: ... (elapsed: ~5s)
// Done! Both commands timed out as expected.
