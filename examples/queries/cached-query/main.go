// Example: queries/cached-query
//
// Demonstrates cached queries using CQPublisher.SendQueryWithCache. The first
// query call invokes the responder and caches the result. The second call with
// the same cache key returns the cached response without hitting the responder.
//
// Channel: watermill-cq.cached-query
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
	responderClient, err := kubemqSDK.NewClient(ctx,
		kubemqSDK.WithAddress("localhost", 50000),
		kubemqSDK.WithClientId("cached-query-responder"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer responderClient.Close()

	// Track how many times the responder is invoked
	invocationCount := 0

	_, err = responderClient.SubscribeToQueries(ctx, "watermill-cq.cached-query", "",
		kubemqSDK.WithOnQueryReceive(func(query *kubemqSDK.QueryReceive) {
			invocationCount++
			fmt.Printf("Responder invoked (call #%d): %s\n", invocationCount, string(query.Body))

			reply := kubemqSDK.NewQueryReply().
				SetRequestId(query.Id).
				SetResponseTo(query.ResponseTo).
				SetExecutedAt(time.Now()).
				SetBody([]byte(fmt.Sprintf(`{"price":42.50,"computed_at":"%s"}`, time.Now().Format(time.RFC3339))))

			if err := responderClient.SendQueryResponse(ctx, reply); err != nil {
				log.Printf("Failed to send query response: %v", err)
			}
		}),
		kubemqSDK.WithOnError(func(err error) {
			log.Printf("Query subscription error: %v", err)
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

	// --- First query: cache miss -- responder is invoked ---
	msg1 := message.NewMessage(watermill.NewUUID(), []byte("get-price-ABC"))

	fmt.Println("Sending first query (cache miss)...")
	reply1, err := cq.SendQueryWithCache(ctx, "watermill-cq.cached-query", msg1,
		10*time.Second,  // timeout
		"price-key-ABC", // cacheKey
		30*time.Second,  // cacheTTL
	)
	if err != nil {
		log.Fatalf("First SendQueryWithCache failed: %v", err)
	}
	fmt.Printf("First reply payload: %s\n", string(reply1.Payload))

	// --- Second query: cache hit -- responder is NOT invoked ---
	msg2 := message.NewMessage(watermill.NewUUID(), []byte("get-price-ABC"))

	fmt.Println("Sending second query (cache hit)...")
	reply2, err := cq.SendQueryWithCache(ctx, "watermill-cq.cached-query", msg2,
		10*time.Second,  // timeout
		"price-key-ABC", // same cacheKey -- should return cached result
		30*time.Second,  // cacheTTL
	)
	if err != nil {
		log.Fatalf("Second SendQueryWithCache failed: %v", err)
	}
	fmt.Printf("Second reply payload: %s\n", string(reply2.Payload))

	// The responder should have been invoked only once
	fmt.Printf("Responder invocation count: %d (expected 1)\n", invocationCount)
	fmt.Println("Done!")
}

// Expected output:
// Sending first query (cache miss)...
// Responder invoked (call #1): get-price-ABC
// First reply payload: {"price":42.50,"computed_at":"<timestamp>"}
// Sending second query (cache hit)...
// Second reply payload: {"price":42.50,"computed_at":"<timestamp>"}
// Responder invocation count: 1 (expected 1)
// Done!
