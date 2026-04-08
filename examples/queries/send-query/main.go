// Example: queries/send-query
//
// Demonstrates sending a query via CQPublisher.SendQuery and handling it
// with a kubemq-go SubscribeToQueries responder. This is the hybrid approach:
// Watermill CQPublisher sends the query, native kubemq-go SDK handles the response.
// The reply is returned as a Watermill *message.Message with Payload and Metadata.
//
// Channel: watermill-cq.send-query
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
		kubemqSDK.WithClientId("query-responder"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer responderClient.Close()

	// Subscribe to queries and respond with data
	_, err = responderClient.SubscribeToQueries(ctx, "watermill-cq.send-query", "",
		kubemqSDK.WithOnQueryReceive(func(query *kubemqSDK.QueryReceive) {
			fmt.Printf("Responder received query: %s\n", string(query.Body))

			// Build and send the reply -- note the required SendQueryResponse wrapper
			reply := kubemqSDK.NewQueryReply().
				SetRequestId(query.Id).
				SetResponseTo(query.ResponseTo).
				SetExecutedAt(time.Now()).
				SetBody([]byte(`{"status":"ok","order_id":"123","total":99.95}`)).
				SetTags(map[string]string{
					"content-type": "application/json",
					"source":       "order-service",
				})

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

	// Build query message
	msg := message.NewMessage(watermill.NewUUID(), []byte("get-order-123"))

	// Send query with 10s timeout -- returns a Watermill *message.Message
	replyMsg, err := cq.SendQuery(ctx, "watermill-cq.send-query", msg, 10*time.Second)
	if err != nil {
		log.Fatalf("SendQuery failed: %v", err)
	}

	// The reply message contains the response body as Payload and tags as Metadata
	fmt.Printf("Query reply payload: %s\n", string(replyMsg.Payload))
	fmt.Printf("Query reply metadata: content-type=%s, source=%s\n",
		replyMsg.Metadata.Get("content-type"),
		replyMsg.Metadata.Get("source"))
	fmt.Println("Done!")
}

// Expected output:
// Responder received query: get-order-123
// Query reply payload: {"status":"ok","order_id":"123","total":99.95}
// Query reply metadata: content-type=application/json, source=order-service
// Done!
