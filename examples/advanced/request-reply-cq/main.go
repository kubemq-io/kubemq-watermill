// Example: advanced/request-reply-cq
//
// Demonstrates the native KubeMQ request-reply pattern using CQPublisher
// for both SendCommand and SendQuery. The CQPublisher provides direct
// request-reply semantics without the Watermill Router/CommandProcessor
// overhead. Compare with advanced/request-reply-watermill for the
// Watermill-native approach.
//
// Architecture:
//
//	CQPublisher.SendCommand() -> [KubeMQ Commands channel] -> kubemq-go responder
//	CQPublisher.SendQuery()   -> [KubeMQ Queries channel]  -> kubemq-go responder
//
// The responder side uses kubemq-go SDK directly (SubscribeToCommands/
// SubscribeToQueries) because the Watermill Pub/Sub interface doesn't
// natively support request-reply. The CQPublisher bridges Watermill
// messages to KubeMQ commands/queries.
//
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

	// --- Create CQPublisher ---
	// CQPublisher handles marshaling Watermill messages to KubeMQ commands/queries.
	cq, err := kubemq.NewCQPublisher(kubemq.CQConfig{
		Address:        "localhost:50000",
		DefaultTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cq.Close()

	// --- Set up command responder using kubemq-go SDK ---
	// The responder subscribes to commands and sends back responses.
	// This uses the kubemq-go SDK directly because Watermill's Pub/Sub
	// interface doesn't support request-reply natively.
	responderClient, err := kubemqSDK.NewClient(ctx,
		kubemqSDK.WithAddress("localhost", 50000),
		kubemqSDK.WithClientId("cq-responder"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer responderClient.Close()

	// Subscribe to commands
	commandCh := make(chan bool, 1)
	_, err = responderClient.SubscribeToCommands(ctx, "watermill-adv.cq-commands", "",
		kubemqSDK.WithOnCommandReceive(func(cmd *kubemqSDK.CommandReceive) {
			fmt.Printf("Command responder: received command, Body=%s\n", string(cmd.Body))

			// Send command response (executed=true)
			reply := &kubemqSDK.CommandReply{
				RequestId:  cmd.Id,
				ResponseTo: cmd.ResponseTo,
				Body:       []byte("command processed"),
			}
			if err := responderClient.SendCommandResponse(ctx, reply); err != nil {
				log.Printf("SendCommandResponse error: %v", err)
			}
			commandCh <- true
		}),
		kubemqSDK.WithOnError(func(err error) {
			log.Printf("Command subscription error: %v", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to queries
	queryCh := make(chan bool, 1)
	_, err = responderClient.SubscribeToQueries(ctx, "watermill-adv.cq-queries", "",
		kubemqSDK.WithOnQueryReceive(func(query *kubemqSDK.QueryReceive) {
			fmt.Printf("Query responder: received query, Body=%s\n", string(query.Body))

			// Send query response with data payload
			reply := &kubemqSDK.QueryReply{
				RequestId:  query.Id,
				ResponseTo: query.ResponseTo,
				Body:       []byte(`{"status": "active", "count": 42}`),
				Tags:       map[string]string{"cache-hit": "false"},
			}
			if err := responderClient.SendQueryResponse(ctx, reply); err != nil {
				log.Printf("SendQueryResponse error: %v", err)
			}
			queryCh <- true
		}),
		kubemqSDK.WithOnError(func(err error) {
			log.Printf("Query subscription error: %v", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Allow subscriptions to establish
	time.Sleep(time.Second)

	// --- Send a command via CQPublisher ---
	fmt.Println("\n--- SendCommand ---")
	cmdMsg := message.NewMessage(watermill.NewUUID(), []byte("shutdown-server-1"))
	cmdResp, err := cq.SendCommand(ctx, "watermill-adv.cq-commands", cmdMsg, 10*time.Second)
	if err != nil {
		log.Fatalf("SendCommand error: %v", err)
	}
	fmt.Printf("Command response: Executed=%v\n", cmdResp.Executed)
	<-commandCh

	// --- Send a query via CQPublisher ---
	fmt.Println("\n--- SendQuery ---")
	queryMsg := message.NewMessage(watermill.NewUUID(), []byte("get-server-status"))
	replyMsg, err := cq.SendQuery(ctx, "watermill-adv.cq-queries", queryMsg, 10*time.Second)
	if err != nil {
		log.Fatalf("SendQuery error: %v", err)
	}
	fmt.Printf("Query reply: UUID=%s, Payload=%s\n", replyMsg.UUID, string(replyMsg.Payload))
	<-queryCh

	fmt.Println("\nDone! Native CQ request-reply demonstrated.")
}

// Expected output:
// --- SendCommand ---
// Command responder: received command, Body=shutdown-server-1
// Command response: Executed=true
//
// --- SendQuery ---
// Query responder: received query, Body=get-server-status
// Query reply: UUID=<uuid>, Payload={"status": "active", "count": 42}
//
// Done! Native CQ request-reply demonstrated.
