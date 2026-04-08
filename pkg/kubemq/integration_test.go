//go:build integration

package kubemq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests require a live KubeMQ broker at localhost:50000.
// Run with: go test -v -race -tags=integration -timeout=5m ./pkg/kubemq/...

const testBrokerAddress = "localhost:50000"

func uniqueTopic(t *testing.T) string {
	return fmt.Sprintf("test_%s_%d", t.Name(), time.Now().UnixNano())
}

func newTestPublisher(t *testing.T, pattern PatternType) *Publisher {
	t.Helper()
	pub, err := NewPublisher(PublisherConfig{
		Address: testBrokerAddress,
		Pattern: pattern,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func newTestSubscriber(t *testing.T, pattern PatternType) *Subscriber {
	t.Helper()
	sub, err := NewSubscriber(SubscriberConfig{
		Address:                testBrokerAddress,
		Pattern:                pattern,
		EventsStoreStartOption: StartFromFirst,
		MaxItems:               1,
		WaitTimeoutSeconds:     1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func newTestClient(t *testing.T) *kubemqSDK.Client {
	t.Helper()
	client, err := kubemqSDK.NewClient(context.Background(),
		kubemqSDK.WithAddress("localhost", 50000),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestEventsPubSub(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternEvents)
	sub := newTestSubscriber(t, PatternEvents)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Allow subscription to establish
	time.Sleep(500 * time.Millisecond)

	sent := message.NewMessage(watermill.NewUUID(), []byte("hello-events"))
	sent.Metadata.Set("test-key", "test-value")
	err = pub.Publish(topic, sent)
	require.NoError(t, err)

	select {
	case received := <-msgs:
		assert.Equal(t, sent.UUID, received.UUID)
		assert.Equal(t, sent.Payload, received.Payload)
		assert.Equal(t, "test-value", received.Metadata.Get("test-key"))
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for event message")
	}
}

func TestQueueAckNack(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternQueues)
	sub := newTestSubscriber(t, PatternQueues)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sent := message.NewMessage(watermill.NewUUID(), []byte("queue-msg"))
	err := pub.Publish(topic, sent)
	require.NoError(t, err)

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	select {
	case received := <-msgs:
		assert.Equal(t, sent.Payload, received.Payload)
		received.Nack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for queue message (first delivery)")
	}

	select {
	case received := <-msgs:
		assert.Equal(t, sent.Payload, received.Payload)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for queue message (redelivery)")
	}
}

func TestEventsWildcard(t *testing.T) {
	baseTopic := uniqueTopic(t)
	specificTopic := baseTopic + ".sub1"

	pub := newTestPublisher(t, PatternEvents)

	// Subscribe with wildcard
	sub, err := NewSubscriber(SubscriberConfig{
		Address: testBrokerAddress,
		Pattern: PatternEvents,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wildcardTopic := baseTopic + ".*"
	msgs, err := sub.Subscribe(ctx, wildcardTopic)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	sent := message.NewMessage(watermill.NewUUID(), []byte("wildcard-msg"))
	err = pub.Publish(specificTopic, sent)
	require.NoError(t, err)

	select {
	case received := <-msgs:
		assert.Equal(t, sent.UUID, received.UUID)
		assert.Equal(t, sent.Payload, received.Payload)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for wildcard event message")
	}
}

func TestEventsStoreReplay(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternEventsStore)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish 3 messages before subscribing
	for i := 0; i < 3; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("replay-%d", i)))
		err := pub.Publish(topic, msg)
		require.NoError(t, err)
	}

	// Small delay to ensure messages are persisted
	time.Sleep(500 * time.Millisecond)

	// Subscribe with StartFromFirst to replay all
	sub, err := NewSubscriber(SubscriberConfig{
		Address:                testBrokerAddress,
		Pattern:                PatternEventsStore,
		EventsStoreStartOption: StartFromFirst,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Should receive all 3 messages
	for i := 0; i < 3; i++ {
		select {
		case received := <-msgs:
			assert.Equal(t, string(received.Payload), fmt.Sprintf("replay-%d", i))
			received.Ack()
		case <-ctx.Done():
			t.Fatalf("timeout waiting for replay message %d", i)
		}
	}
}

func TestEventsStoreStartFromSequence(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternEventsStore)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("seq-%d", i)))
		err := pub.Publish(topic, msg)
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	// Subscribe starting from sequence 3
	sub, err := NewSubscriber(SubscriberConfig{
		Address:                testBrokerAddress,
		Pattern:                PatternEventsStore,
		EventsStoreStartOption: StartFromSequence,
		EventsStoreSequence:    3,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Should receive messages from sequence 3 onward (seq-2, seq-3, seq-4)
	received := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgs:
			received = append(received, string(msg.Payload))
			// Verify _kubemq_sequence metadata is set
			seqStr := msg.Metadata.Get("_kubemq_sequence")
			assert.NotEmpty(t, seqStr, "expected _kubemq_sequence metadata")
			msg.Ack()
		case <-ctx.Done():
			t.Fatalf("timeout waiting for message %d, got %d so far: %v", i, len(received), received)
		}
	}

	assert.Contains(t, received, "seq-2")
	assert.Contains(t, received, "seq-3")
	assert.Contains(t, received, "seq-4")
}

func TestQueueDLQ(t *testing.T) {
	topic := uniqueTopic(t)
	dlqTopic := topic + "-dlq"

	// Publish with DLQ policy: MaxReceiveCount=2
	pub, err := NewPublisher(PublisherConfig{
		Address: testBrokerAddress,
		Pattern: PatternQueues,
		QueueMessagePolicy: &QueueMessagePolicy{
			MaxReceiveCount: 2,
			MaxReceiveQueue: dlqTopic,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sent := message.NewMessage(watermill.NewUUID(), []byte("dlq-msg"))
	err = pub.Publish(topic, sent)
	require.NoError(t, err)

	// Subscribe and Nack twice to trigger DLQ
	sub := newTestSubscriber(t, PatternQueues)
	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		select {
		case received := <-msgs:
			received.Nack()
		case <-ctx.Done():
			t.Fatalf("timeout waiting for queue message (delivery %d)", i+1)
		}
	}

	// Message should now be in the DLQ
	time.Sleep(1 * time.Second)

	dlqSub, err := NewSubscriber(SubscriberConfig{
		Address:            testBrokerAddress,
		Pattern:            PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dlqSub.Close() })

	dlqMsgs, err := dlqSub.Subscribe(ctx, dlqTopic)
	require.NoError(t, err)

	select {
	case received := <-dlqMsgs:
		assert.Equal(t, sent.Payload, received.Payload)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for DLQ message")
	}
}

func TestQueueCompetingConsumers(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternQueues)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	totalMsgs := 10

	// Two subscribers (competing consumers)
	sub1 := newTestSubscriber(t, PatternQueues)
	sub2 := newTestSubscriber(t, PatternQueues)

	msgs1, err := sub1.Subscribe(ctx, topic)
	require.NoError(t, err)
	msgs2, err := sub2.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Publish messages
	for i := 0; i < totalMsgs; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("competing-%d", i)))
		err := pub.Publish(topic, msg)
		require.NoError(t, err)
	}

	var count1, count2 int32
	var wg sync.WaitGroup
	wg.Add(2)

	// Consumer 1
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-msgs1:
				if !ok {
					return
				}
				atomic.AddInt32(&count1, 1)
				msg.Ack()
				if atomic.LoadInt32(&count1)+atomic.LoadInt32(&count2) >= int32(totalMsgs) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Consumer 2
	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-msgs2:
				if !ok {
					return
				}
				atomic.AddInt32(&count2, 1)
				if atomic.LoadInt32(&count1)+atomic.LoadInt32(&count2) >= int32(totalMsgs) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	total := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	assert.Equal(t, int32(totalMsgs), total, "all messages should be consumed")
	// At least one consumer should have received something (true competing)
	t.Logf("Consumer 1: %d, Consumer 2: %d", atomic.LoadInt32(&count1), atomic.LoadInt32(&count2))
}

func TestCQPublisherCommand(t *testing.T) {
	channel := uniqueTopic(t)

	// Set up a command responder using kubemq-go directly
	receiver := newTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var received atomic.Bool

	_, err := receiver.SubscribeToCommands(ctx, channel, "",
		kubemqSDK.WithOnCommandReceive(func(cmd *kubemqSDK.CommandReceive) {
			received.Store(true)
			_ = receiver.SendCommandResponse(ctx, kubemqSDK.NewCommandReply().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now()))
		}),
		kubemqSDK.WithOnError(func(err error) {
			t.Logf("command receiver error: %v", err)
		}),
	)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Send command via CQPublisher
	cq, err := NewCQPublisher(CQConfig{
		Address: testBrokerAddress,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cq.Close() })

	msg := message.NewMessage(watermill.NewUUID(), []byte("cmd-payload"))
	resp, err := cq.SendCommand(ctx, channel, msg, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, resp.Executed)
	assert.True(t, received.Load())
}

func TestCQPublisherQuery(t *testing.T) {
	channel := uniqueTopic(t)

	// Set up a query responder using kubemq-go directly
	receiver := newTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := receiver.SubscribeToQueries(ctx, channel, "",
		kubemqSDK.WithOnQueryReceive(func(q *kubemqSDK.QueryReceive) {
			_ = receiver.SendQueryResponse(ctx, kubemqSDK.NewQueryReply().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetExecutedAt(time.Now()).
				SetBody([]byte("query-response-body")).
				SetTags(map[string]string{"reply-key": "reply-value"}))
		}),
		kubemqSDK.WithOnError(func(err error) {
			t.Logf("query receiver error: %v", err)
		}),
	)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Send query via CQPublisher
	cq, err := NewCQPublisher(CQConfig{
		Address: testBrokerAddress,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cq.Close() })

	msg := message.NewMessage(watermill.NewUUID(), []byte("query-payload"))
	replyMsg, err := cq.SendQuery(ctx, channel, msg, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, replyMsg)
	assert.Equal(t, []byte("query-response-body"), []byte(replyMsg.Payload))
	assert.Equal(t, "reply-value", replyMsg.Metadata.Get("reply-key"))
}

func TestGracefulShutdown(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternQueues)

	// Publish several messages
	for i := 0; i < 5; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("shutdown-%d", i)))
		err := pub.Publish(topic, msg)
		require.NoError(t, err)
	}

	sub, err := NewSubscriber(SubscriberConfig{
		Address:            testBrokerAddress,
		Pattern:            PatternQueues,
		MaxItems:           1,
		WaitTimeoutSeconds: 1,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Receive one message but don't ack
	select {
	case received := <-msgs:
		_ = received // Don't ack or nack — hold it
		// Close subscriber while message is pending
		err = sub.Close()
		assert.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for first message")
	}

	// After close, the channel should be closed
	_, ok := <-msgs
	assert.False(t, ok, "output channel should be closed after Close()")
}

func TestReconnection(t *testing.T) {
	// Test that publisher and subscriber can be created, used, closed, and re-created
	// (simulating reconnection pattern without restarting the broker)
	topic := uniqueTopic(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First connection
	pub1 := newTestPublisher(t, PatternEvents)
	sub1 := newTestSubscriber(t, PatternEvents)

	msgs1, err := sub1.Subscribe(ctx, topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	msg1 := message.NewMessage(watermill.NewUUID(), []byte("conn-1"))
	err = pub1.Publish(topic, msg1)
	require.NoError(t, err)

	select {
	case received := <-msgs1:
		assert.Equal(t, msg1.UUID, received.UUID)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout on first connection")
	}

	// Close first connection
	_ = pub1.Close()
	_ = sub1.Close()

	// Second connection (re-create)
	pub2 := newTestPublisher(t, PatternEvents)
	sub2 := newTestSubscriber(t, PatternEvents)

	msgs2, err := sub2.Subscribe(ctx, topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	msg2 := message.NewMessage(watermill.NewUUID(), []byte("conn-2"))
	err = pub2.Publish(topic, msg2)
	require.NoError(t, err)

	select {
	case received := <-msgs2:
		assert.Equal(t, msg2.UUID, received.UUID)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout on second connection")
	}
}

func TestConcurrentPublish(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternEvents)
	sub := newTestSubscriber(t, PatternEvents)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	numGoroutines := 5
	msgsPerGoroutine := 10
	totalMsgs := numGoroutines * msgsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < msgsPerGoroutine; i++ {
				msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("g%d-m%d", gid, i)))
				if err := pub.Publish(topic, msg); err != nil {
					t.Errorf("publish error from goroutine %d: %v", gid, err)
				}
			}
		}(g)
	}
	wg.Wait()

	// Receive all messages
	receivedCount := 0
	timeout := time.After(15 * time.Second)
	for receivedCount < totalMsgs {
		select {
		case msg := <-msgs:
			msg.Ack()
			receivedCount++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages", receivedCount, totalMsgs)
		}
	}

	assert.Equal(t, totalMsgs, receivedCount)
}

func TestMetadataRoundTrip(t *testing.T) {
	topic := uniqueTopic(t)
	pub := newTestPublisher(t, PatternEvents)
	sub := newTestSubscriber(t, PatternEvents)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	sent := message.NewMessage(watermill.NewUUID(), []byte("metadata-test"))
	sent.Metadata.Set("string-key", "string-value")
	sent.Metadata.Set("empty-key", "")
	sent.Metadata.Set("unicode-key", "日本語")
	sent.Metadata.Set("special-chars", "a=b&c=d")

	err = pub.Publish(topic, sent)
	require.NoError(t, err)

	select {
	case received := <-msgs:
		assert.Equal(t, sent.UUID, received.UUID)
		assert.Equal(t, "string-value", received.Metadata.Get("string-key"))
		assert.Equal(t, "", received.Metadata.Get("empty-key"))
		assert.Equal(t, "日本語", received.Metadata.Get("unicode-key"))
		assert.Equal(t, "a=b&c=d", received.Metadata.Get("special-chars"))
		// Internal UUID tag should not leak into metadata
		assert.Empty(t, received.Metadata.Get(WatermillUUIDTag))
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for metadata message")
	}
}

func TestHealthCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub := newTestPublisher(t, PatternEvents)
	err := pub.HealthCheck(ctx)
	assert.NoError(t, err)

	sub := newTestSubscriber(t, PatternEvents)
	err = sub.HealthCheck(ctx)
	assert.NoError(t, err)
}

func TestExistingClient(t *testing.T) {
	// Create a shared kubemq-go Client
	sharedClient := newTestClient(t)

	topic := uniqueTopic(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create publisher with existing client
	pub, err := NewPublisher(PublisherConfig{
		ExistingClient:  sharedClient,
		Pattern:         PatternEvents,
		DisableStreaming: true, // use sync API to avoid stream handle ownership issues
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })

	// Create subscriber with same existing client
	sub, err := NewSubscriber(SubscriberConfig{
		ExistingClient: sharedClient,
		Pattern:        PatternEvents,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	msgs, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	sent := message.NewMessage(watermill.NewUUID(), []byte("shared-client"))
	err = pub.Publish(topic, sent)
	require.NoError(t, err)

	select {
	case received := <-msgs:
		assert.Equal(t, sent.UUID, received.UUID)
		assert.Equal(t, sent.Payload, received.Payload)
		received.Ack()
	case <-ctx.Done():
		t.Fatal("timeout waiting for message via shared client")
	}
}
