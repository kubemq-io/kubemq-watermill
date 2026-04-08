// Example: advanced/custom-marshaler
//
// Demonstrates implementing a custom MarshalerUnmarshaler for the KubeMQ
// Watermill integration. The CompressedJSONMarshaler gzip-compresses the
// message payload before sending and decompresses it on receive, reducing
// bandwidth for large payloads.
//
// The MarshalerUnmarshaler interface requires two methods:
//
//	Marshal(topic string, msg *message.Message) (*MarshaledMessage, error)
//	Unmarshal(received *ReceivedMessage) (*message.Message, error)
//
// Satisfies: R20 (Custom marshaler example)
//
// Channel: watermill-adv.custom-marshaler
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

// --- CompressedJSONMarshaler ---
// Implements kubemq.MarshalerUnmarshaler by gzip-compressing the message
// payload. Metadata is stored in KubeMQ Tags as usual. The Watermill UUID
// is preserved in a dedicated tag.

type CompressedJSONMarshaler struct{}

// Marshal compresses the message payload with gzip and stores metadata in tags.
func (m CompressedJSONMarshaler) Marshal(topic string, msg *message.Message) (*kubemq.MarshaledMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("compressed-marshaler: message is nil")
	}

	// Gzip-compress the payload
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(msg.Payload); err != nil {
		return nil, fmt.Errorf("compressed-marshaler: gzip write error: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("compressed-marshaler: gzip close error: %w", err)
	}

	// Build tags from metadata + UUID
	tags := make(map[string]string, len(msg.Metadata)+2)
	tags["_watermill_uuid"] = msg.UUID
	tags["_content_encoding"] = "gzip"

	for k, v := range msg.Metadata {
		if k == "_watermill_uuid" || k == "_content_encoding" {
			return nil, fmt.Errorf("compressed-marshaler: metadata key %q is reserved", k)
		}
		tags[k] = v
	}

	return &kubemq.MarshaledMessage{
		Body: buf.Bytes(),
		Tags: tags,
	}, nil
}

// Unmarshal decompresses the gzip payload and restores metadata from tags.
func (m CompressedJSONMarshaler) Unmarshal(received *kubemq.ReceivedMessage) (*message.Message, error) {
	if received == nil {
		return nil, fmt.Errorf("compressed-marshaler: received message is nil")
	}

	// Check content encoding
	encoding := ""
	if received.Tags != nil {
		encoding = received.Tags["_content_encoding"]
	}

	var payload []byte
	if encoding == "gzip" {
		// Decompress gzip payload
		gz, err := gzip.NewReader(bytes.NewReader(received.Body))
		if err != nil {
			return nil, fmt.Errorf("compressed-marshaler: gzip reader error: %w", err)
		}
		defer gz.Close()

		decompressed, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("compressed-marshaler: gzip read error: %w", err)
		}
		payload = decompressed
	} else {
		// Fallback: uncompressed payload
		payload = received.Body
	}

	// Restore UUID from tags
	uuid := ""
	if received.Tags != nil {
		uuid = received.Tags["_watermill_uuid"]
	}
	if uuid == "" {
		uuid = received.ID
	}
	if uuid == "" {
		uuid = watermill.NewUUID()
	}

	// Restore metadata from tags (exclude internal keys)
	metadata := make(message.Metadata, len(received.Tags))
	for k, v := range received.Tags {
		if k == "_watermill_uuid" || k == "_content_encoding" {
			continue
		}
		metadata[k] = v
	}

	msg := message.NewMessage(uuid, payload)
	msg.Metadata = metadata
	return msg, nil
}

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create publisher with custom marshaler ---
	customMarshaler := CompressedJSONMarshaler{}

	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address:   "localhost:50000",
		Pattern:   kubemq.PatternEvents,
		Marshaler: customMarshaler,
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Create subscriber with custom unmarshaler ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address:     "localhost:50000",
		Pattern:     kubemq.PatternEvents,
		Unmarshaler: customMarshaler,
		Logger:      logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// Subscribe
	msgs, err := sub.Subscribe(ctx, "watermill-adv.custom-marshaler")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	// --- Publish a message with a large-ish JSON payload ---
	payload := `{"event": "order_created", "data": {"order_id": "ORD-12345", ` +
		`"items": [{"sku": "WIDGET-A", "qty": 10}, {"sku": "GADGET-B", "qty": 5}], ` +
		`"total": 149.90, "currency": "USD", "customer": "Alice"}}`

	msg := message.NewMessage(watermill.NewUUID(), []byte(payload))
	msg.Metadata.Set("source", "order-service")
	msg.Metadata.Set("version", "1.0")

	// Show compression ratio
	originalSize := len(payload)
	var compBuf bytes.Buffer
	gz := gzip.NewWriter(&compBuf)
	gz.Write([]byte(payload))
	gz.Close()
	compressedSize := compBuf.Len()

	fmt.Printf("Original payload size:   %d bytes\n", originalSize)
	fmt.Printf("Compressed payload size: %d bytes\n", compressedSize)
	fmt.Printf("Compression ratio:       %.1f%%\n", float64(compressedSize)/float64(originalSize)*100)

	if err := pub.Publish("watermill-adv.custom-marshaler", msg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nPublished compressed message")

	// --- Receive and verify decompression ---
	select {
	case received := <-msgs:
		fmt.Printf("\nReceived: UUID=%s\n", received.UUID)
		fmt.Printf("Payload (decompressed): %s\n", string(received.Payload))
		fmt.Printf("Metadata source=%s, version=%s\n",
			received.Metadata.Get("source"),
			received.Metadata.Get("version"))

		// Verify roundtrip integrity
		if string(received.Payload) == payload {
			fmt.Println("\nPayload integrity: OK (roundtrip preserved)")
		} else {
			fmt.Println("\nPayload integrity: MISMATCH")
		}

		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout waiting for message")
	}

	fmt.Println("Done! Custom compressed marshaler demonstrated.")
}

// Expected output:
// Original payload size:   208 bytes
// Compressed payload size: ~180 bytes (varies)
// Compression ratio:       ~86.5%
//
// Published compressed message
//
// Received: UUID=<uuid>
// Payload (decompressed): {"event": "order_created", ...}
// Metadata source=order-service, version=1.0
//
// Payload integrity: OK (roundtrip preserved)
// Done! Custom compressed marshaler demonstrated.
