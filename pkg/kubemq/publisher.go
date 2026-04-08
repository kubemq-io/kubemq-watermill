package kubemq

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
)

// Publisher implements message.Publisher for KubeMQ.
// Each Publisher instance serves a single pattern (Events, EventsStore, or Queues).
// Publisher is safe for concurrent use.
type Publisher struct {
	config     PublisherConfig
	client     *kubemqSDK.Client
	ownsClient bool

	// Streaming handles (one is active based on pattern)
	eventStream      *kubemqSDK.EventStreamHandle
	eventStoreStream *kubemqSDK.EventStoreStreamHandle
	queueUpstream    *kubemqSDK.QueueUpstreamHandle

	closed int32 // atomic: 0 = open, 1 = closed
}

// NewPublisher creates a new KubeMQ Watermill Publisher.
func NewPublisher(config PublisherConfig) (*Publisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	pub := &Publisher{
		config: config,
	}

	// Create or reuse client
	if config.ExistingClient != nil {
		pub.client = config.ExistingClient
		pub.ownsClient = false
	} else {
		opts := []kubemqSDK.Option{
			kubemqSDK.WithAddress(parseHost(config.Address), parsePort(config.Address)),
		}
		if config.ClientID != "" {
			opts = append(opts, kubemqSDK.WithClientId(config.ClientID))
		}
		if config.AuthToken != "" {
			opts = append(opts, kubemqSDK.WithAuthToken(config.AuthToken))
		}
		if config.TLS != nil {
			opts = append(opts, buildTLSOptions(config.TLS)...)
		}
		client, err := kubemqSDK.NewClient(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("watermill-kubemq: create client: %w", err)
		}
		pub.client = client
		pub.ownsClient = true
	}

	// Open streaming handle based on pattern
	if !config.DisableStreaming {
		ctx := context.Background()
		switch config.Pattern {
		case PatternEvents:
			handle, err := pub.client.SendEventStream(ctx)
			if err != nil {
				if pub.ownsClient {
					_ = pub.client.Close()
				}
				return nil, fmt.Errorf("watermill-kubemq: open event stream: %w", err)
			}
			pub.eventStream = handle
			go func() {
				for err := range handle.Errors {
					config.Logger.Error("Event stream error", err, nil)
				}
			}()

		case PatternEventsStore:
			handle, err := pub.client.SendEventStoreStream(ctx)
			if err != nil {
				if pub.ownsClient {
					_ = pub.client.Close()
				}
				return nil, fmt.Errorf("watermill-kubemq: open event store stream: %w", err)
			}
			pub.eventStoreStream = handle
			// Note: handle.Results is typed <-chan *EventStreamResult (same type as Event stream).
			// EventStreamResult is shared between Event and EventStore streams in kubemq-go.
			go func() {
				for result := range handle.Results {
					if !result.Sent {
						config.Logger.Error("EventStore stream send failed",
							fmt.Errorf("event %s: %s", result.EventID, result.Error), nil)
					}
				}
			}()

		case PatternQueues:
			handle, err := pub.client.QueueUpstream(ctx)
			if err != nil {
				if pub.ownsClient {
					_ = pub.client.Close()
				}
				return nil, fmt.Errorf("watermill-kubemq: open queue upstream: %w", err)
			}
			pub.queueUpstream = handle
			go func() {
				for result := range handle.Results {
					if result.IsError {
						config.Logger.Error("Queue upstream error",
							fmt.Errorf("%s", result.Error), nil)
					}
				}
			}()
		}
	}

	config.Logger.Info("Publisher created", watermill.LogFields{
		"pattern":   config.Pattern.String(),
		"address":   config.Address,
		"streaming": !config.DisableStreaming,
	})

	return pub, nil
}

// Publish publishes messages to the given topic using the configured pattern.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return fmt.Errorf("watermill-kubemq: publisher is closed")
	}
	if topic == "" {
		return fmt.Errorf("watermill-kubemq: topic cannot be empty")
	}

	switch p.config.Pattern {
	case PatternEvents:
		return p.publishEvents(topic, messages...)
	case PatternEventsStore:
		return p.publishEventsStore(topic, messages...)
	case PatternQueues:
		return p.publishQueues(topic, messages...)
	default:
		return fmt.Errorf("watermill-kubemq: unsupported pattern %d", p.config.Pattern)
	}
}

func (p *Publisher) publishEvents(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		injectTraceContext(msg)

		marshaled, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return fmt.Errorf("watermill-kubemq: marshal error: %w", err)
		}

		event := &kubemqSDK.Event{
			Channel: topic,
			Body:    marshaled.Body,
			Tags:    marshaled.Tags,
		}

		if p.eventStream != nil {
			// Streaming path (default)
			if err := p.eventStream.Send(event); err != nil {
				return fmt.Errorf("watermill-kubemq: send event error: %w", err)
			}
		} else {
			// Non-streaming fallback (DisableStreaming: true)
			if err := p.client.SendEvent(msg.Context(), event); err != nil {
				return fmt.Errorf("watermill-kubemq: send event error: %w", err)
			}
		}

		p.config.Logger.Trace("Event published", watermill.LogFields{
			"topic": topic,
			"uuid":  msg.UUID,
		})
	}
	return nil
}

func (p *Publisher) publishEventsStore(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		injectTraceContext(msg)

		marshaled, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return fmt.Errorf("watermill-kubemq: marshal error: %w", err)
		}

		event := &kubemqSDK.EventStore{
			Channel: topic,
			Body:    marshaled.Body,
			Tags:    marshaled.Tags,
		}

		if p.eventStoreStream != nil {
			// Streaming path (default)
			if err := p.eventStoreStream.Send(event); err != nil {
				return fmt.Errorf("watermill-kubemq: send event store error: %w", err)
			}
		} else {
			// Non-streaming fallback (DisableStreaming: true)
			result, err := p.client.SendEventStore(msg.Context(), event)
			if err != nil {
				return fmt.Errorf("watermill-kubemq: send event store error: %w", err)
			}
			if !result.Sent {
				return fmt.Errorf("watermill-kubemq: event store not sent: %v", result.Err)
			}
		}

		p.config.Logger.Trace("EventStore published", watermill.LogFields{
			"topic": topic,
			"uuid":  msg.UUID,
		})
	}
	return nil
}

func (p *Publisher) publishQueues(topic string, messages ...*message.Message) error {
	queueMsgs := make([]*kubemqSDK.QueueMessage, 0, len(messages))

	for _, msg := range messages {
		injectTraceContext(msg)

		marshaled, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return fmt.Errorf("watermill-kubemq: marshal error: %w", err)
		}

		qm := &kubemqSDK.QueueMessage{
			Channel: topic,
			Body:    marshaled.Body,
			Tags:    marshaled.Tags,
		}
		if p.config.QueueMessagePolicy != nil {
			qm.Policy = &kubemqSDK.QueuePolicy{
				ExpirationSeconds: p.config.QueueMessagePolicy.ExpirationSeconds,
				DelaySeconds:      p.config.QueueMessagePolicy.DelaySeconds,
				MaxReceiveCount:   p.config.QueueMessagePolicy.MaxReceiveCount,
				MaxReceiveQueue:   p.config.QueueMessagePolicy.MaxReceiveQueue,
			}
		}
		queueMsgs = append(queueMsgs, qm)
	}

	if p.queueUpstream != nil {
		// Streaming path (default)
		requestID := watermill.NewUUID()
		if err := p.queueUpstream.Send(requestID, queueMsgs); err != nil {
			return fmt.Errorf("watermill-kubemq: send queue message error: %w", err)
		}
	} else {
		// Non-streaming fallback (DisableStreaming: true)
		results, err := p.client.SendQueueMessages(messages[0].Context(), queueMsgs)
		if err != nil {
			return fmt.Errorf("watermill-kubemq: send queue messages error: %w", err)
		}
		for _, r := range results {
			if r.IsError {
				return fmt.Errorf("watermill-kubemq: queue send error: %s", r.Error)
			}
		}
	}

	for _, msg := range messages {
		p.config.Logger.Trace("Queue message published", watermill.LogFields{
			"topic": topic,
			"uuid":  msg.UUID,
		})
	}
	return nil
}

// Close closes the publisher and releases resources.
func (p *Publisher) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.eventStream != nil {
		p.eventStream.Close()
	}
	if p.eventStoreStream != nil {
		p.eventStoreStream.Close()
	}
	if p.queueUpstream != nil {
		p.queueUpstream.Close()
	}

	if p.ownsClient && p.client != nil {
		return p.client.Close()
	}

	return nil
}

// HealthCheck verifies connectivity to the KubeMQ broker.
func (p *Publisher) HealthCheck(ctx context.Context) error {
	_, err := p.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("watermill-kubemq: health check failed: %w", err)
	}
	return nil
}

// buildTLSOptions converts TLSConfig to kubemq-go Options.
// kubemq-go uses WithCredentials(certFile, serverOverrideDomain) for file-based
// and WithCertificate(certData, serverOverrideDomain) for PEM data-based TLS.
func buildTLSOptions(tls *TLSConfig) []kubemqSDK.Option {
	var opts []kubemqSDK.Option
	if tls.CertFile != "" {
		opts = append(opts, kubemqSDK.WithCredentials(tls.CertFile, tls.ServerOverrideDomain))
	} else if tls.CertData != "" {
		opts = append(opts, kubemqSDK.WithCertificate(tls.CertData, tls.ServerOverrideDomain))
	}
	if tls.InsecureSkipVerify {
		opts = append(opts, kubemqSDK.WithInsecureSkipVerify())
	}
	return opts
}
