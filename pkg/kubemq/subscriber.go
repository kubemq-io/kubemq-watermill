package kubemq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
)

// Subscriber implements message.Subscriber for KubeMQ.
// Each Subscriber instance serves a single pattern.
// Supports multiple concurrent Subscribe() calls on different topics.
type Subscriber struct {
	config     SubscriberConfig
	client     *kubemqSDK.Client
	ownsClient bool

	subscribersMu sync.Mutex
	subscribers   []context.CancelFunc

	wg     sync.WaitGroup
	closed bool
	mu     sync.Mutex
}

// NewSubscriber creates a new KubeMQ Watermill Subscriber.
func NewSubscriber(config SubscriberConfig) (*Subscriber, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	sub := &Subscriber{
		config: config,
	}

	if config.ExistingClient != nil {
		sub.client = config.ExistingClient
		sub.ownsClient = false
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
		sub.client = client
		sub.ownsClient = true
	}

	config.Logger.Info("Subscriber created", watermill.LogFields{
		"pattern":        config.Pattern.String(),
		"consumer_group": config.ConsumerGroup,
		"address":        config.Address,
	})

	return sub, nil
}

// Subscribe subscribes to the given topic and returns a channel of Watermill messages.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("watermill-kubemq: subscriber is closed")
	}
	s.mu.Unlock()

	if topic == "" {
		return nil, fmt.Errorf("watermill-kubemq: topic cannot be empty")
	}

	switch s.config.Pattern {
	case PatternEvents:
		return s.subscribeEvents(ctx, topic)
	case PatternEventsStore:
		return s.subscribeEventsStore(ctx, topic)
	case PatternQueues:
		return s.subscribeQueues(ctx, topic)
	default:
		return nil, fmt.Errorf("watermill-kubemq: unsupported pattern %d", s.config.Pattern)
	}
}

func (s *Subscriber) subscribeEvents(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)
	subCtx, subCancel := context.WithCancel(ctx)

	s.subscribersMu.Lock()
	s.subscribers = append(s.subscribers, subCancel)
	s.subscribersMu.Unlock()

	sub, err := s.client.SubscribeToEvents(subCtx, topic, s.config.ConsumerGroup,
		kubemqSDK.WithOnEvent(func(event *kubemqSDK.Event) {
			received := &ReceivedMessage{
				ID:   event.Id,
				Body: event.Body,
				Tags: event.Tags,
			}
			wmMsg, err := s.config.Unmarshaler.Unmarshal(received)
			if err != nil {
				s.config.Logger.Error("Unmarshal error", err, watermill.LogFields{
					"topic": topic,
				})
				return
			}
			extractTraceContext(wmMsg)

			// Propagate Event metadata to Watermill message metadata
			wmMsg.Metadata.Set("_kubemq_channel", event.Channel)

			select {
			case output <- wmMsg:
				s.config.Logger.Trace("Event delivered to output channel", watermill.LogFields{
					"topic": topic,
					"uuid":  wmMsg.UUID,
				})
			case <-subCtx.Done():
				return
			}
		}),
		kubemqSDK.WithOnError(func(err error) {
			s.config.Logger.Error("Subscription error", err, watermill.LogFields{
				"topic": topic,
			})
		}),
	)
	if err != nil {
		subCancel()
		return nil, fmt.Errorf("watermill-kubemq: subscribe to events error: %w", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(output)
		<-sub.Done()
	}()

	return output, nil
}

func (s *Subscriber) subscribeEventsStore(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)
	subCtx, subCancel := context.WithCancel(ctx)

	s.subscribersMu.Lock()
	s.subscribers = append(s.subscribers, subCancel)
	s.subscribersMu.Unlock()

	startOpt := s.resolveEventsStoreStartOption()

	sub, err := s.client.SubscribeToEventsStore(subCtx, topic, s.config.ConsumerGroup, startOpt,
		kubemqSDK.WithOnEventStoreReceive(func(event *kubemqSDK.EventStoreReceive) {
			received := &ReceivedMessage{
				ID:   event.Id,
				Body: event.Body,
				Tags: event.Tags,
			}
			wmMsg, err := s.config.Unmarshaler.Unmarshal(received)
			if err != nil {
				s.config.Logger.Error("Unmarshal error", err, watermill.LogFields{
					"topic": topic,
				})
				return
			}
			extractTraceContext(wmMsg)

			// Propagate EventsStore metadata to Watermill message metadata
			wmMsg.Metadata.Set("_kubemq_sequence", fmt.Sprintf("%d", event.Sequence))
			wmMsg.Metadata.Set("_kubemq_timestamp", event.Timestamp.Format(time.RFC3339Nano))
			wmMsg.Metadata.Set("_kubemq_channel", event.Channel)

			select {
			case output <- wmMsg:
				s.config.Logger.Trace("EventStore delivered", watermill.LogFields{
					"topic":    topic,
					"uuid":     wmMsg.UUID,
					"sequence": event.Sequence,
				})
			case <-subCtx.Done():
				return
			}
		}),
		kubemqSDK.WithOnError(func(err error) {
			s.config.Logger.Error("EventStore subscription error", err, watermill.LogFields{
				"topic": topic,
			})
		}),
	)
	if err != nil {
		subCancel()
		return nil, fmt.Errorf("watermill-kubemq: subscribe to events store error: %w", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(output)
		<-sub.Done()
	}()

	return output, nil
}

func (s *Subscriber) resolveEventsStoreStartOption() kubemqSDK.SubscriptionOption {
	switch s.config.EventsStoreStartOption {
	case StartFromFirst:
		return kubemqSDK.StartFromFirstEvent()
	case StartFromLast:
		return kubemqSDK.StartFromLastEvent()
	case StartFromSequence:
		return kubemqSDK.StartFromSequence(int(s.config.EventsStoreSequence))
	case StartFromTime:
		return kubemqSDK.StartFromTime(s.config.EventsStoreStartTime)
	case StartFromTimeDelta:
		return kubemqSDK.StartFromTimeDelta(s.config.EventsStoreTimeDelta)
	default:
		return kubemqSDK.StartFromNewEvents()
	}
}

func (s *Subscriber) subscribeQueues(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)
	subCtx, subCancel := context.WithCancel(ctx)

	s.subscribersMu.Lock()
	s.subscribers = append(s.subscribers, subCancel)
	s.subscribersMu.Unlock()

	receiver, err := s.client.NewQueueDownstreamReceiver(subCtx)
	if err != nil {
		subCancel()
		return nil, fmt.Errorf("watermill-kubemq: create queue receiver error: %w", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(output)
		defer func() { _ = receiver.Close() }()

		const (
			initialBackoff = 100 * time.Millisecond
			maxBackoff     = 5 * time.Second
		)
		backoff := time.Duration(0)
		backoffTimer := time.NewTimer(0)
		if !backoffTimer.Stop() {
			<-backoffTimer.C
		}
		defer backoffTimer.Stop()

		for {
			select {
			case <-subCtx.Done():
				return
			default:
			}

			resp, err := receiver.Poll(subCtx, &kubemqSDK.PollRequest{
				Channel:            topic,
				MaxItems:           s.config.MaxItems,
				WaitTimeoutSeconds: s.config.WaitTimeoutSeconds,
				AutoAck:            false,
			})
			if err != nil {
				if subCtx.Err() != nil {
					return
				}
				s.config.Logger.Error("Queue poll error", err, watermill.LogFields{
					"topic": topic,
				})
				if backoff == 0 {
					backoff = initialBackoff
				} else {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				backoffTimer.Reset(backoff)
				select {
				case <-backoffTimer.C:
				case <-subCtx.Done():
					return
				}
				continue
			}

			if resp.IsError {
				s.config.Logger.Error("Queue poll response error", fmt.Errorf("%s", resp.Error), watermill.LogFields{
					"topic": topic,
				})
				if backoff == 0 {
					backoff = initialBackoff
				} else {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				backoffTimer.Reset(backoff)
				select {
				case <-backoffTimer.C:
				case <-subCtx.Done():
					return
				}
				continue
			}

			// Reset backoff on successful poll
			backoff = 0

			for _, qMsg := range resp.Messages {
				received := &ReceivedMessage{
					ID:   qMsg.Message.ID,
					Body: qMsg.Message.Body,
					Tags: qMsg.Message.Tags,
				}
				wmMsg, err := s.config.Unmarshaler.Unmarshal(received)
				if err != nil {
					s.config.Logger.Error("Unmarshal error", err, watermill.LogFields{
						"topic": topic,
					})
					_ = qMsg.Nack()
					continue
				}
				extractTraceContext(wmMsg)

				// Bridge goroutine: Watermill ack/nack -> KubeMQ ack/nack
				s.wg.Add(1)
				go func(qm *kubemqSDK.QueueDownstreamMessage, wm *message.Message) {
					defer s.wg.Done()
					select {
					case <-wm.Acked():
						if err := qm.Ack(); err != nil {
							s.config.Logger.Error("Queue ack error", err, watermill.LogFields{
								"topic": topic,
								"uuid":  wm.UUID,
							})
						}
					case <-wm.Nacked():
						if err := qm.Nack(); err != nil {
							s.config.Logger.Error("Queue nack error", err, watermill.LogFields{
								"topic": topic,
								"uuid":  wm.UUID,
							})
						}
					case <-subCtx.Done():
						_ = qm.Nack()
					}
				}(qMsg, wmMsg)

				select {
				case output <- wmMsg:
					s.config.Logger.Trace("Queue message delivered", watermill.LogFields{
						"topic": topic,
						"uuid":  wmMsg.UUID,
					})
				case <-subCtx.Done():
					_ = qMsg.Nack()
					return
				}
			}
		}
	}()

	return output, nil
}

// Close closes the subscriber and all active subscriptions.
func (s *Subscriber) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock() // Release lock before blocking wait

	// Cancel all active subscriptions
	s.subscribersMu.Lock()
	for _, cancel := range s.subscribers {
		cancel()
	}
	s.subscribersMu.Unlock()

	// Wait for all subscription goroutines to finish
	s.wg.Wait()

	if s.ownsClient && s.client != nil {
		return s.client.Close()
	}

	return nil
}

// HealthCheck verifies connectivity to the KubeMQ broker.
func (s *Subscriber) HealthCheck(ctx context.Context) error {
	_, err := s.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("watermill-kubemq: health check failed: %w", err)
	}
	return nil
}
