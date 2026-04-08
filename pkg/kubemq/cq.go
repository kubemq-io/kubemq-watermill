package kubemq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
)

// CQPublisher provides native KubeMQ command/query request-reply.
// It does NOT implement message.Publisher -- it is a separate API.
type CQPublisher struct {
	client     *kubemqSDK.Client
	config     CQConfig
	ownsClient bool
	closed     bool
	mu         sync.Mutex
}

// NewCQPublisher creates a CQPublisher for native KubeMQ request-reply.
func NewCQPublisher(config CQConfig) (*CQPublisher, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	pub := &CQPublisher{
		config: config,
	}

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
			return nil, fmt.Errorf("watermill-kubemq: create CQ client: %w", err)
		}
		pub.client = client
		pub.ownsClient = true
	}

	return pub, nil
}

// SendCommand sends a command via KubeMQ Commands.
func (p *CQPublisher) SendCommand(ctx context.Context, channel string, msg *message.Message, timeout time.Duration) (*kubemqSDK.CommandResponse, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("watermill-kubemq: CQPublisher is closed")
	}
	p.mu.Unlock()

	if timeout <= 0 {
		timeout = p.config.DefaultTimeout
	}

	marshaled, err := p.config.Marshaler.Marshal(channel, msg)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: marshal error: %w", err)
	}

	command := &kubemqSDK.Command{
		Channel: channel,
		Body:    marshaled.Body,
		Tags:    marshaled.Tags,
		Timeout: timeout,
	}

	resp, err := p.client.SendCommand(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: send command error: %w", err)
	}

	return resp, nil
}

// SendQuery sends a query via KubeMQ Queries, returns response as Watermill message.
func (p *CQPublisher) SendQuery(ctx context.Context, channel string, msg *message.Message, timeout time.Duration) (*message.Message, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("watermill-kubemq: CQPublisher is closed")
	}
	p.mu.Unlock()

	if timeout <= 0 {
		timeout = p.config.DefaultTimeout
	}

	marshaled, err := p.config.Marshaler.Marshal(channel, msg)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: marshal error: %w", err)
	}

	query := &kubemqSDK.Query{
		Channel: channel,
		Body:    marshaled.Body,
		Tags:    marshaled.Tags,
		Timeout: timeout,
	}

	if p.config.CacheKey != "" {
		query.CacheKey = p.config.CacheKey
		query.CacheTTL = p.config.CacheTTL
	}

	resp, err := p.client.SendQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: send query error: %w", err)
	}

	if !resp.Executed {
		return nil, fmt.Errorf("watermill-kubemq: query not executed: %s", resp.Error)
	}

	received := &ReceivedMessage{
		ID:   resp.QueryId,
		Body: resp.Body,
		Tags: resp.Tags,
	}
	replyMsg, err := p.config.Marshaler.Unmarshal(received)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: unmarshal reply error: %w", err)
	}

	return replyMsg, nil
}

// SendQueryWithCache sends a query with explicit per-query cache settings.
func (p *CQPublisher) SendQueryWithCache(ctx context.Context, channel string, msg *message.Message, timeout time.Duration, cacheKey string, cacheTTL time.Duration) (*message.Message, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("watermill-kubemq: CQPublisher is closed")
	}
	p.mu.Unlock()

	if timeout <= 0 {
		timeout = p.config.DefaultTimeout
	}

	marshaled, err := p.config.Marshaler.Marshal(channel, msg)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: marshal error: %w", err)
	}

	query := &kubemqSDK.Query{
		Channel:  channel,
		Body:     marshaled.Body,
		Tags:     marshaled.Tags,
		Timeout:  timeout,
		CacheKey: cacheKey,
		CacheTTL: cacheTTL,
	}

	resp, err := p.client.SendQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: send query error: %w", err)
	}

	if !resp.Executed {
		return nil, fmt.Errorf("watermill-kubemq: query not executed: %s", resp.Error)
	}

	received := &ReceivedMessage{
		ID:   resp.QueryId,
		Body: resp.Body,
		Tags: resp.Tags,
	}
	replyMsg, err := p.config.Marshaler.Unmarshal(received)
	if err != nil {
		return nil, fmt.Errorf("watermill-kubemq: unmarshal reply error: %w", err)
	}

	return replyMsg, nil
}

// Close closes the CQPublisher and releases resources.
func (p *CQPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.ownsClient {
		return p.client.Close()
	}
	return nil
}
