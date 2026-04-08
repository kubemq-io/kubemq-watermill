package kubemq

import (
	"fmt"
	"time"

	watermillPkg "github.com/ThreeDotsLabs/watermill"
	kubemqSDK "github.com/kubemq-io/kubemq-go/v2"
)

// PatternType determines which KubeMQ messaging pattern the Publisher or
// Subscriber uses. Each Publisher/Subscriber instance serves exactly one pattern.
type PatternType int

const (
	// PatternEvents uses KubeMQ fire-and-forget events.
	PatternEvents PatternType = iota
	// PatternEventsStore uses KubeMQ persistent events.
	PatternEventsStore
	// PatternQueues uses KubeMQ reliable queues with explicit ack/nack.
	PatternQueues
)

// String returns the string representation of the pattern type.
func (p PatternType) String() string {
	switch p {
	case PatternEvents:
		return "events"
	case PatternEventsStore:
		return "events_store"
	case PatternQueues:
		return "queues"
	default:
		return "unknown"
	}
}

// PublisherConfig configures a KubeMQ Watermill Publisher.
type PublisherConfig struct {
	// Address is the KubeMQ server address in "host:port" format.
	// Required unless ExistingClient is set.
	Address string

	// ClientID uniquely identifies this publisher to the KubeMQ broker.
	// If empty, a random UUID is generated.
	ClientID string

	// AuthToken is the JWT or token for authentication.
	AuthToken string

	// TLS configures TLS for the connection.
	TLS *TLSConfig

	// Pattern selects the KubeMQ messaging pattern.
	// Required. One of PatternEvents, PatternEventsStore, PatternQueues.
	Pattern PatternType

	// Marshaler converts Watermill messages to KubeMQ message format.
	// Default: DefaultMarshaler{}
	Marshaler Marshaler

	// ExistingClient allows reusing an existing kubemq-go Client.
	// When set, Address, ClientID, AuthToken, and TLS are ignored.
	// The caller is responsible for closing the Client.
	ExistingClient *kubemqSDK.Client

	// DisableStreaming disables streaming APIs for publishing.
	// Default: false (streaming enabled)
	DisableStreaming bool

	// QueueMessagePolicy sets default delivery policy for queue messages.
	// Only used when Pattern is PatternQueues.
	QueueMessagePolicy *QueueMessagePolicy

	// Logger is the Watermill logger adapter.
	// Default: watermill.NopLogger{}
	Logger watermillPkg.LoggerAdapter
}

// Validate checks all configuration fields.
func (c *PublisherConfig) Validate() error {
	if c.ExistingClient == nil && c.Address == "" {
		return fmt.Errorf("watermill-kubemq: Address is required when ExistingClient is nil")
	}
	if c.Pattern < PatternEvents || c.Pattern > PatternQueues {
		return fmt.Errorf("watermill-kubemq: invalid Pattern %d", c.Pattern)
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
	if c.Logger == nil {
		c.Logger = watermillPkg.NopLogger{}
	}
	return nil
}

// SubscriberConfig configures a KubeMQ Watermill Subscriber.
type SubscriberConfig struct {
	Address        string
	ClientID       string
	AuthToken      string
	TLS            *TLSConfig
	Pattern        PatternType
	Unmarshaler    Unmarshaler
	ExistingClient *kubemqSDK.Client
	ConsumerGroup  string

	// Queues-specific
	MaxItems           int32
	WaitTimeoutSeconds int32

	// EventsStore-specific
	EventsStoreStartOption EventsStoreStartOption
	EventsStoreSequence    int64
	EventsStoreStartTime   time.Time
	EventsStoreTimeDelta   time.Duration

	Logger watermillPkg.LoggerAdapter
}

// Validate checks all configuration fields.
func (c *SubscriberConfig) Validate() error {
	if c.ExistingClient == nil && c.Address == "" {
		return fmt.Errorf("watermill-kubemq: Address is required when ExistingClient is nil")
	}
	if c.Pattern < PatternEvents || c.Pattern > PatternQueues {
		return fmt.Errorf("watermill-kubemq: invalid Pattern %d", c.Pattern)
	}
	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultMarshaler{}
	}
	if c.Logger == nil {
		c.Logger = watermillPkg.NopLogger{}
	}
	if c.Pattern == PatternQueues {
		if c.MaxItems <= 0 {
			c.MaxItems = 1
		}
		if c.WaitTimeoutSeconds <= 0 {
			c.WaitTimeoutSeconds = 1
		}
	}
	if c.Pattern == PatternEventsStore {
		switch c.EventsStoreStartOption {
		case StartFromSequence:
			if c.EventsStoreSequence <= 0 {
				return fmt.Errorf("watermill-kubemq: EventsStoreSequence must be > 0 for StartFromSequence")
			}
		case StartFromTime:
			if c.EventsStoreStartTime.IsZero() {
				return fmt.Errorf("watermill-kubemq: EventsStoreStartTime must be set for StartFromTime")
			}
		case StartFromTimeDelta:
			if c.EventsStoreTimeDelta <= 0 {
				return fmt.Errorf("watermill-kubemq: EventsStoreTimeDelta must be > 0 for StartFromTimeDelta")
			}
		}
	}
	return nil
}

// EventsStoreStartOption controls the starting position for EventsStore subscriptions.
type EventsStoreStartOption int

const (
	StartFromNew EventsStoreStartOption = iota
	StartFromFirst
	StartFromLast
	StartFromSequence
	StartFromTime
	StartFromTimeDelta
)

// QueueMessagePolicy configures delivery policy for queue messages.
type QueueMessagePolicy struct {
	ExpirationSeconds int
	DelaySeconds      int
	MaxReceiveCount   int
	MaxReceiveQueue   string
}

// TLSConfig configures TLS for the KubeMQ connection.
type TLSConfig struct {
	CertFile             string
	CertData             string
	ServerOverrideDomain string
	InsecureSkipVerify   bool
}

// CQConfig configures a KubeMQ CQPublisher.
type CQConfig struct {
	Address        string
	ClientID       string
	AuthToken      string
	TLS            *TLSConfig
	ExistingClient *kubemqSDK.Client
	DefaultTimeout time.Duration
	CacheKey       string
	CacheTTL       time.Duration
	Marshaler      MarshalerUnmarshaler
	Logger         watermillPkg.LoggerAdapter
}

// Validate checks all CQ configuration fields.
func (c *CQConfig) Validate() error {
	if c.ExistingClient == nil && c.Address == "" {
		return fmt.Errorf("watermill-kubemq: Address is required when ExistingClient is nil")
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 5 * time.Second
	}
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
	if c.Logger == nil {
		c.Logger = watermillPkg.NopLogger{}
	}
	return nil
}
