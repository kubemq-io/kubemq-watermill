package kubemq

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func TestPublisher_Publish_WhenClosed(t *testing.T) {
	pub := &Publisher{
		config: PublisherConfig{
			Pattern:   PatternEvents,
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}
	err := pub.Close()
	assert.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	err = pub.Publish("topic", msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestPublisher_Publish_EmptyTopic(t *testing.T) {
	pub := &Publisher{
		config: PublisherConfig{
			Pattern:   PatternEvents,
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	err := pub.Publish("", msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestPublisher_Close_Idempotent(t *testing.T) {
	pub := &Publisher{
		config: PublisherConfig{
			Pattern:   PatternEvents,
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}

	err1 := pub.Close()
	assert.NoError(t, err1)

	err2 := pub.Close()
	assert.NoError(t, err2)
}

func TestPublisher_Publish_UnsupportedPattern(t *testing.T) {
	pub := &Publisher{
		config: PublisherConfig{
			Pattern:   PatternType(99),
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	err := pub.Publish("topic", msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}
