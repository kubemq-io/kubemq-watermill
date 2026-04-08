package kubemq

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber_Subscribe_WhenClosed(t *testing.T) {
	sub := &Subscriber{
		config: SubscriberConfig{
			Pattern:     PatternEvents,
			Logger:      watermill.NopLogger{},
			Unmarshaler: DefaultMarshaler{},
		},
	}
	err := sub.Close()
	assert.NoError(t, err)

	_, err = sub.Subscribe(context.Background(), "topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestSubscriber_Subscribe_EmptyTopic(t *testing.T) {
	sub := &Subscriber{
		config: SubscriberConfig{
			Pattern:     PatternEvents,
			Logger:      watermill.NopLogger{},
			Unmarshaler: DefaultMarshaler{},
		},
	}

	_, err := sub.Subscribe(context.Background(), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestSubscriber_Subscribe_UnsupportedPattern(t *testing.T) {
	sub := &Subscriber{
		config: SubscriberConfig{
			Pattern:     PatternType(99),
			Logger:      watermill.NopLogger{},
			Unmarshaler: DefaultMarshaler{},
		},
	}

	_, err := sub.Subscribe(context.Background(), "topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestSubscriber_Close_Idempotent(t *testing.T) {
	sub := &Subscriber{
		config: SubscriberConfig{
			Pattern:     PatternEvents,
			Logger:      watermill.NopLogger{},
			Unmarshaler: DefaultMarshaler{},
		},
	}

	err1 := sub.Close()
	assert.NoError(t, err1)

	err2 := sub.Close()
	assert.NoError(t, err2)
}
