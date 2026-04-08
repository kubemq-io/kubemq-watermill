package kubemq

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
)

func TestCQPublisher_SendCommand_WhenClosed(t *testing.T) {
	cq := &CQPublisher{
		config: CQConfig{
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}
	err := cq.Close()
	assert.NoError(t, err)

	_, err = cq.SendCommand(context.Background(), "channel", nil, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestCQPublisher_SendQuery_WhenClosed(t *testing.T) {
	cq := &CQPublisher{
		config: CQConfig{
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}
	err := cq.Close()
	assert.NoError(t, err)

	_, err = cq.SendQuery(context.Background(), "channel", nil, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestCQPublisher_Close_Idempotent(t *testing.T) {
	cq := &CQPublisher{
		config: CQConfig{
			Logger:    watermill.NopLogger{},
			Marshaler: DefaultMarshaler{},
		},
	}

	err1 := cq.Close()
	assert.NoError(t, err1)

	err2 := cq.Close()
	assert.NoError(t, err2)
}
