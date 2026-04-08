package kubemq

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	// WatermillUUIDTag is the KubeMQ Tag key used to store the Watermill message UUID.
	WatermillUUIDTag = "_watermill_uuid"
)

// MarshaledMessage is the intermediate representation between Watermill Message
// and KubeMQ message types.
type MarshaledMessage struct {
	Body []byte
	Tags map[string]string
}

// ReceivedMessage is the intermediate representation of a received KubeMQ message.
type ReceivedMessage struct {
	ID   string
	Body []byte
	Tags map[string]string
}

// Marshaler converts a Watermill message to a KubeMQ-compatible format.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*MarshaledMessage, error)
}

// Unmarshaler converts a KubeMQ message to a Watermill message.
type Unmarshaler interface {
	Unmarshal(msg *ReceivedMessage) (*message.Message, error)
}

// MarshalerUnmarshaler combines both Marshaler and Unmarshaler interfaces.
type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// DefaultMarshaler implements MarshalerUnmarshaler using KubeMQ Tags for Watermill
// metadata and Body for Watermill payload.
type DefaultMarshaler struct{}

// Marshal converts a Watermill message to a MarshaledMessage.
func (m DefaultMarshaler) Marshal(topic string, msg *message.Message) (*MarshaledMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("watermill-kubemq: message is nil")
	}

	tags := make(map[string]string, len(msg.Metadata)+1)
	tags[WatermillUUIDTag] = msg.UUID

	for k, v := range msg.Metadata {
		if k == WatermillUUIDTag {
			return nil, fmt.Errorf("watermill-kubemq: metadata key %q is reserved for Watermill UUID", WatermillUUIDTag)
		}
		tags[k] = v
	}

	return &MarshaledMessage{
		Body: msg.Payload,
		Tags: tags,
	}, nil
}

// Unmarshal converts a ReceivedMessage to a Watermill message.
func (m DefaultMarshaler) Unmarshal(msg *ReceivedMessage) (*message.Message, error) {
	if msg == nil {
		return nil, fmt.Errorf("watermill-kubemq: received message is nil")
	}

	uuid := ""
	if msg.Tags != nil {
		uuid = msg.Tags[WatermillUUIDTag]
	}
	if uuid == "" {
		uuid = msg.ID
	}
	if uuid == "" {
		uuid = watermill.NewUUID()
	}

	metadata := make(message.Metadata, len(msg.Tags))
	for k, v := range msg.Tags {
		if k == WatermillUUIDTag {
			continue
		}
		metadata[k] = v
	}

	wmMsg := message.NewMessage(uuid, msg.Body)
	wmMsg.Metadata = metadata
	return wmMsg, nil
}
