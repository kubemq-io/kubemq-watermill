package kubemq

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultMarshalerMarshal(t *testing.T) {
	m := DefaultMarshaler{}
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("key", "value")

	result, err := m.Marshal("topic", msg)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), result.Body)
	assert.Equal(t, msg.UUID, result.Tags[WatermillUUIDTag])
	assert.Equal(t, "value", result.Tags["key"])
}

func TestDefaultMarshalerUnmarshal(t *testing.T) {
	m := DefaultMarshaler{}
	received := &ReceivedMessage{
		ID:   "kubemq-id",
		Body: []byte("payload"),
		Tags: map[string]string{
			WatermillUUIDTag: "test-uuid",
			"key":            "value",
		},
	}

	result, err := m.Unmarshal(received)
	require.NoError(t, err)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, message.Payload("payload"), result.Payload)
	assert.Equal(t, "value", result.Metadata.Get("key"))
	assert.Empty(t, result.Metadata.Get(WatermillUUIDTag))
}

func TestDefaultMarshalerReservedKey(t *testing.T) {
	m := DefaultMarshaler{}
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set(WatermillUUIDTag, "conflict")

	_, err := m.Marshal("topic", msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reserved")
}

func TestDefaultMarshalerNilMessage(t *testing.T) {
	m := DefaultMarshaler{}

	_, err := m.Marshal("topic", nil)
	assert.Error(t, err)

	_, err = m.Unmarshal(nil)
	assert.Error(t, err)
}

func TestDefaultMarshalerFallbackUUID(t *testing.T) {
	m := DefaultMarshaler{}

	// Fallback to KubeMQ ID when UUID tag missing
	received := &ReceivedMessage{
		ID:   "kubemq-fallback-id",
		Body: []byte("payload"),
		Tags: map[string]string{"key": "value"},
	}
	result, err := m.Unmarshal(received)
	require.NoError(t, err)
	assert.Equal(t, "kubemq-fallback-id", result.UUID)

	// Generate new UUID when both missing
	received2 := &ReceivedMessage{
		Body: []byte("payload"),
	}
	result2, err := m.Unmarshal(received2)
	require.NoError(t, err)
	assert.NotEmpty(t, result2.UUID)
}

func TestDefaultMarshalerRoundTrip(t *testing.T) {
	m := DefaultMarshaler{}
	original := message.NewMessage(watermill.NewUUID(), []byte("round-trip-payload"))
	original.Metadata.Set("meta-key", "meta-value")

	marshaled, err := m.Marshal("topic", original)
	require.NoError(t, err)

	received := &ReceivedMessage{
		ID:   "ignored-id",
		Body: marshaled.Body,
		Tags: marshaled.Tags,
	}
	restored, err := m.Unmarshal(received)
	require.NoError(t, err)

	assert.Equal(t, original.UUID, restored.UUID)
	assert.Equal(t, original.Payload, restored.Payload)
	assert.Equal(t, "meta-value", restored.Metadata.Get("meta-key"))
}
