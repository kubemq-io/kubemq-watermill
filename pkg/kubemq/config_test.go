package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublisherConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  PublisherConfig
		wantErr bool
	}{
		{name: "valid", config: PublisherConfig{Address: "localhost:50000", Pattern: PatternEvents}, wantErr: false},
		{name: "missing address no client", config: PublisherConfig{Pattern: PatternEvents}, wantErr: true},
		{name: "invalid pattern", config: PublisherConfig{Address: "localhost:50000", Pattern: PatternType(99)}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPublisherConfigDefaults(t *testing.T) {
	config := PublisherConfig{Address: "localhost:50000", Pattern: PatternEvents}
	err := config.Validate()
	require.NoError(t, err)
	assert.NotNil(t, config.Marshaler)
	assert.NotNil(t, config.Logger)
}

func TestSubscriberConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  SubscriberConfig
		wantErr bool
	}{
		{name: "valid events", config: SubscriberConfig{Address: "localhost:50000", Pattern: PatternEvents}, wantErr: false},
		{name: "valid queues", config: SubscriberConfig{Address: "localhost:50000", Pattern: PatternQueues}, wantErr: false},
		{name: "missing address", config: SubscriberConfig{Pattern: PatternEvents}, wantErr: true},
		{name: "invalid pattern", config: SubscriberConfig{Address: "localhost:50000", Pattern: PatternType(99)}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSubscriberConfigEventsStoreValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  SubscriberConfig
		wantErr bool
	}{
		{
			name: "StartFromSequence valid",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromSequence, EventsStoreSequence: 10,
			},
			wantErr: false,
		},
		{
			name: "StartFromSequence zero",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromSequence, EventsStoreSequence: 0,
			},
			wantErr: true,
		},
		{
			name: "StartFromTime valid",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromTime, EventsStoreStartTime: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "StartFromTime zero",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromTime,
			},
			wantErr: true,
		},
		{
			name: "StartFromTimeDelta valid",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromTimeDelta, EventsStoreTimeDelta: time.Hour,
			},
			wantErr: false,
		},
		{
			name: "StartFromTimeDelta zero",
			config: SubscriberConfig{
				Address: "localhost:50000", Pattern: PatternEventsStore,
				EventsStoreStartOption: StartFromTimeDelta,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCQConfigValidate(t *testing.T) {
	config := CQConfig{Address: "localhost:50000"}
	err := config.Validate()
	assert.NoError(t, err)
	assert.Equal(t, 5*time.Second, config.DefaultTimeout)
	assert.NotNil(t, config.Marshaler)
	assert.NotNil(t, config.Logger)
}

func TestPatternTypeString(t *testing.T) {
	assert.Equal(t, "events", PatternEvents.String())
	assert.Equal(t, "events_store", PatternEventsStore.String())
	assert.Equal(t, "queues", PatternQueues.String())
	assert.Equal(t, "unknown", PatternType(99).String())
}

func TestSubscriberConfigQueueDefaults(t *testing.T) {
	config := SubscriberConfig{Address: "localhost:50000", Pattern: PatternQueues}
	err := config.Validate()
	require.NoError(t, err)
	assert.Equal(t, int32(1), config.MaxItems)
	assert.Equal(t, int32(1), config.WaitTimeoutSeconds)
}
