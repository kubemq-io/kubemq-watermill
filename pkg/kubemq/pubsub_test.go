//go:build compatibility

package kubemq

import (
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
)

func generateTopicName(t *testing.T) string {
	return fmt.Sprintf("test_%s_%d", t.Name(), time.Now().UnixNano())
}

func TestPubSubEvents(t *testing.T) {
	tests.TestPubSub(t, tests.TestContext{
		TestID: tests.NewTestID(),
		GenerateTopicName: func(testID tests.TestID) string {
			return generateTopicName(t)
		},
		Features: tests.Features{
			ConsumerGroups:                   true,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       false,
			NewSubscriberReceivesOldMessages: false,
		},
		PubSubConstructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
			pub, err := NewPublisher(PublisherConfig{
				Address: "localhost:50000",
				Pattern: PatternEvents,
			})
			require.NoError(t, err)
			sub, err := NewSubscriber(SubscriberConfig{
				Address: "localhost:50000",
				Pattern: PatternEvents,
			})
			require.NoError(t, err)
			return pub, sub
		},
	})
}

func TestPubSubEventsStore(t *testing.T) {
	tests.TestPubSub(t, tests.TestContext{
		TestID: tests.NewTestID(),
		GenerateTopicName: func(testID tests.TestID) string {
			return generateTopicName(t)
		},
		Features: tests.Features{
			ConsumerGroups:                   true,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  true,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		PubSubConstructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
			pub, err := NewPublisher(PublisherConfig{
				Address: "localhost:50000",
				Pattern: PatternEventsStore,
			})
			require.NoError(t, err)
			sub, err := NewSubscriber(SubscriberConfig{
				Address:                "localhost:50000",
				Pattern:                PatternEventsStore,
				EventsStoreStartOption: StartFromFirst,
			})
			require.NoError(t, err)
			return pub, sub
		},
	})
}

func TestPubSubQueues(t *testing.T) {
	tests.TestPubSub(t, tests.TestContext{
		TestID: tests.NewTestID(),
		GenerateTopicName: func(testID tests.TestID) string {
			return generateTopicName(t)
		},
		Features: tests.Features{
			ConsumerGroups:                   true,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  true,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		PubSubConstructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
			pub, err := NewPublisher(PublisherConfig{
				Address: "localhost:50000",
				Pattern: PatternQueues,
			})
			require.NoError(t, err)
			sub, err := NewSubscriber(SubscriberConfig{
				Address:            "localhost:50000",
				Pattern:            PatternQueues,
				WaitTimeoutSeconds: 1,
				MaxItems:           1,
			})
			require.NoError(t, err)
			return pub, sub
		},
	})
}
