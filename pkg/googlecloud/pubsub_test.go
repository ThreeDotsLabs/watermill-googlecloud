package googlecloud_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func newPubSub(t *testing.T, enableMessageOrdering bool, marshaler googlecloud.Marshaler, unmarshaler googlecloud.Unmarshaler, subscriptionName googlecloud.SubscriptionNameFn) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID:             "tests",
			EnableMessageOrdering: enableMessageOrdering,
			Marshaler:             marshaler,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID:                "tests",
			GenerateSubscriptionName: subscriptionName,
			SubscriptionConfig: pubsub.SubscriptionConfig{
				RetainAckedMessages:   false,
				EnableMessageOrdering: enableMessageOrdering,
			},
			Unmarshaler: unmarshaler,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	var defaultMarshalerUnmarshaler googlecloud.DefaultMarshalerUnmarshaler
	return newPubSub(t, false, defaultMarshalerUnmarshaler, defaultMarshalerUnmarshaler, googlecloud.TopicSubscriptionName)
}

func createPubSubWithSubscriptionName(t *testing.T, subscriptionName string) (message.Publisher, message.Subscriber) {
	var defaultMarshalerUnmarshaler googlecloud.DefaultMarshalerUnmarshaler
	return newPubSub(t, false, defaultMarshalerUnmarshaler, defaultMarshalerUnmarshaler,
		googlecloud.TopicSubscriptionNameWithSuffix(subscriptionName),
	)
}

func createPubSubWithOrdering(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		true,
		googlecloud.NewOrderingMarshaler(func(topic string, msg *message.Message) (string, error) {
			return "ordering_key", nil
		}),
		googlecloud.NewOrderingUnmarshaler(func(orderingKey string, msg *message.Message) error {
			return nil
		}),
		googlecloud.TopicSubscriptionName,
	)
}

func createPubSubWithSubscriptionNameWithOrdering(t *testing.T, subscriptionName string) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		true,
		googlecloud.NewOrderingMarshaler(func(topic string, msg *message.Message) (string, error) {
			return "ordering_key", nil
		}),
		googlecloud.NewOrderingUnmarshaler(func(orderingKey string, msg *message.Message) error {
			return nil
		}),
		googlecloud.TopicSubscriptionNameWithSuffix(subscriptionName),
	)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithSubscriptionName,
	)
}

func TestPublishSubscribeOrdering(t *testing.T) {
	t.Skip("skipping because the emulator does not currently redeliver nacked messages when ordering is enabled")

	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPubSubWithOrdering,
		createPubSubWithSubscriptionNameWithOrdering,
	)
}

func TestSubscriberAllowedWhenAttachedToAnotherTopic(t *testing.T) {
	testNumber := rand.Int()
	logger := watermill.NewStdLogger(true, true)

	subNameFn := func(topic string) string {
		return fmt.Sprintf("sub_%d", testNumber)
	}

	sub1, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		GenerateSubscriptionName: subNameFn,
	}, logger)
	require.NoError(t, err)

	topic1 := fmt.Sprintf("topic1_%d", testNumber)

	sub2, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		GenerateSubscriptionName:                subNameFn,
		DoNotEnforceSubscriptionAttachedToTopic: true,
	}, logger)
	require.NoError(t, err)
	topic2 := fmt.Sprintf("topic2_%d", testNumber)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = sub1.Subscribe(ctx, topic1)
	require.NoError(t, err)

	// without the DoNotEnforceSubscriptionAttachedToTopic, this call would fail because subNameFn will return the
	// same value for both sub1 and sub2, and sub1 will create a subscription and topic attached to each other
	// making sub2.Subscribe fail because the requested subscription (topic2) is not attached to the GCP topic
	_, err = sub2.Subscribe(ctx, topic2)
	require.NoError(t, err)
}

func TestSubscriberUnexpectedTopicForSubscription(t *testing.T) {
	testNumber := rand.Int()
	logger := watermill.NewStdLogger(true, true)

	subNameFn := func(topic string) string {
		return fmt.Sprintf("sub_%d", testNumber)
	}

	sub1, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID:                "tests",
		GenerateSubscriptionName: subNameFn,
	}, logger)
	require.NoError(t, err)

	topic1 := fmt.Sprintf("topic1_%d", testNumber)

	sub2, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID:                "tests",
		GenerateSubscriptionName: subNameFn,
	}, logger)
	require.NoError(t, err)
	topic2 := fmt.Sprintf("topic2_%d", testNumber)

	howManyMessages := 100

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messagesTopic1, err := sub1.Subscribe(ctx, topic1)
	require.NoError(t, err)

	allMessagesReceived := make(chan struct{})
	go func() {
		defer close(allMessagesReceived)
		messagesReceived := 0
		for range messagesTopic1 {
			messagesReceived++
			if messagesReceived == howManyMessages {
				return
			}
		}
	}()

	produceMessages(t, topic1, howManyMessages)

	select {
	case <-allMessagesReceived:
		t.Log("All topic 1 messages received")
	case <-ctx.Done():
		t.Fatal("Test timed out")
	}

	_, err = sub2.Subscribe(ctx, topic2)
	require.Equal(t, googlecloud.ErrUnexpectedTopic, errors.Cause(err))
}

func TestReceivedMessageContainsMessageId(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
	}, logger)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic_%d", rand.Int())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	howManyMessages := 1
	produceMessages(t, topic, howManyMessages)

	msg := <-messages
	if msg.Metadata.Get(googlecloud.GoogleMessageIDHeaderKey) == "" {
		t.Fatalf("Message %s does not contain %s", msg.UUID, googlecloud.GoogleMessageIDHeaderKey)
	}
}

func TestPublishedMessageIdMatchesReceivedMessageId(t *testing.T) {
	logger := watermill.NewStdLogger(true, true)
	topic := fmt.Sprintf("topic_message_id_match_%d", rand.Int())

	// Set up subscriber
	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
	}, logger)
	require.NoError(t, err)

	// Subscribe to the topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// Set up publisher
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: "tests",
	}, nil)
	require.NoError(t, err)
	defer pub.Close()

	// Publish a message
	publishedMsg := message.NewMessage(watermill.NewUUID(), []byte{})
	require.NoError(t, pub.Publish(topic, publishedMsg))
	publishedMessageId := publishedMsg.Metadata.Get(googlecloud.GoogleMessageIDHeaderKey)

	if publishedMessageId == "" {
		t.Fatalf("Published message %s does not contain %s", publishedMsg.UUID, googlecloud.GoogleMessageIDHeaderKey)
	}

	receivedMsg := <-messages
	receivedMessageId := receivedMsg.Metadata.Get(googlecloud.GoogleMessageIDHeaderKey)
	if publishedMessageId != receivedMessageId {
		t.Fatalf("Published message ID %s does not match received message ID %s", publishedMessageId, receivedMessageId)
	}
}

func TestPublisherDoesNotAttemptToCreateTopic(t *testing.T) {
	topic := fmt.Sprintf("missing_topic_%d", rand.Int())

	// Set up publisher
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: "tests",
		// DoNotCheckTopicExistence is set to true, so the publisher will not check
		// if the topic exists and will also not attempt to create it.
		DoNotCheckTopicExistence:  true,
		DoNotCreateTopicIfMissing: false,
	}, nil)
	require.NoError(t, err)
	defer pub.Close()

	// Publish a message
	publishedMsg := message.NewMessage(watermill.NewUUID(), []byte{})
	require.Error(t, pub.Publish(topic, publishedMsg), googlecloud.ErrTopicDoesNotExist)
}

func produceMessages(t *testing.T, topic string, howMany int) {
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: "tests",
	}, nil)
	require.NoError(t, err)
	defer pub.Close()

	messages := make([]*message.Message, howMany)
	for i := 0; i < howMany; i++ {
		messages[i] = message.NewMessage(watermill.NewUUID(), []byte{})
	}

	require.NoError(t, pub.Publish(topic, messages...))
}

func TestPublishOrdering(t *testing.T) {
	pub, sub := newPubSub(
		t,
		true,
		googlecloud.NewOrderingMarshaler(func(topic string, msg *message.Message) (string, error) {
			return msg.Metadata["ordering"], nil
		}),
		googlecloud.NewOrderingUnmarshaler(func(orderingKey string, msg *message.Message) error {
			return nil
		}),
		googlecloud.TopicSubscriptionNameWithSuffix("TestPublishOrdering"),
	)

	defer func() {
		_ = pub.Close()
		_ = sub.Close()
	}()

	topic := fmt.Sprintf("topic_ordering_%v", uuid.NewString())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	newMsg := func(id string, ordering string) *message.Message {
		msg := message.NewMessage(id, []byte{})
		msg.Metadata["ordering"] = ordering
		return msg
	}

	toPublish := []*message.Message{
		newMsg("1", "A"),
		newMsg("2", "A"),
		newMsg("3", "B"),
		newMsg("4", "B"),
	}

	for i := range toPublish {
		err := pub.Publish(topic, toPublish[i])
		require.NoError(t, err)
	}

	received := map[string][]string{}

	for i := 0; i < len(toPublish); i++ {
		select {
		case msg := <-messages:
			key := msg.Metadata["ordering"]
			received[key] = append(received[key], msg.UUID)
			msg.Ack()
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}

	assert.Equal(t, []string{"1", "2"}, received["A"])
	assert.Equal(t, []string{"3", "4"}, received["B"])
}
