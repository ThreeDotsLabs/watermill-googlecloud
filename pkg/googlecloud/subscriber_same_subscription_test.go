package googlecloud_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestSubscribeTwiceToTheSameSubscription(t *testing.T) {
	topic := "topic_same_subscription_" + uuid.NewString()

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
	}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: "tests",
		// the emulator may be slow when it is under load
		PublishTimeout: 30 * time.Second,
	}, nil)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	messages1, err := sub.Subscribe(ctx1, topic)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	messages2, err := sub.Subscribe(ctx2, topic)
	require.NoError(t, err)

	published := make([]string, 10)
	for i := range published {
		published[i] = watermill.NewUUID()
		require.NoError(t, pub.Publish(topic, message.NewMessage(published[i], []byte{})))
	}

	received := map[string]struct{}{}
	receivedPerChannel := [2]int{}
	timeout := time.After(10 * time.Second)
	for len(received) < len(published) {
		var msg *message.Message
		var ok bool

		select {
		case msg, ok = <-messages1:
			receivedPerChannel[0]++
		case msg, ok = <-messages2:
			receivedPerChannel[1]++
		case <-timeout:
			t.Fatalf("received %d of %d messages", len(received), len(published))
		}
		require.True(t, ok, "channel closed unexpectedly")

		received[msg.UUID] = struct{}{}
		msg.Ack()
	}
	t.Logf("messages received per channel: %v", receivedPerChannel)

	for _, id := range published {
		assert.Contains(t, received, id)
	}

	cancel2()
	timeout = time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-messages2:
			if !ok {
				return
			}
			msg.Nack()
		case <-timeout:
			t.Fatal("second channel not closed within 5s after its context was canceled")
		}
	}
}

func TestSubscriberChecksSubscriptionExistenceOnce(t *testing.T) {
	var getSubscriptionCalls atomic.Int64

	countGetSubscriptionCalls := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := req.(*pubsubpb.GetSubscriptionRequest); ok {
			getSubscriptionCalls.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(countGetSubscriptionCalls)),
		},
	}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	topic := "topic_subscription_checked_once_" + uuid.NewString()

	require.NoError(t, sub.SubscribeInitialize(topic))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	_, err = sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	assert.EqualValues(t, 1, getSubscriptionCalls.Load())
}

func TestSubscriberUnexpectedTopicForInitializedSubscription(t *testing.T) {
	testID := uuid.NewString()

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
		GenerateSubscriptionName: func(topic string) string {
			return "sub_" + testID
		},
	}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = sub.Subscribe(ctx, "topic1_"+testID)
	require.NoError(t, err)

	_, err = sub.Subscribe(ctx, "topic2_"+testID)
	assert.Equal(t, googlecloud.ErrUnexpectedTopic, errors.Cause(err))
}

func TestSubscriberAllowsOtherTopicForInitializedSubscriptionWhenNotEnforced(t *testing.T) {
	testID := uuid.NewString()

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
		GenerateSubscriptionName: func(topic string) string {
			return "sub_" + testID
		},
		DoNotEnforceSubscriptionAttachedToTopic: true,
	}, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = sub.Subscribe(ctx, "topic1_"+testID)
	require.NoError(t, err)

	_, err = sub.Subscribe(ctx, "topic2_"+testID)
	assert.NoError(t, err)
}
