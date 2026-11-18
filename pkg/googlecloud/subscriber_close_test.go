package googlecloud_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

const subscriberCloseTestTimeout = 10 * time.Second

func TestSubscribeInitializeAfterClose(t *testing.T) {
	var rpcCalls atomic.Int64

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID:     "tests",
		ClientOptions: []option.ClientOption{countRPCCalls(&rpcCalls)},
	}, nil)
	require.NoError(t, err)

	require.NoError(t, sub.Close())

	err = sub.SubscribeInitialize(newSubscriberCloseTestTopic())
	assert.ErrorIs(t, err, googlecloud.ErrSubscriberClosed)
	assert.Zero(t, rpcCalls.Load(), "closed subscriber should not call Pub/Sub")
}

func TestSubscribeConcurrentWithClose(t *testing.T) {
	t.Parallel()

	t.Run("client_not_initialized", func(t *testing.T) {
		topic := newSubscriberCloseTestTopic()
		blocked := newBlockingCall()
		var rpcCalls atomic.Int64

		sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
			ProjectID: "tests",
			GenerateSubscriptionName: func(topic string) string {
				// runs after Subscribe checked that the subscriber is not closed
				blocked.block()
				return topic
			},
			ClientOptions: []option.ClientOption{countRPCCalls(&rpcCalls)},
		}, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), subscriberCloseTestTimeout)
		defer cancel()

		subscribed := subscribeAsync(ctx, sub, topic)
		blocked.waitUntilEntered(t)

		require.NoError(t, sub.Close())
		blocked.release()

		result := waitForSubscribeResult(t, subscribed)
		assert.ErrorIs(t, result.err, googlecloud.ErrSubscriberClosed)
		assert.Nil(t, result.messages)
		assert.Zero(t, rpcCalls.Load(), "subscriber closed before Subscribe initialized the client should not call Pub/Sub")
	})

	t.Run("client_initialized", func(t *testing.T) {
		t.Parallel()

		firstTopic := newSubscriberCloseTestTopic()
		blockedTopic := newSubscriberCloseTestTopic()
		blocked := newBlockingCall()

		sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
			ProjectID: "tests",
			GenerateSubscriptionName: func(topic string) string {
				if topic == blockedTopic {
					blocked.block()
				}
				return topic
			},
		}, nil)
		require.NoError(t, err)

		// initializes the client
		require.NoError(t, sub.SubscribeInitialize(firstTopic))

		ctx, cancel := context.WithTimeout(context.Background(), subscriberCloseTestTimeout)
		defer cancel()

		subscribed := subscribeAsync(ctx, sub, blockedTopic)
		blocked.waitUntilEntered(t)

		require.NoError(t, sub.Close())
		blocked.release()

		result := waitForSubscribeResult(t, subscribed)
		assert.ErrorIs(t, result.err, googlecloud.ErrSubscriberClosed)
		assert.Nil(t, result.messages)
	})

	t.Run("client_closed_during_subscription_initialization", func(t *testing.T) {
		t.Parallel()

		topic := newSubscriberCloseTestTopic()
		blocked := newBlockingCall()

		blockGetSubscription := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if r, ok := req.(*pubsubpb.GetSubscriptionRequest); ok && strings.HasSuffix(r.Subscription, "/"+topic) {
				blocked.block()
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
			ProjectID: "tests",
			ClientOptions: []option.ClientOption{
				option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(blockGetSubscription)),
			},
		}, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), subscriberCloseTestTimeout)
		defer cancel()

		subscribed := subscribeAsync(ctx, sub, topic)
		blocked.waitUntilEntered(t)

		// closes the client while Subscribe is calling Pub/Sub
		require.NoError(t, sub.Close())
		blocked.release()

		result := waitForSubscribeResult(t, subscribed)
		assert.ErrorIs(t, result.err, googlecloud.ErrSubscriberClosed)
		assert.Nil(t, result.messages)
	})

	t.Run("closed_after_subscription_initialized", func(t *testing.T) {
		t.Parallel()

		topic := newSubscriberCloseTestTopic()
		blocked := newBlockingCall()

		blockAfterCreateSubscription := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if r, ok := req.(*pubsubpb.Subscription); ok && strings.HasSuffix(r.Name, "/"+topic) {
				blocked.block()
			}
			return err
		}

		sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
			ProjectID: "tests",
			ClientOptions: []option.ClientOption{
				option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(blockAfterCreateSubscription)),
			},
		}, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), subscriberCloseTestTimeout)
		defer cancel()

		subscribed := subscribeAsync(ctx, sub, topic)
		blocked.waitUntilEntered(t)

		// Close doesn't wait for Subscribe that didn't start receiving yet
		require.NoError(t, sub.Close())
		blocked.release()

		result := waitForSubscribeResult(t, subscribed)
		assert.ErrorIs(t, result.err, googlecloud.ErrSubscriberClosed)
		assert.Nil(t, result.messages)
	})
}

func TestSubscribeWithShutdownTimeoutWaitsForInFlightCallbacks(t *testing.T) {
	t.Parallel()

	topic := newSubscriberCloseTestTopic()
	unmarshaler := newBlockingUnmarshaler()

	sub, err := googlecloud.NewSubscriber(googlecloud.SubscriberConfig{
		ProjectID: "tests",
		ReceiveSettings: pubsub.ReceiveSettings{
			// with zero timeout, Receive returns without waiting for running callbacks
			ShutdownOptions: &pubsub.ShutdownOptions{Timeout: 0},
		},
		Unmarshaler: unmarshaler,
	}, nil)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()
	defer unmarshaler.release()

	ctx, cancel := context.WithTimeout(context.Background(), subscriberCloseTestTimeout)
	defer cancel()

	messages, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	const messagesCount = 20
	publishConcurrently(t, topic, messagesCount)

	require.Eventually(
		t,
		func() bool { return unmarshaler.entered.Load() >= messagesCount },
		subscriberCloseTestTimeout,
		10*time.Millisecond,
		"all messages should be in flight",
	)

	cancel()

	outputClosed := make(chan struct{})
	go func() {
		defer close(outputClosed)
		for msg := range messages {
			msg.Nack()
		}
	}()

	select {
	case <-outputClosed:
		// released callbacks would send to the closed output
		t.Error("output closed while callbacks were in flight")
	case <-time.After(500 * time.Millisecond):
	}

	unmarshaler.release()

	select {
	case <-outputClosed:
	case <-time.After(subscriberCloseTestTimeout):
		t.Fatal("output not closed after in-flight callbacks finished")
	}
}

// blockingUnmarshaler blocks unmarshaling until released, keeping Receive callbacks in flight.
type blockingUnmarshaler struct {
	entered     atomic.Int64
	released    chan struct{}
	releaseOnce sync.Once
}

func newBlockingUnmarshaler() *blockingUnmarshaler {
	return &blockingUnmarshaler{released: make(chan struct{})}
}

func (u *blockingUnmarshaler) Unmarshal(msg *pubsub.Message) (*message.Message, error) {
	u.entered.Add(1)
	<-u.released
	return googlecloud.DefaultMarshalerUnmarshaler{}.Unmarshal(msg)
}

func (u *blockingUnmarshaler) release() {
	u.releaseOnce.Do(func() { close(u.released) })
}

// publishConcurrently publishes messages concurrently, so they are batched by the client.
func publishConcurrently(t *testing.T, topic string, howMany int) {
	t.Helper()

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID:      "tests",
		PublishTimeout: subscriberCloseTestTimeout,
	}, nil)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < howMany; i++ {
		wg.Go(func() {
			assert.NoError(t, pub.Publish(topic, message.NewMessage(uuid.NewString(), []byte{})))
		})
	}
	wg.Wait()
}

func newSubscriberCloseTestTopic() string {
	return "subscriber_close_" + uuid.NewString()
}

func countRPCCalls(calls *atomic.Int64) option.ClientOption {
	return option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			calls.Add(1)
			return invoker(ctx, method, req, reply, cc, opts...)
		},
	))
}

// blockingCall blocks the first call to block until release is called.
type blockingCall struct {
	once     sync.Once
	entered  chan struct{}
	released chan struct{}
}

func newBlockingCall() *blockingCall {
	return &blockingCall{
		entered:  make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (b *blockingCall) block() {
	b.once.Do(func() {
		close(b.entered)
		<-b.released
	})
}

func (b *blockingCall) waitUntilEntered(t *testing.T) {
	t.Helper()

	select {
	case <-b.entered:
	case <-time.After(subscriberCloseTestTimeout):
		require.FailNow(t, "blocking call was not entered")
	}
}

func (b *blockingCall) release() {
	close(b.released)
}

type subscribeResult struct {
	messages <-chan *message.Message
	err      error
}

func subscribeAsync(ctx context.Context, sub *googlecloud.Subscriber, topic string) <-chan subscribeResult {
	result := make(chan subscribeResult, 1)
	go func() {
		messages, err := sub.Subscribe(ctx, topic)
		result <- subscribeResult{messages: messages, err: err}
	}()
	return result
}

func waitForSubscribeResult(t *testing.T, subscribed <-chan subscribeResult) subscribeResult {
	t.Helper()

	select {
	case result := <-subscribed:
		return result
	case <-time.After(subscriberCloseTestTimeout):
		require.FailNow(t, "Subscribe did not return")
		return subscribeResult{}
	}
}
