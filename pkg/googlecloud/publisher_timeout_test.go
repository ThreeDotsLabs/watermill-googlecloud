package googlecloud_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

// The tests below control Pub/Sub latency with a gRPC interceptor, so they don't depend on how fast the emulator is.

func TestPublisherAppliesPublishTimeoutToEachMessage(t *testing.T) {
	// each message comfortably fits in publishTimeout, but all of them together don't
	const (
		publishTimeout    = 3 * time.Second
		publishDelay      = time.Second
		messagesToPublish = 4
	)

	topic := createTopic(t, "publish_timeout_per_message")

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID:                "tests",
		DoNotCheckTopicExistence: true,
		PublishTimeout:           publishTimeout,
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
				delayRequests[*pubsubpb.PublishRequest](publishDelay, nil),
			)),
		},
	}, nil)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	messages := make([]*message.Message, messagesToPublish)
	for i := range messages {
		messages[i] = message.NewMessage(watermill.NewUUID(), []byte{})
	}

	start := time.Now()
	require.NoError(t, pub.Publish(topic, messages...))
	require.Greater(t, time.Since(start), publishTimeout, "the whole Publish call should take longer than PublishTimeout")

	for _, msg := range messages {
		assert.NotEmpty(t, msg.Metadata.Get(googlecloud.GoogleMessageIDHeaderKey), "message %s was not published", msg.UUID)
	}
}

func TestPublisherFailsMessageExceedingPublishTimeout(t *testing.T) {
	const publishTimeout = time.Second

	// the topic doesn't need to exist, the Publish RPC is held until the test ends
	topic := fmt.Sprintf("publish_timeout_exceeded_%s", uuid.NewString())

	releasePublishRequests := make(chan struct{})

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID:                "tests",
		DoNotCheckTopicExistence: true,
		PublishTimeout:           publishTimeout,
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
				delayRequests[*pubsubpb.PublishRequest](time.Hour, releasePublishRequests),
			)),
		},
	}, nil)
	require.NoError(t, err)
	defer func() {
		// Close waits for in-flight Publish RPCs
		close(releasePublishRequests)
		_ = pub.Close()
	}()

	start := time.Now()
	err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte{}))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, publishTimeout+2*time.Second, "Publish should give up soon after PublishTimeout")
}

func TestPublisherBoundsTopicInitializationByPublishTimeout(t *testing.T) {
	const publishTimeout = time.Second

	topic := fmt.Sprintf("publish_timeout_topic_initialization_%s", uuid.NewString())

	releaseGetTopicRequests := make(chan struct{})

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID:      "tests",
		PublishTimeout: publishTimeout,
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
				delayRequests[*pubsubpb.GetTopicRequest](time.Hour, releaseGetTopicRequests),
			)),
		},
	}, nil)
	require.NoError(t, err)
	defer func() {
		close(releaseGetTopicRequests)
		_ = pub.Close()
	}()

	start := time.Now()
	err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte{}))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "could not check if topic")
	assert.Less(t, elapsed, publishTimeout+2*time.Second, "Publish should give up soon after PublishTimeout")
}

func TestPublisherStopsWaitingWhenMessageContextIsCancelled(t *testing.T) {
	// long enough that only the cancellation can end the wait in time
	const publishTimeout = 10 * time.Second

	// the topic doesn't need to exist, the Publish RPC is held until the test ends
	topic := fmt.Sprintf("publish_message_context_cancelled_%s", uuid.NewString())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	releasePublishRequests := make(chan struct{})

	cancelContextDuringPublish := func(rpcCtx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := req.(*pubsubpb.PublishRequest); ok {
			// the message's context gets cancelled while its Publish RPC is in flight,
			// and the RPC doesn't complete until the test ends
			cancel()
			<-releasePublishRequests
		}
		return invoker(rpcCtx, method, req, reply, cc, opts...)
	}

	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID:                "tests",
		DoNotCheckTopicExistence: true,
		PublishTimeout:           publishTimeout,
		ClientOptions: []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(cancelContextDuringPublish)),
		},
	}, nil)
	require.NoError(t, err)
	defer func() {
		close(releasePublishRequests)
		_ = pub.Close()
	}()

	msg := message.NewMessage(watermill.NewUUID(), []byte{})
	msg.SetContext(ctx)

	start := time.Now()
	err = pub.Publish(topic, msg)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, publishTimeout/2, "Publish should stop waiting as soon as the message's context is cancelled")
}

// delayRequests returns a gRPC interceptor that holds every request of type Req for the given delay,
// or until release is closed, simulating a slow Pub/Sub regardless of how fast the emulator actually is.
func delayRequests[Req any](delay time.Duration, release <-chan struct{}) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := req.(Req); ok {
			select {
			case <-time.After(delay):
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// createTopic creates a uniquely named topic with a generous timeout,
// so that a slow emulator doesn't eat into the PublishTimeout of the test's publisher.
func createTopic(t *testing.T, prefix string) string {
	t.Helper()

	topic := fmt.Sprintf("%s_%s", prefix, uuid.NewString())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := pubsub.NewClient(ctx, "tests")
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: fmt.Sprintf("projects/tests/topics/%s", topic),
	})
	require.NoError(t, err)

	return topic
}
