package googlecloud

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	// ErrPublisherClosed happens when trying to publish to a topic while the publisher is closed or closing.
	ErrPublisherClosed = errors.New("publisher is closed")
	// ErrTopicDoesNotExist happens when trying to publish or subscribe to a topic that doesn't exist.
	ErrTopicDoesNotExist = errors.New("topic does not exist")
	// ErrConnectTimeout happens when the Google Cloud PubSub connection context times out.
	ErrConnectTimeout = errors.New("connect timeout")
)

type Publisher struct {
	publishers map[string]*pubsub.Publisher
	topicsLock sync.RWMutex
	closed     bool

	client *pubsub.Client
	config PublisherConfig

	logger watermill.LoggerAdapter
}

type PublisherConfig struct {
	// ProjectID is the Google Cloud Engine project ID.
	ProjectID string

	// If true, `Publisher` does not check if the topic exists before publishing.
	DoNotCheckTopicExistence bool

	// If false (default), `Publisher` tries to create a topic if there is none with the requested name.
	// Otherwise, trying to subscribe to non-existent subscription results in `ErrTopicDoesNotExist`.
	DoNotCreateTopicIfMissing bool
	// Enables the topic message ordering
	EnableMessageOrdering bool
	// Enables automatic resume publish upon error
	EnableMessageOrderingAutoResumePublishOnError bool

	// ConnectTimeout defines the timeout for connecting to Pub/Sub
	ConnectTimeout time.Duration
	// PublishTimeout defines the timeout for publishing messages.
	PublishTimeout time.Duration

	// Settings for cloud.google.com/go/pubsub client library.
	PublishSettings *pubsub.PublishSettings
	ClientOptions   []option.ClientOption
	ClientConfig    *pubsub.ClientConfig

	Marshaler Marshaler
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshalerUnmarshaler{}
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = time.Second * 10
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = time.Second * 5
	}
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	pub := &Publisher{
		publishers: map[string]*pubsub.Publisher{},
		config:     config,
		logger:     logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()

	cc, errc, err := connect(ctx, config)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ErrConnectTimeout
	case pub.client = <-cc:
	case err = <-errc:
		return nil, err
	}

	return pub, nil
}

func connect(ctx context.Context, config PublisherConfig) (<-chan *pubsub.Client, <-chan error, error) {
	out := make(chan *pubsub.Client)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		// blocking
		c, err := pubsub.NewClientWithConfig(context.Background(), config.ProjectID, config.ClientConfig, config.ClientOptions...)
		if err != nil {
			errc <- err
			return
		}
		select {
		case out <- c:
			// ok, carry on
		case <-ctx.Done():
			return
		}

	}()

	return out, errc, nil
}

// Publish publishes a set of messages on a Google Cloud Pub/Sub topic.
// It blocks until all the messages are successfully published or an error occurred.
//
// To receive messages published to a topic, you must create a subscription to that topic.
// Only messages published to the topic after the subscription is created are available to subscriber applications.
//
// See https://cloud.google.com/pubsub/docs/publisher to find out more about how Google Cloud Pub/Sub Publishers work.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.closed {
		return ErrPublisherClosed
	}

	deadline := time.Now().Add(p.config.PublishTimeout)

	var ctx context.Context
	if len(messages) > 0 {
		ctx = messages[0].Context()
	} else {
		ctx = context.Background()
	}

	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	pub, err := p.publisher(ctx, topic)
	if err != nil {
		return err
	}

	for _, msg := range messages {
		err = p.publishMessage(pub, msg, topic, deadline)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher) publishMessage(pub *pubsub.Publisher, msg *message.Message, topic string, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(msg.Context(), deadline)
	defer cancel()

	logFields := watermill.LogFields{
		"topic":        topic,
		"message_uuid": msg.UUID,
	}
	p.logger.Trace("Sending message to Google PubSub", logFields)

	googlecloudMsg, err := p.config.Marshaler.Marshal(topic, msg)
	if err != nil {
		return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
	}

	result := pub.Publish(ctx, googlecloudMsg)

	serverMessageID, err := result.Get(ctx)
	if err != nil {
		if p.config.EnableMessageOrdering && p.config.EnableMessageOrderingAutoResumePublishOnError && googlecloudMsg.OrderingKey != "" {
			pub.ResumePublish(googlecloudMsg.OrderingKey)
		}
		return errors.Wrapf(err, "publishing message %s failed", msg.UUID)
	}

	msg.Metadata.Set(GoogleMessageIDHeaderKey, serverMessageID)

	p.logger.Trace("Message published to Google PubSub", logFields)

	return nil
}

// Close notifies the Publisher to stop processing messages, send all the remaining messages and close the connection.
func (p *Publisher) Close() error {
	p.logger.Info("Closing Google PubSub publisher", nil)
	defer p.logger.Info("Google PubSub publisher closed", nil)

	if p.closed {
		return nil
	}
	p.closed = true

	p.topicsLock.Lock()
	for _, t := range p.publishers {
		t.Stop()
	}
	p.topicsLock.Unlock()

	return p.client.Close()
}

func (p *Publisher) publisher(ctx context.Context, topic string) (pub *pubsub.Publisher, err error) {
	p.topicsLock.RLock()
	pub, ok := p.publishers[topic]
	p.topicsLock.RUnlock()
	if ok {
		return pub, nil
	}

	p.topicsLock.Lock()
	defer func() {
		if err == nil {
			pub.EnableMessageOrdering = p.config.EnableMessageOrdering
			p.publishers[topic] = pub
		}
		p.topicsLock.Unlock()
	}()

	pub = p.client.Publisher(topic)

	// todo: theoretically, one could want different publish settings per topic, which is supported by the client lib
	// different instances of publisher may be used then
	if p.config.PublishSettings != nil {
		pub.PublishSettings = *p.config.PublishSettings
	}

	if p.config.DoNotCheckTopicExistence {
		return pub, nil
	}

	exists, err := topicExists(ctx, p.client, p.config.ProjectID, topic)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check if topic %s exists", topic)
	}

	if exists {
		return pub, nil
	}

	if p.config.DoNotCreateTopicIfMissing {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topic)
	}

	_, err = p.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: fullyQualifiedTopicName(p.config.ProjectID, topic),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not create topic %s", topic)
	}

	return pub, nil
}

func topicExists(ctx context.Context, client *pubsub.Client, projectID string, topic string) (bool, error) {
	_, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: fullyQualifiedTopicName(projectID, topic),
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return false, nil
		}
		return false, errors.Wrapf(err, "could not check if topic %s exists", topic)
	}
	return true, nil
}

func fullyQualifiedTopicName(projectID string, topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", projectID, topic)
}
