package googlecloud

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/cenkalti/backoff/v3"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	// ErrSubscriberClosed happens when trying to subscribe to a new topic while the subscriber is closed or closing.
	ErrSubscriberClosed = errors.New("subscriber is closed")
	// ErrSubscriptionDoesNotExist happens when trying to use a subscription that does not exist.
	ErrSubscriptionDoesNotExist = errors.New("subscription does not exist")
	// ErrUnexpectedTopic happens when the subscription resolved from SubscriptionNameFn is for a different topic than expected.
	ErrUnexpectedTopic = errors.New("requested subscription already exists, but for other topic than expected")
)

// Subscriber attaches to a Google Cloud Pub/Sub subscription and returns a Go channel with messages from the topic.
// Be aware that in Google Cloud Pub/Sub, only messages sent after the subscription was created can be consumed.
//
// For more info on how Google Cloud Pub/Sub Subscribers work, check https://cloud.google.com/pubsub/docs/subscriber.
type Subscriber struct {
	closing chan struct{}
	closed  atomic.Bool

	allSubscriptionsWaitGroup sync.WaitGroup
	activeSubscribers         map[string]*pubsub.Subscriber
	activeSubscribersLock     sync.RWMutex

	client     *pubsub.Client
	clientLock sync.RWMutex

	config SubscriberConfig

	logger watermill.LoggerAdapter
}

type SubscriberConfig struct {
	// GenerateSubscriptionName generates subscription name for a given topic.
	// The subscription connects the topic to a subscriber application that receives and processes
	// messages published to the topic.
	//
	// By default, subscriptions expire after 31 days of inactivity.
	//
	// A topic can have multiple subscriptions, but a given subscription belongs to a single topic.
	GenerateSubscriptionName SubscriptionNameFn

	// ProjectID is the Google Cloud Engine project ID.
	ProjectID string

	// TopicProjectID is an optionnal configuration value representing
	// the underlying topic Google Cloud Engine project ID.
	// This can be helpful when subscription is linked to a topic for another project.
	TopicProjectID string

	// If false (default), `Subscriber` tries to create a subscription if there is none with the requested name.
	// Otherwise, trying to use non-existent subscription results in `ErrSubscriptionDoesNotExist`.
	DoNotCreateSubscriptionIfMissing bool

	// If false (default), `Subscriber` tries to create a topic if there is none with the requested name
	// and it is trying to create a new subscription with this topic name.
	// Otherwise, trying to create a subscription on non-existent topic results in `ErrTopicDoesNotExist`.
	DoNotCreateTopicIfMissing bool

	// If false (default), when the subscription already exists, `Subscriber` will make sure that the subscription is
	// attached to the provided topic, and will return a ErrUnexpectedTopic if not.
	// Otherwise, it won't check to which topic the subscription is attached to.
	DoNotEnforceSubscriptionAttachedToTopic bool

	// deprecated: ConnectTimeout is no longer used, please use timeout on context in Subscribe() method
	ConnectTimeout time.Duration

	// InitializeTimeout defines the timeout for initializing topics.
	InitializeTimeout time.Duration

	// Settings for cloud.google.com/go/pubsub client library.
	ReceiveSettings pubsub.ReceiveSettings

	// Deprecated: Use GenerateSubscription instead.
	// This config comes from the v1 client library. It will be mapped to the v2 config, but some new fields may not be supported.
	SubscriptionConfig   pubsubv1.SubscriptionConfig
	GenerateSubscription func(params GenerateSubscriptionParams) *pubsubpb.Subscription

	ClientOptions []option.ClientOption
	ClientConfig  *pubsub.ClientConfig

	// Unmarshaler transforms the client library format into watermill/message.Message.
	// Use a custom unmarshaler if needed, otherwise the default Unmarshaler should cover most use cases.
	Unmarshaler Unmarshaler
}

type GenerateSubscriptionParams struct{}

func (c *SubscriberConfig) topicProjectID() string {
	if c.TopicProjectID != "" {
		return c.TopicProjectID
	}

	return c.ProjectID
}

type SubscriptionNameFn func(topic string) string

// TopicSubscriptionName uses the topic name as the subscription name.
func TopicSubscriptionName(topic string) string {
	return topic
}

// TopicSubscriptionNameWithSuffix uses the topic name with a chosen suffix as the subscription name.
func TopicSubscriptionNameWithSuffix(suffix string) SubscriptionNameFn {
	return func(topic string) string {
		return topic + suffix
	}
}

func (c *SubscriberConfig) setDefaults() {
	if c.GenerateSubscriptionName == nil {
		c.GenerateSubscriptionName = TopicSubscriptionName
	}
	if c.InitializeTimeout == 0 {
		c.InitializeTimeout = time.Second * 10
	}
	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultMarshalerUnmarshaler{}
	}
	if c.GenerateSubscription == nil {
		c.GenerateSubscription = func(params GenerateSubscriptionParams) *pubsubpb.Subscription {
			return subscriptionFromSubscriptionConfig(c.SubscriptionConfig)
		}
	}
}

func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	config.setDefaults()

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		closing: make(chan struct{}, 1),

		allSubscriptionsWaitGroup: sync.WaitGroup{},
		activeSubscribers:         map[string]*pubsub.Subscriber{},
		activeSubscribersLock:     sync.RWMutex{},

		config: config,

		logger: logger,
	}, nil
}

// Subscribe consumes Google Cloud Pub/Sub and outputs them as Waterfall Message objects on the returned channel.
//
// In Google Cloud Pub/Sub, it is impossible to subscribe directly to a topic. Instead, a *subscription* is used.
// Each subscription has one topic, but there may be multiple subscriptions to one topic (with different names).
//
// The `topic` argument is transformed into subscription name with the configured `GenerateSubscriptionName` function.
// By default, if the subscription or topic don't exist, the are created. This behavior may be changed in the config.
//
// Be aware that in Google Cloud Pub/Sub, only messages sent after the subscription was created can be consumed.
//
// See https://cloud.google.com/pubsub/docs/subscriber to find out more about how Google Cloud Pub/Sub Subscriptions work.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed.Load() {
		return nil, ErrSubscriberClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	subscriptionName := s.config.GenerateSubscriptionName(topic)

	logFields := watermill.LogFields{
		"provider":          ProviderName,
		"topic":             topic,
		"subscription_name": subscriptionName,
	}
	s.logger.Info("Subscribing to Google Cloud PubSub topic", logFields)

	output := make(chan *message.Message)

	sub, err := s.subscription(ctx, subscriptionName, topic)
	if err != nil {
		cancel()
		return nil, err
	}

	receiveFinished := make(chan struct{})
	s.allSubscriptionsWaitGroup.Add(1)
	go func() {
		exponentialBackoff := backoff.NewExponentialBackOff()
		exponentialBackoff.MaxElapsedTime = 0 // 0 means it never expires

		if err := backoff.Retry(func() error {
			err := s.receive(ctx, sub, logFields, output)
			if err == nil {
				s.logger.Info("Receiving messages finished with no error", logFields)
				return nil
			}

			if s.closed.Load() {
				s.logger.Info("Receiving messages failed while closed", logFields)
				return backoff.Permanent(err)
			}

			s.logger.Error("Receiving messages failed, retrying", err, logFields)
			return err
		}, exponentialBackoff); err != nil {
			s.logger.Error("Retrying receiving messages failed", err, logFields)
		}

		close(receiveFinished)
	}()

	go func() {
		<-s.closing
		s.logger.Debug("Closing message consumer", logFields)
		cancel()
	}()

	go func() {
		<-receiveFinished
		close(output)
		s.allSubscriptionsWaitGroup.Done()
	}()

	return output, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.InitializeTimeout)
	defer cancel()

	subscriptionName := s.config.GenerateSubscriptionName(topic)
	logFields := watermill.LogFields{
		"provider":          ProviderName,
		"topic":             topic,
		"subscription_name": subscriptionName,
	}
	s.logger.Info("Initializing subscription to Google Cloud PubSub topic", logFields)

	if _, err := s.subscription(ctx, subscriptionName, topic); err != nil {
		return err
	}

	return nil
}

// Close notifies the Subscriber to stop processing messages on all subscriptions, close all the output channels
// and terminate the connection.
func (s *Subscriber) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(s.closing)

	s.allSubscriptionsWaitGroup.Wait()

	s.clientLock.Lock()
	defer s.clientLock.Unlock()

	if s.client != nil {
		err := s.client.Close()
		if err != nil {
			return fmt.Errorf("closing client: %w", err)
		}
	}

	s.logger.Debug("Google Cloud PubSub subscriber closed", nil)

	return nil
}

func (s *Subscriber) receive(
	ctx context.Context,
	sub *pubsub.Subscriber,
	subcribeLogFields watermill.LogFields,
	output chan *message.Message,
) error {
	return sub.Receive(ctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
		logFields := subcribeLogFields.Copy()

		msg, err := s.config.Unmarshaler.Unmarshal(pubsubMsg)
		if err != nil {
			s.logger.Error("Could not unmarshal Google Cloud PubSub message", err, logFields)
			pubsubMsg.Nack()
			return
		}
		logFields["message_uuid"] = msg.UUID

		ctx, cancelCtx := context.WithCancel(ctx)
		msg.SetContext(ctx)
		defer cancelCtx()

		select {
		case <-s.closing:
			s.logger.Info(
				"Message not consumed, subscriber is closing",
				logFields,
			)
			pubsubMsg.Nack()
			return
		case <-ctx.Done():
			s.logger.Info(
				"Message not consumed, ctx canceled",
				logFields,
			)
			pubsubMsg.Nack()
			return
		case output <- msg:
			// message consumed, wait for ack (or nack)
		}

		select {
		case <-s.closing:
			pubsubMsg.Nack()
			s.logger.Trace(
				"Closing, nacking message",
				logFields,
			)
		case <-ctx.Done():
			pubsubMsg.Nack()
			s.logger.Trace(
				"Ctx done, nacking message",
				logFields,
			)
		case <-msg.Acked():
			s.logger.Trace(
				"Msg acked",
				logFields,
			)
			pubsubMsg.Ack()
		case <-msg.Nacked():
			pubsubMsg.Nack()
			s.logger.Trace(
				"Msg nacked",
				logFields,
			)
		}
	})
}

// subscription obtains a subscription object.
// If subscription doesn't exist on PubSub, create it, unless config variable DoNotCreateSubscriptionWhenMissing is set.
func (s *Subscriber) subscription(ctx context.Context, subscriptionName, topicName string) (sub *pubsub.Subscriber, err error) {
	s.activeSubscribersLock.RLock()
	sub, ok := s.activeSubscribers[subscriptionName]
	s.activeSubscribersLock.RUnlock()
	if ok {
		return sub, nil
	}

	s.activeSubscribersLock.Lock()
	defer s.activeSubscribersLock.Unlock()
	defer func() {
		if err == nil {
			s.activeSubscribers[subscriptionName] = sub
		}
	}()

	err = s.initClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize client")
	}

	sub = s.client.Subscriber(subscriptionName)

	subResp, err := s.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: fullyQualifiedSubscriptionName(s.config.ProjectID, subscriptionName),
	})
	if err == nil {
		return s.existingSubscriber(sub, subResp, topicName)
	}
	if err != nil {
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.NotFound {
			return nil, errors.Wrapf(err, "could not get subscription %s", subscriptionName)
		}
	}

	if s.config.DoNotCreateSubscriptionIfMissing {
		return nil, errors.Wrap(ErrSubscriptionDoesNotExist, subscriptionName)
	}

	tExists, err := topicExists(ctx, s.client, s.config.ProjectID, topicName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not check if topic %s exists", topicName)
	}

	if !tExists && s.config.DoNotCreateTopicIfMissing {
		return nil, errors.Wrap(ErrTopicDoesNotExist, topicName)
	}

	if !tExists {
		_, err = s.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
			Name: fullyQualifiedTopicName(s.config.topicProjectID(), topicName),
		})

		if status.Code(err) == codes.AlreadyExists {
			s.logger.Debug("Topic already exists", watermill.LogFields{"topic": topicName})
		} else if err != nil {
			return nil, errors.Wrap(err, "could not create topic for subscription")
		}
	}

	subConfig := s.config.GenerateSubscription(GenerateSubscriptionParams{})
	subConfig.Name = fullyQualifiedSubscriptionName(s.config.ProjectID, subscriptionName)
	subConfig.Topic = fullyQualifiedTopicName(s.config.topicProjectID(), topicName)

	_, err = s.client.SubscriptionAdminClient.CreateSubscription(ctx, subConfig)
	if status.Code(err) == codes.AlreadyExists {
		s.logger.Debug("Subscription already exists", watermill.LogFields{"subscription": subscriptionName})
	} else if err != nil {
		return nil, errors.Wrap(err, "cannot create subscription")
	}

	sub.ReceiveSettings = s.config.ReceiveSettings

	return sub, nil
}

func (s *Subscriber) initClient(ctx context.Context) error {
	s.clientLock.Lock()
	defer s.clientLock.Unlock()

	if s.client != nil {
		return nil
	}

	c, err := pubsub.NewClientWithConfig(ctx, s.config.ProjectID, s.config.ClientConfig, s.config.ClientOptions...)
	if err != nil {
		return fmt.Errorf("could not create client: %w", err)
	}

	s.client = c

	return nil
}

func (s *Subscriber) existingSubscriber(sub *pubsub.Subscriber, subscription *pubsubpb.Subscription, topic string) (*pubsub.Subscriber, error) {
	sub.ReceiveSettings = s.config.ReceiveSettings

	if s.config.DoNotEnforceSubscriptionAttachedToTopic {
		return sub, nil
	}

	fullName := fullyQualifiedTopicName(s.config.topicProjectID(), topic)

	if subscription.Topic != fullName {
		return nil, errors.Wrap(
			ErrUnexpectedTopic,
			fmt.Sprintf("topic of existing sub: %s; expecting: %s", subscription.Topic, fullName),
		)
	}

	return sub, nil
}

func subscriptionFromSubscriptionConfig(cfg pubsubv1.SubscriptionConfig) *pubsubpb.Subscription {
	pushConfig := &pubsubpb.PushConfig{
		PushEndpoint:         cfg.PushConfig.Endpoint,
		Attributes:           cfg.PushConfig.Attributes,
		AuthenticationMethod: nil, // TODO
		Wrapper:              nil, // TODO
	}

	bigQueryConfig := &pubsubpb.BigQueryConfig{
		Table:               "",
		UseTopicSchema:      false,
		WriteMetadata:       false,
		DropUnknownFields:   false,
		State:               0,
		UseTableSchema:      false,
		ServiceAccountEmail: "",
	}

	cloudStorageConfig := &pubsubpb.CloudStorageConfig{
		Bucket:                 "",
		FilenamePrefix:         "",
		FilenameSuffix:         "",
		FilenameDatetimeFormat: "",
		OutputFormat:           nil,
		MaxDuration:            nil,
		MaxBytes:               0,
		MaxMessages:            0,
		State:                  0,
		ServiceAccountEmail:    "",
	}

	var messageRetentionDuration *durationpb.Duration
	if cfg.RetentionDuration != 0 {
		messageRetentionDuration = durationpb.New(cfg.RetentionDuration)
	}

	var topicMessageRetentionDuration *durationpb.Duration
	if cfg.TopicMessageRetentionDuration != 0 {
		topicMessageRetentionDuration = durationpb.New(cfg.TopicMessageRetentionDuration)
	}

	var expirationPolicy *pubsubpb.ExpirationPolicy
	if cfg.ExpirationPolicy != nil {
		expirationPolicy = &pubsubpb.ExpirationPolicy{
			Ttl: durationpb.New(cfg.ExpirationPolicy.(time.Duration)),
		}
	}

	var deadLetterPolicy *pubsubpb.DeadLetterPolicy
	if cfg.DeadLetterPolicy != nil {
		deadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			MaxDeliveryAttempts: int32(cfg.DeadLetterPolicy.MaxDeliveryAttempts),
			DeadLetterTopic:     cfg.DeadLetterPolicy.DeadLetterTopic,
		}
	}

	var retryPolicy *pubsubpb.RetryPolicy
	if cfg.RetryPolicy != nil {
		retryPolicy = &pubsubpb.RetryPolicy{}

		if cfg.RetryPolicy.MinimumBackoff != nil {
			retryPolicy.MinimumBackoff = durationpb.New(cfg.RetryPolicy.MinimumBackoff.(time.Duration))
		}

		if cfg.RetryPolicy.MaximumBackoff != nil {
			retryPolicy.MaximumBackoff = durationpb.New(cfg.RetryPolicy.MaximumBackoff.(time.Duration))
		}
	}

	var messageTransforms []*pubsubpb.MessageTransform
	for _, transform := range cfg.MessageTransforms {
		udf := transform.Transform.(pubsubv1.JavaScriptUDF)
		messageTransforms = append(messageTransforms, &pubsubpb.MessageTransform{
			Transform: &pubsubpb.MessageTransform_JavascriptUdf{
				JavascriptUdf: &pubsubpb.JavaScriptUDF{
					FunctionName: udf.FunctionName,
					Code:         udf.Code,
				},
			},
			Disabled: transform.Disabled,
		})
	}

	return &pubsubpb.Subscription{
		PushConfig:                    pushConfig,
		BigqueryConfig:                bigQueryConfig,
		CloudStorageConfig:            cloudStorageConfig,
		AckDeadlineSeconds:            int32(cfg.AckDeadline.Seconds()),
		RetainAckedMessages:           cfg.RetainAckedMessages,
		MessageRetentionDuration:      messageRetentionDuration,
		Labels:                        cfg.Labels,
		EnableMessageOrdering:         cfg.EnableMessageOrdering,
		ExpirationPolicy:              expirationPolicy,
		Filter:                        cfg.Filter,
		DeadLetterPolicy:              deadLetterPolicy,
		RetryPolicy:                   retryPolicy,
		Detached:                      cfg.Detached,
		EnableExactlyOnceDelivery:     cfg.EnableExactlyOnceDelivery,
		TopicMessageRetentionDuration: topicMessageRetentionDuration,
		State:                         pubsubpb.Subscription_State(cfg.State),
		AnalyticsHubSubscriptionInfo:  nil, // Unmapped
		MessageTransforms:             messageTransforms,
	}
}

func fullyQualifiedSubscriptionName(projectID, subscriptionName string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subscriptionName)
}
