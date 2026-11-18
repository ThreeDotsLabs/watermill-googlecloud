package googlecloud

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cenkalti/backoff/v3"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// closedLock makes marking the subscriber as closed mutually exclusive with adding subscriptions
	// to allSubscriptionsWaitGroup, so no subscription is added after Close started waiting for them.
	closedLock sync.Mutex

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
		return nil, s.wrapClosedError(err)
	}

	if !s.addAllSubscriptionsWaitGroup() {
		cancel()
		return nil, ErrSubscriberClosed
	}

	// When ReceiveSettings.ShutdownOptions are set, Receive returns without waiting for running callbacks
	// (and may even start a callback after it returned), so callbacks may still send to output.
	// Callbacks hold outputLock for reading while they may use output, and output is closed with outputLock held
	// for writing, so it's never closed while a callback uses it.
	var outputLock sync.RWMutex
	outputClosed := false

	receiveFinished := make(chan struct{})
	go func() {
		exponentialBackoff := backoff.NewExponentialBackOff()
		exponentialBackoff.MaxElapsedTime = 0 // 0 means it never expires

		if err := backoff.Retry(func() error {
			err := s.receive(ctx, sub, logFields, output, &outputLock, &outputClosed)
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

		// Waits for running callbacks: Receive cancelled their contexts when it returned,
		// so callbacks waiting for the consumer or for an ack return right away.
		outputLock.Lock()
		outputClosed = true
		close(output)
		outputLock.Unlock()

		s.allSubscriptionsWaitGroup.Done()
	}()

	return output, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if s.closed.Load() {
		return ErrSubscriberClosed
	}

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
		return s.wrapClosedError(err)
	}

	return nil
}

// addAllSubscriptionsWaitGroup adds a subscription to allSubscriptionsWaitGroup, which Close waits for.
// It returns false if the subscriber is already closed.
func (s *Subscriber) addAllSubscriptionsWaitGroup() bool {
	s.closedLock.Lock()
	defer s.closedLock.Unlock()

	if s.closed.Load() {
		return false
	}

	s.allSubscriptionsWaitGroup.Add(1)
	return true
}

// wrapClosedError returns ErrSubscriberClosed (wrapped with err's message) if the subscriber was closed
// while err happened, as closing the client makes in-flight calls fail with errors that are hard to recognize.
func (s *Subscriber) wrapClosedError(err error) error {
	if !s.closed.Load() || errors.Is(err, ErrSubscriberClosed) {
		return err
	}

	return errors.Wrap(ErrSubscriberClosed, err.Error())
}

// Close notifies the Subscriber to stop processing messages on all subscriptions, close all the output channels
// and terminate the connection.
func (s *Subscriber) Close() error {
	// closedLock is held only while marking the subscriber as closed, not while waiting for subscriptions,
	// so concurrent Subscribe calls don't block until Close finishes.
	s.closedLock.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.closedLock.Unlock()
		return nil
	}
	close(s.closing)
	s.closedLock.Unlock()

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
	outputLock *sync.RWMutex,
	outputClosed *bool,
) error {
	return sub.Receive(ctx, func(ctx context.Context, pubsubMsg *pubsub.Message) {
		logFields := subcribeLogFields.Copy()

		outputLock.RLock()
		defer outputLock.RUnlock()

		if *outputClosed {
			s.logger.Info(
				"Message not consumed, receiving messages finished",
				logFields,
			)
			pubsubMsg.Nack()
			return
		}

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

		ack, outcome := waitForAck(ctx, s.closing, msg)
		if ack {
			pubsubMsg.Ack()
		} else {
			pubsubMsg.Nack()
		}
		s.logger.Trace(outcome, logFields)
	})
}

// waitForAck waits until the consumer acks or nacks msg, the subscriber is closing, or ctx is done.
// It returns true if msg should be acked, and the outcome to log.
func waitForAck(ctx context.Context, closing <-chan struct{}, msg *message.Message) (ack bool, outcome string) {
	// When more than one case is ready, select picks a random one, so closing (or ctx cancellation)
	// can win over an ack that already happened. Acked messages are acked anyway.
	select {
	case <-closing:
		if isAcked(msg) {
			return true, "Msg acked"
		}
		return false, "Closing, nacking message"
	case <-ctx.Done():
		if isAcked(msg) {
			return true, "Msg acked"
		}
		return false, "Ctx done, nacking message"
	case <-msg.Acked():
		return true, "Msg acked"
	case <-msg.Nacked():
		return false, "Msg nacked"
	}
}

// isAcked checks if msg is acked without waiting for it.
func isAcked(msg *message.Message) bool {
	select {
	case <-msg.Acked():
		return true
	default:
		return false
	}
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

	tExists, err := topicExists(ctx, s.client, s.config.topicProjectID(), topicName)
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

	var subConfig *pubsubpb.Subscription
	if s.config.GenerateSubscription != nil {
		subConfig = s.config.GenerateSubscription(GenerateSubscriptionParams{})
	} else {
		subConfig = &pubsubpb.Subscription{}
	}

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

	// Close marks the subscriber as closed before closing the client (with clientLock held),
	// so a client created here is either closed by Close, or not created at all.
	if s.closed.Load() {
		return ErrSubscriberClosed
	}

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

func fullyQualifiedSubscriptionName(projectID, subscriptionName string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subscriptionName)
}
