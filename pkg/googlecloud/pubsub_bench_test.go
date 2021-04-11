package googlecloud_test

import (
	"testing"

	"github.com/sidkik/watermill"
	"github.com/sidkik/watermill-googlecloud/pkg/googlecloud"
	"github.com/sidkik/watermill/message"
	"github.com/sidkik/watermill/pubsub/tests"
)

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func BenchmarkSubscriber(b *testing.B) {
	tests.BenchSubscriber(b, func(n int) (message.Publisher, message.Subscriber) {
		logger := watermill.NopLogger{}

		publisher, err := googlecloud.NewPublisher(
			googlecloud.PublisherConfig{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		subscriber, err := googlecloud.NewSubscriber(
			googlecloud.SubscriberConfig{},
			logger,
		)
		if err != nil {
			panic(err)
		}

		return publisher, subscriber
	})
}
