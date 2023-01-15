// +build stress

package googlecloud_test

import (
	"testing"
	"runtime"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)


func init() {
	// Set GOMAXPROCS to double the number of CPUs
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}

// Run `docker-compose up` and set PUBSUB_EMULATOR_HOST=localhost:8085 for this to work

func TestPublishSubscribe_stress(t *testing.T) {
	tests.TestPubSubStressTest(
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
