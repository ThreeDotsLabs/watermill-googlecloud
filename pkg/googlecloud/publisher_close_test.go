package googlecloud_test

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

// The unsynchronised access to the closed flag is only reported by the race detector,
// so this test can catch a regression only when run with -race.
func TestPublisherPublishConcurrentWithClose(t *testing.T) {
	pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
		ProjectID: "tests",
	}, nil)
	require.NoError(t, err)

	topic := fmt.Sprintf("topic_publish_concurrent_with_close_%s", uuid.NewString())
	require.NoError(t, pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte{})))

	publishing := make(chan struct{}, 1)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				err := pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte{}))
				if errors.Is(err, googlecloud.ErrPublisherClosed) {
					return
				}
				// other errors are expected while the publisher is closing

				select {
				case publishing <- struct{}{}:
				default:
				}
			}
		})
	}

	// close while the goroutines keep publishing
	<-publishing
	require.NoError(t, pub.Close())
	wg.Wait()

	assert.ErrorIs(t, pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte{})), googlecloud.ErrPublisherClosed)
}

func TestPublisherConcurrentClose(t *testing.T) {
	// The window between checking and setting the closed flag is narrow,
	// so the check is repeated with fresh publishers to catch a regression reliably.
	const attempts = 50
	// more spinning goroutines than CPUs would only wait for preemption
	closers := int32(max(2, min(4, runtime.GOMAXPROCS(0))))

	for i := range attempts {
		pub, err := googlecloud.NewPublisher(googlecloud.PublisherConfig{
			ProjectID: "tests",
		}, nil)
		require.NoError(t, err)

		var ready atomic.Int32
		errs := make(chan error, closers)

		var wg sync.WaitGroup
		for range closers {
			wg.Go(func() {
				// spin instead of blocking on a channel, so all goroutines call Close at nearly the same time
				ready.Add(1)
				for ready.Load() < closers {
				}
				errs <- pub.Close()
			})
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err, "attempt %d", i)
		}
	}
}
