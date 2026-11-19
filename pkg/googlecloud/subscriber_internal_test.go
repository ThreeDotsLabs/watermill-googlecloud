package googlecloud

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestWaitForAck(t *testing.T) {
	t.Parallel()

	notClosing := make(chan struct{})
	closing := make(chan struct{})
	close(closing)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name        string
		ctx         context.Context
		closing     <-chan struct{}
		consumerAck func(msg *message.Message)
		expectedAck bool
	}{
		{
			name:        "acked",
			ctx:         context.Background(),
			closing:     notClosing,
			consumerAck: func(msg *message.Message) { msg.Ack() },
			expectedAck: true,
		},
		{
			name:        "nacked",
			ctx:         context.Background(),
			closing:     notClosing,
			consumerAck: func(msg *message.Message) { msg.Nack() },
			expectedAck: false,
		},
		{
			name:        "not_acked_when_closing",
			ctx:         context.Background(),
			closing:     closing,
			consumerAck: func(msg *message.Message) {},
			expectedAck: false,
		},
		{
			name:        "not_acked_when_ctx_done",
			ctx:         canceledCtx,
			closing:     notClosing,
			consumerAck: func(msg *message.Message) {},
			expectedAck: false,
		},
		{
			name:        "nacked_when_closing",
			ctx:         canceledCtx,
			closing:     closing,
			consumerAck: func(msg *message.Message) { msg.Nack() },
			expectedAck: false,
		},
		{
			name:        "acked_when_closing",
			ctx:         context.Background(),
			closing:     closing,
			consumerAck: func(msg *message.Message) { msg.Ack() },
			expectedAck: true,
		},
		{
			name:        "acked_when_ctx_done",
			ctx:         canceledCtx,
			closing:     notClosing,
			consumerAck: func(msg *message.Message) { msg.Ack() },
			expectedAck: true,
		},
		{
			name:        "acked_when_closing_and_ctx_done",
			ctx:         canceledCtx,
			closing:     closing,
			consumerAck: func(msg *message.Message) { msg.Ack() },
			expectedAck: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// select picks a random case when more than one is ready, so a single call could pass by chance
			for i := 0; i < 1000; i++ {
				msg := message.NewMessage(watermill.NewUUID(), nil)
				tc.consumerAck(msg)

				ack, _ := waitForAck(tc.ctx, tc.closing, msg)
				require.Equal(t, tc.expectedAck, ack, "iteration %d", i)
			}
		})
	}
}
