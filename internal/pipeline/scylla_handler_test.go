package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/mocks"
	"github.com/stretchr/testify/require"
)

func TestScyllaHandler_Handle(t *testing.T) {
	ctx := context.Background()
	evt := models.FirehoseEvent{
		ID:         "evt-1",
		DID:        "did:plc:test",
		Kind:       models.EventPost,
		Text:       "hello world!",
		ReceivedAt: time.Now(),
	}
	dbErr := errors.New("db error")

	testCases := []struct {
		name        string
		setupMock   func(*testing.T) *mocks.MockStorer
		expectedErr error
	}{
		{
			name: "successful insert returns nil",
			setupMock: func(t *testing.T) *mocks.MockStorer {
				ms := mocks.NewMockStorer(t)
				ms.EXPECT().InsertEvent(ctx, evt).Return(nil).Once()
				return ms
			},
		},
		{
			name: "store error is propagated",
			setupMock: func(t *testing.T) *mocks.MockStorer {
				ms := mocks.NewMockStorer(t)
				ms.EXPECT().InsertEvent(ctx, evt).Return(dbErr).Once()
				return ms
			},
			expectedErr: dbErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewScyllaHandler(tc.setupMock(t))
			err := h.Handle(ctx, evt)
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
