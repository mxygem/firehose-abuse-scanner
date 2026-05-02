package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/mocks"
)

// Compile-time check: the generated mock satisfies the Storer contract.
// If the interface gains/loses methods and the mock isn't regenerated,
// this fails at build time rather than at the call site.
var _ storage.Storer = (*mocks.MockStorer)(nil)

// TestStorer_RoundTrip exercises all three interface methods against the
// generated mock. Doubles as documentation for how downstream packages
// should plug a fake storer into their tests.
func TestStorer_RoundTrip(t *testing.T) {
	ctx := context.Background()

	evt := models.FirehoseEvent{
		ID:         "evt-1",
		DID:        "did:plc:test",
		Kind:       models.EventPost,
		Text:       "hello",
		ReceivedAt: time.Now(),
		CreatedAt:  time.Now(),
	}

	flagged := storage.FlaggedRecord{
		EventID:    "evt-1",
		DID:        "did:plc:test",
		Kind:       models.EventPost,
		Text:       "BUY NOW",
		ReceivedAt: time.Now(),
		RuleID:     "spam-keyword",
		Severity:   "medium",
		Reason:     "matched promotional keyword",
		Evidence:   map[string]string{"match": "BUY NOW"},
	}

	m := mocks.NewMockStorer(t)
	m.EXPECT().InsertEvent(ctx, evt).Return(nil).Once()
	m.EXPECT().InsertFlagged(ctx, flagged).Return(nil).Once()
	m.EXPECT().Close().Return(nil).Once()

	// The handler-side code only ever depends on the interface; never the
	// concrete impl. Bind to storage.Storer here to make that explicit.
	var s storage.Storer = m
	require.NoError(t, s.InsertEvent(ctx, evt))
	require.NoError(t, s.InsertFlagged(ctx, flagged))
	require.NoError(t, s.Close())
}

// TestStorer_PropagatesErrors verifies that errors returned by an
// implementation flow back to the caller unchanged. The pipeline relies
// on this to count handler errors, so a wrapper swallowing them would be
// a real bug.
func TestStorer_PropagatesErrors(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("backend down")

	m := mocks.NewMockStorer(t)
	m.EXPECT().InsertEvent(ctx, models.FirehoseEvent{}).Return(wantErr).Once()
	m.EXPECT().InsertFlagged(ctx, storage.FlaggedRecord{}).Return(wantErr).Once()
	m.EXPECT().Close().Return(wantErr).Once()

	var s storage.Storer = m
	assert.ErrorIs(t, s.InsertEvent(ctx, models.FirehoseEvent{}), wantErr)
	assert.ErrorIs(t, s.InsertFlagged(ctx, storage.FlaggedRecord{}), wantErr)
	assert.ErrorIs(t, s.Close(), wantErr)
}
