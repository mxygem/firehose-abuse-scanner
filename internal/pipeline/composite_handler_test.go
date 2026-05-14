package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/detect"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/mocks"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/mock"
)

type fakeDetector struct {
	hits []detect.Hit
}

func (f *fakeDetector) Inspect(_ context.Context, _ models.FirehoseEvent) []detect.Hit {
	return f.hits
}

func TestCompositeHandler_NoDetectorsJustInsertsEvent(t *testing.T) {
	ctx := context.Background()
	evt := models.FirehoseEvent{ID: "e1", DID: "did:plc:a", Kind: models.EventPost}

	ms := mocks.NewMockStorer(t)
	ms.EXPECT().InsertEvent(ctx, evt).Return(nil).Once()

	h := NewCompositeHandler(ms)
	require.NoError(t, h.Handle(ctx, evt))
}

func TestCompositeHandler_InsertEventErrorShortCircuits(t *testing.T) {
	ctx := context.Background()
	evt := models.FirehoseEvent{ID: "e1", DID: "did:plc:a", Kind: models.EventPost}
	dbErr := errors.New("db down")

	ms := mocks.NewMockStorer(t)
	ms.EXPECT().InsertEvent(ctx, evt).Return(dbErr).Once()

	d := &fakeDetector{hits: []detect.Hit{{RuleID: "should-not-run"}}}
	h := NewCompositeHandler(ms, d)
	err := h.Handle(ctx, evt)
	require.ErrorIs(t, err, dbErr)
}

func TestCompositeHandler_WritesFlaggedPerHit(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	evt := models.FirehoseEvent{
		ID:         "e1",
		DID:        "did:plc:a",
		Kind:       models.EventPost,
		Text:       "BUY NOW",
		ReceivedAt: now,
	}

	ms := mocks.NewMockStorer(t)
	ms.EXPECT().InsertEvent(ctx, evt).Return(nil).Once()
	ms.EXPECT().
		InsertFlagged(ctx, mock.MatchedBy(func(r storage.FlaggedRecord) bool {
			return r.EventID == "e1" && r.RuleID == "spam.keyword" && r.Severity == "medium"
		})).
		Return(nil).Once()
	ms.EXPECT().
		InsertFlagged(ctx, mock.MatchedBy(func(r storage.FlaggedRecord) bool {
			return r.EventID == "e1" && r.RuleID == "link.blocklist" && r.Severity == "high"
		})).
		Return(nil).Once()

	d1 := &fakeDetector{hits: []detect.Hit{{RuleID: "spam.keyword", Severity: "medium", Reason: "x"}}}
	d2 := &fakeDetector{hits: []detect.Hit{{RuleID: "link.blocklist", Severity: "high", Reason: "y"}}}

	h := NewCompositeHandler(ms, d1, d2)
	require.NoError(t, h.Handle(ctx, evt))
}

func TestCompositeHandler_AggregatesFlaggedErrors(t *testing.T) {
	ctx := context.Background()
	evt := models.FirehoseEvent{ID: "e1", DID: "did:plc:a", Kind: models.EventPost}

	flagErr := errors.New("flagged insert failed")
	ms := mocks.NewMockStorer(t)
	ms.EXPECT().InsertEvent(ctx, evt).Return(nil).Once()
	ms.EXPECT().InsertFlagged(ctx, mock.Anything).Return(flagErr).Twice()

	d := &fakeDetector{hits: []detect.Hit{
		{RuleID: "spam.keyword"},
		{RuleID: "link.blocklist"},
	}}
	h := NewCompositeHandler(ms, d)
	err := h.Handle(ctx, evt)
	require.ErrorIs(t, err, flagErr)
}
