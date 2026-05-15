package scylla

import (
	"context"
	"errors"
	"testing"
	"time"

	gocqlmock "github.com/Agent-Plus/gocqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

func TestStore_RecentEventsByDID(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 14, 12, 30, 0, 0, time.UTC)

	rows := gocqlmock.NewRows().
		AddRow("did:plc:alice", now, "evt-2", "app.bsky.feed.post", "hello again",
			[]string{"en"}, []string{"https://bsky.app"}, now.Add(-time.Second)).
		AddRow("did:plc:alice", now.Add(-time.Minute), "evt-1", "app.bsky.feed.post", "hello",
			[]string{"en"}, []string{}, now.Add(-time.Minute-time.Second))

	store, ms := newMockStore(t)
	ms.ExpectQuery(`SELECT .* FROM events_by_did`).ExpectIter().WithResult(rows)

	got, err := store.RecentEventsByDID(ctx, "did:plc:alice", 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "evt-2", got[0].ID)
	assert.Equal(t, models.EventPost, got[0].Kind)
	assert.Equal(t, "hello again", got[0].Text)
	assert.Equal(t, "evt-1", got[1].ID)
}

func TestStore_RecentEventsByDID_Empty(t *testing.T) {
	ctx := context.Background()
	store, ms := newMockStore(t)
	ms.ExpectQuery(`SELECT .* FROM events_by_did`).ExpectIter().WithResult(gocqlmock.NewRows())

	got, err := store.RecentEventsByDID(ctx, "did:plc:nobody", 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestStore_RecentEventsByDID_IterCloseError(t *testing.T) {
	ctx := context.Background()
	store, ms := newMockStore(t)
	dbErr := errors.New("scan failed")
	ms.ExpectQuery(`SELECT .* FROM events_by_did`).ExpectIter().WithError(dbErr)

	_, err := store.RecentEventsByDID(ctx, "did:plc:alice", 10)
	require.ErrorContains(t, err, "query events_by_did")
}

// Note: RecentFlagged scans a map<text,text> column which the gocqlmock
// driver doesn't decode (it errors with "can't set destination argument
// type map"). Round-trip coverage for that path lives in the integration
// test against a real Scylla node (TestRecentFlagged_RoundTrip).
