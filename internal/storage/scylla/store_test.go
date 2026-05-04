package scylla

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	gocqlmock "github.com/Agent-Plus/gocqlmock"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

// mockCQLSession wraps *gocqlmock.Session to satisfy the unexported cqlSession
// interface. ExpectQuery/ExpectExec/etc. are promoted from the embedded type,
// so tests set expectations directly on *mockCQLSession.
type mockCQLSession struct {
	*gocqlmock.Session
}

// QueryContext satisfies cqlSession. Context is ignored by the mock —
// gocqlmock matches expectations by query regex and argument order only.
func (m *mockCQLSession) QueryContext(_ context.Context, stmt string, args ...interface{}) cqlQuery {
	return m.Session.Query(stmt, args...)
}

func (m *mockCQLSession) Close() {}

// newMockStore returns a Store wired to a fresh gocqlmock session.
// Callers set expectations on the returned *mockCQLSession before exercising
// the Store method under test.
func newMockStore(t *testing.T) (*Store, *mockCQLSession) {
	t.Helper()
	ms := &mockCQLSession{gocqlmock.New(t)}
	return &Store{session: ms, l: slog.Default()}, ms
}

func TestStore_InsertEvent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	evt := models.FirehoseEvent{
		ID:         "evt-1",
		DID:        "did:plc:test",
		Kind:       models.EventPost,
		Text:       "hello",
		Langs:      []string{"en"},
		Links:      []string{"https://bsky.app"},
		CreatedAt:  now.Add(-time.Second),
		ReceivedAt: now,
	}
	dbErr := errors.New("db error")

	tests := []struct {
		name        string
		setup       func(*mockCQLSession)
		expectedErr string
	}{
		{
			name: "both views written successfully",
			setup: func(ms *mockCQLSession) {
				ms.ExpectQuery(`INSERT INTO events_by_did`).ExpectExec()
				ms.ExpectQuery(`INSERT INTO events_by_minute`).ExpectExec()
			},
		},
		{
			name: "events_by_did write fails",
			setup: func(ms *mockCQLSession) {
				ms.ExpectQuery(`INSERT INTO events_by_did`).ExpectExec().WithError(dbErr)
			},
			expectedErr: "insert events_by_did",
		},
		{
			name: "events_by_minute write fails",
			setup: func(ms *mockCQLSession) {
				ms.ExpectQuery(`INSERT INTO events_by_did`).ExpectExec()
				ms.ExpectQuery(`INSERT INTO events_by_minute`).ExpectExec().WithError(dbErr)
			},
			expectedErr: "insert events_by_minute",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ms := newMockStore(t)
			tc.setup(ms)

			err := store.InsertEvent(ctx, evt)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestStore_InsertFlagged(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	rec := storage.FlaggedRecord{
		EventID:    "evt-1",
		DID:        "did:plc:test",
		Kind:       models.EventPost,
		Text:       "BUY NOW",
		ReceivedAt: now,
		RuleID:     "spam-keyword",
		Severity:   "medium",
		Reason:     "matched promotional keyword",
		Evidence:   map[string]string{"match": "BUY NOW"},
	}
	dbErr := errors.New("db error")

	tests := []struct {
		name        string
		setup       func(*mockCQLSession)
		expectedErr string
	}{
		{
			name: "flagged record written successfully",
			setup: func(ms *mockCQLSession) {
				ms.ExpectQuery(`INSERT INTO flagged_events`).ExpectExec()
			},
		},
		{
			name: "write fails returns wrapped error",
			setup: func(ms *mockCQLSession) {
				ms.ExpectQuery(`INSERT INTO flagged_events`).ExpectExec().WithError(dbErr)
			},
			expectedErr: "insert flagged_events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, ms := newMockStore(t)
			tc.setup(ms)

			err := store.InsertFlagged(ctx, rec)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSplitStatements(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected []string
	}{
		{
			desc:     "empty input",
			input:    "",
			expected: []string{},
		},
		{
			desc:     "single statement no terminator",
			input:    "CREATE KEYSPACE foo",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "single statement with terminator",
			input:    "CREATE KEYSPACE foo;",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "two statements",
			input:    "USE foo; CREATE TABLE bar (id text PRIMARY KEY);",
			expected: []string{"USE foo", "CREATE TABLE bar (id text PRIMARY KEY)"},
		},
		{
			desc:     "line comments stripped",
			input:    "-- this creates a keyspace\nCREATE KEYSPACE foo;",
			expected: []string{"CREATE KEYSPACE foo"},
		},
		{
			desc:     "trailing whitespace and only-comment chunks dropped",
			input:    "-- only a comment\n\n   \n;",
			expected: []string{},
		},
		{
			desc: "multi-line statement preserved",
			input: `CREATE TABLE foo (
    id text PRIMARY KEY,
    name text
);`,
			expected: []string{"CREATE TABLE foo (\n    id text PRIMARY KEY,\n    name text\n)"},
		},
		{
			desc: "inline comment trimmed mid-line",
			input: `CREATE TABLE foo ( -- the foo table
    id text PRIMARY KEY
);`,
			expected: []string{"CREATE TABLE foo ( \n    id text PRIMARY KEY\n)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, splitStatements(tc.input))
		})
	}
}

func TestParseConsistency(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected gocql.Consistency
	}{
		{"empty defaults to ONE", "", gocql.One},
		{"unknown falls back to ONE", "BANANA", gocql.One},
		{"uppercase ONE", "ONE", gocql.One},
		{"lowercase one", "one", gocql.One},
		{"surrounding whitespace tolerated", "  QUORUM  ", gocql.Quorum},
		{"ANY", "ANY", gocql.Any},
		{"TWO", "TWO", gocql.Two},
		{"THREE", "THREE", gocql.Three},
		{"QUORUM", "QUORUM", gocql.Quorum},
		{"ALL", "ALL", gocql.All},
		{"LOCAL_QUORUM", "LOCAL_QUORUM", gocql.LocalQuorum},
		{"EACH_QUORUM", "EACH_QUORUM", gocql.EachQuorum},
		{"LOCAL_ONE", "LOCAL_ONE", gocql.LocalOne},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, parseConsistency(tc.input))
		})
	}
}

func TestIsUseStatement(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		expected bool
	}{
		{"plain USE", "USE foo", true},
		{"lowercase use", "use foo", true},
		{"leading whitespace", "  USE foo", true},
		{"USE substring is not a USE statement", "USERS_TABLE", false},
		{"CREATE is not USE", "CREATE TABLE foo (id text PRIMARY KEY)", false},
		{"empty", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.expected, isUseStatement(tc.input))
		})
	}
}
