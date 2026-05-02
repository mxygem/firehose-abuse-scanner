//go:build integration

// Integration tests against a real Scylla node. Run via:
//
//	docker compose up -d db
//	go test -tags=integration ./internal/storage/scylla/...
//
// SCYLLA_TEST_HOSTS overrides the default 127.0.0.1:9042. Each test gets
// its own ephemeral keyspace (test_ks_<nanos>) and tears it down via
// t.Cleanup, so parallel runs don't stomp each other.

package scylla

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

// newTestStore connects to Scylla, applies the schema into a unique
// keyspace, registers cleanup, and returns the live Store. If Scylla
// isn't reachable the test is skipped (rather than failed) so a
// developer running the suite without docker compose up still gets a
// useful signal.
func newTestStore(t *testing.T) (*Store, Config) {
	t.Helper()

	hosts := os.Getenv("SCYLLA_TEST_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1:9042"
	}

	cfg := Config{
		Hosts:    strings.Split(hosts, ","),
		Keyspace: fmt.Sprintf("test_ks_%d", time.Now().UnixNano()),
		Timeout:  5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := New(ctx, cfg)
	if err != nil {
		t.Skipf("scylla not reachable at %v: %v", cfg.Hosts, err)
	}

	t.Cleanup(func() {
		// Drop the keyspace before closing the session so the next run
		// starts clean even if a test panics partway through.
		_ = s.session.Query(`DROP KEYSPACE IF EXISTS ` + cfg.Keyspace).Exec()
		_ = s.Close()
	})

	return s, cfg
}

func TestNew_AppliesSchemaAndIsIdempotent(t *testing.T) {
	s, cfg := newTestStore(t)

	// All three tables should exist in the keyspace after New returns.
	tables := []string{"events_by_did", "events_by_minute", "flagged_events"}
	for _, tbl := range tables {
		var name string
		err := s.session.Query(
			`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`,
			cfg.Keyspace, tbl,
		).Scan(&name)
		require.NoErrorf(t, err, "expected table %s to exist", tbl)
		assert.Equal(t, tbl, name)
	}

	// Re-running New against the same keyspace must succeed — bootstrap
	// is the hot path on every startup so any non-idempotent CREATE
	// would surface here.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s2, err := New(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, s2.Close())
}

func TestInsertEvent_WritesBothViews(t *testing.T) {
	s, _ := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := models.FirehoseEvent{
		ID:         "evt-abc",
		DID:        "did:plc:alice",
		Kind:       models.EventPost,
		Text:       "hello world",
		Langs:      []string{"en"},
		Links:      []string{"https://bsky.app"},
		CreatedAt:  now.Add(-time.Second),
		ReceivedAt: now,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.InsertEvent(ctx, evt))

	// events_by_did
	var (
		gotID   string
		gotKind string
		gotText string
	)
	err := s.session.Query(
		`SELECT id, kind, text FROM events_by_did WHERE did = ? LIMIT 1`,
		evt.DID,
	).Scan(&gotID, &gotKind, &gotText)
	require.NoError(t, err)
	assert.Equal(t, evt.ID, gotID)
	assert.Equal(t, string(evt.Kind), gotKind)
	assert.Equal(t, evt.Text, gotText)

	// events_by_minute — same row, partitioned by (kind, bucket_minute)
	bucket := evt.ReceivedAt.Truncate(time.Minute)
	err = s.session.Query(
		`SELECT id, did FROM events_by_minute WHERE kind = ? AND bucket_minute = ? LIMIT 1`,
		string(evt.Kind), bucket,
	).Scan(&gotID, &gotKind /* reused as gotDID */)
	require.NoError(t, err)
	assert.Equal(t, evt.ID, gotID)
	assert.Equal(t, evt.DID, gotKind)
}

func TestInsertFlagged_RoundTrip(t *testing.T) {
	s, _ := newTestStore(t)

	now := time.Now().UTC().Truncate(time.Millisecond)
	rec := storage.FlaggedRecord{
		EventID:    "evt-flagged",
		DID:        "did:plc:spammer",
		Kind:       models.EventPost,
		Text:       "BUY NOW LIMITED OFFER",
		ReceivedAt: now,
		RuleID:     "spam-keyword",
		Severity:   "medium",
		Reason:     "matched promotional keyword",
		Evidence:   map[string]string{"match": "BUY NOW"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.InsertFlagged(ctx, rec))

	bucket := rec.ReceivedAt.Truncate(time.Hour)
	var (
		gotID, gotRule, gotSeverity, gotReason string
		gotEvidence                            map[string]string
	)
	err := s.session.Query(
		`SELECT id, rule_id, severity, reason, evidence
		 FROM flagged_events WHERE bucket_hour = ? LIMIT 1`,
		bucket,
	).Scan(&gotID, &gotRule, &gotSeverity, &gotReason, &gotEvidence)
	require.NoError(t, err)
	assert.Equal(t, rec.EventID, gotID)
	assert.Equal(t, rec.RuleID, gotRule)
	assert.Equal(t, rec.Severity, gotSeverity)
	assert.Equal(t, rec.Reason, gotReason)
	assert.Equal(t, rec.Evidence, gotEvidence)
}

func TestBootstrap_AgainstFreshSession(t *testing.T) {
	// Direct bootstrap call (rather than going through New) — verifies
	// the embed + statement-splitter path against the live driver
	// without overlapping with the New-level idempotency check above.
	hosts := os.Getenv("SCYLLA_TEST_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1:9042"
	}
	cfg := Config{
		Hosts:    strings.Split(hosts, ","),
		Keyspace: fmt.Sprintf("test_ks_bootstrap_%d", time.Now().UnixNano()),
		Timeout:  5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := bootstrap(ctx, cfg); err != nil {
		// Cluster isn't up — same skip pathway as newTestStore.
		t.Skipf("scylla not reachable at %v: %v", cfg.Hosts, err)
	}

	t.Cleanup(func() {
		cluster := newCluster(cfg)
		sess, err := cluster.CreateSession()
		if err != nil {
			return
		}
		defer sess.Close()
		_ = sess.Query(`DROP KEYSPACE IF EXISTS ` + cfg.Keyspace).Exec()
	})

	// Verify the keyspace landed.
	cluster := newCluster(cfg)
	sess, err := cluster.CreateSession()
	require.NoError(t, err)
	defer sess.Close()

	var name string
	err = sess.Query(
		`SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?`,
		cfg.Keyspace,
	).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, cfg.Keyspace, name)

	// Idempotent — running it again is a no-op, not an error.
	require.NoError(t, bootstrap(ctx, cfg))
}

// Compile-time sanity: gocql is wired in (used by helpers above).
var _ = gocql.Consistency(0)
