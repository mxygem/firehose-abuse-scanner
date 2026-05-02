// Package scylla wraps gocql with the small surface the scanner needs:
// schema bootstrap on startup, plus prepared inserts for raw and flagged
// events. The wrapper is intentionally thin — gocql's session already
// caches prepared statements and is safe for concurrent use, so the Store
// is just a typed facade over the queries this app writes.
package scylla

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

//go:embed schema.cql
var schemaCQL string

// Config carries everything needed to dial Scylla. Hosts are "host:port"
// strings; an empty Consistency defaults to ONE (matches RF=1 demo).
type Config struct {
	Hosts       []string
	Keyspace    string
	Consistency string
	Timeout     time.Duration
}

// FlaggedRecord is the persistence-layer view of a detector hit. The
// detect package will produce its own Hit type in Phase 2; the composite
// handler translates Hit → FlaggedRecord at write time.
type FlaggedRecord struct {
	EventID    string
	DID        string
	Kind       models.EventKind
	Text       string
	ReceivedAt time.Time
	RuleID     string
	Severity   string
	Reason     string
	Evidence   map[string]string
}

type Store struct {
	session *gocql.Session
}

const (
	insertEventByDIDStmt = `
		INSERT INTO events_by_did
			(did, received_at, id, kind, text, langs, links, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	insertEventByMinuteStmt = `
		INSERT INTO events_by_minute
			(kind, bucket_minute, received_at, id, did, text, langs, links, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertFlaggedStmt = `
		INSERT INTO flagged_events
			(bucket_hour, received_at, id, rule_id, did, kind, text, severity, reason, evidence)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// New connects to Scylla, applies the embedded schema, and returns a
// ready-to-use Store. Bootstrap is idempotent (CREATE ... IF NOT EXISTS)
// so it's safe to run on every startup.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if err := bootstrap(ctx, cfg); err != nil {
		return nil, fmt.Errorf("bootstrap schema: %w", err)
	}

	cluster := newCluster(cfg)
	cluster.Keyspace = cfg.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	return &Store{session: session}, nil
}

// InsertEvent writes the same event into both views (events_by_did and
// events_by_minute). The two writes are not batched — for the demo,
// eventual consistency between the views is acceptable, and avoiding a
// LOGGED BATCH keeps the throughput numbers honest. Phase 1.5 introduces
// per-worker UNLOGGED BATCH buffering as a perf lever.
func (s *Store) InsertEvent(ctx context.Context, e models.FirehoseEvent) error {
	bucketMinute := e.ReceivedAt.Truncate(time.Minute)

	if err := s.session.Query(insertEventByDIDStmt,
		e.DID, e.ReceivedAt, e.ID, string(e.Kind), e.Text, e.Langs, e.Links, e.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("insert events_by_did: %w", err)
	}

	if err := s.session.Query(insertEventByMinuteStmt,
		string(e.Kind), bucketMinute, e.ReceivedAt, e.ID, e.DID, e.Text, e.Langs, e.Links, e.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("insert events_by_minute: %w", err)
	}

	return nil
}

func (s *Store) InsertFlagged(ctx context.Context, r FlaggedRecord) error {
	bucketHour := r.ReceivedAt.Truncate(time.Hour)

	if err := s.session.Query(insertFlaggedStmt,
		bucketHour, r.ReceivedAt, r.EventID, r.RuleID, r.DID, string(r.Kind), r.Text, r.Severity, r.Reason, r.Evidence,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("insert flagged_events: %w", err)
	}

	return nil
}

func (s *Store) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

// bootstrap opens a no-keyspace session, applies every statement in the
// embedded schema, and closes. The CREATE KEYSPACE has to land before the
// CREATE TABLEs, but otherwise statement ordering inside schema.cql is
// preserved.
func bootstrap(ctx context.Context, cfg Config) error {
	cluster := newCluster(cfg)
	// Deliberately leave Keyspace empty — CREATE KEYSPACE has to run before
	// the session can usefully bind to one.

	sess, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer sess.Close()

	for _, stmt := range splitStatements(schemaCQL) {
		// USE statements don't behave reliably across a connection pool —
		// each conn tracks its own current keyspace. Fully-qualified names
		// in the CREATE TABLEs would fix this, but the simpler move is to
		// skip USE here and re-open the working session with Keyspace set.
		if isUseStatement(stmt) {
			continue
		}
		if err := sess.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("apply statement: %w\n%s", err, stmt)
		}
	}
	return nil
}

func newCluster(cfg Config) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Consistency = parseConsistency(cfg.Consistency)
	if cfg.Timeout > 0 {
		cluster.Timeout = cfg.Timeout
		cluster.ConnectTimeout = cfg.Timeout
	}
	return cluster
}

// splitStatements turns the embedded schema text into individual CQL
// statements. CQL line comments (`-- ...`) are stripped first so the
// driver doesn't have to parse them, and empty chunks are dropped.
//
// The schema deliberately avoids `;` inside string literals so a naive
// split is sufficient. If that ever changes, swap this for a proper
// tokenizer.
func splitStatements(s string) []string {
	parts := strings.Split(s, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		var lines []string
		for _, line := range strings.Split(p, "\n") {
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
			lines = append(lines, line)
		}
		cleaned := strings.TrimSpace(strings.Join(lines, "\n"))
		if cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return out
}

func isUseStatement(stmt string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "USE ")
}

func parseConsistency(s string) gocql.Consistency {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "ANY":
		return gocql.Any
	case "TWO":
		return gocql.Two
	case "THREE":
		return gocql.Three
	case "QUORUM":
		return gocql.Quorum
	case "ALL":
		return gocql.All
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	case "EACH_QUORUM":
		return gocql.EachQuorum
	case "LOCAL_ONE":
		return gocql.LocalOne
	default:
		return gocql.One
	}
}
