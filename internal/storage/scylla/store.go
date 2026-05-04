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
	"log/slog"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

// Compile-time check: *Store satisfies storage.Storer.
var _ storage.Storer = (*Store)(nil)

//go:embed schema.cql
var schemaCQL string

// Config carries everything needed to dial Scylla. Hosts are "host:port"
// strings; an empty Consistency defaults to ONE (matches RF=1 demo).
type Config struct {
	Logger      *slog.Logger
	Hosts       []string
	Keyspace    string
	Consistency string
	Timeout     time.Duration
}

type Store struct {
	session *gocql.Session
	l       *slog.Logger
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
	s := &Store{
		l: cfg.Logger,
	}
	if s.l == nil {
		s.l = slog.Default()
	}

	if err := bootstrap(ctx, cfg); err != nil {
		return nil, fmt.Errorf("bootstrap schema: %w", err)
	}

	cluster := newCluster(cfg)
	cluster.Keyspace = cfg.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	s.session = session

	return s, nil
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

func (s *Store) InsertFlagged(ctx context.Context, r storage.FlaggedRecord) error {
	bucketHour := r.ReceivedAt.Truncate(time.Hour)

	q := s.session.Query(insertFlaggedStmt,
		bucketHour, r.ReceivedAt, r.EventID, r.RuleID, r.DID, string(r.Kind), r.Text, r.Severity, r.Reason, r.Evidence,
	).WithContext(ctx)
	s.l.Debug("insert flagged event", "query", q)
	if err := q.Exec(); err != nil {
		return fmt.Errorf("insert flagged_events: %w", err)
	}

	return nil
}

func (s *Store) Close() error {
	if s.session != nil {
		s.session.Close()
	}
	return nil
}

// bootstrap applies the embedded schema in two phases:
//  1. No-keyspace session: runs CREATE KEYSPACE (and skips USE).
//  2. Keyspace-bound session: runs CREATE TABLE statements.
//
// USE statements are skipped because gocql routes queries across a connection
// pool and each connection tracks its own current keyspace, making USE
// unreliable. Opening the second session with Keyspace set is the fix.
func bootstrap(ctx context.Context, cfg Config) error {
	stmts := splitStatements(schemaCQL)

	// Phase 1: CREATE KEYSPACE — no keyspace bound yet.
	sess, err := newCluster(cfg).CreateSession()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	var tableStmts []string
	for _, stmt := range stmts {
		if isUseStatement(stmt) {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(stmt), "CREATE KEYSPACE") {
			if err := sess.Query(stmt).WithContext(ctx).Exec(); err != nil {
				sess.Close()
				return fmt.Errorf("apply statement: %w\n%s", err, stmt)
			}
		} else {
			tableStmts = append(tableStmts, stmt)
		}
	}
	sess.Close()

	// Phase 2: CREATE TABLE — session bound to the keyspace.
	cluster := newCluster(cfg)
	cluster.Keyspace = cfg.Keyspace
	sess2, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect with keyspace: %w", err)
	}
	defer sess2.Close()
	for _, stmt := range tableStmts {
		if err := sess2.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("apply statement: %w\n%s", err, stmt)
		}
	}
	return nil
}

func newCluster(cfg Config) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Consistency = parseConsistency(cfg.Consistency)
	// Scylla 5.x supports CQL native protocol v4. Setting it explicitly
	// avoids gocql's startup-time protocol discovery, which can fail while
	// a single-node dev container is still warming up.
	cluster.ProtoVersion = 4
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
	// Strip line comments before splitting on ";", so a semicolon inside a
	// comment (e.g. "-- foo; bar") never produces a spurious empty statement.
	var b strings.Builder
	for line := range strings.SplitSeq(s, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	parts := strings.Split(b.String(), ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if cleaned := strings.TrimSpace(p); cleaned != "" {
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
