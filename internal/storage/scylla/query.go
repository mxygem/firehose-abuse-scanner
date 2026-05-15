package scylla

import (
	"context"
	"fmt"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// EventRow is a row read out of events_by_did. Mirrors models.FirehoseEvent
// but lives in the storage package so callers don't have to translate it
// back into the firehose type for display.
type EventRow struct {
	DID        string
	ID         string
	Kind       models.EventKind
	Text       string
	Langs      []string
	Links      []string
	CreatedAt  time.Time
	ReceivedAt time.Time
}

// FlaggedRow is a row read out of flagged_events. Same shape as the write
// side's storage.FlaggedRecord, kept separate so the read API isn't bound
// to the write struct's evolution.
type FlaggedRow struct {
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

const (
	selectEventsByDIDStmt = `
		SELECT did, received_at, id, kind, text, langs, links, created_at
		FROM events_by_did WHERE did = ? LIMIT ?`

	selectFlaggedByBucketStmt = `
		SELECT id, rule_id, did, kind, text, severity, reason, evidence, received_at
		FROM flagged_events
		WHERE bucket_hour = ? AND received_at >= ?
		LIMIT ?`
)

// RecentEventsByDID returns the newest events written for the given DID,
// capped at limit. Order matches the events_by_did clustering — received_at
// descending.
func (s *Store) RecentEventsByDID(ctx context.Context, did string, limit int) ([]EventRow, error) {
	if limit <= 0 {
		limit = 50
	}
	iter := s.session.IterContext(ctx, selectEventsByDIDStmt, did, limit)

	var out []EventRow
	var (
		kind string
		row  EventRow
	)
	for iter.Scan(&row.DID, &row.ReceivedAt, &row.ID, &kind, &row.Text, &row.Langs, &row.Links, &row.CreatedAt) {
		row.Kind = models.EventKind(kind)
		out = append(out, row)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("query events_by_did: %w", err)
	}
	return out, nil
}

// RecentFlagged returns flagged-event rows received within the last `since`,
// newest first, capped at limit. It walks bucket_hour partitions from the
// current hour backwards until it covers the cutoff or fills the cap — so
// a 10-minute window typically touches one partition, an 80-minute window
// at most two.
func (s *Store) RecentFlagged(ctx context.Context, since time.Duration, limit int) ([]FlaggedRow, error) {
	if since <= 0 {
		since = 10 * time.Minute
	}
	if limit <= 0 {
		limit = 100
	}

	now := time.Now().UTC()
	cutoff := now.Add(-since)
	currentBucket := now.Truncate(time.Hour)
	earliestBucket := cutoff.Truncate(time.Hour)

	var out []FlaggedRow
	for bucket := currentBucket; !bucket.Before(earliestBucket) && len(out) < limit; bucket = bucket.Add(-time.Hour) {
		remaining := limit - len(out)
		iter := s.session.IterContext(ctx, selectFlaggedByBucketStmt, bucket, cutoff, remaining)
		var (
			kind string
			row  FlaggedRow
		)
		for iter.Scan(&row.EventID, &row.RuleID, &row.DID, &kind, &row.Text, &row.Severity, &row.Reason, &row.Evidence, &row.ReceivedAt) {
			row.Kind = models.EventKind(kind)
			out = append(out, row)
			if len(out) >= limit {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("query flagged_events bucket %s: %w", bucket.Format(time.RFC3339), err)
		}
	}
	return out, nil
}
