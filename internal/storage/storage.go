// Package storage defines the Storer interface that sits between the
// scanner pipeline and whatever durable backend it's writing to. Concrete
// implementations live in subpackages (e.g. internal/storage/scylla);
// tests and examples can supply in-memory or recording stand-ins.
package storage

import (
	"context"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// Storer is the persistence surface the scanner depends on. Implementations
// must be safe for concurrent use — the pipeline calls Insert* from many
// worker goroutines.
type Storer interface {
	InsertEvent(ctx context.Context, e models.FirehoseEvent) error
	InsertFlagged(ctx context.Context, r FlaggedRecord) error
	Close() error
}

// FlaggedRecord is the persistence-layer view of a detector hit. The
// detect package (Phase 2) produces its own Hit type; the composite
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
