package noop

import (
	"context"
	"sync/atomic"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

var _ storage.Storer = (*Store)(nil)

type Store struct {
	events  atomic.Uint64
	flagged atomic.Uint64
}

func New() *Store { return &Store{} }

func (s *Store) InsertEvent(_ context.Context, _ models.FirehoseEvent) error {
	s.events.Add(1)
	return nil
}

func (s *Store) InsertFlagged(_ context.Context, _ storage.FlaggedRecord) error {
	s.flagged.Add(1)
	return nil
}

func (s *Store) Close() error { return nil }

func (s *Store) InsertedEvents()  uint64 { return s.events.Load() }
func (s *Store) InsertedFlagged() uint64 { return s.flagged.Load() }
