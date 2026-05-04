package scylla

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

// Compile-time check: *BatchStore satisfies storage.Storer.
var _ storage.Storer = (*BatchStore)(nil)

// BatchConfig controls buffering behavior for the batched writer.
type BatchConfig struct {
	// MaxSize triggers a flush when the buffer reaches this many events.
	MaxSize int
	// FlushInterval triggers a flush after this much time regardless of buffer size.
	FlushInterval time.Duration
	// FlushWorkers is the number of goroutines executing Scylla batches in parallel.
	FlushWorkers int
	// FlushQueueSize is the max number of full event-batches waiting to flush.
	FlushQueueSize int
	// BufferShards is the number of independent in-memory buffers used to
	// reduce lock contention on InsertEvent.
	BufferShards int
}

// BatchStore buffers InsertEvent calls and flushes them as UNLOGGED BATCHes,
// reducing round-trips to Scylla at the cost of a short write-delay window.
// InsertFlagged is unbatched — abuse hits are rare and should land promptly.
type BatchStore struct {
	session       *gocql.Session
	l             *slog.Logger
	maxSize       int
	flushInterval time.Duration
	flushWorkers  int

	closeMu sync.RWMutex
	closed  bool
	shardRR uint64
	shards  []batchBuffer

	flushCh chan []models.FirehoseEvent
	stopCh  chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
}

type batchBuffer struct {
	mu  sync.Mutex
	buf []models.FirehoseEvent
}

var errBatchStoreClosed = errors.New("batch store is closed")

// NewBatched connects to Scylla, applies the schema, and starts a background
// flush goroutine. Close must be called to drain remaining events and release
// the session.
func NewBatched(ctx context.Context, cfg Config, bcfg BatchConfig) (*BatchStore, error) {
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
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

	maxSize := bcfg.MaxSize
	if maxSize <= 0 {
		maxSize = 100
	}
	flushInterval := bcfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 50 * time.Millisecond
	}
	flushWorkers := bcfg.FlushWorkers
	if flushWorkers <= 0 {
		flushWorkers = 4
	}
	flushQueueSize := bcfg.FlushQueueSize
	if flushQueueSize <= 0 {
		flushQueueSize = flushWorkers * 4
		if flushQueueSize < 16 {
			flushQueueSize = 16
		}
	}
	bufferShards := bcfg.BufferShards
	if bufferShards <= 0 {
		bufferShards = flushWorkers * 2
		if bufferShards < 8 {
			bufferShards = 8
		}
	}

	bs := &BatchStore{
		session:       session,
		l:             l,
		maxSize:       maxSize,
		flushInterval: flushInterval,
		flushWorkers:  flushWorkers,
		shards:        make([]batchBuffer, bufferShards),
		flushCh:       make(chan []models.FirehoseEvent, flushQueueSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	for i := range bs.shards {
		bs.shards[i].buf = make([]models.FirehoseEvent, 0, maxSize)
	}

	for i := 0; i < bs.flushWorkers; i++ {
		bs.wg.Add(1)
		go bs.flushWorker()
	}
	go bs.flusher()

	return bs, nil
}

// InsertEvent buffers the event and flushes immediately if the buffer is full.
// The caller's context is intentionally not forwarded to the flush: a shutdown
// signal must not abort a flush that's already holding buffered data.
func (s *BatchStore) InsertEvent(_ context.Context, e models.FirehoseEvent) error {
	s.closeMu.RLock()
	if s.closed {
		s.closeMu.RUnlock()
		return errBatchStoreClosed
	}
	idx := int(atomic.AddUint64(&s.shardRR, 1)-1) % len(s.shards)
	events := s.appendToShard(idx, e)
	if err := s.enqueue(events); err != nil {
		s.closeMu.RUnlock()
		return err
	}
	s.closeMu.RUnlock()
	return nil
}

func (s *BatchStore) appendToShard(idx int, e models.FirehoseEvent) []models.FirehoseEvent {
	shard := &s.shards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.buf = append(shard.buf, e)
	if len(shard.buf) < s.maxSize {
		return nil
	}
	events := shard.buf
	shard.buf = make([]models.FirehoseEvent, 0, s.maxSize)
	return events
}

// InsertFlagged writes a flagged-event record immediately without buffering.
func (s *BatchStore) InsertFlagged(ctx context.Context, r storage.FlaggedRecord) error {
	bucketHour := r.ReceivedAt.Truncate(time.Hour)
	if err := s.session.Query(insertFlaggedStmt,
		bucketHour, r.ReceivedAt, r.EventID, r.RuleID, r.DID, string(r.Kind), r.Text, r.Severity, r.Reason, r.Evidence,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("insert flagged_events: %w", err)
	}
	return nil
}

// Close stops the background flusher, drains any remaining buffered events,
// and closes the underlying gocql session.
func (s *BatchStore) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stopCh)
	s.closeMu.Unlock()
	<-s.doneCh

	if err := s.flushAllShards(); err != nil {
		s.l.Warn("final batch flush on close", "error", err)
	}

	close(s.flushCh)
	s.wg.Wait()

	if s.session != nil {
		s.session.Close()
	}
	return nil
}

func (s *BatchStore) flusher() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.flushAllShards(); err != nil {
				s.l.Warn("interval batch flush", "error", err)
			}
		}
	}
}

func (s *BatchStore) flushWorker() {
	defer s.wg.Done()
	for events := range s.flushCh {
		if err := s.executeBatch(events); err != nil {
			s.l.Warn("execute event batch", "events", len(events), "error", err)
		}
	}
}

// flushAllShards swaps out shard buffers and enqueues non-empty batches.
func (s *BatchStore) flushAllShards() error {
	for idx := range s.shards {
		if err := s.enqueue(s.swapShardBuffer(idx)); err != nil {
			return err
		}
	}
	return nil
}

func (s *BatchStore) swapShardBuffer(idx int) []models.FirehoseEvent {
	shard := &s.shards[idx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if len(shard.buf) == 0 {
		return nil
	}
	events := shard.buf
	shard.buf = make([]models.FirehoseEvent, 0, s.maxSize)
	return events
}

func (s *BatchStore) enqueue(events []models.FirehoseEvent) error {
	if len(events) == 0 {
		return nil
	}
	s.flushCh <- events
	return nil
}

func (s *BatchStore) executeBatch(events []models.FirehoseEvent) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	for _, e := range events {
		bucketMinute := e.ReceivedAt.Truncate(time.Minute)
		batch.Query(insertEventByDIDStmt,
			e.DID, e.ReceivedAt, e.ID, string(e.Kind), e.Text, e.Langs, e.Links, e.CreatedAt)
		batch.Query(insertEventByMinuteStmt,
			string(e.Kind), bucketMinute, e.ReceivedAt, e.ID, e.DID, e.Text, e.Langs, e.Links, e.CreatedAt)
	}

	if err := s.session.ExecuteBatch(batch.WithContext(context.Background())); err != nil {
		return fmt.Errorf("execute batch (%d events): %w", len(events), err)
	}
	return nil
}
