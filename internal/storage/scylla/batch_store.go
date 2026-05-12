package scylla

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"

	"github.com/mxygem/firehose-abuse-scanner/internal/metrics"
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
	// reduce lock contention. The same count is used for each table — events
	// are routed to a shard by hashing that table's partition key, so events
	// for the same partition always co-locate in one shard and emitted
	// batches stay partition-coalesced. This is what lets the shard-aware
	// gocql fork actually route batches to a single Scylla shard.
	BufferShards int
}

// BatchStore buffers InsertEvent calls and flushes them as UNLOGGED BATCHes,
// reducing round-trips to Scylla at the cost of a short write-delay window.
//
// Each event becomes two writes — one per query-driven view — but they are
// buffered separately and emitted as table-homogeneous batches:
//   - events_by_did: shard = hash(did) % N
//   - events_by_minute: shard = hash(kind, bucket_minute) % N
//
// Co-locating events for the same partition key in the same shard keeps each
// flushed batch partition-coalesced. Combined with token-aware routing in the
// scylladb/gocql fork this lets the driver send the batch directly to the
// owning Scylla shard, which is the dominant write-throughput lever.
//
// InsertFlagged is unbatched — abuse hits are rare and should land promptly.
type BatchStore struct {
	session       *gocql.Session
	l             *slog.Logger
	maxSize       int
	flushInterval time.Duration
	flushWorkers  int

	closeMu sync.RWMutex
	closed  bool

	seed         maphash.Seed
	didShards    []batchBuffer
	minuteShards []batchBuffer

	flushCh chan batchJob
	stopCh  chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup

	// Outcome counters. The pipeline-side handler returns nil from
	// InsertEvent (writes are buffered), so flush failures here are the
	// only honest signal of how much actually landed in Scylla.
	flushedEvents  atomic.Uint64
	flushedBatches atomic.Uint64
	failedEvents   atomic.Uint64
	failedBatches  atomic.Uint64
}

// BatchStats is a point-in-time view of what the batched writer has
// attempted and how it turned out. Use it to reconcile the pipeline's
// "processed" count against rows that actually landed in Scylla.
type BatchStats struct {
	FlushedEvents  uint64
	FlushedBatches uint64
	FailedEvents   uint64
	FailedBatches  uint64
}

type batchBuffer struct {
	mu  sync.Mutex
	buf []models.FirehoseEvent
}

// batchTarget identifies which view a batch of events belongs to.
type batchTarget int

const (
	targetByDID batchTarget = iota
	targetByMinute
)

// batchJob is one homogeneous batch ready to flush.
type batchJob struct {
	target batchTarget
	events []models.FirehoseEvent
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
		seed:          maphash.MakeSeed(),
		didShards:     make([]batchBuffer, bufferShards),
		minuteShards:  make([]batchBuffer, bufferShards),
		flushCh:       make(chan batchJob, flushQueueSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	for i := range bs.didShards {
		bs.didShards[i].buf = make([]models.FirehoseEvent, 0, maxSize)
		bs.minuteShards[i].buf = make([]models.FirehoseEvent, 0, maxSize)
	}

	for i := 0; i < bs.flushWorkers; i++ {
		bs.wg.Add(1)
		go bs.flushWorker()
	}
	go bs.flusher()

	return bs, nil
}

// InsertEvent buffers the event for both views and flushes whichever shard
// fills first. The caller's context is intentionally not forwarded to the
// flush: a shutdown signal must not abort a flush that's already holding
// buffered data.
func (s *BatchStore) InsertEvent(_ context.Context, e models.FirehoseEvent) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return errBatchStoreClosed
	}

	didIdx := s.shardForDID(e.DID)
	if events := s.appendShard(&s.didShards[didIdx], e); events != nil {
		s.enqueue(batchJob{target: targetByDID, events: events})
	}

	minuteIdx := s.shardForMinute(e.Kind, e.ReceivedAt.Truncate(time.Minute))
	if events := s.appendShard(&s.minuteShards[minuteIdx], e); events != nil {
		s.enqueue(batchJob{target: targetByMinute, events: events})
	}
	return nil
}

// shardForDID routes events_by_did writes by their partition key (DID).
// Events for the same author always land in the same shard, so an emitted
// batch's rows share a partition (or at most a small set) and the
// shard-aware driver can route the batch to one Scylla shard.
func (s *BatchStore) shardForDID(did string) int {
	var h maphash.Hash
	h.SetSeed(s.seed)
	h.WriteString(did)
	return int(h.Sum64() % uint64(len(s.didShards)))
}

// shardForMinute routes events_by_minute writes by their partition key
// (kind, bucket_minute). Same idea as shardForDID — keep partition-mates
// together so the eventually-flushed batch is partition-coalesced.
func (s *BatchStore) shardForMinute(kind models.EventKind, bucket time.Time) int {
	var h maphash.Hash
	h.SetSeed(s.seed)
	h.WriteString(string(kind))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(bucket.UnixNano()))
	_, _ = h.Write(b[:])
	return int(h.Sum64() % uint64(len(s.minuteShards)))
}

// appendShard adds e to a shard buffer. Returns the buffer's contents (and
// resets it) when the shard hits MaxSize, otherwise returns nil.
func (s *BatchStore) appendShard(shard *batchBuffer, e models.FirehoseEvent) []models.FirehoseEvent {
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
	for job := range s.flushCh {
		target := targetLabel(job.target)
		start := time.Now()
		err := s.executeBatch(job)
		metrics.ScyllaBatchDuration.WithLabelValues(target).Observe(time.Since(start).Seconds())
		n := uint64(len(job.events))
		if err != nil {
			s.failedBatches.Add(1)
			s.failedEvents.Add(n)
			metrics.ScyllaBatchFailures.WithLabelValues(target).Inc()
			metrics.ScyllaBatchEventsFailed.WithLabelValues(target).Add(float64(n))
			s.l.Warn("execute event batch", "target", target, "events", n, "error", err)
			continue
		}
		s.flushedBatches.Add(1)
		s.flushedEvents.Add(n)
		metrics.ScyllaBatchesFlushed.WithLabelValues(target).Inc()
		metrics.ScyllaBatchEventsFlushed.WithLabelValues(target).Add(float64(n))
	}
}

// Stats returns a snapshot of batch outcomes since startup. The pipeline's
// Processed counter overstates writes by the failed-events count.
func (s *BatchStore) Stats() BatchStats {
	return BatchStats{
		FlushedEvents:  s.flushedEvents.Load(),
		FlushedBatches: s.flushedBatches.Load(),
		FailedEvents:   s.failedEvents.Load(),
		FailedBatches:  s.failedBatches.Load(),
	}
}

func targetLabel(t batchTarget) string {
	switch t {
	case targetByDID:
		return "events_by_did"
	case targetByMinute:
		return "events_by_minute"
	default:
		return "unknown"
	}
}

// flushAllShards swaps every shard buffer (for both views) and enqueues
// non-empty batches.
func (s *BatchStore) flushAllShards() error {
	for i := range s.didShards {
		if events := s.swapShardBuffer(&s.didShards[i]); events != nil {
			s.enqueue(batchJob{target: targetByDID, events: events})
		}
	}
	for i := range s.minuteShards {
		if events := s.swapShardBuffer(&s.minuteShards[i]); events != nil {
			s.enqueue(batchJob{target: targetByMinute, events: events})
		}
	}
	return nil
}

func (s *BatchStore) swapShardBuffer(shard *batchBuffer) []models.FirehoseEvent {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if len(shard.buf) == 0 {
		return nil
	}
	events := shard.buf
	shard.buf = make([]models.FirehoseEvent, 0, s.maxSize)
	return events
}

func (s *BatchStore) enqueue(job batchJob) {
	if len(job.events) == 0 {
		return
	}
	s.flushCh <- job
}

// executeBatch turns one batchJob into a single UNLOGGED BATCH targeting one
// table. Keeping each batch homogeneous (one statement template, one
// partition-keyed routing decision) is what makes shard-aware routing pay
// off — a multi-statement batch would force coordinator fan-out.
func (s *BatchStore) executeBatch(job batchJob) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch)
	switch job.target {
	case targetByDID:
		for _, e := range job.events {
			batch.Query(insertEventByDIDStmt,
				e.DID, e.ReceivedAt, e.ID, string(e.Kind), e.Text, e.Langs, e.Links, e.CreatedAt)
		}
	case targetByMinute:
		for _, e := range job.events {
			bucketMinute := e.ReceivedAt.Truncate(time.Minute)
			batch.Query(insertEventByMinuteStmt,
				string(e.Kind), bucketMinute, e.ReceivedAt, e.ID, e.DID, e.Text, e.Langs, e.Links, e.CreatedAt)
		}
	default:
		return fmt.Errorf("unknown batch target: %d", job.target)
	}

	if err := s.session.ExecuteBatch(batch.WithContext(context.Background())); err != nil {
		return fmt.Errorf("execute batch (target=%d, events=%d): %w", job.target, len(job.events), err)
	}
	return nil
}
