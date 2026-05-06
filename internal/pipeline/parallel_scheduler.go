package pipeline

import (
	"context"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/metrics"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// ParallelScheduler runs a fixed pool of workers, each owning a private channel.
// A given DID always hashes to the same worker, so a single account's events
// are processed in arrival order — the property stateful detectors will rely
// on (sliding-window rate limits, near-duplicate dedup, etc.).
//
// This is the simpler cousin of indigo's events/schedulers/parallel: that one
// uses an active-set map so any worker can pick up any DID (better load
// balance under skew) but never two workers concurrently. Hash-mod is enough
// for our cardinality and avoids the shared map.
type ParallelScheduler struct {
	handler          Handler
	backpressureMode config.BackpressureMode
	workers          []chan models.FirehoseEvent
	seed             maphash.Seed

	stats SchedulerStats

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Compile-time check.
var _ Scheduler = (*ParallelScheduler)(nil)

// NewParallelScheduler builds a scheduler sized from cfg. ChannelBuffer is
// divided across WorkerCount per-worker channels; if the result is < 1 each
// worker gets a single slot.
func NewParallelScheduler(cfg *config.Config, handler Handler) *ParallelScheduler {
	n := cfg.WorkerCount
	if n < 1 {
		n = 1
	}
	perWorker := cfg.ChannelBuffer / n
	if perWorker < 1 {
		perWorker = 1
	}
	s := &ParallelScheduler{
		handler:          handler,
		backpressureMode: cfg.BackpressureMode,
		workers:          make([]chan models.FirehoseEvent, n),
		seed:             maphash.MakeSeed(),
	}
	for i := range s.workers {
		s.workers[i] = make(chan models.FirehoseEvent, perWorker)
	}
	return s
}

// Start spins up one goroutine per worker channel. The provided context is
// passed to handler.Handle; workers themselves keep running until their
// channel is closed by Shutdown so already-buffered events still get drained.
func (s *ParallelScheduler) Start(ctx context.Context) {
	for i := range s.workers {
		s.wg.Add(1)
		go s.runWorker(ctx, i)
	}
}

// workerFor maps a DID to the index of the worker that owns it.
func (s *ParallelScheduler) workerFor(did string) int {
	var h maphash.Hash
	h.SetSeed(s.seed)
	h.WriteString(did)
	return int(h.Sum64() % uint64(len(s.workers)))
}

func (s *ParallelScheduler) AddWork(ctx context.Context, did string, evt models.FirehoseEvent) error {
	idx := s.workerFor(did)
	switch s.backpressureMode {
	case config.ModeDrop:
		select {
		case s.workers[idx] <- evt:
			return nil
		default:
			atomic.AddUint64(&s.stats.Dropped, 1)
			metrics.EventsDropped.Inc()
			return ErrDropped
		}
	default:
		// ModeBlock and unset both block on the send.
		select {
		case s.workers[idx] <- evt:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Shutdown closes every worker channel and waits for the worker goroutines to
// finish draining. ctx bounds the wait — if it expires, returns ctx.Err().
func (s *ParallelScheduler) Shutdown(ctx context.Context) error {
	s.closeOnce.Do(func() {
		for _, w := range s.workers {
			close(w)
		}
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ParallelScheduler) Stats() SchedulerStats {
	return SchedulerStats{
		Processed: atomic.LoadUint64(&s.stats.Processed),
		Dropped:   atomic.LoadUint64(&s.stats.Dropped),
		Errors:    atomic.LoadUint64(&s.stats.Errors),
	}
}

func (s *ParallelScheduler) QueueDepth() int {
	total := 0
	for _, w := range s.workers {
		total += len(w)
	}
	return total
}

func (s *ParallelScheduler) QueueCapacity() int {
	total := 0
	for _, w := range s.workers {
		total += cap(w)
	}
	return total
}

func (s *ParallelScheduler) runWorker(ctx context.Context, idx int) {
	defer s.wg.Done()
	l := slog.Default()
	for evt := range s.workers[idx] {
		start := time.Now()
		err := s.handler.Handle(ctx, evt)
		metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			if ctx.Err() != nil {
				// Context canceled at shutdown; stop quietly.
				return
			}
			atomic.AddUint64(&s.stats.Errors, 1)
			metrics.EventErrors.Inc()
			l.Warn("handling event", "event_id", evt.ID, "error", err)
			continue
		}
		atomic.AddUint64(&s.stats.Processed, 1)
		metrics.EventsProcessed.Inc()
	}
}
