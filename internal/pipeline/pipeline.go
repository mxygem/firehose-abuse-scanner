package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/metrics"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// Handler runs the per-event work (typically: persist to storage, run
// detectors). One Handler instance is shared across all scheduler workers, so
// implementations must be safe for concurrent use.
type Handler interface {
	Handle(ctx context.Context, event models.FirehoseEvent) error
}

// Stats is a point-in-time snapshot of the pipeline's counters. Received is
// owned by the pipeline (counted at source-read time); the rest come from the
// scheduler.
type Stats struct {
	Received  uint64
	Processed uint64
	Dropped   uint64
	Errors    uint64
}

// Pipeline reads events off a firehose source and dispatches them to a
// Scheduler. The scheduler owns the worker pool, per-DID affinity, and
// backpressure; the pipeline is just the source loop plus the stats reporter.
type Pipeline struct {
	cfg       *config.Config
	scheduler Scheduler

	received uint64
}

// DefaultSourceConcurrency is the number of goroutines that read from the
// firehose src channel and feed the scheduler. More goroutines eliminate the
// single-consumer bottleneck that caps throughput at ~300-500K events/sec.
const DefaultSourceConcurrency = 8

func New(cfg *config.Config, scheduler Scheduler) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		scheduler: scheduler,
	}
}

// Run starts the scheduler's workers, then fans events from src into
// scheduler.AddWork until ctx is canceled or src closes. On exit it shuts the
// scheduler down with a bounded grace period so already-buffered work drains.
func (p *Pipeline) Run(ctx context.Context, src <-chan models.FirehoseEvent) error {
	p.scheduler.Start(ctx)
	metrics.QueueCapacity.Set(float64(p.scheduler.QueueCapacity()))

	statsCtx, stopStats := context.WithCancel(ctx)
	defer stopStats()
	go p.reportStats(statsCtx)

	srcErr := p.runSource(ctx, src)

	stopStats()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.scheduler.Shutdown(shutdownCtx); err != nil {
		slog.Default().Warn("scheduler shutdown", "error", err)
	}

	return srcErr
}

// runSource starts DefaultSourceConcurrency goroutines to read from src and
// dispatch to the scheduler. Multiple readers eliminate the single-consumer
// channel bottleneck — at 4 readers the per-reader throughput ceiling is
// ~2.5M/sec, enough for the >1M target.
func (p *Pipeline) runSource(ctx context.Context, src <-chan models.FirehoseEvent) error {
	var wg sync.WaitGroup

	for i := 0; i < DefaultSourceConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case evt, ok := <-src:
					if !ok {
						return
					}
					atomic.AddUint64(&p.received, 1)
					if err := p.scheduler.AddWork(ctx, evt.DID, evt); err != nil {
						if errors.Is(err, ErrDropped) {
							continue
						}
						if ctx.Err() != nil {
							return
						}
						slog.Default().Warn("scheduler add work", "event_id", evt.ID, "error", err)
					}
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

// reportStats logs pipeline throughput every 5 seconds.
func (p *Pipeline) reportStats(ctx context.Context) {
	l := slog.Default()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastReceived, lastProcessed, lastDropped uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := p.Snapshot()

			depth := p.scheduler.QueueDepth()
			capacity := p.scheduler.QueueCapacity()
			metrics.QueueDepth.Set(float64(depth))
			if capacity > 0 {
				metrics.QueueSaturation.Set(float64(depth) / float64(capacity))
			}

			metrics.EventsReceived.Add(float64(snap.Received - lastReceived))
			metrics.EventsProcessed.Add(float64(snap.Processed - lastProcessed))
			metrics.EventsDropped.Add(float64(snap.Dropped - lastDropped))

			deltaRec := snap.Received - lastReceived
			deltaProc := snap.Processed - lastProcessed
			deltaDrop := snap.Dropped - lastDropped

			l.Info("pipeline stats",
				"recv/sec", deltaRec/5,
				"proc/sec", deltaProc/5,
				"drop/sec", deltaDrop/5,
				"total_recv", snap.Received,
				"total_proc", snap.Processed,
				"total_drop", snap.Dropped,
				"total_errors", snap.Errors,
				"queue_depth", depth,
				"queue_cap", capacity,
			)

			lastReceived = snap.Received
			lastProcessed = snap.Processed
			lastDropped = snap.Dropped
		}
	}
}

// Snapshot returns a point-in-time copy of the pipeline's counters, merging
// pipeline-side Received with scheduler-side Processed/Dropped/Errors.
func (p *Pipeline) Snapshot() Stats {
	s := p.scheduler.Stats()
	return Stats{
		Received:  atomic.LoadUint64(&p.received),
		Processed: s.Processed,
		Dropped:   s.Dropped,
		Errors:    s.Errors,
	}
}
