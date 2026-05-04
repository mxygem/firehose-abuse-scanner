package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

type Handler interface {
	Handle(ctx context.Context, event models.FirehoseEvent) error
}

// stats holds live pipeline counters. All fields are updated atomically.
type Stats struct {
	Received  uint64
	Processed uint64
	Dropped   uint64
	Errors    uint64
}

type Pipeline struct {
	cfg     *config.Config
	handler Handler
	stats   Stats

	work chan models.FirehoseEvent
	wg   sync.WaitGroup
}

func New(cfg *config.Config, handler Handler) *Pipeline {
	return &Pipeline{
		cfg:     cfg,
		handler: handler,
		work:    make(chan models.FirehoseEvent, cfg.ChannelBuffer),
	}
}

func (p *Pipeline) Run(ctx context.Context, src <-chan models.FirehoseEvent) error {
	// Spin up the worker pool.
	for range p.cfg.WorkerCount {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	// Start the stats reporter.
	go p.reportStats(ctx)

	// Fan events from the firehose channel into the work queue.
	for {
		select {
		case <-ctx.Done():
			// Stop accepting new events, drain the work queue, then shut down workers.
			close(p.work)
			p.wg.Wait()
			return ctx.Err()
		case evt, ok := <-src:
			if !ok {
				close(p.work)
				p.wg.Wait()
				return nil
			}
			atomic.AddUint64(&p.stats.Received, 1)
			p.enqueue(evt)
		}
	}
}

func (p *Pipeline) enqueue(evt models.FirehoseEvent) {
	switch p.cfg.BackpressureMode {
	case config.ModeDrop:
		select {
		case p.work <- evt:
		default:
			// Queue is full, drop the event and record it.
			atomic.AddUint64(&p.stats.Dropped, 1)
		}
	case config.ModeBlock:
		// Block the ingestion goroutine until a worker slot opens.
		p.work <- evt
	}
}

// worker pulls events off the queue and calls the handler.
func (p *Pipeline) worker(ctx context.Context) {
	l := slog.Default()
	defer p.wg.Done()

	for evt := range p.work {
		if err := p.handler.Handle(ctx, evt); err != nil {
			if ctx.Err() != nil {
				// Context canceled at shutdown; stop quietly.
				return
			}
			atomic.AddUint64(&p.stats.Errors, 1)
			l.Warn("handling event", "event_id", evt.ID, "error", err)
			continue
		}

		atomic.AddUint64(&p.stats.Processed, 1)
	}
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
			received := atomic.LoadUint64(&p.stats.Received)
			processed := atomic.LoadUint64(&p.stats.Processed)
			dropped := atomic.LoadUint64(&p.stats.Dropped)
			errors := atomic.LoadUint64(&p.stats.Errors)

			deltaRec := received - lastReceived
			deltaProc := processed - lastProcessed
			deltaDrop := dropped - lastDropped

			l.Info("pipeline stats",
				"recv/sec", deltaRec/5,
				"proc/sec", deltaProc/5,
				"drop/sec", deltaDrop/5,
				"total_recv", received,
				"total_proc", processed,
				"total_drop", dropped,
				"total_errors", errors,
				"queue_depth", len(p.work),
				"queue_cap", cap(p.work),
			)

			lastReceived = received
			lastProcessed = processed
			lastDropped = dropped
		}
	}
}

// Snapshot returns a point-in-time copy of the current stats.
func (p *Pipeline) Snapshot() Stats {
	return Stats{
		Received:  atomic.LoadUint64(&p.stats.Received),
		Processed: atomic.LoadUint64(&p.stats.Processed),
		Dropped:   atomic.LoadUint64(&p.stats.Dropped),
		Errors:    atomic.LoadUint64(&p.stats.Errors),
	}
}
