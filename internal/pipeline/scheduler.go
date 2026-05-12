package pipeline

import (
	"context"
	"errors"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// Scheduler owns the worker pool and dispatches events to handlers. It is the
// seam between "receive a firehose event" (the pipeline's job) and "do work
// for that event" (the handler's job).
//
// Implementations are responsible for:
//   - Worker lifecycle (Start spins workers, Shutdown drains and stops them).
//   - Per-DID work affinity, so events for the same author hit the same worker
//     in arrival order. Stateful detectors (rate limits, dedup) rely on this.
//   - Honouring the configured backpressure mode in AddWork.
type Scheduler interface {
	// Start spins up workers. Must be called before AddWork.
	Start(ctx context.Context)

	// AddWork dispatches an event to the worker that owns this DID. In drop
	// mode it returns ErrDropped when the worker is at capacity; in block
	// mode it blocks until a slot opens or ctx is canceled.
	AddWork(ctx context.Context, did string, evt models.FirehoseEvent) error

	// Shutdown closes worker channels and waits for in-flight work to drain.
	// Returns ctx.Err() if the wait is interrupted.
	Shutdown(ctx context.Context) error

	// Stats returns a point-in-time snapshot of work the scheduler has done.
	Stats() SchedulerStats

	// QueueDepth is the total number of events currently buffered across
	// every worker channel. Used by the pipeline's stats reporter.
	QueueDepth() int

	// QueueCapacity is the total buffer capacity across every worker channel.
	QueueCapacity() int
}

// SchedulerStats holds the counters a Scheduler maintains. Received lives on
// the Pipeline (it's measured at source-read time, before scheduling).
type SchedulerStats struct {
	Processed uint64
	Dropped   uint64
	Errors    uint64
}

// LatencySummary captures handler-latency percentiles in microseconds. It's
// produced by schedulers that maintain per-worker histograms and consumed by
// the end-of-run benchmark printer.
type LatencySummary struct {
	Count int64
	P50   int64
	P95   int64
	P99   int64
	P999  int64
	Max   int64
}

// ErrDropped is returned by AddWork in drop mode when the target worker has no
// capacity. The scheduler has already incremented its dropped counter, so the
// caller can treat this as a no-op.
var ErrDropped = errors.New("scheduler: event dropped, worker at capacity")
