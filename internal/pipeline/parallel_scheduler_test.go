package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// recordingHandler records (workerOrder, event) pairs so tests can assert that
// events with the same DID land on the same worker and arrive in order.
type recordingHandler struct {
	mu       sync.Mutex
	byDID    map[string][]string // did -> list of event IDs in handle order
	gateOnce sync.Once
	gate     chan struct{} // optional: blocks Handle until released
	useGate  bool
	err      error
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{byDID: make(map[string][]string)}
}

func (h *recordingHandler) Handle(_ context.Context, evt models.FirehoseEvent) error {
	if h.useGate {
		<-h.gate
	}
	if h.err != nil {
		return h.err
	}
	h.mu.Lock()
	h.byDID[evt.DID] = append(h.byDID[evt.DID], evt.ID)
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) eventsFor(did string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.byDID[did]))
	copy(out, h.byDID[did])
	return out
}

func newScheduler(t *testing.T, mode config.BackpressureMode, workers, buffer int, h Handler) *ParallelScheduler {
	t.Helper()
	cfg := &config.Config{
		WorkerCount:      workers,
		ChannelBuffer:    buffer,
		BackpressureMode: mode,
	}
	return NewParallelScheduler(cfg, h)
}

func TestParallelScheduler_PerDIDAffinity(t *testing.T) {
	// Two events with the same DID should always hash to the same worker.
	s := newScheduler(t, config.ModeBlock, 8, 64, newRecordingHandler())
	for _, did := range []string{
		"did:plc:abc",
		"did:plc:def",
		"did:plc:ghi",
		"did:plc:jkl",
		"did:plc:mno",
	} {
		first := s.workerFor(did)
		for i := 0; i < 32; i++ {
			require.Equal(t, first, s.workerFor(did), "did %s should always map to the same worker", did)
		}
	}
}

func TestParallelScheduler_PreservesPerDIDOrder(t *testing.T) {
	h := newRecordingHandler()
	s := newScheduler(t, config.ModeBlock, 4, 64, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Send 50 events for the same DID; the scheduler must process them in
	// arrival order because they all land on the same worker.
	const did = "did:plc:ordered"
	const n = 50
	for i := 0; i < n; i++ {
		evt := models.FirehoseEvent{ID: fmt.Sprintf("evt-%03d", i), DID: did}
		require.NoError(t, s.AddWork(ctx, did, evt))
	}

	shutdownCtx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	require.NoError(t, s.Shutdown(shutdownCtx))

	got := h.eventsFor(did)
	require.Len(t, got, n)
	for i, id := range got {
		require.Equal(t, fmt.Sprintf("evt-%03d", i), id, "event %d out of order", i)
	}
}

func TestParallelScheduler_DropMode(t *testing.T) {
	// Single worker, single-slot buffer, gated handler so events pile up:
	// the second AddWork fills the slot, the third must be dropped.
	h := newRecordingHandler()
	h.useGate = true
	h.gate = make(chan struct{})

	s := newScheduler(t, config.ModeDrop, 1, 1, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	const did = "did:plc:hot"
	// 1st: handler picks it up immediately (then blocks on gate).
	require.NoError(t, s.AddWork(ctx, did, models.FirehoseEvent{ID: "evt-0", DID: did}))
	// 2nd: sits in the worker's 1-slot buffer.
	// Wait briefly to ensure the first is being handled (gated).
	require.Eventually(t, func() bool {
		// Give the worker time to pull the first event off the channel.
		return len(s.workers[s.workerFor(did)]) == 0
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, s.AddWork(ctx, did, models.FirehoseEvent{ID: "evt-1", DID: did}))
	// 3rd: buffer full, handler still gated → drop.
	err := s.AddWork(ctx, did, models.FirehoseEvent{ID: "evt-2", DID: did})
	require.ErrorIs(t, err, ErrDropped)
	require.Equal(t, uint64(1), s.Stats().Dropped)

	// Release the handler and shut down so we don't leak goroutines.
	close(h.gate)
	shutdownCtx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	require.NoError(t, s.Shutdown(shutdownCtx))
}

func TestParallelScheduler_BlockModeRespectsCtx(t *testing.T) {
	// Single worker, single-slot, gated handler. After filling the buffer,
	// the next AddWork should block until ctx is canceled.
	h := newRecordingHandler()
	h.useGate = true
	h.gate = make(chan struct{})

	s := newScheduler(t, config.ModeBlock, 1, 1, h)
	bg := context.Background()
	s.Start(bg)

	const did = "did:plc:slow"
	require.NoError(t, s.AddWork(bg, did, models.FirehoseEvent{ID: "evt-0", DID: did}))
	require.Eventually(t, func() bool {
		return len(s.workers[s.workerFor(did)]) == 0
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, s.AddWork(bg, did, models.FirehoseEvent{ID: "evt-1", DID: did}))

	addCtx, addCancel := context.WithCancel(bg)
	addErrCh := make(chan error, 1)
	go func() {
		addErrCh <- s.AddWork(addCtx, did, models.FirehoseEvent{ID: "evt-2", DID: did})
	}()

	// Confirm it's actually blocked.
	select {
	case err := <-addErrCh:
		t.Fatalf("AddWork returned early in block mode: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	addCancel()
	select {
	case err := <-addErrCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("AddWork did not unblock on ctx cancel")
	}

	close(h.gate)
	shutdownCtx, scancel := context.WithTimeout(bg, 2*time.Second)
	defer scancel()
	require.NoError(t, s.Shutdown(shutdownCtx))
}

func TestParallelScheduler_Stats(t *testing.T) {
	// Mix of successful and erroring handles; counters must reflect both.
	var processed, errored atomic.Uint64
	h := handlerFunc(func(_ context.Context, evt models.FirehoseEvent) error {
		if evt.ID == "fail" {
			errored.Add(1)
			return errors.New("boom")
		}
		processed.Add(1)
		return nil
	})

	s := newScheduler(t, config.ModeBlock, 4, 32, h)
	ctx := context.Background()
	s.Start(ctx)

	for i := 0; i < 10; i++ {
		require.NoError(t, s.AddWork(ctx, "a", models.FirehoseEvent{ID: "ok", DID: "a"}))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, s.AddWork(ctx, "b", models.FirehoseEvent{ID: "fail", DID: "b"}))
	}

	shutdownCtx, scancel := context.WithTimeout(ctx, 2*time.Second)
	defer scancel()
	require.NoError(t, s.Shutdown(shutdownCtx))

	stats := s.Stats()
	require.Equal(t, uint64(10), stats.Processed)
	require.Equal(t, uint64(3), stats.Errors)
	require.Equal(t, uint64(0), stats.Dropped)
}

// handlerFunc adapts a plain function to the Handler interface.
type handlerFunc func(ctx context.Context, evt models.FirehoseEvent) error

func (f handlerFunc) Handle(ctx context.Context, evt models.FirehoseEvent) error {
	return f(ctx, evt)
}
