package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/firehose"
	"github.com/mxygem/firehose-abuse-scanner/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	l := slog.Default()
	l.Info("starting firehose abuse scanner")

	cfg := config.MustLoad(os.Getenv("ENV"))

	l.Info("starting firehose abuse scanner",
		"workers", cfg.WorkerCount,
		"channel_buffer", cfg.ChannelBuffer,
		"events_per_second", cfg.EventsPerSecond,
		"backpressure_mode", cfg.BackpressureMode,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Swap NewSimulator with a real WebSocket client later.
	client := firehose.NewSimulator(
		firehose.WithEventsPerSecond(cfg.EventsPerSecond),
		firehose.WithBurstMultiplier(cfg.BurstMultiplier),
		firehose.WithBurstDuration(cfg.BurstDuration),
		firehose.WithConcurrency(cfg.SimulatorConcurrency),
	)
	l.Info("firehose client ready", "source", client.Name())

	src, err := client.Subscribe(ctx)
	if err != nil {
		l.Error("subscribining to firehose", "error", err)
		os.Exit(1)
	}

	handler := &pipeline.LogHandler{}

	p := pipeline.New(cfg, handler)
	if err := p.Run(ctx, src); err != nil && err != context.Canceled {
		l.Error("pipeline exited with error", "error", err)
		os.Exit(1)
	}

	l.Info("pipeline completed successfully", "stats", p.Snapshot())
}
