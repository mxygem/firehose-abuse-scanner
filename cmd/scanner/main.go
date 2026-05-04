package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/firehose"
	"github.com/mxygem/firehose-abuse-scanner/internal/pipeline"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/scylla"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	l := slog.Default()
	l.Info("starting firehose abuse scanner")

	cfg := config.MustLoad(os.Getenv("ENV"))
	l.Info("config", "config", cfg)

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
		firehose.WithDuration(cfg.SimulatorDuration),
	)
	l.Info("firehose client ready", "source", client.Name())

	store, err := scylla.NewBatched(ctx, scylla.Config{
		Hosts:       cfg.ScyllaHosts,
		Keyspace:    cfg.ScyllaKeyspace,
		Consistency: cfg.ScyllaConsistency,
		Timeout:     time.Duration(cfg.ScyllaTimeoutMS) * time.Millisecond,
		NumConns:    cfg.ScyllaNumConns,
	}, scylla.BatchConfig{
		MaxSize:        cfg.ScyllaBatchSize,
		FlushInterval:  time.Duration(cfg.ScyllaBatchFlushMS) * time.Millisecond,
		FlushWorkers:   cfg.ScyllaBatchFlushWorkers,
		FlushQueueSize: cfg.ScyllaBatchQueueSize,
		BufferShards:   cfg.ScyllaBatchShards,
	})
	if err != nil {
		l.Error("creating scylla store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	src, err := client.Subscribe(ctx)
	if err != nil {
		l.Error("subscribing to firehose", "error", err)
		os.Exit(1)
	}

	p := pipeline.New(cfg, pipeline.NewScyllaHandler(store))
	if err := p.Run(ctx, src); err != nil && err != context.Canceled {
		l.Error("pipeline exited with error", "error", err)
		os.Exit(1)
	}

	l.Info("pipeline completed successfully", "stats", p.Snapshot())
}
