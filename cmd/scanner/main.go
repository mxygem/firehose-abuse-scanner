package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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

	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
		go func() {
			l.Info("metrics server listening", "addr", cfg.MetricsAddr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				l.Error("metrics server", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
	}

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
