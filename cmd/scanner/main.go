package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/detect"
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

	detectors, err := buildDetectors(cfg)
	if err != nil {
		l.Error("building detectors", "error", err)
		os.Exit(1)
	}
	l.Info("detectors ready", "count", len(detectors))

	scheduler := pipeline.NewParallelScheduler(cfg, pipeline.NewCompositeHandler(store, detectors...))
	p := pipeline.New(cfg, scheduler)

	runStart := time.Now()
	runErr := p.Run(ctx, src)
	elapsed := time.Since(runStart)
	if runErr != nil && runErr != context.Canceled {
		l.Error("pipeline exited with error", "error", runErr)
		os.Exit(1)
	}

	printBenchmarkSummary(p.Snapshot(), scheduler.LatencySnapshot(), store.Stats(), elapsed)
}

// printBenchmarkSummary prints a single end-of-run block to stderr that
// reconciles the pipeline counters with what actually landed in Scylla, plus
// handler-latency percentiles. This is the source of truth for "how fast did
// it really go" — scanner_events_processed_total is buffered InsertEvent
// returns, not durable writes.
func printBenchmarkSummary(p pipeline.Stats, lat pipeline.LatencySummary, b scylla.BatchStats, elapsed time.Duration) {
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	dropPct := 0.0
	if p.Received > 0 {
		dropPct = 100 * float64(p.Dropped) / float64(p.Received)
	}
	totalAttempted := b.FlushedEvents + b.FailedEvents
	failPct := 0.0
	if totalAttempted > 0 {
		failPct = 100 * float64(b.FailedEvents) / float64(totalAttempted)
	}

	out := os.Stderr
	fmt.Fprintln(out)
	fmt.Fprintln(out, "──────────────── benchmark summary ────────────────")
	fmt.Fprintf(out, "  duration         %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintln(out, "  pipeline:")
	fmt.Fprintf(out, "    received       %d (%.0f/s)\n", p.Received, float64(p.Received)/secs)
	fmt.Fprintf(out, "    processed      %d (%.0f/s)\n", p.Processed, float64(p.Processed)/secs)
	fmt.Fprintf(out, "    dropped        %d (%.1f%%)\n", p.Dropped, dropPct)
	fmt.Fprintf(out, "    handler errors %d\n", p.Errors)
	fmt.Fprintln(out, "  scylla writes (truth):")
	fmt.Fprintf(out, "    flushed events %d (%.0f/s)\n", b.FlushedEvents, float64(b.FlushedEvents)/secs)
	fmt.Fprintf(out, "    failed events  %d (%.1f%% of attempts)\n", b.FailedEvents, failPct)
	fmt.Fprintf(out, "    flushed batches %d, failed batches %d\n", b.FlushedBatches, b.FailedBatches)
	fmt.Fprintln(out, "  handler latency:")
	fmt.Fprintf(out, "    p50  %s\n", usToDuration(lat.P50))
	fmt.Fprintf(out, "    p95  %s\n", usToDuration(lat.P95))
	fmt.Fprintf(out, "    p99  %s\n", usToDuration(lat.P99))
	fmt.Fprintf(out, "    p99.9 %s\n", usToDuration(lat.P999))
	fmt.Fprintf(out, "    max  %s\n", usToDuration(lat.Max))
	fmt.Fprintln(out, "───────────────────────────────────────────────────")
}

func usToDuration(us int64) time.Duration {
	return time.Duration(us) * time.Microsecond
}

// buildDetectors assembles the detector chain in a deterministic order:
// cheap content checks first (keyword, blocklist), then stateful rate
// detection. A rule with no configured input (e.g. empty keyword list) is
// skipped so a feature can be turned off purely via config.
func buildDetectors(cfg *config.Config) ([]detect.Detector, error) {
	var ds []detect.Detector

	if len(cfg.SpamKeywords) > 0 || len(cfg.SpamRegexes) > 0 {
		kw, err := detect.NewKeywordRule("spam", cfg.SpamKeywords, cfg.SpamRegexes, cfg.SpamSeverity)
		if err != nil {
			return nil, fmt.Errorf("keyword rule: %w", err)
		}
		ds = append(ds, kw)
	}
	if len(cfg.BlocklistDomains) > 0 {
		ds = append(ds, detect.NewBlocklistRule("blocklist", cfg.BlocklistDomains, cfg.BlocklistSeverity))
	}
	if cfg.RateThreshold > 0 && cfg.RateWindowMS > 0 {
		window := time.Duration(cfg.RateWindowMS) * time.Millisecond
		ds = append(ds, detect.NewRateRule(window, cfg.RateThreshold, cfg.RateMaxDIDs, cfg.RateSeverity))
	}
	return ds, nil
}
