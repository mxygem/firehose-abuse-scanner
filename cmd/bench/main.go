package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/config"
	"github.com/mxygem/firehose-abuse-scanner/internal/detect"
	"github.com/mxygem/firehose-abuse-scanner/internal/firehose"
	"github.com/mxygem/firehose-abuse-scanner/internal/pipeline"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage/noop"
)

func main() {
	l := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(l)

	fmt.Fprintln(os.Stderr, "── pipeline throughput benchmark (noop store, direct dispatch) ──")

	cfg := &config.Config{
		WorkerCount:          200,
		ChannelBuffer:        20000,
		BackpressureMode:     config.ModeDrop,
		EventsPerSecond:      2000000,
		SimulatorConcurrency: 20,
		SimulatorDuration:    20 * time.Second,
		SpamKeywords:         []string{"BUY NOW", "free crypto", "free bitcoin", "[SPAM]"},
		SpamRegexes:          []string{"(?i)earn\\s+money\\s+fast"},
		SpamSeverity:         "medium",
		BlocklistDomains:     []string{"spam-link.xyz", "phishing.example.com"},
		BlocklistSeverity:    "high",
		RateWindowMS:         1000,
		RateThreshold:        50,
		RateMaxDIDs:          4096,
		RateSeverity:         "medium",
	}

	store := noop.New()

	detectors, err := buildDetectors(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "detectors: %v\n", err)
		os.Exit(1)
	}

	scheduler := pipeline.NewParallelScheduler(cfg, pipeline.NewCompositeHandler(store, detectors...))

	var received atomic.Uint64
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	scheduler.Start(ctx)

	runStart := time.Now()

	// Direct dispatch: simulator goroutines call s.AddWork directly,
	// bypassing the single src channel bottleneck.
	var simWg sync.WaitGroup
	eventsPerRunner := cfg.EventsPerSecond / cfg.SimulatorConcurrency // events/sec per goroutine
	const tickInterval = time.Millisecond
	perTick := eventsPerRunner / 1000 // events per ms per goroutine
	if perTick < 1 {
		perTick = 1
	}

	for i := 0; i < cfg.SimulatorConcurrency; i++ {
		simWg.Add(1)
		go func() {
			defer simWg.Done()
			ticker := time.NewTicker(tickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-ticker.C:
					for j := 0; j < perTick; j++ {
						evt := firehose.GenerateEvent(t)
						received.Add(1)
						if err := scheduler.AddWork(ctx, evt.DID, evt); err != nil {
							// drop — already counted by scheduler
						}
					}
				}
			}
		}()
	}

	simWg.Wait()

	shutdownCtx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer scancel()
	_ = scheduler.Shutdown(shutdownCtx)

	elapsed := time.Since(runStart)

	stats := scheduler.Stats()
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}

	pct := 0.0
	recv := received.Load()
	if recv > 0 {
		dropped := recv - stats.Processed
		pct = 100 * float64(dropped) / float64(recv)
	}

	fmt.Fprintf(os.Stderr, "  duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "  generated:   %d (%.0f/s)\n", recv, float64(recv)/secs)
	fmt.Fprintf(os.Stderr, "  processed:   %d (%.0f/s)\n", stats.Processed, float64(stats.Processed)/secs)
	fmt.Fprintf(os.Stderr, "  dropped:     %d (%.1f%%)\n", recv-stats.Processed, pct)
	fmt.Fprintf(os.Stderr, "  errors:      %d\n", stats.Errors)
	fmt.Fprintf(os.Stderr, "  inserted:    %d (%.0f/s)\n", store.InsertedEvents(), float64(store.InsertedEvents())/secs)
	fmt.Fprintf(os.Stderr, "────────────────────────────────────────────────\n")

	fmt.Printf("%.0f,%.0f,%.1f%%\n",
		float64(recv)/secs,
		float64(stats.Processed)/secs,
		pct)
}

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
