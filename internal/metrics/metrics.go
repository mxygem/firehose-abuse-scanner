package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ── Pipeline throughput ───────────────────────────────────────────────────────

var (
	EventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_events_received_total",
		Help: "Total number of events received from the firehose.",
	})

	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_events_processed_total",
		Help: "Total number of events successfully processed by a worker.",
	})

	EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_events_dropped_total",
		Help: "Total number of events dropped due to queue saturation (drop mode only).",
	})

	EventErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_event_errors_total",
		Help: "Total number of handler errors.",
	})

	ProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scanner_event_processing_duration_seconds",
		Help:    "Time spent processing a single event in the handler.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 100µs → ~800ms
	})
)

// ── Queue health ──────────────────────────────────────────────────────────────

var (
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scanner_queue_depth",
		Help: "Current number of events waiting in the work channel.",
	})

	QueueCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scanner_queue_capacity",
		Help: "Maximum capacity of the work channel.",
	})

	QueueSaturation = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scanner_queue_saturation_ratio",
		Help: "Queue depth as a fraction of capacity (0–1). Alert at >0.8.",
	})
)

// ── Scylla batch writer ───────────────────────────────────────────────────────
// Honest signal of how much landed in Scylla. The pipeline's
// scanner_events_processed_total counts events that returned from the buffered
// InsertEvent (always nil) — failed flushes are invisible there.

var (
	ScyllaBatchesFlushed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_scylla_batches_flushed_total",
		Help: "Number of UNLOGGED BATCHes successfully written to Scylla, by target table.",
	}, []string{"target"})

	ScyllaBatchEventsFlushed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_scylla_batch_events_flushed_total",
		Help: "Number of event rows actually written to Scylla, by target table.",
	}, []string{"target"})

	ScyllaBatchFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_scylla_batch_failures_total",
		Help: "Number of batches that failed to write to Scylla, by target table.",
	}, []string{"target"})

	ScyllaBatchEventsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_scylla_batch_events_failed_total",
		Help: "Number of event rows lost to a failed batch, by target table.",
	}, []string{"target"})

	ScyllaBatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scanner_scylla_batch_duration_seconds",
		Help:    "End-to-end latency of one UNLOGGED BATCH execution against Scylla.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms → ~16s
	}, []string{"target"})
)

// ── ETL signal hits ───────────────────────────────────────────────────────────
// These are stubbed now and wired into the real ETL in the next milestone.

var (
	// Tier 1 — stateless signals
	SpamKeywordHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_etl_spam_keyword_hits_total",
		Help: "Number of posts matching a spam keyword, labelled by keyword.",
	}, []string{"keyword"})

	SuspiciousDomainHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_etl_suspicious_domain_hits_total",
		Help: "Number of posts linking to a suspicious domain, labelled by domain.",
	}, []string{"domain"})

	ExcessiveCapsHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_etl_excessive_caps_hits_total",
		Help: "Number of posts flagged for excessive capitalisation.",
	})

	// Tier 2 — Redis-backed signals
	RateLimitTrips = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_etl_rate_limit_trips_total",
		Help: "Number of DIDs that tripped a rate-limit window, labelled by window.",
	}, []string{"window"}) // e.g. "1m", "10m"

	MentionStormHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_etl_mention_storm_hits_total",
		Help: "Number of mention-storm detections.",
	})

	RepetitionHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scanner_etl_repetition_hits_total",
		Help: "Number of near-duplicate post detections.",
	})

	// Tier 3 — async flag-store signals
	FlaggedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scanner_etl_flagged_events_total",
		Help: "Events written to the flag store, labelled by reason.",
	}, []string{"reason"}) // e.g. "spam_keyword", "rate_limit", "mention_storm"

	FlagWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scanner_etl_flag_write_duration_seconds",
		Help:    "Latency of writing a flag record to the flag store.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms → ~1s
	})
)
