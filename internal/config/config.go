package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type BackpressureMode string

const (
	ModeBlock BackpressureMode = "block"
	ModeDrop  BackpressureMode = "drop"
)

type Config struct {
	// Pipeline
	WorkerCount      int
	ChannelBuffer    int
	BackpressureMode BackpressureMode

	// Scylla
	ScyllaHosts             []string
	ScyllaKeyspace          string
	ScyllaConsistency       string
	ScyllaTimeoutMS         int
	ScyllaNumConns          int
	ScyllaBatchSize         int
	ScyllaBatchFlushMS      int
	ScyllaBatchFlushWorkers int
	ScyllaBatchQueueSize    int
	ScyllaBatchShards       int

	// Observability
	MetricsAddr string

	// Testing/Simulation
	EventsPerSecond      int
	BurstMultiplier      float64
	BurstDuration        int
	SimulatorConcurrency int
	SimulatorDuration    time.Duration
}

// MustLoad reads config.json then layers config.<env>.json on top. The env
// argument typically comes from os.Getenv("ENV"); empty falls back to "dev".
// ENV is the only environment variable consulted — every other knob lives in
// the JSON files so that runs are reproducible from a checked-in config.
func MustLoad(env string) *Config {
	l := slog.Default()
	k := koanf.New(".")

	if err := k.Load(file.Provider("config.json"), json.Parser()); err != nil {
		panic(fmt.Errorf("load config.json: %v", err))
	}

	envName := strings.ToLower(env)
	if envName == "" {
		envName = "dev"
	}

	envFile := fmt.Sprintf("config.%s.json", envName)
	if _, err := os.Stat(envFile); err != nil {
		panic(fmt.Errorf("config file %s not found (set ENV to one of the config.<env>.json files)", envFile))
	}
	if err := k.Load(file.Provider(envFile), json.Parser()); err != nil {
		panic(fmt.Errorf("load %s: %v", envFile, err))
	}

	cfg := &Config{
		WorkerCount:             k.Int("worker_count"),
		ChannelBuffer:           k.Int("channel_buffer"),
		EventsPerSecond:         k.Int("events_per_second"),
		BackpressureMode:        BackpressureMode(k.String("backpressure_mode")),
		MetricsAddr:             k.String("metrics_addr"),
		SimulatorConcurrency:    k.Int("simulator_concurrency"),
		SimulatorDuration:       k.Duration("simulator_duration"),
		BurstMultiplier:         k.Float64("burst_multiplier"),
		BurstDuration:           k.Int("burst_duration"),
		ScyllaHosts:             k.Strings("scylla_hosts"),
		ScyllaKeyspace:          k.String("scylla_keyspace"),
		ScyllaConsistency:       k.String("scylla_consistency"),
		ScyllaTimeoutMS:         k.Int("scylla_timeout_ms"),
		ScyllaNumConns:          k.Int("scylla_num_conns"),
		ScyllaBatchSize:         k.Int("scylla_batch_size"),
		ScyllaBatchFlushMS:      k.Int("scylla_batch_flush_ms"),
		ScyllaBatchFlushWorkers: k.Int("scylla_batch_flush_workers"),
		ScyllaBatchQueueSize:    k.Int("scylla_batch_queue_size"),
		ScyllaBatchShards:       k.Int("scylla_batch_shards"),
	}

	if cfg.BackpressureMode != ModeBlock && cfg.BackpressureMode != ModeDrop {
		panic(fmt.Errorf("invalid backpressure_mode: %q (want %q or %q)", cfg.BackpressureMode, ModeBlock, ModeDrop))
	}

	l.Info("config loaded", "env", envName, "config", cfg)
	return cfg
}
