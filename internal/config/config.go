package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
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

	if err := k.Load(file.Provider(fmt.Sprintf("config.%s.json", envName)), json.Parser()); err != nil {
		panic(fmt.Errorf("load config.%s.json: %v", envName, err))
	}

	cfg := &Config{
		WorkerCount:             k.Int("worker_count"),
		ChannelBuffer:           k.Int("channel_buffer"),
		EventsPerSecond:         k.Int("events_per_second"),
		BackpressureMode:        BackpressureMode(k.String("backpressure_mode")),
		MetricsAddr:             k.String("metrics_addr"),
		SimulatorConcurrency:    k.Int("simulator_concurrency"),
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
	l.Info("config from files", "config", cfg)

	intEnvs := map[string]int{
		"WORKER_COUNT":      cfg.WorkerCount,
		"CHANNEL_BUFFER":    cfg.ChannelBuffer,
		"EVENTS_PER_SECOND": cfg.EventsPerSecond,
		"BURST_DURATION":    cfg.BurstDuration,
	}
	for k := range intEnvs {
		if ev := os.Getenv(k); ev != "" {
			iv, err := strconv.Atoi(ev)
			if err != nil {
				panic(fmt.Errorf("invalid %s: %v", k, err))
			}
			intEnvs[k] = iv
		}
	}

	floatEnvs := map[string]float64{
		"BURST_MULTIPLIER": cfg.BurstMultiplier,
	}
	for k := range floatEnvs {
		if ev := os.Getenv(k); ev != "" {
			fv, err := strconv.ParseFloat(ev, 64)
			if err != nil {
				panic(fmt.Errorf("invalid %s: %v", k, err))
			}
			floatEnvs[k] = fv
		}
	}

	if v := os.Getenv("BACKPRESSURE_MODE"); v != "" {
		mode := BackpressureMode(v)
		if mode != ModeBlock && mode != ModeDrop {
			panic(fmt.Errorf("invalid BACKPRESSURE_MODE: %s", v))
		}
		cfg.BackpressureMode = mode
	}

	durEnvs := map[string]time.Duration{
		"SIMULATOR_DURATION": cfg.SimulatorDuration,
	}
	for k := range durEnvs {
		if ev := os.Getenv(k); ev != "" {
			d, err := time.ParseDuration(ev)
			if err != nil {
				panic(fmt.Errorf("invalid %s: %v", k, err))
			}
			durEnvs[k] = d
		}
	}

	l.Info("config from environment variables", "config", cfg)

	return cfg
}
