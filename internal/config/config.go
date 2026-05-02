package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

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

	// Storage (populated in later release)
	PostgresDSN string
	RedisAddr   string

	// Scylla
	ScyllaHosts       []string
	ScyllaKeyspace    string
	ScyllaConsistency string
	ScyllaTimeoutMS   int

	// Observability
	MetricsAddr string

	// Testing/Simulation
	EventsPerSecond      int
	BurstMultiplier      float64
	BurstDuration        int
	SimulatorConcurrency int
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
		WorkerCount:          k.Int("worker_count"),
		ChannelBuffer:        k.Int("channel_buffer"),
		EventsPerSecond:      k.Int("events_per_second"),
		BackpressureMode:     BackpressureMode(k.String("backpressure_mode")),
		PostgresDSN:          k.String("postgres_dsn"),
		RedisAddr:            k.String("redis_addr"),
		MetricsAddr:          k.String("metrics_addr"),
		SimulatorConcurrency: k.Int("simulator_concurrency"),
		BurstMultiplier:      k.Float64("burst_multiplier"),
		BurstDuration:        k.Int("burst_duration"),
		ScyllaHosts:          k.Strings("scylla_hosts"),
		ScyllaKeyspace:       k.String("scylla_keyspace"),
		ScyllaConsistency:    k.String("scylla_consistency"),
		ScyllaTimeoutMS:      k.Int("scylla_timeout_ms"),
	}
	l.Info("config from files", "config", cfg)

	// Override with environment variables if set
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		count, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid WORKER_COUNT: %v", err))
		}
		cfg.WorkerCount = count
	}
	if v := os.Getenv("CHANNEL_BUFFER"); v != "" {
		buffer, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid CHANNEL_BUFFER: %v", err))
		}
		cfg.ChannelBuffer = buffer
	}
	if v := os.Getenv("EVENTS_PER_SECOND"); v != "" {
		events, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid EVENTS_PER_SECOND: %v", err))
		}
		cfg.EventsPerSecond = events
	}
	if v := os.Getenv("BACKPRESSURE_MODE"); v != "" {
		mode := BackpressureMode(v)
		if mode != ModeBlock && mode != ModeDrop {
			panic(fmt.Errorf("invalid BACKPRESSURE_MODE: %s", v))
		}
		cfg.BackpressureMode = mode
	}
	if v := os.Getenv("SIMULATOR_CONCURRENCY"); v != "" {
		concurrency, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid SIMULATOR_CONCURRENCY: %v", err))
		}
		cfg.SimulatorConcurrency = concurrency
	}
	if v := os.Getenv("BURST_MULTIPLIER"); v != "" {
		multiplier, err := strconv.ParseFloat(v, 64)
		if err != nil {
			panic(fmt.Errorf("invalid BURST_MULTIPLIER: %v", err))
		}
		cfg.BurstMultiplier = multiplier
	}
	if v := os.Getenv("BURST_DURATION"); v != "" {
		duration, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid BURST_DURATION: %v", err))
		}
		cfg.BurstDuration = duration
	}

	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		if v == "" {
			panic(fmt.Errorf("POSTGRES_DSN is required"))
		}
		cfg.PostgresDSN = v
	}

	if v := os.Getenv("REDIS_ADDR"); v != "" {
		if v == "" {
			panic(fmt.Errorf("REDIS_ADDR is required"))
		}
		cfg.RedisAddr = v
	}

	if v := os.Getenv("METRICS_ADDR"); v != "" {
		if v == "" {
			panic(fmt.Errorf("METRICS_ADDR is required"))
		}
		cfg.MetricsAddr = v
	}

	if v := os.Getenv("SCYLLA_HOSTS"); v != "" {
		cfg.ScyllaHosts = strings.Split(v, ",")
	}
	if v := os.Getenv("SCYLLA_KEYSPACE"); v != "" {
		cfg.ScyllaKeyspace = v
	}
	if v := os.Getenv("SCYLLA_CONSISTENCY"); v != "" {
		cfg.ScyllaConsistency = v
	}
	if v := os.Getenv("SCYLLA_TIMEOUT_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Errorf("invalid SCYLLA_TIMEOUT_MS: %v", err))
		}
		cfg.ScyllaTimeoutMS = ms
	}

	l.Info("config from environment variables", "config", cfg)

	return cfg
}
