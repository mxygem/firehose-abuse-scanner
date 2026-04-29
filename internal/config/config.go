package config

import (
	"fmt"
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
	EventsPerSecond  int
	BackpressureMode BackpressureMode

	// Storage (populated in later release)
	PostgresDSN string
	RedisAddr   string

	// Observability
	MetricsAddr string
}

func MustLoad(env string) *Config {
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
		WorkerCount:      k.Int("worker_count"),
		ChannelBuffer:    k.Int("channel_buffer"),
		EventsPerSecond:  k.Int("events_per_second"),
		BackpressureMode: BackpressureMode(k.String("backpressure_mode")),
		PostgresDSN:      k.String("postgres_dsn"),
		RedisAddr:        k.String("redis_addr"),
		MetricsAddr:      k.String("metrics_addr"),
	}

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

	return cfg
}
