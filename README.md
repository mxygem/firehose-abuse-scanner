# firehose-abuse-scanner

A high-throughput event ingestion and analysis service that consumes Bluesky event data, running abuse detection rules against, and surfaces flagged content through an administration dashboard.

## Architecture

```
┌─────────────────┐
│  Firehose       │
│  (Simulator or  │      Configurable event rate
│  WebSocket)     │      & burst patterns
└────────┬────────┘
         │
         │ FirehoseEvent channel
         │
┌────────▼──────────────────────────────────────┐
│  Pipeline (Fan-in)                             │
│  ├─ Receive loop (single goroutine)            │
│  └─ Work queue (buffered channel)              │
│     BACKPRESSURE_MODE: drop or block           │
└────────┬──────────────────────────────────────┘
         │
         │ Work distribution
         │
    ┌────┴─────────────────────────┐
    │                              │
┌───▼──┐  ┌──────┐  ┌──────┐  ┌──▼───┐
│ W1   │  │ W2   │  │ W3   │  │ ...  │  Worker pool (configurable count)
│      │  │      │  │      │  │      │
└───┬──┘  └──┬───┘  └──┬───┘  └──┬───┘
    │       │        │        │
    │       │        │        │  Handler.Handle()
    └───────┼────────┼────────┘  (abuse detection)
            │
         Stats (atomic counters)
         ├─ Received
         ├─ Processed
         ├─ Dropped
         └─ Errors
```

## Running

Prerequisites:

* [go](https://go.dev/) - Known working with go version `1.26.2`

With defaults (50 workers, 10k channel buffer):

```bash
go run ./cmd/scanner
```

With overridden values:

```bash
BACKPRESSURE_MODE=block WORKER_COUNT=100 EVENTS_PER_SECOND=10000 go run ./cmd/scanner
```

### Sample output

```bash
{17:06}~/code/mxygem/firehose-abuse-scanner:main ✗ ➭ BACKPRESSURE_MODE=drop go run ./cmd/scanner
{"time":"2026-04-28T17:06:55.271589539-07:00","level":"INFO","msg":"starting firehose abuse scanner"}
{"time":"2026-04-28T17:06:55.272150269-07:00","level":"INFO","msg":"starting firehose abuse scanner","workers":5,"channel_buffer":1000,"events_per_second":20000,"backpressure_mode":"drop"}
{"time":"2026-04-28T17:06:55.272654995-07:00","level":"INFO","msg":"firehose client ready","source":"local-simulator"}
{"time":"2026-04-28T17:07:00.275418059-07:00","level":"INFO","msg":"pipeline stats","recv/sec":3095,"proc/sec":3095,"drop/sec":0,"total_recv":15476,"total_proc":15476,"total_drop":0,"total_errors":0,"queue_depth":0,"queue_cap":1000}
{"time":"2026-04-28T17:07:05.274909682-07:00","level":"INFO","msg":"pipeline stats","recv/sec":2577,"proc/sec":2577,"drop/sec":0,"total_recv":28363,"total_proc":28363,"total_drop":0,"total_errors":0,"queue_depth":0,"queue_cap":1000}
{"time":"2026-04-28T17:07:10.273905926-07:00","level":"INFO","msg":"pipeline stats","recv/sec":2559,"proc/sec":2559,"drop/sec":0,"total_recv":41161,"total_proc":41161,"total_drop":0,"total_errors":0,"queue_depth":0,"queue_cap":1000}
^C{"time":"2026-04-28T17:07:14.695936071-07:00","level":"INFO","msg":"pipeline completed successfully","stats":{"Received":52761,"Processed":52761,"Dropped":0,"Errors":0}}
```

## Project Structure

```
firehose-abuse-scanner/
├── cmd/
│   └── scanner/
│       └── main.go
├── internal/
│   ├── config/
│   │   ├── config.go         # env-based config
│   ├── firehose/
│   │   ├── simulator.go      # local event generator
│   │   └── client.go         # interface (swap in real WS later)
│   ├── pipeline/
│   │   ├── pipeline.go       # worker pool + channel orchestration
│   │   └── backpressure.go   # drop vs block mode
│   └── models/
│       └── event.go          # shared event types
├── config.dev.json
├── docker-compose.yml
├── go.mod
└── README.md
```
