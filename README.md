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
│  ├─ Scheduler (round-robin or per-DID)         │
│  └─ Work queue (buffered channel)              │
│     Backpressure: drop or block                │
└────────┬──────────────────────────────────────┘
         │
         │ Work distribution
         │
    ┌────┴────────────────────────┐
    │           │           │     │
┌───▼──┐  ┌─────▼──┐  ┌─────▼──┐  ┌──▼───┐
│ W1   │  │ W2     │  │ W3     │  │ ...  │   Worker pool (configurable count)
│      │  │        │  │        │  │      │   Handler.Handle(evt)
└───┬──┘  └─────┬──┘  └─────┬──┘  └──┬───┘
    │           │           │        │
    │           │           │        │
┌───▼───────────▼───────────▼────────▼────┐
│  Detect (rules)                          │   Inspect(evt) []Hit
│  ├─ keyword / regex                      │
│  ├─ link blocklist                       │
│  └─ per-DID rate spike (sliding window)  │
└───┬──────────────────────────────────────┘
    │
    │ raw event + any hits
    │
┌───▼────────────────────────────────────────┐
│  Storage (Scylla)                           │
│  ├─ events_by_did       (per-author)        │   Batched writes
│  ├─ events_by_minute    (time-window)       │   (UNLOGGED BATCH)
│  └─ flagged_events      (detector hits)     │
└─────────────────────────────────────────────┘

Stats (atomic counters): Received │ Processed │ Dropped │ Errors
Metrics: Prometheus exporter (worker queue depth, handler latency, write throughput)
```

## Running

Prerequisites:

* [go](https://go.dev/) - Known working with go version `1.26.2`
* [Docker](https://docs.docker.com/get-docker/) with Compose v2 — only needed once the storage layer is wired up (Phase 1)

Defaults come from `config.json` layered with `config.dev.json`:

```bash
go run ./cmd/scanner
```

Pick a different profile by setting `ENV` to the suffix of any `config.<env>.json` file (e.g. `stress`, `stress-02`, `stress-max`):

```bash
ENV=stress go run ./cmd/scanner
```

`ENV` is the only environment variable the scanner consults — every other knob lives in the JSON files so a run is reproducible from the checked-in config alone. The [justfile](justfile) wraps the common profiles as `just run`, `just run-stress`, `just run-stress-02`, `just run-stress-max`, and `just run-env <name>` for ad-hoc envs.

### Storage (Scylla)

The scanner persists events to a single-node [Scylla](https://www.scylladb.com/) instance for the demo. Bluesky runs Scylla in production for the atproto data plane, so the schema and partitioning choices in this repo mirror that environment.

Bring it up:

```bash
docker compose up -d db
```

The container is sized for a workstation-class demo (`--smp 8 --memory 4G --overprovisioned 1 --developer-mode 1`) — eight shards and 4 GiB of off-heap memory for the row cache, which keeps Scylla from being the bottleneck under the `stress-max` profile. Bootstrap takes ~30–60 seconds; wait for the healthcheck to go green before pointing the scanner at it:

```bash
docker compose ps      # STATUS should show "healthy"
```

Connect a CQL shell for ad-hoc queries:

```bash
docker exec -it firehose-scylla cqlsh
```

Tear down (data persists in the `scylla-data` volume):

```bash
docker compose down
```

Wipe the volume too (fresh schema on next boot):

```bash
docker compose down -v
```

#### Connection settings

The scanner reads Scylla connection details from `config.json` / `config.<env>.json`:

| config key           | default              | notes                                         |
| -------------------- | -------------------- | --------------------------------------------- |
| `scylla_hosts`       | `["127.0.0.1:9042"]` | list of `host:port` strings                   |
| `scylla_keyspace`    | `firehose_scanner`   | created on first boot via embedded schema     |
| `scylla_consistency` | `ONE`                | matches RF=1 single-node demo                 |
| `scylla_timeout_ms`  | `5000`               | applies to both connect and per-query timeout |

The schema in [internal/storage/scylla/schema.cql](internal/storage/scylla/schema.cql) is embedded into the binary and applied (idempotently) on every startup, so a fresh `docker compose down -v && docker compose up -d db` is enough to reset state for a clean demo run.

### Sample output

```bash
{17:06}~/code/mxygem/firehose-abuse-scanner:main ✗ ➭ ENV=stress go run ./cmd/scanner
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
│   │   └── config.go              # koanf-loaded JSON + ENV layering
│   ├── detect/
│   │   ├── detect.go              # Detector interface + Hit
│   │   ├── keyword.go             # keyword / regex spam rule
│   │   └── blocklist.go           # link-host blocklist rule
│   ├── firehose/
│   │   ├── client.go              # Subscribe interface (swap in real WS later)
│   │   └── simulator.go           # local event generator
│   ├── metrics/
│   │   └── metrics.go             # Prometheus exporter
│   ├── models/
│   │   └── event.go               # FirehoseEvent + EventKind
│   ├── pipeline/
│   │   ├── pipeline.go            # worker pool + drop/block backpressure
│   │   ├── scheduler.go           # round-robin scheduler
│   │   ├── parallel_scheduler.go  # per-DID-affinity scheduler
│   │   ├── log_handler.go         # debug Handler
│   │   └── scylla_handler.go      # Scylla-backed Handler
│   └── storage/
│       ├── storage.go             # Storer interface + FlaggedRecord
│       └── scylla/
│           ├── schema.cql         # embedded keyspace + tables
│           ├── session.go         # gocql session bootstrap
│           ├── store.go           # single-row Insert*
│           └── batch_store.go     # UNLOGGED BATCH writer
├── config.json                    # base config
├── config.dev.json                # ENV=dev overlay
├── config.stress*.json            # benchmark profiles
├── docker-compose.yml             # Scylla + observability stack
├── justfile                       # run/test wrappers
├── go.mod
├── TASKS.md
└── README.md
```

## Performance

* Peak: 151k flushed events/s (stress-max-3)
* Conservative repeatable band: ~123k–151k flushed/s on stress-max (with 0 failed-event runs)

### Most Recent Guidance

```
Great question — I’ll inspect the batch writer internals to pinpoint where your current ceiling is coming from, then give you a concrete tuning order to push beyond it.

Biggest thing first: you’re comparing against a target that likely uses a different unit.

Your summary’s flushed events is row writes, not logical firehose events.
In this code, each logical event writes to two tables (events_by_did + events_by_minute) in BatchStore.InsertEvent.
So your best 151k flushed events/s is roughly ~75k logical events/s durable.
That means “1m+ durable events/s” would require ~2m row writes/s with current schema/write amplification — not realistic on a single Scylla container.

What to do to improve (in order)
Fix benchmark accounting first (important): current summary is printed before store.Close() drains final buffered batches, so you undercount landed writes in tail drain.
Reduce write amplification: if benchmark goal is raw ingest ceiling, write one primary table only, or move events_by_minute projection async/offline.
Run deterministic load for tuning: your simulator includes bursts/randomness, which causes huge run-to-run variance; tune on steady load first, then re-enable bursts.
Tune batch path with a sweep: push scylla_batch_size, scylla_batch_flush_workers, scylla_num_conns, scylla_batch_shards together; look for first point where failures/timeouts appear, then back off 10–20%.
Scale Scylla, not just app workers: once queue is saturated and drop% is high, more Go workers mostly increase pressure; you need more DB capacity (more cores/memory and ideally multi-node).
Trim payload size for peak tests: text, links, langs are written twice; payload-heavy rows cap throughput early.
Reality check on your numbers
You already hit ~700k–900k recv/s in some runs.
Durable ceiling currently is around ~75k logical events/s (from 151k row writes/s best case).
To get “1m+ logical durable/s” on this design, you need roughly 10–15x improvement, which is architecture/infrastructure-level (not just config tuning).
If you want, I can give you a tight sweep plan (exact parameter grid and stop conditions) to find the next 20–50% on this current single-node setup before bigger architecture changes.
```
