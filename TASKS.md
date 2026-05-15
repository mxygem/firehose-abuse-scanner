# TASKS

Work plan to take the scanner from its current "simulator → log handler" state to a Scylla-backed perf demo suitable for the project goals.

Each task is intended to be a single, reviewable change. Phases are sequential; tasks within a phase can usually be reordered or parallelized.

---

## Phase 1 — Storage foundation (Scylla)

Goal: a running Scylla node and a handler that durably writes every event the pipeline processes.

- [X] **1.1 Add Scylla to docker-compose**
  - Single-node `scylladb/scylla` service with `--smp 2 --memory 1G --overprovisioned 1` for laptop-friendly resource use.
  - Healthcheck on CQL port 9042.
  - Document `docker compose up scylla` in the README.

- [X] **1.2 Schema design (`schema.cql`)**
  - Keyspace with `SimpleStrategy` RF=1 (single node demo).
  - Table `events_by_did` — partition by `did`, cluster by `(received_at DESC, id)`. Stores raw events for per-author lookups.
  - Table `events_by_minute` — partition by `(kind, bucket_minute)`, cluster by `(received_at DESC, id)`. Supports time-window scans for rate analysis.
  - Table `flagged_events` — partition by `bucket_hour`, cluster by `(received_at DESC, id)`. Holds detector hits.
  - Document the partition-key reasoning inline (atproto DIDs are naturally high-cardinality → good partition keys; minute buckets prevent hot partitions on the time-series table).

- [X] **1.3 Scylla client wrapper (`internal/storage/scylla`)**
  - Wrap `gocql` with a small `Store` struct exposing `InsertEvent`, `InsertFlagged`, `Close`.
  - Connection config (hosts, keyspace, consistency, timeout) added to `Config` and `config.*.json`.
  - Use prepared statements; reuse a single session.

- [X] **1.4 ScyllaHandler**
  - Implements `pipeline.Handler`, replacing `LogHandler` as the default in `cmd/scanner/main.go`.
  - Writes each event to `events_by_did` and `events_by_minute`.
  - Returns an error (pipeline already counts these) — no retry yet.

- [X] **1.5 Batched writer (perf lever)**
  - Add an in-memory write buffer per worker that flushes by size (e.g. 100) or interval (e.g. 50ms) using `UNLOGGED BATCH` keyed by partition.
  - Make the batch size + flush interval configurable so the demo can show the throughput curve.
  - Compare batched vs. single-insert numbers in the README. *(deferred to 4.3 — depends on benchmark mode in 4.1)*

---

## Phase 2 — Abuse detection

Goal: a small but realistic rule set that produces hits the dashboard/CLI can show.

- [X] **2.1 Detector interface**
  - `type Detector interface { Inspect(ctx, evt) []Hit }` in `internal/detect`.
  - `Hit` carries `RuleID`, `Severity`, `Reason`, `Evidence`.

- [X] **2.2 Rule: spam keyword / regex**
  - Compile a small set of patterns (the simulator already seeds obvious ones like "BUY NOW", "free crypto").
  - Cheap, allocation-free hot path.

- [X] **2.3 Rule: link blocklist**
  - Match against a static set of bad domains; the simulator's `spam-link.xyz` and `phishing.example.com` are the obvious seeds.

- [X] **2.4 Rule: per-DID rate spike**
  - Sliding-window counter per DID (in-memory, e.g. `sync.Map` of ring buffers, capped LRU).
  - Flag when a DID exceeds N events per window. Tunable thresholds.

- [X] **2.5 Composite handler**
  - Replaces ScyllaHandler as the pipeline handler. Order: write raw event → run detectors → if any hit, write flagged row.
  - Detectors run sequentially per event (each event already runs on its own goroutine via the worker pool).

---

## Phase 3 — Surface (read path)

Goal: prove the data is queryable. A full dashboard is out of scope; pick one of the two below.

- [X] **3.1 CLI subcommand `scanner query`** *(preferred — minimal)*
  - `scanner query did <did>` → recent events for a DID.
  - `scanner query flagged --since 10m` → recent hits.
  - Demonstrates the partition-key choices from 1.2 paying off.

- [ ] **3.2 (Optional) Tiny HTTP read API**
  - `GET /flagged?since=10m`, `GET /did/{did}/events`. JSON only.
  - Skip unless time permits — the CLI is enough to talk through the schema in an interview.

---

## Phase 4 — Performance measurement

Goal: numbers to point at. This is the headline of the demo.

- [ ] **4.1 Benchmark mode**
  - `--benchmark` flag: run for N seconds or M events, then exit and print a summary (events received, processed, dropped, p50/p95/p99 handler latency, sustained writes/sec to Scylla).
  - Histogram via `hdrhistogram-go`.

- [ ] **4.2 Sweep script**
  - Bash script that runs the binary across a matrix of `worker_count × batch_size × events_per_second` and writes a CSV.
  - Used to produce a throughput-vs-latency chart for the README.

- [ ] **4.3 README perf section**
  - Hardware used, configuration matrix, the chart, a one-paragraph interpretation (where the bottleneck is — channel, handler, Scylla writes).

---

## Phase 5 — Polish for the interview

- [ ] **5.1 README rewrite**
  - Replace the "PostgreSQL/Redis" leftovers in `Config` with the actual Scylla story.
  - Architecture diagram updated to include the storage layer.
  - "Why these choices" section: why Scylla (matches Bluesky prod), why Go (matches atproto), why drop-vs-block, why per-DID partitioning.

- [X] **5.2 Code cleanup pass**
  - Typos: `controlsThe` (simulator.go), `subscribing` (main.go), comment grammar in `log_handler.go`.
  - Drop `PostgresDSN` / `RedisAddr` from `Config` — they're unused and misleading.
  - Extract `MustLoad` env-override repetition into a small helper if it grows much further (currently borderline; leave alone if it doesn't).

- [ ] **5.3 Test coverage on the hot path**
  - Pipeline drop-mode behavior under a full queue.
  - Detector unit tests (one per rule).
  - Skip integration tests against Scylla unless they're cheap to run; the perf script is the real validation.

---

## Out of scope

- Real WebSocket firehose client (interface is already there; swapping in `indigo` is mechanical).
- Multi-node Scylla / replication tuning.
- Auth, multi-tenancy, retention/TTL policies.
- ML-based detection — rules are enough to demonstrate the pipeline shape.
