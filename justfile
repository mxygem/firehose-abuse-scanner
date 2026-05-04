# firehose-abuse-scanner justfile

# list available recipes
default:
    @just --list

# ── Build & Run ───────────────────────────────────────────────────────────────

# run the scanner (uses config.dev.json)
run:
    go run ./cmd/scanner

# run the scanner for a fixed duration then exit cleanly (e.g.: just run-timed 5s)
run-timed duration:
    SIMULATOR_DURATION={{ duration }} go run ./cmd/scanner

# run the scanner with high-throughput drop config
run-stress:
    BACKPRESSURE_MODE=drop WORKER_COUNT=100 EVENTS_PER_SECOND=1000000 SIMULATOR_CONCURRENCY=20 SCYLLA_NUM_CONNS=8 SCYLLA_BATCH_FLUSH_WORKERS=8 SCYLLA_BATCH_QUEUE_SIZE=256 go run ./cmd/scanner

# run the scanner with tuned Scylla concurrency baseline (30s)
run-stress-02:
    BACKPRESSURE_MODE=drop WORKER_COUNT=100 EVENTS_PER_SECOND=1000000 SIMULATOR_CONCURRENCY=20 SCYLLA_NUM_CONNS=32 SCYLLA_BATCH_FLUSH_WORKERS=32 SCYLLA_BATCH_QUEUE_SIZE=2048 SCYLLA_BATCH_SIZE=200 SCYLLA_BATCH_SHARDS=64 SIMULATOR_DURATION=30s go run ./cmd/scanner

# run the scanner with max-throughput profile from sweep (30s)
run-stress-max:
    BACKPRESSURE_MODE=drop WORKER_COUNT=100 EVENTS_PER_SECOND=1000000 SIMULATOR_CONCURRENCY=20 SCYLLA_NUM_CONNS=64 SCYLLA_BATCH_FLUSH_WORKERS=64 SCYLLA_BATCH_QUEUE_SIZE=4096 SCYLLA_BATCH_SIZE=400 SCYLLA_BATCH_SHARDS=128 SIMULATOR_DURATION=30s go run ./cmd/scanner

# build the scanner binary
build:
    go build -o ./scanner ./cmd/scanner

# ── Tests ─────────────────────────────────────────────────────────────────────

# run all unit tests
test:
    go test ./...

# run unit tests with verbose output
test-v:
    go test -v ./...

# run a single test by name (e.g.: just test-one TestFoo ./internal/pipeline/...)
test-one name pkg:
    go test {{ pkg }} -run {{ name }}

# run integration tests (requires Scylla running)
test-integration:
    go test -tags=integration ./internal/storage/scylla/...

# run integration tests against a custom host
test-integration-host host:
    SCYLLA_TEST_HOSTS={{ host }} go test -tags=integration ./internal/storage/scylla/...

# ── Code Generation ───────────────────────────────────────────────────────────

# regenerate mocks (requires mockery)
generate:
    go generate ./...

# ── Scylla — standalone containers ────────────────────────────────────────────

# start a single standalone Scylla container
scylla-start:
    docker run --name some-scylla --hostname some-scylla -p 9042:9042 -d scylladb/scylla \
        --developer-mode=1 --overprovisioned=1 --smp=1 --memory=1G --reserve-memory=0M

# add a second Scylla node and seed it from the first (forms a 2-node cluster)
scylla-add-node:
    @echo "Waiting for seed node (some-scylla) to be ready..."
    @until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @echo "Seed node is ready. Starting second node..."
    docker run --name some-scylla2 --hostname some-scylla2 -d scylladb/scylla \
        --developer-mode=1 --overprovisioned=1 --smp=1 --memory=1G --reserve-memory=0M \
        --seeds="$(docker inspect some-scylla | python3 -c 'import json,sys; obj=json.load(sys.stdin)[0]; nets=obj["NetworkSettings"]["Networks"]; print(next(iter(nets.values()))["IPAddress"])' | tr -d '\n')"

# check cluster status via nodetool
scylla-status:
    @echo "--- some-scylla ---"
    @docker exec some-scylla nodetool status || echo "some-scylla not ready yet"
    @echo "--- some-scylla2 ---"
    @docker exec some-scylla2 nodetool status || echo "some-scylla2 not ready yet"

# wait until the node is UN (Up/Normal) — polls every 5
scylla-wait:
    @echo "Waiting for Scylla to be ready..."
    @until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @echo "Scylla is ready."

# wait until both nodes in the 2-node cluster are UN
scylla-wait-cluster:
    @echo "Waiting for 2-node Scylla cluster to be ready..."
    @until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @until docker exec some-scylla2 nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @echo "Scylla cluster is ready."

# open a CQL shell into the standalone Scylla container
scylla-shell:
    docker exec -it some-scylla cqlsh

# stop and remove standalone Scylla containers
scylla-stop:
    docker rm -f some-scylla some-scylla2 2>/dev/null || true
