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
    BACKPRESSURE_MODE=drop WORKER_COUNT=100 EVENTS_PER_SECOND=10000 go run ./cmd/scanner

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
    docker run --name some-scylla --hostname some-scylla -p 9042:9042 -d scylladb/scylla

# add a second Scylla node and seed it from the first (forms a 2-node cluster)
scylla-add-node:
    docker run --name some-scylla2 --hostname some-scylla2 -d scylladb/scylla \
        --seeds="$(docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' some-scylla)"

# check cluster status via nodetool
scylla-status:
    docker exec -it some-scylla nodetool status

# wait until the node is UN (Up/Normal) — polls every 5s
scylla-wait:
    @echo "Waiting for Scylla to be ready..."
    @until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @echo "Scylla is ready."

# open a CQL shell into the standalone Scylla container
scylla-shell:
    docker exec -it some-scylla cqlsh

# stop and remove standalone Scylla containers
scylla-stop:
    docker rm -f some-scylla some-scylla2 2>/dev/null || true
