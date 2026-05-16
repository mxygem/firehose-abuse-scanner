# firehose-abuse-scanner justfile

# list available recipes
default:
    @just --list

# ── Build & Run ───────────────────────────────────────────────────────────────

# run the scanner (uses config.dev.json)
run:
    go run ./cmd/scanner

# run the scanner against a named env (e.g.: just run-env stress)
run-env env:
    ENV={{ env }} go run ./cmd/scanner

# run the scanner with high-throughput drop config (config.stress.json, 60s)
run-stress:
    ENV=stress go run ./cmd/scanner

# run the scanner with tuned Scylla concurrency baseline (config.stress-02.json, 60s)
run-stress-02:
    ENV=stress-02 go run ./cmd/scanner

# run the scanner with max-throughput profile from sweep (config.stress-max.json, 5m)
run-stress-max:
    ENV=stress-max go run ./cmd/scanner

# run a 60-second benchmark with config.bench.json + --benchmark flag (did_only writes)
run-bench:
    ENV=bench go run ./cmd/scanner --benchmark

# run a 60-second full-mode benchmark with config.bench.json (writes both tables)
run-bench-full:
    ENV=bench go run ./cmd/scanner

# build + run the sweep matrix (requires Scylla on localhost:9042)
run-sweep:
    bash scripts/sweep.sh --build --duration 60s > sweep-results.csv
    @echo "sweep complete → sweep-results.csv"

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

# run all unit tests with coverage and open the HTML report in a browser
test-coverage:
    go test -coverprofile=coverage.out ./...
    go tool cover -html=coverage.out -o coverage.html
    @if command -v wslview >/dev/null 2>&1; then wslview coverage.html; \
        elif command -v explorer.exe >/dev/null 2>&1; then explorer.exe coverage.html || true; \
        elif command -v xdg-open >/dev/null 2>&1; then xdg-open coverage.html; \
        elif command -v open >/dev/null 2>&1; then open coverage.html; \
        else echo "No opener found; report at $(pwd)/coverage.html"; fi

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

# start a standalone Scylla seed (e.g.: just scylla-start 4 2G)
scylla-start smp="1" memory="1G":
    docker run --name some-scylla --hostname some-scylla -p 9042:9042 -d scylladb/scylla \
        --developer-mode=1 --overprovisioned=1 --smp={{ smp }} --memory={{ memory }} --reserve-memory=0M

# add the next Scylla node, seeded from some-scylla (e.g.: just scylla-add-node 4 2G)
scylla-add-node smp="1" memory="1G":
    @set -eu; \
        echo "Waiting for seed node (some-scylla) to be ready..."; \
        until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done; \
        next_node_num=$(docker ps -a --filter "name=^/some-scylla[0-9]*$" --no-trunc | awk 'NR > 1 {print $NF}' | sed -n 's/^some-scylla$/1/p; s/^some-scylla\([0-9][0-9]*\)$/\1/p' | sort -n | awk 'END {print}'); \
        if [ -z "$next_node_num" ]; then echo "No seed container named some-scylla found."; exit 1; fi; \
        next_node_num=$((next_node_num + 1)); \
        node_name="some-scylla${next_node_num}"; \
        seed_ip="$(docker inspect some-scylla | python3 -c 'import json,sys; obj=json.load(sys.stdin)[0]; nets=obj["NetworkSettings"]["Networks"]; print(next(iter(nets.values()))["IPAddress"])' | tr -d '\n')"; \
        echo "Seed node is ready. Starting ${node_name}..."; \
        docker run --name "$node_name" --hostname "$node_name" -d scylladb/scylla \
            --developer-mode=1 --overprovisioned=1 --smp={{ smp }} --memory={{ memory }} --reserve-memory=0M \
            --seeds="$seed_ip"

# check nodetool status across every running some-scylla* container
scylla-status:
    @set -eu; \
        nodes=$(docker ps --filter "name=^/some-scylla[0-9]*$" --no-trunc | awk 'NR > 1 {print $NF}' | sort); \
        if [ -z "$nodes" ]; then echo "No running some-scylla* containers."; exit 0; fi; \
        for c in $nodes; do \
            echo "--- $c ---"; \
            docker exec "$c" nodetool status 2>/dev/null || echo "$c not ready yet"; \
        done

# wait until the seed node is UN (Up/Normal)
scylla-wait:
    @echo "Waiting for Scylla to be ready..."
    @until docker exec some-scylla nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done
    @echo "Scylla is ready."

# wait until every running some-scylla* container is UN
scylla-wait-cluster:
    @set -eu; \
        echo "Waiting for Scylla cluster to be ready..."; \
        nodes=$(docker ps --filter "name=^/some-scylla[0-9]*$" --no-trunc | awk 'NR > 1 {print $NF}' | sort); \
        if [ -z "$nodes" ]; then echo "No running some-scylla* containers."; exit 1; fi; \
        for c in $nodes; do \
            until docker exec "$c" nodetool status 2>/dev/null | grep -q "^UN"; do sleep 5; done; \
            echo "$c is UN"; \
        done; \
        echo "Scylla cluster is ready."

# open a CQL shell into the seed node
scylla-shell:
    docker exec -it some-scylla cqlsh

# stop and remove every some-scylla* container
scylla-stop:
    @docker ps -a --filter "name=^/some-scylla[0-9]*$" --no-trunc | awk 'NR > 1 {print $NF}' | xargs -r docker rm -f

# ── Observability — Prometheus + Grafana via docker-compose ───────────────────

# bring up Prometheus (localhost:9091) + Grafana (localhost:3000); scrapes scanner on host:9090
o11y-up:
    docker compose up -d prometheus grafana

# tear down the o11y stack (data volumes preserved)
o11y-down:
    docker compose down

# tear down the o11y stack and wipe its data volumes
o11y-wipe:
    docker compose down -v

# tail Prometheus + Grafana logs
o11y-logs:
    docker compose logs -f prometheus grafana
