#!/usr/bin/env bash
# ── sweep.sh ─────────────────────────────────────────────────────────────
# Runs the scanner across a matrix of configuration knobs and writes a CSV
# of results to stdout.  Each row is one run.
#
# Usage:
#   bash scripts/sweep.sh [--build] [--dry-run] [--duration 30s] > results.csv
#
# The CSV columns are:
#   mode,workers,batch_size,flush_workers,conns,received_sec,processed_sec,
#   scylla_events_sec,p50_us,p95_us,p99_us,dropped_pct,error_pct
#
# Environment:
#   SCANNER_BIN  — path to scanner binary (default: ./scanner)
#   SWEEP_RUNS   — number of runs per cell (default: 1)
#
# Dependencies: go (if --build), the scanner binary, and a running Scylla node.

set -euo pipefail
cd "$(dirname "$0")/.."

BIN="${SCANNER_BIN:-./scanner}"
RUNS="${SWEEP_RUNS:-1}"
DRY_RUN=false
BUILD=false
DURATION="30s"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build) BUILD=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --duration) DURATION="$2"; shift 2 ;;
        *) echo "unknown: $1"; exit 1 ;;
    esac
done

if "$BUILD"; then
    echo >&2 "building scanner..."
    go build -o "$BIN" ./cmd/scanner
fi

if ! "$DRY_RUN" && [ ! -x "$BIN" ]; then
    echo >&2 "binary not found: $BIN (use --build or set SCANNER_BIN)"
    exit 1
fi

# ── matrix ────────────────────────────────────────────────────────────────
MODE=(full did_only)
WORKERS=(50 100 200)
BATCH_SIZE=(200 500 1000)
FLUSH_WORKERS=(32 64 128)
CONNS=(32 64 128)

echo "mode,workers,batch_size,flush_workers,conns,received_sec,processed_sec,scylla_events_sec,p50_ms,p95_ms,p99_ms,dropped_pct,error_pct"

for mode in "${MODE[@]}"; do
  for workers in "${WORKERS[@]}"; do
    for bsize in "${BATCH_SIZE[@]}"; do
      for fw in "${FLUSH_WORKERS[@]}"; do
        for nc in "${CONNS[@]}"; do
          for run in $(seq 1 "$RUNS"); do
            # Build overlay config from base config.bench.json
            tmp=$(mktemp /tmp/bench-XXXXXX.json)
            cat config.bench.json > "$tmp"

            # Override fields for this cell
            # Use python for portable JSON manipulation
            python3 -c "
import json
with open('$tmp') as f:
    c = json.load(f)
c['worker_count'] = $workers
c['scylla_batch_size'] = $bsize
c['scylla_batch_flush_workers'] = $fw
c['scylla_num_conns'] = $nc
c['scylla_write_mode'] = '$mode'
c['simulator_duration'] = '$DURATION'
with open('$tmp', 'w') as f:
    json.dump(c, f)
"

            if "$DRY_RUN"; then
                echo >&2 "DRY-RUN: ENV=bench go run ./cmd/scanner --benchmark (mode=$mode workers=$workers batch=$bsize fw=$fw conns=$nc run=$run)"
                rm -f "$tmp"
                continue
            fi

            # Run and parse the benchmark summary from stderr.
            output=$(ENV=bench "$BIN" --benchmark 2>&1 >/dev/null) || true

            # Parse key fields from the ASCII-art summary.
            recv=$(echo "$output" | sed -n 's/.*received.*(\([0-9]*\)\/s).*/\1/p')
            proc=$(echo "$output" | sed -n 's/.*processed.*(\([0-9]*\)\/s).*/\1/p')
            scylla=$(echo "$output" | sed -n 's/.*flushed events.*(\([0-9]*\)\/s).*/\1/p')
            p50=$(echo "$output" | sed -n 's/.*p50[[:space:]]*\([0-9]*\)[0-9]*\..*/\1/p')

            # Safer extraction with awk
            recv=$(echo "$output" | awk '/received/ && /s\)/ {gsub(/[^0-9]/,"",$3); print $3}')
            proc=$(echo "$output" | awk '/processed/ && /s\)/ {gsub(/[^0-9]/,"",$3); print $3}')
            syc=$(echo "$output" | awk '/flushed events/ && /s\)/ {gsub(/[^0-9]/,"",$4); print $4}')
            dropped=$(echo "$output" | awk '/dropped/ && /%/ {gsub(/[%]/,"",$3); print $3}')
            errors=$(echo "$output" | awk '/handler errors/ {print $3}')

            # Latency: output lines like "    p50  123µs" or "    p50  1.234ms"
            p50=$(echo "$output" | awk '/p50/ && !/p95/ && !/p99/ {print $2}')
            p95=$(echo "$output" | awk '/p95/ && !/p99/ {print $2}')
            p99=$(echo "$output" | awk '/p99/ && !/p99.9/ && !/p95/ {print $2}')

            # Convert latency to milliseconds (handle µs and ms suffixes)
            to_ms() {
                local v=$1
                case "$v" in
                    *µs) echo "scale=3; ${v%µs}/1000" | bc ;;
                    *ms) echo "${v%ms}" ;;
                    *) echo "0" ;;
                esac
            }

            p50_ms=$(to_ms "$p50")
            p95_ms=$(to_ms "$p95")
            p99_ms=$(to_ms "$p99")

            echo "$mode,$workers,$bsize,$fw,$nc,$recv,$proc,$syc,$p50_ms,$p95_ms,$p99_ms,$dropped,$errors"

            rm -f "$tmp"
          done
        done
      done
    done
  done
done
