#!/usr/bin/env bash
set -e

# Number of runs (default: 1, use 0 for infinite)
MAX_ITERATIONS=${1:-1}
# Bound one pathological seed while keeping its traces for diagnosis.
SEED_TIMEOUT=${LEADER_ELECTION_SEED_TIMEOUT:-15m}

# Create trace directory
mkdir -p ./target/traces

# Build in release mode
echo "Building in release mode..."
cargo build --release -p foundationdb-recipes-simulation

TEST="foundationdb-recipes-simulation/test_poll_leader_election.toml"

echo ""
echo "=========================================="
echo "Running: $TEST"
echo "=========================================="

iteration=1
while [ "$MAX_ITERATIONS" -eq 0 ] || [ "$iteration" -le "$MAX_ITERATIONS" ]; do
    config=$(basename "$TEST" .toml)
    seed=$(((RANDOM << 17) | (RANDOM << 2) | (RANDOM & 3)))
    trace_dir="./target/traces/${config}-iteration-${iteration}-seed-${seed}"
    mkdir -p "$trace_dir"

    echo "----------------------"
    echo "Config: $TEST"
    echo "Iteration: $iteration"
    echo "Seed: $seed"
    echo "Per-seed timeout: $SEED_TIMEOUT"
    echo "----------------------"

    if timeout --kill-after=1m "$SEED_TIMEOUT" \
        fdbserver -r simulation -f "$TEST" -b on --trace-format json -L "$trace_dir" --logsize 1GiB --seed "$seed"; then
        rm -rf -- "$trace_dir"
        echo "Iteration $iteration passed"
    else
        status=$?
        if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
            echo "TIMED OUT after $SEED_TIMEOUT on iteration $iteration"
        fi
        echo "FAILED on iteration $iteration for $TEST"
        echo "Seed: $seed"
        echo "Traces: $trace_dir"
        printf 'Reproduce: fdbserver -r simulation -f %q -b on --trace-format json -L %q --logsize 1GiB --seed %s\n' \
            "$TEST" "$trace_dir" "$seed"
        exit "$status"
    fi
    iteration=$((iteration + 1))
done

echo "PASSED: $TEST"

echo ""
echo "=========================================="
echo "All tests passed!"
echo "=========================================="
