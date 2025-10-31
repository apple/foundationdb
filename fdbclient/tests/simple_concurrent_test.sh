#!/usr/bin/env bash
#
# Simple test of concurrent s3client_test.sh execution
#
set -euo pipefail

BUILD_DIR="/Users/stack/build_output"
TEST_INSTANCES=10

echo "$(date -Iseconds) Testing concurrent s3client execution"
echo "$(date -Iseconds) Starting $TEST_INSTANCES concurrent instances..."

# Array to track PIDs
PIDS=()

# Start test instances
for i in $(seq 1 $TEST_INSTANCES); do
    SCRATCH_DIR=$(mktemp -d "/tmp/s3test_${i}.XXXXXX")
    echo "$(date -Iseconds) Starting instance $i with scratch: $SCRATCH_DIR"
    
    # Run s3client_test.sh in background
    (
        cd fdbclient/tests
        ./s3client_test.sh "$BUILD_DIR" "$SCRATCH_DIR" > "/tmp/s3test_${i}.log" 2>&1
        echo "$(date -Iseconds) Instance $i completed successfully"
    ) &
    
    PIDS+=($!)
    echo "$(date -Iseconds) Instance $i started with PID ${PIDS[$((i-1))]}"
    
    # Stagger starts slightly to increase chance of port conflicts
    sleep 1
done

echo "$(date -Iseconds) All instances started, waiting for completion..."

# Wait for all instances
for i in "${!PIDS[@]}"; do
    pid=${PIDS[$i]}
    if wait "$pid"; then
        echo "$(date -Iseconds) Instance $((i+1)) (PID $pid) completed successfully"
    else
        echo "$(date -Iseconds) Instance $((i+1)) (PID $pid) failed"
    fi
done

echo "$(date -Iseconds) Checking for port conflicts in logs..."
for i in $(seq 1 $TEST_INSTANCES); do
    if grep -q "Port.*already in use\|trying next port" "/tmp/s3test_${i}.log"; then
        echo "$(date -Iseconds) Instance $i: Port conflict detected and handled"
        grep "Port.*already in use\|trying next port\|MockS3Server ready" "/tmp/s3test_${i}.log"
    else
        echo "$(date -Iseconds) Instance $i: No port conflicts detected"
    fi
done

echo "$(date -Iseconds) Test completed!"