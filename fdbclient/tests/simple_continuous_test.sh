#!/usr/bin/env bash
#
# Simple continuous test: Run 10 concurrent s3client tests repeatedly for 5 minutes
#
set -euo pipefail

BUILD_DIR="/Users/stack/build_output"
CONCURRENT_INSTANCES=10
DURATION_MINUTES=5
DURATION_SECONDS=$((DURATION_MINUTES * 60))

echo "$(date -Iseconds) Starting simple continuous test for $DURATION_MINUTES minutes"
echo "$(date -Iseconds) Running $CONCURRENT_INSTANCES concurrent instances per batch"

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_SECONDS))
BATCH_NUM=1
TOTAL_TESTS=0
TOTAL_SUCCESS=0
TOTAL_FAILED=0

while [[ $(date +%s) -lt $END_TIME ]]; do
    echo ""
    echo "$(date -Iseconds) ===== BATCH $BATCH_NUM ====="
    
    # Start concurrent instances
    PIDS=()
    for i in $(seq 1 $CONCURRENT_INSTANCES); do
        SCRATCH_DIR=$(mktemp -d "/tmp/continuous_${BATCH_NUM}_${i}.XXXXXX")
        
        (
            cd fdbclient/tests
            ./s3client_test.sh "$BUILD_DIR" "$SCRATCH_DIR" > "/tmp/continuous_${BATCH_NUM}_${i}.log" 2>&1
        ) &
        
        PIDS+=($!)
        echo "$(date -Iseconds) Started instance ${BATCH_NUM}_${i} (PID ${PIDS[$((i-1))]})"
        
        # Brief stagger
        sleep 0.2
    done
    
    echo "$(date -Iseconds) Batch $BATCH_NUM: All $CONCURRENT_INSTANCES instances started, waiting..."
    
    # Wait for this batch to complete
    BATCH_SUCCESS=0
    BATCH_FAILED=0
    
    for i in "${!PIDS[@]}"; do
        pid=${PIDS[$i]}
        if wait "$pid"; then
            ((BATCH_SUCCESS++))
            ((TOTAL_SUCCESS++))
        else
            ((BATCH_FAILED++))
            ((TOTAL_FAILED++))
        fi
        ((TOTAL_TESTS++))
    done
    
    # Check port conflicts
    PORT_CONFLICTS=0
    for i in $(seq 1 $CONCURRENT_INSTANCES); do
        if grep -q "Port.*already in use\|trying next port" "/tmp/continuous_${BATCH_NUM}_${i}.log" 2>/dev/null; then
            ((PORT_CONFLICTS++))
        fi
    done
    
    echo "$(date -Iseconds) Batch $BATCH_NUM completed: $BATCH_SUCCESS success, $BATCH_FAILED failed, $PORT_CONFLICTS port conflicts"
    
    # Show some port conflict examples
    if [[ $PORT_CONFLICTS -gt 0 ]]; then
        echo "$(date -Iseconds) Port conflict examples:"
        for i in $(seq 1 $CONCURRENT_INSTANCES); do
            if grep -q "MockS3Server ready on port" "/tmp/continuous_${BATCH_NUM}_${i}.log" 2>/dev/null; then
                grep "MockS3Server ready on port" "/tmp/continuous_${BATCH_NUM}_${i}.log" | head -1
                break
            fi
        done
    fi
    
    # Running stats
    ELAPSED=$(($(date +%s) - START_TIME))
    REMAINING=$((END_TIME - $(date +%s)))
    SUCCESS_RATE=$((TOTAL_SUCCESS * 100 / TOTAL_TESTS))
    
    echo "$(date -Iseconds) Running stats: $TOTAL_TESTS tests, $TOTAL_SUCCESS success ($SUCCESS_RATE%), ${ELAPSED}s elapsed, ${REMAINING}s remaining"
    
    ((BATCH_NUM++))
    
    # Check if we have time for another batch (need at least 15 seconds)
    if [[ $REMAINING -lt 15 ]]; then
        echo "$(date -Iseconds) Less than 15 seconds remaining, ending test"
        break
    fi
    
    # Brief pause between batches
    sleep 1
done

echo ""
echo "$(date -Iseconds) ===== FINAL RESULTS ====="
echo "Duration: $DURATION_MINUTES minutes"
echo "Batches completed: $((BATCH_NUM - 1))"
echo "Total tests: $TOTAL_TESTS"
echo "Total successful: $TOTAL_SUCCESS"
echo "Total failed: $TOTAL_FAILED"
echo "Success rate: $((TOTAL_SUCCESS * 100 / TOTAL_TESTS))%"

# Check cleanup
if pgrep -f "mocks3server\|MockS3Server" >/dev/null 2>&1; then
    ORPHANED=$(pgrep -f "mocks3server\|MockS3Server" | wc -l)
    echo "WARNING: $ORPHANED orphaned processes found"
    pkill -f "mocks3server\|MockS3Server" 2>/dev/null || true
else
    echo "✅ Perfect cleanup - no orphaned processes"
fi

echo "$(date -Iseconds) Continuous test completed!"
echo ""
echo "Check logs: ls /tmp/continuous_*.log"
echo "Check port conflicts: grep -l 'Port.*already in use' /tmp/continuous_*.log | wc -l"