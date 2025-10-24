#!/usr/bin/env bash
#
# Continuous stress test: Run batches of 10 concurrent s3client_test.sh instances
# for 5 minutes, restarting immediately when batches complete
#
set -euo pipefail

BUILD_DIR="/Users/stack/build_output"
CONCURRENT_INSTANCES=10
TEST_DURATION_MINUTES=5
TEST_DURATION_SECONDS=$((TEST_DURATION_MINUTES * 60))

# Statistics tracking
TOTAL_BATCHES=0
TOTAL_INSTANCES=0
TOTAL_SUCCESSFUL=0
TOTAL_FAILED=0
TOTAL_PORT_CONFLICTS=0
START_TIME=$(date +%s)
END_TIME=$((START_TIME + TEST_DURATION_SECONDS))

echo "$(date -Iseconds) Starting continuous stress test for $TEST_DURATION_MINUTES minutes"
echo "$(date -Iseconds) Each batch: $CONCURRENT_INSTANCES concurrent instances"
echo "$(date -Iseconds) Will run until: $(date -Iseconds -r $END_TIME)"

# Function to run a single batch of concurrent tests
run_batch() {
    local batch_num="$1"
    local batch_start_time=$(date +%s)
    
    echo "$(date -Iseconds) ===== BATCH $batch_num: Starting $CONCURRENT_INSTANCES concurrent instances ====="
    
    # Array to track PIDs for this batch
    local PIDS=()
    local batch_successful=0
    local batch_failed=0
    local batch_port_conflicts=0
    
    # Start concurrent instances
    for i in $(seq 1 $CONCURRENT_INSTANCES); do
        local instance_id="${batch_num}_${i}"
        SCRATCH_DIR=$(mktemp -d "/tmp/stress_${instance_id}.XXXXXX")
        
        # Run s3client_test.sh in background
        (
            cd fdbclient/tests
            if ./s3client_test.sh "$BUILD_DIR" "$SCRATCH_DIR" > "/tmp/stress_${instance_id}.log" 2>&1; then
                echo "$(date -Iseconds) Instance ${instance_id} SUCCESS"
            else
                echo "$(date -Iseconds) Instance ${instance_id} FAILED"
            fi
        ) &
        
        PIDS+=($!)
        
        # Very brief stagger to increase port conflicts
        sleep 0.1
    done
    
    echo "$(date -Iseconds) Batch $batch_num: All instances started, waiting for completion..."
    
    # Wait for all instances in this batch
    for i in "${!PIDS[@]}"; do
        local pid=${PIDS[$i]}
        local instance_id="${batch_num}_$((i+1))"
        
        if wait "$pid"; then
            ((batch_successful++))
            echo "$(date -Iseconds) Instance ${instance_id} (PID $pid) completed successfully"
        else
            ((batch_failed++))
            echo "$(date -Iseconds) Instance ${instance_id} (PID $pid) FAILED"
        fi
    done
    
    # Check for port conflicts in this batch
    for i in $(seq 1 $CONCURRENT_INSTANCES); do
        local instance_id="${batch_num}_${i}"
        if [[ -f "/tmp/stress_${instance_id}.log" ]] && grep -q "Port.*already in use\|trying next port" "/tmp/stress_${instance_id}.log"; then
            ((batch_port_conflicts++))
        fi
    done
    
    local batch_end_time=$(date +%s)
    local batch_duration=$((batch_end_time - batch_start_time))
    
    # Update global statistics
    ((TOTAL_BATCHES++))
    TOTAL_INSTANCES=$((TOTAL_INSTANCES + CONCURRENT_INSTANCES))
    TOTAL_SUCCESSFUL=$((TOTAL_SUCCESSFUL + batch_successful))
    TOTAL_FAILED=$((TOTAL_FAILED + batch_failed))
    TOTAL_PORT_CONFLICTS=$((TOTAL_PORT_CONFLICTS + batch_port_conflicts))
    
    echo "$(date -Iseconds) Batch $batch_num completed in ${batch_duration}s: ${batch_successful}/${CONCURRENT_INSTANCES} successful, $batch_port_conflicts port conflicts"
    
    # Check for orphaned processes
    local orphaned_count=0
    if pgrep -f "mocks3server\|MockS3Server" >/dev/null 2>&1; then
        orphaned_count=$(pgrep -f "mocks3server\|MockS3Server" | wc -l)
        echo "$(date -Iseconds) WARNING: $orphaned_count orphaned MockS3Server processes detected!"
        # Kill orphaned processes
        pkill -f "mocks3server\|MockS3Server" 2>/dev/null || true
    fi
    
    # Print running statistics
    local current_time=$(date +%s)
    local elapsed_time=$((current_time - START_TIME))
    local remaining_time=$((END_TIME - current_time))
    
    echo "$(date -Iseconds) === RUNNING STATS (${elapsed_time}s elapsed, ${remaining_time}s remaining) ==="
    echo "  Batches: $TOTAL_BATCHES"
    echo "  Total instances: $TOTAL_INSTANCES"
    echo "  Successful: $TOTAL_SUCCESSFUL ($((TOTAL_SUCCESSFUL * 100 / TOTAL_INSTANCES))%)"
    echo "  Failed: $TOTAL_FAILED"
    echo "  Port conflicts: $TOTAL_PORT_CONFLICTS"
    echo "  Success rate: $((TOTAL_SUCCESSFUL * 100 / TOTAL_INSTANCES))%"
    echo ""
}

# Main stress test loop
batch_counter=1
while [[ $(date +%s) -lt $END_TIME ]]; do
    # Check if we have enough time for another batch (estimate 30 seconds per batch)
    current_time=$(date +%s)
    remaining_time=$((END_TIME - current_time))
    
    if [[ $remaining_time -lt 30 ]]; then
        echo "$(date -Iseconds) Less than 30 seconds remaining, ending stress test"
        break
    fi
    
    run_batch "$batch_counter"
    ((batch_counter++))
    
    # Brief pause between batches to avoid overwhelming the system
    sleep 2
done

# Final statistics
echo "$(date -Iseconds) ===== FINAL STRESS TEST RESULTS ====="
echo "Test duration: $TEST_DURATION_MINUTES minutes"
echo "Total batches completed: $TOTAL_BATCHES"
echo "Total instances run: $TOTAL_INSTANCES"
echo "Total successful: $TOTAL_SUCCESSFUL"
echo "Total failed: $TOTAL_FAILED"
echo "Total port conflicts detected: $TOTAL_PORT_CONFLICTS"
echo "Overall success rate: $((TOTAL_SUCCESSFUL * 100 / TOTAL_INSTANCES))%"
echo "Average instances per batch: $((TOTAL_INSTANCES / TOTAL_BATCHES))"
echo "Port conflicts per batch: $((TOTAL_PORT_CONFLICTS / TOTAL_BATCHES))"

# Final cleanup check
if pgrep -f "mocks3server\|MockS3Server" >/dev/null 2>&1; then
    orphaned_count=$(pgrep -f "mocks3server\|MockS3Server" | wc -l)
    echo "WARNING: $orphaned_count orphaned processes remain"
    pkill -f "mocks3server\|MockS3Server" 2>/dev/null || true
else
    echo "✅ Perfect cleanup - no orphaned processes"
fi

echo "$(date -Iseconds) Continuous stress test completed!"

# Provide summary of what logs to check
echo ""
echo "Log files created: /tmp/stress_*.log"
echo "To check individual results: grep 'PASSED\\|FAILED' /tmp/stress_*.log"
echo "To check port conflicts: grep 'Port.*already in use' /tmp/stress_*.log"