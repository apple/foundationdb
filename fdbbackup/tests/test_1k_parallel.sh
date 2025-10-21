#!/usr/bin/env bash

# 1K s3_backup_test.sh with LIMITED parallelism (2 concurrent instances)
# Based on proven simple_s3backup_test.sh pattern that works

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TOTAL_TESTS=1000
readonly MAX_CONCURRENT=2  # CRITICAL: Only 2 concurrent safe! 3+ causes failures!
readonly SOURCE_DIR="/Users/stack/checkouts/fdb/foundationdb"  
readonly BUILD_DIR="/Users/stack/build_output"
readonly LOG_PREFIX="/tmp/s3backup_1k_parallel"

START_TIME=$(date)
START_TIMESTAMP=$(date +%s)  # Store as timestamp for macOS compatibility
COMPLETED_TESTS=0
declare -a RUNNING_PIDS=()
declare -a RUNNING_IDS=()

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Function to run a single test in background
start_test() {
    local test_id="$1"
    local scratch_dir logfile
    
    scratch_dir=$(mktemp -d "/tmp/s3backup_parallel_${test_id}.XXXXXX")
    logfile="${LOG_PREFIX}_${test_id}.log"
    
    # Run s3_backup_test.sh in background
    (
        "${SCRIPT_DIR}/s3_backup_test.sh" "${SOURCE_DIR}" "${BUILD_DIR}" "${scratch_dir}" &> "${logfile}" || true
        rm -rf "${scratch_dir}" 2>/dev/null || true
    ) &
    
    local pid=$!
    RUNNING_PIDS+=("$pid")
    RUNNING_IDS+=("$test_id")
    
    log "Started test ${test_id}/${TOTAL_TESTS} (PID: $pid)"
}

# Function to wait for one test to complete
wait_for_completion() {
    # Wait for any background job to complete
    wait -n
    local exit_code=$?
    
    # Find which process completed
    local completed_idx=-1
    for i in "${!RUNNING_PIDS[@]}"; do
        local pid="${RUNNING_PIDS[$i]}"
        if ! kill -0 "$pid" 2>/dev/null; then
            completed_idx=$i
            break
        fi
    done
    
    if [[ $completed_idx -ge 0 ]]; then
        local test_id="${RUNNING_IDS[$completed_idx]}"
        local logfile="${LOG_PREFIX}_${test_id}.log"
        
        # Check if test passed by looking for "PASSED" in log
        if grep -q "PASSED test_s3_backup_and_restore" "${logfile}"; then
            COMPLETED_TESTS=$((COMPLETED_TESTS + 1))
            log "✅ Test ${test_id} PASSED (${COMPLETED_TESTS}/${TOTAL_TESTS})"
        else
            log "❌ Test ${test_id} FAILED - stopping execution"
            log "Check log: ${logfile}"
            
            # Kill remaining background processes
            for pid in "${RUNNING_PIDS[@]}"; do
                kill "$pid" 2>/dev/null || true
            done
            exit 1
        fi
        
        # Remove completed process from arrays
        unset RUNNING_PIDS[$completed_idx]
        unset RUNNING_IDS[$completed_idx]
        RUNNING_PIDS=("${RUNNING_PIDS[@]}")  # Reindex array
        RUNNING_IDS=("${RUNNING_IDS[@]}")    # Reindex array
        
        # Progress update every 50 tests
        if [[ $((COMPLETED_TESTS % 50)) -eq 0 ]]; then
            local elapsed=$(($(date +%s) - START_TIMESTAMP))
            local rate=$(( COMPLETED_TESTS * 60 / elapsed ))
            log "Progress: ${COMPLETED_TESTS}/${TOTAL_TESTS} completed (${rate} tests/minute)"
        fi
    fi
}

log "=== S3 Backup Test 1K Parallel Runner ==="
log "Total tests to run: ${TOTAL_TESTS}"
log "Maximum concurrency: ${MAX_CONCURRENT} (CRITICAL LIMIT - DO NOT INCREASE)"
log "⚠️  WARNING: Each s3_backup_test.sh runs full FDB cluster (very resource intensive)"
log "⚠️  WARNING: 3+ concurrent instances cause failures due to resource exhaustion"
log "Start time: ${START_TIME}"

# Clean up any existing log files
rm -f "${LOG_PREFIX}"_*.log 2>/dev/null || true

# Run tests with limited parallelism
for test_id in $(seq 1 $TOTAL_TESTS); do
    # Start new test if we have capacity
    if [[ ${#RUNNING_PIDS[@]} -lt $MAX_CONCURRENT ]]; then
        start_test "$test_id"
        
        # Small delay to stagger startups
        sleep 3
    fi
    
    # If we're at capacity, wait for one to complete
    if [[ ${#RUNNING_PIDS[@]} -ge $MAX_CONCURRENT ]]; then
        wait_for_completion
    fi
done

# Wait for remaining tests to complete
log "Waiting for final ${#RUNNING_PIDS[@]} tests to complete..."
while [[ ${#RUNNING_PIDS[@]} -gt 0 ]]; do
    wait_for_completion
done

# Final report
end_time=$(date)
total_elapsed=$(($(date +%s) - START_TIMESTAMP))

log "🎉 ALL ${TOTAL_TESTS} TESTS COMPLETED SUCCESSFULLY! 🎉"
log "Total time: $((total_elapsed / 3600))h $(((total_elapsed % 3600) / 60))m $((total_elapsed % 60))s"
log "Success rate: 100%"
log "Parallelism: ${MAX_CONCURRENT} concurrent tests"

# Generate proof file
proof_file="/tmp/s3backup_1k_parallel_proof.txt"
{
    echo "=== S3 Backup Test 1K Parallel Execution Proof ==="
    echo "Start Time: ${START_TIME}"
    echo "End Time: ${end_time}"
    echo "Total Duration: $((total_elapsed / 3600))h $(((total_elapsed % 3600) / 60))m $((total_elapsed % 60))s"
    echo "Tests Completed: ${COMPLETED_TESTS}/${TOTAL_TESTS}"
    echo "Concurrency Level: ${MAX_CONCURRENT}"
    echo "Success Rate: 100%"
    echo "Log Files: $(ls "${LOG_PREFIX}"_*.log | wc -l)"
    echo "Average Rate: $(( COMPLETED_TESTS * 3600 / total_elapsed )) tests/hour"
    echo "Completion: $(date)"
} > "${proof_file}"

log "Proof written to: ${proof_file}"
exit 0
