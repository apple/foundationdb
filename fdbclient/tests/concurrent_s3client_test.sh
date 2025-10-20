#!/usr/bin/env bash
#
# Test script for concurrent s3client_test.sh execution to verify:
# 1. Port conflict handling works correctly
# 2. All processes get cleaned up properly
# 3. Multiple concurrent instances can run simultaneously
#
# Usage: ./concurrent_s3client_test.sh <build_dir> [max_concurrent] [test_duration]
#

set -euo pipefail

# Configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly S3CLIENT_TEST="${SCRIPT_DIR}/s3client_test.sh"
readonly LOG_PREFIX="concurrent_s3test"
readonly DEFAULT_MAX_CONCURRENT=5
readonly DEFAULT_TEST_DURATION=60
readonly MAX_PORT_RETRIES=10
readonly BASE_PORT=8080

# Global arrays to track test instances
declare -a TEST_PIDS=()
declare -a TEST_LOGS=()
declare -a TEST_SCRATCH_DIRS=()

# Test statistics
TESTS_STARTED=0
TESTS_COMPLETED=0
TESTS_FAILED=0
TESTS_KILLED=0
PORT_CONFLICTS_DETECTED=0

# Cleanup function
function cleanup() {
    echo "$(date -Iseconds) Cleaning up concurrent test..."
    
    # Kill any remaining test processes
    if [[ ${#TEST_PIDS[@]} -gt 0 ]]; then
        for pid in "${TEST_PIDS[@]}"; do
            if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
                echo "$(date -Iseconds) Killing test process $pid"
                kill -TERM "$pid" 2>/dev/null || true
                sleep 2
                if kill -0 "$pid" 2>/dev/null; then
                    kill -KILL "$pid" 2>/dev/null || true
                fi
            fi
        done
    fi
    
    # Wait for processes to clean up
    sleep 3
    
    # Check for orphaned MockS3Server processes
    echo "$(date -Iseconds) Checking for orphaned MockS3Server processes..."
    if pgrep -f "mocks3server\|MockS3Server" > /dev/null; then
        echo "$(date -Iseconds) WARNING: Found orphaned MockS3Server processes:"
        pgrep -f -l "mocks3server\|MockS3Server" || true
        echo "$(date -Iseconds) Killing orphaned processes..."
        pkill -f "mocks3server\|MockS3Server" 2>/dev/null || true
    fi
    
    # Generate final report
    generate_report
}

# Set up trap handlers
trap cleanup EXIT
trap "echo 'Interrupted by signal'; exit 1" HUP INT PIPE QUIT TERM

# Logging function
function log() {
    echo "$(date -Iseconds) [CONCURRENT_TEST] $*"
}

# Error logging function
function error() {
    echo "$(date -Iseconds) [CONCURRENT_TEST] ERROR: $*" >&2
}

# Check if a port is in use
function is_port_in_use() {
    local port="$1"
    if command -v netstat >/dev/null 2>&1; then
        netstat -an | grep -q ":${port}.*LISTEN" 2>/dev/null
    elif command -v ss >/dev/null 2>&1; then
        ss -an | grep -q ":${port}.*LISTEN" 2>/dev/null
    else
        # Fallback: try to connect
        timeout 1 bash -c "</dev/tcp/127.0.0.1/${port}" 2>/dev/null
    fi
}

# Monitor port usage during test
function monitor_ports() {
    local duration="$1"
    local end_time=$(($(date +%s) + duration))
    local ports_file=$(mktemp)
    
    log "Monitoring port usage for ${duration} seconds..."
    
    while [[ $(date +%s) -lt $end_time ]]; do
        local ports_in_use=""
        for port in $(seq $BASE_PORT $((BASE_PORT + MAX_PORT_RETRIES))); do
            if is_port_in_use "$port"; then
                ports_in_use="$ports_in_use $port"
            fi
        done
        echo "$(date +%s): $ports_in_use" >> "$ports_file"
        sleep 2
    done
    
    # Analyze port usage patterns
    log "Port usage analysis:"
    local max_concurrent=0
    while IFS=': ' read -r timestamp ports; do
        local count=$(echo "$ports" | wc -w)
        if [[ $count -gt $max_concurrent ]]; then
            max_concurrent=$count
        fi
    done < "$ports_file"
    
    log "Maximum concurrent ports used: $max_concurrent"
    rm -f "$ports_file"
}

# Start a background port monitor
function start_port_monitor() {
    local duration="$1"
    {
        monitor_ports "$duration"
    } &
    local monitor_pid=$!
    echo "$monitor_pid"
}

# Create a port occupier process
function occupy_port() {
    local port="$1"
    local duration="$2"
    
    log "Occupying port $port for $duration seconds"
    
    # Use netcat or socat to bind to port
    if command -v nc >/dev/null 2>&1; then
        timeout "$duration" nc -l -p "$port" >/dev/null 2>&1 &
    elif command -v socat >/dev/null 2>&1; then
        timeout "$duration" socat TCP-LISTEN:"$port",reuseaddr,fork /dev/null &
    else
        # Fallback: use Python
        timeout "$duration" python3 -c "
import socket
import time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', $port))
s.listen(1)
time.sleep($duration)
" &
    fi
    
    local occupier_pid=$!
    log "Port $port occupier started with PID $occupier_pid"
    echo "$occupier_pid"
}

# Run a single s3client_test.sh instance
function run_test_instance() {
    local instance_id="$1"
    local build_dir="$2"
    local scratch_base="$3"
    
    local instance_scratch="${scratch_base}/instance_${instance_id}"
    local log_file="${instance_scratch}/test.log"
    
    mkdir -p "$instance_scratch"
    TEST_SCRATCH_DIRS+=("$instance_scratch")
    TEST_LOGS+=("$log_file")
    
    log "Starting test instance $instance_id with scratch dir: $instance_scratch"
    
    # Set environment to avoid using real S3
    export USE_S3=false
    
    # Run the test with output redirected to log file
    if "$S3CLIENT_TEST" "$build_dir" "$instance_scratch" >"$log_file" 2>&1; then
        log "Test instance $instance_id completed successfully"
        ((TESTS_COMPLETED++))
        return 0
    else
        local exit_code=$?
        error "Test instance $instance_id failed with exit code $exit_code"
        ((TESTS_FAILED++))
        return $exit_code
    fi
}

# Start test instances with staggered delays
function start_test_instances() {
    local max_concurrent="$1"
    local build_dir="$2"
    local scratch_base="$3"
    local stagger_delay="$4"
    
    log "Starting $max_concurrent test instances with ${stagger_delay}s stagger delay"
    
    for i in $(seq 1 "$max_concurrent"); do
        log "Starting test instance $i"
        
        # Run test instance in background
        run_test_instance "$i" "$build_dir" "$scratch_base" &
        local pid=$!
        TEST_PIDS+=("$pid")
        ((TESTS_STARTED++))
        
        log "Test instance $i started with PID $pid"
        
        # Stagger the starts to increase chance of port conflicts
        if [[ $i -lt $max_concurrent ]]; then
            sleep "$stagger_delay"
        fi
    done
}

# Wait for test instances to complete
function wait_for_tests() {
    local timeout="$1"
    local end_time=$(($(date +%s) + timeout))
    
    log "Waiting for test instances to complete (timeout: ${timeout}s)"
    
    while [[ $(date +%s) -lt $end_time ]]; do
        local running_count=0
        
        for pid in "${TEST_PIDS[@]}"; do
            if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
                ((running_count++))
            fi
        done
        
        if [[ $running_count -eq 0 ]]; then
            log "All test instances completed"
            return 0
        fi
        
        log "Still waiting for $running_count test instances..."
        sleep 5
    done
    
    # Timeout reached - kill remaining processes
    log "Timeout reached - killing remaining test processes"
    for pid in "${TEST_PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            log "Killing test process $pid"
            kill -TERM "$pid" 2>/dev/null || true
            ((TESTS_KILLED++))
        fi
    done
    
    return 1
}

# Analyze test results
function analyze_results() {
    log "Analyzing test results..."
    
    # Check for port conflict messages in logs
    if [[ ${#TEST_LOGS[@]} -gt 0 ]]; then
        for log_file in "${TEST_LOGS[@]}"; do
            if [[ -f "$log_file" ]]; then
                if grep -q "Port.*already in use\|address.*in use\|bind.*failed" "$log_file"; then
                    ((PORT_CONFLICTS_DETECTED++))
                    log "Port conflict detected in: $log_file"
                fi
            fi
        done
    fi
    
    # Check for orphaned processes
    local orphaned_count=0
    if pgrep -f "mocks3server\|MockS3Server" >/dev/null; then
        orphaned_count=$(pgrep -f "mocks3server\|MockS3Server" | wc -l)
        error "Found $orphaned_count orphaned MockS3Server processes"
    fi
    
    # Generate summary
    log "Test Summary:"
    log "  Tests Started: $TESTS_STARTED"
    log "  Tests Completed: $TESTS_COMPLETED"
    log "  Tests Failed: $TESTS_FAILED"
    log "  Tests Killed: $TESTS_KILLED"
    log "  Port Conflicts Detected: $PORT_CONFLICTS_DETECTED"
    log "  Orphaned Processes: $orphaned_count"
    
    # Determine overall result
    if [[ $TESTS_COMPLETED -eq $TESTS_STARTED && $orphaned_count -eq 0 ]]; then
        log "OVERALL RESULT: SUCCESS - All tests completed successfully with proper cleanup"
        return 0
    elif [[ $TESTS_COMPLETED -gt 0 && $orphaned_count -eq 0 ]]; then
        log "OVERALL RESULT: PARTIAL SUCCESS - Some tests completed successfully with proper cleanup"
        return 1
    else
        log "OVERALL RESULT: FAILURE - Tests failed or cleanup issues detected"
        return 2
    fi
}

# Generate detailed report
function generate_report() {
    local report_file="${TMPDIR:-/tmp}/concurrent_s3client_test_report.txt"
    
    {
        echo "=== Concurrent S3Client Test Report ==="
        echo "Date: $(date -Iseconds)"
        echo "Tests Started: $TESTS_STARTED"
        echo "Tests Completed: $TESTS_COMPLETED" 
        echo "Tests Failed: $TESTS_FAILED"
        echo "Tests Killed: $TESTS_KILLED"
        echo "Port Conflicts Detected: $PORT_CONFLICTS_DETECTED"
        echo ""
        
        echo "=== Test Instance Details ==="
        for i in "${!TEST_PIDS[@]}"; do
            local pid="${TEST_PIDS[$i]}"
            local log_file=""
            local scratch_dir=""
            
            if [[ $i -lt ${#TEST_LOGS[@]} ]]; then
                log_file="${TEST_LOGS[$i]}"
            fi
            
            if [[ $i -lt ${#TEST_SCRATCH_DIRS[@]} ]]; then
                scratch_dir="${TEST_SCRATCH_DIRS[$i]}"
            fi
            
            echo "Instance $((i+1)):"
            echo "  PID: $pid"
            echo "  Log: $log_file"
            echo "  Scratch: $scratch_dir"
            
            if [[ -f "$log_file" ]]; then
                echo "  Status: $(if grep -q "PASSED\|SUCCESS" "$log_file"; then echo "PASSED"; else echo "FAILED"; fi)"
                echo "  Log size: $(wc -l < "$log_file") lines"
            else
                echo "  Status: NO LOG FILE"
            fi
            echo ""
        done
        
        echo "=== Port Usage Analysis ==="
        for port in $(seq $BASE_PORT $((BASE_PORT + MAX_PORT_RETRIES))); do
            if [[ ${#TEST_LOGS[@]} -gt 0 ]] && grep -q ":$port" "${TEST_LOGS[@]}" 2>/dev/null; then
                echo "Port $port: Used"
            fi
        done
        
    } > "$report_file"
    
    log "Detailed report written to: $report_file"
}

# Test with pre-occupied ports
function test_with_occupied_ports() {
    local build_dir="$1"
    local scratch_base="$2"
    local num_to_occupy="$3"
    local occupy_duration="$4"
    
    log "Testing with $num_to_occupy pre-occupied ports"
    
    # Start port occupiers
    local occupier_pids=()
    for i in $(seq 0 $((num_to_occupy - 1))); do
        local port=$((BASE_PORT + i))
        local occupier_pid=$(occupy_port "$port" "$occupy_duration")
        occupier_pids+=("$occupier_pid")
    done
    
    # Wait for ports to be occupied
    sleep 2
    
    # Start test instances
    start_test_instances 3 "$build_dir" "$scratch_base" 1
    
    # Wait for tests to complete
    wait_for_tests 120
    
    # Clean up port occupiers
    for pid in "${occupier_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
}

# Signal handling test
function test_signal_handling() {
    local build_dir="$1"
    local scratch_base="$2"
    
    log "Testing signal handling and cleanup"
    
    # Start a few test instances
    start_test_instances 3 "$build_dir" "$scratch_base" 1
    
    # Let them run for a bit
    sleep 10
    
    # Send SIGTERM to the test processes
    log "Sending SIGTERM to test processes"
    for pid in "${TEST_PIDS[@]}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    # Wait for cleanup
    sleep 5
    
    # Check for orphaned processes
    if pgrep -f "mocks3server\|MockS3Server" >/dev/null; then
        error "Signal handling test failed: Found orphaned processes after SIGTERM"
        return 1
    else
        log "Signal handling test passed: No orphaned processes found"
        return 0
    fi
}

# Main function
function main() {
    local build_dir="${1:-}"
    local max_concurrent="${2:-$DEFAULT_MAX_CONCURRENT}"
    local test_duration="${3:-$DEFAULT_TEST_DURATION}"
    
    # Validate arguments
    if [[ -z "$build_dir" ]]; then
        echo "Usage: $0 <build_dir> [max_concurrent] [test_duration]"
        echo "Example: $0 ~/build_output 5 60"
        exit 1
    fi
    
    if [[ ! -d "$build_dir" ]]; then
        error "Build directory does not exist: $build_dir"
        exit 1
    fi
    
    if [[ ! -f "$S3CLIENT_TEST" ]]; then
        error "s3client_test.sh not found at: $S3CLIENT_TEST"
        exit 1
    fi
    
    if [[ ! -x "$S3CLIENT_TEST" ]]; then
        error "s3client_test.sh is not executable: $S3CLIENT_TEST"
        exit 1
    fi
    
    # Create test scratch directory
    local scratch_base
    scratch_base=$(mktemp -d "${TMPDIR:-/tmp}/concurrent_s3client_test.XXXXXX")
    
    log "Starting concurrent s3client test"
    log "Build directory: $build_dir"
    log "Max concurrent: $max_concurrent"
    log "Test duration: $test_duration seconds"
    log "Scratch base: $scratch_base"
    
    # Run different test scenarios
    log "=== Test 1: Basic concurrent execution ==="
    
    # Start port monitor in background - simplified
    monitor_ports 120 &
    local monitor_pid=$!
    
    start_test_instances "$max_concurrent" "$build_dir" "$scratch_base" 2
    wait_for_tests "$test_duration"
    
    # Reset for next test
    TEST_PIDS=()
    TEST_LOGS=()
    TEST_SCRATCH_DIRS=()
    
    log "=== Test 2: Port occupation simulation ==="
    test_with_occupied_ports "$build_dir" "$scratch_base" 3 30
    
    # Reset for next test  
    TEST_PIDS=()
    TEST_LOGS=()
    TEST_SCRATCH_DIRS=()
    
    log "=== Test 3: Signal handling test ==="
    test_signal_handling "$build_dir" "$scratch_base"
    
    # Stop port monitor
    if kill -0 "$monitor_pid" 2>/dev/null; then
        kill "$monitor_pid" 2>/dev/null || true
    fi
    
    # Analyze results and generate report
    analyze_results
    local result=$?
    
    # Cleanup scratch directory
    rm -rf "$scratch_base"
    
    exit $result
}

# Run main function with all arguments
main "$@"