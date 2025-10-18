#!/usr/bin/env bash

# Mock S3 Server fixture for ctests
# Replaces SeaweedFS with in-process MockS3Server
#
# Usage:
#   1. Source this script:
#      source fdbclient/tests/mocks3_fixture.sh
#
#   2. Start the server (optionally specify build directory):
#      start_mocks3 [/path/to/build_output]
#      # If no path provided, searches for build_output automatically
#
#   3. Get the blobstore URL:
#      URL=$(get_mocks3_url)
#      # Returns: blobstore://mocks3:mocksecret@127.0.0.1:8080
#
#   4. Shutdown the server:
#      shutdown_mocks3
#
# Environment Variables:
#   MOCKS3_HOST - Server host (default: 127.0.0.1)
#   MOCKS3_PORT - Starting port to try (default: 8080)
#                 Script will auto-increment if port is unavailable
#
# Example standalone usage:
#   $ source fdbclient/tests/mocks3_fixture.sh
#   $ start_mocks3 ~/build_output
#   Starting MockS3Server on 127.0.0.1:8080 (attempt 1/10)
#   MockS3Server ready on port 8080
#   $ URL=$(get_mocks3_url)
#   $ echo $URL
#   blobstore://mocks3:mocksecret@127.0.0.1:8080
#   $ # ... run your tests ...
#   $ shutdown_mocks3
#   Shutting down MockS3Server (PID: 12345)

set -e

MOCKS3_HOST=${MOCKS3_HOST:-127.0.0.1}
MOCKS3_PORT=${MOCKS3_PORT:-8080}
MOCKS3_PID=""
MOCKS3_LOG_FILE=""

start_mocks3() {
    local build_dir_param="${1:-}"
    local max_attempts=10
    local attempt

    # Use provided build directory or try to find it
    BUILD_DIR="$build_dir_param"
    if [ -z "$BUILD_DIR" ]; then
        # Find the build directory - look for fdbserver binary
        for dir in "../build_output" "../../build_output" "../../../build_output" "./build_output" "."; do
            if [ -x "$dir/bin/fdbserver" ]; then
                BUILD_DIR="$dir"
                break
            fi
        done
    fi

    if [ -z "$BUILD_DIR" ]; then
        echo "ERROR: Could not find fdbserver binary to run MockS3Server"
        return 1
    fi

    if [ ! -f "$BUILD_DIR/bin/fdbserver" ]; then
        echo "ERROR: fdbserver binary not found at $BUILD_DIR/bin/fdbserver"
        return 1
    fi

    # Try to start MockS3Server with port retry logic
    # We try to bind directly rather than pre-checking with netstat
    # to avoid race conditions (TOCTOU)
    for attempt in $(seq 1 $max_attempts); do
        echo "Starting MockS3Server on ${MOCKS3_HOST}:${MOCKS3_PORT} (attempt $attempt/$max_attempts)"

        # Create temporary log file to capture stderr
        MOCKS3_LOG_FILE=$(mktemp /tmp/mocks3.XXXXXX)
        
        # Start MockS3Server using fdbserver with the new role
        # Redirect stderr to log file to detect bind failures
        # Use >| to force overwrite even if noclobber is set
        "$BUILD_DIR/bin/fdbserver" \
            --role mocks3server \
            --public-address "${MOCKS3_HOST}:${MOCKS3_PORT}" \
            --listen-address "${MOCKS3_HOST}:${MOCKS3_PORT}" \
            2>|"$MOCKS3_LOG_FILE" &
        MOCKS3_PID=$!

        # Wait briefly for server to start or fail
        local ready=false
        local bind_failed=false

        for i in {1..10}; do
            # Check if the process is still running
            if ! kill -0 $MOCKS3_PID 2>/dev/null; then
                # Process died - check if it was due to bind failure
                # Match various formats: "address already in use", "Local address in use", etc.
                if grep -q -i "address.*in use\|.*address in use\|bind.*failed\|cannot assign requested address" "$MOCKS3_LOG_FILE" 2>/dev/null; then
                    echo "Port ${MOCKS3_PORT} already in use, trying next port"
                    bind_failed=true
                else
                    echo "ERROR: MockS3Server process died unexpectedly"
                    cat "$MOCKS3_LOG_FILE" >&2
                fi
                break
            fi

            # Wait a bit for server to be ready
            if [ $i -gt 3 ]; then
                echo "MockS3Server ready on port ${MOCKS3_PORT}"
                ready=true
                break
            fi
            sleep 1
        done

        if [ "$ready" = true ]; then
            return 0
        fi

        # Clean up the log file if we're retrying
        rm -f "$MOCKS3_LOG_FILE"

        # If we get here, the server failed to start
        # Kill any remaining process and try next port
        if kill -0 $MOCKS3_PID 2>/dev/null; then
            kill $MOCKS3_PID 2>/dev/null || true
            wait $MOCKS3_PID 2>/dev/null || true
        fi

        # Only retry if it was a bind failure
        if [ "$bind_failed" = false ]; then
            echo "ERROR: MockS3Server failed to start (not a port conflict)"
            return 1
        fi

        # Try next port for next attempt
        MOCKS3_PORT=$((MOCKS3_PORT + 1))
    done

    echo "ERROR: MockS3Server failed to start after $max_attempts attempts"
    return 1
}

shutdown_mocks3() {
    if [ -n "$MOCKS3_PID" ]; then
        echo "Shutting down MockS3Server (PID: $MOCKS3_PID)"
        # Try graceful shutdown first
        if kill -0 $MOCKS3_PID 2>/dev/null; then
            kill $MOCKS3_PID 2>/dev/null || true
            # Wait up to 5 seconds for graceful shutdown
            for i in {1..5}; do
                if ! kill -0 $MOCKS3_PID 2>/dev/null; then
                    break
                fi
                sleep 1
            done
            # Force kill if still running
            if kill -0 $MOCKS3_PID 2>/dev/null; then
                echo "Force killing MockS3Server (PID: $MOCKS3_PID)"
                kill -9 $MOCKS3_PID 2>/dev/null || true
            fi
            wait $MOCKS3_PID 2>/dev/null || true
        fi
    fi

    # Clean up log file
    if [ -n "$MOCKS3_LOG_FILE" ] && [ -f "$MOCKS3_LOG_FILE" ]; then
        rm -f "$MOCKS3_LOG_FILE"
    fi
}

get_mocks3_url() {
    echo "blobstore://mocks3:mocksecret@${MOCKS3_HOST}:${MOCKS3_PORT}"
}

# Note: No EXIT trap here - main scripts handle cleanup via their own traps
# This prevents conflicts with main script trap handlers

# Export functions for use by test scripts
export -f start_mocks3
export -f shutdown_mocks3  
export -f get_mocks3_url

