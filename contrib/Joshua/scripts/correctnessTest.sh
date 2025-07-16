#!/bin/bash

#
# correctnessTest.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Set defaults for key environment variables
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
export TH_DISABLE_ROCKSDB_CHECK="${TH_DISABLE_ROCKSDB_CHECK:-false}"

# Cleanup function - preserve logs on failure if archival is enabled
cleanup() {
    echo "--- correctnessTest.sh cleanup starting ---" >&2
    
    local preserve_files=false
    local test_failed=false
    
    # Check if test failed by parsing XML output
    if [ -f "${PYTHON_APP_STDOUT_FILE}" ] && grep -q 'Ok="0"' "${PYTHON_APP_STDOUT_FILE}"; then
        test_failed=true
    fi
    
    # Preserve files if:
    # 1. Always preserve flag is set, OR
    # 2. Preserve on success flag is set and test passed, OR
    # 3. (Python crashed OR test failed) AND archival is enabled
    if [ "${TH_PRESERVE_TEMP_DIRS_ON_EXIT}" = "true" ] || \
       ( [ "${TH_PRESERVE_TEMP_DIRS_ON_SUCCESS}" = "true" ] && [ "${test_failed}" = "false" ] ) || \
       ( ([ "${PYTHON_EXIT_CODE}" -ne "0" ] || [ "${test_failed}" = "true" ]) && [ "${TH_ARCHIVE_LOGS_ON_FAILURE}" = "true" ] ); then
        preserve_files=true
    fi
    
    if [ "${preserve_files}" = "true" ]; then
        echo "Preserving test artifacts in: ${TOP_LEVEL_OUTPUT_DIR}" >&2
        if [ "${test_failed}" = "true" ] || [ "${PYTHON_EXIT_CODE}" -ne "0" ]; then
            echo "Test failed - logs preserved for debugging. Use 'kubectl cp' to copy from pod." >&2
        else
            echo "Test passed - logs preserved as requested." >&2
        fi
    else
        echo "Cleaning up test artifacts: ${TOP_LEVEL_OUTPUT_DIR}" >&2
        rm -rf "${TOP_LEVEL_OUTPUT_DIR}"
    fi
}

trap cleanup EXIT

# Setup unique output directory for log preservation
TH_OUTPUT_BASE_DIR="${TH_OUTPUT_DIR:-${DIAG_LOG_DIR:-/tmp}}"

# Use ensemble ID if available, otherwise fall back to joshua seed
if [ -n "${JOSHUA_ENSEMBLE_ID}" ]; then
    UNIQUE_RUN_SUFFIX="${JOSHUA_ENSEMBLE_ID}"
    echo "Using ensemble ID for directory name: ${UNIQUE_RUN_SUFFIX}" >&2
else
    # Try to extract ensemble ID from working directory (like joshua_logtool.py does)
    EXTRACTED_ENSEMBLE_ID=$(echo "${PWD}" | grep -o 'ensembles/[0-9A-Za-z._-]*' | cut -d'/' -f2)
    if [ -n "${EXTRACTED_ENSEMBLE_ID}" ]; then
        UNIQUE_RUN_SUFFIX="${EXTRACTED_ENSEMBLE_ID}"
        echo "Extracted ensemble ID from working directory: ${UNIQUE_RUN_SUFFIX}" >&2
    else
        UNIQUE_RUN_SUFFIX="${JOSHUA_SEED}"
        echo "Using joshua seed for directory name: ${UNIQUE_RUN_SUFFIX}" >&2
    fi
fi

# Collect all logs in here.
TOP_LEVEL_OUTPUT_DIR="${TH_OUTPUT_BASE_DIR}/th_run_${UNIQUE_RUN_SUFFIX}"
APP_RUN_TEMP_DIR="${TOP_LEVEL_OUTPUT_DIR}"

# Create directories
mkdir -p "${APP_RUN_TEMP_DIR}"
if [ ! -d "${APP_RUN_TEMP_DIR}" ]; then
    echo "FATAL: Failed to create required directories" >&2
    exit 1
fi

# Set permissions
chmod 777 "${TOP_LEVEL_OUTPUT_DIR}"

# Validate required environment
if [ -z "${JOSHUA_SEED}" ]; then
    echo "FATAL: JOSHUA_SEED environment variable is required" >&2
    echo '<Test Ok="0" Error="InternalError"><JoshuaMessage Severity="40" Message="JOSHUA_SEED environment variable is not set" /></Test>'
    exit 1
fi

# Build Python command arguments
PYTHON_CMD_ARGS=(
    "--joshua-seed" "${JOSHUA_SEED}"
    "--run-temp-dir" "${APP_RUN_TEMP_DIR}"
    "--no-clean-up"
    "--no-verbose-on-failure"
)

# Add optional arguments
if [ -n "${JOSHUA_TEST_FILES_DIR}" ]; then
    PYTHON_CMD_ARGS+=("--test-source-dir" "${JOSHUA_TEST_FILES_DIR}")
else
    # Default to current working directory + tests if JOSHUA_TEST_FILES_DIR is not set
    # This handles the case where test files are extracted from tarball to current directory
    if [ -d "tests" ]; then
        PYTHON_CMD_ARGS+=("--test-source-dir" "tests")
    elif [ -d "." ]; then
        # Fallback: use current directory if tests/ doesn't exist
        PYTHON_CMD_ARGS+=("--test-source-dir" ".")
    fi
fi

if [ -n "${OLDBINDIR}" ]; then
    PYTHON_CMD_ARGS+=("--old-binaries-path" "${OLDBINDIR}")
else
    PYTHON_CMD_ARGS+=("--old-binaries-path" "/app/deploy/global_data/oldBinaries")
fi

if [ "${TH_ARCHIVE_LOGS_ON_FAILURE}" = "true" ]; then
    PYTHON_CMD_ARGS+=("--archive-logs-on-failure")
fi

# Setup joshua output capture
PYTHON_APP_STDOUT_FILE="${APP_RUN_TEMP_DIR}/python_app_stdout.log"
PYTHON_APP_STDERR_FILE="${APP_RUN_TEMP_DIR}/python_app_stderr.log"

# Execute Python test harness
echo "Executing TestHarness2 with seed ${JOSHUA_SEED}..." >&2
python3 -m test_harness.app "${PYTHON_CMD_ARGS[@]}" 2> "${PYTHON_APP_STDERR_FILE}" | tee "${PYTHON_APP_STDOUT_FILE}"
PYTHON_EXIT_CODE=$?

echo "TestHarness2 execution finished. Exit code: ${PYTHON_EXIT_CODE}" >&2

# Note: stdout is already output via tee, no need to cat the file

# Check if test actually failed
if [ -f "${PYTHON_APP_STDOUT_FILE}" ] && grep -q 'Ok="0"' "${PYTHON_APP_STDOUT_FILE}"; then
    echo "Test result: FAILED" >&2
    TEST_FAILED=true
else
    echo "Test result: PASSED" >&2
    TEST_FAILED=false
fi

# Exit with appropriate code
if [ "${PYTHON_EXIT_CODE}" -ne 0 ]; then
    exit ${PYTHON_EXIT_CODE}
elif [ "${TEST_FAILED}" = "true" ]; then
    exit 1
else
    exit 0
fi