#!/bin/bash

# Long-running correctness test wrapper for Joshua/TestHarness2
# Runs long-running simulation tests (extended duration, no sim speedup).

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="detect_leaks=0"

if [ -z "${JOSHUA_SEED}" ]; then
    echo "FATAL: JOSHUA_SEED environment variable is required" >&2
    echo '<Test Ok="0" Error="InternalError"><JoshuaMessage Severity="40" Message="JOSHUA_SEED environment variable is not set"/></Test>'
    exit 1
fi

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"

# Setup run temp directory (required by TestHarness2)
TH_OUTPUT_BASE_DIR="${TH_OUTPUT_DIR:-${DIAG_LOG_DIR:-/tmp}}"
RUN_TEMP_DIR="${TH_OUTPUT_BASE_DIR}/th_longrunning_${JOSHUA_SEED}"
mkdir -p "${RUN_TEMP_DIR}" || { echo "FATAL: Failed to create ${RUN_TEMP_DIR}" >&2; exit 1; }

python3 -m test_harness.app \
    --joshua-seed "${JOSHUA_SEED}" \
    --old-binaries-path "${OLDBINDIR}" \
    --long-running \
    --run-temp-dir "${RUN_TEMP_DIR}" \
    2> "${RUN_TEMP_DIR}/python_app_stderr.log"
