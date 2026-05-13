#!/bin/bash

# Long-running correctness test wrapper for Joshua/TestHarness2
# Runs long-running simulation tests (extended duration, no sim speedup).

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="detect_leaks=0"

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"

# Setup run temp directory (required by TestHarness2)
RUN_TEMP_DIR="/tmp/th_longrunning_${JOSHUA_SEED}"
mkdir -p "${RUN_TEMP_DIR}"

python3 -m test_harness.app \
    --joshua-seed "${JOSHUA_SEED}" \
    --old-binaries-path "${OLDBINDIR}" \
    --long-running \
    --run-temp-dir "${RUN_TEMP_DIR}" \
    2> "${RUN_TEMP_DIR}/python_app_stderr.log"
