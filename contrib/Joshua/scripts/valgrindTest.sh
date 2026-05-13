#!/bin/bash

# Valgrind test wrapper for Joshua/TestHarness2
# Runs simulation tests under valgrind for memory error detection.

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"

# Setup run temp directory (required by TestHarness2)
RUN_TEMP_DIR="/tmp/th_valgrind_${JOSHUA_SEED}"
mkdir -p "${RUN_TEMP_DIR}"

python3 -m test_harness.app \
    --joshua-seed "${JOSHUA_SEED}" \
    --old-binaries-path "${OLDBINDIR}" \
    --use-valgrind \
    --run-temp-dir "${RUN_TEMP_DIR}" \
    2> "${RUN_TEMP_DIR}/python_app_stderr.log"
