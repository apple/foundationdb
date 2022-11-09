#!/bin/sh

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="detect_leaks=0"

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/snowflakeBinaries}"
#mono bin/TestHarness.exe joshua-run "${OLDBINDIR}" false

python3 -m test_harness.app -s ${JOSHUA_SEED} --old-binaries-path ${OLDBINDIR}
