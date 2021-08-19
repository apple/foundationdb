#!/bin/sh

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="detect_leaks=0"

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"
FAULT_INJECTION=true
BUGGIFY=true

for var in "$@"; do
    case "$var" in
    "--buggify-disabled") BUGGIFY=false ;;
    "--fault-injection-disabled") FAULT_INJECTION=false ;;
    esac
done

mono bin/TestHarness.exe joshua-run "${OLDBINDIR}" false 3 $BUGGIFY $FAULT_INJECTION
