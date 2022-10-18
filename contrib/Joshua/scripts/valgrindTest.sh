#!/bin/sh
OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"
python3 -m test_harness.app -s ${JOSHUA_SEED} --old-binaries-path ${OLDBINDIR} --use-valgrind
