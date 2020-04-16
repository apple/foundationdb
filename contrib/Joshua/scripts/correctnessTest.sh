#!/bin/sh
OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"
mono bin/TestHarness.exe joshua-run "${OLDBINDIR}" false
