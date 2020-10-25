#!/bin/sh

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"
FDBVER=`"${SCRIPTDIR}/bin/fdbserver" --version | head -n1 | sed -e 's/.*(v//' -e 's/[-a-zA-Z]*)//'`
mono bin/TestHarness.exe joshua-run "${OLDBINDIR}" "${FDBVER}" false
