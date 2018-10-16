#!/bin/sh

set -e

FDBSERVER="`pwd`/$1"
shift
TESTFILE="`pwd`/$1"
shift
SEED="$1"
shift

if test "x${SEED}" = xX; then
    # let fdb pick the seed
    SEED_ARGS=""
else
    SEED_ARGS="-s ${SEED}"
fi

# work out of the temp directory
cd "${TEST_TMPDIR}"

FDB_STATUS=0
"${FDBSERVER}" -r simulation -f "${TESTFILE}" $SEED_ARGS || FDB_STATUS="$?"

if grep 'Severity="40"' trace*; then
    echo sev40 failed the test
    # TODO(rescriva): Too many tests fail with this, so check with upstream if
    # this is the right thing to do.
    exit 1
fi

# cleanup test directory only on success
if test "x${FDB_STATUS}" = x0; then
    cd /
    rm -r "${TEST_TMPDIR}"
fi

exit ${FDB_STATUS}
