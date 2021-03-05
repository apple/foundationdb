#!/bin/bash

TESTS="$@"
RET=0
TIMEOUT=60
DMESG_FILTER="cat"
TEST_DIR=$(dirname $0)
TEST_FILES=""
FAILED=""
SKIPPED=""
MAYBE_FAILED=""

# Only use /dev/kmsg if running as root
DO_KMSG="1"
[ "$(id -u)" != "0" ] && DO_KMSG="0"

# Include config.local if exists and check TEST_FILES for valid devices
if [ -f "$TEST_DIR/config.local" ]; then
	. $TEST_DIR/config.local
	for dev in $TEST_FILES; do
		if [ ! -e "$dev" ]; then
			echo "Test file $dev not valid"
			exit 1
		fi
	done
fi

_check_dmesg()
{
	local dmesg_marker="$1"
	local seqres="$2.seqres"

	if [ $DO_KMSG -eq 0 ]; then
		return 0
	fi

	dmesg | bash -c "$DMESG_FILTER" | grep -A 9999 "$dmesg_marker" >"${seqres}.dmesg"
	grep -q -e "kernel BUG at" \
	     -e "WARNING:" \
	     -e "BUG:" \
	     -e "Oops:" \
	     -e "possible recursive locking detected" \
	     -e "Internal error" \
	     -e "INFO: suspicious RCU usage" \
	     -e "INFO: possible circular locking dependency detected" \
	     -e "general protection fault:" \
	     -e "blktests failure" \
	     "${seqres}.dmesg"
	# shellcheck disable=SC2181
	if [[ $? -eq 0 ]]; then
		return 1
	else
		rm -f "${seqres}.dmesg"
		return 0
	fi
}

run_test()
{
	local test_name="$1"
	local dev="$2"
	local test_string=$test_name

	# Specify test string to print
	if [ -n "$dev" ]; then
		test_string="$test_name $dev"
	fi

	# Log start of the test
	if [ "$DO_KMSG" -eq 1 ]; then
		local dmesg_marker="Running test $test_string:"
		echo $dmesg_marker | tee /dev/kmsg
	else
		local dmesg_marker=""
		echo Running test $test_name $dev
	fi

	# Do we have to exclude the test ?
	echo $TEST_EXCLUDE | grep -w "$test_name" > /dev/null 2>&1
	if [ $? -eq 0 ]; then
		echo "Test skipped"
		SKIPPED="$SKIPPED <$test_string>"
		return
	fi

	# Run the test
	timeout --preserve-status -s INT -k $TIMEOUT $TIMEOUT ./$test_name $dev
	local status=$?

	# Check test status
	if [ "$status" -eq 124 ]; then
		echo "Test $test_name timed out (may not be a failure)"
	elif [ "$status" -ne 0 ]; then
		echo "Test $test_name failed with ret $status"
		FAILED="$FAILED <$test_string>"
		RET=1
	elif ! _check_dmesg "$dmesg_marker" "$test_name"; then
		echo "Test $test_name failed dmesg check"
		FAILED="$FAILED <$test_string>"
		RET=1
	elif [ -n "$dev" ]; then
		sleep .1
		ps aux | grep "\[io_wq_manager\]" > /dev/null
		if [ $? -eq 0 ]; then
			MAYBE_FAILED="$MAYBE_FAILED $test_string"
		fi
	fi
}

# Run all specified tests
for tst in $TESTS; do
	run_test $tst
	if [ ! -z "$TEST_FILES" ]; then
		for dev in $TEST_FILES; do
			run_test $tst $dev
		done
	fi
done

if [ -n "$SKIPPED" ]; then
	echo "Tests skipped: $SKIPPED"
fi

if [ "${RET}" -ne 0 ]; then
	echo "Tests failed: $FAILED"
	exit $RET
else
	sleep 1
	ps aux | grep "\[io_wq_manager\]" > /dev/null
	if [ $? -ne 0 ]; then
		MAYBE_FAILED=""
	fi
	if [ ! -z "$MAYBE_FAILED" ]; then
		echo "Tests _maybe_ failed: $MAYBE_FAILED"
	fi
	echo "All tests passed"
	exit 0
fi
