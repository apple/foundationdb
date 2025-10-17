#!/usr/bin/env bash
######################################################
#
# FoundationDB Binding Test Script
#
# Test script for running FoundationDB binding tests
#
# Defines:
#
# Author: Alvin Moore
# Date:	 16-04-28
# Version: 1.0
######################################################

# Defines
SCRIPTDIR=$( cd "${BASH_SOURCE[0]%\/*}" && pwd )
CWD=$(pwd)
OSNAME="$(uname -s)"
DEBUGLEVEL="${DEBUGLEVEL:-1}"
DISPLAYERROR="${DISPLAYERROR:-0}"
OPERATIONS="${OPERATIONS:-1000}"
HCAOPERATIONS="${HCAOPERATIONS:-100}"
CONCURRENCY="${CONCURRENCY:-5}"
BREAKONERROR="${BREAKONERROR:-0}"
RUNSCRIPTS="${RUNSCRIPTS:-1}"
RUNTESTS="${RUNTESTS:-1}"
RANDOMTEST="${RANDOMTEST:-0}"
# BINDINGTESTS="${BINDINGTESTS:-python python3 java java_async ruby go flow}"
BINDINGTESTS="${BINDINGTESTS:-python python3 java java_async go flow swift}"
LOGLEVEL="${LOGLEVEL:-INFO}"
_BINDINGTESTS=(${BINDINGTESTS})
DISABLEDTESTS=()
TESTFILE="${SCRIPTDIR}/bindingtester.py"
TESTTYPES=('API' 'Concurrent API' 'Directory' 'Directory HCA')
TESTTOTAL="${#TESTTYPES[@]}"
TESTINDEX="${TESTINDEX:-$TESTTOTAL}"
LOGSTDOUT="${LOGSTDOUT:-0}"
CONSOLELOG="${CONSOLELOG:-${CWD}/console.log}"
VERSION="1.6"

# Display syntax
if [ "${#}" -lt 2 ]
then
	echo 'run_binding_tester.sh <number of cycles> <error file>'
	echo '   cycles:   number of cycles to run test (0 => unlimitted)'
	echo ''
	echo '   Modifiable Environment Variables:'
	echo '       CONCURRENCY:   number of concurrent requests'
	echo '       OPERATIONS:    number of operations per test'
	echo '       HCAOPERATIONS: number of HCA operations per test'
	echo '       BINDINGTESTS:  lists of binding tests to run'
	echo '       BREAKONERROR:  stop on first error, if positive number'
	echo "       TESTINDEX:     (0-${TESTTOTAL}) ${TESTTYPES[*]}"
	echo '       RANDOMTEST:    select a single random test, if positive number'
	echo '       LOGLEVEL:      ERROR, WARNING, INFO, DEBUG'
	echo ''
	echo "   version: ${VERSION}"
	exit 1
fi

# Read arguments
MAXCYCLES="${1}"
ERRORFILE="${2}"

function logError()
{
	local status=0

	if [ "$#" -lt 3 ]
	then
		echo "runCommand <message> <output> <command executable> [args ...]"
		let status="${status} + 1"
	else
		local message="${1}"
		local output="${2}"
		local command="${3}"
		shift
		shift
		shift

		let errorTotal="${errorTotal} + 1"

		# Display the error, if enabled
		if [ "${DISPLAYERROR}" -gt 0 ]
		then
			printf '%-16s Error #%3d:\n' "$(date '+%F %H-%M-%S')" "${errorTotal}"
			echo "Message: '${message}'"
			echo "Command: '${command} ${@}'"
			echo "Error: ${output}"
		fi

		# Create the file, if not present
		if [[ ! -f "${ERRORFILE}" ]]
		then
			dir=$(dirname "${ERRORFILE}")

			if [ ! -d "${dir}" ] && ! mkdir -p "${dir}"
			then
				echo "Failed to create directory: ${dir} for error file: ${ERRORFILE}"
				let status="${status} + 1"
				printf '\n%-16s Error #%3d:\n' "$(date '+%F %H-%M-%S')" "${errorTotal}"
				echo "Message: '${message}'"
				echo "Command: '${command} ${@}'"
				echo "Error: ${output}"
			fi
		fi

		# Initialize the error log, if first error
		if [[ "${errorTotal}" -eq 1 ]]
		then
			:
		fi

		# Write the error to the log
		if [[ "${status}" -eq 0 ]]
		then
			printf '\n%-16s Error #%3d:\n' "$(date '+%F %H-%M-%S')" "${errorTotal}" >> "${ERRORFILE}"
			echo "Message: '${message}'" >> "${ERRORFILE}"
			echo "Command: '${command} ${@}'" >> "${ERRORFILE}"
			echo -n "Error:" >> "${ERRORFILE}"
			echo "${output}" >> "${ERRORFILE}"
			echo '----------------------------------------------------------------------------------------------------' >> "${ERRORFILE}"
		fi
	fi

	return "${status}"
}

function runCommand()
{
	local status=0

	if [ "$#" -lt 2 ]
	then
		echo "runCommand <message> <executable> [args ...]"
		let status="${status} + 1"
	else
		local message="${1}"
		local command="${2}"
		local time="${SECONDS}"
		shift
		shift

		if [ "${DEBUGLEVEL}" -gt 2 ]; then
			printf "%-16s        %-70s \n"	"" "${command} ${*}"
		fi

		if [ "${DEBUGLEVEL}" -gt 1 ]; then
			printf "%-16s     %-40s "	"" "${message}"
		fi

		if [ "${LOGSTDOUT}" -gt 0 ] ; then
			printf "Running command: ${command} ${*}\n\n" >> "${CONSOLELOG}"
			"${command}" "${@}" 2>&1 >> "${CONSOLELOG}"
			result=$?
			output=$(cat "${CONSOLELOG}")
		else
			output=$("${command}" "${@}" 2>&1)
			result=$?
		fi
		let time="${SECONDS} - ${time}"

		# Check return code
		if [ "${result}" -ne 0 ]
		then
			if [ "${DEBUGLEVEL}" -gt 0 ]; then
				echo "failed after ${time} seconds."
			fi
			let status="${status} + 1"
			logError "${message}" "${output}" "${command}" "${@}"
		elif [ "${DEBUGLEVEL}" -gt 0 ];then
			echo "passed in ${time} seconds."
		fi
	fi

	return "${status}"
}

function runScriptedTest()
{
	local status=0

	if [ "$#" -lt 1 ]
	then
		echo "runScriptedTest <test>"
		let status="${status} + 1"
	else
		local test="${1}"

		if ! runCommand "Scripting ${test} ..."  'python3' '-u' "${TESTFILE}" "${test}" --test-name scripted --logging-level "${LOGLEVEL}"
		then
			let status="${status} + 1"
		fi
	fi

	return "${status}"
}

function runTest()
{
	local status=0

	if [ "$#" -lt 1 ]
	then
		echo "runTest <test>"
		let status="${status} + 1"
	else
		local test="${1}"

		if [ "${DEBUGLEVEL}" -gt 0 ]; then
			printf "%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "Testing ${test}"
		fi

		# API
		if ([[ "${TESTINDEX}" -eq 0 ]] || [[ "${TESTINDEX}" -eq "${TESTTOTAL}" ]]) && ([[ "${BREAKONERROR}" -eq 0 ]] || [[ "${status}" -eq 0 ]]) && ! runCommand "   ${TESTTYPES[0]}" 'python3' '-u' "${TESTFILE}" "${test}" --test-name api --compare --num-ops "${OPERATIONS}" --logging-level "${LOGLEVEL}"
		then
			let status="${status} + 1"
		fi

		# Concurrent API
		if ([[ "${TESTINDEX}" -eq 1 ]] || [[ "${TESTINDEX}" -eq "${TESTTOTAL}" ]]) && ([[ "${BREAKONERROR}" -eq 0 ]] || [[ "${status}" -eq 0 ]]) &&  ! runCommand "   ${TESTTYPES[1]}" 'python3' '-u' "${TESTFILE}" "${test}" --test-name api --concurrency "${CONCURRENCY}" --num-ops "${OPERATIONS}" --logging-level "${LOGLEVEL}"
		then
			let status="${status} + 1"
		fi

		# Directory
		if ([[ "${TESTINDEX}" -eq 2 ]] || [[ "${TESTINDEX}" -eq "${TESTTOTAL}" ]]) && ([[ "${BREAKONERROR}" -eq 0 ]] || [[ "${status}" -eq 0 ]]) &&  ! runCommand "   ${TESTTYPES[2]}" 'python3' '-u' "${TESTFILE}" "${test}" --test-name directory --compare --num-ops "${OPERATIONS}" --logging-level "${LOGLEVEL}"
		then
			let status="${status} + 1"
		fi

		# Directory HCA
		if ([[ "${TESTINDEX}" -eq 3 ]] || [[ "${TESTINDEX}" -eq "${TESTTOTAL}" ]]) && ([[ "${BREAKONERROR}" -eq 0 ]] || [[ "${status}" -eq 0 ]]) &&  ! runCommand "   ${TESTTYPES[3]}" 'python3' '-u' "${TESTFILE}" "${test}" --test-name directory_hca --concurrency "${CONCURRENCY}"  --num-ops "${HCAOPERATIONS}" --logging-level "${LOGLEVEL}"
		then
			let status="${status} + 1"
		fi
	fi

	return "${status}"
}

# Initialize the variables
status=0
cycles=0
rundate="$(date +%F_%H-%M-%S)"
errorTotal=0


# Select a random test, if enabled
if [ "${RANDOMTEST}" -gt 0 ]
then
	let testIndex="${RANDOM} % ${#_BINDINGTESTS[@]}"
	randomTest="${_BINDINGTESTS[$testIndex]}"
	# Remove the random test from the list of binding tests
	_BINDINGTESTS=("${_BINDINGTESTS[@]/${randomTest}}")
	DISABLEDTESTS+=("${_BINDINGTESTS[@]}")
	_BINDINGTESTS=("${randomTest}")

	# Choose a random test
	let TESTINDEX="${RANDOM} % ${TESTTOTAL}"

	# Select scripted or tests, if enabled
	if [ "${RUNSCRIPTS}" -gt 0 ] && [ "${RUNTESTS}" -gt 0 ]; then
		# Select scripted tests, if 1 out of 100
		if [ $((${RANDOM} % 100)) -eq 0 ]; then
			RUNTESTS=0
		else
			RUNSCRIPTS=0
		fi
	fi
fi

# Determine the name of the test type
# from the test index
if [ "${TESTINDEX}" -lt "${TESTTOTAL}" ]; then
	TESTNAME="${TESTTYPES[$TESTINDEX]}"
else
	TESTNAME="All Tests"
	TESTINDEX="${TESTTOTAL}"
fi

if [ "${DEBUGLEVEL}" -gt 0 ]
then
	echo ''
	echo ''
	echo '*******************************************************************************************'
	echo ''
	printf "%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "FoundationDb Binding Tester"
	printf "%-20s     Host OS:        %-40s \n"	"" "${OSNAME}"
	printf "%-20s     Max Cycles:     %-40s \n"	"" "${MAXCYCLES}"
	printf "%-20s     Operations:     %-40s \n"	"" "${OPERATIONS}"
	printf "%-20s     HCA Operations: %-40s \n"	"" "${HCAOPERATIONS}"
	printf "%-20s     Concurrency:    %-40s \n"	"" "${CONCURRENCY}"
	printf "%-20s     Tests:     (%2d) %-40s \n" "" "${#_BINDINGTESTS[@]}" "${_BINDINGTESTS[*]}"
	printf "%-20s     Disabled:  (%2d) %-40s \n" "" "${#DISABLEDTESTS[@]}" "${DISABLEDTESTS[*]}"
	printf "%-20s     Error Log:      %-40s \n"	"" "${ERRORFILE}"
	printf "%-20s     Log Level:      %-40s \n"	"" "${LOGLEVEL}"
	printf "%-20s     Random Test:    %-40s \n"	"" "${RANDOMTEST}"
	printf "%-20s     Test Type:      (%d) %-40s \n"	"" "${TESTINDEX}" "${TESTNAME}"
	printf "%-20s     Run Scripts:    %-40s \n"	"" "${RUNSCRIPTS}"
	printf "%-20s     Run Tests:      %-40s \n"	"" "${RUNTESTS}"
	printf "%-20s     Debug Level:    %-40s \n"	"" "${DEBUGLEVEL}"
	printf "%-20s     Script Version: %-40s \n"	"" "${VERSION}"
	echo ''
fi

# Run the scripted tests, if enabled
if [ "${RUNSCRIPTS}" -gt 0 ]
then
	if [ "${DEBUGLEVEL}" -gt 0 ]; then
		printf "%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "Running scripted tests"
	fi

	for test in "${_BINDINGTESTS[@]}"
	do
		# Run the specified scripted test
		if ! runScriptedTest "${test}"
		then
			let status="${status} + 1"

			# Break Stop the test, if enabled
			if [[ "${BREAKONERROR}" -ne 0 ]]
			then
				break
			fi
		fi
	done
fi

# Run the individual tests, if enabled
while [[ "${RUNTESTS}" -gt 0 ]] && ([[ "${BREAKONERROR}" -eq 0 ]] || [[ "${status}" -eq 0 ]]) && ([[ "${cycles}" -lt "${MAXCYCLES}" ]] || [[ "${MAXCYCLES}" -eq 0 ]])
do
	let cycles="${cycles} + 1"
	if [ "${DEBUGLEVEL}" -gt 0 ]; then
		printf "\n%-16s  Cycle #%3d \n"		"$(date '+%F %H-%M-%S')" "${cycles}"
	fi

	for test in "${_BINDINGTESTS[@]}"
	do
		# Run the specified test
		if ! runTest "${test}"
		then
			let status="${status} + 1"

			# Break Stop the test, if enabled
			if [[ "${BREAKONERROR}" -ne 0 ]]
			then
				break
			fi
		fi
	done
done

# Final report
if [ "${status}" -eq 0 ]
then
	if [ "${DEBUGLEVEL}" -gt 0 ]; then
		printf "\n%-16s  Successfully completed ${cycles} cycles of the FDB binding tester for ${#_BINDINGTESTS[@]} binding tests in %d seconds.\n"	"$(date '+%F %H-%M-%S')" "${SECONDS}"
	fi
elif [ "${DEBUGLEVEL}" -gt 0 ]; then
	printf "\n%-16s  Failed to complete all ${cycles} cycles of the FDB binding tester for ${#_BINDINGTESTS[@]} binding tests in %d seconds.\n"	"$(date '+%F %H-%M-%S')" "${SECONDS}"
fi

if [ "${DEBUGLEVEL}" -gt 0 ]
then
	echo ''
	echo ''
	echo '*******************************************************************************************'
	echo ''
	printf "%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "Binding Tester Results"
	printf "%-20s     Cycles:         %-40s \n"	"" "${cycles}"
	printf "%-20s     Failed Tests:   %-40s \n"	"" "${status}"
	printf "%-20s     Errors:         %-40s \n"	"" "${errorTotal}"
	printf "%-20s     Tests:     (%2d) %-40s \n" "" "${#_BINDINGTESTS[@]}" "${_BINDINGTESTS[*]}"
	printf "%-20s     Version:        %-40s \n"	"" "${VERSION}"
fi

# Ensure that status is a returnable number
if [[ "${status}" -ne 0 ]]; then
	status=1
fi

exit "${status}"
