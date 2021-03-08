#/bin/bash
SCRIPTDIR=$( cd "${BASH_SOURCE[0]%\/*}" && pwd )
cwd="$(pwd)"
BINDIR="${BINDIR:-${SCRIPTDIR}}"
LIBDIR="${BINDIR}:${LD_LIBRARY_PATH}"
SCRIPTID="${$}"
SAVEONERROR="${SAVEONERROR:-1}"
PYTHONDIR="${BINDIR}/tests/python"
testScript="${BINDIR}/tests/bindingtester/run_binding_tester.sh"
VERSION="1.9"

source ${SCRIPTDIR}/localClusterStart.sh

# Display syntax
if [ "$#" -lt 1 ]
then
	echo "bindingTestScript.sh <number of test cycles>"
	echo "   version: ${VERSION}"
	exit 1
fi

cycles="${1}"

if [ "${DEBUGLEVEL}" -gt 0 ]
then
	echo "Work dir:       ${WORKDIR}"
	echo "Bin dir:        ${BINDIR}"
	echo "Log dir:        ${LOGDIR}"
	echo "Python path:    ${PYTHONDIR}"
	echo "Lib dir:        ${LIBDIR}"
	echo "Cluster String: ${FDBCLUSTERTEXT}"
	echo "Script Id:      ${SCRIPTID}"
	echo "Version:        ${VERSION}"
fi

# Begin the cluster using the logic in localClusterStart.sh.
startCluster

# Stop the cluster on exit
trap "stopCluster" EXIT

# Display user message
if [ "${status}" -ne 0 ]; then
	:
elif ! displayMessage "Running binding tester"
then
	echo 'Failed to display user message'
	let status="${status} + 1"

elif ! PYTHONPATH="${PYTHONDIR}" LD_LIBRARY_PATH="${LIBDIR}" FDB_CLUSTER_FILE="${FDBCONF}" LOGSTDOUT=1 CONSOLELOG="${WORKDIR}/console.log" "${testScript}" "${cycles}" "${WORKDIR}/errors/run.log"
then
	if [ "${DEBUGLEVEL}" -gt 0 ]; then
		printf "\n%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "Failed to complete binding tester in ${SECONDS} seconds."
	fi
	let status="${status} + 1"

elif [ "${DEBUGLEVEL}" -gt 0 ]; then
	printf "\n%-16s  %-40s \n"		"$(date '+%F %H-%M-%S')" "Completed binding tester in ${SECONDS} seconds"
fi

# Display directory and log information, if an error occurred
if [ "${status}" -ne 0 ]
then
	ls "${WORKDIR}" &> "${LOGDIR}/dir.log"
	ps -eafwH &> "${LOGDIR}/process-preclean.log"
	if [ -f "${FDBCONF}" ]; then
		cp -f "${FDBCONF}" "${LOGDIR}/"
	fi
	# Display the severity errors
	if [ -d "${LOGDIR}" ]; then
		grep -ir 'Severity="40"' "${LOGDIR}"
	fi
fi

# Save debug information files, environment, and log information, if an error occurred
if [ "${status}" -ne 0 ] && [ "${SAVEONERROR}" -gt 0 ]; then
	ps -eafwH &> "${LOGDIR}/process-exit.log"
	netstat -na &> "${LOGDIR}/netstat.log"
	df -h &> "${LOGDIR}/disk.log"
	env &> "${LOGDIR}/env.log"
fi

# Stop the cluster
if stopCluster; then
	unset FDBSERVERID
fi

exit "${status}"
