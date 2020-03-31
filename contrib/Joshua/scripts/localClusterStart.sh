#!/bin/bash
SCRIPTDIR="${SCRIPTDIR:-$( cd "${BASH_SOURCE[0]%\/*}" && pwd )}"
DEBUGLEVEL="${DEBUGLEVEL:-1}"
WORKDIR="${WORKDIR:-${SCRIPTDIR}/tmp/fdb.work}"
LOGDIR="${WORKDIR}/log"
ETCDIR="${WORKDIR}/etc"
BINDIR="${BINDIR:-${SCRIPTDIR}}"
FDBSERVERPORT="${FDBSERVERPORT:-4500}"
FDBCONF="${ETCDIR}/fdb.cluster"
LOGFILE="${LOGFILE:-${LOGDIR}/startcluster.log}"

# Initialize the variables
status=0
messagetime=0
messagecount=0

function log
{
	local status=0
	if [ "$#" -lt 1 ]
	then
		echo "Usage: log <message> [echo]"
		echo
		echo "Logs the message and timestamp to LOGFILE (${LOGFILE}) and, if the"
		echo "second argument is either not present or is set to 1, stdout."
		let status="${status} + 1"
	else
		# Log to stdout.
		if [ "$#" -lt 2 ] || [ "${2}" -ge 1 ]
		then
			echo "${1}"
		fi

		# Log to file.
		datestr=$(date +"%Y-%m-%d %H:%M:%S (%s)")
		dir=$(dirname "${LOGFILE}")
		if ! [ -d "${dir}" ] && ! mkdir -p "${dir}"
		then
			echo "Could not create directory to log output."
			let status="${status} + 1"
		elif ! [ -f "${LOGFILE}" ] && ! touch "${LOGFILE}"
		then
			echo "Could not create file ${LOGFILE} to log output."
			let status="${status} + 1"
		elif ! echo "[ ${datestr} ] ${1}" >> "${LOGFILE}"
		then
			echo "Could not log output to ${LOGFILE}."
			let status="${status} + 1"
		fi
	fi

	return "${status}"
}

# Display a message for the user.
function displayMessage
{
	local status=0

	if [ "$#" -lt 1 ]
	then
		echo "displayMessage <message>"
		let status="${status} + 1"
	elif ! log "${1}" 0
	then
		log "Could not write message to file."
	else
		# Increment the message counter
		let messagecount="${messagecount} + 1"

		# Display successful message, if previous message
		if [ "${messagecount}" -gt 1 ]
		then
			# Determine the amount of transpired time
			let timespent="${SECONDS}-${messagetime}"

			if [ "${DEBUGLEVEL}" -gt 0 ]; then
				printf "... done in %3d seconds\n" "${timespent}"
			fi
		fi

		# Display message
		if [ "${DEBUGLEVEL}" -gt 0 ]; then
			printf "%-16s	  %-35s " "$(date "+%F %H-%M-%S")" "$1"
		fi

		# Update the variables
		messagetime="${SECONDS}"
	fi

	return "${status}"
}

# Create the directories used by the server.
function createDirectories {
	# Display user message
	if ! displayMessage "Creating directories"
	then
		echo 'Failed to display user message'
		let status="${status} + 1"
	
	elif ! mkdir -p "${LOGDIR}" "${ETCDIR}"
	then
		log "Failed to create directories"
		let status="${status} + 1"
	
	# Display user message
	elif ! displayMessage "Setting file permissions"
	then
		log 'Failed to display user message'
		let status="${status} + 1"
	
	elif ! chmod 755 "${BINDIR}/fdbserver" "${BINDIR}/fdbcli"
	then
		log "Failed to set file permissions"
		let status="${status} + 1"
	
	else
		while read filepath
		do
				if [ -f "${filepath}" ] && [ ! -x "${filepath}" ]
				then
					# if [ "${DEBUGLEVEL}" -gt 1 ]; then
					# 	log "   Enable executable: ${filepath}"
					# fi
					log "   Enable executable: ${filepath}" "${DEBUGLEVEL}"
					if ! chmod 755 "${filepath}"
					then
						log "Failed to set executable for file: ${filepath}"
						let status="${status} + 1"
					fi
				fi
		done < <(find "${BINDIR}" -iname '*.py' -o -iname '*.rb' -o -iname 'fdb_flow_tester' -o -iname '_stacktester' -o -iname '*.js' -o -iname '*.sh' -o -iname '*.ksh')
	fi

	return ${status}
}

# Create a cluster file for the local cluster.
function createClusterFile {
	if [ "${status}" -ne 0 ]; then
		:
	# Display user message
	elif ! displayMessage "Creating Fdb Cluster file"
	then
		log 'Failed to display user message'
		let status="${status} + 1"
	else
		description=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom 2> /dev/null | head -c 8)
		random_str=$(LC_CTYPE=C tr -dc A-Za-z0-9 < /dev/urandom 2> /dev/null | head -c 8)
		echo "$description:$random_str@127.0.0.1:${FDBSERVERPORT}" > "${FDBCONF}"
	fi

	if [ "${status}" -ne 0 ]; then
		:
	elif ! chmod 0664 "${FDBCONF}"; then
		log "Failed to set permissions on fdbconf: ${FDBCONF}"
		let status="${status} + 1"
	fi

	return ${status}
}

# Start the server running.
function startFdbServer {
	if [ "${status}" -ne 0 ]; then
		:
	elif ! displayMessage "Starting Fdb Server"
	then
		log 'Failed to display user message'
		let status="${status} + 1"

	elif ! "${BINDIR}/fdbserver" -C "${FDBCONF}" -p "auto:${FDBSERVERPORT}" -L "${LOGDIR}" -d "${WORKDIR}/fdb/$$" &> "${LOGDIR}/fdbserver.log" &
	then
		log "Failed to start FDB Server"
		# Maybe the server is already running
		FDBSERVERID="$(pidof fdbserver)"
		let status="${status} + 1"
	else
		FDBSERVERID="${!}"
	fi

	if ! kill -0 ${FDBSERVERID} ; then
		log "FDB Server start failed."
		let status="${status} + 1"
	fi

	return ${status}
}

function getStatus {
	if [ "${status}" -ne 0 ]; then
		:
	elif ! date &>> "${LOGDIR}/fdbclient.log"
	then
		log 'Failed to get date'
		let status="${status} + 1"
	elif ! "${BINDIR}/fdbcli" -C "${FDBCONF}" --exec 'status json' --timeout 120 &>> "${LOGDIR}/fdbclient.log"
	then
		log 'Failed to get status from fdbcli'
		let status="${status} + 1"
	elif !  date &>> "${LOGDIR}/fdbclient.log"
	then
		log 'Failed to get date'
		let status="${status} + 1"
	fi

	return ${status}
}

# Verify that the cluster is available.
function verifyAvailable {
	# Verify that the server is running.
	if ! kill -0 "${FDBSERVERID}"
	then
		log "FDB server process (${FDBSERVERID}) is not running"
		let status="${status} + 1"
		return 1

	# Display user message.
	elif ! displayMessage "Checking cluster availability"
	then
		log 'Failed to display user message'
		let status="${status} + 1"
		return 1

	# Determine if status json says the database is available.
	else
		avail=`"${BINDIR}/fdbcli" -C "${FDBCONF}" --exec 'status json' --timeout 10 2> /dev/null | grep -E '"database_available"|"available"' | grep 'true'`
		log "Avail value: ${avail}" "${DEBUGLEVEL}"
		if [[ -n "${avail}" ]] ; then
			return 0
		else
			return 1
		fi
	fi
}

# Configure the database on the server.
function createDatabase {
	if [ "${status}" -ne 0 ]; then
		:
	# Ensure that the server is running
	elif ! kill -0 "${FDBSERVERID}"
	then
		log "FDB server process: (${FDBSERVERID}) is not running"
		let status="${status} + 1"

	# Display user message
	elif ! displayMessage "Creating database"
	then
		log 'Failed to display user message'
		let status="${status} + 1"
	elif ! echo "Client log:" &> "${LOGDIR}/fdbclient.log"
	then
		log 'Failed to create fdbclient.log'
		let status="${status} + 1"
	elif ! getStatus
	then
		log 'Failed to get status'
		let status="${status} + 1"

	# Configure the database.
	else
		"${BINDIR}/fdbcli" -C "${FDBCONF}" --exec 'configure new single memory; status' --timeout 240 --log --log-dir "${LOGDIR}" &>> "${LOGDIR}/fdbclient.log"

		if ! displayMessage "Checking if config succeeded"
		then
			log 'Failed to display user message.'
		fi

		iteration=0
		while [[ "${iteration}" -lt 10 ]] && ! verifyAvailable
		do
			log "Database not created (iteration ${iteration})."
			let iteration="${iteration} + 1"
		done

		if ! verifyAvailable
		then
			log "Failed to create database via cli"
			getStatus
			cat "${LOGDIR}/fdbclient.log"
			log "Ignoring -- moving on"
			#let status="${status} + 1"
		fi
	fi

	return ${status}
}

# Begin the local cluster from scratch.
function startCluster {
	if [ "${status}" -ne 0 ]; then
		:
	elif ! createDirectories
	then
		log "Could not create directories."
		let status="${status} + 1"
	elif ! createClusterFile
	then
		log "Could not create cluster file."
		let status="${status} + 1"
	elif ! startFdbServer
	then
		log "Could not start FDB server."
		let status="${status} + 1"
	elif ! createDatabase
	then
		log "Could not create database."
		let status="${status} + 1"
	fi

	return ${status}
}
