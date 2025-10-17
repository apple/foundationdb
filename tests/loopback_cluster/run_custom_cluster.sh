#!/bin/bash
set -euo pipefail

SERVER_COUNT=1
readonly PORT_PREFIX="${PORT_PREFIX:-1500}"

# default cluster settings, override with options
STATELESS_COUNT=4
REPLICATION_COUNT=1
LOGS_COUNT=8
STORAGE_COUNT=16
KNOBS=
LOGS_TASKSET=""
STATELESS_TASKSET=""
STORAGE_TASKSET=""
STORAGE_TYPE="ssd"
LOGROUTER_COUNT=0
DUMP_PIDS=
PIDS=()

function usage {
	echo "Usage"
	printf "\tcd working-directory; %s path-to-build-root [OPTIONS]\n\r" "${0}"
	echo "Options"
	printf "\t--knobs '--knob-KNOBNAME=KNOBVALUE' \n\r\t\tChanges a database knob. Enclose in single quotes.\n\r"
	printf "\t--stateless_count COUNT\n\r\t\t number of stateless daemons to start.  Default %s\n\r" "${STATELESS_COUNT}"
	printf "\t--stateless_taskset BITMASK\n\r\t\tBitmask of CPUs to pin stateless tasks to. Default is all CPUs.\n\r"
	printf "\t--logs_count COUNT\n\r\t\tNumber of stateless daemons to start.  Default %s\n\r" "${LOGS_COUNT}"
	printf "\t--logs_taskset BITMASK\n\r\t\tbitmask of CPUs to pin logs to. Default is all CPUs.\n\r"
	printf "\t--storage_count COUNT\n\r\t\tnumber of storage daemons to start.  Default %s\n\r" "${STORAGE_COUNT}"
	printf "\t--storage_taskset BITMASK\n\r\t\tBitmask of CPUs to pin storage to. Default is all CPUs.\n\r"
	printf "\t--storage_type [ssd|ssd-rocksdb-v1|ssd-sharded-rocksdb]\n\r\t\tStorage type to use. Default is ssd.\n\r"
	printf "\t--replication_count COUNT\n\r\t\tReplication count may be 1,2 or 3. Default is 1.\n\r"
	printf "\t--dump_pids [on|off]\n\r\t\tDump out the PIDs of all processes started by this script. Default is off.\n\r"
	echo "Example"
	printf "\t%s . --knobs '--knob_proxy_use_resolver_private_mutations=1' --stateless_count 4 --stateless_taskset 0xf --logs_count 8 --logs_taskset 0xff0 --storage_taskset 0xffff000\n\r" "${0}"
	exit 1
}

# Print passed in parameters on STDERR
function err {
  echo "$(date -Iseconds) ERROR: $*" >&2
}

function start_servers {
  for j in $(seq 1 "${1}"); do
    local logdir="${LOOPBACK_DIR}/${SERVER_COUNT}/log"
    local datadir="${LOOPBACK_DIR}/${SERVER_COUNT}/data"
    if ! mkdir -p "${logdir}" "${datadir}"; then
      echo "Failed to create the directories ${logdir} and ${datadir}"
      exit 1
    fi
    local port=$(( PORT_PREFIX + SERVER_COUNT ))
    local zone="${4}-Z-$(( j % REPLICATION_COUNT ))"
    # There may be more than one knob in KNOBS separated by spaces. Bash will quote
    # it all with single-quotes because the string has a space in it. We don't want
    # that behavior; fdbserver won't be able to parse the quoted knobs. To get around
    # this native bash behavior, we printf the KNOBS string.
    ${2} "${FDB}" -p auto:"${port}" ${KNOBS:+$(printf "%s" "${KNOBS}")} -c "${3}" \
      -d "${datadir}" -L "${logdir}" -C "${CLUSTER}" \
      --datacenter_id="${4}" \
      --locality-zoneid "${zone}" \
      --locality-machineid M-$SERVER_COUNT &
    local pid=$!
    # Sleep so the pid has time to show in process list.
    sleep 1
    if ! ps -p "${pid}" &> /dev/null; then
      wait "${pid}"
      status=$?
      # Kill any servers that we started before this failure.
      # We collect them in the global PIDS
      for (( i=0; i<${#PIDS[@]}; ++i )); do
        echo kill -9 "${PIDS[i]}"
        kill -9 "${PIDS[i]}"
      done
      err "Failed to start server on port ${port}"
      return "${status}"
    fi
    # Add this successfully started server to our PIDS array.
    PIDS+=("${pid}")
		SERVER_COUNT=$(( SERVER_COUNT + 1 ))
	done
}

function create_fileconfig {
	cat > /tmp/fdbfileconfig.json <<EOF
{
	"regions": [{
		"datacenters": [{
			"id": "DC1",
			"priority": 1
		}, {
			"id": "DC2",
			"priority": 0,
			"satellite": 1,
			"satellite_logs": 8
		}],
		"satellite_redundancy_mode": "one_satellite_double"
	}, {
		"datacenters": [{
			"id": "DC3",
			"priority": -1
		}]
	}]
}
EOF
}

if (( $# < 1 )) ; then
	echo Wrong number of arguments
	usage
fi

if [[ $1 == "-h" || $1 == "--help" ]]; then 
	usage 
fi

BUILD=$1
shift;

while [[ $# -gt 0 ]]; do
	case "${1}" in
    --dump_pids)
      DUMP_PIDS="${2}"
      ;;
		--knobs)
			KNOBS="$2"
			;;
		--stateless_taskset)
			STATELESS_TASKSET="taskset ${2}"
			;;			
		--logs_taskset)
			LOGS_TASKSET="taskset ${2}"
			;;			
		--storage_taskset)
			STORAGE_TASKSET="taskset ${2}"
			;;	
		--storage_type)
			STORAGE_TYPE="${2}"
			;;
		--stateless_count)
			STATELESS_COUNT=$2
			;;			
		--logs_count)
			LOGS_COUNT=$2
			;;			
		--storage_count)
			STORAGE_COUNT=$2
			;;	
		--replication_count)
			REPLICATION_COUNT=$2
			;;
		--logrouter_count)
			LOGROUTER_COUNT=$2
			;;
		*)
      echo "ERROR: unknown input $1"
			usage
			;;
	esac
	shift; shift
done

FDB=${BUILD}/bin/fdbserver
if [[ ! -f "${FDB}" ]]; then
	echo "Error: ${FDB} not found!"
	usage
fi

if (( "${REPLICATION_COUNT}" == 1 )); then
	replication="single"
elif (( "${REPLICATION_COUNT}" == 2 )); then
	replication="double"
elif (( "${REPLICATION_COUNT}" == 3 )); then
	replication="triple"
else
	usage
fi

LOOPBACK_DIR=${LOOPBACK_DIR:-./loopback-cluster}
rm -rf "${LOOPBACK_DIR}"
mkdir -p "${LOOPBACK_DIR}"

CLUSTER_FILE="test1:testdb1@127.0.0.1:$(( PORT_PREFIX + 1))"
CLUSTER="${LOOPBACK_DIR}/fdb.cluster"
echo "${CLUSTER_FILE}" > "${CLUSTER}"

echo "Starting Cluster: " $CLUSTER_FILE

if ! start_servers "${STATELESS_COUNT}" "$STATELESS_TASKSET" stateless DC1; then
  err "Failed to start DC1 stateless"
  exit 1
fi
if ! start_servers "${LOGS_COUNT}" "$LOGS_TASKSET" log DC1; then
  err "Failed to start DC1 logs"
  exit 1
fi
if ! start_servers "${STORAGE_COUNT}" "$STORAGE_TASKSET" storage DC1; then
  err "Failed to start DC1 storage"
  exit 1
fi

CLI="$BUILD/bin/fdbcli -C ${CLUSTER} --exec"
echo "configure new ${STORAGE_TYPE} $replication - stand by"

# sleep 2 seconds to wait for workers to join cluster, then configure database and coordinators
( sleep 2 ; $CLI "configure new ${STORAGE_TYPE} $replication" ; $CLI "coordinators auto")

if (( "${LOGROUTER_COUNT}" > 0 )); then
	if ! start_servers "${LOGROUTER_COUNT}" "$STORAGE_TASKSET" router DC3; then
    err "Failed to start DC3 logrouters"
    exit 1
  fi
	# Same number remote/satellite logs and ss as primary
	if ! start_servers "${LOGS_COUNT}" "$LOGS_TASKSET" log DC2; then
    err "Failed to start DC2 logs"
    exit 1
  fi
	if ! start_servers "${LOGS_COUNT}" "$LOGS_TASKSET" log DC3; then
    err "Failed to start DC3 logs"
    exit 1
  fi
	if ! start_servers "${STORAGE_COUNT}" "$STORAGE_TASKSET" storage DC3; then
    err "Failed to start DC3 storage"
    exit 1
  fi
	create_fileconfig
	$CLI "fileconfigure /tmp/fdbfileconfig.json"
	echo "Wait for data to be fully replicated (Healthy), then issue: $CLI configure usable_regions=2"
fi
if [[ -n "${DUMP_PIDS}" ]]; then
  echo "PIDS=${PIDS[*]}"
fi
