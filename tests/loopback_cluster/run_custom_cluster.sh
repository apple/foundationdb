#!/bin/bash
set -euo pipefail

ROOT=`pwd`
SERVER_COUNT=1
PORT_PREFIX=1500

# default cluster settings, override with options
STATELESS_COUNT=4
REPLICATION_COUNT=1
LOGS_COUNT=8
STORAGE_COUNT=16
KNOBS=""
LOGS_TASKSET=""
STATELESS_TASKSET=""
STORAGE_TASKSET=""
LOGROUTER_COUNT=0

function usage {
	echo "Usage"
	printf "\tcd working-directory; ${0} path-to-build-root [OPTIONS]\n\r"
	echo "Options"
	printf "\t--knobs '--knob-KNOBNAME=KNOBVALUE' \n\r\t\tChanges a database knob. Enclose in single quotes.\n\r"
	printf "\t--stateless_count COUNT\n\r\t\t number of stateless daemons to start.  Default ${STATELESS_COUNT}\n\r"
	printf "\t--stateless_taskset BITMASK\n\r\t\tBitmask of CPUs to pin stateless tasks to. Default is all CPUs.\n\r"
	printf "\t--logs_count COUNT\n\r\t\tNumber of stateless daemons to start.  Default ${LOGS_COUNT}\n\r"
	printf "\t--logs_taskset BITMASK\n\r\t\tbitmask of CPUs to pin logs to. Default is all CPUs.\n\r"
	printf "\t--storage_count COUNT\n\r\t\tnumber of storage daemons to start.  Default ${STORAGE_COUNT}\n\r"
	printf "\t--storage_taskset BITMASK\n\r\t\tBitmask of CPUs to pin storage to. Default is all CPUs.\n\r"
	printf "\t--replication_count COUNT\n\r\t\tReplication count may be 1,2 or 3. Default is 1.\n\r"
	echo "Example"
	printf "\t${0} . --knobs '--knob_proxy_use_resolver_private_mutations=1' --stateless_count 4 --stateless_taskset 0xf --logs_count 8 --logs_taskset 0xff0 --storage_taskset 0xffff000\n\r"
	exit 1
}

function start_servers {
	for j in `seq 1 $1`; do
		LOG=${DIR}/${SERVER_COUNT}/log
		DATA=${DIR}/${SERVER_COUNT}/data
		mkdir -p ${LOG} ${DATA}
		PORT=$(( $PORT_PREFIX + $SERVER_COUNT ))
		ZONE=$4-Z-$(( $j % $REPLICATION_COUNT ))
		$2 ${FDB} -p auto:${PORT} $KNOBS -c $3 -d $DATA -L $LOG -C $CLUSTER --datacenter_id=$4 --locality-zoneid $ZONE --locality-machineid M-$SERVER_COUNT &
		SERVER_COUNT=$(( $SERVER_COUNT + 1 ))
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
	case "$1" in
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
	esac
	shift; shift
done

FDB=${BUILD}/bin/fdbserver
if [ ! -f ${FDB} ]; then
	echo "Error: ${FDB} not found!"
	usage
fi

if [ $REPLICATION_COUNT -eq 1 ]; then
	replication="single"
elif [ $REPLICATION_COUNT -eq 2 ]; then
	replication="double"
elif [ $REPLICATION_COUNT -eq 3 ]; then
	replication="triple"
else
	usage
fi

DIR=./loopback-cluster
rm -rf $DIR
mkdir -p ${DIR}

CLUSTER_FILE="test1:testdb1@127.0.0.1:$(( $PORT_PREFIX + 1))"
CLUSTER=${DIR}/fdb.cluster
echo $CLUSTER_FILE > $CLUSTER

echo "Starting Cluster: " $CLUSTER_FILE

start_servers $STATELESS_COUNT "$STATELESS_TASKSET" stateless DC1
start_servers $LOGS_COUNT "$LOGS_TASKSET" log DC1
start_servers $STORAGE_COUNT "$STORAGE_TASKSET" storage DC1

CLI="$BUILD/bin/fdbcli -C ${CLUSTER} --exec"
echo "configure new ssd $replication - stand by"

# sleep 2 seconds to wait for workers to join cluster, then configure database and coordinators
( sleep 2 ; $CLI "configure new ssd $replication" ; $CLI "coordinators auto")

if [ $LOGROUTER_COUNT -gt 0 ]; then
	start_servers $LOGROUTER_COUNT "$STORAGE_TASKSET" router DC3
	# Same number remote/satellite logs and ss as primary
	start_servers $LOGS_COUNT "$LOGS_TASKSET" log DC2
	start_servers $LOGS_COUNT "$LOGS_TASKSET" log DC3
	start_servers $STORAGE_COUNT "$STORAGE_TASKSET" storage DC3
	create_fileconfig
	$CLI "fileconfigure /tmp/fdbfileconfig.json"
	echo "Wait for data to be fully replicated (Healthy), then issue: $CLI configure usable_regions=2"
fi
