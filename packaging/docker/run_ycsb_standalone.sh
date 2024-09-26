#!/usr/bin/env bash
# This script is like the run_ycsb.sh script only there is no presumption of
# a k8s context. It also takes a method from the adjacent fdb.bash script for
# writing out an fdb.cluster file based off environment variables.
set -Eeuo pipefail

function logg () {
    printf "##### $(date +'%Y-%m-%dT%H:%M:%SZ') #  %-56.55s #####\n" "${1}"
}

function error_exit () {
    echo "################################################################################"
    logg "${0} FAILED"
    logg "RUN_ID: ${RUN_ID}"
    logg "WORKLOAD: ${WORKLOAD}"
    logg "ENVIRONMENT IS:"
    env
    echo "################################################################################"
}

function create_cluster_file() {
    FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}
    mkdir -p "$(dirname $FDB_CLUSTER_FILE)"

    if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
        echo "$FDB_CLUSTER_FILE_CONTENTS" > "$FDB_CLUSTER_FILE"
    elif [[ -n $FDB_COORDINATOR ]]; then
        coordinator_ip=$(dig +short "$FDB_COORDINATOR")
        if [[ -z "$coordinator_ip" ]]; then
            echo "Failed to look up coordinator address for $FDB_COORDINATOR" 1>&2
            exit 1
        fi
        coordinator_port=${FDB_COORDINATOR_PORT:-4500}
        echo "docker:docker@$coordinator_ip:$coordinator_port" > "$FDB_CLUSTER_FILE"
    else
        echo "FDB_COORDINATOR environment variable not defined" 1>&2
        exit 1
    fi
}

trap error_exit ERR

logg "RUNNING YCSB ${WORKLOAD}"
set -x
create_cluster_file
./bin/ycsb.sh "${MODE}" foundationdb -s -P "workloads/${WORKLOAD}" "${YCSB_ARGS}"
set +x
logg "YCSB ${WORKLOAD} FINISHED"

echo "################################################################################"
logg "COMPLETED ${0}"
logg "RUN_ID: ${RUN_ID}"
logg "WORKLOAD: ${WORKLOAD}"
echo "################################################################################"
