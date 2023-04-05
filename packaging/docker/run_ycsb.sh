#!/usr/bin/env bash
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

trap error_exit ERR

namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)

logg "WAITING FOR ${NUM_PODS} PODS TO COME UP IN ${namespace}"
while [[ $(kubectl get pods -n "${namespace}" -l name=ycsb,run="${RUN_ID}" --field-selector=status.phase=Running | grep -cv NAME) -lt ${NUM_PODS} ]]; do
    sleep 1
done
logg "${NUM_PODS} PODS ARE UP IN ${namespace}"

logg "RUNNING YCSB ${WORKLOAD}"
set -x
./bin/ycsb.sh "${MODE}" foundationdb -s -P "workloads/${WORKLOAD}" "${YCSB_ARGS}"
set +x
logg "YCSB ${WORKLOAD} FINISHED"

logg "COPYING HISTOGRAMS TO S3"
set -x
aws s3 sync --sse aws:kms --exclude "*" --include "histogram.*" /tmp "s3://${BUCKET}/ycsb_histograms/${namespace}/${POD_NAME}"
set +x
logg "COPYING HISTOGRAMS TO S3 FINISHED"

echo "################################################################################"
logg "COMPLETED ${0}"
logg "RUN_ID: ${RUN_ID}"
logg "WORKLOAD: ${WORKLOAD}"
echo "################################################################################"
