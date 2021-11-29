#!/usr/bin/env bash
set -Eeuo pipefail

namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
POD_NUM=$(echo $POD_NAME | cut -d - -f3)
KEY="ycsb_load_${POD_NUM}_of_${NUM_PODS}_complete"
CLI=$(ls /var/dynamic-conf/bin/*/fdbcli | head -n1)
if [ ${MODE} != "load" ]; then
    echo "WAITING FOR ALL PODS TO COME UP"
    while [[ $(kubectl get pods -n ${namespace} -l name=ycsb,run=${RUN_ID} --field-selector=status.phase=Running | grep -cv NAME) -lt ${NUM_PODS} ]]; do
        sleep 0.1
    done
    echo "ALL PODS ARE UP"
else
    if ${CLI} --exec "get ${KEY}" | grep is ;
    then
        # load already completed
        exit 0
    fi
fi;

echo "RUNNING YCSB"
./bin/ycsb.sh ${MODE} foundationdb -s -P workloads/${WORKLOAD} ${YCSB_ARGS}
echo "YCSB FINISHED"

echo "COPYING HISTOGRAMS TO S3"
aws s3 sync --sse aws:kms --exclude "*" --include "histogram.*" /tmp s3://${BUCKET}/ycsb_histograms/${namespace}/${POD_NAME}
echo "COPYING HISTOGRAMS TO S3 FINISHED"

if [ ${MODE} == "load" ]; then
    ${CLI} --exec "writemode on; set ${KEY} 1"
    echo "WROTE LOAD COMPLETION KEY"
fi
