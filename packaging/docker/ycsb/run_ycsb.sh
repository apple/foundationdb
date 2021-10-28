#!/usr/bin/env bash
set -Eeuo pipefail

namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)

echo "WAITING FOR ALL PODS TO COME UP"
while [[ $(kubectl get pods -n ${namespace} -l name=ycsb,run=${RUN_ID} --field-selector=status.phase=Running | grep -cv NAME) -lt ${NUM_PODS} ]]; do
    sleep 0.1
done
echo "ALL PODS ARE UP"

echo "RUNNING YCSB"
./bin/ycsb.sh ${MODE} foundationdb -s -P workloads/${WORKLOAD} ${YCSB_ARGS}
echo "YCSB FINISHED"

echo "COPYING HISTOGRAMS TO S3"
aws s3 sync --sse aws:kms --exclude "*" --include "histogram.*" /tmp s3://${BUCKET}/ycsb_histgorams/${namespace}/${POD_NAME}
echo "COPYING HISTOGRAMS TO S3 FINISHED"