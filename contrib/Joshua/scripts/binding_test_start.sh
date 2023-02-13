#! /usr/bin/env bash

set -e
set -o pipefail

# It is necessary to tee to output.log in case timeout happens
python3 ./binding_test.py --stop-at-failure 10 --fdbserver-path $(pwd)/fdbserver --fdbcli-path $(pwd)/fdbcli --libfdb-path $(pwd) --num-ops 1000 --num-hca-ops 100 --concurrency 5 --test-timeout 60 --random | tee output.log