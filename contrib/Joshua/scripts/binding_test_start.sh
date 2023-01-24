#! /usr/bin/env bash

python3 ./binding_test.py --debug --stop-at-failure 10 --fdbserver-path $(pwd)/fdbserver --fdbcli-path $(pwd)/fdbcli --libfdb-path $(pwd) --num-ops 1000 --num-hca-ops 100 --concurrency 5 --test-timeout 600