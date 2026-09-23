#! /usr/bin/env bash

set -e
set -o pipefail

# The Joshua agent image sets these to point at an external client directory that
# may hold libfdb_c versions too old for the binaries under test, which makes the
# multi-version client fail to load (api_function_missing, 2204). The tests here
# use the client shipped in the package, so drop them (see also bindingTest.sh).
unset FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY
unset FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY

# It is necessary to tee to output.log in case timeout happens
python3 ./binding_test.py --stop-at-failure 10 --fdbserver-path $(pwd)/fdbserver --fdbcli-path $(pwd)/fdbcli --libfdb-path $(pwd) --num-ops 1000 --num-hca-ops 100 --concurrency 5 --test-timeout 60 --random 2>&1 | tee output.log
