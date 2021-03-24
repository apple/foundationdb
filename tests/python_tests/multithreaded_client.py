#!/bin/env python2

import argparse

parser = argparse.ArgumentParser("Run multithreaded client tests")

parser.add_argument("cluster_file", nargs='+', help='List of fdb.cluster files to connect to')
parser.add_argument("--skip-so-files", default=False, action='store_true', help='Do not load .so files')
parser.add_argument("--threads", metavar="N", type=int, default=3, help='Number of threads to use.  Zero implies local client')
parser.add_argument("--build-dir", metavar="DIR", default='.', help='Path to root directory of FDB build output')
parser.add_argument("--client-log-dir", metavar="DIR", default="client-logs", help="Path to write client logs to.  The directory will be created if it does not exist.")
args = parser.parse_args()

import sys

### sample usage (from inside your FDB build output directory):

## These should pass:
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster'
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster --threads 1'
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster --threads 1 --skip-so-files'
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster --threads 0'
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster --threads 0 --skip-so-files'

## This fails (unsupported configuration):
# ../tests/loopback_cluster/run_cluster.sh . 3 '../tests/python_tests/multithreaded_client.py loopback-cluster-*/fdb.cluster --threads 2 --skip-so-files'

sys.path.append(args.build_dir + '/bindings/python')

import fdb
import os
import random
import time
fdb.api_version(630)

if not os.path.exists(args.client_log_dir):
    os.mkdir(args.client_log_dir)

fdb.options.set_trace_enable(args.client_log_dir)
fdb.options.set_knob("min_trace_severity=5")

if not args.skip_so_files:
    print("Loading .so files")
    fdb.options.set_external_client_directory(args.build_dir + '/lib')

if args.threads > 0:
    fdb.options.set_client_threads_per_version(args.threads)

dbs = []
for v in args.cluster_file:
    dbs.append(fdb.open(cluster_file=v))

counter = 0
for i in range(100):
    key = b"test_%d" % random.randrange(0, 100000000)
    val = b"value_%d" % random.randrange(0, 10000000)
    db = dbs[i % len(dbs)]
    print ("Writing: ", key, val, db)
    db[key] = val
    assert (val == db[key])
