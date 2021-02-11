#!/bin/env python2
import sys

### sample usage (from inside your FDB build output directory):

## Spawn three test clusters:
# ../tests/loopback_cluster/run_cluster.sh . 3 &

## Run this test
# ../tests/python_tests/multithreaded_client.py . loopback-cluster-*/fdb.cluster

## Cleanup
# pkill run_cluster.sh
# rm -rf loopback-cluster-* client-logs

builddir = sys.argv[1]
clusters = sys.argv[2:]

sys.path.append(builddir + '/bindings/python')

import fdb
import os
import random
import time
fdb.api_version(630)

if not os.path.exists("client-logs"):
    os.mkdir("client-logs")

fdb.options.set_trace_enable("client-logs/")
fdb.options.set_external_client_directory(builddir + '/lib')
fdb.options.set_knob("min_trace_severity=5")


fdb.options.set_client_threads_per_version(len(clusters))

dbs = []
for v in clusters:
    dbs.append(fdb.open(cluster_file=v))

counter = 0
for i in range(100):
    key = b"test_%d" % random.randrange(0, 100000000)
    val = b"value_%d" % random.randrange(0, 10000000)
    db = dbs[i % len(dbs)]
    print ("Writing: ", key, val, db)
    db[key] = val
    assert (val == db[key])
