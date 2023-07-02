#!/usr/bin/python
#
# unit_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os
import sys
import time
import traceback
import json

import fdb

if __name__ == "__main__":
    fdb.api_version(fdb.LATEST_API_VERSION)

from cancellation_timeout_tests import test_timeouts
from cancellation_timeout_tests import test_db_timeouts
from cancellation_timeout_tests import test_cancellation
from cancellation_timeout_tests import test_retry_limits
from cancellation_timeout_tests import test_db_retry_limits
from cancellation_timeout_tests import test_combinations

from size_limit_tests import test_size_limit_option, test_get_approximate_size
from tenant_tests import test_tenants

VERBOSE = False


def log(msg):
    if VERBOSE:
        print(msg, file=sys.stderr, flush=True)


def test_fdb_transactional_generator(db):
    try:

        @fdb.transactional
        def function_that_yields(tr):
            yield 0

        assert (
            fdb.get_api_version() < 630
        ), "Pre-6.3, a decorator may wrap a function that yields"
    except ValueError:
        assert (
            fdb.get_api_version() >= 630
        ), "Post-6.3, a decorator should throw if wrapped function yields"


def test_fdb_transactional_returns_generator(db):
    try:

        def function_that_yields(tr):
            yield 0

        @fdb.transactional
        def function_that_returns(tr):
            return function_that_yields(tr)

        function_that_returns()
        assert fdb.get_api_version() < 630, "Pre-6.3, returning a generator is allowed"
    except ValueError:
        assert (
            fdb.get_api_version() >= 630
        ), "Post-6.3, returning a generator should throw"


def test_db_options(db):
    db.options.set_location_cache_size(100001)
    db.options.set_max_watches(100001)
    db.options.set_datacenter_id("dc_id")
    db.options.set_machine_id("machine_id")
    db.options.set_snapshot_ryw_enable()
    db.options.set_snapshot_ryw_disable()
    db.options.set_transaction_logging_max_field_length(1000)
    db.options.set_transaction_timeout(100000)
    db.options.set_transaction_timeout(0)
    db.options.set_transaction_timeout(0)
    db.options.set_transaction_max_retry_delay(100)
    db.options.set_transaction_size_limit(100000)
    db.options.set_transaction_retry_limit(10)
    db.options.set_transaction_retry_limit(-1)
    db.options.set_transaction_causal_read_risky()
    db.options.set_transaction_include_port_in_address()


@fdb.transactional
def test_options(tr):
    tr.options.set_priority_system_immediate()
    tr.options.set_priority_batch()
    tr.options.set_causal_read_risky()
    tr.options.set_causal_write_risky()
    tr.options.set_read_your_writes_disable()
    tr.options.set_read_system_keys()
    tr.options.set_access_system_keys()
    tr.options.set_transaction_logging_max_field_length(1000)
    tr.options.set_timeout(60 * 1000)
    tr.options.set_retry_limit(50)
    tr.options.set_max_retry_delay(100)
    tr.options.set_used_during_commit_protection_disable()
    tr.options.set_debug_transaction_identifier("my_transaction")
    tr.options.set_log_transaction()
    tr.options.set_read_lock_aware()
    tr.options.set_lock_aware()
    tr.options.set_include_port_in_address()
    tr.get(b"\xff").wait()


def check_watches(db, watches, expected):
    for i, watch in enumerate(watches):
        if watch.is_ready() or expected:
            try:
                watch.wait()
                if not expected:
                    assert False, "Watch %d is ready" % i
            except fdb.FDBError as e:
                tr = db.create_transaction()
                tr.on_error(e).wait()
                return False

    return True


def test_watches(db):
    while True:
        db[b"w0"] = b"0"
        db[b"w3"] = b"3"

        watches = [None]

        @fdb.transactional
        def txn1(tr):
            watches[0] = tr.watch(b"w0")
            tr.set(b"w0", b"0")
            assert not watches[0].is_ready()

        txn1(db)

        watches.append(db.clear_and_watch(b"w1"))
        watches.append(db.set_and_watch(b"w2", b"2"))
        watches.append(db.get_and_watch(b"w3"))

        assert watches[3][0] == b"3"
        watches[3] = watches[3][1]

        time.sleep(1)

        if not check_watches(db, watches, False):
            continue

        del db[b"w1"]

        time.sleep(5)

        if not check_watches(db, watches, False):
            continue

        db[b"w0"] = b"a"
        db[b"w1"] = b"b"
        del db[b"w2"]
        db.bit_xor(b"w3", b"\xff\xff")

        if check_watches(db, watches, True):
            return


@fdb.transactional
def test_locality(tr):
    tr.options.set_timeout(60 * 1000)
    tr.options.set_read_system_keys()  # We do this because the last shard (for now, someday the last N shards) is in the /FF/ keyspace

    # This isn't strictly transactional, thought we expect it to be given the size of our database
    boundary_keys = list(fdb.locality.get_boundary_keys(tr, b"", b"\xff\xff")) + [
        b"\xff\xff"
    ]
    end_keys = [
        tr.get_key(fdb.KeySelector.last_less_than(k)) for k in boundary_keys[1:]
    ]

    start_addresses = [
        fdb.locality.get_addresses_for_key(tr, k) for k in boundary_keys[:-1]
    ]
    end_addresses = [fdb.locality.get_addresses_for_key(tr, k) for k in end_keys]

    if [set(s.wait()) for s in start_addresses] != [
        set(e.wait()) for e in end_addresses
    ]:
        raise Exception("Locality not internally consistent.")


def test_predicates():
    assert fdb.predicates.is_retryable(fdb.FDBError(1020))
    assert not fdb.predicates.is_retryable(fdb.FDBError(10))


def test_get_client_status(db):
    @fdb.transactional
    def simple_txn(tr):
        tr.get_read_version().wait()

    # Execute a simple transaction
    # to make sure the database is initialized
    simple_txn(db)
    # Here we just check if a meaningful client report status is returned
    # Different report attributes and error cases are covered by C API tests
    status_str = db.get_client_status().wait()
    status = json.loads(status_str)
    assert "Healthy" in status
    assert status["Healthy"]


def run_unit_tests(db):
    try:
        log("test_db_options")
        test_db_options(db)
        log("test_options")
        test_options(db)
        log("test_watches")
        test_watches(db)
        log("test_cancellation")
        test_cancellation(db)
        log("test_retry_limits")
        test_retry_limits(db)
        log("test_db_retry_limits")
        test_db_retry_limits(db)
        log("test_timeouts")
        test_timeouts(db)
        log("test_db_timeouts")
        test_db_timeouts(db)
        log("test_combinations")
        test_combinations(db)
        log("test_locality")
        test_locality(db)
        log("test_predicates")
        test_predicates()
        log("test_size_limit_option")
        test_size_limit_option(db)
        log("test_get_approximate_size")
        test_get_approximate_size(db)
        log("test_get_client_status")
        test_get_client_status(db)

        if fdb.get_api_version() >= 710:
            log("test_tenants")
            test_tenants(db)

    except fdb.FDBError as e:
        print("Unit tests failed: %s" % e.description)
        traceback.print_exc()

        raise Exception("Unit tests failed: %s" % e.description)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
        Unit tests for python FDB API.
        """,
    )
    parser.add_argument(
        "--cluster-file",
        "-C",
        help="FDB cluster file",
        required=True,
    )
    parser.add_argument(
        "--verbose",
        "-V",
        help="Print diagnostic info",
        action="store_true",
    )
    args = parser.parse_args()
    if args.verbose:
        VERBOSE = True
    log("Opening database {}".format(args.cluster_file))
    db = fdb.open(args.cluster_file)
    run_unit_tests(db)
