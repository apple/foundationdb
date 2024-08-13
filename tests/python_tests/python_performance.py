#!/usr/bin/python
#
# python_performance.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
import random
import math
import traceback

from collections import OrderedDict

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from python_tests import PythonTest

import fdb
import fdb.tuple

fdb.api_version(400)


class PythonPerformance(PythonTest):
    tests = {
        "future_latency": "Python API future throughput",
        "set": "Python API set throughput",
        "clear": "Python API clear throughput",
        "clear_range": "Python API clear_range throughput",
        "parallel_get": "Python API parallel get throughput",
        "serial_get": "Python API serial get throughput",
        "get_range": "Python API get_range throughput",
        "get_key": "Python API get_key throughput",
        "get_single_key_range": "Python API get_single_key_range throughput",
        "alternating_get_set": "Python API alternating get and set throughput",
        "write_transaction": "Python API single-key transaction throughput",
    }

    def __init__(self, key_count=1000000, key_size=16, value_size=100):
        super(PythonPerformance, self).__init__()
        self.key_count = key_count
        self.key_size = key_size
        self.value_str = "".join(["x" for _ in range(value_size)])

    # Python Performance Tests (checks if functions run and yield correct results, gets performance indicators)
    def run_test(self):
        try:
            db = fdb.open(None, "DB")
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("fdb.open failed"))
            return

        try:
            self.test_performance(db)
        except KeyboardInterrupt:
            raise
        except Exception:
            self.result.add_error(self.get_error("Failed to complete all tests"))

    def random_key(self):
        return self.key(random.randint(0, self.key_count - 1))

    def key(self, num):
        return "%0*d" % (self.key_size, num)

    def value(self, key):
        return self.value_str

    def insert_data(self, db):
        print("Loading database")
        del db[:]
        num_keys = 100000 / (self.key_size + len(self.value_str))

        trs = [
            db.create_transaction()
            for _ in range(int(math.ceil(float(self.key_count) / num_keys)))
        ]
        success = [False for _ in range(len(trs))]

        while not all(success):
            futures = {}

            for i in range(self.key_count):
                if not success[i / num_keys]:
                    trs[i / num_keys][self.key(i)] = self.value(self.key(i))

            for i, tr in enumerate(trs):
                if not success[i]:
                    tr.options.set_retry_limit(5)
                    futures[i] = tr.commit()

            for i, f in futures.items():
                try:
                    f.wait()
                    success[i] = True
                except fdb.FDBError as e:
                    trs[i].on_error(e).wait()

        time.sleep(60)  # Give the database time to rebalance

    # Tests the performance of the API functions
    def test_performance(self, db):
        self.insert_data(db)

        if not self.args.tests_to_run:
            self.args.tests_to_run = PythonPerformance.tests.keys()
        else:
            for t in self.args.tests_to_run:
                if t not in PythonPerformance.tests:
                    raise Exception("Unknown Python performance test '%s'" % t)

        num_runs = 25

        # Run each test
        for test in self.args.tests_to_run:
            time.sleep(5)
            print("Running test %s" % test)
            results = []

            fxn_name = "run_%s" % test
            assert hasattr(self, fxn_name), (
                "Test function %s not implemented" % fxn_name
            )

            # Perform each test several times
            for x in range(0, num_runs):
                try:
                    results.append(getattr(self, fxn_name)(db))
                except KeyboardInterrupt:
                    raise
                except Exception:
                    self.result.add_error(
                        self.get_error(
                            "Performance test failed: " + PythonPerformance.tests[test]
                        )
                    )
                    break

            if len(results) == num_runs:
                median = sorted(results)[num_runs / 2]
                self.result.add_kpi(
                    "%s (%s)"
                    % (PythonPerformance.tests[test], self.multi_version_description()),
                    int(median),
                    "keys/s",
                )

    @fdb.transactional
    def run_future_latency(self, tr, count=100000):
        tr.options.set_retry_limit(5)
        tr.get_read_version().wait()

        s = time.time()

        for i in range(count):
            tr.get_read_version().wait()

        return count / (time.time() - s)

    # Tests the performance of the 'clear' function
    def run_clear(self, db, count=100000):
        tr = db.create_transaction()
        s = time.time()

        for i in range(count):
            del tr[self.random_key()]

        return count / (time.time() - s)

    # Tests the performance of the 'clear_range' function
    def run_clear_range(self, db, count=100000):
        tr = db.create_transaction()
        s = time.time()

        for i in range(count):
            key = self.random_key()
            del tr[key : self.key(int(key) + 1)]

        return count / (time.time() - s)

    # Tests the performance of the 'set' function
    def run_set(self, db, count=100000):
        tr = db.create_transaction()
        s = time.time()

        for i in range(count):
            key = self.random_key()
            tr[key] = self.value(key)

        return count / (time.time() - s)

    # Tests the parallel performance of the 'get' function
    @fdb.transactional
    def run_parallel_get(self, tr, count=10000):
        tr.options.set_retry_limit(5)
        s = time.time()

        futures = []
        for i in range(count):
            futures.append(tr[self.random_key()])

        for future in futures:
            future.wait()

        return count / (time.time() - s)

    @fdb.transactional
    def run_alternating_get_set(self, tr, count=2000):
        tr.options.set_retry_limit(5)
        s = time.time()

        futures = []

        for i in range(count):
            key = self.random_key()
            val = self.value(key)
            tr[key] = val
            futures.append(tr[key])

        for f in futures:
            f.wait()

        return count / (time.time() - s)

    # Tests the serial performance of the 'get' function
    @fdb.transactional
    def run_serial_get(self, tr, count=2000):
        tr.options.set_retry_limit(5)

        if count > self.key_count / 2:
            keys = [self.random_key() for _ in range(count)]
        else:
            key_set = OrderedDict()
            while len(key_set) < count:
                key_set[self.random_key()] = ""
            keys = key_set.keys()

        s = time.time()

        for k in keys:
            tr[k].wait()

        return count / (time.time() - s)

    # Tests the performance of the 'get_range' function
    @fdb.transactional
    def run_get_range(self, tr, count=100000):
        tr.options.set_retry_limit(5)
        b = random.randint(0, self.key_count - count)
        s = time.time()

        list(tr[self.key(b) : self.key(b + count)])

        return count / (time.time() - s)

    # Tests the performance of the 'get_key' function
    @fdb.transactional
    def run_get_key(self, tr, count=2000):
        tr.options.set_retry_limit(5)
        s = time.time()

        for i in range(count):
            tr.get_key(
                fdb.KeySelector(self.random_key(), True, random.randint(-10, 10))
            ).wait()

        return count / (time.time() - s)

    @fdb.transactional
    def run_get_single_key_range(self, tr, count=2000):
        tr.options.set_retry_limit(5)
        s = time.time()

        for i in range(count):
            index = random.randint(0, self.key_count)
            list(tr.get_range(self.key(index), self.key(index + 1), limit=2))

        return count / (time.time() - s)

    @fdb.transactional
    def single_set(self, tr):
        key = self.random_key()
        tr[key] = self.value(key)

    def run_write_transaction(self, db, count=1000):
        s = time.time()

        for i in range(count):
            self.single_set(db)

        return count / (time.time() - s)

    # Adds the stack trace to an error message
    def get_error(self, message):
        error_message = message + "\n" + traceback.format_exc()
        print("%s" % error_message)
        return error_message


if __name__ == "__main__":
    print(
        "Running PythonPerformance test on Python version %d.%d.%d%s%d"
        % (
            sys.version_info[0],
            sys.version_info[1],
            sys.version_info[2],
            sys.version_info[3][0],
            sys.version_info[4],
        )
    )

    parser = argparse.ArgumentParser()

    tests = sorted(PythonPerformance.tests.keys())
    assert len(tests) > 0, "Python performance test has no test functions"
    test_string = ", ".join(tests[:-1])
    if len(tests) > 1:
        test_string += ", and "

    test_string += tests[-1]

    parser.add_argument(
        "--tests-to-run",
        nargs="*",
        help="Names of tests to run. Can be any of %s. By default, all tests are run."
        % test_string,
    )
    PythonPerformance().run(parser=parser)
