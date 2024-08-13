#!/usr/bin/env python
#
# ryw_benchmark.py
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
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from python_tests import PythonTest

import fdb

fdb.api_version(400)


class RYWBenchmark(PythonTest):
    tests = {
        "get_single": "RYW: get single cached value throughput",
        "get_many_sequential": "RYW: get sequential cached values throughput",
        "get_range_basic": "RYW: get range cached values throughput",
        "single_clear_get_range": "RYW: get range cached values with clears throughput",
        "clear_range_get_range": "RYW: get range cached values with clear ranges throughput",
        "interleaved_sets_gets": "RYW: interleaved sets and gets on a single key throughput",
    }

    def __init__(self, key_count=10000, key_size=16):
        super(RYWBenchmark, self).__init__()
        self.key_count = key_count
        self.key_size = key_size

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

    def key(self, num):
        return "%0*d" % (self.key_size, num)

    # Adds the stack trace to an error message
    def get_error(self, message):
        error_message = message + "\n" + traceback.format_exc()
        print(error_message)
        return error_message

    def test_performance(self, db):
        tr = db.create_transaction()
        self.insert_data(tr)

        if not self.args.tests_to_run:
            self.args.tests_to_run = RYWBenchmark.tests.keys()
        else:
            for t in self.args.tests_to_run:
                if t not in RYWBenchmark.tests:
                    raise Exception("Unknown RYW benchmark test '%s'" % t)

        num_runs = 25

        for test in self.args.tests_to_run:
            time.sleep(5)
            print("Running test %s" % test)
            results = []

            fxn_name = "run_%s" % test
            assert hasattr(self, fxn_name), (
                "Test function %s is not implemented" % fxn_name
            )

            for x in range(0, num_runs):
                try:
                    results.append(getattr(self, fxn_name)(tr))
                except KeyboardInterrupt:
                    raise
                except Exception:
                    self.result.add_error(
                        self.get_error(
                            "Performance test failed: " + RYWBenchmark.tests[test]
                        )
                    )
                    break

            if len(results) == num_runs:
                median = sorted(results)[num_runs / 2]
                self.result.add_kpi(RYWBenchmark.tests[test], int(median), "keys/s")

    def insert_data(self, tr):
        del tr[:]
        for i in range(0, 10000):
            tr[self.key(i)] = "foo"

    def run_get_single(self, tr, count=10000):
        start = time.time()
        for i in range(count):
            tr.get(self.key(5001)).wait()
        return count / (time.time() - start)

    def run_get_many_sequential(self, tr, count=10000):
        start = time.time()
        for j in range(count):
            tr.get(self.key(j)).wait()
        return count / (time.time() - start)

    def run_get_range_basic(self, tr, count=100):
        start = time.time()
        for i in range(count):
            list(tr.get_range(self.key(0), self.key(self.key_count)))
        return self.key_count * count / (time.time() - start)

    def run_single_clear_get_range(self, tr, count=100):
        for i in range(0, self.key_count, 2):
            tr.clear(self.key(i))
        start = time.time()
        for i in range(0, count):
            list(tr.get_range(self.key(0), self.key(self.key_count)))
        kpi = self.key_count * count / 2 / (time.time() - start)
        self.insert_data(tr)
        return kpi

    def run_clear_range_get_range(self, tr, count=100):
        for i in range(0, self.key_count, 4):
            tr.clear_range(self.key(i), self.key(i + 1))
        start = time.time()
        for i in range(0, count):
            list(tr.get_range(self.key(0), self.key(self.key_count)))
        kpi = self.key_count * count * 3 / 4 / (time.time() - start)
        self.insert_data(tr)
        return kpi

    def run_interleaved_sets_gets(self, tr, count=10000):
        start = time.time()
        tr["foo"] = str(1)
        for i in range(count):
            old = int(tr.get("foo").wait())
            tr.set("foo", str(old + 1))
        return count / (time.time() - start)


if __name__ == "__main__":
    print(
        "Running RYW Benchmark test on Python version %d.%d.%d%s%d"
        % (
            sys.version_info[0],
            sys.version_info[1],
            sys.version_info[2],
            sys.version_info[3][0],
            sys.version_info[4],
        )
    )

    parser = argparse.ArgumentParser()

    tests = sorted(RYWBenchmark.tests.keys())
    assert len(tests) > 0, "RYW benchmark test has no test_functions"
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
    RYWBenchmark().run(parser=parser)
