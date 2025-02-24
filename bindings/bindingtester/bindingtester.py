#!/usr/bin/env python3
#
# bindingtester.py
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


import sys
import subprocess
import random
import argparse
import os
import copy
import traceback
from threading import Timer, Event

import logging.config

from collections import OrderedDict
from functools import reduce

sys.path[:0] = [os.path.join(os.path.dirname(__file__), "..")]

from bindingtester import FDB_API_VERSION
from bindingtester import Result

from bindingtester import util
from bindingtester.tests import Test, InstructionSet

from bindingtester.known_testers import Tester

import fdb
import fdb.tuple


API_VERSIONS = [
    13,
    14,
    16,
    21,
    22,
    23,
    100,
    200,
    300,
    400,
    410,
    420,
    430,
    440,
    450,
    460,
    500,
    510,
    520,
    600,
    610,
    620,
    630,
    700,
    710,
    720,
    730,
    800,
]

assert (
    API_VERSIONS[-1] == FDB_API_VERSION
), "Bindingtester API version list must be updated to include all supported versions"

fdb.api_version(FDB_API_VERSION)


class ResultSet(object):
    def __init__(self, specification):
        self.specification = specification
        self.tester_results = OrderedDict()

    def add(self, name, results):
        num = 1
        base_name = name
        while name in self.tester_results:
            num += 1
            name = "%s (%d)" % (base_name, num)

        self.tester_results[name] = results

    @staticmethod
    def _min_tuple(t1, t2):
        return t1 if fdb.tuple.compare(t1, t2) < 0 else t2

    def check_for_errors(self):
        if len(self.tester_results) == 1:
            return (0, False)

        util.get_logger().info(
            "Comparing results from '%s'..."
            % repr(util.subspace_to_tuple(self.specification.subspace))
        )

        num_errors = 0
        has_filtered_error = False

        # Tracks the current result being evaluated for each tester
        indices = [0 for i in range(len(self.tester_results))]

        name_length = max([len(name) for name in self.tester_results.keys()])

        while True:
            # Gets the next result for each tester
            results = {
                i: r[indices[i]]
                for i, r in enumerate(self.tester_results.values())
                if len(r) > indices[i]
            }
            if len(results) == 0:
                break

            # Attempt to 'align' the results. If two results have matching sequence numbers, then they should be compared.
            # Only those testers which have a result matching the minimum current sequence number will be included. All
            # others are considered to have not produced a result and will be evaluated in a future iteration.
            sequence_nums = [
                r.sequence_num(self.specification) for r in results.values()
            ]
            if any([s is not None for s in sequence_nums]):
                results = {
                    i: r
                    for i, r in results.items()
                    if r.sequence_num(self.specification) == min(sequence_nums)
                }

            # If these results aren't using sequence numbers, then we match two results based on whether they share the same key
            else:
                min_key = reduce(
                    ResultSet._min_tuple,
                    [r.key(self.specification) for r in results.values()],
                )
                results = {
                    i: r
                    for i, r in results.items()
                    if Result.tuples_match(r.key(self.specification), min_key)
                }

            # Increment the indices for those testers which produced a result in this iteration
            for i in results.keys():
                indices[i] += 1

            # Fill in 'None' values for testers that didn't produce a result and generate an output string describing the results
            all_results = {
                i: results[i] if i in results else None
                for i in range(len(self.tester_results))
            }
            result_keys = list(self.tester_results.keys())
            result_str = "\n".join(
                [
                    "  %-*s - %s" % (name_length, result_keys[i], r)
                    for i, r in all_results.items()
                ]
            )

            result_list = list(results.values())

            # If any of our results matches the global error filter, we ignore the result
            if any(
                r.matches_global_error_filter(self.specification) for r in result_list
            ):
                has_filtered_error = True

            # The result is considered correct if every tester produced a value and all the values meet the matching criteria
            if len(results) < len(all_results) or not all(
                result_list[0].matches(r, self.specification) for r in result_list
            ):
                util.get_logger().error("\nIncorrect result: \n%s" % result_str)
                num_errors += 1
            else:
                util.get_logger().debug("\nCorrect result: \n%s" % result_str)

        if num_errors > 0:
            util.get_logger().error("")
        else:
            util.get_logger().debug("")

        return (num_errors, has_filtered_error)


def choose_api_version(
    selected_api_version,
    tester_min_version,
    tester_max_version,
    test_min_version,
    test_max_version,
):
    if selected_api_version is not None:
        if (
            selected_api_version < tester_min_version
            or selected_api_version > tester_max_version
        ):
            raise Exception(
                "Not all testers support the API version %d (min=%d, max=%d)"
                % (selected_api_version, tester_min_version, tester_max_version)
            )
        elif (
            selected_api_version < test_min_version
            or selected_api_version > test_max_version
        ):
            raise Exception(
                "API version %d is not supported by the specified test (min=%d, max=%d)"
                % (selected_api_version, test_min_version, test_max_version)
            )

        api_version = selected_api_version
    else:
        min_version = max(tester_min_version, test_min_version)
        max_version = min(tester_max_version, test_max_version)

        if min_version > max_version:
            raise Exception(
                "Not all testers support the API versions required by the specified test"
                "(tester: min=%d, max=%d; test: min=%d, max=%d)"
                % (
                    tester_min_version,
                    tester_max_version,
                    test_min_version,
                    test_max_version,
                )
            )

        if random.random() < 0.7:
            api_version = max_version
        elif random.random() < 0.7:
            api_version = min_version
        elif random.random() < 0.9:
            api_version = random.choice(
                [v for v in API_VERSIONS if v >= min_version and v <= max_version]
            )
        else:
            api_version = random.randint(min_version, max_version)

    return api_version


class TestRunner(object):
    def __init__(self, args):
        self.args = copy.copy(args)

        self.db = fdb.open(self.args.cluster_file)
        self.test_seed = random.randint(0, 0xFFFFFFFF)

        self.testers = [Tester.get_test(self.args.test1)]
        if self.args.test2 is not None:
            self.testers.append(Tester.get_test(self.args.test2))

        self.test = Test.create_test(
            self.args.test_name, fdb.Subspace((self.args.output_subspace,))
        )

        if self.test is None:
            raise Exception("the test '%s' could not be found" % self.args.test_name)

        min_api_version = max([tester.min_api_version for tester in self.testers])
        max_api_version = min([tester.max_api_version for tester in self.testers])
        self.args.api_version = choose_api_version(
            self.args.api_version,
            min_api_version,
            max_api_version,
            self.test.min_api_version,
            self.test.max_api_version,
        )

        util.get_logger().info(
            "\nCreating test at API version %d" % self.args.api_version
        )

        max_int_bits = min([tester.max_int_bits for tester in self.testers])
        if self.args.max_int_bits is None:
            self.args.max_int_bits = max_int_bits
        elif self.args.max_int_bits > max_int_bits:
            raise Exception(
                "The specified testers support at most %d-bit ints, but --max-int-bits was set to %d"
                % (max_int_bits, self.args.max_int_bits)
            )

        self.args.no_threads = self.args.no_threads or any(
            [not tester.threads_enabled for tester in self.testers]
        )
        if self.args.no_threads and self.args.concurrency > 1:
            raise Exception("Not all testers support concurrency")

        # Test types should be intersection of all tester supported types
        self.args.types = list(
            reduce(
                lambda t1, t2: filter(t1.__contains__, t2),
                map(lambda tester: tester.types, self.testers),
            )
        )

        self.args.no_directory_snapshot_ops = (
            self.args.no_directory_snapshot_ops
            or any(
                [not tester.directory_snapshot_ops_enabled for tester in self.testers]
            )
        )
        self.args.no_tenants = (
            self.args.no_tenants
            or any([not tester.tenants_enabled for tester in self.testers])
            or self.args.api_version < 710
        )

    def print_test(self):
        test_instructions = self._generate_test()

        for top_level_subspace, top_level_thread in test_instructions.items():
            for subspace, thread in top_level_thread.get_threads(
                top_level_subspace
            ).items():
                util.get_logger().error(
                    "\nThread at prefix %r:" % util.subspace_to_tuple(subspace)
                )
                if self.args.print_all:
                    instructions = thread
                    offset = 0
                else:
                    instructions = thread.core_instructions()
                    offset = thread.core_test_begin

                for i, instruction in enumerate(instructions):
                    if self.args.print_all or (
                        instruction.operation != "SWAP"
                        and instruction.operation != "PUSH"
                    ):
                        util.get_logger().error("  %d. %r" % (i + offset, instruction))

        util.get_logger().error("")

    def run_test(self):
        test_instructions = self._generate_test()
        expected_results = self.test.get_expected_results()

        tester_results = {
            s.subspace: ResultSet(s) for s in self.test.get_result_specifications()
        }
        for subspace, results in expected_results.items():
            tester_results[subspace].add("expected", results)

        tester_errors = {}

        for tester in self.testers:
            self._insert_instructions(test_instructions)
            self.test.pre_run(self.db, self.args)
            return_code = self._run_tester(tester)
            if return_code != 0:
                util.get_logger().error(
                    "Test of type %s failed to complete successfully with random seed %d and %d operations\n"
                    % (self.args.test_name, self.args.seed, self.args.num_ops)
                )
                return 2

            tester_errors[tester] = self.test.validate(self.db, self.args)

            for spec in self.test.get_result_specifications():
                tester_results[spec.subspace].add(
                    tester.name, self._get_results(spec.subspace)
                )

        return_code = self._validate_results(tester_errors, tester_results)
        util.get_logger().info(
            "Completed %s test with random seed %d and %d operations\n"
            % (self.args.test_name, self.args.seed, self.args.num_ops)
        )

        return return_code

    def insert_test(self):
        test_instructions = self._generate_test()
        self._insert_instructions(test_instructions)

    def _generate_test(self):
        util.get_logger().info(
            "Generating %s test at seed %d with %d op(s) and %d concurrent tester(s)..."
            % (
                self.args.test_name,
                self.args.seed,
                self.args.num_ops,
                self.args.concurrency,
            )
        )

        random.seed(self.test_seed)

        if self.args.concurrency == 1:
            self.test.setup(self.args)
            test_instructions = {
                fdb.Subspace(
                    (bytes(self.args.instruction_prefix, "utf-8"),)
                ): self.test.generate(self.args, 0)
            }
        else:
            test_instructions = {}
            main_thread = InstructionSet()
            for i in range(self.args.concurrency):
                # thread_spec = fdb.Subspace(('thread_spec', i))
                thread_spec = b"thread_spec%d" % i
                main_thread.push_args(thread_spec)
                main_thread.append("START_THREAD")
                self.test.setup(self.args)
                test_instructions[fdb.Subspace((thread_spec,))] = self.test.generate(
                    self.args, i
                )

            test_instructions[
                fdb.Subspace((bytes(self.args.instruction_prefix, "utf-8"),))
            ] = main_thread

        return test_instructions

    def _insert_instructions(self, test_instructions):
        util.get_logger().info("\nInserting test into database...")
        del self.db[:]

        while True:
            tr = self.db.create_transaction()
            try:
                tr.options.set_special_key_space_enable_writes()
                del tr[
                    b"\xff\xff/management/tenant/map/":b"\xff\xff/management/tenant/map0"
                ]
                tr.commit().wait()
                break
            except fdb.FDBError as e:
                tr.on_error(e).wait()

        for subspace, thread in test_instructions.items():
            thread.insert_operations(self.db, subspace)

    def _run_tester(self, test):
        params = test.cmd.split(" ") + [
            self.args.instruction_prefix,
            str(self.args.api_version),
        ]
        if self.args.cluster_file is not None:
            params += [self.args.cluster_file]

        util.get_logger().info("\nRunning tester '%s'..." % " ".join(params))
        sys.stdout.flush()
        proc = subprocess.Popen(params)
        timed_out = Event()

        def killProc():
            proc.kill()
            timed_out.set()

        timer = Timer(self.args.timeout, killProc)
        try:
            timer.start()
            ret_code = proc.wait()
        except Exception as e:
            raise Exception("Unable to run tester (%s)" % e)
        finally:
            timer.cancel()

        if ret_code != 0:
            signal_name = str(ret_code)
            if ret_code < 0:
                signal_name = util.signal_number_to_name(-ret_code)

            reason = "exit code: %s" % (signal_name,)
            if timed_out.is_set():
                reason = "timed out after %d seconds" % (self.args.timeout,)
            util.get_logger().error(
                "\n'%s' did not complete successfully (%s)" % (params[0], reason)
            )

        util.get_logger().info("")
        return ret_code

    def _get_results(self, subspace, instruction_index=None):
        util.get_logger().info(
            "Reading results from '%s'..." % repr(util.subspace_to_tuple(subspace))
        )

        results = []
        next_key = subspace.range().start
        while True:
            next_results = self.db.get_range(next_key, subspace.range().stop, 1000)
            if len(next_results) == 0:
                break

            results += [Result(subspace, kv.key, (kv.value,)) for kv in next_results]
            next_key = fdb.KeySelector.first_greater_than(next_results[-1].key)

        return results

    def _validate_results(self, tester_errors, tester_results):
        util.get_logger().info("")

        num_incorrect = 0
        has_filtered_error = False
        for r in tester_results.values():
            (count, filtered_error) = r.check_for_errors()
            num_incorrect += count
            has_filtered_error = has_filtered_error or filtered_error

        num_errors = sum([len(e) for e in tester_errors.values()])

        for tester, errors in tester_errors.items():
            if len(errors) > 0:
                util.get_logger().error(
                    "The %s tester reported errors:\n" % tester.name
                )
                for i, error in enumerate(errors):
                    util.get_logger().error("  %d. %s" % (i + 1, error))

        log_message = (
            "\nTest with seed %d and concurrency %d had %d incorrect result(s) and %d error(s) at API version %d"
            % (
                self.args.seed,
                self.args.concurrency,
                num_incorrect,
                num_errors,
                self.args.api_version,
            )
        )
        if num_errors == 0 and (num_incorrect == 0 or has_filtered_error):
            util.get_logger().info(log_message)
            if has_filtered_error:
                util.get_logger().info(
                    "Test had permissible non-deterministic errors; disregarding results..."
                )
            return 0
        else:
            util.get_logger().error(log_message)
            return 1


def bisect(test_runner, args):
    util.get_logger().info("")

    lower_bound = 0
    upper_bound = args.num_ops

    while True:
        test_runner.args.num_ops = int((lower_bound + upper_bound) / 2)
        result = test_runner.run_test()

        if lower_bound == upper_bound:
            if result != 0:
                util.get_logger().error(
                    "Found minimal failing test with %d operations" % lower_bound
                )
                if args.print_test:
                    test_runner.print_test()

                return 0
            elif upper_bound < args.num_ops:
                util.get_logger().error(
                    "Error finding minimal failing test for seed %d. The failure may not be deterministic"
                    % args.seed
                )
                return 1
            else:
                util.get_logger().error(
                    "No failing test found for seed %d with %d ops. Try specifying a larger --num-ops parameter."
                    % (args.seed, args.num_ops)
                )
                return 0

        elif result == 0:
            util.get_logger().info(
                "Test with %d operations succeeded\n" % test_runner.args.num_ops
            )
            lower_bound = test_runner.args.num_ops + 1

        else:
            util.get_logger().info(
                "Test with %d operations failed with error code %d\n"
                % (test_runner.args.num_ops, result)
            )
            upper_bound = test_runner.args.num_ops


def parse_args(argv):
    parser = argparse.ArgumentParser(description="FoundationDB Binding API Tester")
    parser.add_argument(
        "--test-name",
        default="scripted",
        help="The name of the test to run. Must be the name of a test specified in the tests folder. (default='scripted')",
    )

    parser.add_argument(
        metavar="tester1", dest="test1", help="Name of the first tester to invoke"
    )
    parser.add_argument(
        "--compare",
        metavar="tester2",
        nargs="?",
        type=str,
        default=None,
        const="python",
        dest="test2",
        help="When specified, a second tester will be run and compared against the first. This flag takes an optional argument "
        "for the second tester to invoke (default = 'python').",
    )
    parser.add_argument(
        "--print-test",
        action="store_true",
        help="Instead of running a test, prints the set of instructions generated for that test. Unless --all is specified, all "
        "setup, finalization, PUSH, and SWAP instructions will be excluded.",
    )
    parser.add_argument(
        "--all",
        dest="print_all",
        action="store_true",
        help="Causes --print-test to print all instructions.",
    )
    parser.add_argument(
        "--bisect",
        action="store_true",
        help="Run the specified test varying the number of operations until a minimal failing test is found. Does not work for "
        "concurrent tests.",
    )
    parser.add_argument(
        "--insert-only",
        action="store_true",
        help="Insert the test instructions into the database, but do not run it.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent test threads to run. (default = 1).",
    )
    parser.add_argument(
        "--num-ops",
        type=int,
        default=100,
        help="The number of operations to generate per thread (default = 100)",
    )
    parser.add_argument(
        "--seed", type=int, help="The random seed to use for generating the test"
    )
    parser.add_argument(
        "--max-int-bits",
        type=int,
        default=None,
        help="Maximum number of bits to use for int types in testers. By default, the largest value supported by the testers being "
        "run will be chosen.",
    )
    parser.add_argument(
        "--api-version",
        default=None,
        type=int,
        help="The API version that the testers should use. Not supported in scripted mode. (default = random version supported by "
        "all testers)",
    )
    parser.add_argument(
        "--cluster-file",
        type=str,
        default=None,
        help="The cluster file for the cluster being connected to. (default None)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="The timeout in seconds for running each individual tester. (default 600)",
    )
    parser.add_argument(
        "--enable-client-trace-logging",
        nargs="?",
        type=str,
        default=None,
        const=".",
        help="Enables trace file output. This flag takes an optional argument specifying the output directory (default = '.').",
    )
    parser.add_argument(
        "--instruction-prefix",
        type=str,
        default="test_spec",
        help="The prefix under which the main thread of test instructions are inserted (default='test_spec').",
    )
    parser.add_argument(
        "--output-subspace",
        type=str,
        default="tester_output",
        help="The string used to create the output subspace for the testers. The subspace will be of the form (<output_subspace>,). "
        "(default='tester_output')",
    )

    parser.add_argument(
        "--logging-level",
        type=str,
        default="INFO",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        help="Specifies the level of detail in the tester output (default='INFO').",
    )

    # SOMEDAY: this applies only to the scripted test. Should we invoke test files specifically (as in circus),
    # or invoke them here and allow tests to add arguments?
    parser.add_argument(
        "--no-threads",
        action="store_true",
        help="Disables the START_THREAD instruction in the scripted test.",
    )

    parser.add_argument(
        "--no-directory-snapshot-ops",
        action="store_true",
        help="Disables snapshot operations for directory instructions.",
    )

    parser.add_argument(
        "--no-tenants", action="store_true", help="Disables tenant operations."
    )

    return parser.parse_args(argv)


def validate_args(args):
    if args.insert_only and args.bisect:
        raise Exception("--bisect cannot be used with --insert-only")
    if args.print_all and not args.print_test:
        raise Exception("cannot specify --all without --print-test")
    if args.bisect and not args.seed:
        raise Exception("--seed must be specified if using --bisect")
    if args.concurrency < 1:
        raise Exception("--concurrency must be a positive integer")
    if args.concurrency > 1 and args.test2:
        raise Exception("--compare cannot be used with concurrent tests")


def main(argv):
    args = parse_args(argv)
    try:
        from bindingtester import LOGGING

        logging.config.dictConfig(LOGGING)
        util.initialize_logger_level(args.logging_level)

        validate_args(args)

        if args.seed is None:
            args.seed = random.randint(0, 0xFFFFFFFF)

        random.seed(args.seed)

        if args.enable_client_trace_logging is not None:
            fdb.options.set_trace_enable(args.enable_client_trace_logging)

        test_runner = TestRunner(args)

        if args.bisect:
            return bisect(test_runner, args)

        if args.print_test:
            return test_runner.print_test()

        if args.insert_only:
            return test_runner.insert_test()

        return test_runner.run_test()

    except Exception as e:
        util.get_logger().error("\nERROR: %s" % e)
        util.get_logger().debug(traceback.format_exc())
        exit(3)

    except BaseException:
        util.get_logger().error("\nERROR: %s" % sys.exc_info()[0])
        util.get_logger().info(traceback.format_exc())
        exit(3)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
