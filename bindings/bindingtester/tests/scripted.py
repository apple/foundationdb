#
# scripted.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

import random

import fdb

from bindingtester import FDB_API_VERSION
from bindingtester import Result

from bindingtester.tests import Test, Instruction, ThreadedInstructionSet, ResultSpecification
from bindingtester.tests import test_util

fdb.api_version(FDB_API_VERSION)

# SOMEDAY: This should probably be broken up into smaller tests


class ScriptedTest(Test):
    TEST_API_VERSION = 720

    def __init__(self, subspace):
        super(ScriptedTest, self).__init__(subspace, ScriptedTest.TEST_API_VERSION, ScriptedTest.TEST_API_VERSION)
        self.workspace = self.subspace['workspace']
        self.results_subspace = self.subspace['results']
        # self.thread_subspace = self.subspace['threads'] # TODO: update START_THREAD so that we can create threads in subspaces

    def setup(self, args):
        if args.concurrency > 1:
            raise Exception('Scripted tests cannot be run with a concurrency greater than 1')

        # SOMEDAY: this is only a limitation because we don't know how many operations the bisection should start with
        # it should be fixable.
        #
        # We also need to enable the commented out support for num_ops in this file and make it so the default value runs
        # the entire test
        if args.bisect:
            raise Exception('Scripted tests cannot be bisected')

        self.api_version = args.api_version

    def generate(self, args, thread_number):
        self.results = []

        test_instructions = ThreadedInstructionSet()
        main_thread = test_instructions.create_thread()

        foo = [self.workspace.pack((b'foo%d' % i,)) for i in range(0, 6)]

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(1020)
        main_thread.append('ON_ERROR')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.append('GET_READ_VERSION')
        main_thread.push_args(foo[1], b'bar')
        main_thread.append('SET')
        main_thread.push_args(foo[1])
        main_thread.append('GET')
        self.add_result(main_thread, args, b'bar')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.push_args(2000)
        main_thread.append('ON_ERROR')
        self.add_result(main_thread, args, test_util.error_string(2000))

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(0)
        main_thread.append('ON_ERROR')
        self.add_result(main_thread, args, test_util.error_string(2000))

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(foo[1])
        main_thread.append('DUP')
        main_thread.append('DUP')
        main_thread.append('GET')
        self.add_result(main_thread, args, b'bar')
        main_thread.append('CLEAR')
        main_thread.append('GET_SNAPSHOT')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.push_args(foo[1])
        main_thread.append('GET_DATABASE')
        self.add_result(main_thread, args, b'bar')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.append('SET_READ_VERSION')
        main_thread.push_args(foo[1])
        main_thread.append('DUP')
        main_thread.append('GET')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.append('CLEAR')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, test_util.error_string(1020))

        main_thread.push_args(foo[1])
        main_thread.append('GET_SNAPSHOT')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.push_args(foo[1])
        main_thread.append('CLEAR')
        main_thread.append('COMMIT')
        main_thread.append('WAIT_FUTURE')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.append('GET_COMMITTED_VERSION')
        main_thread.append('RESET')
        main_thread.append('EMPTY_STACK')

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(1, b'bar', foo[1], foo[2], b'bar2', foo[3], b'bar3', foo[4], b'bar4', foo[5], b'bar5')
        main_thread.append('SWAP')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET_DATABASE')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.append('SET_READ_VERSION')
        main_thread.push_args(foo[2])
        main_thread.append('GET')
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(b'', 0, -1, b'')
        main_thread.append('GET_KEY')
        self.add_result(main_thread, args, b'')

        main_thread.append('NEW_TRANSACTION')
        main_thread.append('GET_READ_VERSION_SNAPSHOT')
        main_thread.push_args(b'random', foo[1], foo[3], 0, 1, 1)
        main_thread.append('POP')
        main_thread.append('GET_RANGE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[2], b'bar2', foo[1], b'bar')))
        main_thread.push_args(foo[1], foo[3], 1, 1, 0)
        main_thread.append('GET_RANGE_SNAPSHOT')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[2], b'bar2')))
        main_thread.push_args(foo[1], foo[3], 0, 0, 4)
        main_thread.append('GET_RANGE_DATABASE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2')))
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.push_args(foo[3], foo[5])
        main_thread.append('CLEAR_RANGE')
        main_thread.push_args(foo[1], 0, 3, b'')
        main_thread.append('GET_KEY')
        self.add_result(main_thread, args, foo[5])
        main_thread.push_args(foo[1], 1, 2, b'')
        main_thread.append('GET_KEY_SNAPSHOT')
        self.add_result(main_thread, args, foo[5])
        main_thread.push_args(foo[5], 0, -2, b'')
        main_thread.append('GET_KEY_DATABASE')
        self.add_result(main_thread, args, foo[2])
        main_thread.push_args(self.workspace.key(), 2, 0, 2)
        main_thread.append('GET_RANGE_STARTS_WITH')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2')))
        main_thread.push_args(self.workspace.key(), 4, 0, 3)
        main_thread.append('GET_RANGE_STARTS_WITH_SNAPSHOT')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2', foo[5], b'bar5')))
        main_thread.push_args(self.workspace.key(), 3, 1, -1)
        main_thread.append('GET_RANGE_STARTS_WITH_DATABASE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[5], b'bar5', foo[4], b'bar4', foo[3], b'bar3')))
        main_thread.push_args(foo[1], 0, 1, foo[1], 0, 3, 0, 0, -1, b'')
        main_thread.append('GET_RANGE_SELECTOR')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2')))
        main_thread.push_args(foo[1], 1, 0, foo[1], 1, 3, 0, 0, -1, b'')
        main_thread.append('GET_RANGE_SELECTOR_SNAPSHOT')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2', foo[5], b'bar5')))
        main_thread.push_args(foo[1], 0, 1, foo[1], 1, 3, 0, 0, -1, b'')
        main_thread.append('GET_RANGE_SELECTOR_DATABASE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[2], b'bar2', foo[3], b'bar3')))
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.push_args(self.workspace.key())
        main_thread.append('CLEAR_RANGE_STARTS_WITH')
        main_thread.push_args(self.workspace.key(), 0, 0, -1)
        main_thread.append('GET_RANGE_STARTS_WITH')
        self.add_result(main_thread, args, b'')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.append('SET_READ_VERSION')
        main_thread.push_args(foo[1])
        main_thread.append('GET')
        self.add_result(main_thread, args, b'bar')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.push_args(foo[1], b'bar', foo[2], b'bar2', foo[3], b'bar3', foo[4], b'bar4', foo[5], b'bar5')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET')
        main_thread.append('SET')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.push_args(foo[2])
        main_thread.append('CLEAR_DATABASE')
        main_thread.append('WAIT_FUTURE')
        main_thread.push_args(self.workspace.key(), 0, 0, -1)
        main_thread.append('GET_RANGE_STARTS_WITH_DATABASE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[3], b'bar3', foo[4], b'bar4', foo[5], b'bar5')))

        main_thread.push_args(foo[3], foo[5])
        main_thread.append('CLEAR_RANGE_DATABASE')
        main_thread.append('WAIT_FUTURE')
        main_thread.push_args(self.workspace.key(), 0, 0, -1)
        main_thread.append('GET_RANGE_STARTS_WITH_DATABASE')
        self.add_result(main_thread, args, fdb.tuple.pack((foo[1], b'bar', foo[5], b'bar5')))

        main_thread.push_args(self.workspace.key())
        main_thread.append('CLEAR_RANGE_STARTS_WITH_DATABASE')
        main_thread.append('WAIT_FUTURE')
        main_thread.push_args(self.workspace.key(), 0, 0, -1)
        main_thread.append('GET_RANGE_STARTS_WITH_DATABASE')
        self.add_result(main_thread, args, b'')

        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')

        main_thread.append('NEW_TRANSACTION')
        main_thread.push_args(foo[1], foo[5], 0, 0, 0)
        main_thread.append('GET_RANGE')
        self.add_result(main_thread, args, test_util.error_string(2210))
        main_thread.push_args(foo[1], foo[5], 0, 0, 0)
        main_thread.append('GET_RANGE_DATABASE')
        self.add_result(main_thread, args, test_util.error_string(2210))

        self.append_range_test(main_thread, args, 100, 256)
        self.append_range_test(main_thread, args, 1000, 8)

        main_thread.append('EMPTY_STACK')
        tup = (0, b'foo', -1093, 'unicode\u9348test', 0xffffffff + 100, b'bar\x00\xff')
        main_thread.push_args(*test_util.with_length(tup))
        main_thread.append('TUPLE_PACK')
        main_thread.append('DUP')
        self.add_result(main_thread, args, fdb.tuple.pack(tup))
        main_thread.append('TUPLE_UNPACK')
        for item in reversed(tup):
            self.add_result(main_thread, args, fdb.tuple.pack((item,)))

        main_thread.push_args(0xffffffff, -100)
        main_thread.append('SUB')
        main_thread.push_args(1)
        main_thread.append('TUPLE_PACK')
        self.add_result(main_thread, args, fdb.tuple.pack((0xffffffff + 100,)))

        main_thread.append('EMPTY_STACK')
        main_thread.push_args(*test_util.with_length(tup))
        main_thread.append('TUPLE_RANGE')
        rng = fdb.tuple.range(tup)
        self.add_result(main_thread, args, rng.stop)
        self.add_result(main_thread, args, rng.start)

        stampKey = b'stampedXXXXXXXXXXsuffix'
        stampKeyIndex = stampKey.find(b'XXXXXXXXXX')
        main_thread.push_args('SET_VERSIONSTAMPED_KEY', self.versionstamp_key(stampKey, stampKeyIndex), b'stampedBar')
        main_thread.append('ATOMIC_OP')
        main_thread.push_args('SET_VERSIONSTAMPED_VALUE', b'stampedValue', self.versionstamp_value(b'XXXXXXXXXX'))
        main_thread.append('ATOMIC_OP')

        if self.api_version >= 520:
            stampValue = b'stampedXXXXXXXXXXsuffix'
            stampValueIndex = stampValue.find(b'XXXXXXXXXX')
            main_thread.push_args('SET_VERSIONSTAMPED_VALUE', b'stampedValue2', self.versionstamp_value(stampValue, stampValueIndex))
            main_thread.append('ATOMIC_OP')

        main_thread.push_args(b'suffix')
        main_thread.append('GET_VERSIONSTAMP')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        main_thread.push_args(b'stamped')
        main_thread.append('CONCAT')
        main_thread.append('CONCAT')
        main_thread.append('GET')
        self.add_result(main_thread, args, b'stampedBar')

        main_thread.push_args(b'stampedValue', b'suffix')
        main_thread.append('GET')
        main_thread.push_args(b'stamped')
        main_thread.append('CONCAT')
        main_thread.append('CONCAT')
        main_thread.append('GET')
        self.add_result(main_thread, args, b'stampedBar')

        if self.api_version >= 520:
            main_thread.push_args(b'stampedValue2')
            main_thread.append('GET')
            main_thread.append('GET')
            self.add_result(main_thread, args, b'stampedBar')

        main_thread.append('GET_VERSIONSTAMP')
        test_util.blocking_commit(main_thread)
        self.add_result(main_thread, args, b'RESULT_NOT_PRESENT')
        self.add_result(main_thread, args, test_util.error_string(2021))

        main_thread.push_args(b'sentinel')
        main_thread.append('UNIT_TESTS')
        self.add_result(main_thread, args, b'sentinel')

        if not args.no_threads:
            wait_key = b'waitKey'
            # threads = [self.thread_subspace[i] for i in range(0, 2)]
            threads = [b'thread_spec%d' % i for i in range(0, 2)]
            for thread_spec in threads:
                main_thread.push_args(self.workspace.pack((wait_key, thread_spec)), b'')
                main_thread.append('SET_DATABASE')
                main_thread.append('WAIT_FUTURE')

            for thread_spec in threads:
                main_thread.push_args(thread_spec)
                # if len(main_thread) < args.num_ops:
                main_thread.append('START_THREAD')
                thread = test_instructions.create_thread(fdb.Subspace((thread_spec,)))
                thread.append('NEW_TRANSACTION')
                thread.push_args(foo[1], foo[1], b'bar%s' % thread_spec, self.workspace.pack(
                    (wait_key, thread_spec)), self.workspace.pack((wait_key, thread_spec)))
                thread.append('GET')
                thread.append('POP')
                thread.append('SET')
                thread.append('CLEAR')
                test_util.blocking_commit(thread)
                thread.append('POP')
                thread.append('CLEAR_DATABASE')
                thread.push_args(self.workspace.pack((wait_key,)))
                thread.append('WAIT_EMPTY')

                thread.append('NEW_TRANSACTION')
                thread.push_args(foo[1])
                thread.append('GET')
                self.add_result(thread, args, b'barthread_spec0', b'barthread_spec1')

        main_thread.append('EMPTY_STACK')
        # if len(main_thread) > args.num_ops:
        #     main_thread[args.num_ops:] = []

        return test_instructions

    def get_result_specifications(self):
        return [
            ResultSpecification(self.results_subspace, ordering_index=0, global_error_filter=[1007, 1009, 1021])
        ]

    def get_expected_results(self):
        return {self.results_subspace: self.results}

    def append_range_test(self, instructions, args, num_pairs, kv_length):
        instructions.append('NEW_TRANSACTION')

        instructions.push_args(self.workspace.key())
        instructions.append('CLEAR_RANGE_STARTS_WITH')

        kvpairs = []
        for i in range(0, num_pairs * 2):
            kvpairs.append(self.workspace.pack((b'foo', bytes([random.randint(0, 254) for i in range(0, kv_length)]))))

        kvpairs = list(set(kvpairs))
        if len(kvpairs) % 2 == 1:
            kvpairs = kvpairs[:-1]
        kvpairs.sort()

        instructions.push_args(*kvpairs)
        for i in range(0, len(kvpairs) // 2):
            instructions.append('SET')
            if i % 100 == 99:
                test_util.blocking_commit(instructions)
                self.add_result(instructions, args, b'RESULT_NOT_PRESENT')

        foo_range = self.workspace.range((b'foo',))
        instructions.push_args(foo_range.start, foo_range.stop, 0, 0, -1)
        instructions.append('GET_RANGE')
        self.add_result(instructions, args, fdb.tuple.pack(tuple(kvpairs)))
        instructions.push_args(self.workspace.key(), 0, 0, -1)
        instructions.append('GET_RANGE_STARTS_WITH')
        self.add_result(instructions, args, fdb.tuple.pack(tuple(kvpairs)))
        instructions.push_args(foo_range.start, 0, 1, foo_range.stop, 0, 1, 0, 0, -1, b'')
        instructions.append('GET_RANGE_SELECTOR')
        self.add_result(instructions, args, fdb.tuple.pack(tuple(kvpairs)))
        test_util.blocking_commit(instructions)
        self.add_result(instructions, args, b'RESULT_NOT_PRESENT')

    def add_result(self, instructions, args, *values):
        key = self.results_subspace.pack((len(self.results),))
        instructions.push_args(key)
        instructions.append('SET_DATABASE')

        # if len(instructions) <= args.num_ops:
        self.results.append(Result(self.results_subspace, key, values))

        instructions.append('POP')
