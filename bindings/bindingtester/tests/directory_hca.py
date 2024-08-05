#
# directory_hca.py
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

import random

import fdb

from bindingtester import FDB_API_VERSION
from bindingtester import util

from bindingtester.tests import Test, Instruction, InstructionSet, ResultSpecification
from bindingtester.tests import test_util, directory_util

fdb.api_version(FDB_API_VERSION)


class DirectoryHcaTest(Test):
    def __init__(self, subspace):
        super(DirectoryHcaTest, self).__init__(subspace)
        self.coordination = subspace["coordination"]
        self.prefix_log = subspace["prefix_log"]
        self.next_path = 1

    def setup(self, args):
        self.random = test_util.RandomGenerator(
            args.max_int_bits, args.api_version, args.types
        )
        self.transactions = [
            b"tr%d" % i for i in range(3)
        ]  # SOMEDAY: parameterize this number?
        self.barrier_num = 0

        self.max_directories_per_transaction = 30
        if args.api_version < 300:
            if args.concurrency > 8:
                raise Exception(
                    "Directory HCA test does not support concurrency larger than 8 with API version less than 300"
                )

            self.max_directories_per_transaction = 8.0 / args.concurrency

    def commit_transactions(self, instructions, args):
        for tr in self.transactions:
            if random.random() < 0.8 or args.api_version < 300:
                instructions.push_args(tr)
                instructions.append("USE_TRANSACTION")
                test_util.blocking_commit(instructions)

    def barrier(self, instructions, thread_number, thread_ending=False):
        if not thread_ending:
            instructions.push_args(
                self.coordination[(self.barrier_num + 1)][thread_number].key(), b""
            )
            instructions.append("SET_DATABASE")
            instructions.append("WAIT_FUTURE")

        instructions.push_args(self.coordination[self.barrier_num][thread_number].key())
        instructions.append("CLEAR_DATABASE")
        instructions.append("WAIT_FUTURE")
        instructions.push_args(self.coordination[self.barrier_num].key())
        instructions.append("WAIT_EMPTY")

        self.barrier_num += 1

    def generate(self, args, thread_number):
        instructions = InstructionSet()

        instructions.append("NEW_TRANSACTION")

        default_path = "default%d" % self.next_path
        self.next_path += 1
        dir_list = directory_util.setup_directories(
            instructions, default_path, self.random
        )
        num_dirs = len(dir_list)

        instructions.push_args(directory_util.DEFAULT_DIRECTORY_INDEX)
        instructions.append("DIRECTORY_CHANGE")

        instructions.setup_complete()

        current_op = 0

        while current_op < args.num_ops:
            if args.concurrency > 1:
                self.barrier(instructions, thread_number)

            instructions.push_args(random.choice(self.transactions))
            instructions.append("USE_TRANSACTION")

            if thread_number == 0 and args.concurrency > 1:
                num_directories = 1
            else:
                num_directories = int(
                    max(
                        1,
                        pow(random.random(), 4)
                        * min(
                            self.max_directories_per_transaction,
                            args.num_ops - current_op,
                        ),
                    )
                )

            for i in range(num_directories):
                path = (self.random.random_unicode_str(16),)
                op_args = test_util.with_length(path) + (b"", None)
                directory_util.push_instruction_and_record_prefix(
                    instructions,
                    "DIRECTORY_CREATE",
                    op_args,
                    path,
                    num_dirs,
                    self.random,
                    self.prefix_log,
                )
                num_dirs += 1

            current_op += num_directories

            if args.concurrency > 1:
                self.barrier(
                    instructions,
                    thread_number,
                    thread_ending=(current_op >= args.num_ops),
                )

            if thread_number == 0:
                self.commit_transactions(instructions, args)

        return instructions

    @fdb.transactional
    def pre_run(self, tr, args):
        if args.concurrency > 1:
            for i in range(args.concurrency):
                tr[self.coordination[0][i]] = b""

    def validate(self, db, args):
        errors = []
        errors += directory_util.check_for_duplicate_prefixes(db, self.prefix_log)
        errors += directory_util.validate_hca_state(db)

        return errors
