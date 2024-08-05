#
# tuple.py
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
import struct

import fdb
import fdb.tuple

from bindingtester import FDB_API_VERSION
from bindingtester import util
from bindingtester.tests import Test, Instruction, InstructionSet, ResultSpecification
from bindingtester.tests import test_util

fdb.api_version(FDB_API_VERSION)


class TupleTest(Test):
    def __init__(self, subspace):
        super(TupleTest, self).__init__(subspace)
        self.workspace = self.subspace[
            "workspace"
        ]  # The keys and values here must match between subsequent runs of the same test
        self.stack_subspace = self.subspace["stack"]

    def setup(self, args):
        self.max_int_bits = args.max_int_bits
        self.api_version = args.api_version

    def generate(self, args, thread_number):
        instructions = InstructionSet()

        min_value = -(2**self.max_int_bits) + 1
        max_value = 2**self.max_int_bits - 1

        instructions.append("NEW_TRANSACTION")

        # Test integer encoding
        mutations = 0
        for i in range(0, self.max_int_bits + 1):
            for sign in [-1, 1]:
                sign_str = "" if sign == 1 else "-"
                for offset in range(-10, 11):
                    val = (2**i) * sign + offset
                    if val >= min_value and val <= max_value:
                        if offset == 0:
                            add_str = ""
                        elif offset > 0:
                            add_str = "+%d" % offset
                        else:
                            add_str = "%d" % offset

                        instructions.push_args(1, val)
                        instructions.append("TUPLE_PACK")
                        instructions.push_args(
                            self.workspace.pack(("%s2^%d%s" % (sign_str, i, add_str),))
                        )
                        instructions.append("SET")
                        mutations += 1

            if mutations >= 5000:
                test_util.blocking_commit(instructions)
                mutations = 0

        instructions.begin_finalization()

        test_util.blocking_commit(instructions)
        instructions.push_args(self.stack_subspace.key())
        instructions.append("LOG_STACK")

        test_util.blocking_commit(instructions)

        return instructions

    def get_result_specifications(self):
        return [
            ResultSpecification(self.workspace, global_error_filter=[1007, 1009, 1021]),
            ResultSpecification(
                self.stack_subspace,
                key_start_index=1,
                ordering_index=1,
                global_error_filter=[1007, 1009, 1021],
            ),
        ]
