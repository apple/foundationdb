#
# __init__.py
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

import math
import re
import struct

import fdb

from bindingtester import FDB_API_VERSION
from bindingtester import util

fdb.api_version(FDB_API_VERSION)


class ResultSpecification(object):
    def __init__(
        self, subspace, key_start_index=0, ordering_index=None, global_error_filter=None
    ):
        self.subspace = subspace
        self.key_start_index = key_start_index
        self.ordering_index = ordering_index

        if global_error_filter is not None:
            error_str = b"|".join([b"%d" % e for e in global_error_filter])
            self.error_regex = re.compile(
                rb"\x01+ERROR\x00\xff*\x01" + error_str + rb"\x00"
            )
        else:
            self.error_regex = None

    def matches_global_error_filter(self, str):
        if self.error_regex is None:
            return False

        return self.error_regex.search(str) is not None


class Test(object):
    def __init__(self, subspace, min_api_version=0, max_api_version=int(1e9)):
        self.subspace = subspace
        self.min_api_version = min_api_version
        self.max_api_version = max_api_version

    # Returns nothing
    def setup(self, args):
        pass

    # Returns an instance of TestInstructions
    def generate(self, args, thread_number):
        pass

    # Returns nothing
    def pre_run(self, db, args):
        pass

    # Returns a list of ResultSpecifications to read data from and compare with other testers
    def get_result_specifications(self):
        return []

    # Returns a dict { subspace => results } of results that the test is expected to have.
    # Compared against subspaces returned by get_result_subspaces. A subspace omitted from this dictionary
    # can still be compared against other testers if it is added to the list returned by get_result_subspaces.
    def get_expected_results(self):
        return {}

    # Returns a list of error strings
    def validate(self, db, args):
        return []

    def versionstamp_key(self, raw_bytes, version_pos):
        if hasattr(self, "api_version") and self.api_version < 520:
            return raw_bytes + struct.pack("<H", version_pos)
        else:
            return raw_bytes + struct.pack("<L", version_pos)

    def versionstamp_value(self, raw_bytes, version_pos=0):
        if hasattr(self, "api_version") and self.api_version < 520:
            if version_pos != 0:
                raise ValueError(
                    "unable to set non-zero version position before 520 in values"
                )
            return raw_bytes
        else:
            return raw_bytes + struct.pack("<L", version_pos)

    @classmethod
    def create_test(cls, name, subspace):
        target = "bindingtester.tests.%s" % name
        test_class = [s for s in cls.__subclasses__() if s.__module__ == target]
        if len(test_class) == 0:
            return None

        return test_class[0](subspace)


class Instruction(object):
    def __init__(self, operation):
        self.operation = operation
        self.argument = None
        self.value = fdb.tuple.pack((self.operation,))

    def to_value(self):
        return self.value

    def __str__(self):
        return self.operation

    def __repr__(self):
        return repr(self.operation)


class PushInstruction(Instruction):
    def __init__(self, argument):
        self.operation = "PUSH"
        self.argument = argument
        self.value = fdb.tuple.pack(("PUSH", argument))

    def __str__(self):
        return "%s %s" % (self.operation, self.argument)

    def __repr__(self):
        return "%r %r" % (self.operation, self.argument)


class TestInstructions(object):
    def __init__(self):
        pass

    # returns a dictionary of subspace => InstructionSets
    def get_threads(self, subspace):
        pass

    def insert_operations(self, db, subspace):
        pass


class InstructionSet(TestInstructions, list):
    def __init__(self):
        TestInstructions.__init__(self)
        list.__init__(self)

        self.core_test_begin = 0
        self.core_test_end = None

    def push_args(self, *args):
        self.extend([PushInstruction(arg) for arg in reversed(args)])

    def append(self, instruction):
        if isinstance(instruction, Instruction):
            list.append(self, instruction)
        else:
            list.append(self, Instruction(instruction))

    def get_threads(self, subspace):
        return {subspace: self}

    def setup_complete(self):
        self.core_test_begin = len(self)

    def begin_finalization(self):
        self.core_test_end = len(self)

    def core_instructions(self):
        return self[self.core_test_begin : self.core_test_end]

    @fdb.transactional
    def _insert_operations_transactional(self, tr, subspace, start, count):
        for i, instruction in enumerate(self[start : start + count]):
            tr[subspace.pack((start + i,))] = instruction.to_value()

    def insert_operations(self, db, subspace):
        for i in range(0, int(math.ceil(len(self) / 5000.0))):
            self._insert_operations_transactional(db, subspace, i * 5000, 5000)


class ThreadedInstructionSet(TestInstructions):
    def __init__(self):
        super(ThreadedInstructionSet, self).__init__()
        self.threads = {}

    def get_threads(self, subspace):
        result = dict(self.threads)
        if None in self.threads:
            result[subspace] = result[None]
            del result[None]

        return result

    def insert_operations(self, db, subspace):
        for thread_subspace, thread in self.threads.items():
            if thread_subspace is None:
                thread_subspace = subspace

            thread.insert_operations(db, thread_subspace)

    def create_thread(self, subspace=None, thread_instructions=None):
        if subspace in self.threads:
            raise "An instruction set with the subspace %r has already been created" % util.subspace_to_tuple(
                subspace
            )

        if thread_instructions == None:
            thread_instructions = InstructionSet()

        self.threads[subspace] = thread_instructions
        return thread_instructions


util.import_subclasses(__file__, "bindingtester.tests")
