#
# directory_util.py
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

import struct

import fdb

from bindingtester import FDB_API_VERSION
from bindingtester import util

from bindingtester.tests import test_util
from bindingtester.tests.directory_state_tree import DirectoryStateTreeNode

fdb.api_version(FDB_API_VERSION)

DEFAULT_DIRECTORY_INDEX = 4
DEFAULT_DIRECTORY_PREFIX = b"default"
DIRECTORY_ERROR_STRING = b"DIRECTORY_ERROR"


def setup_directories(instructions, default_path, random):
    # Clients start with the default directory layer in the directory list
    DirectoryStateTreeNode.reset()
    dir_list = [DirectoryStateTreeNode.get_layer(b"\xfe")]

    instructions.push_args(0, b"\xfe")
    instructions.append("DIRECTORY_CREATE_SUBSPACE")
    dir_list.append(DirectoryStateTreeNode(False, True))

    instructions.push_args(0, b"")
    instructions.append("DIRECTORY_CREATE_SUBSPACE")
    dir_list.append(DirectoryStateTreeNode(False, True))

    instructions.push_args(1, 2, 1)
    instructions.append("DIRECTORY_CREATE_LAYER")
    dir_list.append(DirectoryStateTreeNode.get_layer(b"\xfe"))

    create_default_directory_subspace(instructions, default_path, random)
    dir_list.append(
        dir_list[0].add_child(
            (default_path,), DirectoryStateTreeNode(True, True, has_known_prefix=True)
        )
    )
    DirectoryStateTreeNode.set_default_directory(dir_list[-1])

    instructions.push_args(DEFAULT_DIRECTORY_INDEX)
    instructions.append("DIRECTORY_SET_ERROR_INDEX")

    return dir_list


def create_default_directory_subspace(instructions, path, random):
    test_util.blocking_commit(instructions)
    instructions.push_args(3)
    instructions.append("DIRECTORY_CHANGE")
    prefix = random.random_string(16)
    instructions.push_args(1, path, b"", b"%s-%s" % (DEFAULT_DIRECTORY_PREFIX, prefix))
    instructions.append("DIRECTORY_CREATE_DATABASE")

    instructions.push_args(DEFAULT_DIRECTORY_INDEX)
    instructions.append("DIRECTORY_CHANGE")


def push_instruction_and_record_prefix(
    instructions, op, op_args, path, dir_index, random, subspace
):
    if not op.endswith("_DATABASE"):
        instructions.push_args(1, *test_util.with_length(path))
        instructions.append("DIRECTORY_EXISTS")

    # This op must leave the stack in the state it is in at this point, with the exception
    # that it may leave an error on the stack
    instructions.push_args(*op_args)
    instructions.append(op)

    if not op.endswith("_DATABASE"):
        instructions.push_args(dir_index)
        instructions.append("DIRECTORY_CHANGE")

        instructions.push_args(1, b"", random.random_string(16), b"")
        instructions.append("DIRECTORY_PACK_KEY")
        test_util.to_front(
            instructions, 3
        )  # move the existence result up to the front of the stack

        t = util.subspace_to_tuple(subspace)
        instructions.push_args(len(t) + 3, *t)

        instructions.append(
            "TUPLE_PACK"
        )  # subspace[<exists>][<packed_key>][random.random_string(16)] = b''
        instructions.append("SET")

        instructions.push_args(DEFAULT_DIRECTORY_INDEX)
        instructions.append("DIRECTORY_CHANGE")


def check_for_duplicate_prefixes(db, subspace):
    last_prefix = None
    start_key = subspace[0].range().start

    duplicates = set()
    count = 0
    while True:
        prefixes = db.get_range(start_key, subspace[0].range().stop, limit=1000)
        if len(prefixes) == 0:
            break

        start_key = fdb.KeySelector.first_greater_than(prefixes[-1].key)

        prefixes = [subspace[0].unpack(kv.key)[0] for kv in prefixes]
        prefixes = [
            p
            for p in prefixes
            if not (
                p.startswith(DEFAULT_DIRECTORY_PREFIX) or p == DIRECTORY_ERROR_STRING
            )
        ]
        count += len(prefixes)

        prefixes = [last_prefix] + prefixes
        duplicates.update([p for i, p in enumerate(prefixes[1:]) if p == prefixes[i]])
        last_prefix = prefixes[-1]

    util.get_logger().info("Checked %d directory prefixes for duplicates" % count)
    return [
        "The prefix %r was allocated multiple times" % d[:-2] for d in set(duplicates)
    ]


def validate_hca_state(db):
    hca = fdb.Subspace((b"\xfe", b"hca"), b"\xfe")
    counters = hca[0]
    recent = hca[1]

    last_counter = db.get_range(
        counters.range().start, counters.range().stop, limit=1, reverse=True
    )
    [(start, reported_count)] = [
        (counters.unpack(kv.key)[0], struct.unpack("<q", kv.value)[0])
        for kv in last_counter
    ] or [(0, 0)]

    actual_count = len(db[recent[start] : recent.range().stop])
    if actual_count > reported_count:
        return [
            "The HCA reports %d prefixes allocated in current window, but it actually allocated %d"
            % (reported_count, actual_count)
        ]

    return []
