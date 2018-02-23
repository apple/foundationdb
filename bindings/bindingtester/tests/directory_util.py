#
# directory_util.py
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
import struct

import fdb

from bindingtester import FDB_API_VERSION
from bindingtester import util

from bindingtester.tests import test_util

fdb.api_version(FDB_API_VERSION)

DEFAULT_DIRECTORY_INDEX = 4
DEFAULT_DIRECTORY_PREFIX = 'default'
DIRECTORY_ERROR_STRING = 'DIRECTORY_ERROR'


class DirListEntry:
    dir_id = 0  # Used for debugging

    def __init__(self, is_directory, is_subspace, has_known_prefix=True, path=(), root=None):
        self.root = root or self
        self.path = path
        self.is_directory = is_directory
        self.is_subspace = is_subspace
        self.has_known_prefix = has_known_prefix
        self.children = {}

        self.dir_id = DirListEntry.dir_id + 1
        DirListEntry.dir_id += 1

    def __repr__(self):
        return 'DirEntry %d %r: %d' % (self.dir_id, self.path, self.has_known_prefix)

    def add_child(self, subpath, default_path, root, child):
        if default_path in root.children:
            # print 'Adding child %r to default directory %r at %r' % (child, root.children[DirectoryTest.DEFAULT_DIRECTORY_PATH].path, subpath)
            c = root.children[default_path]._add_child_impl(subpath, child)
            child.has_known_prefix = c.has_known_prefix and child.has_known_prefix
            # print 'Added %r' % c

        # print 'Adding child %r to directory %r at %r' % (child, self.path, subpath)
        c = self._add_child_impl(subpath, child)
        # print 'Added %r' % c
        return c

    def _add_child_impl(self, subpath, child):
        # print '%d, %d. Adding child (recursive): %s %s' % (self.dir_id, child.dir_id, repr(self.path), repr(subpath))
        if len(subpath) == 0:
            self.has_known_prefix = self.has_known_prefix and child.has_known_prefix
            # print '%d, %d. Setting child: %d' % (self.dir_id, child.dir_id, self.has_known_prefix)
            self._merge_children(child)

            return self
        else:
            if not subpath[0] in self.children:
                # print '%d, %d. Path %s was absent (%s)' % (self.dir_id, child.dir_id, repr(self.path + subpath[0:1]), repr(self.children))
                subdir = DirListEntry(True, True, path=self.path + subpath[0:1], root=self.root)
                subdir.has_known_prefix = len(subpath) == 1
                self.children[subpath[0]] = subdir
            else:
                subdir = self.children[subpath[0]]
                subdir.has_known_prefix = False
                # print '%d, %d. Path was present' % (self.dir_id, child.dir_id)

            return subdir._add_child_impl(subpath[1:], child)

    def _merge_children(self, other):
        for c in other.children:
            if c not in self.children:
                self.children[c] = other.children[c]
            else:
                self.children[c].has_known_prefix = self.children[c].has_known_prefix and other.children[c].has_known_prefix
                self.children[c]._merge_children(other.children[c])


def setup_directories(instructions, default_path, random):
    dir_list = [DirListEntry(True, False, True)]
    instructions.push_args(0, '\xfe')
    instructions.append('DIRECTORY_CREATE_SUBSPACE')
    dir_list.append(DirListEntry(False, True))

    instructions.push_args(0, '')
    instructions.append('DIRECTORY_CREATE_SUBSPACE')
    dir_list.append(DirListEntry(False, True))

    instructions.push_args(1, 2, 1)
    instructions.append('DIRECTORY_CREATE_LAYER')
    dir_list.append(DirListEntry(True, False, True))

    create_default_directory_subspace(instructions, default_path, random)
    dir_list.append(DirListEntry(True, True, True))

    instructions.push_args(DEFAULT_DIRECTORY_INDEX)
    instructions.append('DIRECTORY_SET_ERROR_INDEX')

    return dir_list


def create_default_directory_subspace(instructions, path, random):
    test_util.blocking_commit(instructions)
    instructions.push_args(3)
    instructions.append('DIRECTORY_CHANGE')
    prefix = random.random_string(16)
    instructions.push_args(1, path, '', '%s-%s' % (DEFAULT_DIRECTORY_PREFIX, prefix))
    instructions.append('DIRECTORY_CREATE_DATABASE')

    instructions.push_args(DEFAULT_DIRECTORY_INDEX)
    instructions.append('DIRECTORY_CHANGE')


def push_instruction_and_record_prefix(instructions, op, op_args, path, dir_index, random, subspace):
    if not op.endswith('_DATABASE'):
        instructions.push_args(1, *test_util.with_length(path))
        instructions.append('DIRECTORY_EXISTS')

    # This op must leave the stack in the state it is in at this point, with the exception
    # that it may leave an error on the stack
    instructions.push_args(*op_args)
    instructions.append(op)

    if not op.endswith('_DATABASE'):
        instructions.push_args(dir_index)
        instructions.append('DIRECTORY_CHANGE')

        instructions.push_args(1, '', random.random_string(16), '')
        instructions.append('DIRECTORY_PACK_KEY')
        test_util.to_front(instructions, 3)  # move the existence result up to the front of the stack

        t = util.subspace_to_tuple(subspace)
        instructions.push_args(len(t) + 3, *t)

        instructions.append('TUPLE_PACK')  # subspace[<exists>][<packed_key>][random.random_string(16)] = ''
        instructions.append('SET')

        instructions.push_args(DEFAULT_DIRECTORY_INDEX)
        instructions.append('DIRECTORY_CHANGE')


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
        prefixes = [p for p in prefixes if not (p.startswith(DEFAULT_DIRECTORY_PREFIX) or p == DIRECTORY_ERROR_STRING)]
        count += len(prefixes)

        prefixes = [last_prefix] + prefixes
        duplicates.update([p for i, p in enumerate(prefixes[1:]) if p == prefixes[i]])
        last_prefix = prefixes[-1]

    util.get_logger().info('Checked %d directory prefixes for duplicates' % count)
    return ['The prefix %r was allocated multiple times' % d[:-2] for d in set(duplicates)]


def validate_hca_state(db):
    hca = fdb.Subspace(('\xfe', 'hca'), '\xfe')
    counters = hca[0]
    recent = hca[1]

    last_counter = db.get_range(counters.range().start, counters.range().stop, limit=1, reverse=True)
    [(start, reported_count)] = [(counters.unpack(kv.key)[0], struct.unpack('<q', kv.value)[0]) for kv in last_counter] or [(0, 0)]

    actual_count = len(db[recent[start]: recent.range().stop])
    if actual_count > reported_count:
        return ['The HCA reports %d prefixes allocated in current window, but it actually allocated %d' % (reported_count, actual_count)]

    return []
