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

# Represents an element of the directory hierarchy, which could have multiple states 
class DirListEntry:
    # A cache of directory layers. We mustn't have multiple entries for the same layer
    layers = {}

    # This is the path of the directory that gets used if the chosen directory is invalid
    # We must assume any operation could also have been applied to it
    default_path = None

    # Used for debugging
    dir_id = 0  

    @classmethod
    def set_default_path(cls, default_path):
        cls.default_path = default_path

    @classmethod
    def get_layer(cls, node_subspace_prefix):
        if node_subspace_prefix not in DirListEntry.layers:
            DirListEntry.layers[node_subspace_prefix] = DirListEntry(True, False, has_known_prefix=False)

        return DirListEntry.layers[node_subspace_prefix]

    def __init__(self, is_directory, is_subspace, has_known_prefix=True, root=None, is_partition=False):
        self.root = root or self
        self.is_directory = is_directory
        self.is_subspace = is_subspace
        self.has_known_prefix = has_known_prefix
        self.children = {}
        self.deleted = False
        self.is_partition = is_partition

        self.dir_id = DirListEntry.dir_id + 1
        DirListEntry.dir_id += 1

    def __repr__(self):
        return '{DirEntry %d: %d}' % (self.dir_id, self.has_known_prefix)

    def _get_descendent(self, subpath, default):
        if not subpath:
            if default is not None:
                self._merge(default)
            return self

        default_child = None
        if default is not None:
            default_child = default.children.get(subpath[0])

        self_child = self.children.get(subpath[0]) 

        if self_child is None:
            if default_child is None:
                return None
            else:
                return default_child._get_descendent(subpath[1:], None)

        return self_child._get_descendent(subpath[1:], default_child)

    def get_descendent(self, subpath):
        return self._get_descendent(subpath, self.root.children.get(DirListEntry.default_path))

    def add_child(self, subpath, child):
        child.root = self.root
        if DirListEntry.default_path and DirListEntry.default_path in self.root.children:
            # print('Adding child %r to default directory at %r' % (child, subpath))
            child = self.root.children[DirListEntry.default_path]._add_child_impl(subpath, child)
            # print('Added %r' % child)

        # print('Adding child %r to directory at %r' % (child, subpath))
        c = self._add_child_impl(subpath, child)
        # print('Added %r' % c)
        return c

    def _add_child_impl(self, subpath, child):
        # print('%d, %d. Adding child (recursive): %r' % (self.dir_id, child.dir_id, subpath))
        if len(subpath) == 0:
            # print('%d, %d. Setting child: %d, %d' % (self.dir_id, child.dir_id, self.has_known_prefix, child.has_known_prefix))
            self._merge(child)
            return self
        else:
            if not subpath[0] in self.children:
                # print('%d, %d. Path %r was absent from %r (%r)' % (self.dir_id, child.dir_id, subpath[0:1], self, self.children))
                subdir = DirListEntry(True, True, root=self.root)
                subdir.has_known_prefix = len(subpath) == 1 # For the last element in the path, the merge will take care of has_known_prefix
                self.children[subpath[0]] = subdir
            else:
                subdir = self.children[subpath[0]]
                subdir.has_known_prefix = False # The directory may have had to be recreated with an unknown prefix
                # print('%d, %d. Path was present' % (self.dir_id, child.dir_id))

            return subdir._add_child_impl(subpath[1:], child)

    def _merge(self, other):
        if self == other:
            return

        self.is_directory = self.is_directory and other.is_directory
        self.is_subspace = self.is_subspace and other.is_subspace
        self.has_known_prefix = self.has_known_prefix and other.has_known_prefix
        self.deleted = self.deleted or other.deleted
        self.is_partition = self.is_partition or other.is_partition

        other.root = self.root
        other.is_directory = self.is_directory
        other.is_subspace = self.is_subspace
        other.has_known_prefix = self.has_known_prefix
        other.children = self.children 
        other.deleted = self.deleted
        other.is_partition = self.is_partition
        other.dir_id = self.dir_id

        other_children = other.children.copy()
        for c in other_children:
            if c not in self.children:
                self.children[c] = other_children[c]

        other.children = self.children

        for c in other_children:
            self.children[c]._merge(other_children[c])

    def _delete_impl(self):
        if not self.deleted:
            self.deleted = True
            for c in self.children.values():
                c._delete_impl()

    def delete(self, path):
        child = self.get_descendent(path)
        if child:
            child._delete_impl()
    
def setup_directories(instructions, default_path, random):
    # Clients start with the default directory layer in the directory list
    dir_list = [DirListEntry.get_layer('\xfe')]

    instructions.push_args(0, '\xfe')
    instructions.append('DIRECTORY_CREATE_SUBSPACE')
    dir_list.append(DirListEntry(False, True))

    instructions.push_args(0, '')
    instructions.append('DIRECTORY_CREATE_SUBSPACE')
    dir_list.append(DirListEntry(False, True))

    instructions.push_args(1, 2, 1)
    instructions.append('DIRECTORY_CREATE_LAYER')
    dir_list.append(DirListEntry.get_layer('\xfe'))

    DirListEntry.set_default_path(default_path)
    create_default_directory_subspace(instructions, default_path, random)
    dir_list.append(dir_list[0].add_child((default_path,), DirListEntry(True, True, has_known_prefix=True)))

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
