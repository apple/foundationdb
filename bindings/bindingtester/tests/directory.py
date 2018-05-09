#
# directory.py
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
from bindingtester import util

from bindingtester.tests import Test, Instruction, InstructionSet, ResultSpecification
from bindingtester.tests import test_util, directory_util

from bindingtester.tests.directory_util import DirListEntry

fdb.api_version(FDB_API_VERSION)


class DirectoryTest(Test):

    def __init__(self, subspace):
        super(DirectoryTest, self).__init__(subspace)
        self.stack_subspace = subspace['stack']
        self.directory_log = subspace['directory_log']['directory']
        self.subspace_log = subspace['directory_log']['subspace']
        self.prefix_log = subspace['prefix_log']

        self.prepopulated_dirs = []
        self.next_path = 1

    def ensure_default_directory_subspace(self, instructions, path):
        directory_util.create_default_directory_subspace(instructions, path, self.random)

        child = self.root.add_child((path,), path, self.root, DirListEntry(True, True))
        self.dir_list.append(child)
        self.dir_index = directory_util.DEFAULT_DIRECTORY_INDEX

    def generate_layer(self):
        if random.random < 0.7:
            return ''
        else:
            choice = random.randint(0, 3)
            if choice == 0:
                return 'partition'
            elif choice == 1:
                return 'test_layer'
            else:
                return self.random.random_string(random.randint(0, 5))

    def setup(self, args):
        self.dir_index = 0
        self.random = test_util.RandomGenerator(args.max_int_bits, args.api_version, args.types)

    def generate(self, args, thread_number):
        instructions = InstructionSet()

        op_choices = ['NEW_TRANSACTION', 'COMMIT']

        general = ['DIRECTORY_CREATE_SUBSPACE', 'DIRECTORY_CREATE_LAYER']

        op_choices += general

        directory_mutations = ['DIRECTORY_CREATE_OR_OPEN', 'DIRECTORY_CREATE', 'DIRECTORY_MOVE', 'DIRECTORY_MOVE_TO',
                               'DIRECTORY_REMOVE', 'DIRECTORY_REMOVE_IF_EXISTS']
        directory_reads = ['DIRECTORY_EXISTS', 'DIRECTORY_OPEN', 'DIRECTORY_LIST']

        directory_db_mutations = [x + '_DATABASE' for x in directory_mutations]
        directory_db_reads = [x + '_DATABASE' for x in directory_reads]
        directory_snapshot_reads = [x + '_SNAPSHOT' for x in directory_reads]

        directory = []
        directory += directory_mutations
        directory += directory_reads
        directory += directory_db_mutations
        directory += directory_db_reads

        if not args.no_directory_snapshot_ops:
            directory += directory_snapshot_reads

        subspace = ['DIRECTORY_PACK_KEY', 'DIRECTORY_UNPACK_KEY', 'DIRECTORY_RANGE', 'DIRECTORY_CONTAINS', 'DIRECTORY_OPEN_SUBSPACE']

        instructions.append('NEW_TRANSACTION')

        default_path = unicode('default%d' % self.next_path)
        self.next_path += 1
        self.dir_list = directory_util.setup_directories(instructions, default_path, self.random)
        self.root = self.dir_list[0]

        instructions.push_args(0)
        instructions.append('DIRECTORY_CHANGE')

        # Generate some directories that we are going to create in advance. This tests that other bindings
        # are compatible with the Python implementation
        self.prepopulated_dirs = [(generate_path(min_length=1), self.generate_layer()) for i in range(5)]

        for path, layer in self.prepopulated_dirs:
            instructions.push_args(layer)
            instructions.push_args(*test_util.with_length(path))
            instructions.append('DIRECTORY_OPEN')
            # print '%d. Selected %s, dir=%s, has_known_prefix=%s, dir_list_len=%d' \
            #        % (len(instructions), 'DIRECTORY_OPEN', repr(self.dir_index), False, len(self.dir_list))
            self.dir_list.append(self.dir_list[0].add_child(path, default_path, self.root, DirListEntry(True, True, has_known_prefix=False)))

        instructions.setup_complete()

        for i in range(args.num_ops):
            if random.random() < 0.5:
                self.dir_index = random.randrange(0, len(self.dir_list))
                instructions.push_args(self.dir_index)
                instructions.append('DIRECTORY_CHANGE')

            choices = op_choices[:]
            if self.dir_list[self.dir_index].is_directory:
                choices += directory
            if self.dir_list[self.dir_index].is_subspace:
                choices += subspace

            op = random.choice(choices)
            dir_entry = self.dir_list[self.dir_index]

            # print '%d. Selected %s, dir=%s, has_known_prefix=%s, dir_list_len=%d' \
            #        % (len(instructions), op, repr(self.dir_index), repr(dir_entry.has_known_prefix), len(self.dir_list))

            if op.endswith('_DATABASE') or op.endswith('_SNAPSHOT'):
                root_op = op[0:-9]
            else:
                root_op = op

            if root_op == 'NEW_TRANSACTION':
                instructions.append(op)

            elif root_op == 'COMMIT':
                test_util.blocking_commit(instructions)

            elif root_op == 'DIRECTORY_CREATE_SUBSPACE':
                path = generate_path()
                instructions.push_args(generate_prefix(allow_empty=False, is_partition=True))
                instructions.push_args(*test_util.with_length(path))
                instructions.append(op)
                self.dir_list.append(DirListEntry(False, True))

            elif root_op == 'DIRECTORY_CREATE_LAYER':
                indices = []
                for i in range(2):
                    instructions.push_args(generate_prefix(allow_empty=False, is_partition=True))
                    instructions.push_args(*test_util.with_length(generate_path()))
                    instructions.append('DIRECTORY_CREATE_SUBSPACE')
                    indices.append(len(self.dir_list))
                    self.dir_list.append(DirListEntry(False, True))

                instructions.push_args(random.choice([0, 1]))
                instructions.push_args(*indices)
                instructions.append(op)
                self.dir_list.append(DirListEntry(True, False, False))

            elif root_op == 'DIRECTORY_CREATE_OR_OPEN':
                # Because allocated prefixes are non-deterministic, we cannot have overlapping
                # transactions that allocate/remove these prefixes in a comparison test
                if op.endswith('_DATABASE') and args.concurrency == 1:
                    test_util.blocking_commit(instructions)

                path = generate_path()
                op_args = test_util.with_length(path) + (self.generate_layer(),)
                directory_util.push_instruction_and_record_prefix(instructions, op, op_args, path, len(self.dir_list), self.random, self.prefix_log)

                if not op.endswith('_DATABASE') and args.concurrency == 1:
                    test_util.blocking_commit(instructions)

                self.dir_list.append(dir_entry.add_child(path, default_path, self.root, DirListEntry(True, True, False)))

            elif root_op == 'DIRECTORY_CREATE':
                layer = self.generate_layer()
                is_partition = layer == 'partition'

                allow_empty_prefix = random.random() < 0.8
                prefix = generate_prefix(allow_empty=allow_empty_prefix, is_partition=is_partition)

                # Because allocated prefixes are non-deterministic, we cannot have overlapping
                # transactions that allocate/remove these prefixes in a comparison test
                if op.endswith('_DATABASE') and args.concurrency == 1:  # and allow_empty_prefix:
                    test_util.blocking_commit(instructions)

                path = generate_path()
                op_args = test_util.with_length(path) + (layer, prefix)
                if prefix is None:
                    directory_util.push_instruction_and_record_prefix(
                        instructions, op, op_args, path, len(self.dir_list), self.random, self.prefix_log)
                else:
                    instructions.push_args(*op_args)
                    instructions.append(op)

                if not op.endswith('_DATABASE') and args.concurrency == 1:  # and allow_empty_prefix:
                    test_util.blocking_commit(instructions)

                self.dir_list.append(dir_entry.add_child(path, default_path, self.root, DirListEntry(True, True, bool(prefix))))

            elif root_op == 'DIRECTORY_OPEN':
                path = generate_path()
                instructions.push_args(self.generate_layer())
                instructions.push_args(*test_util.with_length(path))
                instructions.append(op)
                self.dir_list.append(dir_entry.add_child(path, default_path, self.root, DirListEntry(True, True)))

            elif root_op == 'DIRECTORY_MOVE':
                old_path = generate_path()
                new_path = generate_path()
                instructions.push_args(*(test_util.with_length(old_path) + test_util.with_length(new_path)))
                instructions.append(op)
                # This could probably be made to sometimes set has_known_prefix to true
                self.dir_list.append(dir_entry.add_child(new_path, default_path, self.root, DirListEntry(True, True, False)))

                # Make sure that the default directory subspace still exists after moving the specified directory
                if dir_entry.is_directory and not dir_entry.is_subspace and old_path == (u'',):
                    self.ensure_default_directory_subspace(instructions, default_path)

            elif root_op == 'DIRECTORY_MOVE_TO':
                new_path = generate_path()
                instructions.push_args(*test_util.with_length(new_path))
                instructions.append(op)
                self.dir_list.append(dir_entry.root.add_child(new_path, default_path, self.root,
                                                              DirListEntry(True, True, dir_entry.has_known_prefix)))

                # Make sure that the default directory subspace still exists after moving the current directory
                self.ensure_default_directory_subspace(instructions, default_path)

            # FIXME: There is currently a problem with removing partitions. In these generated tests, it's possible
            # for a removed partition to resurrect itself and insert keys into the database using its allocated
            # prefix. The result is non-deterministic HCA errors.
            elif root_op == 'DIRECTORY_REMOVE' or root_op == 'DIRECTORY_REMOVE_IF_EXISTS':
                # Because allocated prefixes are non-deterministic, we cannot have overlapping
                # transactions that allocate/remove these prefixes in a comparison test
                if op.endswith('_DATABASE') and args.concurrency == 1:
                    test_util.blocking_commit(instructions)

                path = ()
                count = random.randint(0, 1)
                if count == 1:
                    path = generate_path()
                    instructions.push_args(*test_util.with_length(path))
                instructions.push_args(count)

                instructions.append(op)

                # Make sure that the default directory subspace still exists after removing the specified directory
                if path == () or (dir_entry.is_directory and not dir_entry.is_subspace and path == (u'',)):
                    self.ensure_default_directory_subspace(instructions, default_path)

            elif root_op == 'DIRECTORY_LIST' or root_op == 'DIRECTORY_EXISTS':
                path = ()
                count = random.randint(0, 1)
                if count == 1:
                    path = generate_path()
                    instructions.push_args(*test_util.with_length(path))
                instructions.push_args(count)
                instructions.append(op)

            elif root_op == 'DIRECTORY_PACK_KEY':
                t = self.random.random_tuple(5)
                instructions.push_args(*test_util.with_length(t))
                instructions.append(op)
                instructions.append('DIRECTORY_STRIP_PREFIX')

            elif root_op == 'DIRECTORY_UNPACK_KEY' or root_op == 'DIRECTORY_CONTAINS':
                if not dir_entry.has_known_prefix or random.random() < 0.2 or root_op == 'DIRECTORY_UNPACK_KEY':
                    t = self.random.random_tuple(5)
                    instructions.push_args(*test_util.with_length(t))
                    instructions.append('DIRECTORY_PACK_KEY')
                    instructions.append(op)
                else:
                    instructions.push_args(fdb.tuple.pack(self.random.random_tuple(5)))
                    instructions.append(op)

            elif root_op == 'DIRECTORY_RANGE' or root_op == 'DIRECTORY_OPEN_SUBSPACE':
                t = self.random.random_tuple(5)
                instructions.push_args(*test_util.with_length(t))
                instructions.append(op)
                if root_op == 'DIRECTORY_OPEN_SUBSPACE':
                    self.dir_list.append(DirListEntry(False, True, dir_entry.has_known_prefix))
                else:
                    test_util.to_front(instructions, 1)
                    instructions.append('DIRECTORY_STRIP_PREFIX')
                    test_util.to_front(instructions, 1)
                    instructions.append('DIRECTORY_STRIP_PREFIX')

        instructions.begin_finalization()

        test_util.blocking_commit(instructions)

        instructions.append('NEW_TRANSACTION')

        for i, dir_entry in enumerate(self.dir_list):
            instructions.push_args(i)
            instructions.append('DIRECTORY_CHANGE')
            if dir_entry.is_directory:
                instructions.push_args(self.directory_log.key())
                instructions.append('DIRECTORY_LOG_DIRECTORY')
            if dir_entry.has_known_prefix and dir_entry.is_subspace:
                # print '%d. Logging subspace: %d' % (i, dir_entry.dir_id)
                instructions.push_args(self.subspace_log.key())
                instructions.append('DIRECTORY_LOG_SUBSPACE')
            if (i + 1) % 100 == 0:
                test_util.blocking_commit(instructions)

        instructions.push_args(self.stack_subspace.key())
        instructions.append('LOG_STACK')

        test_util.blocking_commit(instructions)
        return instructions

    def pre_run(self, db, args):
        for (path, layer) in self.prepopulated_dirs:
            try:
                util.get_logger().debug('Prepopulating directory: %r (layer=%r)' % (path, layer))
                fdb.directory.create_or_open(db, path, layer)
            except Exception as e:
                util.get_logger().debug('Could not create directory %r: %r' % (path, e))
                pass

    def validate(self, db, args):
        errors = []
        # This check doesn't work in the current test because of the way we use partitions.
        # If a partition is created, allocates a prefix, and then is removed, subsequent prefix
        # allocations could collide with prior ones. We can get around this by not allowing
        # a removed directory (or partition) to be used, but that weakens the test in another way.
        # errors += directory_util.check_for_duplicate_prefixes(db, self.prefix_log)
        return errors

    def get_result_specifications(self):
        return [
            ResultSpecification(self.stack_subspace, key_start_index=1, ordering_index=1),
            ResultSpecification(self.directory_log, ordering_index=0),
            ResultSpecification(self.subspace_log, ordering_index=0)
        ]


# Utility functions


def generate_path(min_length=0):
    length = int(random.random() * random.random() * (4 - min_length)) + min_length
    path = ()
    for i in range(length):
        if random.random() < 0.05:
            path = path + (u'',)
        else:
            path = path + (random.choice([u'1', u'2', u'3']),)

    return path


def generate_prefix(allow_empty=True, is_partition=False):
    if allow_empty and random.random() < 0.8:
        return None
    elif is_partition or random.random() < 0.5:
        length = random.randint(0 if allow_empty else 1, 5)
        if length == 0:
            return ''

        if not is_partition:
            first = chr(random.randint(ord('\x1d'), 255) % 255)
            return first + ''.join(chr(random.randrange(0, 256)) for i in range(0, length - 1))
        else:
            return ''.join(chr(random.randrange(ord('\x02'), ord('\x14'))) for i in range(0, length))
    else:
        prefix = 'abcdefg'
        generated = prefix[0:random.randrange(0 if allow_empty else 1, len(prefix))]
        return generated
