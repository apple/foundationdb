#
# api.py
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
import fdb.tuple

from bindingtester import FDB_API_VERSION
from bindingtester import util
from bindingtester.tests import Test, Instruction, InstructionSet, ResultSpecification
from bindingtester.tests import test_util

fdb.api_version(FDB_API_VERSION)


class ApiTest(Test):
    def __init__(self, subspace):
        super(ApiTest, self).__init__(subspace)
        self.workspace = self.subspace['workspace']  # The keys and values here must match between subsequent runs of the same test
        self.scratch = self.subspace['scratch']  # The keys and values here can differ between runs
        self.stack_subspace = self.subspace['stack']

        self.versionstamped_values = self.scratch['versionstamped_values']
        self.versionstamped_values_2 = self.scratch['versionstamped_values_2']
        self.versionstamped_keys = self.scratch['versionstamped_keys']

    def setup(self, args):
        self.stack_size = 0
        self.string_depth = 0
        self.key_depth = 0
        self.max_keys = 1000

        self.has_version = False
        self.can_set_version = True
        self.can_get_commit_version = False
        self.can_use_key_selectors = True

        self.generated_keys = []
        self.outstanding_ops = []
        self.random = test_util.RandomGenerator(args.max_int_bits, args.api_version, args.types)
        self.api_version = args.api_version
        self.allocated_tenants = set()

    def add_stack_items(self, num):
        self.stack_size += num
        self.string_depth = 0
        self.key_depth = 0

    def add_strings(self, num):
        self.stack_size += num
        self.string_depth += num
        self.key_depth = 0

    def add_keys(self, num):
        self.stack_size += num
        self.string_depth += num
        self.key_depth += num

    def remove(self, num):
        self.stack_size -= num
        self.string_depth = max(0, self.string_depth - num)
        self.key_depth = max(0, self.key_depth - num)

        self.outstanding_ops = [i for i in self.outstanding_ops if i[0] <= self.stack_size]

    def ensure_string(self, instructions, num):
        while self.string_depth < num:
            instructions.push_args(self.random.random_string(random.randint(0, 100)))
            self.add_strings(1)

        self.remove(num)

    def choose_key(self):
        if random.random() < float(len(self.generated_keys)) / self.max_keys:
            tup = random.choice(self.generated_keys)
            if random.random() < 0.3:
                return self.workspace.pack(tup[0:random.randint(0, len(tup))])

            return self.workspace.pack(tup)

        tup = self.random.random_tuple(5)
        self.generated_keys.append(tup)

        return self.workspace.pack(tup)

    def ensure_key(self, instructions, num):
        while self.key_depth < num:
            instructions.push_args(self.choose_key())
            self.add_keys(1)

        self.remove(num)

    def ensure_key_value(self, instructions):
        if self.string_depth == 0:
            instructions.push_args(self.choose_key(), self.random.random_string(random.randint(0, 100)))

        elif self.string_depth == 1 or self.key_depth == 0:
            self.ensure_key(instructions, 1)
            self.remove(1)

        else:
            self.remove(2)

    def preload_database(self, instructions, num):
        for i in range(num):
            self.ensure_key_value(instructions)
            instructions.append('SET')

            if i % 100 == 99:
                test_util.blocking_commit(instructions)

        test_util.blocking_commit(instructions)
        self.add_stack_items(1)

    def wait_for_reads(self, instructions):
        while len(self.outstanding_ops) > 0 and self.outstanding_ops[-1][0] <= self.stack_size:
            read = self.outstanding_ops.pop()
            # print '%d. waiting for read at instruction %r' % (len(instructions), read)
            test_util.to_front(instructions, self.stack_size - read[0])
            instructions.append('WAIT_FUTURE')

    def choose_tenant(self, new_tenant_probability):
        if len(self.allocated_tenants) == 0 or random.random() < new_tenant_probability:
            return self.random.random_string(random.randint(0, 30))
        else:
            return random.choice(list(self.allocated_tenants))

    def generate(self, args, thread_number):
        instructions = InstructionSet()

        op_choices = ['NEW_TRANSACTION', 'COMMIT']

        reads = ['GET', 'GET_KEY', 'GET_RANGE', 'GET_RANGE_STARTS_WITH', 'GET_RANGE_SELECTOR']
        mutations = ['SET', 'CLEAR', 'CLEAR_RANGE', 'CLEAR_RANGE_STARTS_WITH', 'ATOMIC_OP']
        snapshot_reads = [x + '_SNAPSHOT' for x in reads]
        database_reads = [x + '_DATABASE' for x in reads]
        database_mutations = [x + '_DATABASE' for x in mutations]
        mutations += ['VERSIONSTAMP']
        versions = ['GET_READ_VERSION', 'SET_READ_VERSION', 'GET_COMMITTED_VERSION']
        snapshot_versions = ['GET_READ_VERSION_SNAPSHOT']
        tuples = ['TUPLE_PACK', 'TUPLE_UNPACK', 'TUPLE_RANGE', 'TUPLE_SORT', 'SUB', 'ENCODE_FLOAT', 'ENCODE_DOUBLE', 'DECODE_DOUBLE', 'DECODE_FLOAT']
        if 'versionstamp' in args.types:
            tuples.append('TUPLE_PACK_WITH_VERSIONSTAMP')
        resets = ['ON_ERROR', 'RESET', 'CANCEL']
        read_conflicts = ['READ_CONFLICT_RANGE', 'READ_CONFLICT_KEY']
        write_conflicts = ['WRITE_CONFLICT_RANGE', 'WRITE_CONFLICT_KEY', 'DISABLE_WRITE_CONFLICT']
        txn_sizes = ['GET_APPROXIMATE_SIZE']
        storage_metrics = ['GET_ESTIMATED_RANGE_SIZE', 'GET_RANGE_SPLIT_POINTS']
        tenants = ['TENANT_CREATE', 'TENANT_DELETE', 'TENANT_SET_ACTIVE', 'TENANT_CLEAR_ACTIVE']

        op_choices += reads
        op_choices += mutations
        op_choices += snapshot_reads
        op_choices += database_reads
        op_choices += database_mutations
        op_choices += versions
        op_choices += snapshot_versions
        op_choices += tuples
        op_choices += read_conflicts
        op_choices += write_conflicts
        op_choices += resets
        op_choices += txn_sizes
        op_choices += storage_metrics

        if not args.no_tenants:
            op_choices += tenants

        idempotent_atomic_ops = ['BIT_AND', 'BIT_OR', 'MAX', 'MIN', 'BYTE_MIN', 'BYTE_MAX']
        atomic_ops = idempotent_atomic_ops + ['ADD', 'BIT_XOR', 'APPEND_IF_FITS']

        if args.concurrency > 1:
            self.max_keys = random.randint(100, 1000)
        else:
            self.max_keys = random.randint(100, 10000)

        instructions.append('NEW_TRANSACTION')
        instructions.append('GET_READ_VERSION')

        self.preload_database(instructions, self.max_keys)

        instructions.setup_complete()

        for i in range(args.num_ops):
            op = random.choice(op_choices)
            index = len(instructions)
            read_performed = False

            # print 'Adding instruction %s at %d' % (op, index)

            if args.concurrency == 1 and (op in database_mutations or op in ['TENANT_CREATE', 'TENANT_DELETE']):
                self.wait_for_reads(instructions)
                test_util.blocking_commit(instructions)
                self.can_get_commit_version = False
                self.add_stack_items(1)

            if op in resets or op == 'NEW_TRANSACTION':
                if args.concurrency == 1:
                    self.wait_for_reads(instructions)

                self.outstanding_ops = []

            if op == 'NEW_TRANSACTION':
                instructions.append(op)
                self.can_get_commit_version = True
                self.can_set_version = True
                self.can_use_key_selectors = True

            elif op == 'ON_ERROR':
                instructions.push_args(random.randint(0, 5000))
                instructions.append(op)

                self.outstanding_ops.append((self.stack_size, len(instructions) - 1))
                if args.concurrency == 1:
                    self.wait_for_reads(instructions)

                instructions.append('NEW_TRANSACTION')
                self.can_get_commit_version = True
                self.can_set_version = True
                self.can_use_key_selectors = True
                self.add_strings(1)

            elif op == 'GET' or op == 'GET_SNAPSHOT' or op == 'GET_DATABASE':
                self.ensure_key(instructions, 1)
                instructions.append(op)
                self.add_strings(1)
                self.can_set_version = False
                read_performed = True

            elif op == 'GET_KEY' or op == 'GET_KEY_SNAPSHOT' or op == 'GET_KEY_DATABASE':
                if op.endswith('_DATABASE') or self.can_use_key_selectors:
                    self.ensure_key(instructions, 1)
                    instructions.push_args(self.workspace.key())
                    instructions.push_args(*self.random.random_selector_params())
                    test_util.to_front(instructions, 3)
                    instructions.append(op)

                    # Don't add key here because we may be outside of our prefix
                    self.add_strings(1)
                    self.can_set_version = False
                    read_performed = True

            elif op == 'GET_RANGE' or op == 'GET_RANGE_SNAPSHOT' or op == 'GET_RANGE_DATABASE':
                self.ensure_key(instructions, 2)
                range_params = self.random.random_range_params()
                instructions.push_args(*range_params)
                test_util.to_front(instructions, 4)
                test_util.to_front(instructions, 4)
                instructions.append(op)

                if range_params[0] >= 1 and range_params[0] <= 1000:  # avoid adding a string if the limit is large
                    self.add_strings(1)
                else:
                    self.add_stack_items(1)

                self.can_set_version = False
                read_performed = True

            elif op == 'GET_RANGE_STARTS_WITH' or op == 'GET_RANGE_STARTS_WITH_SNAPSHOT' or op == 'GET_RANGE_STARTS_WITH_DATABASE':
                # TODO: not tested well
                self.ensure_key(instructions, 1)
                range_params = self.random.random_range_params()
                instructions.push_args(*range_params)
                test_util.to_front(instructions, 3)
                instructions.append(op)

                if range_params[0] >= 1 and range_params[0] <= 1000:  # avoid adding a string if the limit is large
                    self.add_strings(1)
                else:
                    self.add_stack_items(1)

                self.can_set_version = False
                read_performed = True

            elif op == 'GET_RANGE_SELECTOR' or op == 'GET_RANGE_SELECTOR_SNAPSHOT' or op == 'GET_RANGE_SELECTOR_DATABASE':
                if op.endswith('_DATABASE') or self.can_use_key_selectors:
                    self.ensure_key(instructions, 2)
                    instructions.push_args(self.workspace.key())
                    range_params = self.random.random_range_params()
                    instructions.push_args(*range_params)
                    instructions.push_args(*self.random.random_selector_params())
                    test_util.to_front(instructions, 6)
                    instructions.push_args(*self.random.random_selector_params())
                    test_util.to_front(instructions, 9)
                    instructions.append(op)

                    if range_params[0] >= 1 and range_params[0] <= 1000:  # avoid adding a string if the limit is large
                        self.add_strings(1)
                    else:
                        self.add_stack_items(1)

                    self.can_set_version = False
                    read_performed = True

            elif op == 'GET_READ_VERSION' or op == 'GET_READ_VERSION_SNAPSHOT':
                instructions.append(op)
                self.has_version = self.can_set_version
                self.add_strings(1)

            elif op == 'SET' or op == 'SET_DATABASE':
                self.ensure_key_value(instructions)
                instructions.append(op)
                if op == 'SET_DATABASE':
                    self.add_stack_items(1)

            elif op == 'SET_READ_VERSION':
                if self.has_version and self.can_set_version:
                    instructions.append(op)
                    self.can_set_version = False

            elif op == 'CLEAR' or op == 'CLEAR_DATABASE':
                self.ensure_key(instructions, 1)
                instructions.append(op)
                if op == 'CLEAR_DATABASE':
                    self.add_stack_items(1)

            elif op == 'CLEAR_RANGE' or op == 'CLEAR_RANGE_DATABASE':
                # Protect against inverted range
                key1 = self.workspace.pack(self.random.random_tuple(5))
                key2 = self.workspace.pack(self.random.random_tuple(5))

                if key1 > key2:
                    key1, key2 = key2, key1

                instructions.push_args(key1, key2)

                instructions.append(op)
                if op == 'CLEAR_RANGE_DATABASE':
                    self.add_stack_items(1)

            elif op == 'CLEAR_RANGE_STARTS_WITH' or op == 'CLEAR_RANGE_STARTS_WITH_DATABASE':
                self.ensure_key(instructions, 1)
                instructions.append(op)
                if op == 'CLEAR_RANGE_STARTS_WITH_DATABASE':
                    self.add_stack_items(1)

            elif op == 'ATOMIC_OP' or op == 'ATOMIC_OP_DATABASE':
                self.ensure_key_value(instructions)
                if op == 'ATOMIC_OP' or args.concurrency > 1:
                    instructions.push_args(random.choice(atomic_ops))
                else:
                    instructions.push_args(random.choice(idempotent_atomic_ops))

                instructions.append(op)
                if op == 'ATOMIC_OP_DATABASE':
                    self.add_stack_items(1)

            elif op == 'VERSIONSTAMP':
                rand_str1 = self.random.random_string(100)
                key1 = self.versionstamped_values.pack((rand_str1,))
                key2 = self.versionstamped_values_2.pack((rand_str1,))

                split = random.randint(0, 70)
                prefix = self.random.random_string(20 + split)
                if prefix.endswith(b'\xff'):
                    # Necessary to make sure that the SET_VERSIONSTAMPED_VALUE check
                    # correctly finds where the version is supposed to fit in.
                    prefix += b'\x00'
                suffix = self.random.random_string(70 - split)
                rand_str2 = prefix + fdb.tuple.Versionstamp._UNSET_TR_VERSION + suffix
                key3 = self.versionstamped_keys.pack() + rand_str2
                index = len(self.versionstamped_keys.pack()) + len(prefix)
                key3 = self.versionstamp_key(key3, index)

                instructions.push_args('SET_VERSIONSTAMPED_VALUE',
                                       key1,
                                       self.versionstamp_value(fdb.tuple.Versionstamp._UNSET_TR_VERSION + rand_str2))
                instructions.append('ATOMIC_OP')

                if args.api_version >= 520:
                    instructions.push_args('SET_VERSIONSTAMPED_VALUE', key2, self.versionstamp_value(rand_str2, len(prefix)))
                    instructions.append('ATOMIC_OP')

                instructions.push_args('SET_VERSIONSTAMPED_KEY', key3, rand_str1)
                instructions.append('ATOMIC_OP')
                self.can_use_key_selectors = False

            elif op == 'READ_CONFLICT_RANGE' or op == 'WRITE_CONFLICT_RANGE':
                self.ensure_key(instructions, 2)
                instructions.append(op)
                self.add_strings(1)

            elif op == 'READ_CONFLICT_KEY' or op == 'WRITE_CONFLICT_KEY':
                self.ensure_key(instructions, 1)
                instructions.append(op)
                self.add_strings(1)

            elif op == 'DISABLE_WRITE_CONFLICT':
                instructions.append(op)

            elif op == 'COMMIT':
                if args.concurrency == 1 or i < self.max_keys or random.random() < 0.9:
                    if args.concurrency == 1:
                        self.wait_for_reads(instructions)
                    test_util.blocking_commit(instructions)
                    self.can_get_commit_version = False
                    self.add_stack_items(1)
                    self.can_set_version = True
                    self.can_use_key_selectors = True
                else:
                    instructions.append(op)
                    self.add_strings(1)

            elif op == 'RESET':
                instructions.append(op)
                self.can_get_commit_version = False
                self.can_set_version = True
                self.can_use_key_selectors = True

            elif op == 'CANCEL':
                instructions.append(op)
                self.can_set_version = False

            elif op == 'GET_COMMITTED_VERSION':
                if self.can_get_commit_version:
                    do_commit = random.random() < 0.5

                    if do_commit:
                        instructions.append('COMMIT')
                        instructions.append('WAIT_FUTURE')
                        self.add_stack_items(1)

                    instructions.append(op)

                    self.has_version = True
                    self.add_strings(1)

                    if do_commit:
                        instructions.append('RESET')
                        self.can_get_commit_version = False
                        self.can_set_version = True
                        self.can_use_key_selectors = True

            elif op == 'GET_APPROXIMATE_SIZE':
                instructions.append(op)
                self.add_strings(1)

            elif op == 'TUPLE_PACK' or op == 'TUPLE_RANGE':
                tup = self.random.random_tuple(10)
                instructions.push_args(len(tup), *tup)
                instructions.append(op)
                if op == 'TUPLE_PACK':
                    self.add_strings(1)
                else:
                    self.add_strings(2)

            elif op == 'TUPLE_PACK_WITH_VERSIONSTAMP':
                tup = (self.random.random_string(20),) + self.random.random_tuple(10, incomplete_versionstamps=True)
                prefix = self.versionstamped_keys.pack()
                instructions.push_args(prefix, len(tup), *tup)
                instructions.append(op)
                self.add_strings(1)

                versionstamp_param = prefix + fdb.tuple.pack(tup)
                first_incomplete = versionstamp_param.find(fdb.tuple.Versionstamp._UNSET_TR_VERSION)
                second_incomplete = -1 if first_incomplete < 0 else \
                    versionstamp_param.find(fdb.tuple.Versionstamp._UNSET_TR_VERSION, first_incomplete + len(fdb.tuple.Versionstamp._UNSET_TR_VERSION) + 1)

                # If there is exactly one incomplete versionstamp, perform the versionstamp operation.
                if first_incomplete >= 0 and second_incomplete < 0:
                    rand_str = self.random.random_string(100)

                    instructions.push_args(rand_str)
                    test_util.to_front(instructions, 1)
                    instructions.push_args('SET_VERSIONSTAMPED_KEY')
                    instructions.append('ATOMIC_OP')

                    if self.api_version >= 520:
                        version_value_key_2 = self.versionstamped_values_2.pack((rand_str,))
                        versionstamped_value = self.versionstamp_value(fdb.tuple.pack(tup), first_incomplete - len(prefix))
                        instructions.push_args('SET_VERSIONSTAMPED_VALUE', version_value_key_2, versionstamped_value)
                        instructions.append('ATOMIC_OP')

                    version_value_key = self.versionstamped_values.pack((rand_str,))
                    instructions.push_args('SET_VERSIONSTAMPED_VALUE', version_value_key,
                                           self.versionstamp_value(fdb.tuple.Versionstamp._UNSET_TR_VERSION + fdb.tuple.pack(tup)))
                    instructions.append('ATOMIC_OP')
                    self.can_use_key_selectors = False

            elif op == 'TUPLE_UNPACK':
                tup = self.random.random_tuple(10)
                instructions.push_args(len(tup), *tup)
                instructions.append('TUPLE_PACK')
                instructions.append(op)
                self.add_strings(len(tup))

            elif op == 'TUPLE_SORT':
                tups = self.random.random_tuple_list(10, 30)
                for tup in tups:
                    instructions.push_args(len(tup), *tup)
                    instructions.append('TUPLE_PACK')
                instructions.push_args(len(tups))
                instructions.append(op)
                self.add_strings(len(tups))

            # Use SUB to test if integers are correctly unpacked
            elif op == 'SUB':
                a = self.random.random_int() // 2
                b = self.random.random_int() // 2
                instructions.push_args(0, a, b)
                instructions.append(op)
                instructions.push_args(1)
                instructions.append('SWAP')
                instructions.append(op)
                instructions.push_args(1)
                instructions.append('TUPLE_PACK')
                self.add_stack_items(1)

            elif op == 'ENCODE_FLOAT':
                f = self.random.random_float(8)
                f_bytes = struct.pack('>f', f)
                instructions.push_args(f_bytes)
                instructions.append(op)
                self.add_stack_items(1)

            elif op == 'ENCODE_DOUBLE':
                d = self.random.random_float(11)
                d_bytes = struct.pack('>d', d)
                instructions.push_args(d_bytes)
                instructions.append(op)
                self.add_stack_items(1)

            elif op == 'DECODE_FLOAT':
                f = self.random.random_float(8)
                instructions.push_args(fdb.tuple.SingleFloat(f))
                instructions.append(op)
                self.add_strings(1)

            elif op == 'DECODE_DOUBLE':
                d = self.random.random_float(11)
                instructions.push_args(d)
                instructions.append(op)
                self.add_strings(1)
            elif op == 'GET_ESTIMATED_RANGE_SIZE':
                # Protect against inverted range and identical keys
                key1 = self.workspace.pack(self.random.random_tuple(1))
                key2 = self.workspace.pack(self.random.random_tuple(1))

                while key1 == key2:
                    key1 = self.workspace.pack(self.random.random_tuple(1))
                    key2 = self.workspace.pack(self.random.random_tuple(1))

                if key1 > key2:
                    key1, key2 = key2, key1

                instructions.push_args(key1, key2)
                instructions.append(op)
                self.add_strings(1)
            elif op == 'GET_RANGE_SPLIT_POINTS':
                # Protect against inverted range and identical keys
                key1 = self.workspace.pack(self.random.random_tuple(1))
                key2 = self.workspace.pack(self.random.random_tuple(1))

                while key1 == key2:
                    key1 = self.workspace.pack(self.random.random_tuple(1))
                    key2 = self.workspace.pack(self.random.random_tuple(1))

                if key1 > key2:
                    key1, key2 = key2, key1

                # TODO: randomize chunkSize but should not exceed 100M(shard limit)
                chunkSize = 10000000 # 10M
                instructions.push_args(key1, key2, chunkSize)
                instructions.append(op)
                self.add_strings(1)
            elif op == 'TENANT_CREATE':
                tenant_name = self.choose_tenant(0.8)
                self.allocated_tenants.add(tenant_name)
                instructions.push_args(tenant_name)
                instructions.append(op)
                self.add_strings(1)
            elif op == 'TENANT_DELETE':
                tenant_name = self.choose_tenant(0.2)
                if tenant_name in self.allocated_tenants:
                    self.allocated_tenants.remove(tenant_name)
                instructions.push_args(tenant_name)
                instructions.append(op)
                self.add_strings(1)
            elif op == 'TENANT_SET_ACTIVE':
                tenant_name = self.choose_tenant(0.8)
                instructions.push_args(tenant_name)
                instructions.append(op)
            elif op == 'TENANT_CLEAR_ACTIVE':
                instructions.append(op)
            else:
                assert False, 'Unknown operation: ' + op

            if read_performed and op not in database_reads:
                self.outstanding_ops.append((self.stack_size, len(instructions) - 1))

            if args.concurrency == 1 and (op in database_reads or op in database_mutations or op in ['TENANT_CREATE', 'TENANT_DELETE']):
                instructions.append('WAIT_FUTURE')

        instructions.begin_finalization()

        if not args.no_tenants:
            instructions.append('TENANT_CLEAR_ACTIVE')

        if args.concurrency == 1:
            self.wait_for_reads(instructions)
            test_util.blocking_commit(instructions)
            self.add_stack_items(1)

        instructions.append('NEW_TRANSACTION')
        instructions.push_args(self.stack_subspace.key())
        instructions.append('LOG_STACK')

        test_util.blocking_commit(instructions)

        return instructions

    @fdb.transactional
    def check_versionstamps(self, tr, begin_key, limit):
        next_begin = None
        incorrect_versionstamps = 0
        for k, v in tr.get_range(begin_key, self.versionstamped_values.range().stop, limit=limit):
            next_begin = k + b'\x00'
            random_id = self.versionstamped_values.unpack(k)[0]
            versioned_value = v[10:].replace(fdb.tuple.Versionstamp._UNSET_TR_VERSION, v[:10], 1)

            versioned_key = self.versionstamped_keys.pack() + versioned_value
            if tr[versioned_key] != random_id:
                util.get_logger().error('  INCORRECT VERSIONSTAMP:')
                util.get_logger().error('    %s != %s', repr(tr[versioned_key]), repr(random_id))
                incorrect_versionstamps += 1

            if self.api_version >= 520:
                k2 = self.versionstamped_values_2.pack((random_id,))
                if tr[k2] != versioned_value:
                    util.get_logger().error('  INCORRECT VERSIONSTAMP:')
                    util.get_logger().error('    %s != %s', repr(tr[k2]), repr(versioned_value))
                    incorrect_versionstamps += 1

        return (next_begin, incorrect_versionstamps)

    def validate(self, db, args):
        errors = []

        begin = self.versionstamped_values.range().start
        incorrect_versionstamps = 0

        while begin is not None:
            (begin, current_incorrect_versionstamps) = self.check_versionstamps(db, begin, 100)
            incorrect_versionstamps += current_incorrect_versionstamps

        if incorrect_versionstamps > 0:
            errors.append('There were %d failed version stamp operations' % incorrect_versionstamps)

        return errors

    def get_result_specifications(self):
        return [
            ResultSpecification(self.workspace, global_error_filter=[1007, 1009, 1021]),
            ResultSpecification(self.stack_subspace, key_start_index=1, ordering_index=1, global_error_filter=[1007, 1009, 1021])
        ]
