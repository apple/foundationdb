#!/usr/bin/python
#
# tester.py
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


import ctypes
import math
import sys
import os
import struct
import threading
import time
import random
import time
import traceback

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..')]
import fdb
fdb.api_version(int(sys.argv[2]))

from fdb import six
from fdb.impl import strinc
import fdb.tuple

from directory_extension import DirectoryExtension

from cancellation_timeout_tests import test_timeouts
from cancellation_timeout_tests import test_db_timeouts
from cancellation_timeout_tests import test_cancellation
from cancellation_timeout_tests import test_retry_limits
from cancellation_timeout_tests import test_db_retry_limits
from cancellation_timeout_tests import test_combinations

from size_limit_tests import test_size_limit_option, test_get_approximate_size
from tenant_tests import test_tenants

random.seed(0)

if len(sys.argv) == 4:
    db = fdb.open(sys.argv[3])
else:
    db = fdb.open()


class Stack:
    def __init__(self):
        self.stack = []

    def __repr__(self):
        return repr(self.stack)

    def __str__(self):
        return str(self.stack)

    def __len__(self):
        return len(self.stack)

    def __getitem__(self, idx):
        return self.stack[idx]

    def __setitem__(self, idx, val):
        self.stack[idx] = val

    def push(self, idx, val):
        self.stack.insert(0, (idx, val))

    def pop(self, count=None, with_idx=False):
        c = count
        if c is None:
            c = 1
        raw = self.stack[:c]
        del self.stack[:c]

        for i in range(len(raw)):
            if isinstance(raw[i][1], fdb.Future):
                try:
                    val = raw[i][1].wait()
                    if val is None or (hasattr(val, 'present') and not val.present()):
                        raw[i] = (raw[i][0], b'RESULT_NOT_PRESENT')
                    else:
                        raw[i] = (raw[i][0], val)
                except fdb.FDBError as e:
                    # print('ERROR: %r' % e)
                    raw[i] = (raw[i][0], fdb.tuple.pack((b'ERROR', str(e.code).encode('ascii'))))

        if count is None:
            if with_idx:
                return raw[0]
            else:
                return raw[0][1]
        else:
            if with_idx:
                return raw
            else:
                return [item[1] for item in raw]


class Instruction:
    def __init__(self, tr, stack, op, index, isDatabase=False, isTenant=False, isSnapshot=False):
        self.tr = tr
        self.stack = stack
        self.op = op
        self.index = index
        self.isDatabase = isDatabase
        self.isTenant = isTenant
        self.isSnapshot = isSnapshot

    def pop(self, count=None, with_idx=False):
        return self.stack.pop(count, with_idx)

    def push(self, val):
        self.stack.push(self.index, val)


def test_fdb_transactional_generator(db):
    try:
        @fdb.transactional
        def function_that_yields(tr):
            yield 0
        assert fdb.get_api_version() < 630, "Pre-6.3, a decorator may wrap a function that yields"
    except ValueError as e:
        assert fdb.get_api_version() >= 630, "Post-6.3, a decorator should throw if wrapped function yields"


def test_fdb_transactional_returns_generator(db):
    try:
        def function_that_yields(tr):
            yield 0
        @fdb.transactional
        def function_that_returns(tr):
            return function_that_yields(tr)
        function_that_returns()
        assert fdb.get_api_version() < 630, "Pre-6.3, returning a generator is allowed"
    except ValueError as e:
        assert fdb.get_api_version() >= 630, "Post-6.3, returning a generator should throw"


def test_db_options(db):
    db.options.set_location_cache_size(100001)
    db.options.set_max_watches(100001)
    db.options.set_datacenter_id("dc_id")
    db.options.set_machine_id("machine_id")
    db.options.set_snapshot_ryw_enable()
    db.options.set_snapshot_ryw_disable()
    db.options.set_transaction_logging_max_field_length(1000)
    db.options.set_transaction_timeout(100000)
    db.options.set_transaction_timeout(0)
    db.options.set_transaction_timeout(0)
    db.options.set_transaction_max_retry_delay(100)
    db.options.set_transaction_size_limit(100000)
    db.options.set_transaction_retry_limit(10)
    db.options.set_transaction_retry_limit(-1)
    db.options.set_transaction_causal_read_risky()
    db.options.set_transaction_include_port_in_address()


@fdb.transactional
def test_options(tr):
    tr.options.set_priority_system_immediate()
    tr.options.set_priority_batch()
    tr.options.set_causal_read_risky()
    tr.options.set_causal_write_risky()
    tr.options.set_read_your_writes_disable()
    tr.options.set_read_system_keys()
    tr.options.set_access_system_keys()
    tr.options.set_transaction_logging_max_field_length(1000)
    tr.options.set_timeout(60 * 1000)
    tr.options.set_retry_limit(50)
    tr.options.set_max_retry_delay(100)
    tr.options.set_used_during_commit_protection_disable()
    tr.options.set_debug_transaction_identifier('my_transaction')
    tr.options.set_log_transaction()
    tr.options.set_read_lock_aware()
    tr.options.set_lock_aware()
    tr.options.set_include_port_in_address()

    tr.get(b'\xff').wait()


def check_watches(db, watches, expected):
    for i, watch in enumerate(watches):
        if watch.is_ready() or expected:
            try:
                watch.wait()
                if not expected:
                    assert False, "Watch %d is ready" % i
            except fdb.FDBError as e:
                tr = db.create_transaction()
                tr.on_error(e).wait()
                return False

    return True


def test_watches(db):
    while True:
        db[b'w0'] = b'0'
        db[b'w3'] = b'3'

        watches = [None]

        @fdb.transactional
        def txn1(tr):
            watches[0] = tr.watch(b'w0')
            tr.set(b'w0', b'0')
            assert not watches[0].is_ready()

        txn1(db)

        watches.append(db.clear_and_watch(b'w1'))
        watches.append(db.set_and_watch(b'w2', b'2'))
        watches.append(db.get_and_watch(b'w3'))

        assert watches[3][0] == b'3'
        watches[3] = watches[3][1]

        time.sleep(1)

        if not check_watches(db, watches, False):
            continue

        del db[b'w1']

        time.sleep(5)

        if not check_watches(db, watches, False):
            continue

        db[b'w0'] = b'a'
        db[b'w1'] = b'b'
        del db[b'w2']
        db.bit_xor(b'w3', b'\xff\xff')

        if check_watches(db, watches, True):
            return


@fdb.transactional
def test_locality(tr):
    tr.options.set_timeout(60 * 1000)
    tr.options.set_read_system_keys()  # We do this because the last shard (for now, someday the last N shards) is in the /FF/ keyspace

    # This isn't strictly transactional, thought we expect it to be given the size of our database
    boundary_keys = list(fdb.locality.get_boundary_keys(tr, b'', b'\xff\xff')) + [b'\xff\xff']
    end_keys = [tr.get_key(fdb.KeySelector.last_less_than(k)) for k in boundary_keys[1:]]

    start_addresses = [fdb.locality.get_addresses_for_key(tr, k) for k in boundary_keys[:-1]]
    end_addresses = [fdb.locality.get_addresses_for_key(tr, k) for k in end_keys]

    if [set(s.wait()) for s in start_addresses] != [set(e.wait()) for e in end_addresses]:
        raise Exception("Locality not internally consistent.")


def test_predicates():
    assert fdb.predicates.is_retryable(fdb.FDBError(1020))
    assert not fdb.predicates.is_retryable(fdb.FDBError(10))


class Tester:
    tr_map = {}
    tr_map_lock = threading.RLock()

    def __init__(self, db, prefix):
        self.db = db
        self.tenant = None

        self.instructions = self.db[fdb.tuple.range((prefix,))]

        self.stack = Stack()
        self.tr_name = prefix
        Tester.tr_map[self.tr_name] = None
        self.last_version = 0

        self.threads = []
        self.directory_extension = DirectoryExtension()

    def push_range(self, inst, iter, prefix_filter=None):
        kvs = []
        for k, v in iter:
            if prefix_filter is None or k.startswith(prefix_filter):
                kvs += [k, v]

        inst.push(fdb.tuple.pack(tuple(kvs)))

    @staticmethod
    @fdb.transactional
    def wait_empty(tr, prefix):
        res = tr.get_range_startswith(prefix, 1).to_list()
        if len(res) == 1:
            raise fdb.FDBError(1020)

    @fdb.transactional
    def log_stack(self, tr, prefix, entries):
        for i, (idx, el) in entries.items():
            pk = prefix + fdb.tuple.pack((i, idx))
            pv = fdb.tuple.pack((el,))

            tr.set(pk, pv[:40000])

    def current_transaction(self):
        with Tester.tr_map_lock:
            return Tester.tr_map[self.tr_name]

    def new_transaction(self):
        with Tester.tr_map_lock:
            tr_source = self.tenant if self.tenant is not None else self.db
            Tester.tr_map[self.tr_name] = tr_source.create_transaction()

    def switch_transaction(self, name):
        self.tr_name = name
        with Tester.tr_map_lock:
            if self.tr_name not in Tester.tr_map:
                self.new_transaction()

    def run(self):
        for idx, i in enumerate(self.instructions):
            op_tuple = fdb.tuple.unpack(i.value)
            op = op_tuple[0]

            # print("Stack is %r" % self.stack)
            # if op != "PUSH" and op != "SWAP":
            #     print("%d. Instruction is %s" % (idx, op))

            isDatabase = op.endswith(six.u('_DATABASE'))
            isTenant = op.endswith(six.u('_TENANT'))
            isSnapshot = op.endswith(six.u('_SNAPSHOT'))

            if isDatabase:
                op = op[:-9]
                obj = self.db
            elif isTenant:
                op = op[:-7]
                obj = self.tenant if self.tenant else self.db
            elif isSnapshot:
                op = op[:-9]
                obj = self.current_transaction().snapshot
            else:
                obj = self.current_transaction()

            inst = Instruction(obj, self.stack, op, idx, isDatabase, isTenant, isSnapshot)

            try:
                if inst.op == six.u("PUSH"):
                    inst.push(op_tuple[1])
                elif inst.op == six.u("DUP"):
                    inst.stack.push(*self.stack[0])
                elif inst.op == six.u("EMPTY_STACK"):
                    self.stack = Stack()
                elif inst.op == six.u("SWAP"):
                    idx = inst.pop()
                    self.stack[0], self.stack[idx] = self.stack[idx], self.stack[0]
                elif inst.op == six.u("POP"):
                    inst.pop()
                elif inst.op == six.u("SUB"):
                    a, b = inst.pop(2)
                    inst.push(a - b)
                elif inst.op == six.u("CONCAT"):
                    a, b = inst.pop(2)
                    inst.push(a + b)
                elif inst.op == six.u("WAIT_FUTURE"):
                    old_idx, item = inst.pop(with_idx=True)
                    inst.stack.push(old_idx, item)
                elif inst.op == six.u("NEW_TRANSACTION"):
                    self.new_transaction()
                elif inst.op == six.u("USE_TRANSACTION"):
                    self.switch_transaction(inst.pop())
                elif inst.op == six.u("ON_ERROR"):
                    inst.push(inst.tr.on_error(inst.pop()))
                elif inst.op == six.u("GET"):
                    key = inst.pop()
                    num = random.randint(0, 2)
                    if num == 0:
                        f = obj[key]
                    elif num == 1:
                        f = obj.get(key)
                    else:
                        f = obj.__getitem__(key)

                    if f == None:
                        inst.push(b'RESULT_NOT_PRESENT')
                    else:
                        inst.push(f)
                elif inst.op == six.u("GET_ESTIMATED_RANGE_SIZE"):
                    begin, end = inst.pop(2)
                    estimatedSize = obj.get_estimated_range_size_bytes(begin, end).wait()
                    inst.push(b"GOT_ESTIMATED_RANGE_SIZE")
                elif inst.op == six.u("GET_RANGE_SPLIT_POINTS"):
                    begin, end, chunkSize = inst.pop(3)
                    estimatedSize = obj.get_range_split_points(begin, end, chunkSize).wait()
                    inst.push(b"GOT_RANGE_SPLIT_POINTS")
                elif inst.op == six.u("GET_KEY"):
                    key, or_equal, offset, prefix = inst.pop(4)
                    result = obj.get_key(fdb.KeySelector(key, or_equal, offset))
                    if result.startswith(prefix):
                        inst.push(result)
                    elif result < prefix:
                        inst.push(prefix)
                    else:
                        inst.push(strinc(prefix))

                elif inst.op == six.u("GET_RANGE"):
                    begin, end, limit, reverse, mode = inst.pop(5)
                    if limit == 0 and mode == -1 and random.random() < 0.5:
                        if reverse:
                            r = obj[begin:end:-1]
                        else:
                            r = obj[begin:end]
                    else:
                        r = obj.get_range(begin, end, limit, reverse, mode)

                    self.push_range(inst, r)
                elif inst.op == six.u("GET_RANGE_STARTS_WITH"):
                    prefix, limit, reverse, mode = inst.pop(4)
                    self.push_range(inst, obj.get_range_startswith(prefix, limit, reverse, mode))
                elif inst.op == six.u("GET_RANGE_SELECTOR"):
                    begin_key, begin_or_equal, begin_offset, end_key, end_or_equal, end_offset, limit, reverse, mode, prefix = inst.pop(10)
                    beginSel = fdb.KeySelector(begin_key, begin_or_equal, begin_offset)
                    endSel = fdb.KeySelector(end_key, end_or_equal, end_offset)
                    if limit == 0 and mode == -1 and random.random() < 0.5:
                        if reverse:
                            r = obj[beginSel:endSel:-1]
                        else:
                            r = obj[beginSel:endSel]
                    else:
                        r = obj.get_range(beginSel, endSel, limit, reverse, mode)

                    self.push_range(inst, r, prefix_filter=prefix)
                elif inst.op == six.u("GET_READ_VERSION"):
                    self.last_version = obj.get_read_version().wait()
                    inst.push(b"GOT_READ_VERSION")
                elif inst.op == six.u("SET"):
                    key, value = inst.pop(2)
                    if random.random() < 0.5:
                        obj[key] = value
                    else:
                        obj.set(key, value)

                    if obj == self.db:
                        inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("LOG_STACK"):
                    prefix = inst.pop()
                    entries = {}
                    while len(self.stack) > 0:
                        stack_index = len(self.stack) - 1
                        entries[stack_index] = inst.pop(with_idx=True)
                        if len(entries) == 100:
                            self.log_stack(self.db, prefix, entries)
                            entries = {}

                    self.log_stack(self.db, prefix, entries)
                elif inst.op == six.u("ATOMIC_OP"):
                    opType, key, value = inst.pop(3)
                    getattr(obj, opType.lower())(key, value)

                    if obj == self.db:
                        inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("SET_READ_VERSION"):
                    inst.tr.set_read_version(self.last_version)
                elif inst.op == six.u("CLEAR"):
                    if random.random() < 0.5:
                        del obj[inst.pop()]
                    else:
                        obj.clear(inst.pop())

                    if obj == self.db:
                        inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("CLEAR_RANGE"):
                    begin, end = inst.pop(2)
                    num = random.randint(0, 2)
                    if num == 0:
                        del obj[begin:end]
                    elif num == 1:
                        obj.clear_range(begin, end)
                    else:
                        obj.__delitem__(slice(begin, end))

                    if obj == self.db:
                        inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("CLEAR_RANGE_STARTS_WITH"):
                    obj.clear_range_startswith(inst.pop())
                    if obj == self.db:
                        inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("READ_CONFLICT_RANGE"):
                    inst.tr.add_read_conflict_range(inst.pop(), inst.pop())
                    inst.push(b"SET_CONFLICT_RANGE")
                elif inst.op == six.u("WRITE_CONFLICT_RANGE"):
                    inst.tr.add_write_conflict_range(inst.pop(), inst.pop())
                    inst.push(b"SET_CONFLICT_RANGE")
                elif inst.op == six.u("READ_CONFLICT_KEY"):
                    inst.tr.add_read_conflict_key(inst.pop())
                    inst.push(b"SET_CONFLICT_KEY")
                elif inst.op == six.u("WRITE_CONFLICT_KEY"):
                    inst.tr.add_write_conflict_key(inst.pop())
                    inst.push(b"SET_CONFLICT_KEY")
                elif inst.op == six.u("DISABLE_WRITE_CONFLICT"):
                    inst.tr.options.set_next_write_no_write_conflict_range()
                elif inst.op == six.u("COMMIT"):
                    inst.push(inst.tr.commit())
                elif inst.op == six.u("RESET"):
                    inst.tr.reset()
                elif inst.op == six.u("CANCEL"):
                    inst.tr.cancel()
                elif inst.op == six.u("GET_COMMITTED_VERSION"):
                    self.last_version = inst.tr.get_committed_version()
                    inst.push(b"GOT_COMMITTED_VERSION")
                elif inst.op == six.u("GET_APPROXIMATE_SIZE"):
                    approximate_size = inst.tr.get_approximate_size().wait()
                    inst.push(b"GOT_APPROXIMATE_SIZE")
                elif inst.op == six.u("GET_VERSIONSTAMP"):
                    inst.push(inst.tr.get_versionstamp())
                elif inst.op == six.u("TUPLE_PACK"):
                    count = inst.pop()
                    items = inst.pop(count)
                    inst.push(fdb.tuple.pack(tuple(items)))
                elif inst.op == six.u("TUPLE_PACK_WITH_VERSIONSTAMP"):
                    prefix = inst.pop()
                    count = inst.pop()
                    items = inst.pop(count)
                    if not fdb.tuple.has_incomplete_versionstamp(items) and random.random() < 0.5:
                        inst.push(b"ERROR: NONE")
                    else:
                        try:
                            packed = fdb.tuple.pack_with_versionstamp(tuple(items), prefix=prefix)
                            inst.push(b"OK")
                            inst.push(packed)
                        except ValueError as e:
                            if str(e).startswith("No incomplete"):
                                inst.push(b"ERROR: NONE")
                            else:
                                inst.push(b"ERROR: MULTIPLE")
                elif inst.op == six.u("TUPLE_UNPACK"):
                    for i in fdb.tuple.unpack(inst.pop()):
                        inst.push(fdb.tuple.pack((i,)))
                elif inst.op == six.u("TUPLE_SORT"):
                    count = inst.pop()
                    items = inst.pop(count)
                    unpacked = map(fdb.tuple.unpack, items)
                    if six.PY3:
                        sorted_items = sorted(unpacked, key=fdb.tuple.pack)
                    else:
                        sorted_items = sorted(unpacked, cmp=fdb.tuple.compare)
                    for item in sorted_items:
                        inst.push(fdb.tuple.pack(item))
                elif inst.op == six.u("TUPLE_RANGE"):
                    count = inst.pop()
                    items = inst.pop(count)
                    r = fdb.tuple.range(tuple(items))
                    inst.push(r.start)
                    inst.push(r.stop)
                elif inst.op == six.u("ENCODE_FLOAT"):
                    f_bytes = inst.pop()
                    f = struct.unpack(">f", f_bytes)[0]
                    if not math.isnan(f) and not math.isinf(f) and not f == -0.0 and f == int(f):
                        f = int(f)
                    inst.push(fdb.tuple.SingleFloat(f))
                elif inst.op == six.u("ENCODE_DOUBLE"):
                    d_bytes = inst.pop()
                    d = struct.unpack(">d", d_bytes)[0]
                    inst.push(d)
                elif inst.op == six.u("DECODE_FLOAT"):
                    f = inst.pop()
                    f_bytes = struct.pack(">f", f.value)
                    inst.push(f_bytes)
                elif inst.op == six.u("DECODE_DOUBLE"):
                    d = inst.pop()
                    d_bytes = struct.pack(">d", d)
                    inst.push(d_bytes)
                elif inst.op == six.u("START_THREAD"):
                    t = Tester(self.db, inst.pop())
                    thr = threading.Thread(target=t.run)
                    thr.start()
                    self.threads.append(thr)
                elif inst.op == six.u("WAIT_EMPTY"):
                    prefix = inst.pop()
                    Tester.wait_empty(self.db, prefix)
                    inst.push(b"WAITED_FOR_EMPTY")
                elif inst.op == six.u("TENANT_CREATE"):
                    name = inst.pop()
                    self.db.allocate_tenant(name)
                    inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("TENANT_DELETE"):
                    name = inst.pop()
                    self.db.delete_tenant(name)
                    inst.push(b"RESULT_NOT_PRESENT")
                elif inst.op == six.u("TENANT_SET_ACTIVE"):
                    name = inst.pop()
                    self.tenant = self.db.open_tenant(name)
                elif inst.op == six.u("TENANT_CLEAR_ACTIVE"):
                    self.tenant = None
                elif inst.op == six.u("UNIT_TESTS"):
                    try:
                        test_db_options(db)
                        test_options(db)
                        test_watches(db)
                        test_cancellation(db)
                        test_retry_limits(db)
                        test_db_retry_limits(db)
                        test_timeouts(db)
                        test_db_timeouts(db)
                        test_combinations(db)
                        test_locality(db)
                        test_predicates()

                        test_size_limit_option(db)
                        test_get_approximate_size(db)

                        test_tenants(db)

                    except fdb.FDBError as e:
                        print("Unit tests failed: %s" % e.description)
                        traceback.print_exc()

                        raise Exception("Unit tests failed: %s" % e.description)
                elif inst.op.startswith(six.u('DIRECTORY_')):
                    self.directory_extension.process_instruction(inst)
                else:
                    raise Exception("Unknown op %s" % inst.op)
            except fdb.FDBError as e:
                # print('ERROR: %r' % e)
                inst.stack.push(idx, fdb.tuple.pack((b"ERROR", str(e.code).encode('ascii'))))

            # print("        to %s" % self.stack)
            # print()

        [thr.join() for thr in self.threads]


if __name__ == '__main__':
    t = Tester(db, sys.argv[1].encode('ascii'))
    t.run()
