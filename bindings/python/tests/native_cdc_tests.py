#!/usr/bin/env python3
#
# native_cdc_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2026 Apple Inc. and the FoundationDB project authors
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

import argparse
import ctypes
import gc
import threading
import time
import unittest
import uuid
from unittest import mock

import fdb


def wait(future, timeout=30):
    ready = threading.Event()
    future.on_ready(lambda _: ready.set())
    if not ready.wait(timeout):
        future.cancel()
        raise AssertionError(
            "CDC operation did not complete within {} seconds".format(timeout)
        )
    return future.wait()


class CdcDecodingTests(unittest.TestCase):
    def test_packed_c_layout(self):
        impl = fdb.impl
        pointer_size = ctypes.sizeof(ctypes.c_void_p)
        layouts = (
            (impl.KeyRangeStruct, 2 * pointer_size + 8),
            (impl.CdcStreamInfoStruct, 3 * pointer_size + 28),
            (impl.CdcMutationStruct, 2 * pointer_size + 12),
            (impl.CdcVersionedMutationsStruct, pointer_size + 12),
        )
        for structure, size in layouts:
            with self.subTest(structure=structure.__name__):
                self.assertEqual(structure._pack_, 4)
                self.assertEqual(ctypes.sizeof(structure), size)
        self.assertEqual(impl.CdcMutationStruct.param1.offset, 4)
        self.assertEqual(impl.CdcStreamInfoStruct.stream_id.offset, pointer_size + 4)

    def test_unknown_mutation_type_and_copied_bytes(self):
        impl = fdb.impl
        key = ctypes.create_string_buffer(b"key\x00\xff")
        value = ctypes.create_string_buffer(b"\x00value\xff")
        mutations = (impl.CdcMutationStruct * 2)(
            impl.CdcMutationStruct(
                255,
                ctypes.cast(key, ctypes.POINTER(ctypes.c_byte)),
                len(key) - 1,
                ctypes.cast(value, ctypes.POINTER(ctypes.c_byte)),
                len(value) - 1,
            ),
            impl.CdcMutationStruct(fdb.CdcMutationType.SET_VALUE, None, 0, None, 0),
        )
        groups = (impl.CdcVersionedMutationsStruct * 2)(
            impl.CdcVersionedMutationsStruct(100, mutations, 2),
            impl.CdcVersionedMutationsStruct(101, None, 0),
        )

        def get_result(pointer, out_groups, out_count, out_version):
            ctypes.cast(
                out_groups,
                ctypes.POINTER(ctypes.POINTER(impl.CdcVersionedMutationsStruct)),
            )[0] = groups
            ctypes.cast(out_count, ctypes.POINTER(ctypes.c_int))[0] = len(groups)
            ctypes.cast(out_version, ctypes.POINTER(ctypes.c_int64))[0] = 150

        future = impl.FutureCdcConsumeResult(1)
        future.capi = mock.Mock()
        future.capi.fdb_future_is_ready.return_value = 1
        future.capi.fdb_future_get_cdc_versioned_mutations.side_effect = get_result
        result = future.wait()
        ctypes.memset(key, 0, len(key))
        ctypes.memset(value, 0, len(value))
        mutations[0].type = 0
        groups[0].version = 0
        del future
        gc.collect()

        self.assertEqual(
            result,
            fdb.CdcConsumeResult(
                (
                    fdb.CdcVersionedMutations(
                        100,
                        (
                            fdb.CdcMutation(255, b"key\x00\xff", b"\x00value\xff"),
                            fdb.CdcMutation(fdb.CdcMutationType.SET_VALUE, b"", b""),
                        ),
                    ),
                    fdb.CdcVersionedMutations(101, ()),
                ),
                150,
            ),
        )
        self.assertIs(type(result.mutations[0].mutations[0].type), int)
        with self.assertRaises(AttributeError):
            result.last_consumed_version = 0

    def test_empty_reply_preserves_progress(self):
        def get_result(pointer, out_groups, out_count, out_version):
            ctypes.cast(out_count, ctypes.POINTER(ctypes.c_int))[0] = 0
            ctypes.cast(out_version, ctypes.POINTER(ctypes.c_int64))[0] = 2**63 - 1

        future = fdb.impl.FutureCdcConsumeResult(1)
        future.capi = mock.Mock()
        future.capi.fdb_future_is_ready.return_value = 1
        future.capi.fdb_future_get_cdc_versioned_mutations.side_effect = get_result
        self.assertEqual(future.wait(), fdb.CdcConsumeResult((), 2**63 - 1))


class NativeCdcTests(unittest.TestCase):
    def setUp(self):
        self.prefix = b"python-cdc/" + uuid.uuid4().bytes + b"\x00/"
        self.name = self.prefix + b"stream\x00\xff"
        self.begin = self.prefix + b"range/"
        self.end = self.prefix + b"range0"
        self.addCleanup(self.db.clear_range, self.prefix, self.prefix + b"\xff")

    def register(self):
        stream_id = wait(self.db.register_cdc_stream(self.name, self.begin, self.end))
        self.addCleanup(lambda: wait(self.db.remove_cdc_stream(self.name)))
        return stream_id

    def stream_info(self):
        future = self.db.list_cdc_streams()
        streams = wait(future)
        self.assertEqual(future.wait(), streams)
        future._release_memory()
        del future
        gc.collect()
        return next(stream for stream in streams if stream.name == self.name)

    def commit(self, write):
        tr = self.db.create_transaction()
        tr.options.set_timeout(30000)
        while True:
            try:
                write(tr)
                wait(tr.commit())
                return tr.get_committed_version()
            except fdb.FDBError as error:
                wait(tr.on_error(error))

    def consume_through(self, consumer, version):
        groups = {}
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            future = consumer.consume()
            result = wait(future)
            self.assertEqual(future.wait(), result)
            future._release_memory()
            del future
            gc.collect()
            self.assertEqual(
                consumer.get_position().last_consumed_version,
                result.last_consumed_version,
            )
            self.assertIsInstance(result.mutations, tuple)
            for group in result.mutations:
                self.assertIsInstance(group.mutations, tuple)
                if group.version in groups:
                    self.assertEqual(groups[group.version], group.mutations)
                groups[group.version] = group.mutations
            if result.last_consumed_version >= version:
                return groups
        self.fail("CDC did not deliver the committed version within 60 seconds")

    def assert_closed(self, consumer):
        consumer.close()
        consumer.close()
        for operation in (
            consumer.consume,
            consumer.acknowledge,
            consumer.get_position,
        ):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(ValueError):
                    operation()

    def test_stream_lifecycle_and_result_ownership(self):
        stream_id = self.register()
        self.assertGreater(stream_id, 0)
        self.assertEqual(
            wait(self.db.register_cdc_stream(self.name, self.begin, self.end)),
            stream_id,
        )
        info = self.stream_info()
        self.assertEqual(
            (info.name, info.stream_id, info.begin_key, info.end_key),
            (self.name, stream_id, self.begin, self.end),
        )
        self.assertGreaterEqual(info.min_version, 0)
        with self.assertRaises(AttributeError):
            info.name = b"different"

        create_future = self.db.create_cdc_consumer(self.name)
        consumer = wait(create_future)
        self.addCleanup(consumer.close)
        self.assertIs(create_future.wait(), consumer)
        self.assertIs(create_future.result(), consumer)
        self.assertIsNone(create_future.exception())
        create_future._release_memory()
        del create_future
        gc.collect()
        self.assertEqual(consumer.get_position(), fdb.CdcCursor(stream_id, -1))

        first_key = self.begin + b"first\x00\xff"
        second_key = self.begin + b"second"
        empty_key = self.begin + b"empty"
        first_value = b"\x00first\xffvalue\x00"
        second_value = b"second-value"

        def write_sets(tr):
            tr[first_key] = first_value
            tr[second_key] = second_value
            tr[self.prefix + b"outside"] = b"not in the stream"

        set_version = self.commit(write_sets)
        empty_version = self.commit(lambda tr: tr.set(empty_key, b""))
        groups = self.consume_through(consumer, empty_version)
        self.assertCountEqual(
            groups[set_version],
            (
                fdb.CdcMutation(fdb.CdcMutationType.SET_VALUE, first_key, first_value),
                fdb.CdcMutation(
                    fdb.CdcMutationType.SET_VALUE, second_key, second_value
                ),
            ),
        )
        self.assertEqual(
            groups[empty_version],
            (fdb.CdcMutation(fdb.CdcMutationType.SET_VALUE, empty_key, b""),),
        )
        cursor = consumer.get_position()
        self.assert_closed(consumer)
        self.assertEqual(self.stream_info().min_version, info.min_version)

        resume_future = self.db.resume_cdc_consumer(cursor)
        resumed = wait(resume_future)
        self.addCleanup(resumed.close)
        self.assertIs(resume_future.wait(), resumed)
        del resume_future
        gc.collect()
        with resumed:
            self.assertEqual(resumed.get_position(), cursor)
            # A resumed handle has no local delivery proof. Its acknowledgement
            # must be at or behind a fresh database read version.
            deadline = time.monotonic() + 30
            while True:
                remaining = deadline - time.monotonic()
                self.assertGreater(remaining, 0, "Read version did not reach cursor")
                tr = self.db.create_transaction()
                read_version = wait(
                    tr.get_read_version(),
                    timeout=max(0, deadline - time.monotonic()),
                )
                if read_version >= cursor.last_consumed_version:
                    break
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
            # Reconcile the durable checkpoint, then reissue the acknowledgement.
            for _ in range(2):
                self.assertIsNone(wait(resumed.acknowledge()))
                self.assertEqual(
                    self.stream_info().min_version,
                    cursor.last_consumed_version + 1,
                )
            clear_end = first_key + b"\x00"
            counter_key = self.begin + b"counter"
            operand = b"\x01\x00\x00\x00"

            def write_raw_mutations(tr):
                tr.clear_range(first_key, clear_end)
                tr.add(counter_key, operand)

            raw_version = self.commit(write_raw_mutations)
            groups = self.consume_through(resumed, raw_version)
            self.assertCountEqual(
                groups[raw_version],
                (
                    fdb.CdcMutation(
                        fdb.CdcMutationType.CLEAR_RANGE, first_key, clear_end
                    ),
                    fdb.CdcMutation(fdb.CdcMutationType.ADD, counter_key, operand),
                ),
            )
            self.assertIsNone(wait(resumed.acknowledge()))
        self.assert_closed(resumed)

        self.assertIsNone(wait(self.db.remove_cdc_stream(self.name)))
        self.assertIsNone(wait(self.db.remove_cdc_stream(self.name)))
        self.assertNotIn(
            self.name, [stream.name for stream in wait(self.db.list_cdc_streams())]
        )

    def test_native_errors_propagate(self):
        with self.assertRaises(fdb.FDBError):
            wait(self.db.create_cdc_consumer(self.name))
        self.register()
        with self.assertRaises(fdb.FDBError):
            wait(self.db.register_cdc_stream(self.name, self.begin, self.end + b"\x00"))
        self.assertEqual(self.stream_info().end_key, self.end)

    def test_missing_cdc_symbols_preserves_normal_database_use(self):
        impl = fdb.impl
        capi = impl._capi

        class WithoutCdcSymbols:
            def __getattr__(self, name):
                if "_cdc_" in name:
                    raise AttributeError(name)
                return getattr(capi, name)

        with mock.patch.object(impl, "_capi", WithoutCdcSymbols()):
            with mock.patch.object(impl, "_cdc_c_api_initialized", False):
                impl.init_c_api()
                with self.assertRaisesRegex(
                    RuntimeError, "does not support native CDC"
                ):
                    self.db.list_cdc_streams()
                self.assertFalse(impl._cdc_c_api_initialized)
                key = self.prefix + b"compatibility"
                self.db[key] = b"ordinary value"
                self.assertEqual(self.db[key], b"ordinary value")

    def test_cursor_integer_boundaries(self):
        for cursor in (
            fdb.CdcCursor(0, -(2**63)),
            fdb.CdcCursor(2**64 - 1, 2**63 - 1),
        ):
            with self.subTest(cursor=cursor):
                with wait(self.db.resume_cdc_consumer(cursor)) as consumer:
                    self.assertEqual(consumer.get_position(), cursor)
        for cursor in (
            fdb.CdcCursor(-1, -1),
            fdb.CdcCursor(2**64, -1),
            fdb.CdcCursor(1, -(2**63) - 1),
            fdb.CdcCursor(1, 2**63),
        ):
            with self.subTest(cursor=cursor):
                with self.assertRaises(ValueError):
                    self.db.resume_cdc_consumer(cursor)
        for cursor in (fdb.CdcCursor(1.5, -1), fdb.CdcCursor(1, "0")):
            with self.subTest(cursor=cursor):
                with self.assertRaises(TypeError):
                    self.db.resume_cdc_consumer(cursor)


class LegacyApiTests(unittest.TestCase):
    def test_normal_database_use_and_cdc_version_gate(self):
        key = b"python-cdc-legacy/" + uuid.uuid4().bytes
        self.addCleanup(self.db.clear, key)
        self.db[key] = b"normal database operations still work"
        self.assertEqual(self.db[key], b"normal database operations still work")
        operations = (
            lambda: self.db.register_cdc_stream(b"legacy", b"a", b"z"),
            lambda: self.db.remove_cdc_stream(b"legacy"),
            lambda: self.db.list_cdc_streams(),
            lambda: self.db.create_cdc_consumer(b"legacy"),
            lambda: self.db.resume_cdc_consumer(fdb.CdcCursor(1, -1)),
        )
        for operation in operations:
            with self.assertRaisesRegex(RuntimeError, "requires API version 800"):
                operation()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Native CDC Python binding tests")
    parser.add_argument("--cluster-file", "-C", required=True)
    parser.add_argument("--api-version", type=int, default=fdb.LATEST_API_VERSION)
    parser.add_argument("--verbose", "-V", action="store_true")
    args = parser.parse_args()
    fdb.api_version(args.api_version)
    db = fdb.open(args.cluster_file)
    db.options.set_transaction_timeout(30000)
    NativeCdcTests.db = db
    LegacyApiTests.db = db
    classes = (
        (CdcDecodingTests, NativeCdcTests)
        if args.api_version >= 800
        else (LegacyApiTests,)
    )
    suite = unittest.TestSuite(
        unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in classes
    )
    result = unittest.TextTestRunner(verbosity=2 if args.verbose else 1).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
