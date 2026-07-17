import importlib
import struct
import sys
import types
import unittest

from test_harness.summarize import Coverage

fdb_stub = sys.modules.get("fdb")
if fdb_stub is None:
    fdb_stub = types.ModuleType("fdb")
    fdb_stub.__path__ = []
    fdb_stub.api_version = lambda *_: None
    fdb_stub.transactional = lambda function: function
    fdb_stub.tuple = types.ModuleType("fdb.tuple")
    sys.modules["fdb"] = fdb_stub
    sys.modules["fdb.tuple"] = fdb_stub.tuple
harness_fdb = importlib.import_module("test_harness.fdb")


class FakeValue:
    def __init__(self, present):
        self._present = present

    def present(self):
        return self._present


class FakeSnapshot:
    def __init__(self, transaction):
        self.transaction = transaction

    def __getitem__(self, key):
        return FakeValue(key in self.transaction.values)


class FakeDirectory:
    def __init__(self, prefix):
        self.prefix = prefix

    def __getitem__(self, key):
        return self.prefix + (key,)

    def pack(self, key):
        return self.prefix + key


class FakeTransaction:
    def __init__(self, values):
        self.values = values
        self.snapshot = FakeSnapshot(self)
        self.mutations = []

    def __getitem__(self, key):
        return FakeValue(key in self.values)

    def add(self, key, value):
        self.mutations.append(key)
        self.values[key] = self.values.get(key, 0) + struct.unpack("<I", value)[0]


class CoveragePersistenceTest(unittest.TestCase):
    def test_late_discovered_zero_hit_probe_is_persisted_once(self):
        coverage_path = ("coverage",)
        metadata_path = ("metadata",)
        fdb_stub.directory = types.SimpleNamespace(
            create_or_open=lambda _, path: FakeDirectory(path)
        )
        hit = Coverage("fdbserver/hit.cpp", 10, "hit", False)
        existing_zero = Coverage("fdbserver/zero.cpp", 20, "zero", False)
        late_zero = Coverage("fdbserver/late.cpp", 30, "late", False)
        hit_key = coverage_path + (hit.file, hit.line, hit.comment, hit.rare)
        existing_zero_key = coverage_path + (
            existing_zero.file,
            existing_zero.line,
            existing_zero.comment,
            existing_zero.rare,
        )
        late_zero_key = coverage_path + (
            late_zero.file,
            late_zero.line,
            late_zero.comment,
            late_zero.rare,
        )
        transaction = FakeTransaction(
            {
                metadata_path + ("initialized",): True,
                hit_key: 5,
                existing_zero_key: 0,
            }
        )
        coverage = [(hit, True), (existing_zero, False), (late_zero, False)]

        initialized = harness_fdb.write_coverage_chunk(
            transaction, coverage_path, metadata_path, coverage, False
        )

        self.assertTrue(initialized)
        self.assertEqual(transaction.values[hit_key], 6)
        self.assertEqual(transaction.values[existing_zero_key], 0)
        self.assertIn(late_zero_key, transaction.values)
        self.assertEqual(transaction.values[late_zero_key], 0)
        self.assertEqual(transaction.mutations, [hit_key, late_zero_key])

        transaction.mutations.clear()
        harness_fdb.write_coverage_chunk(
            transaction, coverage_path, metadata_path, coverage, initialized
        )

        self.assertEqual(transaction.values[hit_key], 7)
        self.assertEqual(transaction.mutations, [hit_key])


if __name__ == "__main__":
    unittest.main()
