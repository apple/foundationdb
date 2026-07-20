import importlib
import struct
import sys
import types
import unittest
from types import SimpleNamespace
from unittest import mock

from test_harness.config import config
from test_harness.summarize import Coverage

fdb_stub = sys.modules.setdefault("fdb", types.ModuleType("fdb"))
fdb_stub.__path__ = []
fdb_stub.api_version = lambda *_: None
fdb_stub.transactional = lambda function: function
fdb_stub.tuple = sys.modules.setdefault("fdb.tuple", types.ModuleType("fdb.tuple"))
harness_fdb = importlib.import_module("test_harness.fdb")
EnsembleResults = importlib.import_module("test_harness.results").EnsembleResults


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
        self.snapshot = self
        self.mutations = []

    def __getitem__(self, key):
        return SimpleNamespace(present=lambda: key in self.values)

    def add(self, key, value):
        self.mutations.append(key)
        self.values[key] = self.values.get(key, 0) + struct.unpack("<I", value)[0]


class CoverageTest(unittest.TestCase):
    def test_frequency_threshold_uses_total_test_runs(self):
        stats = SimpleNamespace(
            stats={"fast": SimpleNamespace(runtime=30, run_count=100000)}
        )
        coverage = {
            Coverage("a.cpp", 1, "nonrare", False): 5,
            Coverage("b.cpp", 2, "rare", True): 4,
            Coverage("c.cpp", 3, "hit", False): 6,
        }

        with mock.patch.object(
            harness_fdb, "Statistics", return_value=stats
        ), mock.patch.object(
            harness_fdb, "read_coverage", return_value=coverage
        ), mock.patch.multiple(
            config,
            disable_code_probes=False,
            hit_per_runs_ratio=20000,
            cov_include_files=r".*",
            cov_exclude_files=r".^",
        ):
            results = EnsembleResults(None, "ensemble")

        self.assertEqual(results.ratio, 5)
        self.assertEqual(results.global_statistics.total_missed_probes, 2)
        self.assertEqual(results.global_statistics.total_missed_nonrare_probes, 1)
        self.assertFalse(results.coverage_ok)

    def test_late_zero_hit_probe_is_persisted_once(self):
        coverage_path, metadata_path = ("coverage",), ("metadata",)
        fdb_stub.directory = SimpleNamespace(
            create_or_open=lambda _, prefix: FakeDirectory(prefix)
        )
        hit = Coverage("hit.cpp", 1, "hit", False)
        existing_zero = Coverage("zero.cpp", 2, "zero", False)
        late_zero = Coverage("late.cpp", 3, "late", False)

        def key(cov):
            return coverage_path + (cov.file, cov.line, cov.comment, cov.rare)

        transaction = FakeTransaction(
            {metadata_path + ("initialized",): True, key(hit): 5, key(existing_zero): 0}
        )
        coverage = [(hit, True), (existing_zero, False), (late_zero, False)]

        initialized = harness_fdb.write_coverage_chunk(
            transaction, coverage_path, metadata_path, coverage, False
        )

        self.assertTrue(initialized)
        self.assertEqual(transaction.values[key(late_zero)], 0)
        self.assertEqual(transaction.mutations, [key(hit), key(late_zero)])
        transaction.mutations.clear()
        harness_fdb.write_coverage_chunk(
            transaction, coverage_path, metadata_path, coverage, initialized
        )
        self.assertEqual(transaction.mutations, [key(hit)])


if __name__ == "__main__":
    unittest.main()
