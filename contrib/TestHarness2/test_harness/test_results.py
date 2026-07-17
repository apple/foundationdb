import collections
import importlib
import sys
import types
import unittest
from types import SimpleNamespace
from unittest import mock

from test_harness.config import config
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
EnsembleResults = importlib.import_module("test_harness.results").EnsembleResults


class EnsembleResultsTest(unittest.TestCase):
    def test_coverage_threshold_uses_total_test_runs(self):
        statistics = SimpleNamespace(
            stats=collections.OrderedDict(
                (
                    ("fast", SimpleNamespace(runtime=20, run_count=60000)),
                    ("slow", SimpleNamespace(runtime=10, run_count=40000)),
                )
            )
        )
        coverage = collections.OrderedDict(
            (
                (Coverage("fdbserver/a.cpp", 10, "nonrare", False), 5),
                (Coverage("fdbserver/b.cpp", 20, "rare", True), 4),
                (Coverage("fdbserver/c.cpp", 30, "hit", False), 6),
            )
        )
        with mock.patch.object(
            harness_fdb, "Statistics", return_value=statistics
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

        self.assertEqual(results.global_statistics.total_test_runs, 100000)
        self.assertEqual(results.global_statistics.total_cpu_time, 30)
        self.assertEqual(results.ratio, 5)
        self.assertEqual(results.global_statistics.total_missed_probes, 2)
        self.assertEqual(results.global_statistics.total_missed_nonrare_probes, 1)
        self.assertEqual(results.min_coverage_hit, 4)
        self.assertFalse(results.coverage_ok)


if __name__ == "__main__":
    unittest.main()
