from __future__ import annotations

from typing import OrderedDict, Tuple

import collections
import fdb
import struct

from test_harness.run import StatFetcher, TestDescription
from test_harness.config import config
from test_harness.summarize import SummaryTree

fdb.api_version(630)


def str_to_tuple(s: str | None):
    if s is None:
        return s
    res = s.split(',')
    return tuple(res)


class TestStatistics:
    def __init__(self, runtime: int, run_count: int):
        self.runtime: int = runtime
        self.run_count: int = run_count


class Statistics:
    def __init__(self, cluster_file: str | None, joshua_dir: Tuple[str, ...]):
        self.db: fdb.Database = fdb.open(cluster_file)
        self.stats_dir: fdb.DirectorySubspace = self.open_stats_dir(self.db, joshua_dir)
        self.stats: OrderedDict[str, TestStatistics] = self.read_stats_from_db(self.db)

    @fdb.transactional
    def open_stats_dir(self, tr, app_dir: Tuple[str]) -> fdb.DirectorySubspace:
        stats_dir = app_dir + ('runtime_stats',)
        return fdb.directory.create_or_open(tr, stats_dir)

    @fdb.transactional
    def read_stats_from_db(self, tr) -> OrderedDict[str, TestStatistics]:
        result = collections.OrderedDict()
        for k, v in tr[self.stats_dir.range()]:
            test_name = self.stats_dir.unpack(k)[0]
            runtime, run_count = struct.unpack('<II', v)
            result[test_name] = TestStatistics(runtime, run_count)
        return result

    @fdb.transactional
    def _write_runtime(self, tr: fdb.Transaction, test_name: str, time: int) -> None:
        key = self.stats_dir.pack((test_name,))
        tr.add(key, struct.pack('<II', time, 1))

    def write_runtime(self, test_name: str, time: int) -> None:
        self._write_runtime(self.db, test_name, time)


class FDBStatFetcher(StatFetcher):
    def __init__(self, tests: OrderedDict[str, TestDescription],
                 joshua_dir: Tuple[str] = str_to_tuple(config.joshua_dir)):
        super().__init__(tests)
        self.statistics = Statistics(config.cluster_file, joshua_dir)

    def read_stats(self):
        for k, v in self.statistics.stats.items():
            if k in self.tests.keys():
                self.tests[k].total_runtime = v.runtime
                self.tests[k].num_runs = v.run_count

    def add_run_time(self, test_name: str, runtime: int, out: SummaryTree):
        self.statistics.write_runtime(test_name, runtime)
        super().add_run_time(test_name, runtime, out)
