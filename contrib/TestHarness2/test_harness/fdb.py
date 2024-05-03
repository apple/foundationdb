from __future__ import annotations

from typing import OrderedDict, Tuple, List

import collections
import fdb
import fdb.tuple
import struct

from test_harness.run import StatFetcher, TestDescription
from test_harness.config import config
from test_harness.summarize import SummaryTree, Coverage

# Before increasing this, make sure that all Joshua clusters (at Apple and Snowflake) have been upgraded.
# This version needs to be changed if we either need newer features from FDB or the current API version is
# getting retired.
fdb.api_version(630)


def str_to_tuple(s: str | None):
    if s is None:
        return s
    return tuple(s.split(","))


fdb_db = None


def open_db(cluster_file: str | None):
    global fdb_db
    if fdb_db is None:
        fdb_db = fdb.open(cluster_file)
    return fdb_db


def chunkify(iterable, sz: int):
    res = []
    for item in iterable:
        res.append(item)
        if len(res) >= sz:
            yield res
            res = []
    if len(res) > 0:
        yield res


@fdb.transactional
def write_coverage_chunk(
    tr,
    path: Tuple[str, ...],
    metadata: Tuple[str, ...],
    coverage: List[Tuple[Coverage, bool]],
    initialized: bool,
) -> bool:
    cov_dir = fdb.directory.create_or_open(tr, path)
    if not initialized:
        metadata_dir = fdb.directory.create_or_open(tr, metadata)
        v = tr[metadata_dir["initialized"]]
        initialized = v.present()
    for cov, covered in coverage:
        if not initialized or covered:
            tr.add(
                cov_dir.pack((cov.file, cov.line, cov.comment, cov.rare)),
                struct.pack("<I", 1 if covered else 0),
            )
    return initialized


@fdb.transactional
def set_initialized(tr, metadata: Tuple[str, ...]):
    metadata_dir = fdb.directory.create_or_open(tr, metadata)
    tr[metadata_dir["initialized"]] = fdb.tuple.pack((True,))


def write_coverage(
    cluster_file: str | None,
    cov_path: Tuple[str, ...],
    metadata: Tuple[str, ...],
    coverage: OrderedDict[Coverage, bool],
):
    db = open_db(cluster_file)
    assert config.joshua_dir is not None
    initialized: bool = False
    for chunk in chunkify(coverage.items(), 100):
        initialized = write_coverage_chunk(db, cov_path, metadata, chunk, initialized)
    if not initialized:
        set_initialized(db, metadata)


@fdb.transactional
def _read_coverage(tr, cov_path: Tuple[str, ...]) -> OrderedDict[Coverage, int]:
    res = collections.OrderedDict()
    cov_dir = fdb.directory.create_or_open(tr, cov_path)
    for k, v in tr[cov_dir.range()]:
        file, line, comment, rare = cov_dir.unpack(k)
        count = struct.unpack("<I", v)[0]
        res[Coverage(file, line, comment, rare)] = count
    return res


def read_coverage(
    cluster_file: str | None, cov_path: Tuple[str, ...]
) -> OrderedDict[Coverage, int]:
    db = open_db(cluster_file)
    return _read_coverage(db, cov_path)


class TestStatistics:
    def __init__(self, runtime: int, run_count: int):
        self.runtime: int = runtime
        self.run_count: int = run_count


class Statistics:
    def __init__(self, cluster_file: str | None, joshua_dir: Tuple[str, ...]):
        self.db = open_db(cluster_file)
        self.stats_dir = self.open_stats_dir(self.db, joshua_dir)
        self.stats: OrderedDict[str, TestStatistics] = self.read_stats_from_db(self.db)

    @fdb.transactional
    def open_stats_dir(self, tr, app_dir: Tuple[str]):
        stats_dir = app_dir + ("runtime_stats",)
        return fdb.directory.create_or_open(tr, stats_dir)

    @fdb.transactional
    def read_stats_from_db(self, tr) -> OrderedDict[str, TestStatistics]:
        result = collections.OrderedDict()
        for k, v in tr[self.stats_dir.range()]:
            test_name = self.stats_dir.unpack(k)[0]
            runtime, run_count = struct.unpack("<II", v)
            result[test_name] = TestStatistics(runtime, run_count)
        return result

    @fdb.transactional
    def _write_runtime(self, tr, test_name: str, time: int) -> None:
        key = self.stats_dir.pack((test_name,))
        tr.add(key, struct.pack("<II", time, 1))

    def write_runtime(self, test_name: str, time: int) -> None:
        assert self.db is not None
        self._write_runtime(self.db, test_name, time)


class FDBStatFetcher(StatFetcher):
    def __init__(
        self,
        tests: OrderedDict[str, TestDescription],
        joshua_dir: Tuple[str] = str_to_tuple(config.joshua_dir),
    ):
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
