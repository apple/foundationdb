from __future__ import annotations

from typing import OrderedDict

import fdb
import struct

from run import StatFetcher, TestDescription


class FDBStatFetcher(StatFetcher):
    def __init__(self, cluster_file: str | None, app_dir: str, tests: OrderedDict[str, TestDescription]):
        super().__init__(tests)
        fdb.api_version(630)
        self.db: fdb.Database = fdb.open(cluster_file)
        self.stats_dir: fdb.DirectorySubspace = self.open_stats_dir(self.db, app_dir)

    @fdb.transactional
    def open_stats_dir(self, tr, app_dir: str) -> fdb.DirectorySubspace:
        app_dir_path = app_dir.split(',')
        app_dir_path.append('runtime_stats')
        return fdb.directory.create_or_open(tr, tuple(app_dir_path))

    @fdb.transactional
    def read_stats_from_db(self, tr):
        for k, v in tr[self.stats_dir.range()]:
            test_name = self.stats_dir.unpack(k)[0]
            if test_name in self.tests.keys():
                self.tests[test_name] = struct.unpack('<I', v)[0]

    def read_stats(self):
        self.read_stats_from_db(self.db)

    @fdb.transactional
    def write_runtime(self, tr: fdb.Transaction, test_name: str, time: int):
        tr.add(self.stats_dir.pack((test_name,)), struct.pack('<I', time))

    def add_run_time(self, test_name: str, runtime: int):
        self.write_runtime(test_name, runtime)
        super().add_run_time(test_name, runtime)
