from __future__ import annotations

import array
import base64
import collections
import json
import os
import resource
import shutil
import subprocess
import random
import re
import sys
import threading
import time
import uuid

from test_harness import version
from test_harness.config import config
from typing import List, Pattern, Callable, OrderedDict
from pathlib import Path

from test_harness.summarize import Summary, SummaryTree


class TestDescription:
    def __init__(self, path: Path, name: str, priority: float):
        self.paths: List[Path] = [path]
        self.name = name
        self.priority = priority
        # we only measure in seconds. Otherwise, keeping determinism will be difficult
        self.total_runtime: int = 0


class StatFetcher:
    def __init__(self, tests: OrderedDict[str, TestDescription]):
        self.tests = tests

    def read_stats(self):
        pass

    def add_run_time(self, test_name: str, runtime: int):
        self.tests[test_name].total_runtime += runtime


class FileStatsFetcher(StatFetcher):
    def __init__(self, stat_file: str, tests: OrderedDict[str, TestDescription]):
        super().__init__(tests)
        self.stat_file = stat_file
        self.last_state = {}

    def read_stats(self):
        p = Path(self.stat_file)
        if not p.exists() or not p.is_file():
            return
        with p.open('r') as f:
            self.last_state = json.load(f)
            for k, v in self.last_state.items():
                if k in self.tests:
                    self.tests[k].total_runtime = v

    def add_run_time(self, test_name: str, runtime: int):
        super().add_run_time(test_name, runtime)
        if test_name not in self.last_state:
            self.last_state[test_name] = 0
        self.last_state[test_name] += runtime
        with Path(self.stat_file).open('w') as f:
            json.dump(self.last_state, f)


StatFetcherCreator = Callable[[OrderedDict[str, TestDescription]], StatFetcher]


class TestPicker:
    def __init__(self, test_dir: Path, fetcher: StatFetcherCreator):
        if not test_dir.exists():
            raise RuntimeError('{} is neither a directory nor a file'.format(test_dir))
        self.test_dir: Path = test_dir
        self.tests: OrderedDict[str, TestDescription] = collections.OrderedDict()
        self.restart_test: Pattern = re.compile(r".*-\d+\.(txt|toml)")
        self.follow_test: Pattern = re.compile(r".*-[2-9]\d*\.(txt|toml)")

        for subdir in self.test_dir.iterdir():
            if subdir.is_dir() and subdir.name in config.TEST_DIRS:
                self.walk_test_dir(subdir)
        self.fetcher = fetcher(self.tests)

    def add_time(self, test_file: Path, run_time: int) -> None:
        # getting the test name is fairly inefficient. But since we only have 100s of tests, I won't bother
        test_name = None
        for name, test in self.tests.items():
            for p in test.paths:
                if p.absolute() == test_file.absolute():
                    test_name = name
                    break
            if test_name is not None:
                break
        assert test_name is not None
        self.fetcher.add_run_time(test_name, run_time)

    def dump_stats(self) -> str:
        res = array.array('I')
        for _, spec in self.tests.items():
            res.append(spec.total_runtime)
        return base64.standard_b64encode(res.tobytes()).decode('utf-8')

    def fetch_stats(self):
        self.fetcher.read_stats()

    def load_stats(self, serialized: str):
        times = array.array('I')
        times.frombytes(base64.standard_b64decode(serialized))
        assert len(times) == len(self.tests.items())
        idx = 0
        for _, spec in self.tests.items():
            spec.total_runtime = times[idx]
            idx += 1

    def parse_txt(self, path: Path):
        with path.open('r') as f:
            test_name: str | None = None
            test_class: str | None = None
            priority: float | None = None
            for line in f:
                line = line.strip()
                kv = line.split('=')
                if len(kv) != 2:
                    continue
                kv[0] = kv[0].strip()
                kv[1] = kv[1].strip(' \r\n\t\'"')
                if kv[0] == 'testTitle' and test_name is None:
                    test_name = kv[1]
                if kv[0] == 'testClass' and test_class is None:
                    test_class = kv[1]
                if kv[0] == 'testPriority' and priority is None:
                    try:
                        priority = float(kv[1])
                    except ValueError:
                        pass
                if test_name is not None and test_class is not None and priority is not None:
                    break
            if test_name is None:
                return
            if test_class is None:
                test_class = test_name
            if priority is None:
                priority = 1.0
            if test_class not in self.tests:
                self.tests[test_class] = TestDescription(path, test_class, priority)
            else:
                self.tests[test_class].paths.append(path)

    def walk_test_dir(self, test: Path):
        if test.is_dir():
            for file in test.iterdir():
                self.walk_test_dir(file)
        else:
            # check whether we're looking at a restart test
            if self.follow_test.match(test.name) is not None:
                return
            if test.suffix == '.txt' or test.suffix == '.toml':
                self.parse_txt(test)

    @staticmethod
    def list_restart_files(start_file: Path) -> List[Path]:
        name = re.sub(r'-\d+.(txt|toml)', '', start_file.name)
        res: List[Path] = []
        for test_file in start_file.parent.iterdir():
            if test_file.name.startswith(name):
                res.append(test_file)
        assert len(res) > 1
        res.sort()
        return res

    def choose_test(self) -> List[Path]:
        min_runtime: float | None = None
        candidates: List[TestDescription] = []
        for _, v in self.tests.items():
            this_time = v.total_runtime * v.priority
            if min_runtime is None:
                min_runtime = this_time
            if this_time < min_runtime:
                min_runtime = this_time
                candidates = [v]
            elif this_time == min_runtime:
                candidates.append(v)
        choice = random.randint(0, len(candidates) - 1)
        test = candidates[choice]
        result = test.paths[random.randint(0, len(test.paths) - 1)]
        if self.restart_test.match(result.name):
            return self.list_restart_files(result)
        else:
            return [result]


class OldBinaries:
    def __init__(self):
        self.first_file_expr = re.compile(r'.*-1\.(txt|toml)')
        self.old_binaries_path: Path = config.OLD_BINARIES_PATH
        self.binaries: OrderedDict[version.Version, Path] = collections.OrderedDict()
        if not self.old_binaries_path.exists() or not self.old_binaries_path.is_dir():
            return
        exec_pattern = re.compile(r'fdbserver-\d+\.\d+\.\d+(\.exe)?')
        for file in self.old_binaries_path.iterdir():
            if not file.is_file() or not os.access(file, os.X_OK):
                continue
            if exec_pattern.fullmatch(file.name) is not None:
                self._add_file(file)

    def _add_file(self, file: Path):
        version_str = file.name.split('-')[1]
        if version_str.endswith('.exe'):
            version_str = version_str[0:-len('.exe')]
        self.binaries[version.Version.parse(version_str)] = file

    def choose_binary(self, test_file: Path) -> Path:
        if len(self.binaries) == 0:
            return config.BINARY
        max_version = version.Version.max_version()
        min_version = version.Version.parse('5.0.0')
        dirs = test_file.parent.parts
        if 'restarting' not in dirs:
            return config.BINARY
        version_expr = dirs[-1].split('_')
        first_file = self.first_file_expr.match(test_file.name) is not None
        if first_file and version_expr[0] == 'to':
            # downgrade test -- first binary should be current one
            return config.BINARY
        if not first_file and version_expr[0] == 'from':
            # upgrade test -- we only return an old version for the first test file
            return config.BINARY
        if version_expr[0] == 'from' or version_expr[0] == 'to':
            min_version = version.Version.parse(version_expr[1])
        if len(version_expr) == 4 and version_expr[2] == 'until':
            max_version = version.Version.parse(version_expr[3])
        candidates: List[Path] = []
        for ver, binary in self.binaries.items():
            if min_version <= ver <= max_version:
                candidates.append(binary)
        return random.choice(candidates)


def is_restarting_test(test_file: Path):
    for p in test_file.parts:
        if p == 'restarting':
            return True
    return False


def is_no_sim(test_file: Path):
    return test_file.parts[-2] == 'noSim'


class ResourceMonitor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.start_time = time.time()
        self.end_time: float | None = None
        self._stop_monitor = False
        self.max_rss = 0

    def run(self) -> None:
        while not self._stop_monitor:
            time.sleep(1)
            resources = resource.getrusage(resource.RUSAGE_CHILDREN)
            self.max_rss = max(resources.ru_maxrss, self.max_rss)

    def stop(self):
        self.end_time = time.time()
        self._stop_monitor = True

    def time(self):
        return self.end_time - self.start_time


class TestRun:
    def __init__(self, binary: Path, test_file: Path, random_seed: int, uid: uuid.UUID,
                 restarting: bool = False, test_determinism: bool = False,
                 stats: str | None = None, expected_unseed: int | None = None):
        self.binary = binary
        self.test_file = test_file
        self.random_seed = random_seed
        self.uid = uid
        self.restarting = restarting
        self.test_determinism = test_determinism
        self.stats: str | None = stats
        self.expected_unseed: int | None = expected_unseed
        self.err_out: str = 'error.xml'
        self.use_valgrind: bool = config.USE_VALGRIND
        self.old_binary_path: str = config.OLD_BINARIES_PATH
        self.buggify_enabled: bool = random.random() < config.BUGGIFY_ON_RATIO
        self.fault_injection_enabled: bool = True
        self.trace_format = config.TRACE_FORMAT
        # state for the run
        self.retryable_error: bool = False
        self.summary: Summary | None = None
        self.run_time: int = 0

    def log_test_plan(self, out: SummaryTree):
        test_plan: SummaryTree = SummaryTree('TestPlan')
        test_plan.attributes['TestUID'] = str(uuid)
        test_plan.attributes['RandomSeed'] = str(self.random_seed)
        test_plan.attributes['TestFile'] = str(self.test_file)
        test_plan.attributes['Buggify'] = '1' if self.buggify_enabled else '0'
        test_plan.attributes['FaultInjectionEnabled'] = '1' if self.fault_injection_enabled else '0'
        test_plan.attributes['DeterminismCheck'] = '1' if self.test_determinism else '0'
        out.append(test_plan)

    def run(self):
        command: List[str] = []
        valgrind_file: Path | None = None
        if self.use_valgrind:
            command.append('valgrind')
            valgrind_file = Path('valgrind-{}.xml'.format(self.random_seed))
            dbg_path = os.getenv('FDB_VALGRIND_DBGPATH')
            if dbg_path is not None:
                command.append('--extra-debuginfo-path={}'.format(dbg_path))
            command += ['--xml=yes', '--xml-file={}'.format(valgrind_file), '-q']
        command += [str(self.binary.absolute()),
                    '-r', 'test' if is_no_sim(self.test_file) else 'simulation',
                    '-f', str(self.test_file),
                    '-s', str(self.random_seed),
                    '-fi', 'on' if self.fault_injection_enabled else 'off',
                    '--trace_format', self.trace_format]
        if self.restarting:
            command.append('--restarting')
        if self.buggify_enabled:
            command += ['-b', 'on']
        if config.CRASH_ON_ERROR:
            command.append('--crash')
        temp_path = config.RUN_DIR / str(self.uid)
        temp_path.mkdir(parents=True, exist_ok=True)

        # self.log_test_plan(out)
        resources = ResourceMonitor()
        resources.start()
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=temp_path)
        did_kill = False
        try:
            process.wait(20 * config.KILL_SECONDS if self.use_valgrind else config.KILL_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            did_kill = True
        resources.stop()
        resources.join()
        self.run_time = round(resources.time())
        self.summary = Summary(self.binary, temp_path, runtime=resources.time(), max_rss=resources.max_rss,
                               was_killed=did_kill, uid=self.uid, stats=self.stats, valgrind_out_file=valgrind_file,
                               expected_unseed=self.expected_unseed)
        return self.summary.ok()


class TestRunner:
    def __init__(self):
        self.uid = uuid.uuid4()
        self.test_path: str = 'tests'
        self.cluster_file: str | None = None
        self.fdb_app_dir: str | None = None
        self.stat_fetcher: StatFetcherCreator = \
            lambda x: FileStatsFetcher(os.getenv('JOSHUA_STAT_FILE', 'stats.json'), x)
        self.binary_chooser = OldBinaries()

    def fetch_stats_from_fdb(self, cluster_file: str, app_dir: str):
        def fdb_fetcher(tests: OrderedDict[str, TestDescription]):
            from . import fdb
            self.stat_fetcher = fdb.FDBStatFetcher(cluster_file, app_dir, tests)

        self.stat_fetcher = fdb_fetcher

    def backup_sim_dir(self, seed: int):
        temp_dir = config.RUN_DIR / str(self.uid)
        src_dir = temp_dir / 'simfdb'
        assert src_dir.is_dir()
        dest_dir = temp_dir / 'simfdb.{}'.format(seed)
        assert not dest_dir.exists()
        shutil.copytree(src_dir, dest_dir)

    def restore_sim_dir(self, seed: int):
        temp_dir = config.RUN_DIR / str(self.uid)
        src_dir = temp_dir / 'simfdb.{}'.format(seed)
        assert src_dir.exists()
        dest_dir = temp_dir / 'simfdb'
        shutil.rmtree(dest_dir)
        shutil.move(src_dir, dest_dir)

    def run_tests(self, test_files: List[Path], seed: int, test_picker: TestPicker) -> bool:
        count = 0
        for file in test_files:
            binary = self.binary_chooser.choose_binary(file)
            unseed_check = random.random() < config.UNSEED_CHECK_RATIO
            if unseed_check and count != 0:
                # for restarting tests we will need to restore the sim2 after the first run
                self.backup_sim_dir(seed + count - 1)
            run = TestRun(binary, file.absolute(), seed + count, self.uid, restarting=count != 0,
                          stats=test_picker.dump_stats())
            success = run.run()
            test_picker.add_time(file, run.run_time)
            if success and unseed_check and run.summary.unseed is not None:
                self.restore_sim_dir(seed + count - 1)
                run2 = TestRun(binary, file.absolute(), seed + count, self.uid, restarting=count != 0,
                               stats=test_picker.dump_stats(), expected_unseed=run.summary.unseed)
                success = run2.run()
                test_picker.add_time(file, run2.run_time)
                run2.summary.out.dump(sys.stdout)
            run.summary.out.dump(sys.stdout)
            if not success:
                return False
            count += 1
        return True

    def run(self, stats: str | None) -> bool:
        seed = random.randint(0, 2 ** 32 - 1)
        # unseed_check: bool = random.random() < Config.UNSEED_CHECK_RATIO
        test_picker = TestPicker(Path(self.test_path), self.stat_fetcher)
        if stats is not None:
            test_picker.load_stats(stats)
        else:
            test_picker.fetch_stats()
        test_files = test_picker.choose_test()
        success = self.run_tests(test_files, seed, test_picker)
        if config.CLEAN_UP:
            shutil.rmtree(config.RUN_DIR / str(self.uid))
        return success
