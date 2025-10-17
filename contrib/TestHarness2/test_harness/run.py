from __future__ import annotations

import array
import base64
import collections
import math
import os
import resource
import shutil
import subprocess
import re
import sys
import threading
import time
import uuid

from functools import total_ordering
from pathlib import Path
from test_harness.version import Version
from test_harness.config import config, BuggifyOptionValue
from typing import Dict, List, Pattern, OrderedDict

from test_harness.summarize import Summary, SummaryTree


@total_ordering
class TestDescription:
    def __init__(self, path: Path, name: str, priority: float):
        self.paths: List[Path] = [path]
        self.name = name
        self.priority: float = priority
        # we only measure in seconds. Otherwise, keeping determinism will be difficult
        self.total_runtime: int = 0
        self.num_runs: int = 0

    def __lt__(self, other):
        if isinstance(other, TestDescription):
            return self.name < other.name
        else:
            return self.name < str(other)

    def __eq__(self, other):
        if isinstance(other, TestDescription):
            return self.name < other.name
        else:
            return self.name < str(other.name)


class StatFetcher:
    def __init__(self, tests: OrderedDict[str, TestDescription]):
        self.tests = tests

    def read_stats(self):
        pass

    def add_run_time(self, test_name: str, runtime: int, out: SummaryTree):
        self.tests[test_name].total_runtime += runtime


class TestPicker:
    def __init__(self, test_dir: Path, binaries: OrderedDict[Version, Path]):
        if not test_dir.exists():
            raise RuntimeError("{} is neither a directory nor a file".format(test_dir))
        self.include_files_regex = re.compile(config.include_test_files)
        self.exclude_files_regex = re.compile(config.exclude_test_files)
        self.include_tests_regex = re.compile(config.include_test_classes)
        self.exclude_tests_regex = re.compile(config.exclude_test_names)
        self.test_dir: Path = test_dir
        self.tests: OrderedDict[str, TestDescription] = collections.OrderedDict()
        self.restart_test: Pattern = re.compile(r".*-\d+\.(txt|toml)")
        self.follow_test: Pattern = re.compile(r".*-[2-9]\d*\.(txt|toml)")
        self.old_binaries: OrderedDict[Version, Path] = binaries
        self.rare_priority: int = int(os.getenv("RARE_PRIORITY", 10))

        for subdir in self.test_dir.iterdir():
            if subdir.is_dir() and subdir.name in config.test_types_to_run:
                self.walk_test_dir(subdir)
        self.stat_fetcher: StatFetcher
        if config.stats is not None or config.joshua_dir is None:
            self.stat_fetcher = StatFetcher(self.tests)
        else:
            from test_harness.fdb import FDBStatFetcher

            self.stat_fetcher = FDBStatFetcher(self.tests)
        if config.stats is not None:
            self.load_stats(config.stats)
        else:
            self.fetch_stats()

        if not self.tests:
            raise Exception(
                "No tests to run! Please check if tests are included/excluded incorrectly or old binaries are missing for restarting tests"
            )

    def add_time(self, test_file: Path, run_time: int, out: SummaryTree) -> None:
        # getting the test name is fairly inefficient. But since we only have 100s of tests, I won't bother
        test_name: str | None = None
        test_desc: TestDescription | None = None
        for name, test in self.tests.items():
            for p in test.paths:
                test_files: List[Path]
                if self.restart_test.match(p.name):
                    test_files = self.list_restart_files(p)
                else:
                    test_files = [p]
                for file in test_files:
                    if file.absolute() == test_file.absolute():
                        test_name = name
                        test_desc = test
                        break
                if test_name is not None:
                    break
            if test_name is not None:
                break
        assert test_name is not None and test_desc is not None
        self.stat_fetcher.add_run_time(test_name, run_time, out)
        out.attributes["TotalTestTime"] = str(test_desc.total_runtime)
        out.attributes["TestRunCount"] = str(test_desc.num_runs)

    def dump_stats(self) -> str:
        res = array.array("I")
        for _, spec in self.tests.items():
            res.append(spec.total_runtime)
        return base64.standard_b64encode(res.tobytes()).decode("utf-8")

    def fetch_stats(self):
        self.stat_fetcher.read_stats()

    def load_stats(self, serialized: str):
        times = array.array("I")
        times.frombytes(base64.standard_b64decode(serialized))
        assert len(times) == len(self.tests.items())
        for idx, (_, spec) in enumerate(self.tests.items()):
            spec.total_runtime = times[idx]

    def parse_txt(self, path: Path):
        if (
            self.include_files_regex.search(str(path)) is None
            or self.exclude_files_regex.search(str(path)) is not None
        ):
            return
        # Skip restarting tests that do not have old binaries in the given version range
        # In particular, this is only for restarting tests with the "until" keyword,
        # since without "until", it will at least run with the current binary.
        if is_restarting_test(path):
            candidates: List[Path] = []
            dirs = path.parent.parts
            version_expr = dirs[-1].split("_")
            if (
                (version_expr[0] == "from" or version_expr[0] == "to")
                and len(version_expr) == 4
                and version_expr[2] == "until"
            ):
                max_version = Version.parse(version_expr[3])
                min_version = Version.parse(version_expr[1])
                for ver, binary in self.old_binaries.items():
                    if min_version <= ver < max_version:
                        candidates.append(binary)
                if not len(candidates):
                    # No valid old binary found
                    return

        with path.open("r") as f:
            test_name: str | None = None
            test_class: str | None = None
            priority: float | None = None
            for line in f:
                line = line.strip()
                kv = line.split("=")
                if len(kv) != 2:
                    continue
                kv[0] = kv[0].strip()
                kv[1] = kv[1].strip(" \r\n\t'\"")
                if kv[0] == "testTitle" and test_name is None:
                    test_name = kv[1]
                if kv[0] == "testClass" and test_class is None:
                    test_class = kv[1]
                if kv[0] == "testPriority" and priority is None:
                    try:
                        priority = float(kv[1])
                    except ValueError:
                        raise RuntimeError(
                            "Can't parse {} -- testPriority in {} should be set to a float".format(
                                kv[1], path
                            )
                        )
                if (
                    test_name is not None
                    and test_class is not None
                    and priority is not None
                ):
                    break
            if test_name is None:
                return
            if test_class is None:
                test_class = test_name
            if priority is None:
                priority = 1.0
            if is_rare(path) and priority <= 1.0:
                priority = self.rare_priority
            if (
                self.include_tests_regex.search(test_class) is None
                or self.exclude_tests_regex.search(test_class) is not None
            ):
                return
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
            if test.suffix == ".txt" or test.suffix == ".toml":
                self.parse_txt(test)

    @staticmethod
    def list_restart_files(start_file: Path) -> List[Path]:
        name = re.sub(r"-\d+.(txt|toml)", "", start_file.name)
        res: List[Path] = []
        for test_file in start_file.parent.iterdir():
            if test_file.name.startswith(name):
                res.append(test_file)
        assert len(res) > 1
        res.sort()
        return res

    def choose_test(self) -> List[Path]:
        candidates: List[TestDescription] = []

        if config.random.random() < 0.99:
            # 99% of the time, select a test with the least runtime
            min_runtime: float | None = None
            for _, v in self.tests.items():
                this_time = v.total_runtime * v.priority
                if min_runtime is None or this_time < min_runtime:
                    min_runtime = this_time
                    candidates = [v]
                elif this_time == min_runtime:
                    candidates.append(v)
        else:
            # 1% of the time, select the test with the fewest runs, rather than the test
            # with the least runtime. This is to improve coverage for long-running tests
            min_runs: int | None = None
            for _, v in self.tests.items():
                if min_runs is None or v.num_runs < min_runs:
                    min_runs = v.num_runs
                    candidates = [v]
                elif v.num_runs == min_runs:
                    candidates.append(v)

        candidates.sort()
        choice = config.random.randint(0, len(candidates) - 1)
        test = candidates[choice]
        result = test.paths[config.random.randint(0, len(test.paths) - 1)]
        if self.restart_test.match(result.name):
            return self.list_restart_files(result)
        else:
            return [result]


class OldBinaries:
    def __init__(self):
        self.first_file_expr = re.compile(r".*-1\.(txt|toml)")
        self.old_binaries_path: Path = config.old_binaries_path
        self.binaries: OrderedDict[Version, Path] = collections.OrderedDict()
        if not self.old_binaries_path.exists() or not self.old_binaries_path.is_dir():
            return
        exec_pattern = re.compile(r"fdbserver-\d+\.\d+\.\d+(\.exe)?")
        for file in self.old_binaries_path.iterdir():
            if not file.is_file() or not os.access(file, os.X_OK):
                continue
            if exec_pattern.fullmatch(file.name) is not None:
                self._add_file(file)

    def _add_file(self, file: Path):
        version_str = file.name.split("-")[1]
        if version_str.endswith(".exe"):
            version_str = version_str[0 : -len(".exe")]
        ver = Version.parse(version_str)
        self.binaries[ver] = file

    def choose_binary(self, test_file: Path) -> Path:
        if len(self.binaries) == 0:
            return config.binary
        max_version = Version.max_version()
        min_version = Version.parse("5.0.0")
        dirs = test_file.parent.parts
        if "restarting" not in dirs:
            return config.binary
        version_expr = dirs[-1].split("_")
        first_file = self.first_file_expr.match(test_file.name) is not None
        if first_file and version_expr[0] == "to":
            # downgrade test -- first binary should be current one
            return config.binary
        if not first_file and version_expr[0] == "from":
            # upgrade test -- we only return an old version for the first test file
            return config.binary
        if version_expr[0] == "from" or version_expr[0] == "to":
            min_version = Version.parse(version_expr[1])
        if len(version_expr) == 4 and version_expr[2] == "until":
            max_version = Version.parse(version_expr[3])
        candidates: List[Path] = []
        for ver, binary in self.binaries.items():
            if min_version <= ver < max_version:
                candidates.append(binary)
        if len(candidates) == 0:
            return config.binary
        return config.random.choice(candidates)


def is_restarting_test(test_file: Path):
    for p in test_file.parts:
        if p == "restarting":
            return True
    return False


def is_negative(test_file: Path):
    return test_file.parts[-2] == "negative"


def is_no_sim(test_file: Path):
    return test_file.parts[-2] == "noSim"


def is_rare(test_file: Path):
	return test_file.parts[-2] == "rare"

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
    def __init__(
        self,
        binary: Path,
        test_file: Path,
        random_seed: int,
        uid: uuid.UUID,
        restarting: bool = False,
        test_determinism: bool = False,
        buggify_enabled: bool = False,
        stats: str | None = None,
        expected_unseed: int | None = None,
        will_restart: bool = False,
    ):
        self.binary = binary
        self.test_file = test_file
        self.random_seed = random_seed
        self.uid = uid
        self.restarting = restarting
        self.test_determinism = test_determinism
        self.stats: str | None = stats
        self.expected_unseed: int | None = expected_unseed
        self.use_valgrind: bool = config.use_valgrind
        self.old_binary_path: Path = config.old_binaries_path
        self.buggify_enabled: bool = buggify_enabled
        self.fault_injection_enabled: bool = True
        self.trace_format: str | None = config.trace_format
        if Version.of_binary(self.binary) < "6.1.0":
            self.trace_format = None
        self.use_tls_plugin = Version.of_binary(self.binary) < "5.2.0"
        self.temp_path = config.run_temp_dir / str(self.uid)
        # state for the run
        self.retryable_error: bool = False
        self.summary: Summary = Summary(
            binary,
            uid=self.uid,
            stats=self.stats,
            expected_unseed=self.expected_unseed,
            will_restart=will_restart,
            long_running=config.long_running,
        )
        self.run_time: int = 0
        self.success = self.run()

    def log_test_plan(self, out: SummaryTree):
        test_plan: SummaryTree = SummaryTree("TestPlan")
        test_plan.attributes["TestUID"] = str(self.uid)
        test_plan.attributes["RandomSeed"] = str(self.random_seed)
        test_plan.attributes["TestFile"] = str(self.test_file)
        test_plan.attributes["Buggify"] = "1" if self.buggify_enabled else "0"
        test_plan.attributes["FaultInjectionEnabled"] = (
            "1" if self.fault_injection_enabled else "0"
        )
        test_plan.attributes["DeterminismCheck"] = "1" if self.test_determinism else "0"
        out.append(test_plan)

    def delete_simdir(self):
        shutil.rmtree(self.temp_path / Path("simfdb"))

    def _run_joshua_logtool(self):
        """Calls Joshua LogTool to upload the test logs if 1) test failed 2) test is RocksDB related"""
        # Look for joshua_logtool.py in multiple locations
        # First try: relative to this script's location (contrib/TestHarness2/test_harness/run.py)
        script_dir = Path(__file__).parent.parent.parent  # Go up 3 levels to repo root
        joshua_logtool_script = script_dir / "contrib" / "joshua_logtool.py"

        if not joshua_logtool_script.exists():
            # Second try: contrib directory relative to current working directory
            joshua_logtool_script = Path("contrib/joshua_logtool.py")
            if not joshua_logtool_script.exists():
                # Third try: current directory for backwards compatibility
                joshua_logtool_script = Path("joshua_logtool.py")
                if not joshua_logtool_script.exists():
                    return {"stdout": "", "stderr": "joshua_logtool.py not found", "exit_code": -1, "tool_skipped": True}

        # Debug: Print paths to understand directory structure
        print(f"DEBUG: self.temp_path = {self.temp_path}", file=sys.stderr)
        print(f"DEBUG: self.temp_path.parent = {self.temp_path.parent}", file=sys.stderr)
        print(f"DEBUG: self.temp_path.parent.parent = {self.temp_path.parent.parent}", file=sys.stderr)
        
        # The app logs (app_log.txt, python_app_stdout.log, python_app_stderr.log) are in the test-specific directory
        # which is self.temp_path.parent, not the top-level directory
        test_specific_dir = self.temp_path.parent
        print(f"DEBUG: test_specific_dir (self.temp_path.parent) = {test_specific_dir}", file=sys.stderr)
        
        command = [
            sys.executable,
            str(joshua_logtool_script),
            "upload",
            "--test-uid", str(self.uid),
            "--log-directory", str(test_specific_dir)
        ]

        # Note: joshua_logtool now uploads all tests by default when enabled
        # No RocksDB filtering is applied unless explicitly requested
        result = subprocess.run(command, capture_output=True, text=True)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.returncode,
            "tool_skipped": False,
        }

    def run(self):
        command: List[str] = []
        env: Dict[str, str] = os.environ.copy()
        valgrind_file: Path | None = None
        if self.use_valgrind and self.binary == config.binary:
            # Only run the binary under test under valgrind. There's nothing we
            # can do about valgrind errors in old binaries anyway, and it makes
            # the test take longer. Also old binaries weren't built with
            # USE_VALGRIND=ON, and we have seen false positives with valgrind in
            # such binaries.
            command.append("valgrind")
            valgrind_file = self.temp_path / Path(
                "valgrind-{}.xml".format(self.random_seed)
            )
            dbg_path = os.getenv("FDB_VALGRIND_DBGPATH")
            if dbg_path is not None:
                command.append("--extra-debuginfo-path={}".format(dbg_path))
            command += [
                "--xml=yes",
                "--xml-file={}".format(valgrind_file.absolute()),
                "-q",
            ]
        command += [
            str(self.binary.absolute()),
            "-r",
            "test" if is_no_sim(self.test_file) else "simulation",
            "-f",
            str(self.test_file),
            "-s",
            str(self.random_seed),
        ]
        if self.trace_format is not None:
            command += ["--trace_format", self.trace_format]
        if self.use_tls_plugin:
            command += ["--tls_plugin", str(config.tls_plugin_path)]
            env["FDB_TLS_PLUGIN"] = str(config.tls_plugin_path)
        if config.disable_kaio:
            command += ["--knob-disable-posix-kernel-aio=1"]
        if Version.of_binary(self.binary) >= "7.1.0":
            command += ["-fi", "on" if self.fault_injection_enabled else "off"]
        if self.restarting:
            command.append("--restarting")
        if self.buggify_enabled:
            command += ["-b", "on"]
        if config.crash_on_error and not is_negative(self.test_file):
            command.append("--crash")
        if config.long_running:
            # disable simulation speedup
            command += ["--knob-sim-speedup-after-seconds=36000"]
            # disable traceTooManyLines Error MAX_TRACE_LINES
            command += ["--knob-max-trace-lines=1000000000"]

        self.temp_path.mkdir(parents=True, exist_ok=True)

        # self.log_test_plan(out)
        resources = ResourceMonitor()
        resources.start()
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            cwd=self.temp_path,
            text=True,
            env=env,
        )
        did_kill = False
        # No timeout for long running tests
        timeout = (
            20 * config.kill_seconds
            if self.use_valgrind
            else (None if config.long_running else config.kill_seconds)
        )
        err_out: str
        try:
            _, err_out = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            _, err_out = process.communicate()
            did_kill = True
        resources.stop()
        resources.join()
        # we're rounding times up, otherwise we will prefer running very short tests (<1s)
        self.run_time = math.ceil(resources.time())
        self.summary.is_negative_test = is_negative(self.test_file)
        self.summary.runtime = resources.time()
        self.summary.max_rss = resources.max_rss
        self.summary.was_killed = did_kill
        self.summary.valgrind_out_file = valgrind_file
        self.summary.error_out = err_out
        self.summary.summarize(self.temp_path, " ".join(command))
        # Note: joshua_logtool is called after determinism analysis file organization
        # in the run_tests method to ensure files are properly organized

        return self.summary.ok()


def decorate_summary(out: SummaryTree, test_file: Path, seed: int, buggify: bool):
    """Sometimes a test can crash before ProgramStart is written to the traces. These
    tests are then hard to reproduce (they can be reproduced through TestHarness but
    require the user to run in the joshua docker container). To account for this we
    will write the necessary information into the attributes if it is missing."""
    if "TestFile" not in out.attributes:
        out.attributes["TestFile"] = str(test_file)
    if "RandomSeed" not in out.attributes:
        out.attributes["RandomSeed"] = str(seed)
    if "BuggifyEnabled" not in out.attributes:
        out.attributes["BuggifyEnabled"] = "1" if buggify else "0"


class TestRunner:
    def __init__(self):
        self.uid = uuid.uuid4()
        self.test_path: Path = config.test_source_dir
        self.cluster_file: str | None = None
        self.fdb_app_dir: str | None = None
        self.binary_chooser = OldBinaries()
        self.test_picker = TestPicker(self.test_path, self.binary_chooser.binaries)



    def backup_sim_dir(self, seed: int):
        temp_dir = config.run_temp_dir / str(self.uid)
        src_dir = temp_dir / "simfdb"
        assert src_dir.is_dir()
        dest_dir = temp_dir / "simfdb.{}".format(seed)
        assert not dest_dir.exists()
        shutil.copytree(src_dir, dest_dir)

    def restore_sim_dir(self, seed: int):
        temp_dir = config.run_temp_dir / str(self.uid)
        src_dir = temp_dir / "simfdb.{}".format(seed)
        assert src_dir.exists()
        dest_dir = temp_dir / "simfdb"
        shutil.rmtree(dest_dir)
        shutil.move(src_dir, dest_dir)


    def backup_trace_files(self, seed: int):
        """Backup trace files before determinism check to preserve initial run traces."""
        temp_dir = config.run_temp_dir / str(self.uid)
        trace_files = list(temp_dir.glob("trace.*.json"))
        
        if not trace_files:
            return
            
        # Create backup directory for initial run traces
        backup_dir = temp_dir / f"trace_backup_{seed}"
        backup_dir.mkdir(exist_ok=True)
        
        # Move trace files to backup (not copy) to clear the main directory
        for trace_file in trace_files:
            shutil.move(trace_file, backup_dir / trace_file.name)
            
        print(f"Moved {len(trace_files)} trace files to backup {backup_dir}", file=sys.stderr)

    def restore_trace_files(self, seed: int):
        """Restore trace files after determinism check for analysis."""
        temp_dir = config.run_temp_dir / str(self.uid)
        backup_dir = temp_dir / f"trace_backup_{seed}"
        
        if not backup_dir.exists():
            return
            
        # Create analysis directory structure
        analysis_dir = temp_dir / f"determinism_analysis_{self.uid}"
        analysis_dir.mkdir(exist_ok=True)
        
        initial_run_dir = analysis_dir / "initial_run"
        determinism_check_dir = analysis_dir / "determinism_check"
        
        initial_run_dir.mkdir(exist_ok=True)
        determinism_check_dir.mkdir(exist_ok=True)
        
        # Get initial trace names before moving them
        initial_trace_names = {f.name for f in backup_dir.glob("trace.*.json")}
        
        # Move backed up traces to initial_run directory
        for trace_file in backup_dir.glob("trace.*.json"):
            shutil.move(trace_file, initial_run_dir / trace_file.name)
        
        # Move ONLY the determinism check trace files to determinism_check directory
        # These are the NEW files from the determinism check run
        current_traces = list(temp_dir.glob("trace.*.json"))
        
        for trace_file in current_traces:
            # Only move files that are NOT duplicates of the initial run
            if trace_file.name not in initial_trace_names:
                shutil.move(trace_file, determinism_check_dir / trace_file.name)
            else:
                # This is a duplicate of an initial run file - check if it's identical
                initial_file = initial_run_dir / trace_file.name
                if initial_file.exists():
                    # Compare file contents
                    if trace_file.read_bytes() == initial_file.read_bytes():
                        # Identical file - remove the duplicate
                        trace_file.unlink()
                        print(f"Removed identical duplicate trace file: {trace_file.name}", file=sys.stderr)
                    else:
                        # Same name but different content - keep it with a new name
                        new_name = f"determinism_check_{trace_file.name}"
                        shutil.move(trace_file, determinism_check_dir / new_name)
                        print(f"Renamed non-identical duplicate: {trace_file.name} -> {new_name}", file=sys.stderr)
                else:
                    # Shouldn't happen, but move it anyway
                    shutil.move(trace_file, determinism_check_dir / trace_file.name)
        
        # Clean up backup directory
        shutil.rmtree(backup_dir)
        
        # Create analysis instructions
        readme_file = analysis_dir / "README.txt"
        with open(readme_file, 'w') as f:
            f.write(f"DETERMINISM CHECK FAILED - Analysis Files\n")
            f.write(f"==========================================\n\n")
            f.write(f"Test UID: {self.uid}\n")
            f.write(f"Seed: {seed}\n\n")
            f.write(f"Directory Structure:\n")
            f.write(f"- initial_run/: Trace files from the first run\n")
            f.write(f"- determinism_check/: Trace files from the determinism check (second run only)\n\n")
            f.write(f"To analyze the determinism failure:\n")
            f.write(f"python3 contrib/TestHarness2/analyze_determinism_failure.py {initial_run_dir} {determinism_check_dir}\n\n")
        
        print(f"Determinism check failed. Analysis files created in {analysis_dir}", file=sys.stderr)

    def _run_joshua_logtool_for_test(self, test_run):
        """Run joshua_logtool for a test run after file organization is complete."""
        print(f"DEBUG: _run_joshua_logtool_for_test called for test {test_run.uid}", file=sys.stderr)
        print(f"DEBUG: test_run.summary.is_negative_test = {test_run.summary.is_negative_test}", file=sys.stderr)
        print(f"DEBUG: test_run.summary.ok() = {test_run.summary.ok()}", file=sys.stderr)
        
        force_joshua_logtool = os.getenv("TH_FORCE_JOSHUA_LOGTOOL", "false").lower() in ("true", "1", "yes")
        archive_logs_on_failure = os.getenv("TH_ARCHIVE_LOGS_ON_FAILURE", "false").lower() in ("true", "1", "yes")
        enable_joshua_logtool = os.getenv("TH_ENABLE_JOSHUA_LOGTOOL", "false").lower() in ("true", "1", "yes")
        
        print(f"DEBUG: TH_FORCE_JOSHUA_LOGTOOL = {force_joshua_logtool}", file=sys.stderr)
        print(f"DEBUG: TH_ARCHIVE_LOGS_ON_FAILURE = {archive_logs_on_failure}", file=sys.stderr)
        print(f"DEBUG: TH_ENABLE_JOSHUA_LOGTOOL = {enable_joshua_logtool}", file=sys.stderr)
        
        if not test_run.summary.is_negative_test and (not test_run.summary.ok() or force_joshua_logtool):
            print(f"DEBUG: Conditions met for joshua_logtool execution", file=sys.stderr)
            
            if not archive_logs_on_failure or not enable_joshua_logtool:
                print(f"DEBUG: joshua_logtool skipped - archive_logs_on_failure={archive_logs_on_failure}, enable_joshua_logtool={enable_joshua_logtool}", file=sys.stderr)
                child = SummaryTree("JoshuaLogTool")
                child.attributes["ExitCode"] = "0"
                child.attributes["Note"] = "Skipped - joshua_logtool not enabled"
                test_run.summary.out.append(child)
            else:
                print(f"DEBUG: Calling joshua_logtool", file=sys.stderr)
                try:
                    logtool_result = test_run._run_joshua_logtool()
                    print(f"DEBUG: joshua_logtool result: {logtool_result}", file=sys.stderr)
                except Exception as e:
                    print(f"DEBUG: joshua_logtool exception: {e}", file=sys.stderr)
                    logtool_result = {
                        "stdout": "",
                        "stderr": f"joshua_logtool failed: {e}",
                        "exit_code": -1,
                        "tool_skipped": True
                    }

                child = SummaryTree("JoshuaLogTool")
                child.attributes["ExitCode"] = str(logtool_result["exit_code"])
                if not logtool_result.get("tool_skipped", False):
                    if logtool_result["exit_code"] == 0:
                        stderr_lines = logtool_result["stderr"].split("\n") if logtool_result["stderr"] else []
                        success_lines = [line for line in stderr_lines if "SUCCESS - Uploaded" in line]
                        if success_lines:
                            child.attributes["Note"] = success_lines[-1].replace("JOSHUA_LOGTOOL: ", "")
                        else:
                            child.attributes["Note"] = "Upload completed"
                    else:
                        child.attributes["StdOut"] = logtool_result["stdout"]
                        child.attributes["StdErr"] = logtool_result["stderr"]
                else:
                    child.attributes["Note"] = logtool_result["stderr"]
                test_run.summary.out.append(child)
        else:
            print(f"DEBUG: Conditions NOT met for joshua_logtool execution", file=sys.stderr)

    def run_tests(
        self, test_files: List[Path], seed: int, test_picker: TestPicker
    ) -> bool:
        result: bool = True
        for count, file in enumerate(test_files):
            will_restart = count + 1 < len(test_files)
            binary = self.binary_chooser.choose_binary(file)
            unseed_check = (
                not is_no_sim(file)
                and config.random.random() < config.unseed_check_ratio
            )
            buggify_enabled: bool = False
            if config.buggify.value == BuggifyOptionValue.ON:
                buggify_enabled = True
            elif config.buggify.value == BuggifyOptionValue.RANDOM:
                buggify_enabled = config.random.random() < config.buggify_on_ratio

            # FIXME: support unseed checks for restarting tests
            run = TestRun(
                binary,
                file.absolute(),
                seed + count,
                self.uid,
                restarting=count != 0,
                stats=test_picker.dump_stats(),
                will_restart=will_restart,
                buggify_enabled=buggify_enabled,
            )
            result = result and run.success
            test_picker.add_time(test_files[0], run.run_time, run.summary.out)
            decorate_summary(run.summary.out, file, seed + count, run.buggify_enabled)
            if (
                unseed_check
                and run.summary.unseed is not None
                and run.summary.unseed >= 0
            ):
                run.summary.out.append(run.summary.list_simfdb())
            
            # Run joshua_logtool for regular test failures (non-determinism)
            if not result:
                self._run_joshua_logtool_for_test(run)
            
            run.summary.out.dump(sys.stdout)
            if not result:
                return False
            if (
                count == 0
                and unseed_check
                and run.summary.unseed is not None
                and run.summary.unseed >= 0
            ):
                # Backup trace files before determinism check
                self.backup_trace_files(seed + count)
                
                run2 = TestRun(
                    binary,
                    file.absolute(),
                    seed + count,
                    self.uid,
                    restarting=count != 0,
                    stats=test_picker.dump_stats(),
                    expected_unseed=run.summary.unseed,
                    will_restart=will_restart,
                    buggify_enabled=buggify_enabled,
                )
                test_picker.add_time(file, run2.run_time, run.summary.out)
                decorate_summary(
                    run2.summary.out, file, seed + count, run.buggify_enabled
                )
                result = result and run2.success

                # Organize trace files for analysis if determinism check failed
                if not result:
                    self.restore_trace_files(seed + count)
                    
                    # Run joshua_logtool after file organization for determinism failures
                    self._run_joshua_logtool_for_test(run2)
                
                # Dump XML output after joshua_logtool has been called
                run2.summary.out.dump(sys.stdout)

                if not result:
                    return False
        return result

    def run(self) -> bool:
        seed = (
            config.random_seed
            if config.random_seed is not None
            else config.random.randint(0, 2**32 - 1)
        )
        test_files = self.test_picker.choose_test()
        success = self.run_tests(test_files, seed, self.test_picker)
        
        # Check if we should preserve logs on failure
        archive_logs_on_failure = os.getenv("TH_ARCHIVE_LOGS_ON_FAILURE", "false").lower() in ("true", "1", "yes")
        if config.clean_up and (success or not archive_logs_on_failure):
            # Only clean up if test succeeded OR if archive_logs_on_failure is not set
            shutil.rmtree(config.run_temp_dir / str(self.uid))
        elif not success and archive_logs_on_failure:
            print(f"DEBUG: Preserving logs due to TH_ARCHIVE_LOGS_ON_FAILURE=true", file=sys.stderr)
        
        return success
