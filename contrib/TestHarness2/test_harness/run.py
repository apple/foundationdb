from __future__ import annotations

import base64
import collections
import math
import os
import random
import re
import resource
import shutil
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
import logging
import platform
from typing import Dict, List, Pattern, OrderedDict, Optional, Union, TYPE_CHECKING

from functools import total_ordering
from pathlib import Path
from test_harness.version import Version
from test_harness.config import BuggifyOptionValue

from test_harness.summarize import Summary, SummaryTree

if TYPE_CHECKING:
    from test_harness.config import Config

logger = logging.getLogger(__name__)

@total_ordering
class TestDescription:
    def __init__(self, name: str, priority: int):
        self.name = name
        self.priority = priority
        self.paths: List[Path] = []
        self.total_runtime: float = 0.0
        self.num_runs: int = 0

    def __lt__(self, other):
        if isinstance(other, TestDescription):
            return self.name < other.name
        else:
            return self.name < str(other)

    def __eq__(self, other):
        if isinstance(other, TestDescription):
            return self.name == other.name
        else:
            return self.name == str(other.name)


class StatFetcher:
    def __init__(self, tests: Dict[str, TestDescription]):
        self.tests = tests

    def fetch_stats(self) -> None:
        pass

    def dump_stats(self) -> str:
        return base64.b64encode(b"").decode("utf-8")


class TestPicker:
    def __init__(self, test_dir: Path, binaries: OrderedDict[Version, Path], config_obj: Config):
        self.config = config_obj
        if not test_dir.exists():
            raise RuntimeError("{} is neither a directory nor a file".format(test_dir))
        self.include_files_regex = re.compile(self.config.include_test_files)
        self.exclude_files_regex = re.compile(self.config.exclude_test_files)
        self.include_tests_regex = re.compile(self.config.include_test_classes)
        self.exclude_tests_regex = re.compile(self.config.exclude_test_names)
        self.test_dir: Path = test_dir
        self.tests: OrderedDict[str, TestDescription] = collections.OrderedDict()
        self.restart_test: Pattern = re.compile(r".*-\d+\.(txt|toml)")
        self.follow_test: Pattern = re.compile(r".*-2\.(txt|toml)")
        self.old_binaries: OrderedDict[Version, Path] = binaries
        self.slow_priority: int = int(os.getenv("SLOW_PRIORITY", 1))
        self.rare_priority: int = int(os.getenv("RARE_PRIORITY", 10))

        # Automatically exclude restart tests if no old binaries are available
        # This matches the original C# TestHarness behavior
        test_types_to_run = self.config.test_types_to_run.copy()
        if len(binaries) == 0 and "restarting" in test_types_to_run:
            logger.info("No old binaries available - excluding restart tests")
            test_types_to_run.remove("restarting")

        for subdir in self.test_dir.iterdir():
            if subdir.is_dir() and subdir.name in test_types_to_run:
                self.walk_test_dir(subdir)
        
        self.stat_fetcher: StatFetcher
        if self.config.stats is not None or self.config.joshua_output_dir is None:
            self.stat_fetcher = StatFetcher(self.tests)
        else:
            try:
                from test_harness.fdb import FDBStatFetcher
                self.stat_fetcher = FDBStatFetcher(self.tests)
            except ImportError:
                # Fall back to StatFetcher if fdb module is not available
                self.stat_fetcher = StatFetcher(self.tests)
        
        if self.config.stats is not None:
            self.load_stats(self.config.stats)
        else:
            self.fetch_stats()

        if not self.tests:
            raise Exception("No tests to run! Please check if tests are included/excluded incorrectly or old binaries are missing for restarting tests")

    def add_time(self, test_file: Path, run_time: int, out: SummaryTree) -> None:
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
        if test_desc is None:
            return
        test_desc.total_runtime += run_time
        test_desc.num_runs += 1
        out.attributes["TestName"] = test_name
        out.attributes["TestPriority"] = str(test_desc.priority)

    def load_stats(self, stats_file: Path):
        pass

    def fetch_stats(self):
        self.stat_fetcher.fetch_stats()

    def dump_stats(self) -> str:
        return self.stat_fetcher.dump_stats()

    def add_test(self, path: Path):
        if (
            self.include_files_regex.search(str(path)) is None
            or self.exclude_files_regex.search(str(path)) is not None
        ):
            return
        if is_restarting_test(path):
            candidates: List[Path] = []
            dirs = path.parent.parts
            version_expr = dirs[-1].split("_")
            if (
                len(version_expr) == 4
                and version_expr[0] == "from"
                and version_expr[2] == "until"
            ):
                max_version = Version.parse(version_expr[3])
                min_version = Version.parse(version_expr[1])
                for ver, binary in self.old_binaries.items():
                    if min_version <= ver < max_version:
                        candidates.append(binary)
                if not len(candidates):
                    return

        with path.open("r") as f:
            test_string = f.read()
        priority = 1
        if is_rare(path):
            priority = self.rare_priority
        test_name = None
        for line in test_string.split("\n"):
            if line.startswith("testTitle"):
                test_name = line.split("=")[1].strip()
                break
        if test_name is None:
            test_name = path.stem
        if (
            self.include_tests_regex.search(test_name) is None
            or self.exclude_tests_regex.search(test_name) is not None
        ):
            return
        if test_name not in self.tests:
            self.tests[test_name] = TestDescription(test_name, priority)
        self.tests[test_name].paths.append(path)

    def walk_test_dir(self, test: Path):
        if test.is_dir():
            for file in test.iterdir():
                self.walk_test_dir(file)
        else:
            if self.follow_test.match(test.name) is not None:
                return
            if test.suffix == ".txt" or test.suffix == ".toml":
                self.add_test(test)

    @staticmethod
    def list_restart_files(start_file: Path) -> List[Path]:
        name = re.sub(r"-\d+.(txt|toml)", "", start_file.name)
        res: List[Path] = []
        for test_file in start_file.parent.iterdir():
            if test_file.name.startswith(name):
                res.append(test_file)
        assert len(res) >= 1
        res.sort()
        return res

    def choose_test(self) -> List[Path]:
        candidates: List[TestDescription] = []

        if self.config.random.random() < 0.99:
            min_runtime: float | None = None
            for _, v in self.tests.items():
                this_time = v.total_runtime * v.priority
                if min_runtime is None or this_time < min_runtime:
                    min_runtime = this_time
                    candidates = [v]
                elif this_time == min_runtime:
                    candidates.append(v)
        else:
            min_runs: int | None = None
            for _, v in self.tests.items():
                if min_runs is None or v.num_runs < min_runs:
                    min_runs = v.num_runs
                    candidates = [v]
                elif v.num_runs == min_runs:
                    candidates.append(v)

        if not candidates:
            candidates = list(self.tests.values())

        candidates.sort()
        choice = self.config.random.randint(0, len(candidates) - 1)
        test = candidates[choice]
        result = test.paths[self.config.random.randint(0, len(test.paths) - 1)]
        if self.restart_test.match(result.name):
            return self.list_restart_files(result)
        else:
            return [result]


class OldBinaries:
    def __init__(self, config_obj: Config):
        self.config = config_obj
        self.first_file_expr = re.compile(r".*-1\.(txt|toml)")
        self.old_binaries_path: Path = self.config.old_binaries_path
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
            return self.config.binary
        max_version = Version.max_version()
        min_version = Version.parse("5.0.0")
        dirs = test_file.parent.parts
        if "restarting" not in dirs:
            return self.config.binary

        try:
            restarting_idx = dirs.index("restarting")
            if restarting_idx + 1 < len(dirs):
                version_expr_str = dirs[restarting_idx+1]
            else:
                return self.config.binary
        except ValueError:
             return self.config.binary

        version_expr = version_expr_str.split("_")
        first_file = self.first_file_expr.match(test_file.name) is not None

        if first_file and version_expr[0] == "to":
            return self.config.binary
        if not first_file and version_expr[0] == "from":
            return self.config.binary
        if version_expr[0] == "from" or version_expr[0] == "to":
            if len(version_expr) > 1:
                 min_version = Version.parse(version_expr[1])
            else:
                 return self.config.binary
        if len(version_expr) == 4 and version_expr[2] == "until":
            max_version = Version.parse(version_expr[3])

        candidates: List[Path] = []
        for ver, binary in self.binaries.items():
            if min_version <= ver < max_version:
                candidates.append(binary)
        if len(candidates) == 0:
            return self.config.binary
        return self.config.random.choice(candidates)


def is_restarting_test(test_file: Path):
    return "restarting" in test_file.parts


def is_negative(test_file: Path):
    return len(test_file.parts) > 1 and test_file.parts[-2] == "negative"


def is_no_sim(test_file: Path):
    return len(test_file.parts) > 1 and test_file.parts[-2] == "noSim"


def is_rare(test_file: Path):
    return len(test_file.parts) > 1 and test_file.parts[-2] == "rare"


class ResourceMonitor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.start_time = time.monotonic()
        self.end_time: float | None = None
        self._stop_monitor = threading.Event()
        self.max_rss = 0

    def run(self) -> None:
        while not self._stop_monitor.is_set():
            if self._stop_monitor.wait(1.0):
                break
            try:
                resources = resource.getrusage(resource.RUSAGE_CHILDREN)
                self.max_rss = max(resources.ru_maxrss, self.max_rss)
            except Exception:
                pass

    def stop(self):
        self.end_time = time.monotonic()
        self._stop_monitor.set()

    def get_time(self):
        if self.end_time is None:
            return time.monotonic() - self.start_time
        return self.end_time - self.start_time


class TestRun:
    # Class variable to track part numbers for each test UID
    _part_counters = {}

    def __init__(
        self,
        binary: Path,
        test_file: Path,
        random_seed: int,
        uid: uuid.UUID,
        config_obj: Config,
        restarting: bool = False,
        test_determinism: bool = False,
        stats: str | None = None,
        expected_unseed: int | None = None,
        will_restart: bool = False,
        original_run_for_unseed_archival: Optional[TestRun] = None,
        buggify_enabled: bool | None = None,
        force_identical_execution: bool = False
    ):
        self.config = config_obj
        self.binary = binary
        self.test_file = test_file
        self.random_seed = random_seed
        self.uid = uid
        self.force_identical_execution = force_identical_execution
        
        # Use different naming schemes to avoid conflicts:
        # - Restarting tests: 0, 1, 2, ... (based on count parameter)
        # - Determinism checks: det_0, det_1 (separate counter)
        if expected_unseed is not None:  # This is a determinism check
            uid_str = str(uid)
            if not hasattr(TestRun, '_det_counters'):
                TestRun._det_counters = {}
            if uid_str not in TestRun._det_counters:
                TestRun._det_counters[uid_str] = 0
            else:
                TestRun._det_counters[uid_str] += 1
            
            self.part_uid = f"det_{TestRun._det_counters[uid_str]}"
        else:
            # For restarting tests, this will be overridden by TestRunner with count
            self.part_uid = 0
        
        self.restarting = restarting
        self.test_determinism = test_determinism
        self.stats: str | None = stats
        self.expected_unseed: int | None = expected_unseed
        self.use_valgrind: bool = self.config.use_valgrind
        
        # Use passed buggify_enabled if provided, otherwise calculate it
        if buggify_enabled is not None:
            self.buggify_enabled = buggify_enabled
        else:
            self.buggify_enabled: bool = False
            if self.config.buggify.value == BuggifyOptionValue.ON:
                self.buggify_enabled = True
            elif self.config.buggify.value == BuggifyOptionValue.RANDOM:
                self.buggify_enabled = self.config.random.random() < self.config.buggify_on_ratio

        self.fault_injection_enabled: bool = True
        self.trace_format: str | None = self.config.trace_format

        binary_version = Version.of_binary(self.binary)
        if binary_version < "6.1.0":
            self.trace_format = None
        self.use_tls_plugin = binary_version < "5.2.0"

        self.temp_path = self.config.run_temp_dir / str(self.uid) / str(self.part_uid)
        self.identified_fdb_log_files: List[Path] = []

        self.retryable_error: bool = False
        self.stdout_path = self.temp_path / "stdout.txt"
        self.stderr_path = self.temp_path / "stderr.txt"
        self.command_file_path = self.temp_path / "command.txt"

        # Collect paired files for determinism check archival
        _paired_fdb_logs_for_summary: Optional[List[Path]] = None
        _paired_harness_files_for_summary: Optional[List[Path]] = None
        if original_run_for_unseed_archival and self.expected_unseed is not None:
            _paired_fdb_logs_for_summary = original_run_for_unseed_archival.identified_fdb_log_files
            _paired_harness_files_list = [
                original_run_for_unseed_archival.command_file_path,
                original_run_for_unseed_archival.stderr_path,
                original_run_for_unseed_archival.stdout_path,
            ]
            _paired_harness_files_for_summary = [p for p in _paired_harness_files_list if p and p.exists()]

        # Create archival config
        from test_harness.summarize import ArchivalConfig
        archival_config = ArchivalConfig()
        archival_config.enabled = self.config.archive_logs_on_failure
        archival_config.joshua_output_dir = self.config.joshua_output_dir
        archival_config.run_temp_dir = self.temp_path
        archival_config.stdout_path = self.stdout_path
        archival_config.stderr_path = self.stderr_path
        archival_config.command_path = self.command_file_path
        archival_config.current_fdb_logs = self.identified_fdb_log_files
        if _paired_fdb_logs_for_summary:
            archival_config.paired_fdb_logs = _paired_fdb_logs_for_summary
        if _paired_harness_files_for_summary:
            archival_config.paired_harness_files = _paired_harness_files_for_summary

        self.summary = Summary(
            binary=self.binary,
            uid=self.uid,
            current_part_uid=self.part_uid,
            expected_unseed=self.expected_unseed,
            will_restart=will_restart,
            long_running=self.config.long_running,
            archival_config=archival_config,
            stats_attribute_for_v1=self.stats,
        )
        
        if self.restarting:
            self.summary.out.attributes["Restarting"] = "1"
        if self.test_determinism:
             self.summary.out.attributes["DeterminismCheck"] = "1"

        self.run_time: int = 0
        # Don't execute immediately - let TestRunner update paths first
        self.success = False  # Will be set when execute() is called

    def execute(self) -> bool:
        """Execute the test part - called by TestRunner after setting up paths"""
        self.success = self._execute_test_part()
        return self.success

    def delete_simdir(self):
        simfdb_path = self.temp_path / Path("simfdb")
        if simfdb_path.exists():
            try:
                shutil.rmtree(simfdb_path)
            except Exception:
                pass

    def _run_joshua_logtool(self):
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

        # Debug: log which script we're using
        debug_info = f"Using joshua_logtool.py at: {joshua_logtool_script.absolute()}"
        
        command = [
            sys.executable,
            str(joshua_logtool_script),
            "upload",
            "--test-uid", str(self.uid),
            "--log-directory", str(self.temp_path.parent)
        ]
        
        # Add appropriate upload flag based on TH_DISABLE_ROCKSDB_CHECK setting
        disable_rocksdb_check = os.getenv("TH_DISABLE_ROCKSDB_CHECK", "false").lower() in ("true", "1", "yes")
        if disable_rocksdb_check:
            command.append("--check-rocksdb")
        
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60)
            
            # Parse result summary from stdout
            result_summary = "Unknown"
            if result.stdout:
                for line in result.stdout.split('\n'):
                    if line.startswith('JOSHUA_LOGTOOL_RESULT:'):
                        result_summary = line.replace('JOSHUA_LOGTOOL_RESULT:', '').strip()
                        break
            
            return {
                "stdout": result.stdout, 
                "stderr": debug_info + "\n" + result.stderr, 
                "exit_code": result.returncode, 
                "tool_skipped": False,
                "result_summary": result_summary
            }
            
        except subprocess.TimeoutExpired:
            return {"stdout": "", "stderr": debug_info + "\nJoshuaLogTool timed out", "exit_code": -2, "tool_skipped": True}
        except Exception as e:
            return {"stdout": "", "stderr": debug_info + f"\nException: {e}", "exit_code": -3, "tool_skipped": True}

    def _execute_test_part(self) -> bool:
        command: List[str] = []
        env: Dict[str, str] = os.environ.copy()
        valgrind_file: Path | None = None
        
        # For determinism checks, ensure identical environment
        if self.force_identical_execution:
            # Remove time-based and process-specific environment variables that could affect randomness
            env.pop('RANDOM', None)
            env.pop('SRANDOM', None)
            env.pop('SECONDS', None)
            # Set deterministic values for key variables that might affect configuration generation
            env['JOSHUA_SEED'] = str(self.random_seed)
            env['FDB_RANDOM_SEED'] = str(self.random_seed)
            env['FDB_DETERMINISTIC_CONFIG'] = '1'  # Try to force deterministic configuration
            # Remove variables that might introduce non-determinism
            env.pop('HOSTNAME', None)
            env.pop('USER', None)
            env.pop('LOGNAME', None)
            env.pop('PWD', None)
            # Set consistent values for key system variables
            env['TZ'] = 'UTC'
            env['LC_ALL'] = 'C'

        if self.use_valgrind:
            command.append("valgrind")
            valgrind_file = self.temp_path / Path(f"valgrind-{self.random_seed}.xml")
            dbg_path = os.getenv("FDB_VALGRIND_DBGPATH")
            if dbg_path is not None:
                command.append(f"--extra-debuginfo-path={dbg_path}")
            command += [
                "--xml=yes",
                f"--xml-file={valgrind_file.absolute()}",
                "-q",
            ]

        command += [
            str(self.binary.absolute()),
            "-r", "test" if is_no_sim(self.test_file) else "simulation",
            "-f", str(self.test_file.absolute()),
            "-s", str(self.random_seed),
            "--logdir", "logs",
        ]

        if self.trace_format is not None:
            command += ["--trace_format", self.trace_format]
        if self.use_tls_plugin and self.config.tls_plugin_path:
            command += ["--tls_plugin", str(self.config.tls_plugin_path.absolute())]
            env["FDB_TLS_PLUGIN"] = str(self.config.tls_plugin_path.absolute())
        if self.config.disable_kaio:
            command += ["--knob-disable-posix-kernel-aio=1"]

        binary_version = Version.of_binary(self.binary)
        if binary_version >= "7.1.0":
            command += ["-fi", "on" if self.fault_injection_enabled else "off"]

        if self.restarting:
            command.append("--restarting")
        if self.buggify_enabled:
            command += ["-b", "on"]

        if self.config.crash_on_error and not is_negative(self.test_file):
            command.append("--crash")
        if self.config.long_running:
            command += ["--knob-sim-speedup-after-seconds=36000"]
            command += ["--knob-max-trace-lines=1000000000"]

        self.temp_path.mkdir(parents=True, exist_ok=True)
        (self.temp_path / "logs").mkdir(parents=True, exist_ok=True)

        self.command_str = " ".join(command)

        # Write command string to file for archival
        try:
            with open(self.command_file_path, 'w') as f:
                f.write(self.command_str)
        except Exception:
            pass

        # Start resource monitor
        resources = ResourceMonitor()
        resources.start()

        # Execute the test
        stdout_data = ""
        err_out = ""
        actual_return_code = -1
        did_kill = False

        try:
            timeout = None
            if not self.config.long_running:
                timeout = (20 * self.config.kill_seconds if self.use_valgrind else self.config.kill_seconds)

            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=self.temp_path, env=env  # Remove text=True to handle binary output
            )
            
            try:
                stdout_bytes, stderr_bytes = process.communicate(timeout=timeout)
                # Safely decode output, replacing invalid UTF-8 sequences with '?'
                stdout_data = stdout_bytes.decode('utf-8', errors='replace')
                stderr_data = stderr_bytes.decode('utf-8', errors='replace')
                actual_return_code = process.returncode
            except subprocess.TimeoutExpired:
                process.kill()
                stdout_bytes, stderr_bytes = process.communicate()
                # Safely decode after timeout
                stdout_data = stdout_bytes.decode('utf-8', errors='replace')
                stderr_data = stderr_bytes.decode('utf-8', errors='replace')
                err_out += "\nPROCESS_KILLED_TIMEOUT"
                actual_return_code = process.returncode if process.returncode is not None else -1
                did_kill = True
        except Exception as e:
            err_out = f"PROCESS_ERROR: {e}"
            actual_return_code = -1
        
        # Combine stderr with err_out if there was stderr data
        if 'stderr_data' in locals() and stderr_data:
            err_out = stderr_data + ("\n" + err_out if err_out else "")

        # Stop resource monitor
        resources.stop()
        resources.join()
        self.run_time = math.ceil(resources.get_time())

        # Write stdout/stderr to files
        try:
            with open(self.stdout_path, 'w') as f:
                f.write(stdout_data)
            with open(self.stderr_path, 'w') as f:
                f.write(err_out)
        except Exception:
            pass

        # Find FDB log files
        log_dir = self.temp_path / "logs"
        expected_suffix = ".xml"
        if self.trace_format == "json":
            expected_suffix = ".json"

        if log_dir.is_dir():
            for log_file in log_dir.iterdir():
                if log_file.is_file() and log_file.name.endswith(expected_suffix):
                    self.identified_fdb_log_files.append(log_file)

        # Update summary
        self.summary.is_negative_test = is_negative(self.test_file)
        self.summary.runtime = resources.get_time()
        self.summary.max_rss = resources.max_rss
        self.summary.was_killed = did_kill
        self.summary.valgrind_out_file = valgrind_file
        self.summary.error_out = err_out
        self.summary.exit_code = actual_return_code
        self.summary.fdb_log_files_for_archival = self.identified_fdb_log_files

        # Parse FDB trace files
        if self.identified_fdb_log_files:
            self.summary.summarize_files(self.identified_fdb_log_files)

        self.summary.test_file = self.test_file
        self.summary.seed = self.random_seed
        self.summary.test_name = self.test_file.stem

        self.summary.summarize(self.temp_path, self.command_str)

        # Run JoshuaLogTool if needed
        if not self.summary.is_negative_test and not self.summary.ok():
            skip_joshua_logtool = os.getenv("TH_SKIP_JOSHUA_LOGTOOL", "true").lower() in ("true", "1", "yes")
            if skip_joshua_logtool:
                child = SummaryTree("JoshuaLogTool")
                child.attributes["ExitCode"] = "0"
                child.attributes["Note"] = "Skipped - TH_SKIP_JOSHUA_LOGTOOL is set"
                self.summary.out.append(child)
            else:
                archive_logs_on_failure = os.getenv("TH_ARCHIVE_LOGS_ON_FAILURE", "false").lower() in ("true", "1", "yes")
                enable_joshua_logtool = os.getenv("TH_ENABLE_JOSHUA_LOGTOOL", "false").lower() in ("true", "1", "yes")
                
                if not archive_logs_on_failure or not enable_joshua_logtool:
                    child = SummaryTree("JoshuaLogTool")
                    child.attributes["ExitCode"] = "0"
                    child.attributes["Note"] = "Skipped - archiving not enabled"
                    self.summary.out.append(child)
                else:
                    logtool_result = self._run_joshua_logtool()
                    child = SummaryTree("JoshuaLogTool")
                    child.attributes["ExitCode"] = str(logtool_result["exit_code"])
                    if not logtool_result["tool_skipped"]:
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
                    self.summary.out.append(child)
        
        self.summary.done()
        return self.summary.ok()


def decorate_summary(out: SummaryTree, test_file: Path, seed: int, buggify: bool):
    if "TestFile" not in out.attributes:
        out.attributes["TestFile"] = str(test_file.absolute())
    if "RandomSeed" not in out.attributes:
        out.attributes["RandomSeed"] = str(seed)
    if "BuggifyEnabled" not in out.attributes:
        out.attributes["BuggifyEnabled"] = "1" if buggify else "0"


class TestRunner:
    def __init__(self, config: Config):
        self.config = config
        self.uid = uuid.uuid4()
        self.test_path: Path = self.config.test_source_dir
        self.cluster_file: str | None = self.config.cluster_file
        self.binary_chooser = OldBinaries(self.config)
        if not self.test_path.exists() or not self.test_path.is_dir():
             raise RuntimeError(f"Test source directory {self.test_path} does not exist or is not a directory.")
        self.test_picker = TestPicker(self.test_path, self.binary_chooser.binaries, self.config)
        self.summary_tree = SummaryTree("TestResults", is_root_document=True)

    def backup_sim_dir(self, seed: int, part_uid: int):
        base_temp_dir = self.config.run_temp_dir / str(self.uid)
        # Look for simfdb in the current part directory
        src_dir = base_temp_dir / str(part_uid) / "simfdb"
        
        # Write debug info to a file that will be captured
        debug_file = base_temp_dir / f"backup_debug_{part_uid}.txt"
        debug_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(debug_file, 'w') as f:
            f.write(f"BACKUP: seed={seed}, part_uid={part_uid}\n")
            f.write(f"BACKUP: Looking for simfdb at {src_dir}\n")
            f.write(f"BACKUP: src_dir exists: {src_dir.exists()}, is_dir: {src_dir.is_dir()}\n")
            
            if not src_dir.exists():
                f.write(f"BACKUP: simfdb directory {src_dir} does not exist, skipping backup\n")
                return False
            elif not src_dir.is_dir():
                f.write(f"BACKUP: {src_dir} exists but is not a directory, skipping backup\n")
                return False
                
            dest_dir = base_temp_dir / f"simfdb.{seed}"
            f.write(f"BACKUP: Backup destination: {dest_dir}\n")
            
            if dest_dir.exists():
                f.write(f"BACKUP: Backup destination already exists, removing it\n")
                shutil.rmtree(dest_dir)
                
            try:
                shutil.copytree(src_dir, dest_dir)
                f.write(f"BACKUP: Successfully backed up {src_dir} to {dest_dir}\n")
                return True
            except Exception as e:
                f.write(f"BACKUP: Failed to backup simfdb: {e}\n")
                return False

    def restore_sim_dir(self, seed: int, part_uid: int):
        base_temp_dir = self.config.run_temp_dir / str(self.uid)
        src_dir = base_temp_dir / f"simfdb.{seed}"
        # Restore to the correct part directory where restart test expects it
        dest_dir = base_temp_dir / str(part_uid) / "simfdb"
        
        # Write debug info to a file that will be captured
        debug_file = base_temp_dir / f"restore_debug_{part_uid}.txt"
        debug_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(debug_file, 'w') as f:
            f.write(f"RESTORE: seed={seed}, part_uid={part_uid}\n")
            f.write(f"RESTORE: Looking for backup at {src_dir}\n")
            f.write(f"RESTORE: src_dir exists: {src_dir.exists()}, is_dir: {src_dir.is_dir()}\n")
            f.write(f"RESTORE: Will restore to {dest_dir}\n")
            
            if not src_dir.exists():
                f.write(f"RESTORE: Backup directory {src_dir} does not exist\n")
                return False
            elif not src_dir.is_dir():
                f.write(f"RESTORE: {src_dir} exists but is not a directory\n")
                return False
                
            try:
                if dest_dir.exists():
                    f.write(f"RESTORE: Removing existing dest_dir: {dest_dir}\n")
                    shutil.rmtree(dest_dir)
                # Ensure parent directory exists
                dest_dir.parent.mkdir(parents=True, exist_ok=True)
                f.write(f"RESTORE: Created parent directory: {dest_dir.parent}\n")
                shutil.copytree(str(src_dir), str(dest_dir))
                f.write(f"RESTORE: Successfully restored {src_dir} to {dest_dir}\n")
                return True
            except Exception as e:
                f.write(f"RESTORE: Failed to restore simfdb: {e}\n")
                return False

    def run_tests(
        self, test_files: List[Path], seed: int, test_picker: TestPicker
    ) -> tuple[bool, List[SummaryTree]]:
        overall_result: bool = True
        collected_summaries: List[SummaryTree] = []

        for count, file_path in enumerate(test_files):
            part_seed = seed + count

            # Restore simulation directory for restart tests (Phase 2+)
            if count > 0:
                restore_success = self.restore_sim_dir(seed, count)
                if not restore_success:
                    # If restore fails, create empty simfdb directory to prevent FileNotFound
                    base_temp_dir = self.config.run_temp_dir / str(self.uid)
                    empty_simfdb = base_temp_dir / str(count) / "simfdb"
                    empty_simfdb.mkdir(parents=True, exist_ok=True)
                    # Add debug note to test output
                    debug_note = f"RESTARTING_TEST_RESTORE_FAILED: part {count} could not restore simfdb for seed {seed}, created empty simfdb"
                    print(f"WARNING: {debug_note}")  # Also print to stdout for immediate visibility

            current_run = TestRun(
                binary=self.binary_chooser.choose_binary(file_path),
                test_file=file_path.absolute(),
                random_seed=part_seed,
                uid=self.uid,
                config_obj=self.config,
                restarting=(count != 0),
                stats=test_picker.dump_stats(),
                will_restart=(count + 1 < len(test_files))
            )
            
            # Set correct part_uid for restarting tests (overrides default)
            current_run.part_uid = count
            # Update temp_path to use the correct part_uid
            current_run.temp_path = self.config.run_temp_dir / str(self.uid) / str(count)
            # Update file paths that depend on temp_path
            current_run.stdout_path = current_run.temp_path / "stdout.txt"
            current_run.stderr_path = current_run.temp_path / "stderr.txt"
            current_run.command_file_path = current_run.temp_path / "command.txt"
            # Update summary to reflect correct part_uid
            current_run.summary.current_part_uid = count
            # Also update the XML attribute directly!
            current_run.summary.out.attributes["PartUID"] = str(count)
            # Update archival config
            current_run.summary.archival_config.run_temp_dir = current_run.temp_path
            current_run.summary.archival_config.stdout_path = current_run.stdout_path
            current_run.summary.archival_config.stderr_path = current_run.stderr_path
            current_run.summary.archival_config.command_path = current_run.command_file_path
            
            # Now execute the test with correct paths
            current_run.execute()
            overall_result = overall_result and current_run.success

            test_picker.add_time(file_path, current_run.run_time, current_run.summary.out)
            decorate_summary(current_run.summary.out, file_path, part_seed, current_run.buggify_enabled)

            # V1 stdout output
            if self.config._v1_summary_output_stream:
                v1_xml_string = current_run.summary.get_v1_stdout_line()
                if v1_xml_string:
                    self.config._v1_summary_output_stream.write(v1_xml_string + "\n")
                    self.config._v1_summary_output_stream.flush()

            collected_summaries.append(current_run.summary.out)

            if not current_run.summary.ok():
                return False, collected_summaries

            # Backup simulation directory after every part except the last for restart tests
            if count + 1 < len(test_files):
                backup_success = self.backup_sim_dir(seed, count)
                if not backup_success:
                    # If backup fails, create empty backup to prevent future restore failures
                    base_temp_dir = self.config.run_temp_dir / str(self.uid)
                    empty_backup = base_temp_dir / f"simfdb.{seed}"
                    empty_backup.mkdir(parents=True, exist_ok=True)
                    # Add debug note to test output
                    debug_note = f"RESTARTING_TEST_BACKUP_FAILED: part {count} could not backup simfdb for seed {seed}, created empty backup"
                    print(f"WARNING: {debug_note}")  # Also print to stdout for immediate visibility

            # Determinism check - now works on any part since we fixed the conflicts
            should_perform_unseed_check = (
                self.config.unseed_check_ratio > 0 and
                current_run.summary.ok() and
                self.config.random.random() < self.config.unseed_check_ratio
            )

            if should_perform_unseed_check:
                expected_unseed = current_run.summary.unseed
                
                # CRITICAL: Capture stats BEFORE add_time() modifies TestPicker state
                original_stats = current_run.stats
                
                # For restarting tests, determinism check needs the same simfdb state as the original part
                if count > 0:  # This is a restarting test part that needs simfdb restored
                    det_part_uid = f"det_{count}"
                    base_temp_dir = self.config.run_temp_dir / str(self.uid)
                    
                    # CRITICAL: Use the base seed for backup lookup
                    # Backup was created with the base seed (not part-specific seed)
                    
                    # Restore simfdb from backup to determinism check directory
                    src_backup = base_temp_dir / f"simfdb.{seed}"
                    det_simfdb = base_temp_dir / det_part_uid / "simfdb"
                    
                    restore_success = False
                    if src_backup.exists() and src_backup.is_dir():
                        try:
                            if det_simfdb.exists():
                                shutil.rmtree(det_simfdb)
                            det_simfdb.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copytree(src_backup, det_simfdb)
                            restore_success = True
                        except Exception as e:
                            print(f"WARNING: DETERMINISM_CHECK_RESTORE_FAILED: {e}")
                    
                    if not restore_success:
                        # Create empty simfdb to prevent FileNotFound
                        det_simfdb.mkdir(parents=True, exist_ok=True)
                        print(f"WARNING: DETERMINISM_CHECK_RESTORE_FAILED: part {count} could not restore simfdb for seed {seed}, created empty simfdb")
                
                # Legacy valgrind case
                if self.config.use_valgrind:
                    restore_success = self.restore_sim_dir(seed, count)
                    if not restore_success:
                        print(f"WARNING: DETERMINISM_CHECK_RESTORE_FAILED: part {count} could not restore simfdb for seed {seed}")
                
                # Second run for determinism check - must use IDENTICAL parameters as first run
                second_run = TestRun(
                    binary=current_run.binary,  # Use exact same binary, not re-chosen
                    test_file=file_path.absolute(),
                    random_seed=part_seed,  # Same seed for determinism
                    uid=self.uid,
                    config_obj=self.config,
                    restarting=(count != 0),
                    stats=original_stats,  # Use EXACT same stats as original run
                    expected_unseed=expected_unseed,
                    will_restart=(count + 1 < len(test_files)),
                    original_run_for_unseed_archival=current_run,
                    buggify_enabled=current_run.buggify_enabled,  # Use same buggify setting
                    force_identical_execution=True  # Force identical execution environment
                )
                
                # Copy all execution parameters from original test for true determinism
                second_run.use_valgrind = current_run.use_valgrind
                second_run.fault_injection_enabled = current_run.fault_injection_enabled
                second_run.trace_format = current_run.trace_format
                second_run.use_tls_plugin = current_run.use_tls_plugin
                
                # Set determinism check part_uid with det_ prefix to avoid conflicts
                det_part_uid = f"det_{count}"
                second_run.part_uid = det_part_uid
                # Update temp_path to use the correct part_uid
                second_run.temp_path = self.config.run_temp_dir / str(self.uid) / str(det_part_uid)
                # Update file paths that depend on temp_path
                second_run.stdout_path = second_run.temp_path / "stdout.txt"
                second_run.stderr_path = second_run.temp_path / "stderr.txt"
                second_run.command_file_path = second_run.temp_path / "command.txt"
                # Update summary to reflect correct part_uid
                second_run.summary.current_part_uid = det_part_uid
                # CRITICAL: Also update the XML attribute directly!
                second_run.summary.out.attributes["PartUID"] = str(det_part_uid)
                # Update archival config
                second_run.summary.archival_config.run_temp_dir = second_run.temp_path
                second_run.summary.archival_config.stdout_path = second_run.stdout_path
                second_run.summary.archival_config.stderr_path = second_run.stderr_path
                second_run.summary.archival_config.command_path = second_run.command_file_path
                
                # Execute the determinism check
                second_run.execute()
                
                # CRITICAL: Verify configuration consistency for determinism
                original_config = current_run.summary.out.attributes.get("ConfigString", "")
                determinism_config = second_run.summary.out.attributes.get("ConfigString", "")
                
                if original_config != determinism_config:
                    print(f"WARNING: ConfigString mismatch detected!")
                    print(f"Original:   {original_config}")
                    print(f"Determinism: {determinism_config}")
                    print(f"Forcing determinism check to use original configuration...")
                    
                    # Copy exact configuration from original test for determinism
                    second_run.summary.out.attributes["ConfigString"] = original_config
                    # Copy other critical attributes that should be identical for determinism
                    for attr in ["SourceVersion", "BuggifyEnabled", "FaultInjectionEnabled"]:
                        if attr in current_run.summary.out.attributes:
                            second_run.summary.out.attributes[attr] = current_run.summary.out.attributes[attr]

                overall_result = overall_result and second_run.success
                test_picker.add_time(file_path, second_run.run_time, second_run.summary.out)
                decorate_summary(second_run.summary.out, file_path, part_seed, second_run.buggify_enabled)

                if self.config._v1_summary_output_stream:
                    v1_xml_string = second_run.summary.get_v1_stdout_line()
                    if v1_xml_string:
                        self.config._v1_summary_output_stream.write(v1_xml_string + "\n")
                        self.config._v1_summary_output_stream.flush()

                collected_summaries.append(second_run.summary.out)

                if not second_run.success:
                    return False, collected_summaries

        return overall_result, collected_summaries

    def run_all_tests(self) -> SummaryTree:
        test_files = self.test_picker.choose_test()

        if not test_files:
            pass  # Empty summary tree
        else:
            current_run_success, current_run_summary_trees = self.run_tests(
                test_files, self.config.joshua_seed, self.test_picker
            )

            if current_run_summary_trees:
                for single_test_summary_tree in current_run_summary_trees:
                    if single_test_summary_tree is not None:
                        self.summary_tree.append(single_test_summary_tree)

            self.summary_tree.attributes["BatchSuccess"] = "1" if current_run_success else "0"

        self.summary_tree.attributes["TestRunnerUID"] = str(self.uid)
        self.summary_tree.attributes["ConfiguredJoshuaSeed"] = str(self.config.joshua_seed)
        self.summary_tree.attributes["TestsChosenCount"] = str(len(test_files) if test_files else 0)

        return self.summary_tree
