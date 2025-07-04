from __future__ import annotations

import array
import base64
import collections
import math
import os
import random
import resource
import shutil
import subprocess
import re
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
            return self.name == other.name
        else:
            return self.name == str(other.name)


class StatFetcher:
    def __init__(self, tests: OrderedDict[str, TestDescription]):
        self.tests = tests

    def read_stats(self):
        pass

    def add_run_time(self, test_name: str, runtime: int, out: SummaryTree):
        self.tests[test_name].total_runtime += runtime


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
        self.follow_test: Pattern = re.compile(r".*-[2-9]\d*\.(txt|toml)")
        self.old_binaries: OrderedDict[Version, Path] = binaries
        self.rare_priority: int = int(os.getenv("RARE_PRIORITY", 10))

        for subdir in self.test_dir.iterdir():
            if subdir.is_dir() and subdir.name in self.config.test_types_to_run:
                self.walk_test_dir(subdir)
        self.stat_fetcher: StatFetcher
        if self.config.stats is not None or self.config.joshua_output_dir is None:
            self.stat_fetcher = StatFetcher(self.tests)
        else:
            from test_harness.fdb import FDBStatFetcher

            self.stat_fetcher = FDBStatFetcher(self.tests)
        if self.config.stats is not None:
            self.load_stats(self.config.stats)
        else:
            self.fetch_stats()

        if not self.tests:
            joshua_output_dir_str = str(self.config.joshua_output_dir) if self.config.joshua_output_dir else "None"
            error_message = (
                "No tests to run! Please check if tests are included/excluded incorrectly "
                "or old binaries are missing for restarting tests. "
                f"Test Dir: {self.test_dir}, "
                f"Include Files: {self.config.include_test_files}, Exclude Files: {self.config.exclude_test_files}, "
                f"Include Tests: {self.config.include_test_classes}, Exclude Tests: {self.config.exclude_test_names}, "
                f"Joshua Output Dir for FDBStatFetcher: {joshua_output_dir_str}"
            )
            logger.error(f"Detailed context for 'No tests to run!': {error_message}")
            raise Exception(error_message)

    def add_time(self, test_file: Path, run_time: int, out: SummaryTree) -> None:
        # getting the test name is fairly inefficient. But since we only have 100s of tests, I won't bother
        test_name: str | None = None
        test_desc: TestDescription | None = None
        for name, test in self.tests.items():
            for p in test.paths:
                test_files_to_check: List[Path]
                if self.restart_test.match(p.name):
                    test_files_to_check = self.list_restart_files(p)
                else:
                    test_files_to_check = [p]
                for file_to_check in test_files_to_check:
                    if file_to_check.absolute() == test_file.absolute():
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
                for ver, binary_path in self.old_binaries.items():
                    if min_version <= ver < max_version:
                        candidates.append(binary_path)
                if not len(candidates):
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
            for file_item in test.iterdir():
                self.walk_test_dir(file_item)
        else:
            if self.follow_test.match(test.name) is not None:
                return
            if test.suffix == ".txt" or test.suffix == ".toml":
                self.parse_txt(test)

    @staticmethod
    def list_restart_files(start_file: Path) -> List[Path]:
        name = re.sub(r"-\d+.(txt|toml)", "", start_file.name)
        res: List[Path] = []
        for test_file_item in start_file.parent.iterdir():
            if test_file_item.name.startswith(name):
                res.append(test_file_item)
        assert len(res) >= 1, f"Restart test {name} starting with {start_file} should have at least one part."
        res.sort()
        return res

    def choose_test(self) -> List[Path]:
        candidates: List[TestDescription] = []

        if self.config.random.random() < 0.99:
            min_runtime: float | None = None
            for _, v_item in self.tests.items():
                this_time = v_item.total_runtime * v_item.priority
                if min_runtime is None or this_time < min_runtime:
                    min_runtime = this_time
                    candidates = [v_item]
                elif this_time == min_runtime:
                    candidates.append(v_item)
        else:
            min_runs: int | None = None
            for _, v_item in self.tests.items():
                if min_runs is None or v_item.num_runs < min_runs:
                    min_runs = v_item.num_runs
                    candidates = [v_item]
                elif v_item.num_runs == min_runs:
                    candidates.append(v_item)

        if not candidates:
            logger.error("No candidates found in choose_test. This indicates an issue with test priorities or runtimes.")
            if not self.tests:
                 raise Exception("No tests available to choose from and TestPicker.tests is empty.")
            candidates = list(self.tests.values())

        candidates.sort()
        choice_idx = self.config.random.randint(0, len(candidates) - 1)
        test_desc = candidates[choice_idx]
        result_path = test_desc.paths[self.config.random.randint(0, len(test_desc.paths) - 1)]
        if self.restart_test.match(result_path.name):
            return self.list_restart_files(result_path)
        else:
            return [result_path]


class OldBinaries:
    def __init__(self, config_obj: Config):
        self.config = config_obj
        self.first_file_expr = re.compile(r".*-1\.(txt|toml)")
        self.old_binaries_path: Path = self.config.old_binaries_path
        self.binaries: OrderedDict[Version, Path] = collections.OrderedDict()
        if not self.old_binaries_path.exists() or not self.old_binaries_path.is_dir():
            return
        exec_pattern = re.compile(r"fdbserver-\d+\.\d+\.\d+(\.exe)?")
        for file_item in self.old_binaries_path.iterdir():
            if not file_item.is_file() or not os.access(file_item, os.X_OK):
                continue
            if exec_pattern.fullmatch(file_item.name) is not None:
                self._add_file(file_item)

    def _add_file(self, file_path: Path):
        version_str = file_path.name.split("-")[1]
        if version_str.endswith(".exe"):
            version_str = version_str[0 : -len(".exe")]
        ver = Version.parse(version_str)
        self.binaries[ver] = file_path

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
        for ver, binary_path in self.binaries.items():
            if min_version <= ver < max_version:
                candidates.append(binary_path)
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
            if self._stop_monitor.wait(0.1):
                break
            try:
                resources_children = resource.getrusage(resource.RUSAGE_CHILDREN)
                self.max_rss = max(resources_children.ru_maxrss, self.max_rss)
            except Exception as e:
                logger.warning(f"ResourceMonitor: Error getting rusage: {e}")


    def stop(self):
        self.end_time = time.monotonic()
        self._stop_monitor.set()

    def get_time(self):
        if self.end_time is None:
            return time.monotonic() - self.start_time
        return self.end_time - self.start_time


class TestRun:
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
        original_run_for_unseed_archival: Optional[TestRun] = None
    ):
        self.config = config_obj
        self.binary = binary
        self.test_file = test_file
        self.random_seed = random_seed
        self.uid = uid
        self.part_uid = uuid.uuid4()
        self.restarting = restarting
        self.test_determinism = test_determinism
        self.stats: str | None = stats
        self.expected_unseed: int | None = expected_unseed
        self.use_valgrind: bool = self.config.use_valgrind
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

        _paired_fdb_logs_for_summary: Optional[List[Path]] = None
        _paired_harness_files_for_summary: Optional[List[Path]] = None
        if original_run_for_unseed_archival and self.expected_unseed is not None:
            logger.debug(f"Unseed check run (part {self.part_uid}) collecting paired files from original run part {original_run_for_unseed_archival.part_uid}")
            _paired_fdb_logs_for_summary = original_run_for_unseed_archival.identified_fdb_log_files
            logger.info(f"  Original run (part {original_run_for_unseed_archival.part_uid}) identified_fdb_log_files for pairing: {[str(f) for f in (_paired_fdb_logs_for_summary or [])]}")

            _paired_harness_files_list = [
                original_run_for_unseed_archival.command_file_path,
                original_run_for_unseed_archival.stderr_path,
                original_run_for_unseed_archival.stdout_path,
            ]
            logger.info(f"  Original run (part {original_run_for_unseed_archival.part_uid}) harness file paths for pairing (raw paths): command='{original_run_for_unseed_archival.command_file_path}', stdout='{original_run_for_unseed_archival.stdout_path}', stderr='{original_run_for_unseed_archival.stderr_path}'")

            _existing_paired_harness_files = []
            for p_idx, p_path in enumerate(_paired_harness_files_list):
                if p_path and p_path.exists():
                    _existing_paired_harness_files.append(p_path)
                    logger.debug(f"    Paired harness file {p_idx} ({p_path}) exists and was added.")
                elif p_path:
                    logger.warning(f"    Original run's harness file {p_idx} ({p_path}) does not exist. Not adding to paired archive list.")
                else:
                    logger.debug(f"    Original run's harness file {p_idx} is None. Skipping.")
            _paired_harness_files_for_summary = _existing_paired_harness_files
            logger.info(f"  Original run (part {original_run_for_unseed_archival.part_uid}) harness files for pairing (that exist): {[str(f) for f in (_paired_harness_files_for_summary or [])]}")

        self.summary = Summary(
            binary=self.binary,
            uid=self.uid,
            current_part_uid=self.part_uid,
            expected_unseed=self.expected_unseed,
            will_restart=will_restart,
            long_running=self.config.long_running,
            paired_run_fdb_logs_for_archival=_paired_fdb_logs_for_summary,
            paired_run_harness_files_for_archival=_paired_harness_files_for_summary,
            archive_logs_on_failure=self.config.archive_logs_on_failure,
            joshua_output_dir=self.config.joshua_output_dir,
            run_temp_dir=self.temp_path,
            stats_attribute_for_v1=self.stats,
            current_run_stdout_path=self.stdout_path,
            current_run_stderr_path=self.stderr_path,
            current_run_command_file_path=self.command_file_path,
            fdb_log_files_for_archival=self.identified_fdb_log_files
        )
        self.fdb_stat_fetcher = None
        self.resource_monitor = None
        if self.restarting:
            self.summary.out.attributes["Restarting"] = "1"
        if self.test_determinism:
             self.summary.out.attributes["DeterminismCheck"] = "1"


        self.run_time: int = 0
        self.success = self._execute_test_part()

    def log_test_plan(self, out: SummaryTree):
        test_plan: SummaryTree = SummaryTree("TestPlan")
        test_plan.attributes["TestUID"] = str(self.part_uid)
        test_plan.attributes["ParentTestUID"] = str(self.uid)
        test_plan.attributes["RandomSeed"] = str(self.random_seed)
        test_plan.attributes["TestFile"] = str(self.test_file)
        test_plan.attributes["Buggify"] = "1" if self.buggify_enabled else "0"
        test_plan.attributes["FaultInjectionEnabled"] = (
            "1" if self.fault_injection_enabled else "0"
        )
        test_plan.attributes["DeterminismCheck"] = "1" if self.test_determinism else "0"

    def delete_simdir(self):
        simfdb_path = self.temp_path / Path("simfdb")
        if simfdb_path.exists():
            try:
                shutil.rmtree(simfdb_path)
                logger.debug(f"Deleted simdir: {simfdb_path}")
            except Exception as e:
                logger.error(f"Error deleting simdir {simfdb_path}: {e}", exc_info=True)
        else:
            logger.warning(f"Simdir not found for deletion: {simfdb_path}")


    def _run_joshua_logtool(self):
        joshua_logtool_script = Path("joshua_logtool.py")
        if not joshua_logtool_script.exists():
            logger.error(f"{joshua_logtool_script.name} missing in PWD ({Path.cwd()}). Cannot upload logs.")
            return {
                "stdout": "", "stderr": f"{joshua_logtool_script.name} not found.", "exit_code": -1,
                "tool_skipped": True
            }

        command = [
            sys.executable,
            str(joshua_logtool_script),
            "upload",
            "--test-uid", str(self.part_uid),
            "--log-directory", str(self.temp_path),
            "--check-rocksdb",
        ]
        logger.info(f"Running JoshuaLogTool: {' '.join(command)}")
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60)
            logger.info(f"JoshuaLogTool stdout: {result.stdout}")
            logger.info(f"JoshuaLogTool stderr: {result.stderr}")
            logger.info(f"JoshuaLogTool exit_code: {result.returncode}")
            return {
                "stdout": result.stdout, "stderr": result.stderr, "exit_code": result.returncode, "tool_skipped": False
            }
        except subprocess.TimeoutExpired:
            logger.error(f"JoshuaLogTool timed out after 60s.")
            return {"stdout": "", "stderr": "JoshuaLogTool timed out.", "exit_code": -2, "tool_skipped": True}
        except Exception as e:
            logger.error(f"Exception running JoshuaLogTool: {e}", exc_info=True)
            return {"stdout": "", "stderr": f"Exception: {e}", "exit_code": -3, "tool_skipped": True}


    def _execute_test_part(self) -> bool:
        command: List[str] = []
        env: Dict[str, str] = os.environ.copy()
        valgrind_file: Path | None = None

        stdout_data: str = ""
        err_out: str = ""
        actual_return_code: int = -1001
        process: Optional[subprocess.Popen] = None

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
        logger.info(f"Executing test part: {self.command_str} in {self.temp_path}")

        # Write command string to command.txt for archival
        try:
            with open(self.command_file_path, 'w') as f_cmd:
                f_cmd.write(self.command_str)
        except Exception as e_write_cmd:
            logger.error(f"Error writing command string to {self.command_file_path} for part {self.part_uid}: {e_write_cmd}")

        process_completed = False
        did_kill = False

        try:
            effective_timeout = None
            logger.info(f"Evaluating effective_timeout for {self.part_uid}. Accessing config: long_running, kill_seconds, use_valgrind")
            logger.debug(f"Config values: config.long_running={self.config.long_running}, config.kill_seconds={self.config.kill_seconds}, config.use_valgrind={self.use_valgrind}")
            if not self.config.long_running:
                effective_timeout = (20 * self.config.kill_seconds if self.use_valgrind else self.config.kill_seconds)
            logger.info(f"Effective timeout for {self.part_uid} calculated: {effective_timeout}")

            logger.info(f"Attempting to Popen for {self.part_uid}: {command}")
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=self.temp_path, env=env,
            )
            logger.info(f"Popen successful for {self.part_uid}, process PID: {process.pid if process else 'N/A'}")
            try:
                logger.info(f"Calling process.communicate(timeout={effective_timeout}) for {self.part_uid}")
                stdout_bytes, stderr_bytes = process.communicate(timeout=effective_timeout)
                process_completed = True
                actual_return_code = process.returncode
                stdout_data = stdout_bytes.decode(errors='replace') if stdout_bytes else ""
                err_out = stderr_bytes.decode(errors='replace') if stderr_bytes else ""
                logger.info(f"process.communicate() returned for {self.part_uid}. process_completed={process_completed}, actual_return_code={actual_return_code}")
            except subprocess.TimeoutExpired:
                logger.warning(f"Test part {self.part_uid} timed out after {effective_timeout}s. Killing process.")
                try:
                    process.kill()
                    killed_stdout_bytes, killed_stderr_bytes = process.communicate(timeout=5)
                    if killed_stdout_bytes: stdout_data += killed_stdout_bytes.decode(errors='replace')
                    if killed_stderr_bytes: err_out += killed_stderr_bytes.decode(errors='replace')
                    err_out += "\nPROCESS_KILLED_TIMEOUT"
                except Exception as e_kill_comm:
                    logger.error(f"Error during communication after killing timed-out process {self.part_uid}: {e_kill_comm}", exc_info=True)
                    err_out += "\nPROCESS_KILLED_TIMEOUT_COMM_ERROR"
                actual_return_code = process.returncode if process.returncode is not None else -1002
                did_kill = True
                logger.info(f"TimeoutExpired block finished for {self.part_uid}. did_kill={did_kill}, actual_return_code={actual_return_code}")
            except Exception as e_comm:
                logger.error(f"Error during process communication for {self.part_uid}: {e_comm}", exc_info=True)
                if isinstance(err_out, bytes):
                    err_out = err_out.decode(errors='replace') + f"\nCOMMUNICATE_ERROR_PRE_DECODE: {e_comm}"
                else:
                    err_out += f"\nCOMMUNICATE_ERROR: {e_comm}"
                if process and process.returncode is not None:
                    actual_return_code = process.returncode
                else:
                    actual_return_code = -1003
                logger.info(f"Exception in communicate block finished for {self.part_uid}. actual_return_code={actual_return_code}")
        except Exception as e_popen:
            logger.error(f"Failed to start process for test part {self.part_uid}: {e_popen}", exc_info=True)
            err_out = f"POPEN_ERROR: {e_popen}"
            actual_return_code = -1004
            logger.info(f"Popen exception block finished for {self.part_uid}. actual_return_code={actual_return_code}")

        self.run_time = 0
        if hasattr(self, 'summary') and self.summary:
             self.summary.max_rss = 0
        logger.info(f"Post-execution (ResourceMonitor disabled) for {self.part_uid}: stdout_len={len(stdout_data)}, stderr_len={len(err_out)}, final_actual_return_code={actual_return_code}")

        try:
            with open(self.stdout_path, 'w') as f_out:
                f_out.write(stdout_data if stdout_data else "")
            with open(self.stderr_path, 'w') as f_err:
                f_err.write(err_out if err_out else "")
        except Exception as e_write:
            logger.error(f"Error writing fdbserver stdout/stderr to files for part {self.part_uid}: {e_write}")

        explicit_log_dir = self.temp_path / "logs"
        expected_suffix = ".xml"
        if self.trace_format == "json": expected_suffix = ".json"
        elif self.trace_format == "xml": expected_suffix = ".xml"

        if explicit_log_dir.is_dir():
            for log_file in explicit_log_dir.iterdir():
                if log_file.is_file() and log_file.name.endswith(expected_suffix):
                    self.identified_fdb_log_files.append(log_file)

            if self.identified_fdb_log_files:
                logger.debug(f"Found FDB log files for part {self.part_uid} with suffix '{expected_suffix}': {self.identified_fdb_log_files}")
            else:
                logger.warning(f"No FDB log files matching suffix '{expected_suffix}' found in {explicit_log_dir} for part {self.part_uid}.")
        else:
            logger.warning(f"Explicitly specified FDB logs directory '{explicit_log_dir}' not found for part {self.part_uid}. Cannot find FDB logs.")

        self.summary.is_negative_test = is_negative(self.test_file)
        self.summary.runtime = 0
        self.summary.max_rss = 0
        self.summary.was_killed = did_kill
        self.summary.valgrind_out_file = valgrind_file
        self.summary.error_out = err_out
        self.summary.exit_code = actual_return_code

        self.summary.fdb_log_files_for_archival = self.identified_fdb_log_files

        if self.identified_fdb_log_files:
            logger.info(f"Parsing FDB trace files for part {self.part_uid}: {[str(f) for f in self.identified_fdb_log_files]}")
            self.summary.summarize_files(self.identified_fdb_log_files)
        else:
            logger.warning(f"No FDB trace files found to parse for part {self.part_uid} (looked in {explicit_log_dir}).")

        self.summary.test_file = self.test_file
        self.summary.seed = self.random_seed
        self.summary.test_name = self.test_file.stem

        logger.info(f"Preparing to summarize for {self.part_uid}. summary.exit_code set to {self.summary.exit_code}. Identified FDB logs: {len(self.identified_fdb_log_files if self.identified_fdb_log_files else [])}")
        self.summary.summarize(self.temp_path, self.command_str)

        if not self.summary.is_negative_test and not self.summary.ok():
            logtool_result = self._run_joshua_logtool()
            child = SummaryTree("JoshuaLogTool")
            child.attributes["ExitCode"] = str(logtool_result["exit_code"])
            if not logtool_result["tool_skipped"]:
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


    def backup_sim_dir(self, seed: int):
        base_temp_dir = self.config.run_temp_dir / str(self.uid)
        src_dir = base_temp_dir / "simfdb"
        if not src_dir.is_dir():
            logger.warning(f"Backup sim_dir: source {src_dir} does not exist or not a dir. Skipping backup.")
            return
        dest_dir = base_temp_dir / f"simfdb.{seed}"
        if dest_dir.exists():
            logger.warning(f"Backup sim_dir: destination {dest_dir} already exists. Skipping.")
            return
        try:
            shutil.copytree(src_dir, dest_dir)
            logger.info(f"Backed up {src_dir} to {dest_dir}")
        except Exception as e:
            logger.error(f"Error backing up {src_dir} to {dest_dir}: {e}", exc_info=True)


    def restore_sim_dir(self, seed: int):
        base_temp_dir = self.config.run_temp_dir / str(self.uid)
        src_dir = base_temp_dir / f"simfdb.{seed}"
        dest_dir = base_temp_dir / "simfdb"
        if not src_dir.exists() or not src_dir.is_dir():
            logger.warning(f"Restore sim_dir: source backup {src_dir} not found. Skipping restore.")
            return
        try:
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            shutil.move(str(src_dir), str(dest_dir))
            logger.info(f"Restored {src_dir} to {dest_dir}")
        except Exception as e:
            logger.error(f"Error restoring {src_dir} to {dest_dir}: {e}", exc_info=True)


    def run_tests(
        self, test_files: List[Path], seed: int, test_picker: TestPicker
    ) -> tuple[bool, List[SummaryTree]]:
        overall_result: bool = True
        collected_summaries: List[SummaryTree] = []

        for count, file_path_part in enumerate(test_files):
            logger.info(f"RUN.PY: Starting test part {count + 1}/{len(test_files)}: {file_path_part.name}")
            part_seed = type(self.config.random)(seed + count).randint(0, 2**63 - 1)

            current_run_part = TestRun(
                binary=self.binary_chooser.choose_binary(file_path_part),
                test_file=file_path_part.absolute(),
                random_seed=part_seed,
                uid=self.uid,
                config_obj=self.config,
                restarting=(count != 0),
                stats=test_picker.dump_stats(),
                will_restart=(count + 1 < len(test_files))
            )
            overall_result = overall_result and current_run_part.success

            test_picker.add_time(file_path_part, current_run_part.run_time, current_run_part.summary.out)
            decorate_summary(current_run_part.summary.out, file_path_part, part_seed, current_run_part.buggify_enabled)

            # === V1 STDOUT SUMMARY LOGIC for main test part ===
            if self.config._v1_summary_output_stream:
                v1_xml_string = current_run_part.summary.get_v1_stdout_line()
                if v1_xml_string:
                    logger.debug(f"RUN.PY: TestRunner (main part) ABOUT TO WRITE to _v1_summary_output_stream for part {current_run_part.summary.current_part_uid}. XML String: >>>{v1_xml_string}<<<")
                    self.config._v1_summary_output_stream.write(v1_xml_string + "\n")
                    self.config._v1_summary_output_stream.flush()
                else:
                    actual_value_for_log = repr(v1_xml_string)
                    logger.warning(f"RUN.PY: TestRunner (main part) - get_v1_stdout_line for part {current_run_part.summary.current_part_uid} returned None or empty. Type: {type(v1_xml_string)}, Value for logging: {actual_value_for_log}, Boolean eval: {bool(v1_xml_string)}. Not writing to V1 stdout.")
            # === END V1 STDOUT SUMMARY LOGIC ===

            collected_summaries.append(current_run_part.summary.out)

            # Determine if an unseed check should be performed for the current_run_part
            # An unseed check is only performed if the first part was successful (Ok="1")
            # and the random chance based on unseed_check_ratio passes.
            should_perform_unseed_check_based_on_ratio_and_success = (
                self.config.unseed_check_ratio > 0 and
                current_run_part.summary.ok() and # Only check if first part thinks it's OK
                type(self.config.random)(part_seed).random() < self.config.unseed_check_ratio
            )

            if not current_run_part.summary.ok():
                logger.info(f"RUN.PY: Main test part for {file_path_part.name} failed (Ok=0, Error: '{current_run_part.summary.error}'). Skipping unseed check.")
                # overall_result is already False because current_run_part.success was anded in.
                return False, collected_summaries # Early exit if first part failed

            # If the first part was successful, proceed to potential unseed check
            if should_perform_unseed_check_based_on_ratio_and_success:
                logger.info(f"RUN.PY: Performing determinism check for {file_path_part.name} (first part was Ok='1').")
                # Forcing the same seed to be used for the second run
                unseed_part_seed = part_seed
                logger.info(f"RUN.PY: Forcing identical seed for determinism check: {unseed_part_seed}")

                expected_unseed_from_first_run = current_run_part.summary.unseed
                logger.info(f"RUN.PY: Unseed from first run (current_run_part.summary.unseed): {expected_unseed_from_first_run}")

                unseed_run_part = TestRun(
                    binary=self.binary_chooser.choose_binary(file_path_part),
                    test_file=file_path_part.absolute(),
                    random_seed=unseed_part_seed,
                    uid=self.uid,
                    config_obj=self.config,
                    restarting=(count != 0),
                    stats=test_picker.dump_stats(),
                    expected_unseed=expected_unseed_from_first_run,
                    will_restart=(count + 1 < len(test_files)),
                    original_run_for_unseed_archival=current_run_part
                )

                # The overall_result now also depends on the success of the unseed run part.
                # current_run_part.success was already part of overall_result.
                # So, we AND in the unseed_run_part.success.
                overall_result = overall_result and unseed_run_part.success

                test_picker.add_time(file_path_part, unseed_run_part.run_time, unseed_run_part.summary.out)
                decorate_summary(unseed_run_part.summary.out, file_path_part, unseed_part_seed, unseed_run_part.buggify_enabled)

                if self.config._v1_summary_output_stream:
                    v1_xml_string_unseed = unseed_run_part.summary.get_v1_stdout_line()
                    if v1_xml_string_unseed:
                        logger.debug(f"RUN.PY: TestRunner (unseed part) ABOUT TO WRITE to _v1_summary_output_stream for part {unseed_run_part.summary.current_part_uid}. XML String: >>>{v1_xml_string_unseed}<<<" )
                        self.config._v1_summary_output_stream.write(v1_xml_string_unseed + "\n")
                        self.config._v1_summary_output_stream.flush()
                    else:
                        actual_value_for_log_unseed = repr(v1_xml_string_unseed)
                        logger.warning(f"RUN.PY: TestRunner (unseed part) - get_v1_stdout_line for part {unseed_run_part.summary.current_part_uid} returned None or empty. Type: {type(v1_xml_string_unseed)}, Value for logging: {actual_value_for_log_unseed}, Boolean eval: {bool(v1_xml_string_unseed)}. Not writing to V1 stdout.")

                collected_summaries.append(unseed_run_part.summary.out)

                if not unseed_run_part.success: # Check specific success of unseed run part
                    logger.info(f"RUN.PY: Unseed check FAILED for {file_path_part.name}. Error: '{unseed_run_part.summary.error}'")
                    logger.info(f"RUN.PY: Original Run Unseed: {current_run_part.summary.unseed}")
                    logger.info(f"RUN.PY: Determinism Check Unseed: {unseed_run_part.summary.unseed}")
                    # overall_result is already False. Return immediately.
                    return False, collected_summaries
                else:
                    logger.info(f"RUN.PY: Unseed check PASSED for {file_path_part.name}.")

            # If we reach here, either unseed check was not performed (and first part was OK),
            # or it was performed and passed.
            # The overall_result reflects the status.

        logger.info(f"RUN.PY: Finished all {len(test_files)} test parts. Overall result: {overall_result}")
        return overall_result, collected_summaries

    def run_all_tests(self) -> SummaryTree:
        logger.info(f"TestRunner.run_all_tests started (UID: {self.uid}). Base seed for this run: {self.config.joshua_seed}")

        # test_files will be a list of paths, e.g., for a single test or multiple parts of a restarting test.
        test_files = self.test_picker.choose_test()

        if not test_files:
            logger.info("Test picker returned no tests for this invocation. Returning an empty summary tree for results.")
            # self.summary_tree is already initialized in __init__ as SummaryTree("TestResults", is_root_document=True)
            # No children will be added if no tests are run.
        else:
            logger.info(f"TestRunner (UID: {self.uid}) chose test file(s): {[str(f) for f in test_files]}")

            # self.run_tests executes the chosen test files (which could be one or more parts of a single test case)
            # It uses the provided joshua_seed to derive specific seeds for each part.
            current_run_success, current_run_summary_trees = self.run_tests(
                test_files, self.config.joshua_seed, self.test_picker
            )

            if current_run_summary_trees:
                for single_test_summary_tree in current_run_summary_trees:
                    if single_test_summary_tree is not None:
                        self.summary_tree.append(single_test_summary_tree)
                    else:
                        logger.warning("run_tests returned a None SummaryTree in its list of results.")
            else:
                logger.warning("run_tests returned no summary trees for the current test set, though test files were chosen.")

            # Add an attribute to indicate if the set of tests (which could be multi-part) processed in this invocation was successful.
            # app.py will determine the final exit code based on children's Ok status.
            self.summary_tree.attributes["BatchSuccess"] = "1" if current_run_success else "0"

        # Add some overall attributes to the root summary_tree for this invocation.
        self.summary_tree.attributes["TestRunnerUID"] = str(self.uid)
        self.summary_tree.attributes["ConfiguredJoshuaSeed"] = str(self.config.joshua_seed)
        self.summary_tree.attributes["TestsChosenCount"] = str(len(test_files) if test_files else 0)

        logger.info(f"TestRunner.run_all_tests (UID: {self.uid}) finished. Returning main summary tree.")
        return self.summary_tree


def is_restarting_test(test_file: Path):
    return "restarting" in test_file.parts
