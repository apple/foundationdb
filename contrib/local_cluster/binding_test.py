#! /usr/bin/env python3

import argparse
import asyncio
import logging
import os
import os.path
import sys

import lib.fdb_process
import lib.local_cluster
import lib.process

from typing import List

logger = logging.getLogger("binding_test")

SCRIPT_DIR = os.path.split(os.path.abspath(__file__))[0]
# At this stage, the binaries are staying together with the current script
BINARY_DIR = SCRIPT_DIR
DEFAULT_FDBSERVER_PATH = os.path.join(BINARY_DIR, "fdbserver")
DEFAULT_FDBCLI_PATH = os.path.join(BINARY_DIR, "fdbcli")
# This is the LD_LIBRARY_PATH, so the file name is not included
DEFAULT_LIBFDB_PATH = os.path.abspath(os.path.join(BINARY_DIR))

DEFAULT_BINDINGTESTER = os.path.join(
    BINARY_DIR, "tests", "bindingtester", "bindingtester.py"
)

# binding test requires a working Python binder. The system default binder may
# be outdated or even not exist. Thus, the binder in the package is used and
# is assumed to work reasonably.
DEFAULT_PYTHON_BINDER = os.path.join(BINARY_DIR, "tests", "python")

DEFAULT_CONCURRENCY = 5
DEFAULT_OPERATIONS = 1000
DEFAULT_HCA_OPERATIONS = 100
DEFAULT_TIMEOUT_PER_TEST = 360.0


def _setup_logs(log_level: int = logging.INFO):
    log_format = logging.Formatter(
        "%(asctime)s | %(name)20s :: %(levelname)-8s :: %(message)s"
    )

    logger.handlers.clear()

    stdout_handler = logging.StreamHandler(stream=sys.stderr)
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(log_format)

    logger.addHandler(stdout_handler)
    logger.setLevel(log_level)

    # Here we might lose some of the logging from lib
    lib_logger = logging.getLogger("lib")
    lib_logger.setLevel(log_level)


def _setup_args() -> argparse.Namespace:
    """Parse the command line arguments"""
    parser = argparse.ArgumentParser("binding_test.py")
    parser.add_argument("--num-cycles", type=int, default=1, help="Number of cycles")
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Debug logging"
    )
    parser.add_argument(
        "--stop-at-failure",
        type=int,
        default=-1,
        help="Stop the test at binding test failure",
    )
    parser.add_argument(
        "--fdbserver-path",
        type=str,
        default=DEFAULT_FDBSERVER_PATH,
        help="Path to fdbserver",
    )
    parser.add_argument(
        "--fdbcli-path", type=str, default=DEFAULT_FDBCLI_PATH, help="Path to fdbcli"
    )
    parser.add_argument(
        "--libfdb-path",
        type=str,
        default=DEFAULT_LIBFDB_PATH,
        help="Path to libfdb.so. NOTE: The file name should not be included.",
    )
    parser.add_argument(
        "--binding-tester-path",
        type=str,
        default=DEFAULT_BINDINGTESTER,
        help="Path to binding tester",
    )
    parser.add_argument(
        "--num-ops", type=int, default=DEFAULT_OPERATIONS, help="Num ops in test"
    )
    parser.add_argument(
        "--num-hca-ops",
        type=int,
        default=DEFAULT_HCA_OPERATIONS,
        help="Num ops in HCA test",
    )
    parser.add_argument(
        "--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrency level"
    )
    parser.add_argument(
        "--test-timeout",
        type=float,
        default=DEFAULT_TIMEOUT_PER_TEST,
        help="Timeout for each single test",
    )
    return parser.parse_args()


def _check_file(path: str, executable: bool = True):
    if not os.path.exists(path):
        raise RuntimeError(f"{path} not found")
    if executable and (not os.path.isfile(path) or not os.access(path, os.X_OK)):
        raise RuntimeError(f"{path} not executable")


# TODO it might be better to import the binding_test rather than calling using subprocess
class TestSet:
    def __init__(
        self,
        binding_tester: str,
        num_ops: int,
        num_hca_ops: int,
        concurrency: int,
        ld_library_path: str,
        timeout: float,
        logging_level: str = "INFO",
    ) -> None:
        self._binding_tester = binding_tester
        self._num_ops = num_ops
        self._num_hca_ops = num_hca_ops
        self._concurrency = concurrency
        self._timeout = timeout
        self._logging_level = logging_level

        self._env = dict(os.environ)
        self._update_path_from_env("LD_LIBRARY_PATH", ld_library_path)
        self._update_path_from_env("PYTHONPATH", DEFAULT_PYTHON_BINDER)

    def _update_path_from_env(self, environment_variable_name: str, new_path: str):
        original_path = os.getenv(environment_variable_name)
        self._env[environment_variable_name] = (
            f"{new_path}:{original_path}" if original_path else new_path
        )
        logger.debug(
            f"{environment_variable_name} for binding tester: {self._env['LD_LIBRARY_PATH']}"
        )

    async def _test_coroutine(
        self,
        api_language: str,
        test_name: str,
        additional_args: List[str],
    ):
        arguments = [
            api_language,
            "--test-name",
            test_name,
            "--logging-level",
            self._logging_level,
        ]
        arguments += additional_args
        process = await lib.process.Process(
            executable=self._binding_tester,
            arguments=arguments,
            env=self._env,
        ).run()
        try:
            await asyncio.wait_for(process.wait(), timeout=self._timeout)
        finally:
            stdout = (await process.stdout.read(-1)).decode("utf-8")
            stderr = (await process.stderr.read(-1)).decode("utf-8")
            if len(stdout):
                logger.info("API Test stdout:\n{}".format(stdout))
            else:
                logger.info("API Test stdout: [Empty]")
            if len(stderr):
                logger.warning("API Test stderr:\n{}".format(stderr))
            else:
                logger.info("API Test stderr: [Empty]")

    async def _run_test(
        self,
        api_language: str,
        test_name: str,
        additional_args: List[str],
    ):
        logger.debug(f"Run test API [{api_language}] Test name [{test_name}]")
        try:
            await self._test_coroutine(
                api_language=api_language,
                test_name=test_name,
                additional_args=additional_args,
            )
        except asyncio.TimeoutError as timeout:
            logger.exception(
                f"Test API [{api_language}] Test name [{test_name}] failed due to timeout {self._timeout}"
            )
            return False
        except Exception as e:
            logger.exception(
                f"Test API [{api_language}] Test name [{test_name}] failed with exception: {str(e)}"
            )
            return False
        logger.debug(f"Test API [{api_language}] Test name [{test_name}] completed")
        return True

    async def run_scripted_test(self, test: str):
        return await self._run_test(test, "scripted", [])

    async def run_api_test(self, test: str):
        return await self._run_test(
            test, "api", ["--compare", "--num-ops", str(self._num_ops)]
        )

    async def run_api_concurrency_test(self, test: str):
        return await self._run_test(
            test,
            "api",
            ["--concurrency", str(self._concurrency), "--num-ops", str(self._num_ops)],
        )

    async def run_directory_test(self, test: str):
        return await self._run_test(
            test,
            "directory",
            [
                "--compare",
                "--num-ops",
                str(self._num_ops),
            ],
        )

    async def run_directory_hca_test(self, test: str):
        return await self._run_test(
            test,
            "directory_hca",
            [
                "--concurrency",
                str(self._concurrency),
                "--num-ops",
                str(self._num_hca_ops),
            ],
        )


API_LANGUAGES = [
    "python3",
    "java",
    "java_async",
    "go",
    "flow",
]


def _log_cluster_lines_with_severity(
    cluster: lib.local_cluster.FDBServerLocalCluster, severity: int
):
    for process_handlers in cluster.handlers:
        for log_file, lines in process_handlers.get_log_with_severity(severity).items():
            if severity == 40:
                reporter = logger.error
            elif severity == 30:
                reporter = logger.warning
            elif severity == 20:
                reporter = logger.info
            else:
                reporter = logger.debug

            if len(lines) == 0:
                reporter(f"{log_file}: No Severity={severity} lines")
            else:
                reporter(
                    "{}: {} lines with Severity={}\n{}".format(
                        log_file, len(lines), severity, "".join(lines)
                    )
                )


async def run_binding_tests(
    test_set: TestSet, num_cycles: int, stop_at_failure: int = None
):
    tests = [
        test_set.run_scripted_test,
        test_set.run_api_test,
        test_set.run_api_concurrency_test,
        test_set.run_directory_test,
        test_set.run_directory_hca_test,
    ]
    num_failures: int = 0

    async def run_tests():
        nonlocal num_failures
        for api_language in API_LANGUAGES:
            for test in tests:
                test_success = await test(api_language)
                if not test_success:
                    num_failures += 1
                    if stop_at_failure and num_failures > stop_at_failure:
                        raise RuntimeError(
                            f"Maximum number of test failures have reached"
                        )

    async with lib.local_cluster.FDBServerLocalCluster(1) as local_cluster:
        logger.info("Start binding test")

        try:
            for cycle in range(num_cycles):
                logger.info(f"Starting cycle {cycle}")
                await run_tests()
        except:
            logger.exception("Error found during the binding test")
        finally:
            logger.info(f"Binding test completed with {num_failures} failures")

            _log_cluster_lines_with_severity(local_cluster, 40)
            _log_cluster_lines_with_severity(local_cluster, 30)


def main():
    args = _setup_args()
    _setup_logs(args.debug)

    _check_file(args.fdbserver_path, True)
    _check_file(args.fdbcli_path, True)
    _check_file(args.libfdb_path, False)

    lib.fdb_process.set_fdbserver_path(args.fdbserver_path)
    lib.fdb_process.set_fdbcli_path(args.fdbcli_path)

    logger.info(f"Executable: {__file__}")
    logger.info(f"PID: {os.getpid()}")
    logger.info(f"fdbserver: {args.fdbserver_path}")
    logger.info(f"fdbcli: {args.fdbcli_path}")
    logger.info(f"libfdb: {args.libfdb_path}")
    logger.info(f"NumCycles: {args.num_cycles}")

    test_set = TestSet(
        binding_tester=args.binding_tester_path,
        num_ops=args.num_ops,
        num_hca_ops=args.num_hca_ops,
        concurrency=args.concurrency,
        ld_library_path=args.libfdb_path,
        timeout=args.test_timeout,
    )

    asyncio.run(run_binding_tests(test_set, args.num_cycles, args.stop_at_failure))

    return 0


if __name__ == "__main__":
    sys.exit(main())
