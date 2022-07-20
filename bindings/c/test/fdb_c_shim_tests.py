#!/usr/bin/env python3
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import os

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tests', 'TestRunner')]
from binary_download import FdbBinaryDownloader, CURRENT_VERSION
from local_cluster import LocalCluster, random_secret_string

LAST_RELEASE_VERSION = "7.1.5"
TESTER_STATS_INTERVAL_SEC = 5
DEFAULT_TEST_FILE = "CApiCorrectnessMultiThr.toml"


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    ver_tuple = version_from_str(ver_str)
    return ver_tuple[0] * 100 + ver_tuple[1] * 10


def version_before(ver_str1, ver_str2):
    return version_from_str(ver_str1) < version_from_str(ver_str2)


class TestEnv(LocalCluster):
    def __init__(
        self,
        build_dir: str,
        downloader: FdbBinaryDownloader,
        version: str,
    ):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        self.tmp_dir = self.build_dir.joinpath("tmp", random_secret_string(16))
        self.tmp_dir.mkdir(parents=True)
        self.downloader = downloader
        self.version = version
        super().__init__(
            self.tmp_dir,
            self.downloader.binary_path(version, "fdbserver"),
            self.downloader.binary_path(version, "fdbmonitor"),
            self.downloader.binary_path(version, "fdbcli"),
            1
        )
        self.set_env_var("LD_LIBRARY_PATH", self.downloader.lib_dir(version))
        client_lib = self.downloader.lib_path(version)
        assert client_lib.exists(), "{} does not exist".format(client_lib)
        self.client_lib_external = self.tmp_dir.joinpath("libfdb_c_external.so")
        shutil.copyfile(client_lib, self.client_lib_external)

    def __enter__(self):
        super().__enter__()
        super().create_database()
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        super().__exit__(xc_type, exc_value, traceback)
        shutil.rmtree(self.tmp_dir)

    def exec_client_command(self, cmd_args, env_vars=None, expected_ret_code=0):
        print("Executing test command: {}".format(
            " ".join([str(c) for c in cmd_args])
        ))
        tester_proc = subprocess.Popen(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env_vars
        )
        tester_retcode = tester_proc.wait()
        assert tester_retcode == expected_ret_code, "Tester completed return code {}, but {} was expected".format(
            tester_retcode, expected_ret_code)


class FdbCShimTests:
    def __init__(
        self,
        args
    ):
        self.build_dir = Path(args.build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(args.build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(args.build_dir)
        self.unit_tests_bin = Path(args.unit_tests_bin).resolve()
        assert self.unit_tests_bin.exists(), "{} does not exist".format(self.unit_tests_bin)
        self.api_tester_bin = Path(args.api_tester_bin).resolve()
        assert self.api_tester_bin.exists(), "{} does not exist".format(self.api_tests_bin)
        self.api_test_dir = Path(args.api_test_dir).resolve()
        assert self.api_test_dir.exists(), "{} does not exist".format(self.api_test_dir)
        self.downloader = FdbBinaryDownloader(args.build_dir)
        # binary downloads are currently available only for x86_64
        self.platform = platform.machine()
        if (self.platform == "x86_64"):
            self.downloader.download_old_binaries(LAST_RELEASE_VERSION)

    def build_c_api_tester_args(self, test_env, test_file):
        test_file_path = self.api_test_dir.joinpath(test_file)
        return [
            self.api_tester_bin,
            "--cluster-file",
            test_env.cluster_file,
            "--test-file",
            test_file_path,
            "--external-client-library",
            test_env.client_lib_external,
            "--disable-local-client",
            "--api-version",
            str(api_version_from_str(test_env.version)),
            "--log",
            "--log-dir",
            test_env.log,
            "--tmp-dir",
            test_env.tmp_dir,
            "--stats-interval",
            str(TESTER_STATS_INTERVAL_SEC * 1000)
        ]

    def run_c_api_test(self, version, test_file):
        print('-' * 80)
        print("C API Test - version: {}, workload: {}".format(version, test_file))
        print('-' * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = self.build_c_api_tester_args(test_env, test_file)
            env_vars = os.environ.copy()
            env_vars["LD_LIBRARY_PATH"] = self.downloader.lib_dir(version)
            test_env.exec_client_command(cmd_args, env_vars)

    def run_c_unit_tests(self, version):
        print('-' * 80)
        print("C Unit Tests - version: {}".format(version))
        print('-' * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = [
                self.unit_tests_bin,
                test_env.cluster_file,
                "fdb",
                test_env.client_lib_external
            ]
            env_vars = os.environ.copy()
            env_vars["LD_LIBRARY_PATH"] = self.downloader.lib_dir(version)
            test_env.exec_client_command(cmd_args, env_vars)

    def test_invalid_c_client_lib_env_var(self, version):
        print('-' * 80)
        print("Test invalid FDB_C_CLIENT_LIBRARY_PATH value")
        print('-' * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = self.build_c_api_tester_args(test_env, DEFAULT_TEST_FILE)
            env_vars = os.environ.copy()
            env_vars["FDB_C_CLIENT_LIBRARY_PATH"] = "dummy"
            test_env.exec_client_command(cmd_args, env_vars, 1)

    def test_valid_c_client_lib_env_var(self, version):
        print('-' * 80)
        print("Test valid FDB_C_CLIENT_LIBRARY_PATH value")
        print('-' * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = self.build_c_api_tester_args(test_env, DEFAULT_TEST_FILE)
            env_vars = os.environ.copy()
            env_vars["FDB_C_CLIENT_LIBRARY_PATH"] = self.downloader.lib_path(version)
            test_env.exec_client_command(cmd_args, env_vars)

    def run_tests(self):
        # binary downloads are currently available only for x86_64
        if (self.platform == "x86_64"):
            self.run_c_api_test(LAST_RELEASE_VERSION, DEFAULT_TEST_FILE)

        self.run_c_api_test(CURRENT_VERSION, DEFAULT_TEST_FILE)
        self.run_c_unit_tests(CURRENT_VERSION)
        self.test_invalid_c_client_lib_env_var(CURRENT_VERSION)
        self.test_valid_c_client_lib_env_var(CURRENT_VERSION)


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
        A script for testing FDB multi-version client in upgrade scenarios. Creates a local cluster,
        generates a workload using fdb_c_api_tester with a specified test file, and performs
        cluster upgrade according to the specified upgrade path. Checks if the workload successfully
        progresses after each upgrade step.
        """,
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        metavar="BUILD_DIRECTORY",
        help="FDB build directory",
        required=True,
    )
    parser.add_argument('--unit-tests-bin', type=str,
                        help='Path to the fdb_c_shim_unit_tests executable.')
    parser.add_argument('--api-tester-bin', type=str,
                        help='Path to the fdb_c_shim_api_tester executable.')
    parser.add_argument('--api-test-dir', type=str,
                        help='Path to a directory with api test definitions.')
    args = parser.parse_args()
    test = FdbCShimTests(args)
    test.run_tests()
