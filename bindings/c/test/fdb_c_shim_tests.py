#!/usr/bin/env python3
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import os
from binary_download import FdbBinaryDownloader
from local_cluster import LocalCluster, random_secret_string
from fdb_version import CURRENT_VERSION, PREV_RELEASE_VERSION

TESTER_STATS_INTERVAL_SEC = 5
DEFAULT_TEST_FILE = "CApiCorrectnessMultiThr.toml"
IMPLIBSO_ERROR_CODE = -6  # SIGABORT


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
            1,
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
        print("Executing test command: {}".format(" ".join([str(c) for c in cmd_args])))
        tester_proc = subprocess.Popen(cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env_vars)
        tester_retcode = tester_proc.wait()
        assert tester_retcode == expected_ret_code, "Tester completed return code {}, but {} was expected".format(
            tester_retcode, expected_ret_code
        )


class FdbCShimTests:
    def __init__(self, args):
        self.build_dir = Path(args.build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(args.build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(args.build_dir)
        self.unit_tests_bin = Path(args.unit_tests_bin).resolve()
        assert self.unit_tests_bin.exists(), "{} does not exist".format(self.unit_tests_bin)
        self.api_tester_bin = Path(args.api_tester_bin).resolve()
        assert self.api_tester_bin.exists(), "{} does not exist".format(self.api_tests_bin)
        self.shim_lib_tester_bin = Path(args.shim_lib_tester_bin).resolve()
        assert self.shim_lib_tester_bin.exists(), "{} does not exist".format(self.shim_lib_tester_bin)
        self.api_test_dir = Path(args.api_test_dir).resolve()
        assert self.api_test_dir.exists(), "{} does not exist".format(self.api_test_dir)
        self.downloader = FdbBinaryDownloader(args.build_dir)
        # binary downloads are currently available only for x86_64
        self.platform = platform.machine()
        if self.platform == "x86_64":
            self.downloader.download_old_binaries(PREV_RELEASE_VERSION)
            self.downloader.download_old_binaries("7.0.0")

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
            str(TESTER_STATS_INTERVAL_SEC * 1000),
        ]

    def run_c_api_test(self, version, test_file):
        print("-" * 80)
        print("C API Test - version: {}, workload: {}".format(version, test_file))
        print("-" * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = self.build_c_api_tester_args(test_env, test_file)
            env_vars = os.environ.copy()
            env_vars["FDB_LOCAL_CLIENT_LIBRARY_PATH"] = self.downloader.lib_path(version)
            test_env.exec_client_command(cmd_args, env_vars)

    def run_c_unit_tests(self, version):
        print("-" * 80)
        print("C Unit Tests - version: {}".format(version))
        print("-" * 80)
        with TestEnv(self.build_dir, self.downloader, version) as test_env:
            cmd_args = [self.unit_tests_bin, test_env.cluster_file, "fdb", test_env.client_lib_external]
            env_vars = os.environ.copy()
            env_vars["FDB_LOCAL_CLIENT_LIBRARY_PATH"] = self.downloader.lib_path(version)
            test_env.exec_client_command(cmd_args, env_vars)

    def run_c_shim_lib_tester(
        self,
        version,
        test_env,
        api_version=None,
        invalid_lib_path=False,
        call_set_path=False,
        set_env_path=False,
        set_ld_lib_path=False,
        use_external_lib=True,
        expected_ret_code=0,
    ):
        print("-" * 80)
        if api_version is None:
            api_version = api_version_from_str(version)
        test_flags = []
        if invalid_lib_path:
            test_flags.append("invalid_lib_path")
        if call_set_path:
            test_flags.append("call_set_path")
        if set_ld_lib_path:
            test_flags.append("set_ld_lib_path")
        if use_external_lib:
            test_flags.append("use_external_lib")
        else:
            test_flags.append("use_local_lib")
        print("C Shim Tests - version: {}, API version: {}, {}".format(version, api_version, ", ".join(test_flags)))
        print("-" * 80)
        cmd_args = [
            self.shim_lib_tester_bin,
            "--cluster-file",
            test_env.cluster_file,
            "--api-version",
            str(api_version),
        ]
        if call_set_path:
            cmd_args = cmd_args + [
                "--local-client-library",
                ("dummy" if invalid_lib_path else self.downloader.lib_path(version)),
            ]
        if use_external_lib:
            cmd_args = cmd_args + ["--disable-local-client", "--external-client-library", test_env.client_lib_external]
        env_vars = os.environ.copy()
        env_vars["LD_LIBRARY_PATH"] = self.downloader.lib_dir(version) if set_ld_lib_path else ""
        if set_env_path:
            env_vars["FDB_LOCAL_CLIENT_LIBRARY_PATH"] = (
                "dummy" if invalid_lib_path else self.downloader.lib_path(version)
            )
        test_env.exec_client_command(cmd_args, env_vars, expected_ret_code)

    def run_tests(self):
        # Test the API workload with the dev version
        self.run_c_api_test(CURRENT_VERSION, DEFAULT_TEST_FILE)

        # Run unit tests with the dev version
        self.run_c_unit_tests(CURRENT_VERSION)

        with TestEnv(self.build_dir, self.downloader, CURRENT_VERSION) as test_env:
            # Test lookup of the client library over LD_LIBRARY_PATH
            self.run_c_shim_lib_tester(CURRENT_VERSION, test_env, set_ld_lib_path=True)

            # Test setting the client library path over an API call
            self.run_c_shim_lib_tester(CURRENT_VERSION, test_env, call_set_path=True)

            # Test setting the client library path over an environment variable
            self.run_c_shim_lib_tester(CURRENT_VERSION, test_env, set_env_path=True)

            # Test using the loaded client library as the local client
            self.run_c_shim_lib_tester(CURRENT_VERSION, test_env, call_set_path=True, use_external_lib=False)

            # Test setting an invalid client library path over an API call
            self.run_c_shim_lib_tester(
                CURRENT_VERSION,
                test_env,
                call_set_path=True,
                invalid_lib_path=True,
                expected_ret_code=IMPLIBSO_ERROR_CODE,
            )

            # Test setting an invalid client library path over an environment variable
            self.run_c_shim_lib_tester(
                CURRENT_VERSION,
                test_env,
                set_env_path=True,
                invalid_lib_path=True,
                expected_ret_code=IMPLIBSO_ERROR_CODE,
            )

            # Test calling a function that exists in the loaded library, but not for the selected API version
            self.run_c_shim_lib_tester(CURRENT_VERSION, test_env, call_set_path=True, api_version=700)

        # binary downloads are currently available only for x86_64
        if self.platform == "x86_64":
            # Test the API workload with the release version
            self.run_c_api_test(PREV_RELEASE_VERSION, DEFAULT_TEST_FILE)

            with TestEnv(self.build_dir, self.downloader, PREV_RELEASE_VERSION) as test_env:
                # Test using the loaded client library as the local client
                self.run_c_shim_lib_tester(PREV_RELEASE_VERSION, test_env, call_set_path=True, use_external_lib=False)

                # Test the client library of the release version in combination with the dev API version
                self.run_c_shim_lib_tester(
                    PREV_RELEASE_VERSION,
                    test_env,
                    call_set_path=True,
                    api_version=api_version_from_str(CURRENT_VERSION),
                    expected_ret_code=1,
                )

                # Test calling a function that does not exist in the loaded library
                self.run_c_shim_lib_tester(
                    "7.0.0", test_env, call_set_path=True, api_version=700, expected_ret_code=IMPLIBSO_ERROR_CODE
                )


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
    parser.add_argument(
        "--unit-tests-bin", type=str, help="Path to the fdb_c_shim_unit_tests executable.", required=True
    )
    parser.add_argument(
        "--api-tester-bin", type=str, help="Path to the fdb_c_shim_api_tester executable.", required=True
    )
    parser.add_argument(
        "--shim-lib-tester-bin", type=str, help="Path to the fdb_c_shim_lib_tester executable.", required=True
    )
    parser.add_argument(
        "--api-test-dir", type=str, help="Path to a directory with api test definitions.", required=True
    )
    args = parser.parse_args()
    test = FdbCShimTests(args)
    test.run_tests()
