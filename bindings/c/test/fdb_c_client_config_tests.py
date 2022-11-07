#!/usr/bin/env python3
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import os
import glob

sys.path[:0] = [os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "TestRunner")]

# fmt: off
from binary_download import FdbBinaryDownloader, CURRENT_VERSION
from local_cluster import LocalCluster, random_secret_string
# fmt: on

PREV_RELEASE_VERSION = "7.1.5"
PREV_PREV_RELEASE_VERSION = "7.0.0"


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    ver_tuple = version_from_str(ver_str)
    return ver_tuple[0] * 100 + ver_tuple[1] * 10


class TestEnv(LocalCluster):
    def __init__(
        self,
        args,
        downloader: FdbBinaryDownloader,
        version: str,
    ):
        self.client_config_tester_bin = Path(args.client_config_tester_bin).resolve()
        assert self.client_config_tester_bin.exists(), "{} does not exist".format(self.client_config_tester_bin)
        self.build_dir = Path(args.build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(args.build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(args.build_dir)
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
        self.failed_cnt = 0

    def __enter__(self):
        super().__enter__()
        super().create_database()
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        super().__exit__(xc_type, exc_value, traceback)
        shutil.rmtree(self.tmp_dir)


class ClientConfigTest:
    def __init__(self, test_env: TestEnv, title: str):
        self.test_env = test_env
        self.title = title
        self.external_lib_dir = None
        self.external_lib_path = None
        self.test_dir = self.test_env.tmp_dir.joinpath(random_secret_string(16))
        self.test_dir.mkdir(parents=True)
        self.log_dir = self.test_dir.joinpath("log")
        self.log_dir.mkdir(parents=True)
        self.tmp_dir = self.test_dir.joinpath("tmp")
        self.tmp_dir.mkdir(parents=True)
        self.disable_local_client = False
        self.ignore_external_client_failures = False
        self.api_version = None
        self.expected_error = None
        self.transaction_timeout = None

    def create_external_lib_dir(self, versions):
        self.external_lib_dir = self.test_dir.joinpath("extclients")
        self.external_lib_dir.mkdir(parents=True)
        for version in versions:
            src_file_path = self.test_env.downloader.lib_path(version)
            assert src_file_path.exists(), "{} does not exist".format(src_file_path)
            target_file_path = self.external_lib_dir.joinpath("libfdb_c.{}.so".format(version))
            shutil.copyfile(src_file_path, target_file_path)
            assert target_file_path.exists(), "{} does not exist".format(target_file_path)

    def create_external_lib_path(self, version):
        src_file_path = self.test_env.downloader.lib_path(version)
        assert src_file_path.exists(), "{} does not exist".format(src_file_path)
        self.external_lib_path = self.test_dir.joinpath("libfdb_c.{}.so".format(version))
        shutil.copyfile(src_file_path, self.external_lib_path)
        assert self.external_lib_path.exists(), "{} does not exist".format(self.external_lib_path)

    def dump_client_logs(self):
        for log_file in glob.glob(os.path.join(self.log_dir, "*")):
            print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
            with open(log_file, "r") as f:
                print(f.read())
            print(">>>>>>>>>>>>>>>>>>>> End of {}:".format(log_file))

    def exec(self):
        print("-" * 80)
        print(self.title)
        print("-" * 80)
        cmd_args = [self.test_env.client_config_tester_bin, "--cluster-file", self.test_env.cluster_file]

        if self.tmp_dir is not None:
            cmd_args += ["--tmp-dir", self.tmp_dir]

        if self.log_dir is not None:
            cmd_args += ["--log", "--log-dir", self.log_dir]

        if self.disable_local_client:
            cmd_args += ["--disable-local-client"]

        if self.external_lib_path is not None:
            cmd_args += ["--external-client-library", self.external_lib_path]

        if self.external_lib_dir is not None:
            cmd_args += ["--external-client-dir", self.external_lib_dir]

        if self.ignore_external_client_failures:
            cmd_args += ["--ignore-external-client-failures"]

        if self.api_version is not None:
            cmd_args += ["--api-version", str(self.api_version)]

        if self.expected_error is not None:
            cmd_args += ["--expected-error", str(self.expected_error)]

        if self.transaction_timeout is not None:
            cmd_args += ["--transaction-timeout", str(self.transaction_timeout)]

        print("Executing test command: {}".format(" ".join([str(c) for c in cmd_args])))
        tester_proc = subprocess.Popen(cmd_args, stdout=sys.stdout, stderr=sys.stderr)
        tester_retcode = tester_proc.wait()
        if tester_retcode != 0:
            print("Test '{}' failed".format(self.title))
            self.test_env.failed_cnt += 1

        self.cleanup()

    def cleanup(self):
        shutil.rmtree(self.test_dir)


class ClientConfigTests:
    def __init__(self, args):
        self.args = args
        self.downloader = FdbBinaryDownloader(args.build_dir)
        # binary downloads are currently available only for x86_64
        self.platform = platform.machine()
        if self.platform == "x86_64":
            self.downloader.download_old_binaries(PREV_RELEASE_VERSION)
            self.downloader.download_old_binaries(PREV_PREV_RELEASE_VERSION)

    def test_local_client_only(self, test_env):
        test = ClientConfigTest(test_env, "Local client only")
        test.exec()

    def test_single_external_client_only(self, test_env):
        test = ClientConfigTest(test_env, "Single external client")
        test.create_external_lib_path(CURRENT_VERSION)
        test.disable_local_client = True
        test.exec()

    def test_same_local_and_external_client(self, test_env):
        test = ClientConfigTest(test_env, "Same Local & External Client")
        test.create_external_lib_path(CURRENT_VERSION)
        test.exec()

    def test_multiple_external_clients(self, test_env):
        test = ClientConfigTest(test_env, "Multiple external clients")
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_PREV_RELEASE_VERSION)
        test.exec()

    def test_no_external_client_support_api_version(self, test_env):
        test = ClientConfigTest(test_env, "Multiple external clients; API version supported by none")
        test.create_external_lib_dir([PREV_PREV_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_no_external_client_support_api_version_ignore(self, test_env):
        test = ClientConfigTest(test_env, "Multiple external clients; API version supported by none; Ignore failures")
        test.create_external_lib_dir([PREV_PREV_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.ignore_external_client_failures = True
        test.expected_error = 2124  # All external clients failed
        test.exec()

    def test_one_external_client_wrong_api_version(self, test_env):
        test = ClientConfigTest(test_env, "Multiple external clients: API version unsupported by one")
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_one_external_client_wrong_api_version_ignore(self, test_env):
        test = ClientConfigTest(test_env, "Multiple external clients;  API version unsupported by one; Ignore failures")
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.ignore_external_client_failures = True
        test.exec()

    def test_prev_release_with_ext_client(self, test_env):
        test = ClientConfigTest(test_env, "Cluster with previous release version")
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.exec()

    def test_prev_release_with_ext_client_unsupported_api(self, test_env):
        test = ClientConfigTest(test_env, "Cluster with previous release version; Unsupported API version")
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_prev_release_with_ext_client_unsupported_api_ignore(self, test_env):
        test = ClientConfigTest(
            test_env, "Cluster with previous release version; Unsupported API version; Ignore failures"
        )
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.transaction_timeout = 100
        test.expected_error = 1031  # Timeout
        test.ignore_external_client_failures = True
        test.exec()

    def run_tests(self):
        failed_cnt = 0
        with TestEnv(self.args, self.downloader, CURRENT_VERSION) as test_env:
            self.test_local_client_only(test_env)
            self.test_single_external_client_only(test_env)
            self.test_same_local_and_external_client(test_env)
            self.test_multiple_external_clients(test_env)
            self.test_no_external_client_support_api_version(test_env)
            self.test_no_external_client_support_api_version_ignore(test_env)
            self.test_one_external_client_wrong_api_version(test_env)
            self.test_one_external_client_wrong_api_version_ignore(test_env)
            failed_cnt += test_env.failed_cnt

        if self.platform == "x86_64":
            with TestEnv(self.args, self.downloader, PREV_RELEASE_VERSION) as test_env:
                self.test_prev_release_with_ext_client(test_env)
                self.test_prev_release_with_ext_client_unsupported_api(test_env)
                self.test_prev_release_with_ext_client_unsupported_api_ignore(test_env)
                failed_cnt += test_env.failed_cnt

        if failed_cnt > 0:
            print("{} tests failed".format(failed_cnt))
        else:
            print("All tests successful")
        return failed_cnt


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
        "--client-config-tester-bin",
        type=str,
        help="Path to the fdb_c_client_config_tester executable.",
        required=True,
    )
    args = parser.parse_args()
    test = ClientConfigTests(args)
    failed_cnt = test.run_tests()
    sys.exit(failed_cnt)
