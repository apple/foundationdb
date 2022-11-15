#!/usr/bin/env python3
import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import os
import glob
import unittest

sys.path[:0] = [os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "TestRunner")]

# fmt: off
from binary_download import FdbBinaryDownloader, CURRENT_VERSION
from local_cluster import LocalCluster, random_secret_string
# fmt: on

PREV_RELEASE_VERSION = "7.1.5"
PREV_PREV_RELEASE_VERSION = "7.0.0"

args = None
downloader = None


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    ver_tuple = version_from_str(ver_str)
    return ver_tuple[0] * 100 + ver_tuple[1] * 10


class TestCluster(LocalCluster):
    def __init__(
        self,
        version: str,
    ):
        self.client_config_tester_bin = Path(args.client_config_tester_bin).resolve()
        assert self.client_config_tester_bin.exists(), "{} does not exist".format(self.client_config_tester_bin)
        self.build_dir = Path(args.build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(args.build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(args.build_dir)
        self.tmp_dir = self.build_dir.joinpath("tmp", random_secret_string(16))
        self.tmp_dir.mkdir(parents=True)
        self.version = version
        super().__init__(
            self.tmp_dir,
            downloader.binary_path(version, "fdbserver"),
            downloader.binary_path(version, "fdbmonitor"),
            downloader.binary_path(version, "fdbcli"),
            1,
        )
        self.set_env_var("LD_LIBRARY_PATH", downloader.lib_dir(version))

    def setup(self):
        self.__enter__()
        self.create_database()

    def tearDown(self):
        self.__exit__(None, None, None)
        shutil.rmtree(self.tmp_dir)


# Client configuration tests using a cluster of the current version
class ClientConfigTest:
    def __init__(self, tc: unittest.TestCase):
        self.tc = tc
        self.cluster = tc.cluster
        self.external_lib_dir = None
        self.external_lib_path = None
        self.test_dir = self.cluster.tmp_dir.joinpath(random_secret_string(16))
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
            src_file_path = downloader.lib_path(version)
            self.tc.assertTrue(src_file_path.exists(), "{} does not exist".format(src_file_path))
            target_file_path = self.external_lib_dir.joinpath("libfdb_c.{}.so".format(version))
            shutil.copyfile(src_file_path, target_file_path)
            self.tc.assertTrue(target_file_path.exists(), "{} does not exist".format(target_file_path))

    def create_external_lib_path(self, version):
        src_file_path = downloader.lib_path(version)
        self.tc.assertTrue(src_file_path.exists(), "{} does not exist".format(src_file_path))
        self.external_lib_path = self.test_dir.joinpath("libfdb_c.{}.so".format(version))
        shutil.copyfile(src_file_path, self.external_lib_path)
        self.tc.assertTrue(self.external_lib_path.exists(), "{} does not exist".format(self.external_lib_path))

    def dump_client_logs(self):
        for log_file in glob.glob(os.path.join(self.log_dir, "*")):
            print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file), file=sys.stderr)
            with open(log_file, "r") as f:
                print(f.read(), file=sys.stderr)
            print(">>>>>>>>>>>>>>>>>>>> End of {}:".format(log_file), file=sys.stderr)

    def exec(self):
        cmd_args = [self.cluster.client_config_tester_bin, "--cluster-file", self.cluster.cluster_file]

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

        print("\nExecuting test command: {}".format(" ".join([str(c) for c in cmd_args])), file=sys.stderr)
        try:
            tester_proc = subprocess.Popen(cmd_args, stdout=sys.stdout, stderr=sys.stderr)
            tester_retcode = tester_proc.wait()
            self.tc.assertEqual(0, tester_retcode)
        finally:
            self.cleanup()

    def cleanup(self):
        shutil.rmtree(self.test_dir)


class ClientConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(CURRENT_VERSION)
        cls.cluster.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.tearDown()

    def test_local_client_only(self):
        # Local client only
        test = ClientConfigTest(self)
        test.exec()

    def test_single_external_client_only(self):
        # Single external client only
        test = ClientConfigTest(self)
        test.create_external_lib_path(CURRENT_VERSION)
        test.disable_local_client = True
        test.exec()

    def test_same_local_and_external_client(self):
        # Same version local & external client
        test = ClientConfigTest(self)
        test.create_external_lib_path(CURRENT_VERSION)
        test.exec()

    def test_multiple_external_clients(self):
        # Multiple external clients, normal case
        test = ClientConfigTest(self)
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(PREV_PREV_RELEASE_VERSION)
        test.exec()

    def test_no_external_client_support_api_version(self):
        # Multiple external clients, API version supported by none of them
        test = ClientConfigTest(self)
        test.create_external_lib_dir([PREV_PREV_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_no_external_client_support_api_version_ignore(self):
        # Multiple external clients; API version supported by none of them; Ignore failures
        test = ClientConfigTest(self)
        test.create_external_lib_dir([PREV_PREV_RELEASE_VERSION, PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.ignore_external_client_failures = True
        test.expected_error = 2124  # All external clients failed
        test.exec()

    def test_one_external_client_wrong_api_version(self):
        # Multiple external clients, API version unsupported by one of othem
        test = ClientConfigTest(self)
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_one_external_client_wrong_api_version_ignore(self):
        # Multiple external clients;  API version unsupported by one of them; Ignore failures
        test = ClientConfigTest(self)
        test.create_external_lib_dir([CURRENT_VERSION, PREV_RELEASE_VERSION, PREV_PREV_RELEASE_VERSION])
        test.disable_local_client = True
        test.api_version = api_version_from_str(CURRENT_VERSION)
        test.ignore_external_client_failures = True
        test.exec()


# Client configuration tests using a cluster of previous release version
class ClientConfigPrevVersionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster(PREV_RELEASE_VERSION)
        cls.cluster.setup()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.tearDown()

    def test_external_client(self):
        # Using an external client to connect
        test = ClientConfigTest(self)
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.api_version = api_version_from_str(PREV_RELEASE_VERSION)
        test.exec()

    def test_prev_release_with_ext_client_unsupported_api(self):
        # Leaving an unsupported API version
        test = ClientConfigTest(self)
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.expected_error = 2204  # API function missing
        test.exec()

    def test_prev_release_with_ext_client_unsupported_api_ignore(self):
        # Leaving an unsupported API version, ignore failures
        test = ClientConfigTest(self)
        test.create_external_lib_path(PREV_RELEASE_VERSION)
        test.transaction_timeout = 100
        test.expected_error = 1031  # Timeout
        test.ignore_external_client_failures = True
        test.exec()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
        Unit tests for running FDB client with different configurations. 
        Also accepts python unit tests command line arguments.
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
    parser.add_argument("unittest_args", nargs=argparse.REMAINDER)

    args = parser.parse_args()
    sys.argv[1:] = args.unittest_args

    downloader = FdbBinaryDownloader(args.build_dir)
    downloader.download_old_binaries(PREV_RELEASE_VERSION)
    downloader.download_old_binaries(PREV_PREV_RELEASE_VERSION)

    unittest.main(verbosity=2)
