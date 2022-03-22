#!/usr/bin/env python3

from argparse import ArgumentParser, RawDescriptionHelpFormatter
import glob
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import sys
from threading import Thread
import time
from urllib import request

from local_cluster import LocalCluster, random_secret_string


SUPPORTED_PLATFORMS = ["x86_64"]
SUPPORTED_VERSIONS = ["7.1.0", "6.3.23",
                      "6.3.22", "6.3.18", "6.3.17", "6.3.16", "6.3.15", "6.3.13", "6.3.12", "6.3.9", "6.2.30",
                      "6.2.29", "6.2.28", "6.2.27", "6.2.26", "6.2.25", "6.2.24", "6.2.23", "6.2.22", "6.2.21",
                      "6.2.20", "6.2.19", "6.2.18", "6.2.17", "6.2.16", "6.2.15", "6.2.10", "6.1.13", "6.1.12",
                      "6.1.11", "6.1.10", "6.0.18", "6.0.17", "6.0.16", "6.0.15", "6.0.14", "5.2.8", "5.2.7",
                      "5.1.7", "5.1.6"]
FDB_DOWNLOAD_ROOT = "https://github.com/apple/foundationdb/releases/download/"
CURRENT_VERSION = "7.1.0"


def make_executable(path):
    mode = os.stat(path).st_mode
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def version_before(ver_str1, ver_str2):
    return version_from_str(ver_str1) < version_from_str(ver_str2)


class UpgradeTest:
    def __init__(self, build_dir: str, upgrade_path: list, process_number: int = 1, port: str = None):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        self.upgrade_path = upgrade_path
        for version in upgrade_path:
            assert version in SUPPORTED_VERSIONS, "Unsupported version {}".format(
                version)
        self.platform = platform.machine()
        assert self.platform in SUPPORTED_PLATFORMS, "Unsupported platform {}".format(
            self.platform)
        self.tmp_dir = self.build_dir.joinpath(
            "tmp",
            random_secret_string(16)
        )
        self.tmp_dir.mkdir(parents=True)
        self.download_dir = self.build_dir.joinpath(
            "tmp",
            "old_binaries"
        )
        self.download_old_binaries()
        self.create_external_lib_dir()
        init_version = upgrade_path[0]
        self.cluster = LocalCluster(
            self.tmp_dir,
            self.binary_path(init_version, "fdbserver"),
            self.binary_path(init_version, "fdbmonitor"),
            self.binary_path(init_version, "fdbcli"),
            process_number,
            port=port,
            create_config=False
        )
        self.cluster.create_cluster_file()
        self.configure_version(init_version)
        self.log = self.cluster.log
        self.etc = self.cluster.etc
        self.data = self.cluster.data

    def binary_path(self, version, bin_name):
        if version == CURRENT_VERSION:
            return self.build_dir.joinpath("bin", bin_name)
        else:
            return self.download_dir.joinpath(version, bin_name)

    def lib_dir(self, version):
        if version == CURRENT_VERSION:
            return self.build_dir.joinpath("lib")
        else:
            return self.download_dir.joinpath(version)

    def download_old_binary(self, version, target_bin_name, remote_bin_name, executable):
        local_file = self.binary_path(version, target_bin_name)
        if (local_file.exists()):
            return
        self.download_dir.joinpath(version).mkdir(
            parents=True, exist_ok=True)
        remote_file = "{}{}/{}".format(FDB_DOWNLOAD_ROOT,
                                       version, remote_bin_name)
        print("Downloading '{}' to '{}'...".format(remote_file, local_file))
        request.urlretrieve(remote_file, local_file)
        print("Download complete")
        assert local_file.exists(), "{} does not exist".format(local_file)
        if executable:
            make_executable(local_file)

    def download_old_binaries(self):
        for version in self.upgrade_path:
            if version == CURRENT_VERSION:
                continue
            self.download_old_binary(version,
                                     "fdbserver", "fdbserver.{}".format(self.platform), True)
            self.download_old_binary(version,
                                     "fdbmonitor", "fdbmonitor.{}".format(self.platform), True)
            self.download_old_binary(version,
                                     "fdbcli", "fdbcli.{}".format(self.platform), True)
            self.download_old_binary(version,
                                     "libfdb_c.so", "libfdb_c.{}.so".format(self.platform), False)

    def create_external_lib_dir(self):
        self.external_lib_dir = self.tmp_dir.joinpath("client_libs")
        self.external_lib_dir.mkdir(parents=True)
        for version in self.upgrade_path:
            src_file_path = self.lib_dir(version).joinpath("libfdb_c.so")
            assert src_file_path.exists(), "{} does not exist".format(src_file_path)
            target_file_path = self.external_lib_dir.joinpath(
                "libfdb_c.{}.so".format(version))
            shutil.copyfile(src_file_path, target_file_path)

    def health_check(self, timeout_sec=5):
        retries = 0
        while retries < timeout_sec:
            retries += 1
            status = self.cluster.get_status()
            if not "processes" in status["cluster"]:
                print("Health check: no processes found. Retrying")
                time.sleep(1)
                continue
            num_proc = len(status["cluster"]["processes"])
            if (num_proc < self.cluster.process_number):
                print("Health check: {} of {} processes found. Retrying",
                      num_proc, self.cluster.process_number)
                time.sleep(1)
                continue
            assert num_proc == self.cluster.process_number, "Number of processes: expected: {}, actual: {}".format(
                self.cluster.process_number, num_proc)
            for (_, proc_stat) in status["cluster"]["processes"].items():
                proc_ver = proc_stat["version"]
                assert proc_ver == self.cluster_version, "Process version: expected: {}, actual: {}".format(
                    self.cluster_version, proc_ver)
            print("Health check: OK")
            return
        assert False, "Health check: Failed"

    def configure_version(self, version):
        self.cluster.fdbmonitor_binary = self.binary_path(
            version, "fdbmonitor")
        self.cluster.fdbserver_binary = self.binary_path(version, "fdbserver")
        self.cluster.fdbcli_binary = self.binary_path(version, "fdbcli")
        self.cluster.set_env_var = "LD_LIBRARY_PATH", self.lib_dir(version)
        if (version_before(version, "7.1.0")):
            self.cluster.use_legacy_conf_syntax = True
        self.cluster.save_config()
        self.cluster_version = version

    def upgrade_to(self, version):
        print("Upgrading to version {}".format(version))
        self.cluster.stop_cluster()
        self.configure_version(version)
        self.cluster.ensure_ports_released()
        self.cluster.start_cluster()
        print("Upgraded to {}".format(version))

    def __enter__(self):
        print("Starting cluster version {}".format(self.cluster_version))
        self.cluster.start_cluster()
        self.cluster.create_database()
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        self.cluster.stop_cluster()
        shutil.rmtree(self.tmp_dir)

    def exec_workload(self, test_file):
        cmd_args = [self.tester_bin,
                    '--cluster-file', self.cluster.cluster_file,
                    '--test-file', test_file,
                    '--external-client-dir', self.external_lib_dir]
        retcode = subprocess.run(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr,
        ).returncode
        return retcode

    def exec_upgrade_test(self):
        self.health_check()
        for version in self.upgrade_path[1:]:
            self.upgrade_to(version)
            self.health_check()

    def exec_test(self, args):
        self.tester_bin = self.build_dir.joinpath("bin", "fdb_c_api_tester")
        assert self.tester_bin.exists(), "{} does not exist".format(self.tester_bin)

        thread = Thread(target=self.exec_workload, args=(args.test_file))
        thread.start()
        self.exec_upgrade_test()
        retcode = thread.join()
        return retcode

    def check_cluster_logs(self, error_limit=100):
        sev40s = (
            subprocess.getoutput(
                "grep -r 'Severity=\"40\"' {}".format(
                    self.cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

        err_cnt = 0
        for line in sev40s:
            # When running ASAN we expect to see this message. Boost coroutine should be using the correct asan annotations so that it shouldn't produce any false positives.
            if line.endswith(
                "WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"
            ):
                continue
            if (err_cnt < error_limit):
                print(line)
            err_cnt += 1

            if err_cnt > 0:
                print(
                    ">>>>>>>>>>>>>>>>>>>> Found {} severity 40 events - the test fails", err_cnt)
            return err_cnt == 0

    def dump_cluster_logs(self):
        for etc_file in glob.glob(os.path.join(self.cluster.etc, "*")):
            print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(etc_file))
            with open(etc_file, "r") as f:
                print(f.read())
        for log_file in glob.glob(os.path.join(self.cluster.log, "*")):
            print(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
            with open(log_file, "r") as f:
                print(f.read())


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
        TBD
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
        '--upgrade-path',
        nargs='+',
        help='Cluster upgrade path: a space separated list of versions',
        default=[CURRENT_VERSION]
    )
    parser.add_argument(
        '--test-file',
        nargs='+',
        help='A .toml file describing a test workload to be generated with fdb_c_api_tester',
        required=True,
        default=[CURRENT_VERSION]
    )
    parser.add_argument(
        "--process-number",
        "-p",
        help="Number of fdb processes running",
        type=int,
        default=1,
    )
    parser.add_argument(
        '--disable-log-dump',
        help='Do not dump cluster log on error',
        action="store_true"
    )
    args = parser.parse_args()

    errcode = 1
    with UpgradeTest(args.build_dir, args.upgrade_path, args.process_number) as test:
        print("log-dir: {}".format(test.log))
        print("etc-dir: {}".format(test.etc))
        print("data-dir: {}".format(test.data))
        print("cluster-file: {}".format(test.etc.joinpath("fdb.cluster")))
        errcode = test.exec_test(args)
        if test.check_cluster_logs():
            errcode = 1 if errcode == 0 else errcode
        if errcode != 0 and not args.disable_log_dump:
            test.dump_cluster_logs()

    sys.exit(errcode)
