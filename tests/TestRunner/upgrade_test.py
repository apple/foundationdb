#!/usr/bin/env python3

from argparse import ArgumentParser, RawDescriptionHelpFormatter
import glob
import os
from pathlib import Path
import platform
import random
import shutil
import stat
import subprocess
import sys
from threading import Thread, Event
import traceback
import time
from urllib import request
import hashlib

from local_cluster import LocalCluster, random_secret_string


SUPPORTED_PLATFORMS = ["x86_64"]
SUPPORTED_VERSIONS = ["7.2.0", "7.1.1", "7.1.0", "7.0.0", "6.3.24", "6.3.23",
                      "6.3.22", "6.3.18", "6.3.17", "6.3.16", "6.3.15", "6.3.13", "6.3.12", "6.3.9", "6.2.30",
                      "6.2.29", "6.2.28", "6.2.27", "6.2.26", "6.2.25", "6.2.24", "6.2.23", "6.2.22", "6.2.21",
                      "6.2.20", "6.2.19", "6.2.18", "6.2.17", "6.2.16", "6.2.15", "6.2.10", "6.1.13", "6.1.12",
                      "6.1.11", "6.1.10", "6.0.18", "6.0.17", "6.0.16", "6.0.15", "6.0.14", "5.2.8", "5.2.7",
                      "5.1.7", "5.1.6"]
FDB_DOWNLOAD_ROOT = "https://github.com/apple/foundationdb/releases/download/"
CURRENT_VERSION = "7.2.0"
HEALTH_CHECK_TIMEOUT_SEC = 5
PROGRESS_CHECK_TIMEOUT_SEC = 30
TRANSACTION_RETRY_LIMIT = 100
MAX_DOWNLOAD_ATTEMPTS = 5
RUN_WITH_GDB = False


def make_executable(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def remove_file_no_fail(filename):
    try:
        os.remove(filename)
    except OSError:
        pass


def version_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    ver_tuple = version_from_str(ver_str)
    return ver_tuple[0]*100+ver_tuple[1]*10


def version_before(ver_str1, ver_str2):
    return version_from_str(ver_str1) < version_from_str(ver_str2)


def random_sleep(minSec, maxSec):
    timeSec = random.uniform(minSec, maxSec)
    print("Sleeping for {0:.3f}s".format(timeSec))
    time.sleep(timeSec)


def compute_sha256(filename):
    hash = hashlib.sha256()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(128*1024)
            if not data:
                break
            hash.update(data)

    return hash.hexdigest()


def read_to_str(filename):
    with open(filename, 'r') as f:
        return f.read()


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
        self.input_pipe_path = self.tmp_dir.joinpath(
            "input.{}".format(random_secret_string(8)))
        self.output_pipe_path = self.tmp_dir.joinpath(
            "output.{}".format(random_secret_string(8)))
        os.mkfifo(self.input_pipe_path)
        os.mkfifo(self.output_pipe_path)
        self.progress_event = Event()

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

    # Download an old binary of a given version from a remote repository
    def download_old_binary(self, version, target_bin_name, remote_bin_name, makeExecutable):
        local_file = self.binary_path(version, target_bin_name)
        if (local_file.exists()):
            return

        # Download to a temporary file and then replace the target file atomically
        # to avoid consistency errors in case of multiple tests are downloading the
        # same file in parallel
        local_file_tmp = Path("{}.{}".format(
            str(local_file), random_secret_string(8)))
        self.download_dir.joinpath(version).mkdir(
            parents=True, exist_ok=True)
        remote_file = "{}{}/{}".format(FDB_DOWNLOAD_ROOT,
                                       version, remote_bin_name)
        remote_sha256 = "{}.sha256".format(remote_file)
        local_sha256 = Path("{}.sha256".format(local_file_tmp))

        for attempt_cnt in range(MAX_DOWNLOAD_ATTEMPTS):
            print("Downloading '{}' to '{}'...".format(
                remote_file, local_file_tmp))
            request.urlretrieve(remote_file, local_file_tmp)
            print("Downloading '{}' to '{}'...".format(
                remote_sha256, local_sha256))
            request.urlretrieve(remote_sha256, local_sha256)
            print("Download complete")
            assert local_file_tmp.exists(), "{} does not exist".format(local_file_tmp)
            assert local_sha256.exists(), "{} does not exist".format(local_sha256)
            expected_checksum = read_to_str(local_sha256)
            actual_checkum = compute_sha256(local_file_tmp)
            if (expected_checksum == actual_checkum):
                print("Checksum OK")
                break
            print("Checksum mismatch. Expected: {} Actual: {}".format(
                expected_checksum, actual_checkum))
            if attempt_cnt == MAX_DOWNLOAD_ATTEMPTS-1:
                assert False, "Failed to download {} after {} attempts".format(
                    local_file_tmp, MAX_DOWNLOAD_ATTEMPTS)

        os.rename(local_file_tmp, local_file)
        os.remove(local_sha256)

        if makeExecutable:
            make_executable(local_file)

    # Download all old binaries required for testing the specified upgrade path
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

    # Create a directory for external client libraries for MVC and fill it
    # with the libraries necessary for the specified upgrade path
    def create_external_lib_dir(self):
        self.external_lib_dir = self.tmp_dir.joinpath("client_libs")
        self.external_lib_dir.mkdir(parents=True)
        for version in self.upgrade_path:
            src_file_path = self.lib_dir(version).joinpath("libfdb_c.so")
            assert src_file_path.exists(), "{} does not exist".format(src_file_path)
            target_file_path = self.external_lib_dir.joinpath(
                "libfdb_c.{}.so".format(version))
            shutil.copyfile(src_file_path, target_file_path)

    # Perform a health check of the cluster: Use fdbcli status command to check if the number of
    # server processes and their versions are as expected
    def health_check(self, timeout_sec=HEALTH_CHECK_TIMEOUT_SEC):
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
                print("Health check: {} of {} processes found. Retrying".format(
                    num_proc, self.cluster.process_number))
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

    # Create and save a cluster configuration for the given version
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

    # Upgrade the cluster to the given version
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
        self.cluster.create_database(enable_tenants=False)
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        self.cluster.stop_cluster()
        shutil.rmtree(self.tmp_dir)

     # Determine FDB API version matching the upgrade path
    def determine_api_version(self):
        self.api_version = api_version_from_str(CURRENT_VERSION)
        for version in self.upgrade_path:
            self.api_version = min(
                api_version_from_str(version), self.api_version)

    # Start the tester to generate the workload specified by the test file
    def exec_workload(self, test_file):
        self.tester_retcode = 1
        try:
            self.determine_api_version()
            cmd_args = [self.tester_bin,
                        '--cluster-file', self.cluster.cluster_file,
                        '--test-file', test_file,
                        '--external-client-dir', self.external_lib_dir,
                        '--disable-local-client',
                        '--input-pipe', self.input_pipe_path,
                        '--output-pipe', self.output_pipe_path,
                        '--api-version', str(self.api_version),
                        '--log',
                        '--log-dir', self.log,
                        '--tmp-dir', self.tmp_dir,
                        '--transaction-retry-limit', str(TRANSACTION_RETRY_LIMIT)]
            if (RUN_WITH_GDB):
                cmd_args = ['gdb', '-ex', 'run', '--args'] + cmd_args
            print("Executing test command: {}".format(
                " ".join([str(c) for c in cmd_args])))

            self.tester_proc = subprocess.Popen(
                cmd_args, stdout=sys.stdout, stderr=sys.stderr)
            self.tester_retcode = self.tester_proc.wait()
            self.tester_proc = None

            if (self.tester_retcode != 0):
                print("Tester failed with return code {}".format(
                    self.tester_retcode))
        except Exception:
            print("Execution of test workload failed")
            print(traceback.format_exc())

    # Perform a progress check: Trigger it and wait until it is completed

    def progress_check(self, ctrl_pipe):
        self.progress_event.clear()
        os.write(ctrl_pipe, b"CHECK\n")
        self.progress_event.wait(
            None if RUN_WITH_GDB else PROGRESS_CHECK_TIMEOUT_SEC)
        if (self.progress_event.is_set()):
            print("Progress check: OK")
        else:
            assert False, "Progress check failed after upgrade to version {}".format(
                self.cluster_version)

    # The main function of a thread for reading and processing
    # the notifications received from the tester
    def output_pipe_reader(self):
        try:
            print("Opening pipe {} for reading".format(self.output_pipe_path))
            self.output_pipe = open(self.output_pipe_path, 'r')
            for line in self.output_pipe:
                msg = line.strip()
                print("Received {}".format(msg))
                if (msg == "CHECK_OK"):
                    self.progress_event.set()
            self.output_pipe.close()
        except Exception as e:
            print("Error while reading output pipe", e)
            print(traceback.format_exc())

    # Execute the upgrade test workflow according to the specified
    # upgrade path: perform the upgrade steps and check success after each step
    def exec_upgrade_test(self):
        print("Opening pipe {} for writing".format(self.input_pipe_path))
        ctrl_pipe = os.open(self.input_pipe_path, os.O_WRONLY)
        try:
            self.health_check()
            self.progress_check(ctrl_pipe)
            for version in self.upgrade_path[1:]:
                random_sleep(0.0, 2.0)
                self.upgrade_to(version)
                self.health_check()
                self.progress_check(ctrl_pipe)
            os.write(ctrl_pipe, b"STOP\n")
        finally:
            os.close(ctrl_pipe)

    # Kill the tester process if it is still alive
    def kill_tester_if_alive(self, workload_thread):
        if not workload_thread.is_alive():
            return
        if self.tester_proc is not None:
            try:
                print("Killing the tester process")
                self.tester_proc.kill()
                workload_thread.join(5)
            except:
                print("Failed to kill the tester process")

    # The main method implementing the test:
    # - Start a thread for generating the workload using a tester binary
    # - Start a thread for reading notifications from the tester
    # - Trigger the upgrade steps and checks in the main thread
    def exec_test(self, args):
        self.tester_bin = self.build_dir.joinpath("bin", "fdb_c_api_tester")
        assert self.tester_bin.exists(), "{} does not exist".format(self.tester_bin)
        self.tester_proc = None
        test_retcode = 1
        try:
            workload_thread = Thread(
                target=self.exec_workload, args=(args.test_file,))
            workload_thread.start()

            reader_thread = Thread(target=self.output_pipe_reader)
            reader_thread.start()

            self.exec_upgrade_test()
            test_retcode = 0
        except Exception:
            print("Upgrade test failed")
            print(traceback.format_exc())
            self.kill_tester_if_alive(workload_thread)
        finally:
            workload_thread.join(5)
            reader_thread.join(5)
            self.kill_tester_if_alive(workload_thread)
            if test_retcode == 0:
                test_retcode = self.tester_retcode
        return test_retcode

    def grep_logs_for_events(self, severity):
        return (
            subprocess.getoutput(
                "grep -r 'Severity=\"{}\"' {}".format(
                    severity,
                    self.cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

    # Check the cluster log for errors
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
            # When running ASAN we expect to see this message. Boost coroutine should be using the
            # correct asan annotations so that it shouldn't produce any false positives.
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
        else:
            print("No errors found in logs")
        return err_cnt == 0

    # Check the server and client logs for warnings and dump them
    def dump_warnings_in_logs(self, limit=100):
        sev30s = (
            subprocess.getoutput(
                "grep -r 'Severity=\"30\"' {}".format(
                    self.cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

        if (len(sev30s) == 0):
            print("No warnings found in logs")
        else:
            print(">>>>>>>>>>>>>>>>>>>> Found {} severity 30 events (warnings):".format(
                len(sev30s)))
            for line in sev30s[:limit]:
                print(line)

    # Dump the last cluster configuration and cluster logs
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
        '--upgrade-path',
        nargs='+',
        help='Cluster upgrade path: a space separated list of versions',
        default=[CURRENT_VERSION]
    )
    parser.add_argument(
        '--test-file',
        help='A .toml file describing a test workload to be generated with fdb_c_api_tester',
        required=True,
    )
    parser.add_argument(
        "--process-number",
        "-p",
        help="Number of fdb processes running (default: 0 - random)",
        type=int,
        default=0,
    )
    parser.add_argument(
        '--disable-log-dump',
        help='Do not dump cluster log on error',
        action="store_true"
    )
    parser.add_argument(
        '--run-with-gdb',
        help='Execute the tester binary from gdb',
        action="store_true"
    )
    args = parser.parse_args()
    if (args.process_number == 0):
        args.process_number = random.randint(1, 5)
        print("Testing with {} processes".format(args.process_number))

    if (args.run_with_gdb):
        RUN_WITH_GDB = True

    errcode = 1
    with UpgradeTest(args.build_dir, args.upgrade_path, args.process_number) as test:
        print("log-dir: {}".format(test.log))
        print("etc-dir: {}".format(test.etc))
        print("data-dir: {}".format(test.data))
        print("cluster-file: {}".format(test.etc.joinpath("fdb.cluster")))
        errcode = test.exec_test(args)
        if not test.check_cluster_logs():
            errcode = 1 if errcode == 0 else errcode
        test.dump_warnings_in_logs()
        if errcode != 0 and not args.disable_log_dump:
            test.dump_cluster_logs()

    sys.exit(errcode)
