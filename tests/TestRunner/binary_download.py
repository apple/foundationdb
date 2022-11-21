#!/usr/bin/env python3

import os
from pathlib import Path
import platform
import shutil
import stat
from urllib import request
import hashlib

from local_cluster import random_secret_string

CURRENT_VERSION = "71.2.4"
FUTURE_VERSION = "71.3.0"

SUPPORTED_PLATFORMS = ["x86_64", "aarch64"]
FDB_DOWNLOAD_ROOT = "https://github.com/apple/foundationdb/releases/download/"
LOCAL_OLD_BINARY_REPO = "/opt/foundationdb/old/"
MAX_DOWNLOAD_ATTEMPTS = 5


def make_executable_path(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def compute_sha256(filename):
    hash_function = hashlib.sha256()
    with open(filename, "rb") as f:
        while True:
            data = f.read(128 * 1024)
            if not data:
                break
            hash_function.update(data)

    return hash_function.hexdigest()


def read_to_str(filename):
    with open(filename, "r") as f:
        return f.read()


def is_local_build_version(version):
    return version == CURRENT_VERSION or version == FUTURE_VERSION


class FdbBinaryDownloader:
    def __init__(self, build_dir):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        self.platform = platform.machine()
        assert self.platform in SUPPORTED_PLATFORMS, "Unsupported platform {}".format(self.platform)
        self.tmp_dir = self.build_dir.joinpath("tmp", random_secret_string(16))
        self.tmp_dir.mkdir(parents=True)
        self.download_dir = self.build_dir.joinpath("tmp", "old_binaries")
        self.local_binary_repo = Path(LOCAL_OLD_BINARY_REPO)
        if not self.local_binary_repo.exists():
            self.local_binary_repo = None

    # Check if the binaries for the given version are available in the local old binaries repository
    def version_in_local_repo(self, version):
        return (self.local_binary_repo is not None) and (self.local_binary_repo.joinpath(version).exists())

    def binary_path(self, version, bin_name):
        if is_local_build_version(version):
            return self.build_dir.joinpath("bin", bin_name)
        elif self.version_in_local_repo(version):
            return self.local_binary_repo.joinpath(version, "bin", "{}-{}".format(bin_name, version))
        else:
            return self.download_dir.joinpath(version, bin_name)

    def lib_dir(self, version):
        if is_local_build_version(version):
            return self.build_dir.joinpath("lib")
        else:
            return self.download_dir.joinpath(version)

    def lib_path(self, version):
        return self.lib_dir(version).joinpath("libfdb_c.so")

    # Download an old binary of a given version from a remote repository
    def download_old_binary(self, version, target_bin_name, remote_bin_name, make_executable):
        local_file = self.download_dir.joinpath(version, target_bin_name)
        if local_file.exists():
            return

        # Download to a temporary file and then replace the target file atomically
        # to avoid consistency errors in case of multiple tests are downloading the
        # same file in parallel
        local_file_tmp = Path("{}.{}".format(str(local_file), random_secret_string(8)))
        self.download_dir.joinpath(version).mkdir(parents=True, exist_ok=True)
        remote_file = "{}{}/{}".format(FDB_DOWNLOAD_ROOT, version, remote_bin_name)
        remote_sha256 = "{}.sha256".format(remote_file)
        local_sha256 = Path("{}.sha256".format(local_file_tmp))

        for attempt_cnt in range(MAX_DOWNLOAD_ATTEMPTS + 1):
            if attempt_cnt == MAX_DOWNLOAD_ATTEMPTS:
                assert False, "Failed to download {} after {} attempts".format(local_file_tmp, MAX_DOWNLOAD_ATTEMPTS)
            try:
                print("Downloading '{}' to '{}'...".format(remote_file, local_file_tmp))
                request.urlretrieve(remote_file, local_file_tmp)
                print("Downloading '{}' to '{}'...".format(remote_sha256, local_sha256))
                request.urlretrieve(remote_sha256, local_sha256)
                print("Download complete")
            except Exception as e:
                print("Retrying on error:", e)
                continue

            assert local_file_tmp.exists(), "{} does not exist".format(local_file_tmp)
            assert local_sha256.exists(), "{} does not exist".format(local_sha256)
            expected_checksum = read_to_str(local_sha256)
            actual_checkum = compute_sha256(local_file_tmp)
            if expected_checksum == actual_checkum:
                print("Checksum OK")
                break
            print("Checksum mismatch. Expected: {} Actual: {}".format(expected_checksum, actual_checkum))

        os.rename(local_file_tmp, local_file)
        os.remove(local_sha256)

        if make_executable:
            make_executable_path(local_file)

    # Copy a client library file from the local old binaries repository
    # The file needs to be renamed to libfdb_c.so, because it is loaded with this name by fdbcli
    def copy_clientlib_from_local_repo(self, version):
        dest_lib_file = self.download_dir.joinpath(version, "libfdb_c.so")
        if dest_lib_file.exists():
            return
        # Avoid race conditions in case of parallel test execution by first copying to a temporary file
        # and then renaming it atomically
        dest_file_tmp = Path("{}.{}".format(str(dest_lib_file), random_secret_string(8)))
        src_lib_file = self.local_binary_repo.joinpath(version, "lib", "libfdb_c-{}.so".format(version))
        assert src_lib_file.exists(), "Missing file {} in the local old binaries repository".format(src_lib_file)
        self.download_dir.joinpath(version).mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src_lib_file, dest_file_tmp)
        os.rename(dest_file_tmp, dest_lib_file)
        assert dest_lib_file.exists(), "{} does not exist".format(dest_lib_file)

    # Download all old binaries required for testing the specified upgrade path
    def download_old_binaries(self, version):
        if is_local_build_version(version):
            return

        if self.version_in_local_repo(version):
            self.copy_clientlib_from_local_repo(version)
            return

        self.download_old_binary(version, "fdbserver", "fdbserver.{}".format(self.platform), True)
        self.download_old_binary(version, "fdbmonitor", "fdbmonitor.{}".format(self.platform), True)
        self.download_old_binary(version, "fdbcli", "fdbcli.{}".format(self.platform), True)
        self.download_old_binary(version, "libfdb_c.so", "libfdb_c.{}.so".format(self.platform), False)
