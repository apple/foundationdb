#!/usr/bin/env python3

import os
from pathlib import Path
import platform
import shutil
import stat
from urllib import request
from fdb_version import CURRENT_VERSION, FUTURE_VERSION

from test_util import random_alphanum_string

SUPPORTED_PLATFORMS = ["x86_64", "aarch64"]
FDB_DOWNLOAD_ROOT = "s3://sfc-eng-jenkins/foundationdb/release/builds/"
LOCAL_OLD_BINARY_REPO = "/opt/foundationdb/old/"
MAX_DOWNLOAD_ATTEMPTS = 5


def make_executable_path(path):
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


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
        assert self.platform in SUPPORTED_PLATFORMS, "Unsupported platform {}".format(
            self.platform
        )
        self.download_dir = self.build_dir.joinpath("tmp", "old_binaries")
        self.local_binary_repo = Path(LOCAL_OLD_BINARY_REPO)
        if not self.local_binary_repo.exists():
            self.local_binary_repo = None

    # Check if the binaries for the given version are available in the local old binaries repository
    def version_in_local_repo(self, version):
        return (self.local_binary_repo is not None) and (
            self.local_binary_repo.joinpath(version).exists()
        )

    def binary_path(self, version, bin_name):
        if is_local_build_version(version):
            return self.build_dir.joinpath("bin", bin_name)
        elif self.version_in_local_repo(version):
            return self.local_binary_repo.joinpath(
                version, "bin", "{}-{}".format(bin_name, version)
            )
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
    def download_old_binary(
        self, version, target_bin_name, remote_bin_name, make_executable
    ):
        local_file = self.download_dir.joinpath(version, target_bin_name)
        if local_file.exists():
            return

        # Download to a temporary file and then replace the target file atomically
        # to avoid consistency errors in case of multiple tests are downloading the
        # same file in parallel
        local_file_tmp = Path(
            "{}.{}".format(str(local_file), random_alphanum_string(8))
        )
        self.download_dir.joinpath(version).mkdir(parents=True, exist_ok=True)
        relpath = "bin" if make_executable else "lib"
        remote_file = "{}{}/snowflake-{}/{}/{}".format(
            FDB_DOWNLOAD_ROOT, self.platform, version, relpath, remote_bin_name
        )

        print("Downloading '{}' to '{}'...".format(remote_file, local_file_tmp))
        ret = os.system("aws s3 cp '{}' '{}'".format(remote_file, local_file_tmp))
        assert ret == 0, "Download failed. Return code: {}".format(ret)
        print("Download complete")

        os.rename(local_file_tmp, local_file)

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
        dest_file_tmp = Path(
            "{}.{}".format(str(dest_lib_file), random_alphanum_string(8))
        )
        src_lib_file = self.local_binary_repo.joinpath(
            version, "lib", "libfdb_c-{}.so".format(version)
        )
        assert (
            src_lib_file.exists()
        ), "Missing file {} in the local old binaries repository".format(src_lib_file)
        self.download_dir.joinpath(version).mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src_lib_file, dest_file_tmp)
        os.rename(dest_file_tmp, dest_lib_file)
        assert dest_lib_file.exists(), "{} does not exist".format(dest_lib_file)

    # Download client library of the given version
    def download_client_library(self, version):
        return self.download_old_binary(version, "libfdb_c.so", "libfdb_c.so", False)

    # Download all old binaries required for testing the specified upgrade path
    def download_old_binaries(self, version):
        if is_local_build_version(version):
            return

        if self.version_in_local_repo(version):
            self.copy_clientlib_from_local_repo(version)
            return

        self.download_old_binary(version, "fdbserver", "fdbserver", True)
        self.download_old_binary(version, "fdbmonitor", "fdbmonitor", True)
        self.download_old_binary(version, "fdbcli", "fdbcli", True)
        self.download_client_library(version)
