#!/usr/bin/env python3

import json
import os
from pathlib import Path
import subprocess
import platform
import shutil
import stat
from urllib import request
from fdb_version import CURRENT_VERSION, FUTURE_VERSION

from test_util import random_alphanum_string

SUPPORTED_PLATFORMS = ["x86_64", "aarch64"]
FDB_DOWNLOAD_BUCKET = "sfc-eng-jenkins"
FDB_DOWNLOAD_PREFIX = "foundationdb/release/builds"


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
        self.available_releases = None

    def load_available_releases(self):
        cmd = [
            "aws",
            "s3api",
            "list-objects",
            "--delimiter",
            "/",
            "--query",
            "CommonPrefixes[].Prefix",
            "--bucket",
            FDB_DOWNLOAD_BUCKET,
            "--prefix",
            "{}/{}/".format(FDB_DOWNLOAD_PREFIX, self.platform),
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE)
        assert (
            res.returncode == 0
        ), "Failed to load downloadable versions. Error: {}, Command: {}".format(
            res.returncode, cmd
        )
        self.available_releases = [key.split("/")[-2] for key in json.loads(res.stdout)]

    def latest_release_for(self, version):
        if self.available_releases is None:
            self.load_available_releases()

        release = "snowflake-{}".format(version)
        if release in self.available_releases:
            return release

        rc_prefix = "snowflake-{}-rc".format(version)
        release_candidates = [
            int(v[len(rc_prefix) :])
            for v in self.available_releases
            if v.startswith(rc_prefix)
        ]
        assert (
            len(release_candidates) > 0
        ), "No releases available for version {}".format(version)
        release = rc_prefix + str(max(release_candidates))
        assert release in self.available_releases
        return release

    def binary_path(self, version, bin_name):
        if is_local_build_version(version):
            return self.build_dir.joinpath("bin", bin_name)
        else:
            return self.download_dir.joinpath(
                self.latest_release_for(version), bin_name
            )

    def lib_dir(self, version):
        if is_local_build_version(version):
            return self.build_dir.joinpath("lib")
        else:
            return self.download_dir.joinpath(self.latest_release_for(version))

    def lib_path(self, version):
        return self.lib_dir(version).joinpath("libfdb_c.so")

    # Download an old binary of a given version from a remote repository
    def download_old_binary(
        self, version, target_bin_name, remote_bin_name, make_executable
    ):
        release = self.latest_release_for(version)
        local_file = self.download_dir.joinpath(release, target_bin_name)
        if local_file.exists():
            return

        # Download to a temporary file and then replace the target file atomically
        # to avoid consistency errors in case of multiple tests are downloading the
        # same file in parallel
        local_file_tmp = Path(
            "{}.{}".format(str(local_file), random_alphanum_string(8))
        )
        relpath = "bin" if make_executable else "lib"
        remote_file = "s3://{}/{}/{}/{}/{}/{}".format(
            FDB_DOWNLOAD_BUCKET,
            FDB_DOWNLOAD_PREFIX,
            self.platform,
            release,
            relpath,
            remote_bin_name,
        )

        print("Downloading '{}' to '{}'...".format(remote_file, local_file_tmp))
        ret = os.system("aws s3 cp '{}' '{}'".format(remote_file, local_file_tmp))
        assert ret == 0, "Download failed. Return code: {}".format(ret)
        print("Download complete")

        os.rename(local_file_tmp, local_file)

        if make_executable:
            make_executable_path(local_file)

    # Download all old binaries required for testing the specified upgrade path
    def download_old_binaries(self, version):
        if is_local_build_version(version):
            return

        self.download_old_binary(version, "fdbserver", "fdbserver", True)
        self.download_old_binary(version, "fdbmonitor", "fdbmonitor", True)
        self.download_old_binary(version, "fdbcli", "fdbcli", True)
        self.download_old_binary(version, "libfdb_c.so", "libfdb_c.so", False)
