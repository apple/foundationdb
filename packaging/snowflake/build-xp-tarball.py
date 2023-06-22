from argparse import ArgumentParser, RawDescriptionHelpFormatter
import glob
import io
import os
from pathlib import Path
from binary_download import FdbBinaryDownloader, is_local_build_version
from fdb_version import CURRENT_VERSION
import subprocess
import tarfile
import tempfile


class PackageBuilder:
    def __init__(self, args):
        self.files = {}
        self.build_dir = self._resolve_dir(args.build_dir)
        self.src_dir = self._resolve_dir(args.src_dir)
        self.output = args.output
        self.release_version = args.release_version
        self.external_client_versions = args.external_clients
        self.downloader = FdbBinaryDownloader(args.build_dir)

    @staticmethod
    def _resolve_dir(path):
        dir = Path(path).resolve()
        assert dir.exists(), "{} does not exist".format(dir)
        assert dir.is_dir(), "{} is not a directory".format(dir)
        return dir

    @staticmethod
    def _version_suffix(version):
        version_tuple = version.split(".")
        return "{}_{}".format(version_tuple[0], version_tuple[1])

    def add_source_file(self, dest_path, src_path):
        self.tar.add(self.src_dir.joinpath(src_path), dest_path)

    def add_build_file(self, dest_path, src_path, strip=False):
        full_src_path = self.build_dir.joinpath(src_path)
        if strip:
            with tempfile.NamedTemporaryFile() as tmp:
                subprocess.check_output(["strip", "-S", full_src_path, "-o", tmp.name])
                self.tar.add(tmp.name, dest_path)
        else:
            self.tar.add(full_src_path, dest_path)

    def build(self):
        for version in self.external_client_versions:
            assert not is_local_build_version(version)
            self.downloader.download_client_library(version)

        if self.release_version != CURRENT_VERSION:
            self.downloader.download_client_library(self.release_version)

        with tarfile.open(self.output, "w:gz") as tar:
            self.tar = tar
            self.add_source_file(
                "include/foundationdb/fdb_c.h", "bindings/c/foundationdb/fdb_c.h"
            )
            self.add_source_file(
                "include/foundationdb/fdb_c_types.h",
                "bindings/c/foundationdb/fdb_c_types.h",
            )
            self.add_source_file(
                "include/foundationdb/fdb_c_shim.h",
                "bindings/c/foundationdb/fdb_c_shim.h",
            )
            self.add_build_file(
                "include/foundationdb/fdb_c_apiversion.g.h",
                "bindings/c/foundationdb/fdb_c_apiversion.g.h",
            )
            self.add_build_file(
                "include/foundationdb/fdb_c_options.g.h",
                "bindings/c/foundationdb/fdb_c_options.g.h",
            )
            self.add_build_file("lib/libfdb_c_prerelease.so", "lib/libfdb_c.so")
            self.add_build_file("lib/libfdb_c_shim.so", "lib/libfdb_c_shim.so")
            self.add_build_file("server/fdbmonitor", "bin/fdbmonitor", strip=True)
            self.add_build_file("server/fdbserver", "bin/fdbserver", strip=True)
            self.add_build_file("server/fdbcli", "bin/fdbcli", strip=True)
            tar.add(self.downloader.lib_path(self.release_version), "lib/libfdb_c.so")
            for version in self.external_client_versions:
                tar.add(
                    self.downloader.lib_path(version),
                    "lib/fdb/libfdb_c_{}.so".format(self._version_suffix(version)),
                )


if __name__ == "__main__":
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="""
        Builds FDB dependency package for XP
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
        "--src-dir",
        "-s",
        metavar="SRC_DIRECTORY",
        help="FDB source directory",
        required=True,
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="OUTPUT",
        help="The target path of XP dependency package",
        required=True,
    )
    parser.add_argument(
        "--external-clients",
        nargs="+",
        help="Space separated list of external client versions to be included into package",
        default=[],
    )
    parser.add_argument(
        "--release-version",
        help="The client release version",
        default=CURRENT_VERSION,
    )
    args = parser.parse_args()
    builder = PackageBuilder(args)
    builder.build()
