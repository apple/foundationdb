#!/usr/bin/env python3

import argparse
import os
from pathlib import Path
import subprocess

from binary_download import FdbBinaryDownloader


def main():
    parser = argparse.ArgumentParser(
        description="Test network setup with a released external client library."
    )
    parser.add_argument("--build-dir", required=True)
    parser.add_argument("--setup-tester-bin", required=True)
    parser.add_argument("--client-version", required=True)
    args = parser.parse_args()

    downloader = FdbBinaryDownloader(args.build_dir)
    if downloader.version_in_local_repo(args.client_version):
        downloader.copy_clientlib_from_local_repo(args.client_version)
    else:
        downloader.download_old_binary(
            args.client_version,
            "libfdb_c.so",
            "libfdb_c.{}.so".format(downloader.platform),
            False,
        )

    # FUTURE_VERSION uses the local build even when that version has been released.
    # This test needs the released library with its actual exported functions.
    client_library = downloader.download_dir.joinpath(
        args.client_version, "libfdb_c.so"
    )
    assert client_library.is_file(), "{} does not exist".format(client_library)

    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("FDB_NETWORK_OPTION_")
    }
    env["FDB_NETWORK_OPTION_EXTERNAL_CLIENT_LIBRARY"] = str(client_library)
    env["FDB_NETWORK_OPTION_DISABLE_LOCAL_CLIENT"] = ""
    subprocess.run(
        [str(Path(args.setup_tester_bin).resolve())],
        check=True,
        env=env,
        timeout=60,
    )


if __name__ == "__main__":
    main()
