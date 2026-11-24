"""Exercise encrypted backup edits on the directory backup test's running cluster."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path


def test_modify(bin_dir, cluster, scratch):
    tag_args = ["-C", cluster, "-t", "encrypted-modify"]

    def cli(command):
        return subprocess.check_output(
            [bin_dir / "fdbcli", "-C", cluster, "--exec", command],
            text=True,
            timeout=60,
        ).strip()

    def backup(action, *args, check=True):
        return subprocess.run(
            [bin_dir / "fdbbackup", action, *tag_args, *args],
            check=check,
            capture_output=True,
            text=True,
            timeout=180,
        )

    def status():
        return json.loads(backup("status", "--json-v2").stdout)

    def wait_for_point(min_version):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            document = status()
            if (
                document.get("ConfirmedRestorablePoint", {}).get("Version", -1)
                >= min_version
            ):
                return document
            time.sleep(1)
        raise AssertionError(f"Backup did not reach version {min_version}: {document}")

    key = scratch / "modify-key"
    wrong_key = scratch / "modify-wrong-key"
    for path in (key, wrong_key):
        path.write_bytes(os.urandom(32))
    range_args = ["-k", "encrypted-modify/ encrypted-modify0"]
    key_args = ["--encryption-key-file", key]
    cli("writemode on; set encrypted-modify/snapshot original")
    destination = (scratch / "encrypted-backup").as_uri()
    backup(
        "start",
        "-d",
        destination,
        *range_args,
        *key_args,
        "-z",
        "-s",
        "3600",
        "--initial-snapshot-interval",
        "0",
    )
    initial = wait_for_point(0)
    url = initial["DestinationURL"]
    assert not url.endswith("/"), url

    # The rejected command would also change the interval if it committed.
    for suffix in ("", "/", "//"):
        for key_option in ([], ["--encryption-key-file", wrong_key]):
            result = backup(
                "modify", "-d", url + suffix, "-s", "7200", *key_option, check=False
            )
            assert (
                result.returncode
                and "encryption key file does not match" in result.stderr
            ), result
            current = status()
            assert current["DestinationURL"] == url, current
            assert current["SnapshotIntervalSeconds"] == 3600, current

    for suffix in ("", "/", "//"):
        backup("modify", "-d", url + suffix, *key_args)
        current = status()
        assert (
            not current.get("DestinationURLChanged") and current["Restorable"]
        ), current
        assert (
            current["ConfirmedRestorablePoint"]["Version"]
            >= initial["ConfirmedRestorablePoint"]["Version"]
        )

    # The restored value must come from a log written after the destination edits.
    before_write = int(cli("getversion"))
    cli("writemode on; set encrypted-modify/log after-modify")
    final = wait_for_point(int(cli("getversion")))
    assert final["LatestSnapshotEnd"]["Version"] <= before_write, final
    point = final["ConfirmedRestorablePoint"]["Version"]
    backup("discontinue", "-w")
    cli("writemode on; clearrange encrypted-modify/ encrypted-modify0")
    restore = [bin_dir / "fdbrestore", "start", *tag_args, *range_args, *key_args]
    subprocess.run(
        [*restore, "-r", url, f"--version={point}", "-w"], check=True, timeout=180
    )
    for name, expected in (("snapshot", "original"), ("log", "after-modify")):
        data_key = f"encrypted-modify/{name}"
        value = cli(f"get {data_key}")
        assert value == f"`{data_key}' is `{expected}'", value
        print(f"Restored at confirmed version {point}: {value}")


if __name__ == "__main__":
    test_modify(*(Path(arg).resolve() for arg in sys.argv[1:]))
