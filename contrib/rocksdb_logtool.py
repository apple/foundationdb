#! /usr/bin/env python3

"""rocksdb_logtool.py

Provides uploading/downloading FoundationDB XML log files to Joshua cluster.

This is a temporary solution for Joshua tests with RocksDB as the storage server
of FoundationDB. Since RocksDB is *not* deterministic, FoundationDB simulation
failures in Joshua may not be reproducible locally. With this script, it is
possible to upload the XML logs on the Joshua side and download them to local
directories.
"""

import argparse
import logging
import os
import os.path
import re
import pathlib
import subprocess
import tempfile

import fdb
import joshua.joshua_model as joshua

from typing import List

# Defined in SimulatedCluster.actor.cpp:SimulationConfig::setStorageEngine
ROCKSDB_TRACEEVENT_STRING = ["RocksDBNonDeterminism", "ShardedRocksDBNonDeterminism"]

# e.g. /var/joshua/ensembles/20230221-051349-xiaogesu-c9fc5b230dcd91cf
ENSEMBLE_ID_REGEXP = re.compile(r"ensembles\/(?P<ensemble_id>[0-9A-Za-z\-_]+)$")

# e.g. <Test TestUID="1ad90d42-824b-4693-aacf-53de3a6ccd27" Statistics="AAAA
TEST_UID_REGEXP = re.compile(r"TestUID=\"(?P<uid>[0-9a-fA-F\-]+)\"")

logger = logging.getLogger(__name__)


def _execute_grep(string: str, paths: List[pathlib.Path]) -> bool:
    command = ["grep", "-F", string] + [str(path) for path in paths]
    result = subprocess.run(command, stdout=subprocess.DEVNULL)
    return result.returncode == 0


def _is_rocksdb_test(log_files: List[pathlib.Path]) -> bool:
    for event_str in ROCKSDB_TRACEEVENT_STRING:
        if _execute_grep(event_str, log_files):
            return True
    return False


def _extract_ensemble_id(work_directory: str) -> str:
    match = ENSEMBLE_ID_REGEXP.search(work_directory)
    if not match:
        return None
    return match.groupdict()["ensemble_id"]


def _get_log_subspace(ensemble_id: str, test_uid: str):
    ensemble_space = joshua.dir_ensembles
    rocksdb_log_space = ensemble_space.create_or_open(joshua.db, "rocksdb_logs")
    return rocksdb_log_space[bytes(ensemble_id, "utf-8")][bytes(test_uid, "utf-8")]


def _tar_xmls(xml_files: List[pathlib.Path], output_file_name: pathlib.Path):
    command = ["tar", "-c", "-f", str(output_file_name), "--xz"] + [
        str(xml_file) for xml_file in xml_files
    ]
    logger.debug(f"Execute tar: {command}")
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def _tar_extract(path_to_archive: pathlib.Path):
    command = ["tar", "xf", str(path_to_archive)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def report_error(
    work_directory: str, log_directory: str, ensemble_id: str, test_uid: str
):
    log_files = list(pathlib.Path(log_directory).glob("**/trace*.xml"))
    if len(log_files) == 0:
        logger.debug(f"No XML file found in directory {log_directory}")
    log_files += list(pathlib.Path(log_directory).glob("**/trace*.json"))
    if len(log_files) == 0:
        logger.debug(f"No JSON file found in directory {log_directory}")
        return
    logger.debug(f"Total {len(log_files)} files found")

    if not _is_rocksdb_test(log_files):
        logger.debug("Not a RocksDB test")
        return

    ensemble_id = ensemble_id or _extract_ensemble_id(work_directory)
    if not ensemble_id:
        logger.debug(f"Ensemble ID missing in work directory {work_directory}")
        raise RuntimeError(f"Ensemble ID missing in work directory {work_directory}")
    logger.debug(f"Ensemble ID: {ensemble_id}")

    with tempfile.NamedTemporaryFile() as archive:
        logger.debug(f"Tarfile: {archive.name}")
        _tar_xmls(log_files, archive.name)
        logger.debug(f"Tarfile size: {os.path.getsize(archive.name)}")
        subspace = _get_log_subspace(ensemble_id, test_uid)
        joshua._insert_blob(joshua.db, subspace, archive, offset=0)


def download_logs(ensemble_id: str, test_uid: str):
    with tempfile.NamedTemporaryFile() as archive:
        subspace = _get_log_subspace(ensemble_id, test_uid)
        logger.debug(
            f"Downloading the archive to {archive.name} at subspace {subspace}"
        )
        joshua._read_blob(joshua.db, subspace, archive)
        logger.debug(f"Tarfile size: {os.path.getsize(archive.name)}")
        _tar_extract(archive.name)


def list_commands(ensemble_id: str):
    for item in joshua.tail_results(ensemble_id, errors_only=True):
        test_harness_output = item[4]
        match = TEST_UID_REGEXP.search(test_harness_output)
        if not match:
            logger.warning(f"Test UID not found in {test_harness_output}")
            continue
        test_uid = match.groupdict()["uid"]
        print(f"python3 {__file__} download --ensemble-id {ensemble_id} --test-uid {test_uid}")


def _setup_args():
    parser = argparse.ArgumentParser(prog="rocksdb_logtool.py")

    parser.add_argument(
        "--cluster-file", type=str, default=None, help="Joshua FDB cluster file"
    )

    subparsers = parser.add_subparsers(help="Possible actions", dest="action")

    report_parser = subparsers.add_parser(
        "report", help="Check the log file, upload them to Joshua cluster if necessary"
    )
    report_parser.add_argument(
        "--work-directory", type=str, default=os.getcwd(), help="Work directory"
    )
    report_parser.add_argument(
        "--log-directory",
        type=str,
        required=True,
        help="Directory contains XML/JSON logs",
    )
    report_parser.add_argument(
        "--ensemble-id", type=str, default=None, required=False, help="Ensemble ID"
    )
    report_parser.add_argument("--test-uid", type=str, required=True, help="Test UID")

    download_parser = subparsers.add_parser(
        "download", help="Download the log file from Joshua to local directory"
    )
    download_parser.add_argument(
        "--ensemble-id", type=str, required=True, help="Joshua ensemble ID"
    )
    download_parser.add_argument("--test-uid", type=str, required=True, help="Test UID")

    list_parser = subparsers.add_parser(
        "list",
        help="List the possible download commands for failed tests in a given ensemble. NOTE: It is possible that the test is not relevant to RocksDB and no log file is available. It is the user's responsibility to verify if this happens.",
    )
    list_parser.add_argument(
        "--ensemble-id", type=str, required=True, help="Joshua ensemble ID"
    )

    return parser.parse_args()


def _main():
    args = _setup_args()

    logging.basicConfig(level=logging.INFO)

    logger.debug(f"Using cluster file {args.cluster_file}")
    joshua.open(args.cluster_file)

    if args.action == "report":
        report_error(
            work_directory=args.work_directory,
            log_directory=args.log_directory,
            ensemble_id=args.ensemble_id,
            test_uid=args.test_uid,
        )
    elif args.action == "download":
        download_logs(ensemble_id=args.ensemble_id, test_uid=args.test_uid)
    elif args.action == "list":
        list_commands(ensemble_id=args.ensemble_id)


if __name__ == "__main__":
    _main()
