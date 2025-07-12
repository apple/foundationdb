#! /usr/bin/env python3

"""joshua_logtool.py

Provides uploading/downloading FoundationDB log files to Joshua cluster.
"""

import argparse
import logging
import os
import os.path
import re
import pathlib
import subprocess
import tempfile
import sys

import fdb
import joshua.joshua_model as joshua

from typing import List, Union

# Defined in SimulatedCluster.actor.cpp:SimulationConfig::setStorageEngine
ROCKSDB_TRACEEVENT_STRING = ["RocksDBNonDeterminism", "ShardedRocksDBNonDeterminism"]

# e.g. /var/joshua/ensembles/20230221-051349-xiaogesu-c9fc5b230dcd91cf
ENSEMBLE_ID_REGEXP = re.compile(r"ensembles\/(?P<ensemble_id>[0-9A-Za-z\-_\.]+)$")

# e.g. <Test TestUID="1ad90d42-824b-4693-aacf-53de3a6ccd27" Statistics="AAAA
TEST_UID_REGEXP = re.compile(r"TestUID=\"(?P<uid>[0-9a-fA-F\-]+)\"")

logger = logging.getLogger(__name__)

def console_log(message):
    """Print message to stderr for immediate visibility"""
    print(f"JOSHUA_LOGTOOL: {message}", file=sys.stderr, flush=True)

def _execute_grep(string: str, paths: List[pathlib.Path]) -> bool:
    command = ["grep", "-F", string] + [str(path) for path in paths]
    logger.debug(f"Executing grep command: {' '.join(command)}")
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    logger.debug(f"Grep result for '{string}': returncode={result.returncode}, stderr='{result.stderr.strip()}'")
    return result.returncode == 0


def _is_rocksdb_test(log_files: List[pathlib.Path]) -> bool:
    logger.info(f"Checking if this is a RocksDB test by scanning {len(log_files)} log files")
    for event_str in ROCKSDB_TRACEEVENT_STRING:
        logger.debug(f"Searching for RocksDB event string: '{event_str}'")
        if _execute_grep(event_str, log_files):
            logger.info(f"✓ Found RocksDB event '{event_str}' - this IS a RocksDB test")
            return True
        else:
            logger.debug(f"✗ RocksDB event '{event_str}' not found")
    logger.info("✗ No RocksDB events found - this is NOT a RocksDB test")
    return False


def _extract_ensemble_id(work_directory: str) -> Union[str, None]:
    logger.debug(f"Extracting ensemble ID from work directory: '{work_directory}'")
    match = ENSEMBLE_ID_REGEXP.search(work_directory)
    if not match:
        logger.debug(f"No ensemble ID pattern found in work directory")
        return None
    ensemble_id = match.groupdict()["ensemble_id"]
    logger.debug(f"Extracted ensemble ID: '{ensemble_id}'")
    return ensemble_id


def _get_log_subspace(ensemble_id: str, test_uid: str):
    subspace = joshua.dir_ensemble_results_application
    log_space = subspace.create_or_open(joshua.db, "simulation_logs")
    final_subspace = log_space[bytes(ensemble_id, "utf-8")][bytes(test_uid, "utf-8")]
    logger.debug(f"Created FDB subspace for ensemble_id='{ensemble_id}', test_uid='{test_uid}': {final_subspace}")
    return final_subspace


def _tar_logs(log_files: List[pathlib.Path], output_file_name: pathlib.Path):
    command = ["tar", "-c", "-f", str(output_file_name), "--xz"] + [
        str(log_file) for log_file in log_files
    ]
    logger.info(f"Creating tar archive with {len(log_files)} files")
    logger.debug(f"Tar command: {' '.join(command)}")
    
    # Log the files being archived
    for i, log_file in enumerate(log_files):
        file_size = log_file.stat().st_size if log_file.exists() else 0
        logger.debug(f"  File {i+1}: {log_file} ({file_size} bytes)")
    
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.debug(f"Tar stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"Tar stderr: {result.stderr}")
        logger.info(f"✓ Tar archive created successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Tar command failed: {e}")
        logger.error(f"Tar stdout: {e.stdout}")
        logger.error(f"Tar stderr: {e.stderr}")
        raise


def _tar_extract(path_to_archive: pathlib.Path):
    command = ["tar", "xf", str(path_to_archive)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def report_error(
    work_directory: str,
    log_directory: str,
    ensemble_id: str,
    test_uid: str,
    check_rocksdb: bool,
):
    logger.info(f"=== JOSHUA_LOGTOOL UPLOAD START ===")
    logger.info(f"Parameters:")
    logger.info(f"  work_directory: {work_directory}")
    logger.info(f"  log_directory: {log_directory}")
    logger.info(f"  ensemble_id: {ensemble_id}")
    logger.info(f"  test_uid: {test_uid}")
    logger.info(f"  check_rocksdb: {check_rocksdb}")
    
    # Also print to stderr for immediate visibility
    console_log(f"Starting upload for test {test_uid}")
    
    # Step 1: Find log files
    logger.info(f"Step 1: Searching for log files in directory: {log_directory}")
    log_directory_path = pathlib.Path(log_directory)
    
    if not log_directory_path.exists():
        logger.error(f"✗ Log directory does not exist: {log_directory}")
        console_log(f"FAILED - Log directory does not exist: {log_directory}")
        return
    
    if not log_directory_path.is_dir():
        logger.error(f"✗ Log directory path is not a directory: {log_directory}")
        console_log(f"FAILED - Log directory path is not a directory: {log_directory}")
        return
    
    # Search for XML files
    xml_files = list(log_directory_path.glob("**/trace*.xml"))
    logger.info(f"Found {len(xml_files)} XML trace files")
    for xml_file in xml_files:
        logger.debug(f"  XML: {xml_file}")
    
    # Search for JSON files
    json_files = list(log_directory_path.glob("**/trace*.json"))
    logger.info(f"Found {len(json_files)} JSON trace files")
    for json_file in json_files:
        logger.debug(f"  JSON: {json_file}")
    
    all_trace_files = xml_files + json_files
    
    # Filter out core files (exclude any file with 'core' in the name)
    log_files = []
    excluded_files = []
    for file in all_trace_files:
        if 'core' in file.name.lower():
            excluded_files.append(file)
            logger.debug(f"  EXCLUDED (core file): {file}")
        else:
            log_files.append(file)
    
    if excluded_files:
        logger.info(f"Excluded {len(excluded_files)} core files from upload")
        for excluded in excluded_files:
            logger.info(f"  Excluded: {excluded}")
    
    logger.info(f"Final file list: {len(log_files)} files to upload")
    
    if len(log_files) == 0:
        logger.warning(f"✗ No trace files found in directory {log_directory}")
        logger.info(f"Directory contents:")
        try:
            for item in log_directory_path.rglob("*"):
                if item.is_file():
                    logger.info(f"  File: {item}")
                elif item.is_dir():
                    logger.info(f"  Dir:  {item}/")
        except Exception as e:
            logger.error(f"Error listing directory contents: {e}")
        console_log(f"SKIPPED - No trace files found")
        return
    
    logger.info(f"✓ Found {len(log_files)} total trace files")
    
    # Step 2: Check RocksDB filtering if requested
    if check_rocksdb:
        logger.info(f"Step 2: RocksDB filtering enabled - checking if this is a RocksDB test")
        if not _is_rocksdb_test(log_files):
            logger.info(f"✗ RocksDB filtering: Test is not a RocksDB test - skipping upload")
            console_log(f"SKIPPED - Not a RocksDB test (--check-rocksdb enabled)")
            return
        else:
            logger.info(f"✓ RocksDB filtering: Test is a RocksDB test - proceeding with upload")
    else:
        logger.info(f"Step 2: RocksDB filtering disabled - uploading all trace files")
    
    # Step 3: Determine ensemble ID
    logger.info(f"Step 3: Determining ensemble ID")
    final_ensemble_id = ensemble_id or _extract_ensemble_id(work_directory)
    if not final_ensemble_id:
        logger.error(f"✗ Ensemble ID missing - provided: '{ensemble_id}', extracted: None")
        logger.error(f"Work directory pattern check: '{work_directory}'")
        console_log(f"FAILED - Ensemble ID missing")
        raise RuntimeError(f"Ensemble ID missing in work directory {work_directory}")
    
    logger.info(f"✓ Using ensemble ID: {final_ensemble_id}")
    
    # Step 4: Create tar archive
    logger.info(f"Step 4: Creating tar.xz archive")
    try:
        with tempfile.NamedTemporaryFile(suffix='.tar.xz', delete=False) as archive:
            archive_path = pathlib.Path(archive.name)
            logger.info(f"Temporary archive file: {archive_path}")
            
            _tar_logs(log_files, archive_path)
            
            archive_size = archive_path.stat().st_size
            logger.info(f"✓ Archive created successfully, size: {archive_size} bytes")
            
            if archive_size == 0:
                logger.error(f"✗ Archive is empty!")
                console_log(f"FAILED - Archive is empty")
                return
            
            # Step 5: Upload to FDB
            logger.info(f"Step 5: Uploading to FDB database")
            subspace = _get_log_subspace(final_ensemble_id, test_uid)
            
            with open(archive_path, 'rb') as archive_file:
                logger.debug(f"Uploading archive to FDB subspace: {subspace}")
                joshua._insert_blob(joshua.db, subspace, archive_file, offset=0)
                logger.info(f"✓ Upload to FDB completed successfully")
            
            # Clean up temporary file
            archive_path.unlink()
            logger.debug(f"Cleaned up temporary archive file")
            
            console_log(f"SUCCESS - Uploaded {len(log_files)} files ({archive_size} bytes)")
            logger.info(f"=== JOSHUA_LOGTOOL UPLOAD COMPLETE ===")
            
    except Exception as e:
        logger.error(f"✗ Error during archive creation or upload: {e}")
        console_log(f"FAILED - {e}")
        raise


def download_logs(ensemble_id: str, test_uid: str):
    with tempfile.NamedTemporaryFile() as archive:
        subspace = _get_log_subspace(ensemble_id, test_uid)
        logger.debug(
            f"Downloading the archive to {archive.name} at subspace {subspace}"
        )
        joshua._read_blob(joshua.db, subspace, archive)
        
        # Check archive size
        archive.seek(0, 2)  # Seek to end
        archive_size = archive.tell()
        archive.seek(0)  # Reset to beginning
        
        logger.debug(f"Downloaded archive size: {archive_size} bytes")
        
        if archive_size == 0:
            console_log(f"No logs were uploaded for test {test_uid}")
            console_log(f"This could mean:")
            console_log(f"- The test was not a RocksDB test (if --check-rocksdb was used)")
            console_log(f"- No trace files were generated")
            console_log(f"- The upload failed")
            return
        
        _tar_extract(archive.name)


def list_commands(ensemble_id: str):
    console_log("Listing ALL tests (both passed and failed)...")
    count = 0
    for item in joshua.tail_results(ensemble_id, errors_only=False):
        count += 1
        if len(item) >= 5:
            test_harness_output = item[4]
            match = TEST_UID_REGEXP.search(test_harness_output)
            if match:
                test_uid = match.groupdict()["uid"]
                console_log(f"python3 {__file__} download --ensemble-id {ensemble_id} --test-uid {test_uid}")
            else:
                logger.warning(f"Test UID not found in output #{count}")
    console_log(f"Found {count} total tests in ensemble")


def _setup_args():
    parser = argparse.ArgumentParser(prog="joshua_logtool.py")

    parser.add_argument(
        "--cluster-file", type=str, default=None, help="Joshua FDB cluster file"
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Add debug logging"
    )

    subparsers = parser.add_subparsers(help="Possible actions", dest="action")

    upload_parser = subparsers.add_parser(
        "upload", help="Check the log file, upload them to Joshua cluster if necessary"
    )
    upload_parser.add_argument(
        "--work-directory", type=str, default=os.getcwd(), help="Work directory"
    )
    upload_parser.add_argument(
        "--log-directory",
        type=str,
        required=True,
        help="Directory contains XML/JSON logs",
    )
    upload_parser.add_argument(
        "--ensemble-id", type=str, default=None, required=False, help="Ensemble ID"
    )
    upload_parser.add_argument("--test-uid", type=str, required=True, help="Test UID")
    upload_parser.add_argument(
        "--check-rocksdb",
        action="store_true",
        help="If true, only upload logs when RocksDB is involved; otherwise, always upload logs.",
    )

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

    # Always enable INFO level logging, DEBUG if requested
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.debug(f"Using cluster file {args.cluster_file}")
    joshua.open(args.cluster_file)

    if args.action == "upload":
        report_error(
            work_directory=args.work_directory,
            log_directory=args.log_directory,
            ensemble_id=args.ensemble_id,
            test_uid=args.test_uid,
            check_rocksdb=args.check_rocksdb,
        )
    elif args.action == "download":
        download_logs(ensemble_id=args.ensemble_id, test_uid=args.test_uid)
    elif args.action == "list":
        list_commands(ensemble_id=args.ensemble_id)


if __name__ == "__main__":
    _main()
