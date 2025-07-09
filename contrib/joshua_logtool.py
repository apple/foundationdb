#!/usr/bin/env python3

"""joshua_logtool.py

Joshua Log Tool

This script provides uploading/downloading of FoundationDB trace logs to/from the Joshua cluster's
coordinating FDB database. It is designed to help with debugging failed tests by archiving and
retrieving simulation trace files.

MAIN FUNCTIONS:
==============

1. UPLOAD MODE (joshua_logtool.py upload)
   - Searches for all files in specified directories, excluding simfdb directories
   - Optionally filters uploads based on RocksDB involvement (--check-rocksdb flag)
   - Creates compressed tar.xz archives of all files (preserving directory structure)
   - Uploads archives to Joshua FDB cluster using ensemble ID and test UID as keys

2. DOWNLOAD MODE (joshua_logtool.py download)
   - Retrieves previously uploaded archives from Joshua FDB cluster
   - Extracts archives to current working directory
   - Useful for local debugging of failed tests

3. LIST MODE (joshua_logtool.py list)
   - Lists all failed tests in a given ensemble
   - Generates download commands for each failed test
   - Helps identify which tests have logs available for download

USAGE EXAMPLES:
==============
# Upload all files (excluding simfdb) for a specific test with RocksDB filtering
python3 joshua_logtool.py upload --log-directory /tmp/test_logs --test-uid 12345-abcd --check-rocksdb

# Upload all files regardless of test type
python3 joshua_logtool.py upload --log-directory /tmp/test_logs --test-uid 12345-abcd

# Download logs for a specific test
python3 joshua_logtool.py download --ensemble-id 20240101-120000-user-abc123 --test-uid 12345-abcd

# List all failed tests in an ensemble
python3 joshua_logtool.py list --ensemble-id 20240101-120000-user-abc123

"""

import argparse
import logging
import os
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

# Typical ensemble ID format: 20230221-051349-user-abc123
ENSEMBLE_ID_REGEXP = re.compile(r"(?P<ensemble_id>\d{8}-\d{6}-\w+-\w+)")

# Extract test UID from XML output
TEST_UID_REGEXP = re.compile(r"TestUID=\"(?P<uid>[0-9a-fA-F\-]+)\"")

logger = logging.getLogger(__name__)

def _execute_grep(string: str, paths: List[pathlib.Path]) -> bool:
    """Execute grep to search for a string in log files"""
    command = ["grep", "-F", string] + [str(path) for path in paths]
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0

def _is_rocksdb_test(log_files: List[pathlib.Path]) -> bool:
    """Check if test involves RocksDB by searching for RocksDB-related trace events"""
    for event_str in ROCKSDB_TRACEEVENT_STRING:
        if _execute_grep(event_str, log_files):
            return True
    return False

def _extract_ensemble_id(work_directory: str) -> Union[str, None]:
    """Extract ensemble ID from work directory path"""
    match = ENSEMBLE_ID_REGEXP.search(work_directory)
    if not match:
        return None
    return match.groupdict()["ensemble_id"]

def _get_log_subspace(ensemble_id: str, test_uid: str):
    """Get FDB subspace for storing/retrieving logs"""
    subspace = joshua.dir_ensemble_results_application
    log_space = subspace.create_or_open(joshua.db, "simulation_logs")
    return log_space[bytes(ensemble_id, "utf-8")][bytes(test_uid, "utf-8")]

def _tar_logs(log_files: List[pathlib.Path], output_file_name: pathlib.Path):
    """Create compressed tar archive of log files"""
    if not log_files:
        return
    
    # Find the common parent directory to create relative paths
    # For TestHarness2 structure, we want to preserve the top-level structure
    # (joshua_output/, run_files/) so we need to find the right common parent
    
    # Get all parent directories
    all_parents = [f.parent for f in log_files]
    
    # Find the deepest common parent that preserves meaningful directory structure
    try:
        common_parent = pathlib.Path(os.path.commonpath([str(p) for p in all_parents]))
        
        # Debug: Show what we're using as common parent
        print(f"JOSHUA_LOGTOOL_DEBUG: Common parent directory: {common_parent}", file=sys.stderr)
        print(f"JOSHUA_LOGTOOL_DEBUG: Sample file paths:", file=sys.stderr)
        for i, f in enumerate(log_files[:3]):
            try:
                rel_path = f.relative_to(common_parent)
                print(f"  {f} -> {rel_path}", file=sys.stderr)
            except ValueError:
                print(f"  {f} -> ERROR: Cannot make relative to {common_parent}", file=sys.stderr)
            if i >= 2:  # Only show first 3
                break
        
    except ValueError:
        # No common path, use the first file's parent
        common_parent = log_files[0].parent
        print(f"JOSHUA_LOGTOOL_DEBUG: No common path found, using: {common_parent}", file=sys.stderr)
    
    # Create tar with relative paths to preserve directory structure
    command = ["tar", "-c", "-f", str(output_file_name), "--xz", "-C", str(common_parent)]
    
    # Add relative paths from the common parent
    relative_paths = []
    for log_file in log_files:
        try:
            relative_path = log_file.relative_to(common_parent)
            relative_paths.append(str(relative_path))
        except ValueError:
            # If file is not relative to common parent, use absolute path
            print(f"JOSHUA_LOGTOOL_DEBUG: Cannot make {log_file} relative to {common_parent}, using absolute path", file=sys.stderr)
            relative_paths.append(str(log_file))
    
    command.extend(relative_paths)
    
    # Debug: Show the tar command being executed
    print(f"JOSHUA_LOGTOOL_DEBUG: Tar command: {' '.join(command[:10])}{'...' if len(command) > 10 else ''}", file=sys.stderr)
    
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _tar_extract(path_to_archive: pathlib.Path):
    """Extract tar archive to current directory"""
    command = ["tar", "-x", "-f", str(path_to_archive)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def report_error(
    work_directory: str,
    log_directory: str,
    ensemble_id: str,
    test_uid: str,
    upload_mode: str,
):
    """Upload all test files (excluding simfdb directories) to Joshua FDB cluster"""
    
    # Find log files
    log_directory_path = pathlib.Path(log_directory)
    if not log_directory_path.exists() or not log_directory_path.is_dir():
        logger.error(f"Log directory does not exist or is not a directory: {log_directory}")
        print(f"JOSHUA_LOGTOOL_RESULT: FAILED - Log directory not found", file=sys.stdout)
        return

    # Find all files, excluding simfdb directories
    all_files = []
    for root, dirs, files in os.walk(log_directory_path):
        root_path = pathlib.Path(root)
        
        # Skip if any parent directory is named 'simfdb'
        if 'simfdb' in root_path.parts:
            continue
            
        # Remove 'simfdb' from dirs to prevent os.walk from descending into them
        dirs[:] = [d for d in dirs if d != 'simfdb']
        
        # Add all files in this directory
        for file in files:
            file_path = root_path / file
            all_files.append(file_path)
    
    # Debug: Show the directory structure being processed
    print(f"JOSHUA_LOGTOOL_DEBUG: Log directory: {log_directory_path}", file=sys.stderr)
    print(f"JOSHUA_LOGTOOL_DEBUG: Found {len(all_files)} files in directory structure:", file=sys.stderr)
    
    # Group files by their top-level directory for debugging
    joshua_output_files = [f for f in all_files if 'joshua_output' in f.parts]
    run_files = [f for f in all_files if 'run_files' in f.parts]
    other_files = [f for f in all_files if 'joshua_output' not in f.parts and 'run_files' not in f.parts]
    
    print(f"JOSHUA_LOGTOOL_DEBUG: - joshua_output files: {len(joshua_output_files)}", file=sys.stderr)
    for f in joshua_output_files:
        print(f"    {f}", file=sys.stderr)
    
    print(f"JOSHUA_LOGTOOL_DEBUG: - run_files: {len(run_files)}", file=sys.stderr)
    for i, f in enumerate(run_files[:5]):  # Show first 5
        print(f"    {f}", file=sys.stderr)
    if len(run_files) > 5:
        print(f"    ... and {len(run_files) - 5} more run_files", file=sys.stderr)
    
    if other_files:
        print(f"JOSHUA_LOGTOOL_DEBUG: - other files: {len(other_files)}", file=sys.stderr)
        for f in other_files:
            print(f"    {f}", file=sys.stderr)
    
    if len(all_files) == 0:
        logger.debug(f"No files found in {log_directory} (excluding simfdb)")
        print(f"JOSHUA_LOGTOOL_RESULT: SKIPPED - No files found", file=sys.stdout)
        return

    # For RocksDB filtering, still check trace files specifically
    if upload_mode == "rocksdb":
        xml_files = [f for f in all_files if f.name.startswith('trace') and f.suffix == '.xml']
        json_files = [f for f in all_files if f.name.startswith('trace') and f.suffix == '.json']
        trace_files = xml_files + json_files
        
        if len(trace_files) == 0:
            logger.debug(f"No trace files found for RocksDB check in {log_directory}")
            print(f"JOSHUA_LOGTOOL_RESULT: SKIPPED - No trace files found", file=sys.stdout)
            return
            
        if not _is_rocksdb_test(trace_files):
            logger.debug("Not a RocksDB test, skipping upload")
            print(f"JOSHUA_LOGTOOL_RESULT: SKIPPED - Not a RocksDB test", file=sys.stdout)
            return

    # Determine ensemble ID
    final_ensemble_id = ensemble_id or _extract_ensemble_id(work_directory)
    if not final_ensemble_id:
        logger.error(f"Ensemble ID missing in work directory {work_directory}")
        print(f"JOSHUA_LOGTOOL_RESULT: FAILED - Ensemble ID missing", file=sys.stdout)
        raise RuntimeError(f"Ensemble ID missing in work directory {work_directory}")

    # Create and upload archive
    try:
        with tempfile.NamedTemporaryFile(suffix='.tar.xz') as archive:
            _tar_logs(all_files, pathlib.Path(archive.name))
            
            archive_size = os.path.getsize(archive.name)
            if archive_size == 0:
                logger.error("Archive is empty")
                print(f"JOSHUA_LOGTOOL_RESULT: FAILED - Archive is empty", file=sys.stdout)
                return
            
            # Upload to FDB
            subspace = _get_log_subspace(final_ensemble_id, test_uid)
            joshua._insert_blob(joshua.db, subspace, archive, offset=0)
            
            print(f"JOSHUA_LOGTOOL_RESULT: SUCCESS - Uploaded {len(all_files)} files ({archive_size} bytes)", file=sys.stdout)
            
    except Exception as e:
        logger.error(f"Error during upload: {e}")
        print(f"JOSHUA_LOGTOOL_RESULT: FAILED - {e}", file=sys.stdout)
        raise

def download_logs(ensemble_id: str, test_uid: str, output_dir: str = None):
    """Download and extract all test files for a specific test from Joshua FDB cluster"""
    with tempfile.NamedTemporaryFile() as archive:
        subspace = _get_log_subspace(ensemble_id, test_uid)
        joshua._read_blob(joshua.db, subspace, archive)
        
        # Check archive size
        archive.seek(0, 2)
        archive_size = archive.tell()
        archive.seek(0)
        
        if archive_size == 0:
            print(f"No logs found for test {test_uid}")
            return
        
        # Create output directory if specified
        if output_dir:
            output_path = pathlib.Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            os.chdir(output_path)
            print(f"Extracting to: {output_path}")
        
        # List archive contents before extraction (for debugging)
        try:
            list_command = ["tar", "-t", "-f", str(archive.name)]
            result = subprocess.run(list_command, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Archive contents:")
                lines = result.stdout.strip().split('\n')
                for line in lines[:10]:  # Show first 10 files
                    print(f"  {line}")
                if len(lines) > 10:
                    print(f"  ... and {len(lines) - 10} more files")
            else:
                print(f"Could not list archive contents: {result.stderr}")
        except Exception as e:
            print(f"Error listing archive: {e}")
        
        _tar_extract(pathlib.Path(archive.name))
        print(f"Downloaded and extracted {archive_size} bytes of logs")
        
        # Show what was actually extracted
        if output_dir:
            print(f"Files extracted to {output_path}:")
            for item in sorted(output_path.rglob('*'))[:10]:  # Show first 10 items
                if item.is_file():
                    print(f"  {item.relative_to(output_path)}")
            if len(list(output_path.rglob('*'))) > 10:
                print(f"  ... and {len(list(output_path.rglob('*'))) - 10} more items")

def list_commands(ensemble_id: str):
    """List download commands for all failed tests in an ensemble"""
    for item in joshua.tail_results(ensemble_id, errors_only=True):
        test_harness_output = item[4]
        match = TEST_UID_REGEXP.search(test_harness_output)
        if not match:
            continue
        test_uid = match.groupdict()["uid"]
        print(f"python3 {__file__} download --ensemble-id {ensemble_id} --test-uid {test_uid}")

def _setup_args():
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Upload/download FoundationDB log files to Joshua cluster"
    )
    parser.add_argument(
        "--cluster-file",
        type=str,
        default="/etc/foundationdb/fdb.cluster",
        help="Path to cluster file",
    )
    parser.add_argument(
        "--debug", action="store_true", default=False, help="Add debug logging"
    )
    parser.add_argument(
        "--quiet", action="store_true", default=False, help="Reduce output verbosity"
    )

    subparsers = parser.add_subparsers(help="Possible actions", dest="action")

    # Upload subcommand
    upload_parser = subparsers.add_parser("upload", help="Upload all files (excluding simfdb) to Joshua cluster")
    upload_parser.add_argument(
        "--work-directory", type=str, default=os.getcwd(), help="Work directory"
    )
    upload_parser.add_argument(
        "--log-directory", type=str, required=True, help="Directory containing all test files to upload (simfdb directories will be excluded)"
    )
    upload_parser.add_argument(
        "--ensemble-id", type=str, default=None, required=False, help="Ensemble ID"
    )
    upload_parser.add_argument("--test-uid", type=str, required=True, help="Test UID")
    upload_parser.add_argument(
        "--check-rocksdb",
        action="store_true",
        default=False,
        help="Only upload if test involves RocksDB (checks trace files for RocksDB events)",
    )

    # Download subcommand
    download_parser = subparsers.add_parser("download", help="Download logs from Joshua cluster")
    download_parser.add_argument(
        "--ensemble-id", type=str, required=True, help="Ensemble ID"
    )
    download_parser.add_argument("--test-uid", type=str, required=True, help="Test UID")
    download_parser.add_argument(
        "--output-dir", type=str, default=None, help="Directory to extract files to (defaults to current directory)"
    )

    # List subcommand
    list_parser = subparsers.add_parser("list", help="List failed tests in ensemble")
    list_parser.add_argument(
        "--ensemble-id", type=str, required=True, help="Ensemble ID"
    )

    return parser.parse_args()

def _main():
    """Main entry point"""
    args = _setup_args()

    # Setup logging based on flags
    if args.quiet:
        logging.basicConfig(level=logging.WARNING)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Initialize Joshua FDB connection
    joshua.open(args.cluster_file)

    # Execute requested action
    if args.action == "upload":
        upload_mode = "rocksdb" if args.check_rocksdb else "all"
        report_error(
            work_directory=args.work_directory,
            log_directory=args.log_directory,
            ensemble_id=args.ensemble_id,
            test_uid=args.test_uid,
            upload_mode=upload_mode,
        )
    elif args.action == "download":
        download_logs(ensemble_id=args.ensemble_id, test_uid=args.test_uid, output_dir=args.output_dir)
    elif args.action == "list":
        list_commands(ensemble_id=args.ensemble_id)

if __name__ == "__main__":
    _main()
