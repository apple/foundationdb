#!/usr/bin/env bash
#
# s3_backup_bulkdump_bulkload.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Test BulkLoad restore validation against traditional restore.
#
# This test validates BulkLoad produces identical results to traditional restore
# by comparing two restore methods using audit_storage validate_restore:
#
# 1. Loads test data into the database
# 2. Creates backup using "both" mode (writes BOTH range files AND SST files)
#    - Range files are used by traditional restore
#    - SST files are used by BulkLoad restore
# 3. Restores with --add-prefix to system keyspace using TRADITIONAL (rangefile) mode
#    - This creates a "known good" baseline in system keys
# 4. Clears normalKeys (original data)
# 5. Restores to normalKeys using BULKLOAD mode (reads SST files)
#    NOTE: If encryption is enabled, uses rangefile mode instead (BulkLoad doesn't support encryption)
# 6. Runs audit_storage validate_restore to compare (skipped if encryption enabled):
#    - BulkLoad-restored data (in normalKeys)
#    - Traditional-restored data (in system key prefix)
#    - This validates BulkLoad produces identical results to traditional restore
# 7. Cleans up validation prefix data
# 8. Tests encryption mismatch handling
#
# Usage:
#   s3_backup_bulkdump_bulkload.sh <source_dir> <build_dir> [scratch_dir] [--encrypt]

# Install signal traps. Depends on globals being set.
# Calls the cleanup function.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
function cleanup {
  # Check if test data should be preserved (common function from tests_common.sh)
  if cleanup_with_preserve_check; then
    return 0
  fi
  
  if type shutdown_fdb_cluster &> /dev/null; then
    shutdown_fdb_cluster
  fi
  if type shutdown_mocks3 &> /dev/null; then
    shutdown_mocks3
  fi
  if type shutdown_aws &> /dev/null; then
    shutdown_aws "${TEST_SCRATCH_DIR}"
  fi
  
  # Clean up encryption key file
  if [[ -n "${ENCRYPTION_KEY_FILE:-}" ]] && [[ -f "${ENCRYPTION_KEY_FILE}" ]]; then
    rm -f "${ENCRYPTION_KEY_FILE}"
  fi
}

# Resolve passed in reference to an absolute path.
# e.g. /tmp on mac is actually /private/tmp.
# $1 path to resolve
function resolve_to_absolute_path {
  local p="${1}"
  while [[ -h "${p}" ]]; do
    dir=$( cd -P "$( dirname "${p}" )" >/dev/null 2>&1 && pwd )
    p=$(readlink "${p}")
    [[ ${p} != /* ]] && p="${dir}/${p}"
  done
  realpath "${p}"
}

# Constants for validation prefix
readonly VALIDATION_PREFIX='\xff\x02/rlog/'
readonly VALIDATION_PREFIX_END='\xff\x02/rlog0'

# Restore with prefix for validation - does NOT run audit or cleanup
# Use this when you want to keep the prefixed data for later comparison
# $1 build directory, $2 scratch directory, $3 backup url, $4 tag
# $5 encryption key file (optional), $6 restore mode (optional), $7 blob credentials file (optional)
function restore_with_prefix_for_validation {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_tag="${4}"
  local local_encryption_key_file="${5:-}"
  local restore_mode="${6:-}"
  local blob_credentials="${7:-}"
  
  local fdbcli="${local_build_dir}/bin/fdbcli"
  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  
  log "Restoring backup with prefix ${VALIDATION_PREFIX} for validation..."
  local cmd_args=(
    "-t" "${local_tag}_validate"
    "-r" "${local_url}"
    "--add-prefix" "${VALIDATION_PREFIX}"
  )
  add_base_args cmd_args "${cluster_file}" "${local_scratch_dir}"
  add_common_optional_args cmd_args "${blob_credentials}" "${restore_mode}" "${local_encryption_key_file}"

  if ! "${local_build_dir}"/bin/fdbrestore start "${cmd_args[@]}"; then
    err "Failed to start validation restore"
    return 1
  fi

  if ! run_restore_wait "${local_build_dir}" "${local_scratch_dir}" "${local_tag}_validate"; then
    err "Validation restore failed to complete"
    return 1
  fi
  
  # Debug: Check if data was restored to the prefix
  log "Checking restored data at prefix..."
  local restored_check
  restored_check=$("${fdbcli}" -C "${cluster_file}" --exec "option on READ_SYSTEM_KEYS; getrangekeys \"${VALIDATION_PREFIX}\" \"${VALIDATION_PREFIX_END}\" 10" 2>&1) || true
  log "Restored data check: ${restored_check}"
  
  log "Validation restore with prefix completed (data kept for comparison)"
  return 0
}

# Run audit_storage validate_restore to compare normalKeys vs prefixed data
# Call this AFTER restoring with prefix and AFTER populating normalKeys with data to compare
# $1 build directory, $2 scratch directory
function run_validate_restore_audit {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  
  local fdbcli="${local_build_dir}/bin/fdbcli"
  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  
  log "Running audit_storage validate_restore..."
  
  local audit_output
  local audit_id
  local max_retries=10
  local retry_delay=5
  local attempt=0
  
  while [[ $attempt -lt $max_retries ]]; do
    attempt=$((attempt + 1))
    log "Audit attempt ${attempt}/${max_retries}..."
    
    audit_output=$("${fdbcli}" -C "${cluster_file}" --exec "audit_storage validate_restore \"\" \\xff" 2>&1)
    log "Audit command output: ${audit_output}"
    
    audit_id=$(echo "${audit_output}" | grep -oE '[0-9a-f]{32}' | head -1)
    
    if [[ -n "${audit_id}" ]]; then
      log "Audit started with ID: ${audit_id}"
      break
    fi
    
    if echo "${audit_output}" | grep -qE "1221|1230|1010"; then
      log "Transient error detected, retrying in ${retry_delay}s..."
      sleep $retry_delay
      continue
    fi
    
    err "Failed to extract audit ID from output: ${audit_output}"
    return 1
  done
  
  if [[ -z "${audit_id}" ]]; then
    err "Failed to start audit after ${max_retries} attempts"
    return 1
  fi

  # Monitor audit progress
  local timeout=300
  local poll_interval=5
  local elapsed=0
  
  log "Waiting for audit to complete (${timeout}s timeout)..."
  
  while [[ $elapsed -lt $timeout ]]; do
    sleep $poll_interval
    elapsed=$((elapsed + poll_interval))
    
    local status_output
    status_output=$("${fdbcli}" -C "${cluster_file}" --exec "get_audit_status validate_restore id ${audit_id}" 2>&1)
    
    if echo "${status_output}" | grep -q "Phase.*2"; then
      log "Audit completed successfully after ${elapsed}s"
      return 0
    fi
    
    if echo "${status_output}" | grep -q "Phase.*[34]"; then
      err "Audit failed with status: ${status_output}"
      return 1
    fi
    
    if [[ $((elapsed % 30)) -eq 0 ]]; then
      log "Still waiting for audit (${elapsed}s)... Status: ${status_output}"
    fi
  done
  
  err "Timeout waiting for audit after ${timeout}s"
  return 1
}

# Clean up the validation prefix data from system keyspace
# $1 build directory, $2 scratch directory
function cleanup_validation_prefix {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  
  local fdbcli="${local_build_dir}/bin/fdbcli"
  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  
  log "Cleaning up validation data from ${VALIDATION_PREFIX}..."
  "${fdbcli}" -C "${cluster_file}" --exec "writemode on; clearrange \"${VALIDATION_PREFIX}\" \"${VALIDATION_PREFIX_END}\"" 2>/dev/null || true
  log "Validation prefix data cleaned up"
}

# Run simple BulkDump backup and BulkLoad restore test.
# $1 The url to use
# $2 the scratch directory
# $3 The credentials file.
# $4 build directory
# $5 encryption key file (optional)
function test_bulkdump_bulkload {
  local local_url="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"
  local local_build_dir="${4}"
  local local_encryption_key_file="${5:-}"
  
  # Edit the url. Backup adds 'data' to the path. Need this url for cleanup.
  local edited_url=$(echo "${local_url}" | sed -e "s/ctest/data\/ctest/" )
  readonly edited_url
  if ! s3_preclear_url "${local_build_dir}" "${local_scratch_dir}" "${edited_url}" "${credentials}"; then
    return 1
  fi
  log "Load minimal data for faster backup"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  # Use "both" mode to create BOTH range files AND SST files
  # This allows traditional restore (uses range files) and BulkLoad (uses SST files) to both work
  log "Run backup with 'both' mode (creates range files AND SST files)"
  if ! run_backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "both" "${credentials}"; then
    err "Failed backup"
    return 1
  fi
  
  # BulkLoad validation: compare BulkLoad restore vs traditional restore
  # 1. Restore with prefix using TRADITIONAL (rangefile) mode - this is our "known good" baseline
  # 2. Clear normalKeys (original data)
  # 3. Restore to normalKeys using BULKLOAD mode
  # 4. Run audit_storage validate_restore to compare BulkLoad result vs traditional result
  # This validates that BulkLoad produces identical results to traditional restore.
  
  # Step 1: Restore with prefix using traditional rangefile mode (keep the data)
  log "Restoring with prefix using traditional rangefile mode..."
  if ! restore_with_prefix_for_validation "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "rangefile" "${credentials}"; then
    err "Failed validation restore with prefix"
    return 1
  fi
  log "Traditional restore with prefix completed"
  
  # Step 2: Clear normalKeys (original data)
  log "Clear fdb normalKeys data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  
  # Step 3: Restore to normalKeys
  # NOTE: BulkLoad doesn't support encryption yet, so use traditional restore when encrypted
  if [[ -n "${local_encryption_key_file}" ]]; then
    log "Restore using rangefile mode (BulkLoad doesn't support encryption yet)"
    if ! run_restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "rangefile" "${credentials}"; then
      err "Failed rangefile restore"
      return 1
    fi
    log "SKIPPING BulkLoad validation (encryption not supported by BulkLoad)"
    # Clean up the prefixed validation data
    cleanup_validation_prefix "${local_build_dir}" "${local_scratch_dir}"
  else
    log "Restore using BulkLoad mode"
    if ! run_restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "bulkload" "${credentials}"; then
      err "Failed BulkLoad restore"
      return 1
    fi
  
    # Step 4: Run audit to compare BulkLoad-restored (normalKeys) vs traditional-restored (prefix)
    log "Running audit_storage validate_restore..."
    log "Comparing BulkLoad-restored data against traditional-restored data..."
    if ! run_validate_restore_audit "${local_build_dir}" "${local_scratch_dir}"; then
      err "Failed audit-based restore validation - BulkLoad result differs from traditional restore!"
      return 1
    fi
    log "Audit validation PASSED - BulkLoad produces identical results to traditional restore"
  
    # Step 5: Clean up the prefixed validation data
    log "Cleaning up validation prefix data..."
    cleanup_validation_prefix "${local_build_dir}" "${local_scratch_dir}"
  fi
  
  # Additional verification
  log "Verify restored data matches expected values"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of restored data"
    return 1
  fi
  log "BulkLoad restore verification PASSED"
  
  # Test encryption mismatches (using shared function)
  log "Testing encryption mismatches"
  test_encryption_mismatches "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "${credentials}"

  # Cleanup test data.
  if ! s3_cleanup_url "${local_build_dir}" "${local_scratch_dir}" "${edited_url}" "${credentials}"; then
    return 1
  fi
  log "Check for Severity=40 errors"
  if ! grep_for_severity40 "${local_scratch_dir}"; then
    err "Found Severity=40 errors in logs"
    return 1
  fi
}

# (test_encryption_mismatches now in tests_common.sh as test_encryption_mismatches_s3)

# set -o xtrace   # a.k.a set -x  # Set this one when debugging (or 'bash -x THIS_SCRIPT').
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Parse command line arguments (keep original - not shared)
USE_ENCRYPTION=false
USE_PARTITIONED_LOG=false  # Default to false for BulkLoad testing
PARAMS=()

while (( "$#" )); do
  case "$1" in
    --encrypt)
      USE_ENCRYPTION=true
      shift
      ;;
    --encrypt-at-random)
      USE_ENCRYPTION=$(((RANDOM % 2)) && echo true || echo false )
      shift
      ;;
    --partitioned-log-experimental)
      USE_PARTITIONED_LOG=true
      shift
      ;;
    --partitioned-log-experimental-at-random)
      USE_PARTITIONED_LOG=$(((RANDOM % 2)) && echo true || echo false )
      shift
      ;;
    -*|--*=) # unsupported flags
      err "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS+=("$1")
      shift
      ;;
  esac
done

# Set positional arguments in their proper place
if [ ${#PARAMS[@]} -ne 0 ]; then
  set -- "${PARAMS[@]}"
fi

# Get the working directory for this script.
if ! path=$(resolve_to_absolute_path "${BASH_SOURCE[0]}"); then
  echo "Failed resolve_to_absolute_path" >&2
  exit 1
fi
if ! cwd=$( cd -P "$( dirname "${path}" )" >/dev/null 2>&1 && pwd ); then
  echo "Failed dirname on ${path}" >&2
  exit 1
fi
readonly cwd

# Use minimal data to prevent infinite backup logs (must be set BEFORE sourcing tests_common.sh)
export FDB_DATA_KEYCOUNT=10

# Source common test functions first (needed for setup_backup_test_environment)
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/tests_common.sh"; then
  echo "Failed to source tests_common.sh" >&2
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/backup_tests_common.sh"; then
  echo "Failed to source backup_tests_common.sh" >&2
  exit 1
fi

# Globals
TEST_SCRATCH_DIR=
readonly TAG="test_backup_bulkdump"

# Setup common environment (USE_S3, KNOBS, TLS_CA_FILE, clears HTTP_PROXY/HTTPS_PROXY)
setup_backup_test_environment 2
# Process command-line options.
if (( $# < 2 )) || (( $# > 3 )); then
    echo "ERROR: ${0} requires the fdb src and build directories --"
    echo "CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR -- and then, optionally,"
    echo "a directory into which we write scratch test data and logs"
    echo "(otherwise we will write to subdirs under $TMPDIR). We will"
    echo "leave the download of seaweed this directory for other"
    echo "tests to find if they need it. Otherwise, we clean everything"
    echo "else up on our way out."
    echo "Example: ${0} ./foundationdb ./build_output ./scratch_dir [--encrypt]"
    exit 1
fi
if ! source_dir=$(is_fdb_source_dir "${1}"); then
  err "${1} is not an fdb source directory"
  exit 1
fi
readonly source_dir
readonly build_dir="${2}"
if [[ ! -d "${build_dir}" ]]; then
  err "${build_dir} is not a directory"
  exit 1
fi
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  scratch_dir="${3}"
fi
readonly scratch_dir

# Create encryption key file if needed
ENCRYPTION_KEY_FILE=""
if [[ "${USE_ENCRYPTION}" == "true" ]]; then
  log "Enabling encryption for backups"
  ENCRYPTION_KEY_FILE="${scratch_dir}/test_encryption_key_file"
  create_encryption_key_file "${ENCRYPTION_KEY_FILE}"
  log "Created encryption key file at ${ENCRYPTION_KEY_FILE}"
else
  log "Using plaintext for backups"
fi
readonly ENCRYPTION_KEY_FILE
readonly USE_PARTITIONED_LOG

# Set host, bucket, and blob_credentials_file whether MockS3Server or s3.
readonly path_prefix="ctests/$$"
host=
query_str=
blob_credentials_file=
if [[ "${USE_S3}" == "true" ]]; then
  log "Testing against s3"
  # Now source in the aws fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/../../fdbclient/tests/aws_fixture.sh"; then
    err "Failed to source aws_fixture.sh"
    exit 1
  fi
  if ! TEST_SCRATCH_DIR=$( create_aws_dir "${scratch_dir}" ); then
    err "Failed creating local aws_dir"
    exit 1
  fi
  readonly TEST_SCRATCH_DIR
  if ! readarray -t configs < <(aws_setup "${build_dir}" "${TEST_SCRATCH_DIR}"); then
    err "Failed aws_setup"
    return 1
  fi
  readonly host="${configs[0]}"
  readonly bucket="${configs[1]}"
  readonly blob_credentials_file="${configs[2]}"
  readonly region="${configs[3]}"
  query_str="bucket=${bucket}&region=${region}&secure_connection=1"
  # Make these environment variables available for the fdb cluster and backup_agent when s3.
  export FDB_BLOB_CREDENTIALS="${blob_credentials_file}"
  export FDB_TLS_CA_FILE="${TLS_CA_FILE}"
else
  log "Testing against MockS3Server"
  # Now source in the mocks3 fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/../../fdbclient/tests/mocks3_fixture.sh"; then
    err "Failed to source mocks3_fixture.sh"
    exit 1
  fi
  if ! TEST_SCRATCH_DIR=$(mktemp -d "${scratch_dir}/mocks3_backup_test.XXXXXX"); then
    err "Failed create of the mocks3 test dir." >&2
    exit 1
  fi
  readonly TEST_SCRATCH_DIR
  # Pass test scratch dir as persistence directory so files are cleaned up with test
  if ! start_mocks3 "${build_dir}" "${TEST_SCRATCH_DIR}/mocks3_data"; then
    err "Failed to start MockS3Server"
    exit 1
  fi
  readonly host="${MOCKS3_HOST}:${MOCKS3_PORT}"
  readonly bucket="test-bucket"
  readonly region="us-east-1"
  # Create an empty blob credentials file (MockS3Server uses simple auth)
  readonly blob_credentials_file="${TEST_SCRATCH_DIR}/blob_credentials.json"
  echo '{}' > "${blob_credentials_file}"
  # Let the connection to MockS3Server be insecure -- not-TLS
  query_str="bucket=${bucket}&region=${region}&secure_connection=0"
  # Set environment variables for MockS3Server
  export FDB_BLOB_CREDENTIALS="${blob_credentials_file}"
fi

# Startup fdb cluster and backup agent with BulkLoad knobs
# Use 2 storage servers so BulkLoad can find a different server than the BulkDump source
setup_fdb_cluster_with_backup "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 2 \
  "--knob_shard_encode_location_metadata=1" "--knob_enable_read_lock_on_range=1" "--knob_blobstore_encryption_type=aws:kms"

# Run tests.
test="test_bulkdump_bulkload"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_bulkdump_bulkload "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_bulkdump_bulkload"
