#!/usr/bin/env bash
#
# Test backup with BulkDump mode and restore validation using audit_storage.
#
# This test uses the restore validation feature (see design/validating_restored_data_using_one_cluster.md):
# 1. Loads test data into the database
# 2. Creates backup using BulkDump mode (writes SST files via storage servers)
# 3. Clears database and restores using BulkLoad mode
# 4. Validates using audit_storage validate_restore:
#    - Restores with --add-prefix using BulkLoad mode
#    - BulkLoad with prefix transforms the task range to destination range
#    - Storage servers that own the destination range read SST and apply prefix
#    - Compares original vs restored data using audit_storage validate_restore
# 5. Tests encryption mismatch handling
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
  
  # Edit the url. Backup adds 'data' to the path. Need this url for
  # cleanup of test data.
  local edited_url=$(echo "${local_url}" | sed -e "s/ctest/data\/ctest/" )
  readonly edited_url
  if [[ "${USE_S3}" == "true" ]]; then
    # Run this rm only if s3. In MockS3Server, it would fail because
    # bucket doesn't exist yet (they are lazily created).
    local preclear_cmd=("${local_build_dir}/bin/s3client")
    preclear_cmd+=("${KNOBS[@]}")
    preclear_cmd+=("--tls-ca-file" "${TLS_CA_FILE}")
    preclear_cmd+=("--blob-credentials" "${credentials}")
    preclear_cmd+=("--log" "--logdir" "${local_scratch_dir}")
    preclear_cmd+=("rm" "${edited_url}")
    
    if ! "${preclear_cmd[@]}"; then
      err "Failed pre-cleanup rm of ${edited_url}"
      return 1
    fi
  fi
  log "Load minimal data for faster backup"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run backup with BulkDump mode"
  if ! run_backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "bulkdump" "${credentials}"; then
    err "Failed backup"
    return 1
  fi
  
  # Step 1: Basic BulkLoad restore test (clear, restore, verify)
  # This populates normalKeys with restored data that the audit will compare against.
  log "Testing basic BulkLoad restore..."
  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  
  log "Restore using BulkLoad mode"
  if ! run_restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "bulkload" "${credentials}"; then
    err "Failed BulkLoad restore"
    return 1
  fi
  
  log "Verify restored data matches original"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of restored data"
    return 1
  fi
  log "Basic BulkLoad restore test PASSED"
  
  # Step 2: Validate using audit_storage validate_restore
  # This restores again with prefix to system keys.
  # BulkLoad with prefix:
  # 1. Reads SST files directly (no data move to preserve normalKeys)
  # 2. Transforms keys with prefix
  # 3. Writes to system keyspace via transaction
  # The audit compares normalKeys data vs restored prefixed data.
  log "Running audit_storage validate_restore validation..."
  if ! validate_restore_with_audit "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "bulkload" "${credentials}"; then
    err "Failed audit-based restore validation"
    return 1
  fi
  log "Audit-based restore validation PASSED"
  
  # Test encryption mismatches (using shared function)
  log "Testing encryption mismatches"
  test_encryption_mismatches "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "${credentials}"

  # Cleanup test data.
  local cleanup_cmd=("${local_build_dir}/bin/s3client")
  cleanup_cmd+=("${KNOBS[@]}")
  
  # Only add TLS CA file for real S3, not MockS3Server
  if [[ "${USE_S3}" == "true" ]]; then
    cleanup_cmd+=("--tls-ca-file" "${TLS_CA_FILE}")
  fi
  
  cleanup_cmd+=("--blob-credentials" "${credentials}")
  cleanup_cmd+=("--log" "--logdir" "${local_scratch_dir}")
  cleanup_cmd+=("rm" "${edited_url}")
  
  if ! "${cleanup_cmd[@]}"; then
    err "Failed rm of ${edited_url}"
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

# Globals
# TEST_SCRATCH_DIR gets set below. Tests should be their data in here.
# It gets cleaned up on the way out of the test.
TEST_SCRATCH_DIR=
readonly HTTP_VERBOSE_LEVEL=2
readonly TAG="test_backup_bulkdump"
# Should we use S3? If USE_S3 is not defined, then check if
# OKTETO_NAMESPACE is defined (It is defined on the okteto
# internal apple dev environments where S3 is available).
readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"

# Set KNOBS based on whether we're using real S3 or MockS3Server
if [[ "${USE_S3}" == "true" ]]; then
  # Use AWS KMS encryption for real S3
  KNOBS=("--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
else
  # No encryption for MockS3Server
  KNOBS=("--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
fi
readonly KNOBS

# Set TLS_CA_FILE only when using real S3, not for MockS3Server
if [[ "${USE_S3}" == "true" ]]; then
  # Try to find a valid TLS CA file if not explicitly set
  if [[ -z "${TLS_CA_FILE:-}" ]]; then
    # Common locations for TLS CA files on different systems
    for ca_file in "/etc/pki/tls/cert.pem" "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem" "/etc/ssl/certs/ca-certificates.crt" "/etc/pki/tls/certs/ca-bundle.crt" "/etc/ssl/cert.pem" "/usr/local/share/ca-certificates/"; do
      if [[ -f "${ca_file}" ]]; then
        TLS_CA_FILE="${ca_file}"
        break
      fi
    done
  fi
  TLS_CA_FILE="${TLS_CA_FILE:-}"
else
  # For MockS3Server, don't use TLS
  TLS_CA_FILE=""
fi
readonly TLS_CA_FILE
# Clear these environment variables. fdbbackup goes looking for them
# and if EITHER is set, it will go via a proxy instead of to where we
# want it to go.
unset HTTP_PROXY
unset HTTPS_PROXY

# Get the working directory for this script.
if ! path=$(resolve_to_absolute_path "${BASH_SOURCE[0]}"); then
  err "Failed resolve_to_absolute_path"
  exit 1
fi
if ! cwd=$( cd -P "$( dirname "${path}" )" >/dev/null 2>&1 && pwd ); then
  err "Failed dirname on ${path}"
  exit 1
fi
readonly cwd
# Use minimal data to prevent infinite backup logs
export FDB_DATA_KEYCOUNT=10
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/tests_common.sh"; then
  err "Failed to source tests_common.sh"
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/backup_tests_common.sh"; then
  err "Failed to source backup_tests_common.sh"
  exit 1
fi
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
readonly path_prefix="ctests"
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

# Source in the fdb cluster.
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Startup fdb cluster and backup agent with BulkLoad knobs
# Use 2 storage servers so BulkLoad can find a different server than the BulkDump source
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 2 "--knob_shard_encode_location_metadata=1" "--knob_enable_read_lock_on_range=1" "--knob_blobstore_encryption_type=aws:kms"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"
if ! start_backup_agent "${build_dir}" "${TEST_SCRATCH_DIR}" "${KNOBS[@]}"; then
  err "Failed start backup_agent"
  exit 1
fi
log "Backup_agent is up"

# Run tests.
test="test_bulkdump_bulkload"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_bulkdump_bulkload "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_bulkdump_bulkload"
