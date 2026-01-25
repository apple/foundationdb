#!/usr/bin/env bash
#
# Test backup and restore from s3.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up MockS3Server or use S3
# if it is available. We then run a backup to 'S3' and then
# a restore. We verify the restore is the same as the original.
#
# Debugging:
#   - Run with -x flag: bash -x s3_backup_test.sh...
#   - Preserve test data: PRESERVE_TEST_DATA=1 ./s3_backup_test.sh ...
#     This will leave all test data including MockS3 persistence files
#     in the test scratch directory for analysis after the test completes.
#
# Usage:
#   s3_backup_unified.sh <source_dir> <build_dir> [scratch_dir] [--encrypt]
#
# See https://apple.github.io/foundationdb/backups.html

# Install signal traps. Depends on globals being set.
# Calls the cleanup function.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
# Has a hard 30-second timeout to prevent CTest timeouts.
function cleanup {
  echo "$(date -Iseconds) cleanup: starting (with 30s hard timeout)"
  start_cleanup_watchdog 30
  
  # Check if test data should be preserved (common function from tests_common.sh)
  if cleanup_with_preserve_check; then
    echo "$(date -Iseconds) cleanup: preserving test data, skipping cleanup"
    cancel_cleanup_watchdog
    return 0
  fi
  
  echo "$(date -Iseconds) cleanup: shutting down FDB cluster"
  if type shutdown_fdb_cluster &> /dev/null; then
    shutdown_fdb_cluster
  else
    echo "$(date -Iseconds) cleanup: shutdown_fdb_cluster not available"
  fi
  
  echo "$(date -Iseconds) cleanup: shutting down MockS3"
  if type shutdown_mocks3 &> /dev/null; then
    shutdown_mocks3
  else
    echo "$(date -Iseconds) cleanup: shutdown_mocks3 not available"
  fi
  
  echo "$(date -Iseconds) cleanup: shutting down AWS"
  if type shutdown_aws &> /dev/null; then
    shutdown_aws "${TEST_SCRATCH_DIR}"
  else
    echo "$(date -Iseconds) cleanup: shutdown_aws not available"
  fi
  
  # Clean up encryption key file
  if [[ -n "${ENCRYPTION_KEY_FILE:-}" ]] && [[ -f "${ENCRYPTION_KEY_FILE}" ]]; then
    echo "$(date -Iseconds) cleanup: removing encryption key file: ${ENCRYPTION_KEY_FILE}"
    rm -f "${ENCRYPTION_KEY_FILE}"
  fi
  
  echo "$(date -Iseconds) cleanup: complete"
  cancel_cleanup_watchdog
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

function create_encryption_key_file {
  local key_file="${1}"
  log "Creating encryption key file at ${key_file}"
  dd if=/dev/urandom bs=32 count=1 of="${key_file}" 2>/dev/null
  chmod 600 "${key_file}"
}

# Run a backup to s3 and then a restore.
# $1 The url to use
# $2 the scratch directory
# $3 The credentials file.
# $4 build directory
# $5 encryption key file (optional)
function test_s3_backup_and_restore {
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
  log "Load data"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run s3 backup"
  if ! run_backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "" "${credentials}"; then
    err "Failed backup"
    return 1
  fi

  test_fdbcli_status_json_for_bkup "${local_build_dir}" "${local_scratch_dir}"

  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  # Test encryption mismatches (always run to test both encrypted and unencrypted scenarios)
  log "Testing encryption mismatches"
  test_encryption_mismatches "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "${credentials}"

  log "Restore from s3"
  if ! run_restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "" "${credentials}"; then
    err "Failed restore"
    return 1
  fi
  log "Verify restore"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of data in fdb"
    return 1
  fi
  
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

# set -o xtrace   # a.k.a set -x  # Set this one when debugging (or 'bash -x THIS_SCRIPT').
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Parse command line arguments
USE_ENCRYPTION=false
USE_PARTITIONED_LOG=$(((RANDOM % 2)) && echo true || echo false )
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
readonly TAG="test_backup"

# Setup common environment (USE_S3, KNOBS, TLS_CA_FILE, clears HTTP_PROXY/HTTPS_PROXY)
setup_backup_test_environment 10
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

# Startup fdb cluster and backup agent
setup_fdb_cluster_with_backup "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 1

# Run tests.
test="test_s3_backup_and_restore"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_s3_backup_and_restore "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_s3_backup_and_restore"
