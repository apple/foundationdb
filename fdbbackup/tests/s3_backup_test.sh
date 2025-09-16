#!/bin/bash
#
# Test backup and restore from s3.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up a seaweedfs instance or use S3
# if it is available. We then run a backup to 'S3' and then
# a restore. We verify the restore is the same as the original.
#
# Debugging, run this script w/ the -x flag: e.g. bash -x s3_backup_test.sh...
# You can also disable the cleanup. This will leave processes up
# so you can manually rerun commands or peruse logs and data
# under SCRATCH_DIR.
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
function cleanup {
  if type shutdown_fdb_cluster &> /dev/null; then
    shutdown_fdb_cluster
  fi
  if type shutdown_weed &> /dev/null; then
    shutdown_weed "${TEST_SCRATCH_DIR}"
  fi
  if type shutdown_aws &> /dev/null; then
    shutdown_aws "${TEST_SCRATCH_DIR}"
  fi
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

function create_encryption_key_file {
  local key_file="${1}"
  log "Creating encryption key file at ${key_file}"
  dd if=/dev/urandom bs=32 count=1 of="${key_file}" 2>/dev/null
  chmod 600 "${key_file}"
}

# Run the fdbbackup command.
# $1 The build directory
# $2 The scratch directory
# $3 The S3 url
# $4 credentials file
# $5 encryption key file (optional)
function backup {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_credentials="${4}"
  local local_encryption_key_file="${5:-}"
  
  # Backup to s3. Without the -k argument in the below, the backup gets
  # 'No restore target version given, will use maximum restorable version from backup description.'
  # TODO: Why is -k needed?
  local cmd_args=(
    "-C" "${local_scratch_dir}/loopback_cluster/fdb.cluster"
    "-t" "${TAG}" "-w"
    "-d" "${local_url}"
    "-k" '"" \xff'
    "--log" "--logdir=${local_scratch_dir}"
    "--blob-credentials" "${local_credentials}"
  )

  if [[ -n "${local_encryption_key_file}" ]]; then
    cmd_args+=("--encryption-key-file" "${local_encryption_key_file}")
  fi

  for knob in "${KNOBS[@]}"; do
    cmd_args+=("${knob}")
  done

  if ! "${local_build_dir}"/bin/fdbbackup start "${cmd_args[@]}"; then
    err "Start fdbbackup failed"
    return 1
  fi
}

# Run the fdbrestore command.
# $1 The build directory
# $2 The scratch directory
# $3 The S3 url
# $4 credentials file
# $5 encryption key file (optional)
function restore {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_credentials="${4}"
  local local_encryption_key_file="${5:-}"
  
  local cmd_args=(
    "--dest-cluster-file" "${local_scratch_dir}/loopback_cluster/fdb.cluster"
    "-t" "${TAG}" "-w"
    "-r" "${url}"
    "--log" "--logdir=${local_scratch_dir}"
    "--blob-credentials" "${local_credentials}"
  )

  if [[ -n "${local_encryption_key_file}" ]]; then
    cmd_args+=("--encryption-key-file" "${local_encryption_key_file}")
  fi

  for knob in "${KNOBS[@]}"; do
    cmd_args+=("${knob}")
  done

  if ! "${local_build_dir}"/bin/fdbrestore start "${cmd_args[@]}"; then
    err "Start fdbrestore failed"
    return 1
  fi
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
  
  log "Load data"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  # Edit the url. Backup adds 'data' to the path. Need this url for
  # cleanup of test data.
  local edited_url=$(echo "${local_url}" | sed -e "s/ctest/data\/ctest/" )
  readonly edited_url
  if [[ "${USE_S3}" == "true" ]]; then
    # Run this rm only if s3. In seaweed, it would fail because
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
  log "Run s3 backup"
  if ! backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}" "${local_encryption_key_file}"; then
    err "Failed backup"
    return 1
  fi
  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  # Test encryption mismatches (always run to test both encrypted and unencrypted scenarios)
  log "Testing encryption mismatches"
  test_encryption_mismatches "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}" "${local_encryption_key_file}"

  log "Restore from s3"
  if ! restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}" "${local_encryption_key_file}"; then
    err "Failed restore"
    return 1
  fi
  log "Verify restore"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of data in fdb"
    return 1
  fi
  # Cleanup test data.
  local cleanup_cmd=("${local_build_dir}/bin/s3client")
  cleanup_cmd+=("${KNOBS[@]}")
  
  # Only add TLS CA file for real S3, not SeaweedFS
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

# Test all encryption mismatch scenarios - all should fail
# $1 The build directory
# $2 The scratch directory
# $3 The S3 url
# $4 credentials file
# $5 encryption key file used for backup (empty if no encryption)
function test_encryption_mismatches {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_credentials="${4}"
  local backup_encryption_key_file="${5}"

  # Create separate log directory for encryption mismatch tests
  local mismatch_logdir="${local_scratch_dir}/encryption_mismatch_logs"
  mkdir -p "${mismatch_logdir}"

  if [[ -n "${backup_encryption_key_file}" ]]; then
    # Backup was encrypted - test mismatches
    log "Testing encryption mismatches for encrypted backup"

    # Test 1: Encrypted backup → restore without encryption (should fail)
    log "Test 1: Attempting restore without encryption on encrypted backup (should fail)"
    local cmd_args1=(
      "--dest-cluster-file" "${local_scratch_dir}/loopback_cluster/fdb.cluster"
      "-t" "${TAG}" "-w"
      "-r" "${local_url}"
      "--log" "--logdir=${mismatch_logdir}"
      "--blob-credentials" "${local_credentials}"
    )
    for knob in "${KNOBS[@]}"; do
      cmd_args1+=("${knob}")
    done

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${cmd_args1[@]}"
    local exit_code1=$?
    set -e

    if [[ ${exit_code1} -eq 0 ]]; then
      err "ERROR: Restore without encryption on encrypted backup succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore without encryption on encrypted backup failed as expected"

    # Test 3: Encrypted backup → restore with wrong encryption key (should fail)
    local wrong_key_file="${local_scratch_dir}/wrong_key"
    create_encryption_key_file "${wrong_key_file}"

    log "Test 3: Attempting restore with wrong encryption key (should fail)"
    local cmd_args3=(
      "--dest-cluster-file" "${local_scratch_dir}/loopback_cluster/fdb.cluster"
      "-t" "${TAG}" "-w"
      "-r" "${local_url}"
      "--log" "--logdir=${mismatch_logdir}"
      "--blob-credentials" "${local_credentials}"
      "--encryption-key-file" "${wrong_key_file}"
    )
    for knob in "${KNOBS[@]}"; do
      cmd_args3+=("${knob}")
    done

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${cmd_args3[@]}"
    local exit_code3=$?
    set -e

    rm -f "${wrong_key_file}"

    if [[ ${exit_code3} -eq 0 ]]; then
      err "ERROR: Restore with wrong encryption key succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore with wrong encryption key failed as expected"

  else
    # Backup was not encrypted - test mismatch
    log "Testing encryption mismatch for unencrypted backup"

    # Test 2: Unencrypted backup → restore with encryption (should fail)
    local any_key_file="${local_scratch_dir}/any_key"
    create_encryption_key_file "${any_key_file}"

    log "Test 2: Attempting restore with encryption on unencrypted backup (should fail)"
    local cmd_args2=(
      "--dest-cluster-file" "${local_scratch_dir}/loopback_cluster/fdb.cluster"
      "-t" "${TAG}" "-w"
      "-r" "${local_url}"
      "--log" "--logdir=${mismatch_logdir}"
      "--blob-credentials" "${local_credentials}"
      "--encryption-key-file" "${any_key_file}"
    )
    for knob in "${KNOBS[@]}"; do
      cmd_args2+=("${knob}")
    done

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${cmd_args2[@]}"
    local exit_code2=$?
    set -e

    rm -f "${any_key_file}"

    if [[ ${exit_code2} -eq 0 ]]; then
      err "ERROR: Restore with encryption on unencrypted backup succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore with encryption on unencrypted backup failed as expected"
  fi

  # Clean up separate log directory
  rm -rf "${mismatch_logdir}"

  log "All encryption mismatch tests completed successfully"
  return 0
}

# set -o xtrace   # a.k.a set -x  # Set this one when debugging (or 'bash -x THIS_SCRIPT').
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Parse command line arguments
USE_ENCRYPTION=false
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
set -- "${PARAMS[@]}"

# Globals
# TEST_SCRATCH_DIR gets set below. Tests should be their data in here.
# It gets cleaned up on the way out of the test.
TEST_SCRATCH_DIR=
readonly HTTP_VERBOSE_LEVEL=10
readonly TAG="test_backup"
# Should we use S3? If USE_S3 is not defined, then check if
# OKTETO_NAMESPACE is defined (It is defined on the okteto
# internal apple dev environments where S3 is available).
readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"

# Set KNOBS based on whether we're using real S3 or SeaweedFS
if [[ "${USE_S3}" == "true" ]]; then
  # Use AWS KMS encryption for real S3
  KNOBS=("--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
else
  # No encryption for SeaweedFS
  KNOBS=("--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
fi
readonly KNOBS

# Set TLS_CA_FILE only when using real S3, not for SeaweedFS
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
  # For SeaweedFS, don't use TLS
  TLS_CA_FILE=""
fi
readonly TLS_CA_FILE
# Clear these environment variables. fdbbackup goes looking for them
# and if EITHER is set, it will go via a proxy instead of to where we.
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
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/tests_common.sh"; then
  err "Failed to source tests_common.sh"
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

# Set host, bucket, and blob_credentials_file whether seaweed or s3.
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
  log "Testing against seaweedfs"
  # Now source in the seaweedfs fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/../../fdbclient/tests/seaweedfs_fixture.sh"; then
    err "Failed to source seaweedfs_fixture.sh"
    exit 1
  fi
  if ! TEST_SCRATCH_DIR=$(create_weed_dir "${scratch_dir}"); then
    err "Failed create of the weed dir." >&2
    return 1
  fi
  readonly TEST_SCRATCH_DIR
  if ! host=$( run_weed "${scratch_dir}" "${TEST_SCRATCH_DIR}"); then
    err "Failed to run seaweed"
    return 1
  fi
  readonly host
  readonly bucket="${SEAWEED_BUCKET}"
  readonly region="all_regions"
  # Reference a non-existent blob file (its ignored by seaweed)
  readonly blob_credentials_file="${TEST_SCRATCH_DIR}/blob_credentials.json"
  # Let the connection to seaweed be insecure -- not-TLS -- because just awkward to set up.
  query_str="bucket=${bucket}&region=${region}&secure_connection=0"
  # Set environment variables for SeaweedFS too
  export FDB_BLOB_CREDENTIALS="${blob_credentials_file}"
fi

# Source in the fdb cluster.
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Startup fdb cluster and backup agent.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 1; then
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
test="test_s3_backup_and_restore"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_s3_backup_and_restore "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_s3_backup_and_restore"