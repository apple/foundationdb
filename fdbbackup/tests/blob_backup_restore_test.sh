#!/usr/bin/env bash
#
# Test backup and restore from blob storage (S3, GCS, Azure, or MockS3Server).
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up MockS3Server or use S3/GCS/Azure
# if configured. We then run a backup to blob storage and then
# a restore. We verify the restore is the same as the original.
#
# Debugging:
#   - Run with -x flag: bash -x blob_backup_restore_test.sh...
#   - Preserve test data: PRESERVE_TEST_DATA=1 ./blob_backup_restore_test.sh ...
#     This will leave all test data including MockS3 persistence files
#     in the test scratch directory for analysis after the test completes.
#
# Usage:
#   blob_backup_restore_test.sh <source_dir> <build_dir> [scratch_dir] [--encrypt]
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

  # Best-effort removal of this run's remote objects, so failed runs against a real bucket
  # do not accumulate orphaned per-run key prefix trees.  Must happen after the cluster and
  # backup agents have stopped (no more writers) but while the blob store (MockS3 or real)
  # is still reachable.
  if [[ -n "${REMOTE_CLEANUP_URL:-}" ]] && [[ -n "${TEST_SCRATCH_DIR:-}" ]]; then
    echo "$(date -Iseconds) cleanup: removing remote objects at ${REMOTE_CLEANUP_URL}"
    s3_cleanup_url "${build_dir}" "${TEST_SCRATCH_DIR}" "${REMOTE_CLEANUP_URL}" "${blob_credentials_file}" || true
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

# Verify backups are namespaced by the "prefix" URL parameter: listing containers under the
# key prefix must find the backup while listing the bucket root must not, and the backup's
# data objects must live under the prefixed data tree rather than the bucket-root one.
# $1 build directory, $2 scratch directory, $3 credentials file
function verify_backup_key_prefix_layout {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"

  local base_url="blobstore://${host}?${query_str}"
  local prefix_base_url="blobstore://${host}?${query_str}&prefix=${BACKUP_URL_KEY_PREFIX}"
  local cmd_prefix=("${local_build_dir}/bin/fdbbackup" "list"
    "--blob-credentials" "${credentials}" "--log" "--logdir=${local_scratch_dir}")
  for knob in "${KNOBS[@]}"; do
    cmd_prefix+=("${knob}")
  done

  local listed
  if ! listed=$("${cmd_prefix[@]}" -b "${prefix_base_url}" 2>&1); then
    err "fdbbackup list failed for ${prefix_base_url}: ${listed}"
    return 1
  fi
  if ! output_contains "${listed}" "prefix=${BACKUP_URL_KEY_PREFIX}"; then
    err "Backup not listed under key prefix '${BACKUP_URL_KEY_PREFIX}': ${listed}"
    return 1
  fi

  # The URL returned by list must itself be usable: re-open it with describe.
  local listed_url
  listed_url=$(grep -E "^blobstore://" <<< "${listed}" | grep -F "${url_path_prefix}/${test}" | head -1 | tr -d '[:space:]' || true)
  if [[ -z "${listed_url}" ]]; then
    err "Could not extract this container's URL from list output: ${listed}"
    return 1
  fi
  # Listed URLs carry the '@' credential marker (credentials resolved from files).  MockS3
  # accepts anonymous requests and its credentials file has no entry for the host, so use
  # the same anonymous form as the original test URL there; real providers keep the marker.
  local reopen_url="${listed_url}"
  if [[ "${USE_S3}" != "true" && "${USE_GCS:-false}" != "true" ]]; then
    reopen_url="${reopen_url/"blobstore://@"/"blobstore://"}"
  fi
  local describe_cmd=("${local_build_dir}/bin/fdbbackup" "describe" "-d" "${reopen_url}"
    "--blob-credentials" "${credentials}" "--log" "--logdir=${local_scratch_dir}")
  for knob in "${KNOBS[@]}"; do
    describe_cmd+=("${knob}")
  done
  local described
  if ! described=$("${describe_cmd[@]}" 2>&1); then
    err "fdbbackup describe failed for listed URL ${listed_url}: ${described}"
    return 1
  fi

  local listed_root
  if ! listed_root=$("${cmd_prefix[@]}" -b "${base_url}" 2>&1); then
    err "fdbbackup list failed for ${base_url}: ${listed_root}"
    return 1
  fi
  if output_contains "${listed_root}" "${url_path_prefix}/${test}"; then
    err "Backup leaked outside key prefix '${BACKUP_URL_KEY_PREFIX}': ${listed_root}"
    return 1
  fi

  # Verify the physical object layout: data files exist under the prefixed data tree and the
  # bucket-root data tree for this container stays empty.
  local ls_cmd=("${local_build_dir}/bin/s3client")
  for knob in "${KNOBS[@]}"; do
    ls_cmd+=("${knob}")
  done
  ls_cmd+=("--blob-credentials" "${credentials}" "--log" "--logdir" "${local_scratch_dir}" "ls" "--recursive")
  local prefixed_data root_data
  if ! prefixed_data=$("${ls_cmd[@]}" "blobstore://${host}/${BACKUP_URL_KEY_PREFIX}/data/${url_path_prefix}/${test}/?${query_str}" 2>&1); then
    err "s3client ls failed for the prefixed data tree: ${prefixed_data}"
    return 1
  fi
  if ! root_data=$("${ls_cmd[@]}" "blobstore://${host}/data/${url_path_prefix}/${test}/?${query_str}" 2>&1); then
    err "s3client ls failed for the bucket-root data tree: ${root_data}"
    return 1
  fi
  if ! output_matches_E "${prefixed_data}" "kvranges|logs|snapshots|properties|bulkdump_data"; then
    err "No backup data objects under prefixed data tree: ${prefixed_data}"
    return 1
  fi
  if output_matches_E "${root_data}" "kvranges|logs|snapshots|properties|bulkdump_data"; then
    err "Backup data objects leaked to the bucket-root data tree: ${root_data}"
    return 1
  fi
  log "Verified backup objects are namespaced under key prefix '${BACKUP_URL_KEY_PREFIX}'"
  return 0
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
  # When a key prefix is used the whole layout (data and index trees) lives under the
  # per-run unique prefix, so clean up that prefix tree instead.  The trailing slash keeps
  # the recursive delete from matching sibling prefixes that merely share this prefix as a
  # leading string.
  local edited_url
  if [[ -n "${BACKUP_URL_KEY_PREFIX}" ]]; then
    edited_url="blobstore://${host}/${BACKUP_URL_KEY_PREFIX}/?${query_str}"
  else
    edited_url=$(echo "${local_url}" | sed -e "s/ctest/data\/ctest/" )
  fi
  readonly edited_url
  REMOTE_CLEANUP_URL="${edited_url}"
  if ! s3_preclear_url "${local_build_dir}" "${local_scratch_dir}" "${edited_url}" "${credentials}"; then
    return 1
  fi
  log "Load data"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run blob storage backup"
  if ! run_backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "" "${credentials}"; then
    err "Failed backup"
    return 1
  fi

  if [[ -n "${BACKUP_URL_KEY_PREFIX}" ]]; then
    log "Verify backup key prefix layout"
    if ! verify_backup_key_prefix_layout "${local_build_dir}" "${local_scratch_dir}" "${credentials}"; then
      err "Failed key prefix layout verification"
      return 1
    fi
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

  log "Restore from blob storage"
  if ! run_restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${TAG}" "${local_encryption_key_file}" "" "${credentials}"; then
    err "Failed restore"
    return 1
  fi
  log "Verify restore"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of data in fdb"
    return 1
  fi

  # Cleanup test data (skip if preserving test data for debugging).
  if [[ "${PRESERVE_TEST_DATA:-0}" != "1" ]]; then
    if ! s3_cleanup_url "${local_build_dir}" "${local_scratch_dir}" "${edited_url}" "${credentials}"; then
      return 1
    fi
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
USE_ENCRYPTION=$(((RANDOM % 2)) && echo true || echo false )
USE_PARTITIONED_LOG=$(((RANDOM % 2)) && echo true || echo false )

# Set USE_ENCRYPTION_BLOCK_SIZE only if encryption is enabled
USE_ENCRYPTION_BLOCK_SIZE=false
if [[ "${USE_ENCRYPTION}" == "true" ]]; then
  USE_ENCRYPTION_BLOCK_SIZE=$(((RANDOM % 2)) && echo true || echo false)
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
# Remote URL to best-effort delete in the EXIT trap; set once the test claims its
# remote object tree so even failed runs clean up after themselves.
REMOTE_CLEANUP_URL=
readonly TAG="test_backup"
# Optional object key prefix for the backup URL ("prefix" parameter).  When set, all backup
# objects land under this key prefix in the bucket instead of the bucket root.  Exercised by
# the blob_backup_restore_prefix_tests ctest variant.  The value is normalized (leading and
# trailing slashes stripped) the same way the container normalizes it, and a per-run unique
# suffix is appended so concurrent runs against the same real bucket cannot delete each
# other's objects during cleanup.
_raw_key_prefix="${BACKUP_URL_KEY_PREFIX:-}"
while [[ "${_raw_key_prefix}" == /* ]]; do _raw_key_prefix="${_raw_key_prefix#/}"; done
while [[ "${_raw_key_prefix}" == */ ]]; do _raw_key_prefix="${_raw_key_prefix%/}"; done
if [[ -n "${_raw_key_prefix}" ]]; then
  # High-entropy per-run id: PIDs alone repeat across hosts/containers sharing one bucket.
  _run_nonce="$(date -u +%Y%m%dt%H%M%S)-$$-$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  _raw_key_prefix="${_raw_key_prefix}/${_run_nonce}"
  unset _run_nonce
fi
readonly BACKUP_URL_KEY_PREFIX="${_raw_key_prefix}"
unset _raw_key_prefix

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
  # Per-run unique name so concurrently running ctest variants cannot clobber each
  # other's key while a restore still needs it.
  ENCRYPTION_KEY_FILE="${scratch_dir}/test_encryption_key_file.$$"
  create_encryption_key_file "${ENCRYPTION_KEY_FILE}"
  log "Created encryption key file at ${ENCRYPTION_KEY_FILE}"
else
  log "Using plaintext for backups"
fi
readonly ENCRYPTION_KEY_FILE
readonly USE_PARTITIONED_LOG
readonly USE_ENCRYPTION_BLOCK_SIZE

# Setup S3/MockS3 environment using common function
readonly temp_dir_prefix="mocks3_backup_test"
readonly url_path_prefix="ctests"
setup_s3_environment "${build_dir}" "${scratch_dir}" "${temp_dir_prefix}"

# Startup fdb cluster and backup agent
setup_fdb_cluster_with_backup "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 1

# Run tests.  The prefixed variant uses a per-run container name so its bucket-root
# negative checks cannot collide with containers from other runs against the same real bucket.
test="test_s3_backup_and_restore"
if [[ -n "${BACKUP_URL_KEY_PREFIX}" ]]; then
  test="test_s3_backup_and_restore_prefixed_${BACKUP_URL_KEY_PREFIX##*/}"
fi
url="blobstore://${host}/${url_path_prefix}/${test}?${query_str}"
if [[ -n "${BACKUP_URL_KEY_PREFIX}" ]]; then
  log "Using backup URL key prefix: ${BACKUP_URL_KEY_PREFIX}"
  url="${url}&prefix=${BACKUP_URL_KEY_PREFIX}"
fi
test_s3_backup_and_restore "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_s3_backup_and_restore"
