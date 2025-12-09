#!/usr/bin/env bash
#
# Test backup and restore from a local directory.
#
# In the below we start a small FDB cluster and populate it with
# some data. We then run a backup, a clear, and  a restores. We
# verify the restore is the same as the original.
#
# See https://apple.github.io/foundationdb/backups.html

# set -o xtrace   # a.k.a set -x  # Set this one when debugging (or 'bash -x THIS_SCRIPT').
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Globals that get set below and are used when we cleanup.
SCRATCH_DIR=
readonly TAG="test_backup"
USE_PARTITIONED_LOG=$(((RANDOM % 2)) && echo true || echo false )
USE_ENCRYPTION=$(((RANDOM % 2)) && echo true || echo false )
ENCRYPTION_KEY_FILE=

# Install signal traps. Calls the cleanup function.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
# Has a hard 30-second timeout to prevent CTest timeouts.
function cleanup {
  echo "$(date -Iseconds) cleanup: starting (with 30s hard timeout)"
  
  # Start a watchdog that will force-kill us if cleanup takes too long
  local my_pid=$$
  (
    sleep 30
    echo "$(date -Iseconds) CLEANUP TIMEOUT after 30s - forcing exit"
    kill -9 -$my_pid 2>/dev/null || kill -9 $my_pid 2>/dev/null
  ) &
  local watchdog_pid=$!
  disown $watchdog_pid 2>/dev/null || true
  
  echo "$(date -Iseconds) cleanup: shutting down FDB cluster"
  shutdown_fdb_cluster
  
  if [[ -d "${SCRATCH_DIR}" ]]; then
    echo "$(date -Iseconds) cleanup: removing scratch dir: ${SCRATCH_DIR}"
    rm -rf "${SCRATCH_DIR}"
  fi
  
  # Clean up encryption key file
  if [[ -n "${ENCRYPTION_KEY_FILE:-}" ]] && [[ -f "${ENCRYPTION_KEY_FILE}" ]]; then
    echo "$(date -Iseconds) cleanup: removing encryption key file: ${ENCRYPTION_KEY_FILE}"
    rm -f "${ENCRYPTION_KEY_FILE}"
  fi
  
  echo "$(date -Iseconds) cleanup: complete"
  
  # Cancel the watchdog since we finished in time
  kill $watchdog_pid 2>/dev/null || true
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
# $1 The build directory so we can find bin/fdbbackup command.
# $2 The scratch directory where the fdb.cluster file can be found.
# $3 encryption key file (optional)
function backup {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local local_encryption_key_file="${3:-}"

  local cmd_args=(
    "-C" "${scratch_dir}/loopback_cluster/fdb.cluster"
    "-t" "${TAG}" "-w"
    "-d" "file://${scratch_dir}/backups"
    "--log" "--logdir=${scratch_dir}"
  )

  if [[ -n "${local_encryption_key_file}" ]]; then
    cmd_args+=("--encryption-key-file" "${local_encryption_key_file}")
  fi

  if [[ "${USE_PARTITIONED_LOG}" == "true" ]]; then
    cmd_args+=("--partitioned-log-experimental")
  fi

  if ! "${local_build_dir}"/bin/fdbbackup start "${cmd_args[@]}"; then
    err "Start fdbbackup failed"
    return 1
  fi
}

# Run the fdbrestore command.
# $1 The build directory
# $2 The scratch directory
# $3 encryption key file (optional)
function restore {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local local_encryption_key_file="${3:-}"

  # Find the most recent backup. See here for why:
  # https://forums.foundationdb.org/t/restoring-a-completed-backup-version-results-in-an-error/1845
  if ! backup=$(ls -dt "${scratch_dir}"/backups/backup-* | head -1 ); then
    err "Failed to list backups under ${scratch_dir}/backups/"
    return 1
  fi
  if ! backup_name=$(basename "${backup}"); then
    err "Failed to get basename"
    return 1
  fi

  local cmd_args=(
    "--dest-cluster-file" "${scratch_dir}/loopback_cluster/fdb.cluster"
    "-t" "${TAG}" "-w"
    "-r" "file://${scratch_dir}/backups/${backup_name}"
    "--log" "--logdir=${scratch_dir}"
  )

  if [[ -n "${local_encryption_key_file}" ]]; then
    cmd_args+=("--encryption-key-file" "${local_encryption_key_file}")
  fi

  if ! "${local_build_dir}"/bin/fdbrestore start "${cmd_args[@]}"; then
    err "Start fdbrestore failed"
    return 1
  fi
}

# Run a backup to the fs and then a restore.
# $1 build directory
# $2 the scratch directory
# $3 encryption key file (optional)
function test_dir_backup_and_restore {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local local_encryption_key_file="${3:-}"

  log "Load data"
  # Just do a few keys.
  if ! load_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run backup"
  if ! backup "${local_build_dir}" "${scratch_dir}" "${local_encryption_key_file}"; then
    err "Failed backup"
    return 1
  fi
  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  log "Restore"
  if ! restore "${local_build_dir}" "${scratch_dir}" "${local_encryption_key_file}"; then
    err "Failed restore"
    return 1
  fi
  log "Verify restore"
  if ! verify_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed verification of data in fdb"
    return 1
  fi
  log "Check for Severity=40 errors"
  if ! grep_for_severity40 "${scratch_dir}"; then
    err "Found Severity=40 errors in logs"
    return 1
  fi
}

# Get the working directory for this script.
if ! path=$(resolve_to_absolute_path "${BASH_SOURCE[0]}"); then
  err "Failed resolve_to_absolute_path"
  exit 1
fi
if ! cwd=$( cd -P "$( dirname "${path}" )" >/dev/null 2>&1 && pwd ); then
  err "Failed dirname on ${path}"
  exit 1
fi
# Source in the fdb cluster and tests_common fixtures.
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Set FDB_DATA_KEYCOUNT before sourcing backup_common.sh so we override default.
# So we read less keys.
export FDB_DATA_KEYCOUNT=10
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
    echo "(otherwise we will write to subdirs under $TMPDIR)."
    echo "Example: ${0} ./foundationdb ./build_output ./scratch_dir"
    exit 1
fi
if ! source_dir=$(is_fdb_source_dir "${1}"); then
  err "${source_dir} is not an fdb source directory"
  exit 1
fi
readonly sourcedir
readonly build_dir="${2}"
if [[ ! -d "${build_dir}" ]]; then
  err "${build_dir} is not a directory"
  exit 1
fi
# Set up scratch directory global.
base_scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  base_scratch_dir="${3}"
fi
# mktemp works differently on mac than on unix; the XXXX's are ignored on mac.
if ! tmpdir=$(mktemp -p "${base_scratch_dir}" --directory -t s3backup.XXXX); then
  err "Failed mktemp"
  exit 1
fi
SCRATCH_DIR=$(resolve_to_absolute_path "${tmpdir}")
readonly SCRATCH_DIR

# Create encryption key file if needed
if [[ "${USE_ENCRYPTION}" == "true" ]]; then
  ENCRYPTION_KEY_FILE="${SCRATCH_DIR}/test_encryption_key_file"
  create_encryption_key_file "${ENCRYPTION_KEY_FILE}"
fi

readonly USE_PARTITIONED_LOG
readonly USE_ENCRYPTION
readonly ENCRYPTION_KEY_FILE

# Startup fdb cluster and backup agent.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${SCRATCH_DIR}" 1; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"
if ! start_backup_agent "${build_dir}" "${SCRATCH_DIR}"; then
  err "Failed start backup_agent"
  exit 1
fi
log "Backup_agent is up"

# Run tests.
test_dir_backup_and_restore "${build_dir}" "${SCRATCH_DIR}" "${ENCRYPTION_KEY_FILE}"
log_test_result $? "test_dir_backup_and_restore"
