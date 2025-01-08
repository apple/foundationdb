#!/bin/bash
#
# Test backup and restore from s3 where we use seaweedfs
# (https://github.com/seaweedfs/seaweedfs) as substitute for
# AWS S3.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up a seaweedfs instance. We then run
# a backup to 'S3' and then a restores. We verify
# the restore is the same as the original.
#
# Debugging, run this script w/ the -x flag: e.g. bash -x s3_backup_test.sh...
# You can also disable the cleanup. This will leave processes up
# so you can manually rerun commands or peruse logs and data
# under SCRATCH_DIR.
#
# See https://apple.github.io/foundationdb/backups.html

# set -o xtrace   # a.k.a set -x  # Set this one when debugging (or 'bash -x THIS_SCRIPT').
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Globals that get set below and are used when we cleanup.
SCRATCH_DIR=
# Use one bucket only for all tests. More buckets means
# we need more seaweed volumes which can be an issue when
# little diskspace
readonly BUCKET="${S3_BUCKET:-testbucket}"
readonly TAG="test_backup"
S3_RESOURCE="backup-$(date -Iseconds | sed -e 's/[[:punct:]]/-/g')"
readonly S3_RESOURCE
readonly HTTP_VERBOSE_LEVEL=2
# Clear these environment variables. fdbbackup goes looking for them
# and if EITHER is set, it will go via a proxy instead of to where we.
# want it to go.
unset HTTP_PROXY
unset HTTPS_PROXY

# Install signal traps. Depends on globals being set.
# Calls the cleanup function.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
function cleanup {
  shutdown_weed
  shutdown_fdb_cluster
  if [[ -d "${SCRATCH_DIR}" ]]; then
    rm -rf "${SCRATCH_DIR}"
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

# Run the fdbbackup command.
# $1 The build directory
# $2 The scratch directory
# $3 The weed port s3 is listening on.
function backup {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local weed_s3_port="${3}"
  # Backup to s3. Without the -k argument in the below, the backup gets
  # 'No restore target version given, will use maximum restorable version from backup description.'
  # TODO: Why is -k needed?
  if ! "${local_build_dir}"/bin/fdbbackup start \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    -t "${TAG}" -w \
    -d "blobstore://localhost:${weed_s3_port}/${S3_RESOURCE}?bucket=${BUCKET}&secure_connection=0&region=us" \
    -k '"" \xff' \
    --log --logdir="${scratch_dir}" \
    --knob_http_verbose_level="${HTTP_VERBOSE_LEVEL}" \
    --knob_http_request_aws_v4_header=true
  then
    err "Start fdbbackup failed"
    return 1
  fi
}

# Run the fdbrestore command.
# $1 The build directory
# $2 The scratch directory
# $3 The weed port s3 is listening on.
function restore {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local weed_s3_port="${3}"
  if ! "${local_build_dir}"/bin/fdbrestore start \
    --dest-cluster-file "${scratch_dir}/loopback_cluster/fdb.cluster" \
    -t "${TAG}" -w \
    -r "blobstore://localhost:${weed_s3_port}/${S3_RESOURCE}?bucket=${BUCKET}&secure_connection=0&region=us" \
    --log --logdir="${scratch_dir}" \
    --knob_http_verbose_level="${HTTP_VERBOSE_LEVEL}" -t first_backup  \
    --knob_http_request_aws_v4_header=true
  then
    err "Start fdbrestore failed"
    return 1
  fi
}

# Run a backup to s3 and then a restore.
# $1 build directory
# $2 the scratch directory
# $3 the s3_port to go against.
function test_s3_backup_and_restore {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local local_s3_port="${3}"
  log "Load data"
  if ! load_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run s3 backup"
  if ! backup "${local_build_dir}" "${scratch_dir}" "${local_s3_port}"; then
    err "Failed backup"
    return 1
  fi
  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  log "Restore from s3"
  if ! restore "${local_build_dir}" "${scratch_dir}" "${local_s3_port}"; then
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

# Log pass or fail.
# $1 Test errcode
# $2 Test name
function log_test_result {
  local test_errcode=$1
  local test_name=$2
  if (( "${test_errcode}" == 0 )); then
    log "PASSED ${test_name}"
  else
    log "FAILED ${test_name}"
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
readonly cwd
# Source in the fdb cluster, backup common, and seaweedfs fixtures.
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/seaweedfs_fixture.sh"; then
  err "Failed to source seaweedfs_fixture.sh"
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
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
    echo "Example: ${0} ./foundationdb ./build_output ./scratch_dir"
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
# Set up scratch directory global.
base_scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  base_scratch_dir="${3}"
fi
readonly base_scratch_dir
# mktemp works differently on mac than on unix; the XXXX's are ignored on mac.
if ! tmpdir=$(mktemp -p "${base_scratch_dir}" --directory -t s3backup.XXXX); then
  err "Failed mktemp"
  exit 1
fi
SCRATCH_DIR=$(resolve_to_absolute_path "${tmpdir}")
readonly SCRATCH_DIR

# Startup fdb cluster and backup agent.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${SCRATCH_DIR}"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"
if ! start_backup_agent "${build_dir}" "${SCRATCH_DIR}"; then
  err "Failed start backup_agent"
  exit 1
fi
log "Backup_agent is up"

# Download seaweed.
# Download to base_scratch_dir so its there for the next test.
log "Fetching seaweedfs..."
if ! weed_binary_path="$(download_weed "${base_scratch_dir}")"; then
  err "Failed download of weed binary."
  exit 1
fi
readonly weed_binary_path
if ! weed_dir=$( create_weed_dir "${SCRATCH_DIR}" ); then
  err "Failed to create the weed dir."
  exit 1
fi
if ! s3_port=$(start_weed "${weed_binary_path}" "${weed_dir}" ); then
  err "failed start of weed server."
  exit 1
fi
readonly s3_port
log "Seaweed server is up; s3.port=${s3_port}"

# Run tests.
test_s3_backup_and_restore "${build_dir}" "${SCRATCH_DIR}" "${s3_port}"
log_test_result $? "test_s3_backup_and_restore"
