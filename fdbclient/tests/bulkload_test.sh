#!/bin/bash
#
# Test bulkload via s3 where we use seaweedfs
# (https://github.com/seaweedfs/seaweedfs) as substitute for
# AWS S3.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up a seaweedfs instance. We then run
# a bulkdump to 'S3' and then a restore. We verify
# the restore is the same as the original.
#
# Debugging, run this script w/ the -x flag: e.g. bash -x bulkdump_test.sh...
# You can also disable the cleanup. This will leave processes up
# so you can manually rerun commands or peruse logs and data
# under SCRATCH_DIR.

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
S3_RESOURCE="bulkdump-$(date -Iseconds | sed -e 's/[[:punct:]]/-/g')"
readonly S3_RESOURCE

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

# Run the bulkdump command.
# $1 The build directory
# $2 The scratch directory
# $3 The weed port s3 is listening on.
function bulkdump {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local weed_s3_port="${3}"
  # Bulkdump to s3. Set bulkdump mode to on
  # Then start a bulkdump and wait till its done.
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkdump mode on"
  then
    err "Bulkdump mode on failed"
    return 1
  fi
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkdump blobstore \"\" \xff \"blobstore://localhost:${weed_s3_port}/${S3_RESOURCE}?bucket=${BUCKET}&secure_connection=0&region=us\""
  then
    err "Bulkdump start failed"
    return 1
  fi
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkdump status \"\" \xff " )
    then
      err "Bulkdump status 1 failed"
      return 1
    fi
    if ! echo "${output}" | grep "Running bulk dumping job"; then
      break
    fi
    sleep 5
  done
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkdump status \"\" \xff " )
    then
      err "Bulkdump status 2 failed"
      return 1
    fi
    if echo "${output}" | grep "No bulk dumping job is running"; then
      return 0
    fi
    sleep 5
  done
  # Verify the job-manifest.txt made it into the bulkdump.
  if ! curl -s "http://localhost:${weed_s3_port}/${BUCKET}" | grep job-manifest.txt> /dev/null; then
    echo "ERROR: Failed to curl job-manifest.txt" >&2
    return 1
  fi
}

# Run a basic bulkdump to s3 and then after a bulkload.
# $1 build directory
# $2 the scratch directory
# $3 the s3_port to go against.
function test_basic_bulkdump_and_bulkload {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local local_s3_port="${3}"
  log "Load data"
  if ! load_data "${local_build_dir}" "${scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run bulkdump"
  if ! bulkdump "${local_build_dir}" "${scratch_dir}" "${local_s3_port}"; then
    err "Failed bulkdump"
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
readonly cwd

# Source in the fdb cluster, tests_common, and seaweedfs fixtures.
# shellcheck source=/dev/null
if ! source "${cwd}/seaweedfs_fixture.sh"; then
  err "Failed to source seaweedfs_fixture.sh"
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/tests_common.sh"; then
  err "Failed to source tests_common.sh"
  exit 1
fi

# Process command-line options.
if (( $# < 2 )) || (( $# > 3 )); then
    echo "ERROR: ${0} requires the fdb src and build directories --"
    echo "CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR -- and then, optionally,"
    echo "a directory into which we write scratch test data and logs"
    echo "\(otherwise we will write to subdirs under $TMPDIR\). We will"
    echo "leave the download of seaweed in this directory for other"
    echo "tests to find if they need it (if we need to download it)."
    echo "Otherwise, we clean everything else up on our way out."
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
if ! tmpdir=$(mktemp -p "${base_scratch_dir}" --directory -t bulkload.XXXX); then
  err "Failed mktemp"
  exit 1
fi
SCRATCH_DIR=$(resolve_to_absolute_path "${tmpdir}")
readonly SCRATCH_DIR

# Startup fdb cluster.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${SCRATCH_DIR}"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"

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
test_basic_bulkdump_and_bulkload "${build_dir}" "${SCRATCH_DIR}" "${s3_port}"
log_test_result $? "test_basic_bulkdump_and_bulkload"
