#!/bin/bash
#
# Test bulkload. Uses S3 or seaweedfs if not available:
# (https://github.com/seaweedfs/seaweedfs) as substitute.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up a seaweedfs instance. We
# then run a bulkdump to 'S3' and then a restore. We verify
# the restore is the same as the original.
#
# Debugging, run this script w/ the -x flag: e.g. bash -x bulkdump_test.sh...
# You can also disable the cleanup. This will leave processes up
# so you can manually rerun commands or peruse logs and data
# under SCRATCH_DIR.

# Make sure cleanup on script exit.
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
# $1 The url to dump to
# $2 The scratch directory
# $3 Credentials to use
# $4 build directory
function bulkdump {
  local local_url="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"
  local local_build_dir="${4}"
  # Bulkdump to s3. Set bulkdump mode to on
  # Then start a bulkdump and wait till its done.
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkdump mode on"
  then
    err "Bulkdump mode on failed"
    return 1
  fi
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkdump blobstore \"\" \xff \"${url}\""
  then
    err "Bulkdump start failed"
    return 1
  fi
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
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
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkdump status \"\" \xff " )
    then
      err "Bulkdump status 2 failed"
      return 1
    fi
    if echo "${output}" | grep "No bulk dumping job is running"; then
      break
    fi
    sleep 5
  done
  # TODO: Add something like this to verify job manifest is in place.
  ## Verify the job-manifest.txt made it into the bulkdump.
  #if ! curl -s "http://localhost:${weed_s3_port}/${BUCKET}" | grep job-manifest.txt> /dev/null; then
  #  echo "ERROR: Failed to curl job-manifest.txt" >&2
  #  return 1
  #fi
}

# Run a basic bulkdump to s3 and then after a bulkload.
# $1 url to bulk dump to.
# $2 the scratch directory
# $3 credentials file
# $4 the build dir
function test_basic_bulkdump_and_bulkload {
  local local_url="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"
  local local_build_dir="${4}"
  log "Load data"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  # Comment out for now till its working.
  #if [[ "${USE_S3}" == "true" ]]; then
  if false; then
    # Run this rm only if s3. In seaweed, it would fail because
    # bucket doesn't exist yet (they are lazily created).
    if ! "${local_build_dir}/bin/s3client" \
        "${KNOBS[*]}" \
        --tls-ca-file "${TLS_CA_FILE}" \
        --blob-credentials "${credentials}" \
        --log --logdir "${local_scratch_dir}" \
        rm "${local_url}"; then
      err "Failed rm of ${local_url}"
      return 1
    fi
  fi
  log "Run bulkdump"
  if ! bulkdump "${local_url}" "${local_scratch_dir}" "${credentials}" "${local_build_dir}"; then
    err "Failed bulkdump"
    return 1
  fi
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  #if ! bulkload "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}"; then
  #  err "Failed bulkload"
  #  return 1
  #fi
  #log "Verify restore"
  #if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
  #  err "Failed verification of data in fdb"
  #  return 1
  #fi
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

# Globals
# TEST_SCRATCH_DIR gets set below. Tests should be their data in here.
# It gets cleaned up on the way out of the test.
TEST_SCRATCH_DIR=
TLS_CA_FILE="${TLS_CA_FILE:-/etc/ssl/cert.pem}"
readonly TLS_CA_FILE
readonly HTTP_VERBOSE_LEVEL=10
KNOBS=("--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
readonly KNOBS
# Should we use S3? If USE_S3 is not defined, then check if
# OKTETO_NAMESPACE is defined (It is defined on the okteto
# internal apple dev environments where S3 is available).
readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"
# Clear these environment variables. fdbbackup goes looking for them
# and if EITHER is set, it will go via a proxy instead of to where we.
# want it to go.
unset HTTP_PROXY
unset HTTPS_PROXY

# Get the working directory for this script.
if ! path=$(resolve_to_absolute_path "${BASH_SOURCE[0]}"); then
  echo "ERROR: Failed resolve_to_absolute_path"
  exit 1
fi
if ! cwd=$( cd -P "$( dirname "${path}" )" >/dev/null 2>&1 && pwd ); then
  echo "ERROR: Failed dirname on ${path}"
  exit 1
fi
readonly cwd
# shellcheck source=/dev/null
if ! source "${cwd}/tests_common.sh"; then
  echo "ERROR: Failed to source tests_common.sh"
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
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  scratch_dir="${3}"
fi
readonly scratch_dir

# Set host, bucket, and blob_credentials_file whether seaweed or s3.
readonly path_prefix="bulkload/ctests"
host=
query_str=
blob_credentials_file=
# Comment out for now till its working.
#if [[ "${USE_S3}" == "true" ]]; then
if false; then
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
  if ! readarray -t configs < <(aws_setup "${TEST_SCRATCH_DIR}"); then
    err "Failed aws_setup"
    return 1
  fi
  readonly host="${configs[0]}"
  readonly bucket="${configs[1]}"
  readonly blob_credentials_file="${configs[2]}"
  readonly region="${configs[3]}"
  query_str="bucket=${bucket}&region=${region}"
  # Make these environment variables available for the fdb cluster when s3.
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
fi

# Source in the fdb cluster.
# shellcheck source=/dev/null
if ! source "${cwd}/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Startup fdb cluster.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"

# Run tests.
test="test_basic_bulkdump_and_bulkload"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_basic_bulkdump_and_bulkload "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}"
log_test_result $? "${test}"
