#!/usr/bin/env bash
#
# Test bulkload. Uses S3 or MockS3Server if not available.
#
# In the below we start a small FDB cluster, populate it with
# some data and then start up MockS3Server. We
# then run a bulkdump to 'S3' and then a restore. We verify
# the restore is the same as the original.
#
# Debugging:
#   - Run with -x flag: bash -x bulkload_test.sh...
#   - Preserve test data: PRESERVE_TEST_DATA=1 ./bulkload_test.sh ...
#     This will leave all test data including MockS3 persistence files
#     in the test scratch directory for analysis after the test completes.

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
    --exec "bulkdump dump \"\" \xff \"${url}\"" > /dev/null
  then
    err "Bulkdump start failed"
    return 1
  fi
  local output=
  local jobid=
  # Now wait until the status is NOT "Running bulk dumping job"
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkdump status" )
    then
      err "Bulkdump status 1 failed"
      return 1
    fi
    if ! echo "${output}" | grep "Running bulk dumping job:" > /dev/null; then
      break
    elif [[ -z "${jobid}" ]]; then
      if line=$(echo "${output}" | grep "Running bulk dumping job:"); then
        jobid=$(echo "${line}" | sed -e 's/.*Running bulk dumping job://' | xargs)
      fi
    fi
    sleep 5
  done
  # Wait until status is 'No bulk dumping job is running'.
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkdump status" )
    then
      err "Bulkdump status 2 failed"
      return 1
    fi
    if echo "${output}" | grep "No bulk dumping job is running" &> /dev/null; then
      break
    fi
    sleep 5
  done
  echo "${jobid}"
  # TODO: Add something like this to verify job manifest is in place.
  ## Verify the job-manifest.txt made it into the bulkdump.
  #if ! curl -s "http://localhost:${weed_s3_port}/${BUCKET}" | grep job-manifest.txt> /dev/null; then
  #  echo "ERROR: Failed to curl job-manifest.txt" >&2
  #  return 1
  #fi
}

# Run the bulkload command.
# $1 The url to load from
# $2 The scratch directory
# $3 Credentials to use
# $4 build directory
function bulkload {
  local local_url="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"
  local local_build_dir="${4}"
  local jobid="${5}"
  # Bulklaod from s3. Set bulkload mode to on
  # Then start a bulkload and wait till its done.
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkload mode on"
  then
    err "Bulkload mode on failed"
    return 1
  fi
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkload addlockowner BulkLoad"
  then
    err "Bulkload add BulkLoad lockower failed"
    return 1
  fi
  if ! "${local_build_dir}"/bin/fdbcli \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "bulkload load ${jobid} \"\" \xff \"${url}\""
  then
    err "Bulkload start failed"
    return 1
  fi
  local output
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkload status" )
    then
      err "Bulkload status 1 failed"
      return 1
    fi
    if ! echo "${output}" | grep "Running bulk loading job:" &> /dev/null; then
      break
    fi
    sleep 5
  done
  while true; do
    if ! output=$( "${local_build_dir}"/bin/fdbcli \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "bulkload status" )
    then
      err "Bulkload status 2 failed"
      return 1
    fi
    if echo "${output}" | grep "No bulk loading job is running" &> /dev/null; then
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
  if [[ "${USE_S3}" == "true" ]]; then
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
  if ! jobid=$(bulkdump "${local_url}" "${local_scratch_dir}" "${credentials}" "${local_build_dir}"); then
    err "Failed bulkdump"
    return 1
  fi
  log "Clear data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  log "Bulkload"
  if ! bulkload "${local_url}" "${local_scratch_dir}" "${credentials}" "${local_build_dir}" "${jobid}"; then
    err "Failed bulkload"
    return 1
  fi
  log "Verify restore"
  if ! verify_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed verification of data in fdb"
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

# Globals
# TEST_SCRATCH_DIR gets set below by setup_s3_environment.
TEST_SCRATCH_DIR=
readonly HTTP_VERBOSE_LEVEL=2

# Setup USE_S3 and TLS_CA_FILE using common functions
readonly USE_S3="$(get_use_s3_default)"
setup_tls_ca_file

# Set KNOBS based on whether we're using real S3 or MockS3Server
if [[ "${USE_S3}" == "true" ]]; then
  # Use AWS KMS encryption for real S3
  KNOBS=("--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
else
  # No encryption for MockS3Server
  KNOBS=("--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
fi
readonly KNOBS

# Clear proxy environment variables
unset HTTP_PROXY
unset HTTPS_PROXY

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

# Setup S3/MockS3 environment using common function
# temp_dir_prefix is used for naming the scratch directory
# url_path_prefix is used in the blobstore URL path
readonly temp_dir_prefix="mocks3_bulkload_test"
readonly url_path_prefix="bulkload/ctests"
setup_s3_environment "${build_dir}" "${scratch_dir}" "${temp_dir_prefix}"

# Source in the fdb cluster.
# shellcheck source=/dev/null
if ! source "${cwd}/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Startup fdb cluster.
# Start up 9 SSs because bulk load tries to avoid loading back on to the team it dumped from.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 9 "${KNOBS[@]}"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"

# Run tests.
test="test_basic_bulkdump_and_bulkload"
url="blobstore://${host}/${url_path_prefix}/${test}?${query_str}"
test_basic_bulkdump_and_bulkload "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}"
log_test_result $? "${test}"
