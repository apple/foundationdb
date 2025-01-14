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
    : # shutdown_aws "${TEST_SCRATCH_DIR}"
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
# $3 The S3 url.
# $4 credentials file
function backup {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_credentials="${4}"
  # Backup to s3. Without the -k argument in the below, the backup gets
  # 'No restore target version given, will use maximum restorable version from backup description.'
  # TODO: Why is -k needed?
  if ! "${local_build_dir}"/bin/fdbbackup start \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    -t "${TAG}" -w \
    -d "${local_url}" \
    -k '"" \xff' \
    --log --logdir="${local_scratch_dir}" \
    --blob-credentials "${local_credentials}" \
    "${KNOBS[*]}"
  then
    err "Start fdbbackup failed"
    return 1
  fi
}

# Run the fdbrestore command.
# $1 The build directory
# $2 The scratch directory
# $3 The S3 url
# $4 credentials file
function restore {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_credentials="${4}"
  if ! "${local_build_dir}"/bin/fdbrestore start \
    --dest-cluster-file "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    -t "${TAG}" -w \
    -r "${url}" \
    --log --logdir="${local_scratch_dir}" \
    --blob-credentials "${local_credentials}" \
    "${KNOBS[*]}"
  then
    err "Start fdbrestore failed"
    return 1
  fi
}

# Run a backup to s3 and then a restore.
# $1 The url to use
# $2 the scratch directory
# $3 The credentials file.
# $4 build directory
function test_s3_backup_and_restore {
  local local_url="${1}"
  local local_scratch_dir="${2}"
  local credentials="${3}"
  local local_build_dir="${4}"
  log "Load data"
  if ! load_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed loading data into fdb"
    return 1
  fi
  log "Run s3 backup"
  if ! backup "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}"; then
    err "Failed backup"
    return 1
  fi
  log "Clear fdb data"
  if ! clear_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "Failed clear data in fdb"
    return 1
  fi
  log "Restore from s3"
  if ! restore "${local_build_dir}" "${local_scratch_dir}" "${local_url}" "${credentials}"; then
    err "Failed restore"
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

# TEST_SCRATCH_DIR gets set below. Tests should be their data in here.
# It gets cleaned up on the way out of the test.
TEST_SCRATCH_DIR=
TLS_CA_FILE="${TLS_CA_FILE:-/etc/ssl/cert.pem}"
readonly TLS_CA_FILE
readonly HTTP_VERBOSE_LEVEL=10
KNOBS=("--knob_http_request_aws_v4_header=true" "--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
readonly KNOBS
readonly TAG="test_backup"
# Should we use S3? If USE_S3 is not defined, then check if
# OKTETO_NAMESPACE is defined (It is defined on the okteto
# internal apple dev environments where S3 is available).
readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"
S3_RESOURCE="backup-$(date -Iseconds | sed -e 's/[[:punct:]]/-/g')"
readonly S3_RESOURCE
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
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  scratch_dir="${3}"
fi
readonly scratch_dir

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
  if ! readarray -t configs < <(aws_setup "${TEST_SCRATCH_DIR}"); then
    err "Failed aws_setup"
    return 1
  fi
  readonly host="${configs[0]}"
  readonly bucket="${configs[1]}"
  readonly blob_credentials_file="${configs[2]}"
  readonly region="${configs[3]}"
  query_str="bucket=${bucket}&region=${region}"
  # Set available when the fdb cluster and the backup_agent starts.
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

# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
# Startup fdb cluster and backup agent.
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}"; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"
if ! start_backup_agent "${build_dir}" "${TEST_SCRATCH_DIR}" "${KNOBS[*]}"; then
  err "Failed start backup_agent"
  exit 1
fi
log "Backup_agent is up"

# Run tests.
test="test_s3_backup_and_restore"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_s3_backup_and_restore "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}"
log_test_result $? "test_s3_backup_and_restore"
