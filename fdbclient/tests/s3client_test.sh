#!/bin/bash
#
# Run s3client against s3 if available or else against a seaweed instance.
# Seaweed server takes about 25 seconds to come up. Tests run for a few seconds after that.
#

# Globals

# TEST_SCRATCH_DIR gets set below. Tests should be their data in here.
# It gets cleaned up on the way out of the test.
TEST_SCRATCH_DIR=

# Some copied from down the page on
# https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
# set -o xtrace   # a.k.a set -x
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# Make sure cleanup on script exit.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
function cleanup {
  if type shutdown_weed &> /dev/null; then
    shutdown_weed "${TEST_SCRATCH_DIR}"
  fi
  if type shutdown_aws &> /dev/null; then
    : #shutdown_aws "${TEST_SCRATCH_DIR}"
  fi
}

# Resolve passed in reference to an absolute path.
# e.g. /tmp on mac is actually /private/tmp.
# $1 path to resolve
function resolve_to_absolute_path {
  local p="${1}"
  local dir
  while [[ -h "${p}" ]]; do
    dir=$( cd -P "$( dirname "${p}" )" >/dev/null 2>&1 && pwd )
    p=$(readlink "${p}")
    [[ ${p} != /* ]] && p="${dir}/${p}"
  done
  realpath "${p}"
}

# Test file upload and download
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_file_upload_and_download {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi
  local testfileup="${dir}/testfile.up"
  local testfiledown="${dir}/testfile.down"
  date -Iseconds &> "${testfileup}"
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      rm "${url}"; then
    err "Failed rm of ${url}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      cp "${testfileup}" "${url}"; then
    err "Failed cp of ${testfileup} to ${url}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      cp "${url}" "${testfiledown}"; then
    err "Failed cp ${url} ${testfiledown}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      rm "${url}"; then
    err "Failed rm ${url}"
    return 1
  fi
  cat "${testfileup}"
  cat "${testfiledown}"
  if ! diff "${testfileup}" "${testfiledown}"; then
    err "ERROR: Test $0 failed; upload and download are not the same." >&2
    return 1
  fi
}

# Test dir upload and download
# $1 The url to go against
# $2 Directory I can write test file in.
# $3 credentials file
# $4 The s3client binary.
function test_dir_upload_and_download {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${2}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi
  local testdirup="${dir}/testdir.up"
  local testdirdown="${dir}/testdir.down"
  mkdir "${testdirup}" "${testdirdown}"
  date -Iseconds &> "${testdirup}/one"
  date -Iseconds &> "${testdirup}/two"
  mkdir "${testdirup}/subdir"
  date -Iseconds  &> "${testdirup}/subdir/three"
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      rm "${url}"; then
    err "Failed rm ${url}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      cp "${testdirup}" "${url}"; then
    err "Failed cp ${testdirup}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      cp "${url}" "${testdirdown}"; then
    err "Failed cp ${url}"
    return 1
  fi
  if ! "${s3client}" \
      --knob_http_verbose_level=10 \
      --knob_blobstore_encryption_type=aws:kms \
      --tls-ca-file /etc/ssl/cert.pem \
      --tls-certificate-file "${dir}/cert.pem" \
      --tls-key-file "${dir}/key.pem" \
      --blob-credentials "${credentials}" \
      --log --logdir "${logsdir}" \
      rm "${url}"; then
    err "Failed rm ${url}"
    return 1
  fi
  if ! diff "${testdirup}" "${testdirdown}"; then
    err "ERROR: Test $0 failed; upload and download are not the same." >&2
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

# shellcheck source=/dev/null
if ! source "${cwd}/tests_common.sh"; then
  err "Failed to source tests_common.sh"
  exit 1
fi
# Process command-line options.
if (( $# < 1 )) || (( $# > 2 )); then
    echo "ERROR: ${0} requires the fdb build directory -- CMAKE_BUILD_DIR -- as its"
    echo "first argument and then, optionally, a directory into which we write scratch"
    echo "test data and logs (otherwise we'll write to subdirs under $TMPDIR)."
    echo "Example: ${0} ./build_output ./scratch_dir"
    exit 1
fi
readonly build_dir="${1}"
if [[ ! -d "${build_dir}" ]]; then
  err "${build_dir} is not a directory" >&2
  exit 1
fi
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 2 )); then
  scratch_dir="${2}"
fi

# Set host, bucket, and blob_credentials_file whether seaweed or s3.
host=
query_str=
blob_credentials_file=
path_prefix=
if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then
  # Now source in the aws fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/aws_fixture.sh"; then
    err "Failed to source aws_fixture.sh"
    exit 1
  fi
  if ! TEST_SCRATCH_DIR=$( create_aws_dir "${scratch_dir}" ); then
    err "Failed creating local aws_dir"
    exit 1
  fi
  readonly TEST_SCRATCH_DIR
  # Get a pem and cert file for TLS to use (Connection needs to be secure when
  # going to S3 w/ blobstore_encryption_type=aws:kms. For seaweed we do an
  # insecure connnection (The TLS args are ignored) just because it is a little
  # awkward running TLS seaweedfs server. Downloading rather than
  # generating pem and cert for now because running openssl generation fails in our dev env
  # with 'DSO support routines:DSO_load:could not load the shared library:crypto/dso/dso_lib.c:162:'
  curl https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/refs/heads/main/config/test-certs/cert.pem \
      -o "${TEST_SCRATCH_DIR}/cert.pem"
  curl https://raw.githubusercontent.com/FoundationDB/fdb-kubernetes-operator/refs/heads/main/config/test-certs/key.pem \
      -o "${TEST_SCRATCH_DIR}/key.pem"
  # Fetch token, region, etc. from our aws environment.
  if ! imdsv2_token=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"); then
    err "Failed reading token"
    exit 1
  fi
  readonly imdsv2_token
  if ! region=$( curl -H "X-aws-ec2-metadata-token: ${imdsv2_token}" \
      "http://169.254.169.254/latest/meta-data/placement/region"); then
    err "Failed reading region"
    exit 1
  fi
  readonly region
  if ! account_id=$( aws --output text sts get-caller-identity --query 'Account' ); then
    err "Failed reading account id"
    exit 1
  fi
  readonly account_id
  readonly bucket="backup-${account_id}-${region}"
  # Add the '@' in front so we force reading of credentials file when s3
  readonly host="@${bucket}.s3.amazonaws.com"
  if ! blob_credentials_file=$(write_blob_credentials "${imdsv2_token}" "${host}" "${TEST_SCRATCH_DIR}"); then
    err "Failed to write credentials file"
    exit 1
  fi
  readonly blob_credentials_file
  # bulkload is where we can write to in apple dev
  readonly path_prefix="bulkload/test"
  query_str="bucket=${bucket}&region=${region}"
else
  # Now source in the seaweedfs fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/seaweedfs_fixture.sh"; then
    err "Failed to source seaweedfs_fixture.sh"
    exit 1
  fi
  # Download seaweed.
  if ! weed_binary_path="$(download_weed "${scratch_dir}")"; then
    err "failed download of weed binary." >&2
    exit 1
  fi
  readonly weed_binary_path
  # Here we create a tmpdir to hold seaweed logs and data in global WEED_DIR hosted
  # by seaweedfs_fixture.sh which we sourced above. Call shutdown_weed to clean up.
  if ! TEST_SCRATCH_DIR=$( create_weed_dir "${scratch_dir}" ); then
    err "failed create of the weed dir." >&2
    exit 1
  fi
  readonly TEST_SCRATCH_DIR
  log "Starting seaweed..."
  if ! s3_port=$(start_weed "${weed_binary_path}" "${TEST_SCRATCH_DIR}"); then
    err "failed start of weed server." >&2
    exit 1
  fi
  readonly host="localhost:${s3_port}"
  readonly bucket="${SEAWEED_BUCKET}"
  readonly region="us"
  # Reference a non-existent blob file (its ignored by seaweed)
  readonly blob_credentials_file="${TEST_SCRATCH_DIR}/blob_credentials.json"
  # Let the connection to seaweed be insecure -- not-TLS -- because just awkward to set up.
  query_str="bucket=${bucket}&region=${region}&secure_connection=0"
fi

# Run tests.
test="test_file_upload_and_download"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_file_upload_and_download "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
log_test_result $? "${test}"

test="test_dir_upload_and_download"
url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
test_dir_upload_and_download "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
log_test_result $? "${test}"
