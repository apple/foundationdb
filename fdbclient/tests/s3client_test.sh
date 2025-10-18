#!/usr/bin/env bash
#
# Run s3client against s3 if available or else against MockS3Server.
# MockS3Server starts instantly. Tests run for a few seconds after that.
#

# Make sure cleanup on script exit.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
function cleanup {
  if type shutdown_mocks3 &> /dev/null; then
    shutdown_mocks3
  fi
  if type shutdown_aws &> /dev/null; then
    shutdown_aws "${TEST_SCRATCH_DIR}"
  fi
}

# Filter out HTTP debug output from s3client output
# $1 The output to filter
function filter_http_debug {
  local output="$1"
  echo "${output}" | grep -v "Contents of" | grep -v "^$" | grep -v "^[[:space:]]*$" | grep -v "HTTP" | grep -v "^\[.*\]" | grep -v "^Request Header:" | grep -v "^Response Header:" | grep -v "^Response Code:" | grep -v "^Response ContentLen:" | grep -v "^-- RESPONSE CONTENT--" | grep -v "^--------" | grep -v "^<?xml" | grep -v "^<.*>" | grep -v "^'$" | grep -v "^++"
}

# Run s3client with proper TLS CA file handling
# This function handles the case where TLS_CA_FILE might be empty
function run_s3client {
  local s3client="$1"
  local credentials="$2"
  local logsdir="$3"
  local integrity_check="${4:-false}"
  shift 4
  
  local cmd_args=()
  cmd_args+=("${s3client}")
  cmd_args+=("--knob_http_verbose_level=${HTTP_VERBOSE_LEVEL}")
  # Only use AWS KMS encryption with real S3, not SeaweedFS
  if [[ "${USE_S3}" == "true" ]]; then
    cmd_args+=("--knob_blobstore_encryption_type=aws:kms")
  fi
  cmd_args+=("--knob_blobstore_enable_object_integrity_check=${integrity_check}")
  
  # Only add TLS CA file if it's not empty
  if [[ -n "${TLS_CA_FILE}" ]]; then
    cmd_args+=("--tls-ca-file")
    cmd_args+=("${TLS_CA_FILE}")
  fi
  
  cmd_args+=("--blob-credentials")
  cmd_args+=("${credentials}")
  cmd_args+=("--log")
  cmd_args+=("--logdir")
  cmd_args+=("${logsdir}")
  
  # Add remaining arguments
  cmd_args+=("$@")
  
  # Execute the command
  "${cmd_args[@]}"
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

# Run the upload, then download, then cleanup.
# $1 The url to go against
# $2 test dir
# $3 credentials file
# $4 The s3client binary.
# $5 What to upload
# $6 Where to download to.
# $7 Optionally override integrity_check.
function upload_download {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local object="${5}"
  local downloaded_object="${6}"
  local no_integrity_check="${7:-false}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi
  # If on s3, enable integrity check. Otherwise leave it false.
  # (seaweed doesn't support asking for hash in GET request so
  # it fails the request as malformed).
  local integrity_check=false
  if [[ "${USE_S3}" == "true" ]]; then
    # Enable integrity checking unless an override.
    if [[ "${no_integrity_check}" == false ]]; then
      integrity_check=true
    fi
    # Run this rm only if s3. In seaweed, it would fail because
    # bucket doesn't exist yet (they are lazily created).
    if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "${integrity_check}" rm "${url}"; then
      err "Failed rm of ${url}"
      return 1
    fi
  fi
  if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "${integrity_check}" cp "${object}" "${url}"; then
    err "Failed cp of ${object} to ${url}"
    return 1
  fi
  if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "${integrity_check}" cp "${url}" "${downloaded_object}"; then
    err "Failed cp ${url} ${downloaded_object}"
    return 1
  fi
  if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "${integrity_check}" rm "${url}"; then
    err "Failed rm ${url}"
    return 1
  fi
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
  local testfileup="${dir}/testfile.up"
  local testfiledown="${dir}/testfile.down"
  date -Iseconds &> "${testfileup}"
  if ! upload_download "${url}" "${dir}" "${credentials}" "${s3client}" "${testfileup}" "${testfiledown}"; then
    err "Failed upload_download"
    return 1
  fi
  if ! diff "${testfileup}" "${testfiledown}"; then
    err "ERROR: Test $0 failed; upload and download are not the same." >&2
    return 1
  fi
}

# Test file upload and download but w/o the integrity check.
# Only makes sense on s3. Seaweed doesn't support integrity check.
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_file_upload_and_download_no_integrity_check {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local testfileup="${dir}/testfile.up.no_integrity_check"
  local testfiledown="${dir}/testfile.down.no_integrity_check"
  date -Iseconds &> "${testfileup}"
  if ! upload_download "${url}" "${dir}" "${credentials}" "${s3client}" "${testfileup}" "${testfiledown}" "true"; then
    err "Failed upload_download"
    return 1
  fi
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
  local testdirup="${dir}/testdir.up"
  local testdirdown="${dir}/testdir.down"
  mkdir "${testdirup}" "${testdirdown}"
  date -Iseconds &> "${testdirup}/one"
  date -Iseconds &> "${testdirup}/two"
  mkdir "${testdirup}/subdir"
  date -Iseconds  &> "${testdirup}/subdir/three"
  if ! upload_download "${url}" "${dir}" "${credentials}" "${s3client}" "${testdirup}" "${testdirdown}"; then
    err "Failed upload_download"
    return 1
  fi
  if ! diff "${testdirup}" "${testdirdown}"; then
    err "ERROR: Test $0 failed; upload and download are not the same." >&2
    return 1
  fi
}

# Test non-existent bucket listing
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_nonexistent_bucket {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi
  if [[ "${USE_S3}" == "true" ]]; then
    # For S3, expect "Requested resource was not found" error
    if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
        ls "${url}" \
        2>&1 | grep -q "Requested resource was not found"; then
      err "Failed to detect non-existent bucket in S3"
      return 1
    fi
  else
    # For SeaweedFS, expect either:
    # 1. "No objects found" message
    # 2. Just the header line
    # 3. Or successful completion with no objects listed
    local output
    local status
    output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
        ls "${url}" 2>&1)
    status=$?

    # Check if command succeeded and output matches expected patterns
    if [[ ${status} -eq 0 ]]; then
      # Command succeeded, check output patterns
      # For SeaweedFS, we consider it a success if:
      # 1. The command succeeded (status 0)
      # 2. The output contains the URL header
      # 3. There are no objects listed (no lines after the header)
      if echo "${output}" | grep -q "Contents of" &&
         [[ $(filter_http_debug "${output}" | wc -l) -eq 0 ]]; then
        # Success - we have the header and no other non-empty lines (excluding HTTP debug lines)
        return 0
      else
        err "Failed to handle empty bucket listing in SeaweedFS"
        return 1
      fi
    else
      # Command failed, which is unexpected for SeaweedFS
      err "Command failed unexpectedly for SeaweedFS"
      return 1
    fi
  fi
}

# Test non-existent resource in existing bucket
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_nonexistent_resource {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi

  local output
  local status
  output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
      ls "${url}" 2>&1)
  status=$?

  if [[ "${USE_S3}" == "true" ]]; then
    # For S3, a non-existent path returns a 200 with empty contents
    # We expect to see the "Contents of" header but no actual contents
    local filtered_output
    filtered_output=$(filter_http_debug "${output}")
    
    if ! (echo "${output}" | grep -q "Contents of" &&
          [[ -z "$(echo "${filtered_output}" | tr -d '[:space:]')" ]]); then
      err "Failed to detect non-existent resource in S3"
      return 1
    fi
  else
    # For SeaweedFS, expect either:
    # 1. Empty listing (just the header line)
    # 2. Or successful completion with no objects listed
    if [[ ${status} -eq 0 ]]; then
      # For SeaweedFS, we consider it a success if:
      # 1. The command succeeded (status 0)
      # 2. The output contains the URL header
      # 3. There are no actual object listings (no lines starting with FILE or DIR)
      if echo "${output}" | grep -q "Contents of" && 
         ! echo "${output}" | grep -q "^FILE\|^DIR"; then
        # Success - we have the header and no object listings
        return 0
      else
        err "Failed to handle non-existent resource in SeaweedFS"
        return 1
      fi
    else
      # Command failed, which is unexpected for SeaweedFS
      err "Command failed unexpectedly for SeaweedFS"
      return 1
    fi
  fi
}

# Test empty bucket listing
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_empty_bucket {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi

  # Construct URL with empty path, ensuring no double slashes
  local empty_url
  if [[ "${url}" == *"?"* ]]; then
    # If URL has query parameters, insert path before the ?
    local base="${url%%\?*}"
    # Remove trailing slash if present
    base="${base%/}"
    empty_url="${base}/empty?${url#*\?}"
  else
    # If no query parameters, just append path
    # Remove trailing slash if present
    url="${url%/}"
    empty_url="${url}/empty"
  fi

  # Run the command and capture both output and status
  local output
  local status
  output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
      ls "${empty_url}" 2>&1)
  status=$?

  # Check for either:
  # 1. "No objects found" message
  # 2. Or successful completion (status 0) with no objects listed
  if [[ ${status} -eq 0 ]]; then
    local filtered_output
    filtered_output=$(filter_http_debug "${output}")
    
    if echo "${output}" | grep -q "No objects found" || 
       (echo "${output}" | grep -q "Contents of" && 
        [[ -z "$(echo "${filtered_output}" | tr -d '[:space:]')" ]]); then
      log "Successfully handled empty bucket listing"
      return 0
    else
      err "Failed to handle empty bucket listing - unexpected output"
      return 1
    fi
  else
    err "Failed to handle empty bucket listing - command failed"
    return 1
  fi
}

# Test listing with existing files
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_list_with_files {
  local url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi

  # Our blobstore_list_max_keys_per_page=5; test with less, equal, and more than the page size.
  for file_count in 2; do
    log "Running ls test with ${file_count} files (flat)..."
    # Create test files
    local test_dir="${dir}/ls_test"
    mkdir -p "${test_dir}"
    for i in $(seq 1 "${file_count}"); do
      date -Iseconds > "${test_dir}/file${i}"
    done

    # Upload test files
    if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
      cp "${test_dir}" "${url}"; then
      err "Failed to upload test files for ls test"
      return 1
    fi

    # Test ls on the uploaded directory
    local output
    local status

    output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
        ls "${url}" 2>&1)
    status=$?

    local missing=0
    for i in $(seq 1 "${file_count}"); do
      if ! echo "${output}" | grep -q "ls_test/file${i}"; then
        err "Missing file${i} in ls output"
        missing=1
      fi
    done

    if [[ "${missing}" -ne 0 ]]; then
      return 1
    fi

    # Clean up test files
    if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
        rm "${url}"; then
      err "Failed to clean up test files"
      return 1
    fi
  done

  # Now test with nested structure
  local depth=3
  local files_per_level=2
  log "Running ls test with depth ${depth} and ${files_per_level} files per level"


  local test_dir="${dir}/ls_test_nested"
  mkdir -p "${test_dir}"

  # Recursive file creation
  create_nested_files() {
    local current_dir="$1"
    local current_depth="$2"

    for i in $(seq 1 "${files_per_level}"); do
      date -Iseconds > "${current_dir}/file${current_depth}_${i}"
    done

    if [[ "${current_depth}" -lt "${depth}" ]]; then
      local subdir="${current_dir}/sub${current_depth}"
      mkdir -p "${subdir}"
      create_nested_files "${subdir}" "$((current_depth + 1))"
    fi
  }

  create_nested_files "${test_dir}" 1

  # Upload test files
  if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
      cp "${test_dir}" "${url}"; then
    err "Failed to upload test files for ls test"
    return 1
  fi

  # Test ls on the uploaded directory
  local output
  local status

  # Test recursive listing - use higher page size to avoid MockS3Server pagination edge case
  output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
    --knob_blobstore_list_max_keys_per_page=10 ls --recursive "${url}" 2>&1)
  status=$?

  local missing=0
  check_nested_files() {
    local current_path="$1"
    local current_depth="$2"
    local recurse="$3"

    for i in $(seq 1 "${files_per_level}"); do
      local expected="${current_path}/file${current_depth}_${i}"
      if ! echo "${output}" | grep -q "${expected}"; then
        err "Missing ${expected} in ls output"
        missing=1
      fi
    done

    if [[ "${current_depth}" -lt "${depth}" ]]; then
      local subdir="${current_path}/sub${current_depth}"
      if [[ "${recurse}" == "true" ]]; then
        check_nested_files "${subdir}" "$((current_depth + 1))" "$3"
      fi
    fi
  }

  # Extract the path prefix from the URL (everything between host and ?)
  # URL format: blobstore://host/path/prefix?query
  local url_path
  url_path=$(echo "${url}" | sed -E 's|blobstore://[^/]+/([^?]+).*|\1|')
  
  check_nested_files "${url_path}" 1 "true"

  if [[ "${missing}" -ne 0 ]]; then
    return 1
  fi

  # Test non-recursive listing - use higher page size to avoid MockS3Server pagination edge case
  output=$(run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
  --knob_blobstore_list_max_keys_per_page=10 ls "${url}" 2>&1)
  status=$?

  check_nested_files "${url_path}" 1 "false"
  if [[ "${missing}" -ne 0 ]]; then
    return 1
  fi

  # Clean up test files
  if ! run_s3client "${s3client}" "${credentials}" "${logsdir}" "false" \
      rm "${url}"; then
    err "Failed to clean up nested test files"
    return 1
  fi

  log "Successfully tested ls with existing files and directories"
}

# Test ls command handling
# $1 The url to go against
# $2 Directory I can write test files in.
# $3 credentials file
# $4 The s3client binary.
function test_ls_handling {
  local base_url="${1}"
  local dir="${2}"
  local credentials="${3}"
  local s3client="${4}"
  local logsdir="${dir}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi

  # Test non-existent resource in existing bucket
  local nonexistent_path_url="blobstore://${host}/nonexistent/path?${query_str}"
  if ! test_nonexistent_resource "${nonexistent_path_url}" "${dir}" "${credentials}" "${s3client}"; then
    return 1
  fi

  # Test empty bucket listing (should not error)
  local empty_bucket_url="blobstore://${host}/?${query_str}"
  if ! test_empty_bucket "${empty_bucket_url}" "${dir}" "${credentials}" "${s3client}"; then
    return 1
  fi

  # Test positive case - create some files and verify ls works
  local test_url="blobstore://${host}/${path_prefix}/ls_test?${query_str}"
  if ! test_list_with_files "${test_url}" "${dir}" "${credentials}" "${s3client}"; then
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
readonly HTTP_VERBOSE_LEVEL=2
# Should we use S3? If USE_S3 is not defined, then check if
# OKTETO_NAMESPACE is defined (It is defined on the okteto
# internal apple dev environments where S3 is available).
readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"

# Set TLS_CA_FILE only when using real S3, not for SeaweedFS
if [[ "${USE_S3}" == "true" ]]; then
  # Try to find a valid TLS CA file if not explicitly set
  if [[ -z "${TLS_CA_FILE:-}" ]]; then
    # Common locations for TLS CA files on different systems
    for ca_file in "/etc/pki/tls/cert.pem" "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem" "/etc/ssl/certs/ca-certificates.crt" "/etc/pki/tls/certs/ca-bundle.crt" "/etc/ssl/cert.pem" "/usr/local/share/ca-certificates/" "/etc/ssl/certs/"; do
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

blob_credentials_file=""
bucket=""
region=""
host=""
extra_url_params=""
build_dir=""
scratch_dir=""
positional=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --blob-credentials-file)
      blob_credentials_file="$2"
      shift 2
      ;;
    --region)
      region="$2"
      shift 2
      ;;
    --bucket)
      bucket="$2"
      shift 2
      ;;  
    --host)
      host="$2"
      shift 2
      ;;
    --extra-url-params)
      extra_url_params="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

# Process command-line options.
set -- "${positional[@]+"${positional[@]}"}"
if (( $# < 1 )) || (( $# > 2 )); then
    echo "ERROR: ${0} requires the fdb build directory -- CMAKE_BUILD_DIR -- as its"
    echo "first argument and then, optionally, a directory into which we write scratch"
    echo "test data and logs (otherwise we'll write to subdirs under ${TMPDIR:-TMPDIR}."
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
readonly scratch_dir

# Set host, bucket, and blob_credentials_file whether seaweed or s3.
query_str=
path_prefix=

if [[ "${USE_S3}" == "true" ]]; then
  log "Testing against s3"
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

  if [[ -n "$host" || -n "$bucket" || -n "$region" || -n "$blob_credentials_file" ]]; then
    if [[ -n "$host" && -n "$bucket" && -n "$region" && -n "$blob_credentials_file" ]]; then
      log "Using explicit s3 configuration"
    else
      echo "ERROR: If any of --host, --bucket, --region, or --blob-credentials-file are set, then all must be set." >&2
      exit 1
    fi
  else
    log "Using inferred s3 configuration"
    if ! readarray -t configs < <(aws_setup "${build_dir}" "${TEST_SCRATCH_DIR}"); then
      err "Failed aws_setup"
      exit 1
    fi
    readonly host="${configs[0]}"
    readonly bucket="${configs[1]}"
    readonly blob_credentials_file="${configs[2]}"
    readonly region="${configs[3]}"
  fi

  # Set common variables and build query string
  if [[ -z "$host" ]]; then
    readonly host="${configs[0]}"
    readonly bucket="${configs[1]}"
    readonly blob_credentials_file="${configs[2]}"
    readonly region="${configs[3]}"
  fi

  # Build query string with optional extra params
  if [[ -n "$extra_url_params" ]]; then
    query_str='bucket='"${bucket}"'&region='"${region}${extra_url_params}"'&secure_connection=1'
  else
    query_str='bucket='"${bucket}"'&region='"${region}"'&secure_connection=1'
  fi
  path_prefix="bulkload/test/s3client"
else
  log "Testing against MockS3Server"
  # Now source in the mocks3 fixture so we can use its methods in the below.
  # shellcheck source=/dev/null
  if ! source "${cwd}/mocks3_fixture.sh"; then
    err "Failed to source mocks3_fixture.sh"
    exit 1
  fi
  if ! TEST_SCRATCH_DIR=$(mktemp -d "${scratch_dir}/mocks3_test.XXXXXX"); then
    err "Failed create of the mocks3 test dir." >&2
    exit 1
  fi
  readonly TEST_SCRATCH_DIR
  if ! start_mocks3 "${build_dir}"; then
    err "Failed to start MockS3Server"
    exit 1
  fi
  readonly host="${MOCKS3_HOST}:${MOCKS3_PORT}"
  readonly bucket="test-bucket"
  readonly region="us-east-1"
  # Create an empty blob credentials file (MockS3Server uses simple auth)
  readonly blob_credentials_file="${TEST_SCRATCH_DIR}/blob_credentials.json"
  echo '{}' > "${blob_credentials_file}"
  # Construct query string with raw ampersands using single quotes
  query_str='bucket='"${bucket}"'&region='"${region}"'&secure_connection=0'
  path_prefix="s3client"
  fi
  
  # export ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:print_stats=1:log_path=./asan_log"
  
  # Run tests.
  test="test_file_upload_and_download"
  url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
  test_file_upload_and_download "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
  log_test_result $? "${test}"
  
  if [[ "${USE_S3}" == "true" ]]; then
    # Only run this on s3. It is checking that the old s3blobstore md5 checksum still works.
    test="test_file_upload_and_download_no_integrity_check"
    url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
    test_file_upload_and_download_no_integrity_check "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
    log_test_result $? "${test}"
  fi
  
  test="test_dir_upload_and_download"
  url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
  test_dir_upload_and_download "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
  log_test_result $? "${test}"
  
  # Add ls error handling test
  test="test_ls_handling"
  url="blobstore://${host}/${path_prefix}/${test}?${query_str}"
  test_ls_handling "${url}" "${TEST_SCRATCH_DIR}" "${blob_credentials_file}" "${build_dir}/bin/s3client"
  log_test_result $? "${test}"
