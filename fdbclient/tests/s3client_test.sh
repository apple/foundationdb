#!/bin/bash
#
# Start a weed server and then run tests of the s3client
# command line tool against it (which uses S3Cp.actor.cpp).
# For use by ctest. seaweed server takes about 25
# seconds to come up. Tests run for a few seconds after that.
#
# Used https://www.shellcheck.net/, https://bertvv.github.io/cheat-sheets/Bash.html,
# and https://bertvv.github.io/cheat-sheets/Bash.html

# Some copied from down the page on
# https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
# set -o xtrace   # a.k.a set -x
set -o errexit  # a.k.a. set -e
set -o nounset  # a.k.a. set -u
set -o pipefail
set -o noclobber

# From https://stackoverflow.com/questions/59895/how-can-i-get-the-source-directory-of-a-bash-script-from-within-the-script-itsel
source=${BASH_SOURCE[0]}
while [[ -h "${source}" ]]; do # resolve $source until the file is no longer a symlink
  dir=$( cd -P "$( dirname "${source}" )" >/dev/null 2>&1 && pwd )
  source=$(readlink "${source}")
  [[ ${source} != /* ]] && source="${dir}/${source}" # if $source was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
cwd=$( cd -P "$( dirname "${source}" )" >/dev/null 2>&1 && pwd )
# Now source in the seaweedfs fixture so we can use its methods in the below.
# shellcheck source=/dev/null
source "${cwd}/seaweedfs_fixture.sh"

# Globals that get set below and are used when we cleanup.
# Use one bucket only for all tests. More buckets means
# we need more volumes which can be an issue when little
# diskspace.
readonly BUCKET="${S3_BUCKET:-testbucket}"

# Make sure cleanup on script exit.
trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup  EXIT

# Cleanup. Called from signal trap.
function cleanup {
  shutdown_weed
}

# Log a message to STDOUT with timestamp prefix
# $1 message to log
function log {
  printf "%s %s\n" "$(date -Iseconds)" "${1}"
}

# Test file upload and download
# $1 The port on localhost where seaweed s3 is running.
# $2 Directory I can write test files in.
# $3 The s3client binary.
function test_file_upload_and_download {
  local port="${1}"
  local dir="${2}"
  local s3client="${3}"
  local logsdir="${2}/logs"
  if [[ ! -d "${logsdir}" ]]; then
    mkdir "${logsdir}"
  fi
  local testfileup="${dir}/testfile.up"
  local testfiledown="${dir}/testfile.down"
  date -Iseconds &> "${testfileup}"
  local blobstoreurl="blobstore://localhost:${port}/x/y/z?bucket=${BUCKET}&region=us&secure_connection=0"
  "${s3client}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${testfileup}" "${blobstoreurl}"
  "${s3client}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${blobstoreurl}" "${testfiledown}"
  if ! diff "${testfileup}" "${testfiledown}"; then
    echo "ERROR: Test $0 failed; upload and download are not the same." >&2
    return 1
  fi
}

# Test dir upload and download
# $1 The port on localhost where seaweed s3 is running.
# $2 Directory I can write test file in.
# $3 The s3client binary.
function test_dir_upload_and_download {
  local port="${1}"
  local dir="${2}"
  local s3client="${3}"
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
  local blobstoreurl="blobstore://localhost:${port}/dir1/dir2?bucket=${BUCKET}&region=us&secure_connection=0"
  "${s3client}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${testdirup}" "${blobstoreurl}"
  "${s3client}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${blobstoreurl}" "${testdirdown}"
  if ! diff "${testdirup}" "${testdirdown}"; then
    echo "ERROR: Test $0 failed; upload and download are not the same." >&2
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
  echo "ERROR: ${build_dir} is not a directory" >&2
  exit 1
fi
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 2 )); then
  scratch_dir="${2}"
fi

# Download seaweed.
if ! weed_binary_path="$(download_weed "${scratch_dir}")"; then
  echo "ERROR: failed download of weed binary." >&2
  exit 1
fi
readonly weed_binary_path
if ! create_weed_dir "${scratch_dir}"; then
  echo "ERROR: failed create of the weed dir." >&2
  exit 1
fi
log "Starting seaweed..."
if ! s3_port=$(start_weed "${weed_binary_path}"); then
  echo "ERROR: failed start of weed server." >&2
  exit 1
fi
readonly s3_port
log "Seaweed server is up; s3.port=${s3_port}"

# Seaweed is up. Run some tests. 
test_file_upload_and_download "${s3_port}" "${WEED_DIR}" "${build_dir}/bin/s3client"
log_test_result $? "test_file_upload_and_download"

test_dir_upload_and_download "${s3_port}" "${WEED_DIR}" "${build_dir}/bin/s3client"
log_test_result $? "test_dir_upload_and_download"
