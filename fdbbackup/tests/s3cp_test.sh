#!/bin/bash
#
# Start a weed server and then run tests of the s3cp
# command line tool against it (which uses S3Cp.actor.cpp).
# For use by ctest. seaweed server takes about 25
# seconds to come up. Tests run for a few seconds after that.
#
# Used https://www.shellcheck.net/
# and https://bertvv.github.io/cheat-sheets/Bash.html

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

# Globals that get set below and are used when we cleanup.
WEED_DIR=
WEED_PID=
# Use one bucket only for all tests. More buckets means
# we need more volumes which can be an issue when little
# diskspace
readonly BUCKET="testbucket"

# Cleanup the mess we've made. Called from signal trap.
cleanup() {
  if [ -n "${WEED_PID}" ]; then
    # KILL! If we send SIGTERM, seaweedfs hangs out
    # ten seconds before shutting down (could config.
    # but just kill -- no state to save).
    kill -9 "${WEED_PID}"
  fi
  if [ -d "${WEED_DIR}" ]; then
    rm -rf "${WEED_DIR}"
  fi
}

# Log a message to STDOUT with timestamp prefix
# $1 message to log
log() {
  printf "%s %s\n" "$(date -Iseconds)" "${1}"
}

# Download seaweed.
# $1 A scratch_dir for test
# Returns the full path to the weed binary.
# Caller should test for error code on return.
download_weed() {
  local dir="${1}"
  local tgz
  local os=
  if [[ "$OSTYPE" =~ ^linux ]]; then
    os="linux"
  elif [[ "$OSTYPE" =~ ^darwin ]]; then
    os="darwin"
  else
    echo "ERROR: Unsupported operating system" >&2
    # Return out of this function (does not exit program).
    exit 1
  fi
  tgz="${os}_$(uname -m).tar.gz"
  # If not already present, download it.
  local fullpath_tgz="${dir}/${tgz}"
  if [ ! -f "${fullpath_tgz}" ]; then
    # Change directory because awkward telling curl where to put download.
    ( cd "${dir}"
      curl -sL "https://github.com/seaweedfs/seaweedfs/releases/download/3.79/${tgz}" \
        -o "${tgz}")
  fi
  local weed_binary="${dir}/weed"
  if [ ! -f "${weed_binary}" ]; then
    tar xfz "${fullpath_tgz}" --directory "${dir}"
  fi
  echo "${weed_binary}"
}

# Create directory for weed to use.
# $1 A scratch directory for test.
# Returns the created weed directory. Caller should
# check error code and return.
create_weed_dir() {
  local dir="${1}"
  local weed_dir=$(mktemp -d -p "${dir}")
  # Exit if the temp directory wasn't created successfully.
  if [ ! -d "${weed_dir}" ]; then
    echo "ERROR: Failed create of weed directory ${weed_dir}" >&2
    exit 1
  fi
  echo "${weed_dir}"
}

# Start up the weed server. It can take 30 seconds to come.
# $1 the weed directory.
# Returns an array of the pid and the s3 port.
# Caller should test return value.
start_weed() {
  local binary="${1}"
  local weed_dir="${2}"
  local master_port=9333
  local s3_port=8333
  local volume_port_grpc=18080
  local volume_port=8080
  local filer_port=8888
  local max=10
  local index
  for index in $(seq 1 ${max}); do
    # Increment port numbers each time through -- even the first time
    # to get past defaults.
    ((master_port=master_port+1))
    ((s3_port=s3_port+1))
    ((volume_port=volume_port+1))
    ((volume_port_grpc=volume_port_grpc+1))
    ((filer_port=filer_port+1))
    # Start weed in background.
    "${binary}" -logdir="${weed_dir}" server -dir="${weed_dir}" \
      -s3 -ip=localhost -master.port="${master_port}" -s3.port="${s3_port}" \
      -volume.port.grpc="${volume_port_grpc}" -volume.port="${volume_port}" \
      -filer.port="${filer_port}" &> /dev/null &
    # Pick up the weed pid.
    local weed_pid=$!
    # Loop while process is coming up. It can take 25 seconds.
    while  kill -0 ${weed_pid} &> /dev/null; do
       if grep "Start Seaweed S3 API Server" "${weed_dir}/weed.INFO" &> /dev/null ; then
         # Its up and running. Breakout of this while loop and the wrapping 'for' loop
         # (hence the '2' in the below)
         break 2
       fi
      sleep 5
     done
     # The process died. If it was because of port clash, go around again w/ new ports.
     if grep "bind: address already in use" "${weed_dir}/weed.INFO" &> /dev/null ; then
       # Clashed w/ existing port. Go around again and get new ports.
       :
     else
       # Seaweed is not up and its not because of port clash. Exit.
       # Dump out the tail of the weed log because its going to get cleaned up when
       # we exit this script
       if [ -f ${weed.dir}/weed.INFO ]; then
        tail -50 "${weed_dir}/weed.INFO" >&2
       fi
       echo "ERROR: Failed to start weed" >&2
       exit 1
     fi
  done
  if [ "${index}" -ge "${max}" ]; then
    echo "ERROR: Ran out of retries (${index})"
    exit 1
  fi
  # Check server is up from client's perspective. Get a file id (fid) and volume URL.
  if ! curl -s "http://localhost:${master_port}/dir/assign" | grep fid &> /dev/null; then
    echo "ERROR: Failed to curl fid" >&2
    exit 1
  fi
  # Return two values.
  echo "${weed_pid}"
  echo "${s3_port}"
}

# Test file upload and download
# $1 The port on localhost where seaweed s3 is running.
# $2 Directory I can write test files in.
# $3 The s3cp binary.
test_file_upload_and_download() {
  local port="${1}"
  local dir="${2}"
  local s3cp="${3}"
  local logsdir="${2}/logs"
  if [ ! -d "${logsdir}" ]; then
    mkdir "${logsdir}"
  fi
  local testfileup="${dir}/testfile.up"
  local testfiledown="${dir}/testfile.down"
  date -Iseconds &> "${testfileup}"
  local blobstoreurl="blobstore://localhost:${port}/x/y/z?bucket=${BUCKET}&region=us&secure_connection=0"
  "${s3cp}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${testfileup}" "${blobstoreurl}"
  "${s3cp}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${blobstoreurl}" "${testfiledown}"
  diff "${testfileup}" "${testfiledown}"
  if test $? -ne 0; then
    echo "ERROR: Test $0 failed; upload and download are not the same."
    exit 1
  fi
}

# Test dir upload and download
# $1 The port on localhost where seaweed s3 is running.
# $2 Directory I can write test file in.
# $3 The s3cp binary.
test_dir_upload_and_download() {
  local port="${1}"
  local dir="${2}"
  local s3cp="${3}"
  local logsdir="${2}/logs"
  if [ ! -d "${logsdir}" ]; then
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
  "${s3cp}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${testdirup}" "${blobstoreurl}"
  "${s3cp}" --knob_http_verbose_level=10 --log --logdir="${logsdir}" "${blobstoreurl}" "${testdirdown}"
  diff "${testdirup}" "${testdirdown}"
  if test $? -ne 0; then
    echo "ERROR: Test $0 failed; upload and download are not the same."
    exit 1
  fi
}

# Log pass or fail.
# $1 Test errcode
# $2 Test name
log_test_result() {
  local test_errcode=$1
  local test_name=$2
  if [ $test_errcode -eq 0 ]; then
    log "PASSED ${test_name}"
  else
    log "FAILED ${test_name}"
  fi
}

# Process command-line options.
if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "ERROR: ${0} requires the fdb build directory -- CMAKE_BUILD_DIR -- as its"
    echo "first argument and then, optionally, a directory into which we write scratch"
    echo "test data and logs (otherwise we'll write to subdirs under $TMPDIR)."
    echo "Example: ${0} ./build_output ./scratch_dir"
    exit 1
fi
readonly build_dir="${1}"
if [ ! -d "${build_dir}" ]; then
  echo "ERROR: ${build_dir} is not a directory";
  exit 1
fi
scratch_dir="${TMPDIR}"
if [ $# -eq 2 ]; then
  scratch_dir="${2}"
fi
# Make sure we have curl installed.
if ! command -v curl &> /dev/null
then
    echo "ERROR: 'curl' not found."
    exit 1
fi
# Download seaweed.
readonly weed_binary_path="$(download_weed "${scratch_dir}")"
if [ $? -ne 0 ] || [ ! -f "${weed_binary_path}" ]; then
  echo "ERROR: failed download of weed binary." >&2
  exit 1
fi
WEED_DIR="$(create_weed_dir "${scratch_dir}")"
if [ $? -ne 0 ] || [ ! -d "${WEED_DIR}" ]; then
  echo "ERROR: failed create of the weed dir." >&2
  exit 1
fi
log "Starting seaweed; logfile=${WEED_DIR}/weed.INFO"
readonly returns=($(start_weed "${weed_binary_path}" "${WEED_DIR}"))
if [ $? -ne 0 ]; then
  echo "ERROR: failed start of weed server." >&2
  exit 1
fi
WEED_PID="${returns[0]}"
readonly s3_port="${returns[1]}"
log "Seaweed server is up; pid=${WEED_PID}, s3.port=${s3_port}"

# Seaweed is up. Run some tests. 
test_file_upload_and_download "${s3_port}" "${WEED_DIR}" "${build_dir}/bin/s3cp"
log_test_result $? "test_file_upload_and_download"

test_dir_upload_and_download "${s3_port}" "${WEED_DIR}" "${build_dir}/bin/s3cp"
log_test_result $? "test_dir_upload_and_download"
