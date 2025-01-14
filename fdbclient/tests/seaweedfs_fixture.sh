#!/bin/bash

# Functions to download and start https://github.com/seaweedfs/seaweedfs,
# a blob store with an S3 API.
#
# Here is how to use this fixture:
#  # Source in the seaweedfs fixture so we can use its methods in the below.
#  if ! source "${cwd}/seaweedfs_fixture.sh"; then
#    err "Failed to source seaweedfs_fixture.sh"
#    exit 1
#  fi
#  # Save off return from create_weed_dir. You'll need it below.
#  if ! TEST_SCRATCH_DIR=$(create_weed_dir "${scratch_dir}"); then
#    err "Failed create of the weed dir." >&2
#    return 1
#  fi
#  readonly TEST_SCRATCH_DIR
#  if ! host=$( run_weed "${scratch_dir}" "${TEST_SCRATCH_DIR}"); then
#    err "Failed to run seaweed"
#    return 1
#  fi
#  # When done, call shutdown_weed passing TEST_SCRATCH_DIR.
#

# Globals.
# Use same bucket always because seaweed makes volumes per bucket and
# we may be resource constrained in test environment.
# Export to make it plain its used by importers of this fixture.
readonly SEAWEED_BUCKET="${WEED_BUCKET:-testbucket}"
export SEAWEED_BUCKET

# Cleanup the mess we've made. For calling from signal trap on exit.
function shutdown_weed {
  local local_test_scratch_dir="${1}"
  if [[ -f "${local_test_scratch_dir}/weed.pid" ]]; then
    # KILL! If we send SIGTERM, seaweedfs hangs out
    # ten seconds before shutting down (could config.
    # time but just kill it -- there is no state to save).
    kill -9 $(cat "${local_test_scratch_dir}/weed.pid")
  fi
  if [[ -d "${local_test_scratch_dir}" ]]; then
    rm -rf "${local_test_scratch_dir}"
  fi
}

# Download seaweed if not already present.
# $1 The directory to download seaweed into.
# Returns the full path to the weed binary.
# Caller should test $? for error code on return.
function download_weed {
  local dir="${1}"
  local tgz
  local os=
  local arch=
  # See if weed is currently installed and use it if found.
  # https://stackoverflow.com/questions/592620/how-can-i-check-if-a-program-exists-from-a-bash-script
  if command -v weed; then
    return 0
  fi
  # Make sure we have curl installed.
  if ! command -v curl &> /dev/null; then
      echo "ERROR: 'curl' not found." >&2
      return 1
  fi
  if [[ "$OSTYPE" =~ ^linux ]]; then
    os="linux"
    arch="$(uname -m)"
    # The seaweedfs site is looking for amd64 as arch.
    if [[ "${arch}" ==  "x86_64" ]]; then
      arch="amd64"
    elif [[ "${arch}" == "aarch64" ]]; then
      arch="arm64"
    else
      echo "ERROR: Unsupported architecture ${arch}" >&2
      # Return out of this function (does not exit program).
      return 1
    fi
  elif [[ "$OSTYPE" =~ ^darwin ]]; then
    os="darwin"
    arch="$(uname -m)"
  else
    echo "ERROR: Unsupported operating system" >&2
    # Return out of this function (does not exit program).
    return 1
  fi
  tgz="${os}_${arch}.tar.gz"
  # If not already present, download it.
  local fullpath_tgz="${dir}/${tgz}"
  if [[ ! -f "${fullpath_tgz}" ]]; then
    # Change directory because awkward telling curl where to put download.
    local url="https://github.com/seaweedfs/seaweedfs/releases/download/3.79/${tgz}"
    # Presuming! that an error in subshell will be propagated because of bash -e?
    # else, wrap in an if ! /then exit?
    (
      cd "${dir}" || return 1
      if ! httpcode=$(curl -sL "${url}" -o "${tgz}" --write-out "%{http_code}"); then
        echo "ERROR: Failed curl download of ${url}; httpcode=${httpcode}." >&2
        # Clean up the tgz -- curl will touch it even if it fails.
        rm -f "${tgz}"
        return 1
      fi
      if (( "${httpcode}" < 200 )) || (( "${httpcode}" > 299 )); then
        echo "ERROR: Bad HTTP code downloading ${url}; httpcode=${httpcode}." >&2
        # Clean up the tgz -- curl will touch it even if it fails.
        rm -f "${tgz}"
        return 1
      fi
    )
  fi
  local weed_binary="${dir}/weed"
  if [[ ! -f "${weed_binary}" ]]; then
    tar xfz "${fullpath_tgz}" --directory "${dir}"
  fi
  echo "${weed_binary}"
}

# Create directory for weed to use.
# Call shutdown_weed to clean up the directory created here for test.
# $1 Directory where we want weed to write data and logs.
# check $? error code on return.
function create_weed_dir {
  local dir="${1}"
  local weed_dir
  weed_dir=$(mktemp -d -p "${dir}" -t weed.$$.XXXX)
  # Exit if the temp directory wasn't created successfully.
  if [[ ! -d "${weed_dir}" ]]; then
    echo "ERROR: Failed create of weed directory ${weed_dir}" >&2
    return 1
  fi
  echo "${weed_dir}"
}

# Start up the weed server. It can take 30 seconds to come up.
# $1 Path to weed binary (returned by download_weed function).
# $2 The path to the weed directory for logs and data.
# Returns pid of started procdess and the s3 port.
# Caller should test return $? value.
function start_weed {
  local binary="${1}"
  local dir="${2}"
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
    "${binary}" -logdir="${dir}" server -dir="${dir}" \
      -s3 -ip=localhost -master.port="${master_port}" -s3.port="${s3_port}" \
      -volume.port.grpc="${volume_port_grpc}" -volume.port="${volume_port}" \
      -filer.port="${filer_port}" &> /dev/null &
    # Pick up the weed pid.
    local weed_pid=$!
    # Loop while process is coming up. It can take 25 seconds.
    while  kill -0 ${weed_pid} &> /dev/null; do
      if grep "Start Seaweed S3 API Server" "${dir}/weed.INFO" &> /dev/null ; then
        # Its up and running. Breakout of this while loop and the wrapping 'for' loop
        # (hence the '2' in the below)
        break 2
      fi
      sleep 5
    done
    # The process died. If it was because of port clash, go around again w/ new ports.
    if grep "bind: address already in use" "${dir}/weed.INFO" &> /dev/null ; then
      # Clashed w/ existing port. Go around again and get new ports.
      :
    else
      # Seaweed is not up and it is not because of port clash. Exit.
      # Dump out the tail of the weed log because its going to get cleaned up when
      # we exit this script. Give the user an idea of what went wrong.
      if [[ -f "${dir}/weed.INFO" ]]; then
        tail -1000 "${dir}/weed.INFO" >&2
      fi
      echo "ERROR: Failed to start weed" >&2
      return 1
    fi
  done
  if (( "${index}" >= "${max}" )); then
    echo "ERROR: Ran out of retries (${index})" >&2
    return 1
  fi
  # Check server is up from client's perspective. Get a file id (fid) and volume URL.
  if ! curl -s "http://localhost:${master_port}/dir/assign" | grep fid &> /dev/null; then
    echo "ERROR: Failed to curl fid" >&2
    return 1
  fi
  # Set the PID into the global.
  echo "${weed_pid}" > "${dir}/weed.pid"
  # Return two values.
  echo "${s3_port}"
}

# Run seaweed.
# Call create_weed_dir first so you have a test_scratch_dir to pass in here.
# User will have to shut it down by calling shutdown_weed ${test_scratch_dir}.
# $1 Where to download weed binary to (we will check this dir to see if already
# a weed binary downloaded).
# $2 weed_dir (return after calling create_weed_dir so call it first).
# Returns s3 host to use for talking to weed.
function run_weed {
  local weed_download_dir="${1}"
  local local_weed_dir="${2}"
  local weed_binary_path
  if ! weed_binary_path="$(download_weed "${weed_download_dir}")"; then
    echo "ERROR: failed download of weed binary." >&2
    return 1
  fi
  if ! s3_port=$(start_weed "${weed_binary_path}" "${local_weed_dir}"); then
    echo "ERROR: failed start of weed server." >&2
    return 1
  fi
  echo "localhost:${s3_port}"
}
