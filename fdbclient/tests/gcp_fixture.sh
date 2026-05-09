#!/usr/bin/env bash
#
# Functions for dealing w/ gcp/GCS
#
# Here is how to use this fixture:
#
# - set GCS_FDB_BUCKET and GCS_APPLICATION_TOKEN
#
#  # First source this fixture.
#  if ! source "${cwd}/gcp_fixture.sh"; then
#    err "Failed to source gcp_fixture.sh"
#    exit 1
#  fi
#  # Save off the return from create_gcp_dir. You'll need it for shutdown.
#  if ! TEST_SCRATCH_DIR=$( create_gcp_dir "${scratch_dir}" ); then
#    err "Failed creating local gcp_dir"
#    exit 1
#  fi
#  readonly TEST_SCRATCH_DIR
#  if ! readarray -t configs < <(gcp_setup "${build_dir}" "${TEST_SCRATCH_DIR}"); then
#    err "Failed gcp_setup"
#    return 1
#  fi
#  readonly host="${configs[0]}"
#  ...etc.
#  # When done, call shutdown_gcp
#  shutdown_gcp "${TEST_SCRATCH_DIR}"
#

# Cleanup any mess we've made. For calling from signal trap on exit.
# $1 The gcp scratch directory to clean up on exit.
function shutdown_gcp {
  local local_scratch_dir="${1}"
  if [[ -d "${local_scratch_dir}" ]]; then
    rm -rf "${local_scratch_dir}"
  fi
}

# Create directory for gcp test to use writing temporary data.
# Call shutdown_gcp to clean up the directory created here for test.
# $1 Directory where we want to write data and logs.
# check $? error code on return.
function create_gcp_dir {
  local dir="${1}"
  local gcp_dir
  gcp_dir=$(mktemp -d "${dir}/gcp.$$.XXXX")
  # Exit if the temp directory wasn't created successfully.
  if [[ ! -d "${gcp_dir}" ]]; then
    echo "ERROR: Failed create of gcp directory ${gcp_dir}" >&2
    return 1
  fi
  echo "${gcp_dir}"
}

# Write out blob_credentials
# $1 The token to use querying credentials
# $2 Hostname going to gcp
# $3 The scratch dir to write the blob credentials file into
# Echos the blob_credentials file path.
function write_gcp_blob_credentials {
  local token="${1}"
  local host="${2}"
  local dir="${3}"
  blob_credentials_str="{\"accounts\": { \"@${host}\": {\"token\": \"${token}\"}}}"
  readonly blob_credentials_file="${dir}/blob_credentials.json"
  echo "${blob_credentials_str}" > "${blob_credentials_file}"
  echo "${blob_credentials_file}"
}

# Set up GCS access.
# $1 build_dir
# $2 gcp_dir (This is what is returned when you call create_gcp_dir
# so call it first).
# Returns array of configurations to use contacting GCS.
function gcp_setup {
  if [ -z "$GCS_APPLICATION_TOKEN" ]; then
    echo "Error: GCS_APPLICATION_TOKEN is not set."
    exit 1
  fi
  if [ -z "$GCS_FDB_BUCKET" ]; then
    echo "Error: GCS_FDB_BUCKET is not set."
    exit 1
  fi
  local local_build_dir="${1}"
  local local_gcp_dir="${2}"

  readonly host="storage.googleapis.com"
  if ! blob_credentials_file=$(write_gcp_blob_credentials "${GCS_APPLICATION_TOKEN}" "${host}" "${local_gcp_dir}"); then
    err "Failed to write credentials file"
    exit 1
  fi
  results_array=("@${host}" "${GCS_FDB_BUCKET}" "${blob_credentials_file}")
  printf "%s\n" "${results_array[@]}"
}
