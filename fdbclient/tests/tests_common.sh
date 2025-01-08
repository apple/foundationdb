#!/bin/bash

# Globals.
# Functions shared by ctests.
#

# Globals.
# Values loaded up into the database.
FDB_DATA=()
readonly FDB_DATA_KEYCOUNT=${FDB_DATA_KEYCOUNT:-100}
readonly FDB_KEY_PREFIX=${FDB_KEY_PREFIX:-"key__"}

# Log a message to STDOUT with timestamp prefix
# $1 message to log
function log {
  printf "%s %s\n" "$(date -Iseconds)" "${1}"
}

# Log to STDERR
# $* What to log.
function err {
  echo "$(date -Iseconds) ERROR: ${*}" >&2
}

# Make a key for fdb.
# $1 an index to use in the key name.
# $2 prefix for the key
function make_key {
  echo "${FDB_KEY_PREFIX}${1}"
}

# Does the database have data?
# $1 the build directory so we can find fdbcli
# $2 scratch directory where we can find fdb.cluster file
function has_data {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  if ! result=$("${local_build_dir}/bin/fdbcli" \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "getrange \"\" \xff 1000" 2>&1 )
  then
    err "Failed to getrange"
    return 1
  fi
  if ! echo "${result}" | grep "${FDB_KEY_PREFIX}"
  then
    err "No data"
    return 1
  fi
}

# Does the database have no data?
# $1 the build directory so we can find fdbcli
# $2 scratch directory so we can find fdb.cluster file.
function has_nodata {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  if ! result=$("${local_build_dir}/bin/fdbcli" \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "getrange \"\" \xff 1000" 2>&1 )
  then
    err "Failed to getrange"
    return 1
  fi
  if ! echo "${result}" | grep "${FDB_KEY_PREFIX}"; then
    # We did not find any keys in the output. Good.
    :
  else
    err "Has data"
    return 1
  fi
}

# Load data up into fdb. As sideeffect we populate
# FDB_DATA array w/ what we put to fdb.
# $1 the build directory so we can find fdbcli
# $2 scratch directory
# Sets the FDB_DATA Global array variable.
function load_data {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  for (( i=0; i<"${FDB_DATA_KEYCOUNT}"; i++)); do
    FDB_DATA+=("${i}.$(date -Iseconds)")
  done
  local load_str="writemode on;"
  for (( i=0; i<"${#FDB_DATA[@]}"; i++)); do
    load_str="${load_str} set $(make_key "${i}") ${FDB_DATA[i]};"
  done
  if ! echo "${load_str}" | \
    "${local_build_dir}/bin/fdbcli" -C "${scratch_dir}/loopback_cluster/fdb.cluster" >&2
  then
    err "Failed to load data"
    return 1
  fi
  if ! has_data "${local_build_dir}" "${scratch_dir}"; then
    err "No data"
    return 1
  fi
}

# Clear out the db.
# $1 the build directory so we can find fdbcli
# $2 scratch directory
function clear_data {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  if ! "${local_build_dir}/bin/fdbcli" \
    -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "writemode on; clearrange \"\" \xff;"
  then
    err "Failed to clearrange"
    return 1
  fi
  if ! has_nodata "${local_build_dir}" "${scratch_dir}"; then
    err "Has data"
    return 1
  fi
}

# Verify data is up in fdb
# $1 the build directory so we can find fdbcli
# $2 scratch directory
# $3 the values to check for in fdb.
# Returns an array of the values we loaded.
function verify_data {
  local local_build_dir="${1}"
  local scratch_dir="${2}"
  local value
  for (( i=0; i<"${#FDB_DATA[@]}"; i++)); do
    value=$("${local_build_dir}/bin/fdbcli" \
      -C "${scratch_dir}/loopback_cluster/fdb.cluster" \
      --exec "get $(make_key "${i}")" | \
      sed -e "s/.*is [[:punct:]]//" | sed -e "s/[[:punct:]]*$//")
    if [[ "${FDB_DATA[i]}" != "${value}" ]]; then
      err "${FDB_DATA[i]} is not equal to ${value}"
      return 1
    fi
  done
}

# Check source directory
# $1 Directory to check
# Check $? on return.
function is_fdb_source_dir {
  local dir="${1}"
  if [[ ! -d "${dir}" ]]; then
    err "${dir} is not a directory"
    return 1
  fi
  if [[ ! -f "${dir}/LICENSE" ]]; then
    err "${dir} is not an fdb source directory"
    return 1
  fi
  echo "${dir}"
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

# Grep for 'Severity=40' errors in logs.
# $1 Dir to search under.
function grep_for_severity40 {
  local dir="${1}"
  if grep -r -e "Severity=\"40\"" "${dir}"; then
    err "Found 'Severity=40' errors"
    return 1
  fi
}
