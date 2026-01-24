#!/usr/bin/env bash

# Globals.
# Functions shared by ctests.
#

# Globals.
# Values loaded up into the database.
FDB_DATA=()
readonly FDB_DATA_KEYCOUNT=${FDB_DATA_KEYCOUNT:-100}
readonly FDB_KEY_PREFIX=${FDB_KEY_PREFIX:-"key__"}

# Cleanup watchdog PID (used by start/cancel_cleanup_watchdog)
CLEANUP_WATCHDOG_PID=""

# Start a watchdog that will force-kill the script if cleanup takes too long.
# This prevents CTest timeouts when cleanup hangs.
# $1 - timeout in seconds (default: 30)
function start_cleanup_watchdog {
  local timeout_seconds="${1:-30}"
  local my_pid=$$
  local my_pgid=$(ps -o pgid= -p $$ 2>/dev/null | tr -d ' ' || echo "")
  
  # Create a more robust watchdog that tries multiple kill strategies
  (
    sleep "$timeout_seconds"
    echo "$(date -Iseconds) CLEANUP TIMEOUT after ${timeout_seconds}s - forcing exit"
    
    # Strategy 1: Kill process group (most effective for shell scripts)
    if [[ -n "$my_pgid" ]] && kill -0 -$my_pgid 2>/dev/null; then
      echo "$(date -Iseconds) WATCHDOG: Killing process group $my_pgid"
      kill -9 -$my_pgid 2>/dev/null && {
        echo "$(date -Iseconds) WATCHDOG: Successfully killed process group"
        exit 0
      }
    fi
    
    # Strategy 2: Kill the main process directly
    if kill -0 $my_pid 2>/dev/null; then
      echo "$(date -Iseconds) WATCHDOG: Killing main process $my_pid"
      kill -9 $my_pid 2>/dev/null && {
        echo "$(date -Iseconds) WATCHDOG: Successfully killed main process"
        exit 0
      }
    fi
    
    # Strategy 3: Nuclear option - kill all processes with same script name
    local script_name=$(basename "${BASH_SOURCE[1]:-$0}" 2>/dev/null || echo "")
    if [[ -n "$script_name" ]] && command -v pkill >/dev/null 2>&1; then
      echo "$(date -Iseconds) WATCHDOG: Nuclear option - killing all $script_name processes"
      pkill -9 -f "$script_name" 2>/dev/null || true
    fi
    
    echo "$(date -Iseconds) WATCHDOG: Cleanup timeout handling complete"
  ) &
  CLEANUP_WATCHDOG_PID=$!
  
  # Don't disown - we want to be able to cancel this if cleanup finishes normally
  echo "$(date -Iseconds) Started cleanup watchdog (PID: $CLEANUP_WATCHDOG_PID, timeout: ${timeout_seconds}s)"
}

# Cancel the cleanup watchdog (call this when cleanup finishes successfully)
function cancel_cleanup_watchdog {
  if [[ -n "${CLEANUP_WATCHDOG_PID:-}" ]]; then
    if kill -0 $CLEANUP_WATCHDOG_PID 2>/dev/null; then
      echo "$(date -Iseconds) Canceling cleanup watchdog (PID: $CLEANUP_WATCHDOG_PID)"
      kill $CLEANUP_WATCHDOG_PID 2>/dev/null || true
      # Wait briefly for it to exit
      local i=0
      while kill -0 $CLEANUP_WATCHDOG_PID 2>/dev/null && [[ $i -lt 10 ]]; do
        sleep 0.1
        i=$((i + 1))
      done
      # Force kill if it didn't exit gracefully
      if kill -0 $CLEANUP_WATCHDOG_PID 2>/dev/null; then
        kill -9 $CLEANUP_WATCHDOG_PID 2>/dev/null || true
      fi
    fi
    CLEANUP_WATCHDOG_PID=""
  fi
}

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

# Check if test data should be preserved (PRESERVE_TEST_DATA=1)
# If yes, prints preservation message and returns 0 (should skip cleanup)
# If no, returns 1 (should continue with normal cleanup)
# Usage in test cleanup functions:
#   if should_preserve_test_data; then
#     shutdown_servers_only  # Shutdown but don't delete
#     return 0
#   fi
function should_preserve_test_data {
  if [[ "${PRESERVE_TEST_DATA:-0}" == "1" ]]; then
    echo "======================================================================"
    echo "PRESERVING TEST DATA (PRESERVE_TEST_DATA=1):"
    echo "  Scratch dir: ${TEST_SCRATCH_DIR:-none}"
    if [[ -n "${TEST_SCRATCH_DIR:-}" ]] && [[ -d "${TEST_SCRATCH_DIR}/mocks3_data" ]]; then
      echo "  MockS3 data: ${TEST_SCRATCH_DIR}/mocks3_data"
    fi
    echo "======================================================================"
    return 0
  fi
  return 1
}

# Common cleanup handler for tests that checks preserve flag and shuts down servers
# Returns 0 if data is being preserved (caller should return immediately)
# Returns 1 if normal cleanup should continue
function cleanup_with_preserve_check {
  if should_preserve_test_data; then
    echo "$(date -Iseconds) cleanup_with_preserve_check: preserving data, shutting down servers only"
    # Shutdown servers but don't delete data
    if type shutdown_fdb_cluster &> /dev/null; then
      echo "$(date -Iseconds) cleanup_with_preserve_check: calling shutdown_fdb_cluster"
      shutdown_fdb_cluster
    fi
    if type shutdown_mocks3 &> /dev/null; then
      echo "$(date -Iseconds) cleanup_with_preserve_check: calling shutdown_mocks3"
      shutdown_mocks3
    fi
    echo "$(date -Iseconds) cleanup_with_preserve_check: done (preserving data)"
    return 0
  fi
  return 1
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
  local local_scratch_dir="${2}"
  if ! result=$("${local_build_dir}/bin/fdbcli" \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
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
  local local_scratch_dir="${2}"
  if ! result=$("${local_build_dir}/bin/fdbcli" \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
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
  local local_scratch_dir="${2}"
  for (( i=0; i<"${FDB_DATA_KEYCOUNT}"; i++)); do
    FDB_DATA+=("${i}.$(date -Iseconds)")
  done
  local load_str="writemode on;"
  for (( i=0; i<"${#FDB_DATA[@]}"; i++)); do
    load_str="${load_str} set $(make_key "${i}") ${FDB_DATA[i]};"
  done
  if ! echo "${load_str}" | \
    "${local_build_dir}/bin/fdbcli" -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" >&2
  then
    err "Failed to load data"
    return 1
  fi
  if ! has_data "${local_build_dir}" "${local_scratch_dir}"; then
    err "No data"
    return 1
  fi
}

# Clear out the db.
# $1 the build directory so we can find fdbcli
# $2 scratch directory
function clear_data {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  if ! "${local_build_dir}/bin/fdbcli" \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --exec "writemode on; clearrange \"\" \xff;"
  then
    err "Failed to clearrange"
    return 1
  fi
  if ! has_nodata "${local_build_dir}" "${local_scratch_dir}"; then
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
  local local_scratch_dir="${2}"
  local value
  for (( i=0; i<"${#FDB_DATA[@]}"; i++)); do
    value=$("${local_build_dir}/bin/fdbcli" \
      -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
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
  if grep -r -C 3 -e "Severity=\"40\"" "${dir}"; then
    err "Found 'Severity=40' errors"
    return 1
  fi
}

function test_fdbcli_status_json_for_bkup {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  # Give backup agent time to write status
  sleep 5
  "${local_build_dir}"/bin/fdbcli -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" --exec 'status json' | jq '.cluster.layers'
}
