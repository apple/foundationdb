#!/usr/bin/env bash

# Functions to stand up an fdb cluster.
# To use:
#  start_fdb_cluster FDB_SOURCE_DIR BUILD_DIR SCRATCH_DIR
#  start_backup_agent BUILD_DIR SCRATCH_DIR #optionally
#  When done, call shutdown_fdb_cluster

# Global of all pids.
FDB_PIDS=()

# Shutdown all processes with explicit logging and aggressive timeouts.
function shutdown_fdb_cluster {
  echo "$(date -Iseconds) shutdown_fdb_cluster: starting (${#FDB_PIDS[@]} tracked PIDs)"
  
  local shutdown_start_time=$(date +%s)
  local max_shutdown_time=15  # Maximum time to spend in shutdown
  
  # Kill all running fdb processes from tracked PIDs
  if [[ ${#FDB_PIDS[@]} -gt 0 ]]; then
    # First pass: Try graceful shutdown (SIGTERM)
    echo "$(date -Iseconds) shutdown_fdb_cluster: sending SIGTERM to all processes"
    for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
      # Check if we're taking too long
      local current_time=$(date +%s)
      if [[ $((current_time - shutdown_start_time)) -gt $max_shutdown_time ]]; then
        echo "$(date -Iseconds) shutdown_fdb_cluster: TIMEOUT - aborting graceful shutdown"
        break
      fi
      
      if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
        echo "$(date -Iseconds) shutdown_fdb_cluster: SIGTERM -> PID ${FDB_PIDS[i]}"
        kill -15 "${FDB_PIDS[i]}" 2>/dev/null || true
      fi
    done
    
    # Brief wait for graceful shutdown
    local current_time=$(date +%s)
    if [[ $((current_time - shutdown_start_time)) -lt $max_shutdown_time ]]; then
      sleep 0.2
    fi
    
    # Second pass: Force kill any survivors with SIGKILL immediately
    echo "$(date -Iseconds) shutdown_fdb_cluster: sending SIGKILL to survivors"
    local still_running=0
    for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
      # Check timeout again
      local current_time=$(date +%s)
      if [[ $((current_time - shutdown_start_time)) -gt $max_shutdown_time ]]; then
        echo "$(date -Iseconds) shutdown_fdb_cluster: TIMEOUT - skipping remaining processes"
        break
      fi
      
      if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
        echo "$(date -Iseconds) shutdown_fdb_cluster: SIGKILL -> PID ${FDB_PIDS[i]} (didn't respond to SIGTERM)"
        kill -9 "${FDB_PIDS[i]}" 2>/dev/null || true
      fi
    done
    
    # Brief check period
    local current_time=$(date +%s)
    if [[ $((current_time - shutdown_start_time)) -lt $max_shutdown_time ]]; then
      sleep 0.1
    fi
    
    # Third pass: Check for unkillable processes (no wait, just report) - but with timeout
    for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
      # Check timeout
      local current_time=$(date +%s)
      if [[ $((current_time - shutdown_start_time)) -gt $max_shutdown_time ]]; then
        echo "$(date -Iseconds) shutdown_fdb_cluster: TIMEOUT - skipping process check"
        break
      fi
      
      if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
        # Get process info for debugging - but don't let ps hang
        local proc_info=$(timeout 2 ps -p "${FDB_PIDS[i]}" -o pid,ppid,stat,comm 2>/dev/null || echo "PID not accessible or timeout")
        echo "$(date -Iseconds) ERROR: Process ${FDB_PIDS[i]} is unkillable (zombie or kernel issue)" >&2
        echo "       Process info: ${proc_info}" >&2
        still_running=$((still_running + 1))
      fi
    done
    
    if [[ ${still_running} -gt 0 ]]; then
      echo "$(date -Iseconds) ERROR: ${still_running} FDB processes remain unkillable" >&2
    fi
  else
    echo "$(date -Iseconds) shutdown_fdb_cluster: no tracked PIDs to shutdown"
  fi
  
  local total_shutdown_time=$(($(date +%s) - shutdown_start_time))
  echo "$(date -Iseconds) shutdown_fdb_cluster: complete (took ${total_shutdown_time}s)"
}

# Start an fdb cluster. If port clashes, try again with new ports.
# $1 source directory
# $2 build directory
# $3 scratch directory
# $4 How many SS to run
# ... knobs to apply to the cluster.
# Check $? on return.
function start_fdb_cluster {
  local local_source_dir="${1}"
  local local_build_dir="${2}"
  local local_scratch_dir="${3}"
  local ss_count="${4}"
  shift 4
  #TODO(BulkLoad): enable_read_lock_on_range
  local knobs="--knob_shard_encode_location_metadata=true"
  if (( $# > 0 )); then
    for item in "${@}"; do
      knobs="${knobs} ${item}"
    done
  fi
  knobs=$(echo "$knobs" | sed 's/^[[:space:]]*//')
  local output="${local_scratch_dir}/output.$$.txt"
  local port_prefix=1500
  while : ; do
    port_prefix="$(( port_prefix + 100 ))"
    # Disable exit on error temporarily so can capture result from run cluster.
    # Then redirect the output of the run_customer_cluster.sh via tee via
    # 'process substitution'; piping to tee hangs on success.
    set +o errexit  # a.k.a. set +e
    set +o noclobber
    # In the below $knobs will pick up single quotes -- its what bash does when it
    # outputs strings with spaces or special characters. In this case, we want the
    # single quotes.
    # TODO(BulkLoad): randomly select storage engine.
    # Currently, we cannot use shardedrocksdb because bulkload fetchkey does not support
    # shardedrocksdb. 
    LOOPBACK_DIR="${local_scratch_dir}/loopback_cluster" PORT_PREFIX="${port_prefix}" \
      "${local_source_dir}/tests/loopback_cluster/run_custom_cluster.sh" \
      "${local_build_dir}" \
      --knobs "${knobs}" \
      --stateless_count 1 --replication_count 1 --logs_count 1 \
      --storage_count "${ss_count}" --storage_type ssd-rocksdb-v1 \
      --dump_pids on \
      > >(tee "${output}") \
      2> >(tee "${output}" >&2)
    status="$?"
    # Restore exit on error.
    set -o errexit  # a.k.a. set -e
    set -o noclobber
    # Set the global FDB_PIDS with retry logic for robustness
    # Use grep -a to treat binary files as text (output may contain binary data from fdbserver)
    FDB_PIDS=()
    local retries=5
    for ((i=0; i<retries; i++)); do
      if [[ -f "${output}" ]]; then
        FDB_PIDS=($(grep -a -e "PIDS=" "${output}" | sed -e 's/PIDS=//' | xargs)) || true
        if [[ ${#FDB_PIDS[@]} -gt 0 ]]; then
          break
        fi
      fi
      echo "Retrying PID extraction (attempt $((i+1))/${retries})..."
      sleep 0.5
    done
    
    # For debugging... on exit, it can complain: 'line 16: kill: Binary: arguments must be process or job IDs'
    if [[ -n "${FDB_PIDS[*]:-}" ]]; then
      echo "Tracked FDB PIDs: ${FDB_PIDS[*]}"
    else
      echo "WARNING: No FDB PIDs found for tracking"
    fi
    if (( status == 0 )); then
      # Give the db a second to come healthy.
      sleep 1
      if ! "${local_build_dir}/bin/fdbcli" -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" --exec status; then
        err "Client failed to obtain healthy status"
        return 1
      fi
      break;
    fi
    # Otherwise, look for 'Local address in use' and if found retry with different ports.
    # Use grep -a to treat binary files as text
    if grep -a 'Local address in use' "${output}"; then
      log "Ports in use; retry cluster start but with different ports"
      continue
    fi
    err "Failed to start fdb cluster"
    return 1
  done
}

# Start backup_agent
# $1 The build dir.
# $2 The scratch dir.
# $@ List of knobs to pass
function start_backup_agent {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  shift 2
  local local_knobs=""
  if (( $# > 0 )); then
    local_knobs="${*}"
  fi
  # Just using ${extra_knobs[*]} in the below will output a string
  # quoted by single quotes; its what bash does when string has spaces
  # or special characters. To get around this, we printf the string instead;
  # this will print out string w/ spaces w/o quotes.
  # Don't quote the printf substatement... bash will add single quotes.
  "${local_build_dir}/bin/backup_agent" \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --log --logdir="${local_scratch_dir}" \
    $(printf "%s" "${local_knobs}") &
  local pid=$!
  if ! ps -p "${pid}" &> /dev/null; then
    wait "${pid}"
    status=$?
    err "Failed to start backup_agent."
    return "${status}"
  fi
  # Otherwise, it came up. Add it to pids to kill on way out.
  FDB_PIDS+=( "${pid}" ) 
}
