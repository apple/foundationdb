#!/usr/bin/env bash

# Functions to stand up an fdb cluster.
# To use:
#  start_fdb_cluster FDB_SOURCE_DIR BUILD_DIR SCRATCH_DIR
#  start_backup_agent BUILD_DIR SCRATCH_DIR #optionally
#  When done, call shutdown_fdb_cluster

# Global of all pids.
FDB_PIDS=()

# Shutdown all processes.
function shutdown_fdb_cluster {
  # Kill all running fdb processes from tracked PIDs
  if [[ ${#FDB_PIDS[@]} -gt 0 ]]; then
    # First pass: kill -9 all tracked PIDs
    for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
      if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
        kill -9 "${FDB_PIDS[i]}" 2>/dev/null || true
      fi
    done
    
    # Give processes a moment to die after kill -9
    sleep 0.5
    
    # Second pass: verify all processes died, retry any survivors
    local still_running=0
    for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
      if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
        # Process survived the kill -9 from first pass - try again
        echo "WARNING: Process ${FDB_PIDS[i]} survived first kill -9, trying again" >&2
        # Try SIGTERM first (may help if process is in certain states)
        kill -15 "${FDB_PIDS[i]}" 2>/dev/null || true
        sleep 0.2
        # Then SIGKILL again
        kill -9 "${FDB_PIDS[i]}" 2>/dev/null || true
        sleep 0.3
        if kill -0 "${FDB_PIDS[i]}" 2>/dev/null; then
          echo "ERROR: Process ${FDB_PIDS[i]} is unkillable (zombie or kernel issue)" >&2
          still_running=$((still_running + 1))
        fi
      fi
    done
    
    if [[ ${still_running} -gt 0 ]]; then
      echo "ERROR: ${still_running} FDB processes remain unkillable - this may cause port conflicts in subsequent tests" >&2
      # Don't use pkill - it would kill processes from other concurrent tests
      # Instead, just report the problem and let the test infrastructure handle it
    fi
  fi
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
    FDB_PIDS=()
    local retries=5
    for ((i=0; i<retries; i++)); do
      if [[ -f "${output}" ]]; then
        FDB_PIDS=($(grep -e "PIDS=" "${output}" | sed -e 's/PIDS=//' | xargs)) || true
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
    if grep 'Local address in use' "${output}"; then
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
