#!/bin/bash

# Functions to stand up an fdb cluster.
# To use:
#  start_fdb_cluster FDB_SOURCE_DIR BUILD_DIR SCRATCH_DIR
#  start_backup_agent BUILD_DIR SCRATCH_DIR #optionally
#  When done, call shutdown_fdb_cluster

# Global of all pids.
FDB_PIDS=()

# Shutdown all processes.
function shutdown_fdb_cluster {
  # Kill all running fdb processes.
  for (( i=0; i < "${#FDB_PIDS[@]}"; ++i)); do
    kill -9 "${FDB_PIDS[i]}"
  done
}

# Start an fdb cluster. If port clashes, try again with new ports.
# $1 source directory
# $2 build directory
# $3 scratch directory
# Check $? on return.
function start_fdb_cluster {
  local local_source_dir="${1}"
  local local_build_dir="${2}"
  local local_scratch_dir="${3}"
  local output="${local_scratch_dir}/output.$$.txt"
  local port_prefix=1500
  while : ; do
    port_prefix="$(( port_prefix + 100 ))"
    # Disable exit on error temporarily so can capture result from run cluster.
    # Then redirect the output of the run_customer_cluster.sh via tee via
    # 'process substitution'; piping to tee hangs on success..
    set +o errexit  # a.k.a. set +e
    set +o noclobber
    LOOPBACK_DIR="${local_scratch_dir}/loopback_cluster" PORT_PREFIX="${port_prefix}" \
      "${local_source_dir}/tests/loopback_cluster/run_custom_cluster.sh" \
      "${local_build_dir}" \
      --knobs "--knob_shard_encode_location_metadata=true" \
      --stateless_count 1 --replication_count 1 --logs_count 1 \
      --storage_count 1 --storage_type ssd-rocksdb-v1 \
      --dump_pids on > >( tee "${output}" ) 2>&1
    status="$?"
    # Restore exit on error.
    set -o errexit  # a.k.a. set -e
    set -o noclobber
    # Set the global FDB_PIDS
    FDB_PIDS=($(grep -e "PIDS=" "${output}" | sed -e 's/PIDS=//' | xargs)) || true
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
  "${local_build_dir}/bin/backup_agent" \
    -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" \
    --log --logdir="${local_scratch_dir}" \
    ${@} &
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
