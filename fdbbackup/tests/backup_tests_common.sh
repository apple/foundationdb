#!/usr/bin/env bash
#
# backup_tests_common.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Common backup test functions
# Shared between s3_backup_test.sh, s3_backup_bulkdump_bulkload.sh, dir_backup_test.sh, etc.
# These functions work with both S3/blobstore and file-based backup testing

# Helper function to add base arguments (cluster file and logging)
# Uses bash nameref (requires bash 4.3+) to modify the array in place
# $1 name of the array variable to modify (passed by name, not value)
# $2 cluster file path
# $3 log directory
# $4 cluster file flag: "backup" uses -C, "restore" uses --dest-cluster-file (default: restore)
function add_base_args {
  local -n _args_ref="$1"
  local cluster_file="${2}"
  local log_dir="${3}"
  local flag_type="${4:-restore}"

  if [[ "${flag_type}" == "backup" ]]; then
    _args_ref+=("-C" "${cluster_file}")
  else
    _args_ref+=("--dest-cluster-file" "${cluster_file}")
  fi
  _args_ref+=("--log" "--logdir=${log_dir}")
}

# Helper function to add common optional arguments to a command args array
# Uses bash nameref (requires bash 4.3+) to modify the array in place
# $1 name of the array variable to modify (passed by name, not value)
# $2 blob credentials file (optional)
# $3 mode (optional): for backup: bulkdump|rangefile|both; for restore: rangefile|bulkload
# $4 encryption key file (optional)
function add_common_optional_args {
  local -n _args_ref="$1"
  local blob_credentials="${2:-}"
  local mode="${3:-}"
  local encryption_key_file="${4:-}"

  if [[ -n "${blob_credentials}" ]]; then
    _args_ref+=("--blob-credentials" "${blob_credentials}")
  fi

  if [[ -n "${mode}" ]]; then
    _args_ref+=("--mode" "${mode}")
  fi

  if [[ -n "${encryption_key_file}" ]]; then
    _args_ref+=("--encryption-key-file" "${encryption_key_file}")
  fi

  for knob in "${KNOBS[@]}"; do
    _args_ref+=("${knob}")
  done
}

# Pre-clear S3 URL before test (only for real S3, not MockS3Server)
# $1 build directory, $2 scratch directory, $3 url, $4 blob credentials file
# Returns 0 if cleared or skipped (MockS3Server), 1 on error
function s3_preclear_url {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local credentials="${4}"
  
  if [[ "${USE_S3}" != "true" ]]; then
    # MockS3Server - buckets are lazily created, skip preclear
    return 0
  fi
  
  local cmd=("${local_build_dir}/bin/s3client")
  cmd+=("${KNOBS[@]}")
  cmd+=("--tls-ca-file" "${TLS_CA_FILE}")
  cmd+=("--blob-credentials" "${credentials}")
  cmd+=("--log" "--logdir" "${local_scratch_dir}")
  cmd+=("rm" "${local_url}")
  
  if ! "${cmd[@]}"; then
    err "Failed pre-cleanup rm of ${local_url}"
    return 1
  fi
  return 0
}

# Cleanup S3 URL after test (works for both S3 and MockS3Server)
# $1 build directory, $2 scratch directory, $3 url, $4 blob credentials file
function s3_cleanup_url {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local credentials="${4}"
  
  local cmd=("${local_build_dir}/bin/s3client")
  cmd+=("${KNOBS[@]}")
  
  # Only add TLS CA file for real S3, not MockS3Server
  if [[ "${USE_S3}" == "true" ]]; then
    cmd+=("--tls-ca-file" "${TLS_CA_FILE}")
  fi
  
  cmd+=("--blob-credentials" "${credentials}")
  cmd+=("--log" "--logdir" "${local_scratch_dir}")
  cmd+=("rm" "${local_url}")
  
  if ! "${cmd[@]}"; then
    err "Failed rm of ${local_url}"
    return 1
  fi
  return 0
}

# Shared backup function with optional parameters
# $1 build directory, $2 scratch directory, $3 backup url, $4 tag
# $5 encryption key file (optional)
# $6 backup mode (optional): bulkdump|rangefile|both - controls snapshot mechanism
# $7 blob credentials file (optional)
function run_backup {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_tag="${4}"
  local local_encryption_key_file="${5:-}"
  local backup_mode="${6:-}"
  local blob_credentials="${7:-}"
  
  # Start backup without waiting, then monitor with timeout
  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  local cmd_args=(
    "-t" "${local_tag}"
    "-d" "${local_url}"
    "-k" '"" \xff'
  )
  add_base_args cmd_args "${cluster_file}" "${local_scratch_dir}" "backup"
  add_common_optional_args cmd_args "${blob_credentials}" "${backup_mode}" "${local_encryption_key_file}"

  if [[ "${USE_PARTITIONED_LOG:-false}" == "true" ]]; then
    cmd_args+=("--partitioned-log-experimental")
  fi

  # Start backup without -w flag to avoid hanging
  if ! "${local_build_dir}"/bin/fdbbackup start "${cmd_args[@]}"; then
    err "Start fdbbackup failed"
    return 1
  fi
  
  # Poll for backup to become restorable (STATE_RUNNING_DIFFERENTIAL)
  # BulkDump mode may take longer than traditional backups as it needs to:
  # 1. Submit a BulkDump job to the DD system
  # 2. Wait for the job to complete (which writes SST files)
  # 3. Write snapshot metadata
  local timeout=600  # 10 minutes for BulkDump jobs
  local poll_interval=5
  local elapsed=0
  
  log "Waiting for backup to become restorable (${timeout}s timeout, polling every ${poll_interval}s)..."
  
  while [[ $elapsed -lt $timeout ]]; do
    sleep $poll_interval
    elapsed=$((elapsed + poll_interval))
    
    # Check backup status using fdbbackup status command
    set +e
    status_output=$("${local_build_dir}"/bin/fdbbackup status -t "${local_tag}" -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" --log --logdir="${local_scratch_dir}" 2>&1)
    status_exit_code=$?
    set -e
    
    # Check if backup is restorable (differential state) or completed
    if echo "${status_output}" | grep -q "is restorable"; then
      log "Backup is now restorable after ${elapsed}s"
      break
    fi
    
    if echo "${status_output}" | grep -q "completed"; then
      log "Backup completed after ${elapsed}s"
      break
    fi
    
    # Log progress every 30 seconds
    if [[ $((elapsed % 30)) -eq 0 ]]; then
      log "Still waiting for backup to become restorable (${elapsed}s elapsed)..."
      # Show current state for debugging
      if echo "${status_output}" | grep -q "is restorable"; then
        log "  Status: backup is restorable (should have exited loop)"
      elif echo "${status_output}" | grep -q "in progress to"; then
        log "  Status: backup running, waiting for snapshot to complete"
      elif echo "${status_output}" | grep -q "just started"; then
        log "  Status: backup submitted, tasks starting up"
      fi
      # Check snapshot mode for debugging
      if echo "${status_output}" | grep -q "Snapshot Mode: bulkdump"; then
        log "  Snapshot Mode: bulkdump (using BulkDump for snapshots)"
      elif echo "${status_output}" | grep -q "Snapshot Mode: both"; then
        log "  Snapshot Mode: both (generating both formats)"
      fi
    fi
  done
  
  if [[ $elapsed -ge $timeout ]]; then
    err "Timeout waiting for backup to become restorable after ${timeout}s"
    log "Final status output:"
    echo "${status_output}"
    return 1
  fi
  
  # Check if backup already completed (no need to discontinue)
  if echo "${status_output}" | grep -q "completed"; then
    log "Backup already completed - no need to discontinue"
    return 0
  fi
  
  # Stop the backup to finalize it (only if still running)
  log "Stopping backup to finalize restorable state"
  set +e
  stop_output=$("${local_build_dir}"/bin/fdbbackup discontinue -t "${local_tag}" -C "${local_scratch_dir}/loopback_cluster/fdb.cluster" --log --logdir="${local_scratch_dir}" 2>&1)
  stop_exit_code=$?
  set -e
  
  if [[ $stop_exit_code -ne 0 ]]; then
    if echo "${stop_output}" | grep -q "already discontinued\|not running\|unneeded"; then
      log "Backup already completed and finalized - this is success!"
    else
      err "Failed to stop backup: ${stop_output}"
      return 1
    fi
  else
    log "Backup stopped successfully"
  fi
  
  # Brief wait for backup to finish and create final metadata
  sleep 5
  log "Backup finalized and should be restorable"
  return 0
}

# Shared restore function with optional parameters
# $1 build directory, $2 scratch directory, $3 backup url, $4 tag
# $5 encryption key file (optional)
# $6 restore mode (optional): rangefile|bulkload - controls restore mechanism
# $7 blob credentials file (optional)
function run_restore {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_tag="${4}"
  local local_encryption_key_file="${5:-}"
  local restore_mode="${6:-}"
  local blob_credentials="${7:-}"
  
  # Start restore without waiting, then monitor with timeout
  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  local cmd_args=(
    "-t" "${local_tag}"
    "-r" "${local_url}"
  )
  add_base_args cmd_args "${cluster_file}" "${local_scratch_dir}"
  add_common_optional_args cmd_args "${blob_credentials}" "${restore_mode}" "${local_encryption_key_file}"

  # Start restore without -w flag to avoid hanging
  if ! "${local_build_dir}"/bin/fdbrestore start "${cmd_args[@]}"; then
    err "Start fdbrestore failed"
    return 1
  fi
  
  # Poll for restore to complete
  # BulkLoad mode may take longer as it uses a different restoration mechanism
  local timeout=600  # 10 minutes
  local poll_interval=5
  local elapsed=0
  
  log "Waiting for restore to complete (${timeout}s timeout, polling every ${poll_interval}s)..."
  
  while [[ $elapsed -lt $timeout ]]; do
    sleep $poll_interval
    elapsed=$((elapsed + poll_interval))
    
    # Check restore status using fdbrestore status command
    set +e
    status_output=$("${local_build_dir}"/bin/fdbrestore status -t "${local_tag}" --dest-cluster-file "${local_scratch_dir}/loopback_cluster/fdb.cluster" --log --logdir="${local_scratch_dir}" 2>&1)
    status_exit_code=$?
    set -e
    
    # Check if restore completed
    # Status output contains "State: completed" or "Phase: Complete" when done
    # Also check "No restore" for when restore tag doesn't exist (completed and cleaned up)
    if echo "${status_output}" | grep -qi "State:.*completed\|Phase:.*Complete\|No restore"; then
      log "Restore completed after ${elapsed}s"
      return 0
    fi
    
    # Check if restore failed (be specific - "LastError: None" contains "Error" so avoid false positives)
    if echo "${status_output}" | grep -qi "State:.*aborted"; then
      err "Restore aborted after ${elapsed}s"
      log "Status output:"
      echo "${status_output}"
      return 1
    fi
    
    # Check for actual errors (not "LastError: None")
    if echo "${status_output}" | grep -i "LastError:" | grep -qvi "None"; then
      err "Restore has error after ${elapsed}s"
      log "Status output:"
      echo "${status_output}"
      return 1
    fi
    
    # Log progress every 30 seconds
    if [[ $((elapsed % 30)) -eq 0 ]]; then
      log "Still waiting for restore to complete (${elapsed}s elapsed)..."
      # Show phase info for debugging
      if echo "${status_output}" | grep -qi "Phase:"; then
        phase_info=$(echo "${status_output}" | grep -i "Phase:" | head -1)
        log "  ${phase_info}"
      fi
    fi
  done
  
  if [[ $elapsed -ge $timeout ]]; then
    err "Timeout waiting for restore to complete after ${timeout}s"
    log "Final status output:"
    echo "${status_output}"
    return 1
  fi
  
  return 0
}

# Test encryption mismatches - works with both S3 and file-based backups
# $1 build directory, $2 scratch directory, $3 backup url, $4 tag
# $5 encryption key file used for backup (empty if no encryption), $6 blob credentials file (optional)
function test_encryption_mismatches {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_url="${3}"
  local local_tag="${4}"
  local backup_encryption_key_file="${5}"
  local blob_credentials="${6:-}"

  local cluster_file="${local_scratch_dir}/loopback_cluster/fdb.cluster"
  
  # Create separate log directory for encryption mismatch tests
  local mismatch_logdir="${local_scratch_dir}/encryption_mismatch_logs"
  mkdir -p "${mismatch_logdir}"

  # Build base restore args once - reused for all tests
  local base_args=(
    "-t" "${local_tag}" "-w"
    "-r" "${local_url}"
  )
  add_base_args base_args "${cluster_file}" "${mismatch_logdir}"
  add_common_optional_args base_args "${blob_credentials}" "" ""

  if [[ -n "${backup_encryption_key_file}" ]]; then
    # Backup was encrypted - test mismatches
    log "Testing encryption mismatches for encrypted backup"

    # Test 1: Encrypted backup → restore without encryption (should fail)
    log "Test 1: Attempting restore without encryption on encrypted backup (should fail)"

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${base_args[@]}" 2>"${mismatch_logdir}/test1_stderr.log"
    local exit_code1=$?
    set -e

    if [[ ${exit_code1} -eq 0 ]]; then
      err "Restore without encryption on encrypted backup succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore without encryption on encrypted backup failed as expected (exit code: ${exit_code1})"

    # Test 2: Encrypted backup → restore with wrong encryption key (should fail)
    local wrong_key_file="${local_scratch_dir}/wrong_key"
    create_encryption_key_file "${wrong_key_file}"

    log "Test 2: Attempting restore with wrong encryption key (should fail)"
    # Copy base_args and add encryption key
    local cmd_args2=("${base_args[@]}" "--encryption-key-file" "${wrong_key_file}")

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${cmd_args2[@]}" 2>"${mismatch_logdir}/test2_stderr.log"
    local exit_code2=$?
    set -e

    rm -f "${wrong_key_file}"

    if [[ ${exit_code2} -eq 0 ]]; then
      err "Restore with wrong encryption key succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore with wrong encryption key failed as expected (exit code: ${exit_code2})"

  else
    # Backup was not encrypted - test mismatch
    log "Testing encryption mismatch for unencrypted backup"

    # Test: Unencrypted backup → restore with encryption (should fail)
    local any_key_file="${local_scratch_dir}/any_key"
    create_encryption_key_file "${any_key_file}"

    log "Test: Attempting restore with encryption on unencrypted backup (should fail)"
    # Copy base_args and add encryption key
    local cmd_args=("${base_args[@]}" "--encryption-key-file" "${any_key_file}")

    set +e
    "${local_build_dir}"/bin/fdbrestore start "${cmd_args[@]}" 2>"${mismatch_logdir}/test_stderr.log"
    local exit_code=$?
    set -e

    rm -f "${any_key_file}"

    if [[ ${exit_code} -eq 0 ]]; then
      err "Restore with encryption on unencrypted backup succeeded when it should have failed!"
      rm -rf "${mismatch_logdir}"
      return 1
    fi
    log "SUCCESS: Restore with encryption on unencrypted backup failed as expected (exit code: ${exit_code})"
  fi

  # Clean up separate log directory
  rm -rf "${mismatch_logdir}"

  log "All encryption mismatch tests completed successfully"
  return 0
}

# Helper function to wait for restore completion by polling status
# $1 build directory, $2 scratch directory, $3 restore tag
function run_restore_wait {
  local local_build_dir="${1}"
  local local_scratch_dir="${2}"
  local local_tag="${3}"
  
  local timeout=600
  local poll_interval=5
  local elapsed=0
  
  while [[ $elapsed -lt $timeout ]]; do
    sleep $poll_interval
    elapsed=$((elapsed + poll_interval))
    
    set +e
    local status_output
    status_output=$("${local_build_dir}"/bin/fdbrestore status -t "${local_tag}" --dest-cluster-file "${local_scratch_dir}/loopback_cluster/fdb.cluster" --log --logdir="${local_scratch_dir}" 2>&1)
    set -e
    
    if echo "${status_output}" | grep -qi "State:.*completed\|Phase:.*Complete\|No restore"; then
      return 0
    fi
    
    if echo "${status_output}" | grep -qi "State:.*aborted"; then
      return 1
    fi
    
    if echo "${status_output}" | grep -i "LastError:" | grep -qvi "None"; then
      return 1
    fi
  done
  
  return 1
}

# NOTE: setup_s3_environment and setup_tls_ca_file are defined in tests_common.sh
# They use TESTS_COMMON_DIR to reliably find aws_fixture.sh and mocks3_fixture.sh

# Setup common backup test environment and knobs
# $1 http verbose level, $2 additional knobs array (optional)
function setup_backup_test_environment {
  local http_verbose_level="${1}"
  local additional_knobs=("${@:2}")
  
  # Clear proxy environment variables
  unset HTTP_PROXY
  unset HTTPS_PROXY
  
  # Set USE_S3 based on environment
  readonly USE_S3="${USE_S3:-$( if [[ -n "${OKTETO_NAMESPACE+x}" ]]; then echo "true" ; else echo "false"; fi )}"
  
  # Set KNOBS based on whether we're using real S3 or MockS3Server
  if [[ "${USE_S3}" == "true" ]]; then
    # Use AWS KMS encryption for real S3
    KNOBS=("--knob_blobstore_encryption_type=aws:kms" "--knob_http_verbose_level=${http_verbose_level}")
  else
    # No encryption for MockS3Server
    KNOBS=("--knob_http_verbose_level=${http_verbose_level}")
  fi
  
  # Add any additional knobs
  KNOBS+=("${additional_knobs[@]}")
  readonly KNOBS
  
  setup_tls_ca_file
}

# Setup FDB cluster with backup agent - common pattern
# $1 source_dir, $2 build_dir, $3 test_scratch_dir, $4 process_count, $5+ cluster_knobs
function setup_fdb_cluster_with_backup {
  local _src_dir="${1}"
  local _bld_dir="${2}"
  local _scratch_dir="${3}"
  local _proc_count="${4:-1}"
  local _cluster_knobs=("${@:5}")

  # Source FDB cluster fixture
  if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
    err "Failed to source fdb_cluster_fixture.sh"
    exit 1
  fi
  
  # Startup fdb cluster
  if ! start_fdb_cluster "${_src_dir}" "${_bld_dir}" "${_scratch_dir}" "${_proc_count}" "${_cluster_knobs[@]}"; then
    err "Failed start FDB cluster"
    exit 1
  fi
  log "FDB cluster is up"
  
  # Start backup agent with KNOBS (set by setup_backup_test_environment)
  if ! start_backup_agent "${_bld_dir}" "${_scratch_dir}" "${KNOBS[@]}"; then
    err "Failed start backup_agent"
    exit 1
  fi
  log "Backup_agent is up"
}