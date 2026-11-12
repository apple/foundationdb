#!/usr/bin/env bash
#
# s3_backup_status_bad_url_test.sh
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
# Test that one backup whose saved URL the agent cannot parse does not drop
# every backup from 'status json' .cluster.layers.backup.
#
# 1. Starts MockS3Server, a cluster, and one backup_agent at default knobs
#    (except a short status delay).
# 2. Starts continuous backup A with a good URL and waits for it to show as
#    running.
# 3. Starts encrypted backup B with --knob_http_request_aws_v4_header=0 and a
#    URL without region=. The agent runs with v4 signing on, so it cannot open
#    B's container ("region is required for aws v4 signature").
# 4. Polls 'status json' until tag A is running and tag B has an error.
#    The wait outlasts the 120s expiry of the agent's last good status
#    document, so an agent that stops publishing loses tag A as well.
# 5. Aborts B and polls until tag A is running and tag B is aborted and still
#    has an error, since status opens the containers of finished backups too.
#
# Runs only against MockS3Server, bound to 127.0.0.2. We use this IP address
# because we don't want the client correctly guessing the region (which it
# would for 127.0.0.1, any localhost, and any S3 domains). This is to induce
# the parsing of the URL failing (missing region= param).
#
# Usage:
#   s3_backup_status_bad_url_test.sh <source_dir> <build_dir> [scratch_dir]

trap "exit 1" HUP INT PIPE QUIT TERM
trap cleanup EXIT

function cleanup {
  if cleanup_with_preserve_check; then
    return 0
  fi
  if type shutdown_fdb_cluster &> /dev/null; then
    shutdown_fdb_cluster
  fi
  if type shutdown_mocks3 &> /dev/null; then
    shutdown_mocks3
  fi
  if [[ -n "${TEST_SCRATCH_DIR:-}" ]] && [[ -d "${TEST_SCRATCH_DIR}" ]]; then
    rm -rf "${TEST_SCRATCH_DIR}"
  fi
}

# Resolve passed in reference to an absolute path.
# $1 path to resolve
function resolve_to_absolute_path {
  local p="${1}"
  while [[ -h "${p}" ]]; do
    dir=$( cd -P "$( dirname "${p}" )" >/dev/null 2>&1 && pwd )
    p=$(readlink "${p}")
    [[ ${p} != /* ]] && p="${dir}/${p}"
  done
  realpath "${p}"
}

# Print .cluster.layers.backup from 'status json', or nothing on failure.
function layers_backup_json {
  "${build_dir}/bin/fdbcli" -C "${cluster_file}" --timeout 10 --exec 'status json' 2>/dev/null |
    jq -c '.cluster.layers.backup // empty' 2>/dev/null || true
}

# Succeeds when tag $2 in layers.backup JSON $1 is running.
function tag_is_running {
  jq -e --arg t "${2}" '.tags[$t].running_backup == true' <<< "${1}" &> /dev/null
}

# Starts a backup without waiting for it.
# $1 tag, $2 url, $3+ extra fdbbackup args
function start_backup {
  local tag="${1}"
  local url="${2}"
  shift 2
  "${build_dir}/bin/fdbbackup" start -C "${cluster_file}" -t "${tag}" -d "${url}" \
    --blob-credentials "${blob_credentials_file}" \
    --log --logdir="${TEST_SCRATCH_DIR}" "${KNOBS[@]}" "$@"
}

# Poll 'status json' until $1 (a predicate on the layers.backup JSON) holds.
# $1 predicate function, $2 timeout in seconds
# Leaves the last layers.backup JSON in LAST_LAYERS.
LAST_LAYERS=
function wait_for_layers {
  local predicate="${1}"
  local timeout="${2}"
  local start=${SECONDS}
  while (( SECONDS - start < timeout )); do
    LAST_LAYERS=$(layers_backup_json)
    if [[ -n "${LAST_LAYERS}" ]] && "${predicate}" "${LAST_LAYERS}"; then
      return 0
    fi
    sleep 2
  done
  return 1
}

function a_is_running {
  tag_is_running "${1}" "${TAG_A}"
}

function a_running_and_b_has_error {
  tag_is_running "${1}" "${TAG_A}" &&
    jq -e --arg t "${TAG_B}" '(.tags[$t].error // "") | length > 0' <<< "${1}" &> /dev/null
}

# An aborted backup's container is still opened, so it keeps reporting the error.
function a_running_and_b_aborted {
  tag_is_running "${1}" "${TAG_A}" &&
    jq -e --arg t "${TAG_B}" '.tags[$t].current_status == "has been aborted" and ((.tags[$t].error // "") | length > 0)' \
      <<< "${1}" &> /dev/null
}

# Explain why LAST_LAYERS doesn't satisfy the wait that just timed out.
# $1 what was expected of tag B
function report_layers_failure {
  if [[ -z "${LAST_LAYERS}" ]]; then
    err "status json has no .cluster.layers.backup: the agent stopped publishing status"
  elif ! jq -e --arg t "${TAG_A}" '.tags | has($t)' <<< "${LAST_LAYERS}" &> /dev/null; then
    err "Tag ${TAG_A} is missing from .cluster.layers.backup.tags"
  elif ! tag_is_running "${LAST_LAYERS}" "${TAG_A}"; then
    err "Tag ${TAG_A} is not running in .cluster.layers.backup.tags"
  elif ! jq -e --arg t "${TAG_B}" '.tags | has($t)' <<< "${LAST_LAYERS}" &> /dev/null; then
    err "Tag ${TAG_B} is missing from .cluster.layers.backup.tags: the agent stopped publishing status"
  else
    err "Tag ${TAG_B} is not ${1} in .cluster.layers.backup.tags"
  fi
  err "layers.backup.tags: $(jq -c '.tags' <<< "${LAST_LAYERS:-null}")"
  grep -h -o 'Type="LayerBackupStatusUpdateError"[^>]*' "${TEST_SCRATCH_DIR}"/trace.*.xml 2>/dev/null | tail -1 >&2 || true
}

function test_status_with_bad_url {
  log "Start backup ${TAG_A} with a good URL"
  if ! start_backup "${TAG_A}" "${good_url}" -z; then
    err "Failed to start backup ${TAG_A}"
    return 1
  fi
  if ! wait_for_layers a_is_running 120; then
    err "Backup ${TAG_A} never showed as running in status json before backup ${TAG_B} existed"
    err "layers.backup: ${LAST_LAYERS:-<missing>}"
    return 1
  fi
  log "Backup ${TAG_A} is running in status json"

  local key_file="${TEST_SCRATCH_DIR}/${TAG_B}.key"
  create_encryption_key_file "${key_file}"
  log "Start encrypted backup ${TAG_B} with v4 signing off and no region in the URL"
  if ! start_backup "${TAG_B}" "${bad_url}" "--knob_http_request_aws_v4_header=0" \
      --encryption-key-file "${key_file}"; then
    err "Failed to start backup ${TAG_B}; cannot save a URL the agent rejects"
    return 1
  fi
  log "Wait for status json to show ${TAG_A} running and ${TAG_B} with an error"
  if ! wait_for_layers a_running_and_b_has_error 180; then
    report_layers_failure "reporting an error"
    return 1
  fi
  log "Tag ${TAG_B} error: $(jq -r --arg t "${TAG_B}" '.tags[$t].error' <<< "${LAST_LAYERS}")"

  log "Abort backup ${TAG_B}"
  if ! "${build_dir}/bin/fdbbackup" abort -C "${cluster_file}" -t "${TAG_B}" \
      --log --logdir="${TEST_SCRATCH_DIR}"; then
    err "Failed to abort backup ${TAG_B}"
    return 1
  fi

  log "Wait for status json to show ${TAG_A} running and ${TAG_B} aborted"
  if ! wait_for_layers a_running_and_b_aborted 180; then
    report_layers_failure "aborted with an error"
    return 1
  fi
  return 0
}

set -o errexit
set -o nounset
set -o pipefail
set -o noclobber

if ! path=$(resolve_to_absolute_path "${BASH_SOURCE[0]}"); then
  echo "Failed resolve_to_absolute_path" >&2
  exit 1
fi
if ! cwd=$( cd -P "$( dirname "${path}" )" >/dev/null 2>&1 && pwd ); then
  echo "Failed dirname on ${path}" >&2
  exit 1
fi
readonly cwd

# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/tests_common.sh"; then
  echo "Failed to source tests_common.sh" >&2
  exit 1
fi
# shellcheck source=/dev/null
if ! source "${cwd}/backup_tests_common.sh"; then
  echo "Failed to source backup_tests_common.sh" >&2
  exit 1
fi

# macOS only puts 127.0.0.1 on loopback, so MockS3Server can't bind 127.0.0.2.
# 66 is this test's SKIP_RETURN_CODE in fdbbackup/CMakeLists.txt.
if [[ "${OSTYPE}" == "darwin"* ]]; then
  log "Skipping: needs 127.0.0.2 on loopback, which macOS doesn't have"
  exit 66
fi

if (( $# < 2 )) || (( $# > 3 )); then
  echo "ERROR: ${0} requires the fdb src and build directories --"
  echo "CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR -- and then, optionally,"
  echo "a directory into which we write scratch test data and logs"
  echo "(otherwise we will write to subdirs under \$TMPDIR)."
  echo "Example: ${0} ./foundationdb ./build_output ./scratch_dir"
  exit 1
fi
if ! source_dir=$(is_fdb_source_dir "${1}"); then
  err "${1} is not an fdb source directory"
  exit 1
fi
readonly source_dir
readonly build_dir="${2}"
if [[ ! -d "${build_dir}" ]]; then
  err "${build_dir} is not a directory"
  exit 1
fi
scratch_dir="${TMPDIR:-/tmp}"
if (( $# == 3 )); then
  scratch_dir="${3}"
fi
readonly scratch_dir

TEST_SCRATCH_DIR=
readonly TAG_A="status_good_url"
readonly TAG_B="status_bad_url"

USE_S3=false
export MOCKS3_HOST=127.0.0.2
unset GCS_FDB_BUCKET GCS_APPLICATION_TOKEN
setup_backup_test_environment 1
setup_s3_environment "${build_dir}" "${scratch_dir}" "mocks3_backup_status_bad_url"
readonly cluster_file="${TEST_SCRATCH_DIR}/loopback_cluster/fdb.cluster"
readonly good_url="blobstore://${host}/ctests/$$/status_good_url?${query_str}"
readonly bad_url="blobstore://${host}/ctests/$$/status_bad_url?bucket=${bucket}&secure_connection=0"

# shellcheck source=/dev/null
if ! source "${cwd}/../../fdbclient/tests/fdb_cluster_fixture.sh"; then
  err "Failed to source fdb_cluster_fixture.sh"
  exit 1
fi
if ! start_fdb_cluster "${source_dir}" "${build_dir}" "${TEST_SCRATCH_DIR}" 1; then
  err "Failed start FDB cluster"
  exit 1
fi
log "FDB cluster is up"
if ! start_backup_agent "${build_dir}" "${TEST_SCRATCH_DIR}" "${KNOBS[@]}" "--knob_backup_status_delay=2"; then
  err "Failed start backup_agent"
  exit 1
fi
log "Backup_agent is up"

rc=0
test_status_with_bad_url || rc=$?
log_test_result "${rc}" "test_status_with_bad_url"
exit "${rc}"
