#!/bin/bash
#
# mako_storage_bench.sh - single-host mako throughput benchmark for catching
# storage-server CPU regressions.
#
# Boots a minimal loopback cluster with exactly one stateless, one log, and one
# storage process (so the storage role is isolated in its own process and its
# CPU is the bottleneck), then runs a fixed mako build + run workload against it.
#
# Purpose: A/B comparison of experimental FDB changes that execute in the
# storage server -- not just storage-engine code, but any FDB software
# infrastructure exercised on the storage server's path (flow, fdbrpc,
# networking, serialization, common libraries, etc.). Run it once on a baseline
# build and once on the experimental build to confirm the change does not add
# significant CPU cost or introduce latency. The two runs must be done on the
# same machine/instance, back to back (results are only comparable under
# identical hardware/conditions). Because the storage role is pinned to a
# single process and effectively a single core, any CPU **overhead** the change
# adds -- extra work on the hot path -- competes for that one core and will
# typically show up as a drop in throughput rather than as obvious latency.
# Watch the TPS delta.
#
# Background: the default workload (ssd-redwood-1 single; 1/1/1 server roles;
# build -p 4; run -p 7 -t 4 --async_xacts 128 --keylen 128 --vallen 512;
# -x g18ui = 18 GETs + 1 update + 1 insert per txn; 300s warmup, 600s run) comes
# from a config shared in the FDB working group and used across teams. On a
# ~32-core single host it produces very low run-to-run noise and has been a
# reliable detector of storage-server performance regressions. The counts are
# tuned for that host size: on a much smaller or larger machine, adjust the
# server/client process counts (env-overridable below) so the storage process
# stays the bottleneck and the clients can keep it saturated without starving it.
#
# Unlike contrib/generate_profile.sh this uses plain Release binaries: no LLVM
# profile instrumentation, no LLVM_PROFILE_FILE, no llvm-profdata merge. Those
# distort hot-path CPU and would invalidate throughput numbers. Use an
# uninstrumented (Release) build here.
#
# Process placement is left to the Linux scheduler on purpose -- the storage
# process is expected to be CPU-hungry and the clients comparatively idle, so no
# taskset pinning is applied.

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly RUN_CLUSTER="${REPO_ROOT}/tests/loopback_cluster/run_custom_cluster.sh"

# --- Fixed workload (override via env if needed) ----------------------------
# Data spec, shared by build and run.
readonly ROWS="${ROWS:-100000}"
readonly KEYLEN="${KEYLEN:-128}"
readonly VALLEN="${VALLEN:-512}"
# Build phase: parallelism only.
readonly BUILD_PROCS="${BUILD_PROCS:-4}"
# Run phase.
readonly RUN_PROCS="${RUN_PROCS:-7}"
readonly RUN_THREADS="${RUN_THREADS:-4}"
readonly ASYNC_XACTS="${ASYNC_XACTS:-128}"
readonly TRANSACTION="${TRANSACTION:-g18ui}"   # 18 GETs + 1 update + 1 insert
readonly WARMUP_SECONDS="${WARMUP_SECONDS:-300}"
readonly SECONDS_RUN="${SECONDS_RUN:-600}"
# Bound the build phase so it fails fast instead of hanging if the database
# never serves. (Does not cover a hang during cluster startup/configure.)
readonly BUILD_TIMEOUT="${BUILD_TIMEOUT:-180}"

# Where clusters and mako output live. Prefer a RAM disk at /mnt/ram when one is
# present and writable, so the storage role is CPU-bound rather than disk-IO-
# bound; otherwise fall back to the current directory.
if [[ -z "${WORKDIR:-}" && -w /mnt/ram ]]; then
  WORKDIR=/mnt/ram/mako_storage_bench
fi
readonly WORKDIR="${WORKDIR:-${PWD}/mako_storage_bench}"

# Extra fdbserver knobs, forwarded to run_custom_cluster.sh --knobs. A tmpfs
# WORKDIR rejects fdbserver's default O_DIRECT path, so when WORKDIR is on tmpfs
# add the EIO-fallback knob automatically. (rocksdb needs more knobs on tmpfs --
# see mako_storage_bench-ramdisk-okteto.md.)
KNOBS="${KNOBS:-}"
if [[ "$(stat -f -c %T "$(dirname "${WORKDIR}")" 2>/dev/null)" == tmpfs \
      && "${KNOBS}" != *disable_posix_kernel_aio* ]]; then
  KNOBS="${KNOBS:+${KNOBS} }--knob_disable_posix_kernel_aio=1"
fi
readonly KNOBS

CLUSTER_PIDS=""

function usage {
  cat >&2 <<EOF
Usage: ${0##*/} <build-root> [engine ...]

  build-root   FDB build directory containing bin/{fdbserver,fdbcli,mako}.
               mako is NOT built by default -- configure the build with
               -D BUILD_MAKO=ON (else bin/mako will be missing here).
  engine       One or more of: redwood rocksdb sharded-rocksdb all
               'all' = redwood + rocksdb (sharded-rocksdb is not included).
               Default: redwood

Engines map to 'configure new <type> single':
  redwood          -> ssd-redwood-1
  rocksdb          -> ssd-rocksdb-v1        (needs a RocksDB-enabled build)
  sharded-rocksdb  -> ssd-sharded-rocksdb   (needs a RocksDB-enabled build)

Workload (override via env): ROWS=${ROWS} KEYLEN=${KEYLEN} VALLEN=${VALLEN}
  build: -p ${BUILD_PROCS}
  run:   -p ${RUN_PROCS} -t ${RUN_THREADS} --async_xacts ${ASYNC_XACTS} \\
         -x ${TRANSACTION} --warmup_seconds ${WARMUP_SECONDS} --seconds ${SECONDS_RUN}

Example:
  ${0##*/} /data/build_output redwood rocksdb
EOF
  exit 1
}

function err { echo "$(date -Iseconds) ERROR: $*" >&2; }

# Map a friendly engine name to the storage type understood by 'configure new'.
function storage_type_for {
  case "${1}" in
    redwood)         echo "ssd-redwood-1" ;;
    rocksdb)         echo "ssd-rocksdb-v1" ;;
    sharded-rocksdb) echo "ssd-sharded-rocksdb" ;;
    *) err "unknown engine '${1}'"; usage ;;
  esac
}

# Kill whatever the current run_custom_cluster.sh invocation started.
function teardown_cluster {
  if [[ -n "${CLUSTER_PIDS}" ]]; then
    # shellcheck disable=SC2086
    kill -9 ${CLUSTER_PIDS} 2>/dev/null || true
    CLUSTER_PIDS=""
  fi
}
trap teardown_cluster EXIT

# Run the full build + run benchmark for a single storage engine.
function bench_engine {
  local label="${1}"
  local storage_type
  storage_type="$(storage_type_for "${label}")"

  local engine_dir="${WORKDIR}/${label}"
  local cluster_file="${engine_dir}/loopback-cluster/fdb.cluster"
  mkdir -p "${engine_dir}"

  echo "============================================================"
  echo "Engine: ${label} (${storage_type} single)"
  echo "============================================================"

  # Start a 1 stateless / 1 log / 1 storage cluster, reading PIDS back from a
  # file. Redirect the launcher to a file rather than capturing with $(...): it
  # backgrounds long-lived fdbservers that would hold the $(...) pipe open and
  # deadlock us.
  local start_log="${engine_dir}/cluster-start.log"
  local knob_opt=()
  [[ -n "${KNOBS}" ]] && knob_opt=(--knobs "${KNOBS}")
  LOOPBACK_DIR="${engine_dir}/loopback-cluster" "${RUN_CLUSTER}" "${BUILD}" \
    "${knob_opt[@]}" \
    --stateless_count 1 --logs_count 1 --storage_count 1 \
    --replication_count 1 \
    --storage_type "${storage_type}" \
    --dump_pids on >"${start_log}" 2>&1
  cat "${start_log}"
  CLUSTER_PIDS="$(sed -n 's/^PIDS=//p' "${start_log}")"

  # mako does all its work but SIGABRTs in _dl_fini at exit (libfdb_c shared-lib
  # teardown), so judge the build by its completed report, not the exit code.
  echo "--- ${label}: mako build ---"
  timeout "${BUILD_TIMEOUT}" "${MAKO}" --mode build --cluster "${cluster_file}" \
    --rows "${ROWS}" -p "${BUILD_PROCS}" \
    --keylen "${KEYLEN}" --vallen "${VALLEN}" \
    2>&1 | tee "${engine_dir}/mako-build.txt" || true
  if ! grep -q "Overall TPS" "${engine_dir}/mako-build.txt"; then
    err "mako build for ${label} (${storage_type}) did not complete within ${BUILD_TIMEOUT}s."
    case "${label}" in
      rocksdb|sharded-rocksdb)
        err "  ${storage_type} needs a RocksDB-enabled build -- is RocksDB compiled into this build?" ;;
    esac
    teardown_cluster
    return 1
  fi

  echo "--- ${label}: mako run ---"
  # Also emit structured JSON alongside the teed report: per-second samples +
  # final stats (--json_report) and the raw latency sketch (--stats_export_path).
  "${MAKO}" --mode run --cluster "${cluster_file}" \
    --rows "${ROWS}" -x "${TRANSACTION}" \
    -p "${RUN_PROCS}" -t "${RUN_THREADS}" --async_xacts "${ASYNC_XACTS}" \
    --keylen "${KEYLEN}" --vallen "${VALLEN}" \
    --warmup_seconds "${WARMUP_SECONDS}" --seconds "${SECONDS_RUN}" \
    --json_report "${engine_dir}/mako.json" \
    --stats_export_path "${engine_dir}/mako-sketch.json" \
    2>&1 | tee "${engine_dir}/mako-run.txt"

  teardown_cluster
  echo "--- ${label}: done (output in ${engine_dir}/mako-run.txt) ---"
}

# --- main -------------------------------------------------------------------
if (( $# < 1 )); then usage; fi
if [[ "${1}" == "-h" || "${1}" == "--help" ]]; then usage; fi

BUILD_ARG="${1}"; shift
# Resolve to an absolute path so LD_LIBRARY_PATH (set below) and the binary
# paths are independent of the caller's working directory.
BUILD="$(cd "${BUILD_ARG}" 2>/dev/null && pwd)" \
  || { err "build root '${BUILD_ARG}' not found or not a directory"; usage; }
FDBCLI="${BUILD}/bin/fdbcli"
MAKO="${BUILD}/bin/mako"
for bin in "${BUILD}/bin/fdbserver" "${FDBCLI}" "${MAKO}"; do
  if [[ ! -x "${bin}" ]]; then err "${bin} not found or not executable"; usage; fi
done
if [[ ! -x "${RUN_CLUSTER}" ]]; then
  err "${RUN_CLUSTER} not found"; exit 1
fi

# mako has no rpath (SKIP_BUILD_RPATH), so point it at this build's libfdb_c.so
# instead of a mismatched system one. LD_BIND_NOW avoids a lazy symbol-resolution
# crash in the dynamic linker on mako's first call.
export LD_LIBRARY_PATH="${BUILD}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export LD_BIND_NOW=1

# Suppress the core dump from mako's harmless _dl_fini abort at exit.
ulimit -c 0

# Resolve the engine list (default: redwood; 'all' expands to every engine).
ENGINES=("$@")
if (( ${#ENGINES[@]} == 0 )); then
  ENGINES=(redwood)
elif [[ "${ENGINES[*]}" == *all* ]]; then
  # 'all' covers the common engines; sharded-rocksdb must be requested explicitly.
  ENGINES=(redwood rocksdb)
fi

mkdir -p "${WORKDIR}"
echo "WORKDIR=${WORKDIR}${KNOBS:+   KNOBS=${KNOBS}}"
status=0
for engine in "${ENGINES[@]}"; do
  bench_engine "${engine}" || status=1
done
exit "${status}"
