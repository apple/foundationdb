#!/usr/bin/env bash
#
# This script reproduces a ThreadPool::stop() reentrancy crash triggered by
# running `suspend` from fdbcli against a real 2-region HA cluster under load.
#
# Basically here we build a small, real (non-simulated) 2-region HA cluster - a `double`
# redundancy `ssd-rocksdb-v1` cluster with 3 coordinators + 2 storage
# processes in a `primary` region, and 2 storage processes in a `remote`
# region - because a single-process or single-region cluster does not
# reliably trigger the race.
# It then runs concurrent read/write load against the cluster while repeatedly calling
# `suspend` on the remote storage processes, watching for a nonzero exit.
# Empirically, an unfixed checkout crashes within the first ~100 suspend
# cycles under load; a fixed checkout survives 1000+ cleanly.
#
# Usage:
#   FDBSERVER_BIN=/path/to/fdbserver FDBCLI_BIN=/path/to/fdbcli ./repro.sh
#
# Exit code 0 = completed $ITERATIONS suspend cycles with no crash.
# Exit code 1 = crash detected (or setup failed) - see $WORKDIR for evidence.
#
# Both `FDBSERVER_BIN` and `FDBCLI_BIN` default to `fdbserver`/`fdbcli` on
# `PATH` if not set. Recommended: build with `-DUSE_ASAN=ON` so a crash is
# unambiguous and immediately actionable (a stack trace pointing at
# `~Thread()`/`ThreadPool::stop()`), though a plain build will also abort
# via the assertion.
#
# Other environment variables (all optional):
# | Variable        | Default             | Meaning                                                                |
# |-----------------|---------------------|------------------------------------------------------------------------|
# | `WORKDIR`       | a fresh `mktemp -d` | where cluster data/logs/state are written                              |
# | `ITERATIONS`    | `1000`              | number of suspend cycles to attempt                                    |
# | `SUSPEND_WAIT`  | `1`                 | seconds passed to `suspend <seconds> ...`                              |
# | `SUSPEND_SLEEP` | `1`                 | seconds slept between suspend cycles                                   |
# | `BASE_PORT`     | `4500`              | first port used; 9 ports are used starting here (5 primary + 4 remote) |

# The script kills everything it started (via a `trap ... EXIT`) when it
# exits, whether that's a clean finish, a detected crash, or `Ctrl-C`.
# `$WORKDIR` is left behind for inspection either way - remove it yourself
# once you're done.

set -uo pipefail

FDBSERVER_BIN="${FDBSERVER_BIN:-fdbserver}"
FDBCLI_BIN="${FDBCLI_BIN:-fdbcli}"
WORKDIR="${WORKDIR:-$(mktemp -d -t ithreadpool_repro.XXXXXX)}"
ITERATIONS="${ITERATIONS:-1000}"
SUSPEND_WAIT="${SUSPEND_WAIT:-1}"
SUSPEND_SLEEP="${SUSPEND_SLEEP:-1}"
BASE_PORT="${BASE_PORT:-4500}"

mkdir -p "$WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)" # make absolute: fdbcli does not expand ~ or relative paths itself
CLUSTER_FILE="$WORKDIR/fdb.cluster"

PRIMARY_PORTS=($((BASE_PORT)) $((BASE_PORT + 1)) $((BASE_PORT + 2)) $((BASE_PORT + 3)) $((BASE_PORT + 4)))
REMOTE_PORTS=($((BASE_PORT + 100)) $((BASE_PORT + 101)) $((BASE_PORT + 102)) $((BASE_PORT + 103)))
REMOTE_STORAGE_PORTS=("${REMOTE_PORTS[2]}" "${REMOTE_PORTS[3]}")

log() { echo "[repro] $*" >&2; }

ALL_PIDS=()
cleanup() {
    log "cleaning up (workdir kept at $WORKDIR for inspection)"
    for pid in "${ALL_PIDS[@]:-}"; do
        kill -9 "$pid" >/dev/null 2>&1
    done
    # generate_load()'s own fdbcli child processes aren't individually tracked
    # in ALL_PIDS (only the shell function's PID is) - catch them, and anything
    # else pointed at this run's cluster file, by pattern instead.
    pkill -9 -f "$CLUSTER_FILE" >/dev/null 2>&1
    wait 2>/dev/null
}
trap cleanup EXIT INT TERM

start_fdbserver() {
    local port="$1" dc="$2" class="${3:-}"
    mkdir -p "$WORKDIR/data_$port" "$WORKDIR/logs_$port"
    local args=(-p "127.0.0.1:$port" -C "$CLUSTER_FILE" -d "$WORKDIR/data_$port" -L "$WORKDIR/logs_$port"
        --listen_address "127.0.0.1:$port" --datacenter-id "$dc" --machine-id "zone-$port")
    [ -n "$class" ] && args+=(--class "$class")
    "$FDBSERVER_BIN" "${args[@]}" >>"$WORKDIR/stdout_$port.log" 2>>"$WORKDIR/stderr_$port.log" &
    ALL_PIDS+=($!)
}

# Loops one fdbserver on $2 (storage class, "remote" dc) forever, restarting
# it on a clean exit (that is what fdbcli> suspend produces on a real
# cluster). Records $port to $WORKDIR/CRASHED and returns on any nonzero exit.
supervise_remote_storage() {
    local port="$1"
    while true; do
        mkdir -p "$WORKDIR/data_$port" "$WORKDIR/logs_$port"
        "$FDBSERVER_BIN" -p "127.0.0.1:$port" -C "$CLUSTER_FILE" -d "$WORKDIR/data_$port" -L "$WORKDIR/logs_$port" \
            --listen_address "127.0.0.1:$port" --datacenter-id remote --machine-id "zone-$port" --class storage \
            >>"$WORKDIR/stdout_$port.log" 2>>"$WORKDIR/stderr_$port.log"
        local code=$?
        if [ "$code" -ne 0 ]; then
            echo "$port exited with code $code" >>"$WORKDIR/CRASHED"
            log "CRASH: port $port exited with code $code - see $WORKDIR/stderr_$port.log and $WORKDIR/logs_$port/"
            return
        fi
        sleep 1
    done
}

# Continuous read/write load so the storage engine's background reader/
# writer threads have real work in flight when suspend lands.
generate_load() {
    local i=$RANDOM
    while true; do
        {
            echo "writemode on"
            for j in $(seq 1 500); do
                i=$((i + 1))
                echo "set loadkey$i loadvalue$i"
                [ $((j % 3)) -eq 0 ] && echo "get loadkey$i"
            done
        } | "$FDBCLI_BIN" -C "$CLUSTER_FILE" >>"$WORKDIR/load.log" 2>&1
    done
}

# Polls `status details` until the replication-health line ends exactly in
# "Healthy" (not "Healthy (Repartitioning)"/"(Re)initializing...") on two
# consecutive checks a few seconds apart - a single momentary "Healthy" can
# appear right as a config change takes effect, before data distribution
# has actually started the work that change implies, so one match alone is
# not reliable enough to safely proceed past.
wait_healthy() {
    local timeout="${1:-180}" start consecutive=0
    start=$(date +%s)
    while true; do
        if "$FDBCLI_BIN" -C "$CLUSTER_FILE" --exec "status details" 2>/dev/null |
            grep -qE 'Replication health\s+-\s+Healthy$'; then
            consecutive=$((consecutive + 1))
            [ "$consecutive" -ge 2 ] && return 0
        else
            consecutive=0
        fi
        if [ $(($(date +%s) - start)) -ge "$timeout" ]; then
            log "timed out after ${timeout}s waiting for Replication health: Healthy (stably)"
            return 1
        fi
        sleep 5
    done
}

fileconfigure() {
    local json_path="$1"
    "$FDBCLI_BIN" -C "$CLUSTER_FILE" --exec "fileconfigure $json_path"
}

main() {
    log "workdir: $WORKDIR"
    echo "repro:reprocluster1@127.0.0.1:${PRIMARY_PORTS[0]},127.0.0.1:${PRIMARY_PORTS[1]},127.0.0.1:${PRIMARY_PORTS[2]}" >"$CLUSTER_FILE"

    log "starting primary region (3 coordinators + 2 storage) and the remote region's 2 generic processes"
    start_fdbserver "${PRIMARY_PORTS[0]}" primary
    start_fdbserver "${PRIMARY_PORTS[1]}" primary
    start_fdbserver "${PRIMARY_PORTS[2]}" primary
    start_fdbserver "${PRIMARY_PORTS[3]}" primary storage
    start_fdbserver "${PRIMARY_PORTS[4]}" primary storage
    start_fdbserver "${REMOTE_PORTS[0]}" remote
    start_fdbserver "${REMOTE_PORTS[1]}" remote
    sleep 5

    log "bootstrapping: double redundancy, ssd-rocksdb-v1"
    "$FDBCLI_BIN" -C "$CLUSTER_FILE" --exec "configure new double ssd-rocksdb-v1" || {
        log "initial configure failed"
        exit 1
    }

    cat >"$WORKDIR/region_apply.json" <<EOF
{
  "regions": [
    {"datacenters": [{"id": "primary", "priority": 1}]},
    {"datacenters": [{"id": "remote", "priority": -1}]}
  ],
  "log_routers": 1,
  "remote_logs": 1
}
EOF
    cat >"$WORKDIR/region_activate.json" <<EOF
{
  "regions": [
    {"datacenters": [{"id": "primary", "priority": 1}]},
    {"datacenters": [{"id": "remote", "priority": 0}]}
  ]
}
EOF

    log "starting remote storage processes (${REMOTE_STORAGE_PORTS[*]})"
    start_fdbserver "${REMOTE_STORAGE_PORTS[0]}" remote storage
    start_fdbserver "${REMOTE_STORAGE_PORTS[1]}" remote storage
    sleep 3

    log "applying region config (remote priority -1)"
    fileconfigure "$WORKDIR/region_apply.json" || exit 1
    wait_healthy || exit 1

    log "enabling usable_regions=2"
    "$FDBCLI_BIN" -C "$CLUSTER_FILE" --exec "configure usable_regions=2" || exit 1
    wait_healthy || exit 1

    log "promoting remote to priority 0 (fully active)"
    fileconfigure "$WORKDIR/region_activate.json" || exit 1
    wait_healthy || exit 1

    log "cluster is up: $($FDBCLI_BIN -C "$CLUSTER_FILE" --exec 'status minimal' 2>&1 | tail -1)"

    log "restarting remote storage under supervision + starting load generators"
    kill -9 $(pgrep -f "127.0.0.1:${REMOTE_STORAGE_PORTS[0]}" || true) >/dev/null 2>&1
    kill -9 $(pgrep -f "127.0.0.1:${REMOTE_STORAGE_PORTS[1]}" || true) >/dev/null 2>&1
    sleep 2
    supervise_remote_storage "${REMOTE_STORAGE_PORTS[0]}" &
    ALL_PIDS+=($!)
    supervise_remote_storage "${REMOTE_STORAGE_PORTS[1]}" &
    ALL_PIDS+=($!)
    for _ in 1 2 3 4 5 6; do
        generate_load &
        ALL_PIDS+=($!)
    done
    sleep 5

    log "hammering suspend against 127.0.0.1:${REMOTE_STORAGE_PORTS[0]} and :${REMOTE_STORAGE_PORTS[1]} for up to $ITERATIONS iterations"
    for ((i = 1; i <= ITERATIONS; i++)); do
        if [ -f "$WORKDIR/CRASHED" ]; then
            log "REPRODUCED after $i attempts: $(cat "$WORKDIR/CRASHED")"
            exit 1
        fi
        printf 'suspend\nsuspend %s 127.0.0.1:%s 127.0.0.1:%s\n' \
            "$SUSPEND_WAIT" "${REMOTE_STORAGE_PORTS[0]}" "${REMOTE_STORAGE_PORTS[1]}" |
            timeout 10 "$FDBCLI_BIN" -C "$CLUSTER_FILE" >>"$WORKDIR/hammer_fdbcli.log" 2>&1
        if [ $((i % 25)) -eq 0 ]; then
            log "$i/$ITERATIONS attempts, no crash yet"
        fi
        sleep "$SUSPEND_SLEEP"
    done

    if [ -f "$WORKDIR/CRASHED" ]; then
        log "REPRODUCED after $ITERATIONS attempts: $(cat "$WORKDIR/CRASHED")"
        exit 1
    fi
    log "PASS: completed $ITERATIONS suspend cycles with no crash"
    exit 0
}

main
