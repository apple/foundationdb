#!/usr/bin/env bash
#
# metadata-audit.sh — Wrapper for FDB metadata audit/backup/restore tools.
#
# Automatically locates libfdb_c and the fdb Python module, then dispatches
# to the appropriate Python script.
#
# Usage:
#   ./metadata-audit.sh check -C fdb.cluster
#   ./metadata-audit.sh backup -C fdb.cluster --output-dir /tmp/backup
#   ./metadata-audit.sh restore --backup-dir /tmp/backup --dry-run -C fdb.cluster
#
#   # With explicit paths:
#   ./metadata-audit.sh --fdb-lib /opt/foundationdb/lib \
#                       --fdb-python ~/build_output/bindings/python \
#                       check -C fdb.cluster
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] COMMAND [ARGS...]

Commands:
  check       Run corruption diagnostics (check_krm_corruption.py)
  backup      Backup metadata to JSON (backup_metadata.py)
  restore     Restore metadata from JSON backup (restore_metadata.py)

Options (must come before COMMAND):
  --fdb-lib PATH      Directory containing libfdb_c.so/dylib
  --fdb-python PATH   Directory containing the fdb Python package
  -h, --help          Show this help

Environment variables:
  FDB_LIB_PATH        Same as --fdb-lib
  FDB_PYTHON_PATH     Same as --fdb-python

EOF
    exit "${1:-0}"
}

# --- Parse wrapper options (before command) ---

FDB_LIB=""
FDB_PYTHON=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fdb-lib)
            FDB_LIB="$2"; shift 2 ;;
        --fdb-python)
            FDB_PYTHON="$2"; shift 2 ;;
        -h|--help)
            usage 0 ;;
        -*)
            echo "Unknown option: $1 (options must come before the command)" >&2
            usage 1 ;;
        *)
            break ;;
    esac
done

if [[ $# -lt 1 ]]; then
    echo "Error: no command specified" >&2
    usage 1
fi

COMMAND="$1"; shift

# --- Map command to script ---

case "$COMMAND" in
    check)
        SCRIPT="$SCRIPT_DIR/check_krm_corruption.py" ;;
    backup)
        SCRIPT="$SCRIPT_DIR/backup_metadata.py" ;;
    restore)
        SCRIPT="$SCRIPT_DIR/restore_metadata.py" ;;
    *)
        echo "Unknown command: $COMMAND" >&2
        usage 1 ;;
esac

# --- Locate libfdb_c ---

find_fdb_lib() {
    local lib_name
    if [[ "$(uname)" == "Darwin" ]]; then
        lib_name="libfdb_c.dylib"
    else
        lib_name="libfdb_c.so"
    fi

    # 1. Explicit --fdb-lib flag
    if [[ -n "$FDB_LIB" ]]; then
        if [[ -f "$FDB_LIB/$lib_name" ]]; then
            echo "$FDB_LIB"; return 0
        fi
        echo "Warning: --fdb-lib '$FDB_LIB' does not contain $lib_name" >&2
    fi

    # 2. FDB_LIB_PATH env var
    if [[ -n "${FDB_LIB_PATH:-}" ]] && [[ -f "$FDB_LIB_PATH/$lib_name" ]]; then
        echo "$FDB_LIB_PATH"; return 0
    fi

    # 3. Already in LD_LIBRARY_PATH / DYLD_LIBRARY_PATH
    local search_path="${LD_LIBRARY_PATH:-}:${DYLD_LIBRARY_PATH:-}"
    IFS=':' read -ra dirs <<< "$search_path"
    for dir in "${dirs[@]}"; do
        if [[ -n "$dir" ]] && [[ -f "$dir/$lib_name" ]]; then
            echo "$dir"; return 0
        fi
    done

    # 4. Common build output locations
    for candidate in \
        "$REPO_ROOT/build_output/lib" \
        "$HOME/build_output/lib" \
        "/usr/lib" \
        "/usr/local/lib"; do
        if [[ -f "$candidate/$lib_name" ]]; then
            echo "$candidate"; return 0
        fi
    done

    return 1
}

FDB_LIB_DIR=$(find_fdb_lib) || {
    if [[ "$(uname)" == "Darwin" ]]; then _lib_name="libfdb_c.dylib"; else _lib_name="libfdb_c.so"; fi
    echo "Error: Cannot find $_lib_name" >&2
    echo "" >&2
    echo "Searched in:" >&2
    [[ -n "${FDB_LIB_PATH:-}" ]] && echo "  FDB_LIB_PATH=$FDB_LIB_PATH" >&2
    [[ -n "${LD_LIBRARY_PATH:-}" ]] && echo "  LD_LIBRARY_PATH=$LD_LIBRARY_PATH" >&2
    [[ -n "${DYLD_LIBRARY_PATH:-}" ]] && echo "  DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH" >&2
    echo "  $REPO_ROOT/build_output/lib/" >&2
    echo "  $HOME/build_output/lib/" >&2
    echo "  /usr/lib/" >&2
    echo "  /usr/local/lib/" >&2
    echo "" >&2
    echo "Fix: specify the directory containing $_lib_name:" >&2
    echo "  ./metadata-audit.sh --fdb-lib /path/to/lib check ..." >&2
    echo "  export FDB_LIB_PATH=/path/to/lib" >&2
    exit 1
}

# Export library path
if [[ "$(uname)" == "Darwin" ]]; then
    export DYLD_LIBRARY_PATH="${FDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
else
    export LD_LIBRARY_PATH="${FDB_LIB_DIR}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# --- Locate fdb Python module ---

find_fdb_python() {
    # 1. Explicit --fdb-python flag
    if [[ -n "$FDB_PYTHON" ]]; then
        if [[ -d "$FDB_PYTHON/fdb" ]] || [[ -f "$FDB_PYTHON/fdb/__init__.py" ]]; then
            echo "$FDB_PYTHON"; return 0
        fi
        echo "Warning: --fdb-python '$FDB_PYTHON' does not contain fdb/ package" >&2
    fi

    # 2. FDB_PYTHON_PATH env var
    if [[ -n "${FDB_PYTHON_PATH:-}" ]]; then
        if [[ -d "$FDB_PYTHON_PATH/fdb" ]]; then
            echo "$FDB_PYTHON_PATH"; return 0
        fi
    fi

    # 3. Already importable
    if python3 -c "import fdb" 2>/dev/null; then
        echo ""; return 0  # empty means no extra path needed
    fi

    # 4. Common build output locations
    for candidate in \
        "$REPO_ROOT/build_output/bindings/python" \
        "$HOME/build_output/bindings/python" \
        "$SCRIPT_DIR"; do
        if [[ -d "$candidate/fdb" ]]; then
            echo "$candidate"; return 0
        fi
    done

    return 1
}

FDB_PYTHON_DIR=$(find_fdb_python) || {
    echo "Error: Cannot find fdb Python module (directory containing fdb/ package)" >&2
    echo "" >&2
    echo "Searched in:" >&2
    [[ -n "${FDB_PYTHON_PATH:-}" ]] && echo "  FDB_PYTHON_PATH=$FDB_PYTHON_PATH" >&2
    echo "  python3 -c 'import fdb' (not importable)" >&2
    echo "  $REPO_ROOT/build_output/bindings/python/" >&2
    echo "  $HOME/build_output/bindings/python/" >&2
    echo "  $SCRIPT_DIR/" >&2
    echo "" >&2
    echo "Fix: specify the directory containing the fdb/ package:" >&2
    echo "  ./metadata-audit.sh --fdb-python /path/to/bindings/python check ..." >&2
    echo "  export FDB_PYTHON_PATH=/path/to/bindings/python" >&2
    echo "  pip install foundationdb" >&2
    exit 1
}

if [[ -n "$FDB_PYTHON_DIR" ]]; then
    export PYTHONPATH="${FDB_PYTHON_DIR}${PYTHONPATH:+:$PYTHONPATH}"
fi

# Also add the script directory so fdb_metadata_utils is importable
export PYTHONPATH="${SCRIPT_DIR}${PYTHONPATH:+:$PYTHONPATH}"

# --- Check for cluster file ---

# See if the user passed -C or --cluster-file in the remaining args
has_cluster_arg=false
for arg in "$@"; do
    if [[ "$arg" == "-C" || "$arg" == "--cluster-file" ]]; then
        has_cluster_arg=true
        break
    fi
done

if [[ "$has_cluster_arg" == "false" ]]; then
    # Check default locations
    cluster_file="${FDB_CLUSTER_FILE:-}"
    if [[ -z "$cluster_file" ]]; then
        if [[ -f "/etc/foundationdb/fdb.cluster" ]]; then
            cluster_file="/etc/foundationdb/fdb.cluster"
        elif [[ -f "$HOME/.fdb/fdb.cluster" ]]; then
            cluster_file="$HOME/.fdb/fdb.cluster"
        fi
    fi

    if [[ -z "$cluster_file" || ! -f "$cluster_file" ]]; then
        echo "Error: No cluster file found" >&2
        echo "" >&2
        echo "Searched in:" >&2
        [[ -n "${FDB_CLUSTER_FILE:-}" ]] && echo "  FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE (not found)" >&2
        echo "  /etc/foundationdb/fdb.cluster" >&2
        echo "  $HOME/.fdb/fdb.cluster" >&2
        echo "" >&2
        echo "Fix: pass the cluster file explicitly:" >&2
        echo "  ./metadata-audit.sh $COMMAND -C /path/to/fdb.cluster ..." >&2
        echo "  export FDB_CLUSTER_FILE=/path/to/fdb.cluster" >&2
        exit 1
    fi
fi

# --- Execute ---

exec python3 "$SCRIPT" "$@"
