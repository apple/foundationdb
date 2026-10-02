#!/usr/bin/env bash

set -euo pipefail

if [[ $# == 0 ]]; then
    echo "Usage: $0 <foundationdb-build-dir> [iterations] [bindingtester options...]" >&2
    exit 2
fi

build_dir=$(cd "$1" && pwd -P)
shift
iterations=${1:-1}
if [[ $# -gt 0 ]]; then
    shift
fi
if [[ ! $iterations =~ ^[1-9][0-9]*$ ]]; then
    echo "iterations must be a positive integer" >&2
    exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
bindings_dir=$(cd "${script_dir}/../.." && pwd -P)
bindingtester="${build_dir}/bindings/bindingtester/bindingtester.py"
fdb_api_version=740
export FDB_RUST_BINDINGTESTER="${build_dir}/bindings/rust/bin/bindingtester"
export PYTHONPATH="${build_dir}/bindings/python${PYTHONPATH:+:${PYTHONPATH}}"
export LD_LIBRARY_PATH="${build_dir}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export DYLD_LIBRARY_PATH="${build_dir}/lib${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"

if [[ ! -x $FDB_RUST_BINDINGTESTER || ! -f ${build_dir}/bindings/python/fdb/fdboptions.py ]]; then
    echo "Build fdb_rust_tester and python_binding first (see setup_bindingtester.sh)." >&2
    exit 1
fi

# The Python tester imports its adjacent fdb package, so stage it beside the
# generated Python binding rather than running it from the source tree.
if [[ $bindings_dir != "${build_dir}/bindings" ]]; then
    mkdir -p "${build_dir}/bindings/bindingtester" "${build_dir}/bindings/python/tests"
    cp -R "${bindings_dir}/bindingtester/." "${build_dir}/bindings/bindingtester/"
    cp -R "${bindings_dir}/python/tests/." "${build_dir}/bindings/python/tests/"
fi

extra_options=("$@")
run_test() {
    python3 "$bindingtester" "${extra_options[@]}" "$@"
}

# These are faulty seeds, for now, it is good to check them all the time to avoid regression.
run_test --num-ops 1000 --api-version $fdb_api_version --test-name api --compare python rust --seed 3534790651
run_test --num-ops 1000 --api-version $fdb_api_version --test-name api --compare python rust --seed 3864917676
run_test --num-ops 1000 --api-version $fdb_api_version --test-name api --concurrency 5 rust --seed 3153055325
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 2095856910
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 2483220251
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 241550211
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1508488514
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4203526853

# https://github.com/foundationdb-rs/foundationdb-rs/issues/38
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4142326254
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4275610547
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1951034301
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1700644945

# https://github.com/foundationdb-rs/foundationdb-rs/issues/42
run_test --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 584458794

# Database-level directory operations must retry concurrent commit conflicts.
run_test --num-ops 100 --api-version 610 --test-name directory_hca --concurrency 5 rust --seed 3179331380

# The shared runner selects the newest supported API for scripted cases and
# samples supported runtime versions for randomized cases.
BINDINGTESTS=rust DISPLAYERROR=1 BREAKONERROR=1 \
  bash "${build_dir}/bindings/bindingtester/run_binding_tester.sh" \
    "$iterations" "${build_dir}/bindings/rust/bindingtester-errors.log" "${extra_options[@]}"
