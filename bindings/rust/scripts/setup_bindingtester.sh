#!/usr/bin/env bash

set -euo pipefail

if [[ $# != 1 ]]; then
    echo "Usage: $0 <foundationdb-build-dir>" >&2
    exit 2
fi

# The configured build must enable BUILD_RUST_BINDING and BUILD_PYTHON_BINDING.
cmake --build "$1" --target fdb_rust_tester python_binding
