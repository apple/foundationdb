#!/usr/bin/env bash

set -ex;

fdb_api_version=740

fdb_rs_dir=$(pwd)
fdb_builddir=${fdb_rs_dir:?}/target/foundationdb_build
cd "${fdb_builddir:?}/foundationdb"

# These are faulty seeds, for now, it is good to check them all the time to avoid regression.
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name api --compare python rust --seed 3534790651
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name api --compare python rust --seed 3864917676
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name api --concurrency 5 rust --seed 3153055325
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 2095856910
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 2483220251
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 241550211
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1508488514
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4203526853

# https://github.com/foundationdb-rs/foundationdb-rs/issues/38
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4142326254
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 4275610547
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1951034301
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 1700644945

# https://github.com/foundationdb-rs/foundationdb-rs/issues/42
./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python --seed 584458794

START=1
END=${1-1}
for i in $(eval echo "{$START..$END}")
do
  echo "Running iteration $i"
  ./bindings/bindingtester/bindingtester.py --test-name scripted rust
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name api --compare python rust
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name api --concurrency 5 rust
  ./bindings/bindingtester/bindingtester.py --num-ops 1000 --api-version $fdb_api_version --test-name directory --concurrency 1 rust --no-directory-snapshot-ops --compare python
  ./bindings/bindingtester/bindingtester.py --num-ops 100 --api-version $fdb_api_version --test-name directory_hca --concurrency 1 rust --no-directory-snapshot-ops
done
