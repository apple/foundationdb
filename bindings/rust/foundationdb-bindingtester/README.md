# Rust FoundationDB bindingtester

This executable implements the shared FoundationDB bindingtester protocol.
The Rust tester is registered as `rust` in `bindings/bindingtester/known_testers.py`.

Configure FoundationDB with `BUILD_RUST_BINDING=ON` and
`BUILD_PYTHON_BINDING=ON`, then build the Rust tester and Python reference binding:

```sh
bindings/rust/scripts/setup_bindingtester.sh /path/to/build
```

With a running FoundationDB cluster, run the fixed regression seeds followed by
one iteration of the API, concurrent API, directory, and directory
allocator tests:

```sh
bindings/rust/scripts/run_bindingtester.sh /path/to/build 1 --cluster-file /path/to/fdb.cluster
```

The script uses Python and Rust artifacts from the same build, tests API version
740, and compares API/directory results with the Python binding. It copies the
shared harness and Python tester into the build tree so the Python tester imports
the generated binding there. It does not install a Python package or clone a
second FoundationDB checkout. Increase the second argument to repeat randomized tests; retain the logged seeds to reproduce
failures.

For an individual case after the script has staged the shared test harness:

```sh
export FDB_RUST_BINDINGTESTER=/path/to/build/bindings/rust/bin/bindingtester
export PYTHONPATH=/path/to/build/bindings/python
export LD_LIBRARY_PATH=/path/to/build/lib
python3 /path/to/build/bindings/bindingtester/bindingtester.py rust --compare python \
  --api-version 740 --test-name api --num-ops 1000 --seed 3534790651 \
  --cluster-file /path/to/fdb.cluster
```

On macOS, use `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH`. `FDB_RUST_BINDINGTESTER`
can also select a tester built directly with Cargo.

The registered tester supports runtime API versions 610 through 740 and the
shared tuple types, including arbitrary-width integers and versionstamps.
Directory snapshot operations are disabled because the imported directory layer
does not implement them. The scripted suite is skipped because it requires the
current API version (800), beyond the Rust binding's API 740 support. Importing
the binding does not establish full feature parity.
