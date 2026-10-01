# Developing the Rust bindings

Run Cargo commands from `bindings/rust`, where the toolchain and Cargo aliases are
configured. The minimum Rust version is 1.85.1. Bindgen also requires libclang.
Follow the FoundationDB repository contribution process for changes to this tree.

## Standalone Cargo

For an installed FoundationDB client, use the existing versioned embedded headers:

```sh
cd bindings/rust
cargo build-fdb-latest
cargo fmt --all -- --check
cargo clippy-all
cargo test -p foundationdb-tuple --all-features --locked
cargo test -p foundationdb-macros --locked
```

`*-fdb-latest` currently selects `fdb-7_4` and `embedded-fdb-include`. Embedded
headers remove the need for installed headers, but linking and running binaries
still require `libfdb_c`. Set `FDB_CLIENT_LIB_PATH` if the library is outside the
linker's standard search path, and configure the platform's runtime library path
when needed. Do not use `--all-features` for the client: API-version features are
mutually exclusive.

For a custom header set, `FDB_INCLUDE_DIR` selects a directory containing
`fdb_c.h`, its companion headers, and `fdb.options`. It takes precedence over
embedded headers. CMake supplies this directory from the current repository.

## Tests

The CMake targets and CTests are described in [README.md](README.md). Enable
`RUN_RUST_INTEGRATION_TESTS` in addition to `BUILD_RUST_BINDING` to run the client
integration tests against a temporary local test cluster. Keep
`BUILD_PYTHON_BINDING=ON` for the shared test-runner fixture, then build and run:

```sh
cmake --build build --target fdb_rust_integration_tests fdbserver fdbcli fdbmonitor python_binding
ctest --test-dir build -R '^rust_' --output-on-failure
```

Run these commands from the repository root (or substitute an absolute build
directory).

When testing with Cargo directly, the client integration tests need a disposable
FoundationDB database. They write test keys and exercise database-wide behavior;
do not point them at an application cluster. Set `FDB_CLUSTER_FILE` to the test
cluster file, then run:

```sh
cargo test-fdb-latest --locked
```

Use the in-tree [binding tester instructions](foundationdb-bindingtester/README.md)
for comparison with Python. Tuple tests and macro tests do not need a cluster.

`rust_unit_tests` also checks simulation context and metrics callbacks, executor
wakeups, and thread ownership. The simulation safety doctests check that borrowed
metrics sinks cannot escape their callback and process switching requires `unsafe`.
`rust_simulation_wake_order_tests` uses Loom to explore weak-memory interleavings of
the executor's wake and dequeue code. Its separate Cargo target directory keeps
model synchronization out of normal builds. These checks do not start the simulator;
workload execution is a separate check against a compatible server.

The simulation crates and their scripts are retained for focused simulator work;
their READMEs describe the workload ABI and required server versions. The original
repository's scheduled correctness, simulation, release, and documentation jobs
are not enabled here. Validation should state the API feature, client/server
version, tests, and binding-tester seeds actually exercised.

## Contributions and licensing

Keep changes focused and preserve public Rust API compatibility when possible.
Run rustfmt and the relevant unit, integration, or binding tests. Existing
changelogs record upstream releases; this migration does not create a new crate
release. Preserve the imported MIT/Apache-2.0 license and contributor notices.
