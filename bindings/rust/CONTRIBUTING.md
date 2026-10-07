# Developing the Rust bindings

Use Rust 1.85.1 or later, Cargo, and libclang. Follow the FoundationDB repository
contribution process and preserve the imported licenses and contributor notices.

## Standalone Cargo

Run commands from `bindings/rust`. For an installed FoundationDB client:

```sh
cargo build-fdb-latest
cargo fmt --all -- --check
cargo clippy-all
cargo test -p foundationdb-tuple --all-features --locked
cargo test -p foundationdb-macros --locked
```

The `*-fdb-latest` aliases select `fdb-7_4` and `embedded-fdb-include`. Embedded
headers support standalone builds, but linking and execution still require
`libfdb_c`. Set `FDB_CLIENT_LIB_PATH` and the platform's runtime library path when
the library is outside the normal search paths. Client API features are mutually
exclusive; do not use `--all-features` for the client.

`FDB_INCLUDE_DIR` overrides embedded headers. For client builds, it must contain
`fdb_c.h`, its companion headers, and `fdb.options`; for simulator builds it must
also contain `CWorkload.h`. CMake stages these inputs from the owning source tree.
Standalone simulator builds otherwise use this repository's canonical
`bindings/c/foundationdb/CWorkload.h`.

## Tests

Configure `BUILD_RUST_BINDING=ON` and build `fdb_rust_tests` for client, tuple,
option-generator, and macro unit tests. `rust_future_safety_tests` also checks
that raw future construction cannot bypass the ownership contract.

Enable `RUN_RUST_INTEGRATION_TESTS=ON` and `BUILD_PYTHON_BINDING=ON` for tests
against a disposable local cluster:

```sh
cmake --build build --target fdb_rust_integration_tests fdbserver fdbcli fdbmonitor python_binding
ctest --test-dir build -R '^rust_' --output-on-failure
```

These commands run from the repository root. Direct Cargo client integration tests
also need a disposable cluster: set `FDB_CLUSTER_FILE`, then run
`cargo test-fdb-latest --locked`. They write test keys and exercise database-wide
behavior, so do not use an application cluster.

The shared [binding tester](foundationdb-bindingtester/README.md) runs fixed
regression seeds plus scripted and randomized comparisons against Python. Client
coverage includes directory-prefix allocation, native retry/error ownership,
commit uncertainty, and runtime-version-specific versionstamp behavior. Separate
library subprocess tests cover compatible and incompatible API selection across
independently loaded Rust libraries.

## Simulator support

Configure `BUILD_RUST_SIMULATION=ON` explicitly and build
`fdb_rust_simulation_tests` to compile the C-ABI adapter, examples, and unit tests.
The `rust_simulation_` CTests check context validity, callback metrics lifetimes,
thread ownership, phase cancellation, and native teardown. The Loom test explores
wake/dequeue memory ordering in a separate Cargo target directory. These checks do
not execute the simulator; workload smoke tests use a compatible server as
described in [foundationdb-simulation/README.md](foundationdb-simulation/README.md).

Hosted checks cover all retained crates against generated current headers and
check historical client API configurations against embedded snapshots separately.
State the actual API feature, client/server versions, tests, and bindingtester
seeds when reporting validation. Upstream release and scheduled simulation
campaigns are not imported.
