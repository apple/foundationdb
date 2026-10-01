# FoundationDB Rust bindings

This Cargo workspace provides an async Rust interface to the FoundationDB C API,
including transactions, tuple and directory layers, and a binding tester. It was
imported from [foundationdb-rs](https://github.com/foundationdb-rs/foundationdb-rs).
See [UPSTREAM.md](UPSTREAM.md) for the source revision and licensing provenance.

## Crates

| Crate | Purpose |
| --- | --- |
| [`foundationdb`](foundationdb/README.md) | Async client, transactions, directory layer, and recipes |
| [`foundationdb-sys`](foundationdb-sys/README.md) | Raw C API bindings |
| [`foundationdb-gen`](foundationdb-gen/README.md) | Generate Rust options from `fdb.options` |
| [`foundationdb-tuple`](foundationdb-tuple/README.md) | Tuple encoding, subspaces, and versionstamps |
| [`foundationdb-macros`](foundationdb-macros/README.md) | API-version conditional compilation |
| [`bindingtester`](foundationdb-bindingtester/README.md) | FoundationDB binding tester protocol |
| [`foundationdb-profiling`](foundationdb-profiling/README.md) | Client profiling data decoder |
| [`foundationdb-simulation`](foundationdb-simulation/README.md) | Rust workloads for the deterministic simulator |
| [`foundationdb-simulation-tracing`](foundationdb-simulation-tracing/README.md) | Simulator trace integration |
| [`foundationdb-recipes-simulation`](foundationdb-recipes-simulation/README.md) | Recipe simulation workloads |

## Building with FoundationDB

Rust is optional and requires Rust 1.85.1 or later, Cargo, and libclang for
bindgen. Enable it when configuring the repository, then build the Rust targets:

```sh
cmake -S . -B build -G Ninja -DBUILD_RUST_BINDING=ON
cmake --build build --target fdb_rust fdb_rust_tester fdb_rust_tests
ctest --test-dir build -R '^rust_' --output-on-failure
```

The CMake build links the in-tree `fdb_c` library and generates bindings from the
same C headers and `fdb.options` as the other language bindings. Cargo artifacts
stay in the build directory. See [CONTRIBUTING.md](CONTRIBUTING.md) for tests and
standalone Cargo development.

## Compatibility and current limits

The migration preserves the upstream crate versions. API corrections are
documented in [UPSTREAM.md](UPSTREAM.md): raw-pointer ownership requires `unsafe`,
integer options take `i64` to cover the C API's full range, and simulation workload
wrappers enforce context lifetime and thread access. Crate versions are
independent of the FoundationDB server release number. Published crates on
crates.io are separate releases; importing the source does not publish or transfer
ownership of them.

Select exactly one Cargo API feature: `fdb-5_1`, `fdb-5_2`, `fdb-6_0`, `fdb-6_1`,
`fdb-6_2`, `fdb-6_3`, `fdb-7_0`, `fdb-7_1`, `fdb-7_3`, or `fdb-7_4`. The in-tree
build selects `fdb-7_4` (API 740), the highest API supported by the imported
workspace. A newer C client can serve that API, but this does not expose every
feature of the current FoundationDB main branch. In particular, this import does
not add API 800 wrappers or the native CDC API. Directory snapshot operations are
also not supported by the Rust binding tester, and its scripted suite is skipped
because it requires API 800. Additional wrappers and API-version support can be
developed separately.

The client follows the same transaction, retry, key-selector, tuple, and directory
protocols as the other bindings, expressed through Rust futures and ownership.
The [binding tester](foundationdb-bindingtester/README.md) exercises protocol
compatibility. The import is not a claim of complete feature parity or a new
cross-platform support guarantee. The CMake integration initially targets Linux
and macOS; standalone Cargo retains upstream platform support.

Simulation and profiling crates are retained as optional tools. Their compatibility
with server-internal interfaces and profiling formats must be checked separately
when changing server versions. Rust crates are not yet included in the native
client installation packages, and the upstream publishing and scheduled simulation
workflows are not installed by this migration.

## License

The imported Rust workspace remains dual licensed under
[Apache 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option. Existing author
and copyright notices are preserved. FoundationDB's other components retain their
existing licenses.
