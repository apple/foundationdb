# FoundationDB Rust bindings

This Cargo workspace provides an async Rust interface to the FoundationDB C API,
including transactions, tuples, subspaces, the directory layer, and the shared
binding tester. It was imported from
[foundationdb-rs](https://github.com/foundationdb-rs/foundationdb-rs).
[UPSTREAM.md](UPSTREAM.md) records the source revision and licensing provenance.

## Crates

| Crate | Purpose |
| --- | --- |
| [`foundationdb`](foundationdb/README.md) | Async client, transactions, and directory layer |
| [`foundationdb-sys`](foundationdb-sys/README.md) | Raw C API bindings |
| [`foundationdb-gen`](foundationdb-gen/README.md) | Rust options generated from `fdb.options` |
| [`foundationdb-tuple`](foundationdb-tuple/README.md) | Tuple encoding, subspaces, and versionstamps |
| [`foundationdb-macros`](foundationdb-macros/README.md) | API-version conditional compilation |
| [`bindingtester`](foundationdb-bindingtester/README.md) | Shared binding tester protocol |
| [`foundationdb-simulation`](foundationdb-simulation/README.md) | Optional C-ABI simulator workloads |

The six client and tester crates are the default workspace members. Simulator
support is selected explicitly. Application layers, client-budget policies,
profiling decoders, and upstream development/release tooling are outside this
import.

## Building with FoundationDB

Rust is optional and requires Rust 1.85.1 or later, Cargo, and libclang for
bindgen. Enable it when configuring the repository:

```sh
cmake -S . -B build -G Ninja -DBUILD_RUST_BINDING=ON
cmake --build build --target fdb_rust fdb_rust_tester fdb_rust_tests
ctest --test-dir build -R '^rust_(unit_tests|future_safety_tests)$' --output-on-failure
```

CMake uses the in-tree `fdb_c` library, generated C headers, and canonical
`fdb.options`. Cargo artifacts stay in the build directory.
[CONTRIBUTING.md](CONTRIBUTING.md) covers standalone Cargo and integration tests.

To build the optional simulator adapter and its examples, also configure
`BUILD_RUST_SIMULATION=ON` and build `fdb_rust_simulation_tests`. It uses the
repository's canonical `CWorkload.h`, the C external-workload interface, and an
executor that keeps futures on their simulator thread. Its unit and safety tests
can be run with `ctest --test-dir build -R '^rust_simulation_' --output-on-failure`.
Running workloads requires a compatible `fdbserver`; see the crate's README.

## Compatibility

Select exactly one client API feature: `fdb-5_1`, `fdb-5_2`, `fdb-6_0`, `fdb-6_1`,
`fdb-6_2`, `fdb-6_3`, `fdb-7_0`, `fdb-7_1`, `fdb-7_3`, or `fdb-7_4`. The in-tree
build selects API 740, the highest API exposed by the imported Rust client.
Historical C headers and option snapshots remain available for standalone Cargo
builds using `embedded-fdb-include`.

A newer C client can serve API 740, but this import does not expose API 800 or
native CDC wrappers. Directory snapshot operations are not supported by the Rust
binding tester. Shared scripted tests select each tester's newest supported API:
740 for Rust and 800 for the current bindings.

Versionstamped mutations require runtime API 520 or later. With runtime API 510,
these mutations panic before reaching the C client; other supported API 510
operations remain available. Tuple versionstamp helpers use four-byte offsets.
High-level versionstamped packing requires exactly one incomplete stamp; use
ordinary packing for completed stamps.

The simulator adapter targets the current C workload ABI. When built against
current C client headers, independently loaded Rust workloads can share an API
selection only after verifying both its runtime and header versions. Historical
client headers cannot verify another library's selection and reject adoption.
Simulator workload contexts, metrics sinks, and phase database handles enforce
their native lifetime and thread boundaries. This does not transfer ownership of
the native network thread.

The in-tree build initially targets Linux and macOS. Native client distribution
packaging and crate publishing are not configured by this import. Imported crate
versions remain unchanged and are independent of the FoundationDB server version;
this source import does not publish crates or transfer their ownership.

## License

The imported workspace remains dual licensed under [Apache 2.0](LICENSE-APACHE)
or [MIT](LICENSE-MIT), at your option. Existing author and copyright notices are
preserved. FoundationDB's other components retain their existing licenses.
