# Import provenance

- Source: https://github.com/foundationdb-rs/foundationdb-rs
- Revision: `f0eda232ae2e99d3e9377229715a905f90cac4db`
- Source commit date: 2026-09-25
- Destination: `bindings/rust`
- License: MIT OR Apache-2.0; see `LICENSE-MIT` and `LICENSE-APACHE`.

The initial source snapshot preserved the upstream workspace. The in-tree
adaptation retains the six client/tester crates and minimal C-ABI simulator
support, with their tests, historical client API snapshots, attribution, and
licenses. Full upstream history remains available at the revision above.

Application recipes and their simulations, client accounting/budget/reporting and
custom retry-policy APIs, profiling and Timekeeper utilities, simulator tracing
integration, the legacy C++ workload bridge, and upstream Nix/Docker tooling are
outside this binding import. They remain available in the original source
snapshot. The core client still provides native transactions, retries, tuples,
subspaces, directories, and mapped results.

CMake and shared tests use the repository's canonical C headers, `fdb.options`,
and C workload interface. Historical client headers/options are preserved for
standalone Cargo builds; do not rewrite them when updating current interfaces.
Repository governance, hosted publishing, and upstream scheduled campaigns are
not imported. Existing crate versions do not imply a new published release.

The adaptation corrects these retained API and safety contracts:

- Raw C future and simulation context/string constructors require `unsafe`.
  Future wrappers take unique ownership of a valid pointer of the declared type.
- Integer options take `i64`, matching the C API's signed 64-bit payloads.
- Native retry handling discovers wrapped FoundationDB errors through their
  source chain and retains commit uncertainty across later attempts.
- Mapped-result selectors preserve server-provided range boundaries; futures
  recheck readiness after installing the current task's waker.
- Shared allocator synchronization protects callers using one transaction.
  Directory metadata with a newer minor version remains readable, while writes
  require compatible versions.
- High-level versionstamped tuple/subspace packing requires exactly one incomplete
  stamp. Versionstamped mutations require runtime API 520 or later; other supported
  API 510 operations remain available.
- Bindingtester database directory operations use native transaction retries and
  publish results after commit. `on_error_with_transaction` preserves the original
  transaction even when error handling fails.
- Simulation wrappers enforce native context/thread lifetimes. Metrics borrow
  their callback sink, process switching requires `unsafe`, and custom metric
  formats must consume exactly one double.
- Simulator futures stay on their owner thread. Workload release cancels suspended
  phases before teardown; phase database handles remain owned by the native caller.
- Current C headers expose the selected runtime/header pair so compatible loaded
  Rust libraries can adopt it safely. Historical headers reject unverified
  adoption. Neither path transfers ownership of the native network.
