# Import provenance

- Source: https://github.com/foundationdb-rs/foundationdb-rs
- Revision: `f0eda232ae2e99d3e9377229715a905f90cac4db`
- Source commit date: 2026-09-25
- Destination: `bindings/rust`
- License: MIT OR Apache-2.0; see `LICENSE-MIT` and `LICENSE-APACHE`.

The initial import preserves the ten-crate workspace, Cargo lockfile, examples,
tests, historical C headers and option definitions, changelogs, and optional Nix
development environment. The upstream project and its contributors retain their
existing attribution. The complete upstream history remains available at the
source repository and exact revision above; this is a source snapshot import.

Repository-specific GitHub workflows, Dependabot/coverage/release configuration,
agent instructions, and repository governance files were excluded. The adaptation
commit integrates CMake, shared binding tests, repository metadata, and development
documentation with FoundationDB. It does not import upstream hosted service
credentials or establish automatic package publishing.

Historical headers and `fdb.options` snapshots support standalone Cargo builds
against older API versions. The in-tree CMake build uses the canonical FoundationDB
headers and option definitions instead. Preserve the historical snapshots when
updating current options or formatting current FoundationDB C/C++ sources.

The in-tree adaptation also corrects public API contracts from this snapshot:

- `FdbFuture::new` is now unsafe because it takes unique ownership of a raw C
  future. Callers must establish pointer validity, ownership, and the result type
  before constructing the wrapper.
- Integer option payloads use `i64`, matching the signed 64-bit values accepted by
  the C API. Callers with explicitly typed `i32` values must widen them with
  `i64::from(value)`; unsuffixed integer literals continue to infer the right type.
- Raw simulation context and string constructors require `unsafe`. Workloads are
  wrapped through `RustWorkload::wrap`; `WrappedWorkload` is opaque, and workload
  callback tables are managed by the crate. Context clones and environments
  panic before accessing C state if used from another thread or after their
  workload is released.
- `Metrics<'_>` borrows its native sink for one `get_metrics` callback. A workload
  cannot retain the sink after returning; collect owned metric values instead.
- `WorkloadContext::set_process_id` is unsafe: its integer argument represents a
  native process pointer and must satisfy the documented lifetime and restoration
  requirements.
- Custom metric formats are checked before reaching native formatting. They must
  contain exactly one floating-point conversion without argument-supplied width,
  precision, positional arguments, or length modifiers; invalid formats panic.
- Simulation phases own their suspended workload and are cancelled before native
  workload teardown. Borrowed database handles are disarmed on completion and
  cancellation; all Rust references must be dropped before either path finishes.
- Versionstamped mutations require runtime API 520 or later. Earlier runtime
  versions panic before issuing either mutation because their key and value
  encodings differ from the tuple helpers' modern encoding.
- Timekeeper range-read errors propagate to the caller's retry loop. `None` means
  a successful read found no matching entry.
- Simulator registration checks API-selection failures. When built with the
  current C headers, it uses `fdb_get_selected_api_versions` to verify an existing
  process-wide runtime/header pair before initializing another Rust library.
  Historical headers cannot verify another library's selection and return an error.
- High-level versionstamped tuple and subspace packing requires exactly one
  incomplete versionstamp, matching the other bindings. Use ordinary packing
  for tuples containing only completed versionstamps.
- Transaction usage counters, client budgets, and metrics APIs require the
  opt-in `accounting` feature. Recipes are also opt-in; the default client
  enables only `uuid`.
- Simulation tracing guards are bound to their installation thread so their
  destruction clears the correct thread-local context.
- Database-level directory instructions in the binding tester use the native
  transaction retry loop and publish stack results only after commit, matching
  the other testers. Transaction-level instructions retain the caller's transaction.
