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
