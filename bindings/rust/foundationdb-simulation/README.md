# foundationdb-simulation

Optional support for Rust workloads loaded by FoundationDB's `ExternalWorkload`
simulator. This crate uses the current `bindings/c/foundationdb/CWorkload.h`
interface and exports its C workload factory. Workload configurations must set
`useCAPI = true`.

The build uses `CWorkload.h` from `FDB_INCLUDE_DIR` when set, or the canonical
header in this source tree otherwise. Use headers and the C client library from
the same FoundationDB build as the simulator. `FDB_CLIENT_LIB_PATH` selects the
client library directory for Cargo builds. The `fdb-7_4` feature selects the
client API used by the Rust binding; it does not select a historical simulator
ABI.

## Workloads

Implement `RustWorkload` to provide asynchronous `setup`, `start`, and `check`
phases, plus synchronous metrics and timeout callbacks. Implement
`SingleRustWorkload` to construct one workload type and register it with
`register_workload!`:

```rust,ignore
use foundationdb_simulation::{
    Metrics, RustWorkload, SimDatabase, SingleRustWorkload, WorkloadContext,
    register_workload,
};

struct MyWorkload {
    context: WorkloadContext,
}

impl SingleRustWorkload for MyWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        Self { context }
    }
}

impl RustWorkload for MyWorkload {
    async fn setup(&mut self, _db: SimDatabase) {}
    async fn start(&mut self, _db: SimDatabase) {
        self.context
            .delay(std::time::Duration::from_secs(1))
            .await
            .expect("simulated delay");
    }
    async fn check(&mut self, _db: SimDatabase) {}
    fn get_metrics(&self, _out: Metrics<'_>) {}
    fn get_check_timeout(&self) -> f64 {
        30.0
    }
}

register_workload!(MyWorkload);
```

For a library containing several workload types, implement `RustWorkloadFactory`
and use `register_factory!` instead. Register exactly one factory per shared
library. Workload crates must set `crate-type = ["cdylib"]`.

The examples cover native operations without an application runtime:

- `noop`: factory dispatch, workload options, tracing, metrics, and simulated delay.
- `atomic`: atomic mutations and a check against committed counter values.
- `shared`: sharing a Rust future backed by the C client.

From `bindings/rust`, build and run the atomic example with the matching
FoundationDB headers and client library available:

```bash
cargo build --locked --release -p foundationdb-simulation --example atomic \
  --features fdb-7_4
fdbserver -r simulation \
  -f foundationdb-simulation/examples/atomic/test_file.toml \
  -b on --trace-format json
```

The example configuration loads `target/release/examples/libatomic.so`. Adjust
`libraryPath` if using a different Cargo target directory.

## Simulator context

`WorkloadContext` exposes the simulator's clock (`now`), deterministic random
numbers (`rnd` and `shared_random_number`), delay futures, client IDs, workload
options, and tracing. Use these instead of machine clocks, ambient randomness,
or external timers so a simulation seed reproduces the workload.

Read every custom option supplied in the TOML configuration; unread options make
the native workload configuration invalid. `get_option<T>` returns `None` for a
missing option or a value that cannot be parsed as `T`.

Context clones do not extend the native workload's lifetime. Their operations
panic before native access when called from another thread or after the
registered workload is released. Raw context and string entry points are unsafe;
factories construct an opaque `WrappedWorkload` through `RustWorkload::wrap()`.

`context.trace(Severity::Error, ...)` fails the simulation. Traces capitalize the
first character of event names and detail keys, and add `RustWorkload="1"`;
error events also add `RustFailure="1"`. `Metric::val` values are summed across
clients, while `Metric::avg` values are averaged. The `Metrics<'_>` sink is
borrowed only for the metrics callback and must not escape it.

## Scheduling and ownership

The simulator drives phases cooperatively on its own thread. Await FoundationDB
futures or `context.delay`; do not busy-wait or use timers from another async
runtime. The executor drains queued Rust futures after native future callbacks.
A wake from another thread is safe but needs a later owner-thread drain to make
progress. See [executor details](docs/fdb_rt.md).

Native workload release cancels a suspended phase before destroying the workload
or invalidating its context. Final metrics collection or a new phase also cancels
an abandoned phase. Late wakes cannot restart it. Timeout queries during `check`
use the value sampled immediately before that phase; a timeout query before
`check` cancels abandoned setup/start work before reading the timeout. Native
callbacks must not free a workload reentrantly while its phase is being polled.

`SimDatabase` represents a borrowed native database using a Rust `Arc`. All
strong and weak database references, transactions, and database futures must be
dropped by the end of their phase, including cancellation. The phase guard frees
only the Rust allocation, preserving the native caller's database reference.
Escaping database references terminate the process before they can outlive that
native handle.

Registration selects the client API through the shared C client. Libraries may
share a selection only when their runtime and header versions match; incompatible
or unverifiable selections fail before workload creation. The simulator retains
ownership of the network and its database handles.

When built under `cargo llvm-cov`, completed metrics callbacks flush their LLVM
profiles because `fdbserver` can exit without running process-exit handlers. A
failure before metrics collection does not guarantee a profile flush.
