# foundationdb-simulation-tracing

Forward tokio [`tracing`](https://docs.rs/tracing) events into the FoundationDB
simulator trace log. Once installed, both your workload's own `tracing::info!`
/ `warn!` / ... calls and the `foundationdb` SDK's internal retry-loop
diagnostics (enabled by the SDK's `trace` feature) land in the simulator's
`trace.*.json` files next to fdbserver's own events, instead of being lost.

## Usage

Call `install` from your workload factory and keep the returned guard alive by
storing it in your workload struct. The guard unbinds the context when the
workload is torn down, so no event can reach a freed context.

```rust,ignore
use foundationdb_simulation::{
    RustWorkload, SimDatabase, SingleRustWorkload, WorkloadContext, register_workload,
};
use foundationdb_simulation_tracing::{TracingGuard, install};

struct MyWorkload {
    // keep the guard alive for the workload's lifetime
    _tracing: TracingGuard,
    context: WorkloadContext,
}

impl SingleRustWorkload for MyWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        let _tracing = install(&context);
        Self { _tracing, context }
    }
}

// impl RustWorkload for MyWorkload { ... }
register_workload!(MyWorkload);
```

From then on, `tracing::info!(client = 0, "starting")` shows up in the trace log
as a TraceEvent of type `RustTracingEvent`. Grep for it:

```bash
grep '"Type": *"RustTracingEvent"' target/traces/trace.*.json
```

By default, the bridge forwards `INFO`, `WARN`, and `ERROR` events. Set
`FDB_SIM_TRACING_LEVEL=debug` (or `trace`) before the workload starts to include
lower-level events and spans. The value is read once when the subscriber is
installed.

Each forwarded event carries `Target`, `File`, `Line`, and `Message` details
(when present) followed by the event's own fields.

## Span context

The subscriber is built on `tracing-subscriber`'s `Registry`, so events fired
inside a span carry that span's context. For an event with an enclosing span,
the forwarded details also include:

- `Span`: the name of the innermost span.
- `SpanPath`: the span names from the root to the innermost span, joined with
  `/` (for example `demo_transaction` or `outer/inner`).
- the fields of every ancestor span, from root to leaf.

Attach a span to an async block with the `tracing::Instrument` trait
(`future.instrument(span)`) rather than `span.enter()`, which must not be held
across an `.await`.

Collision rule: an event's own field always wins over a span field of the same
name. A span field is skipped when the key is already present in the details, so
no duplicate key is produced for that case.

## Severity mapping

`tracing` levels map to FDB severities as follows:

| `tracing` level | FDB `Severity` |
|---|---|
| `TRACE`, `DEBUG` | `Debug` |
| `INFO` | `Info` |
| `WARN` | `Warn` |
| `ERROR` | `WarnAlways` |

`ERROR` maps to `WarnAlways`, never to `Severity::Error`. In the simulator a
`Severity::Error` event flags the whole run as failed, and a dependency's (or
your own) `tracing::error!` must not silently kill a simulation. If you want to
fail the run on purpose, call `context.trace(Severity::Error, ...)` directly.

## Caveats

- Last created workload wins: in a multi-client simulation every factory call
  re-binds the current thread's context slot. All contexts write to the same
  simulator trace stream, so attribution is cosmetic, and each guard only
  clears the slot if it still holds its own binding.
- Reserved keys: FDB capitalizes the first letter of every detail key, so a
  field named `file`, `line`, `target`, or `message` collides with the meta
  keys this crate prepends (and `type`, `time`, `machine`, ... collide with
  TraceEvent's own reserved keys). Duplicate keys are kept as-is in the JSON.

## Features

One FDB version feature must be selected: `fdb-7_1`, `fdb-7_3`, or `fdb-7_4`.
`embedded-fdb-include` (on by default) compiles without a local FoundationDB
install. These simply forward to the matching `foundationdb-simulation`
features.
