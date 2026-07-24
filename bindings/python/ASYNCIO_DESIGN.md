# asyncio integration: design and trade-offs

This document describes the asyncio surface added in `fdb/asyncio_impl.py`,
explains why it is a wrapper rather than a native integration, and sketches
the remaining work for fully native asyncio support.

## What landed

* `fdb/asyncio_impl.py` exposes `AsyncDatabase`, `AsyncTransaction`,
  `open_async`, and `wrap_database`.
* `AsyncTransaction.get`, `commit`, `on_error`, etc. are coroutines that
  resolve the underlying `fdb.impl.Future` by submitting `Future.wait()` to
  the running event loop's default thread executor via
  `loop.run_in_executor`.
* `AsyncDatabase.transactional_async` mirrors the retry loop of
  `fdb.transactional`, but driven by `await` and `Transaction.on_error`.
* `tests/asyncio_test.py` is a smoke test that exercises a round-trip
  `set` / `get` and a multi-write transactional block. It requires a
  running FoundationDB cluster.

## Why this is a wrapper and not native asyncio

The C client already runs an internal network thread that fires callbacks
when a future is ready (`fdb_future_set_callback`). The existing Python
binding turns that callback into either a thread-blocking `wait()` or, for
the legacy `event_model="asyncio"` path, a monkey-patched
`asyncio.futures._FUTURE_CLASSES` entry. The `_FUTURE_CLASSES` approach
broke when CPython removed that internal in 3.5+, which is the bug tracked
in #208.

A correct shim has to satisfy two constraints simultaneously:

1. The event loop must remain non-blocking. Calling `Future.wait()` on the
   coroutine's thread blocks the loop and defeats the point of asyncio.
2. The result delivery must run on the loop's thread, because user-visible
   coroutines and any downstream `await` machinery expect that.

The simplest construction that satisfies both is to off-load `Future.wait()`
to a thread executor: the FDB network thread fires the callback that
unblocks `wait()`, the executor thread returns, and `run_in_executor` posts
the result back to the loop. This is what `fdb.asyncio_impl` does. The
trade-off is one extra thread hop per future, which is fine for typical
mixed I/O workloads but is not zero-cost.

## What a native integration would look like

A native integration would attach the FDB callback directly to the event
loop without the intervening executor thread:

1. Implement `Future.__await__` such that it suspends the current task,
   registers `loop.call_soon_threadsafe(_resolve, value)` as the FDB
   callback, and resumes when `_resolve` runs on the loop thread.
2. Provide an `asyncio.Future`-compatible adapter (`_asyncio_future_blocking`
   etc.) so that `await fdb_future` interoperates with `asyncio.gather`,
   `asyncio.wait`, and timeout helpers without falling back to the
   executor.
3. Replace the legacy `_FUTURE_CLASSES` monkey-patch in `impl.py` with the
   adapter above. Drop trollius and the Python 2 fallbacks.
4. Audit the binding tester (`bindings/python/tests/tester.py`) so the
   asyncio path is exercised in CI rather than only the default event
   model. The tester currently runs only the default model (see issue
   discussion).

The wrapper here is intended to coexist with that future work: callers
written against `AsyncDatabase` / `AsyncTransaction` continue to work when
the underlying futures gain native `__await__`, since the wrapper would
then become a near zero-cost passthrough.

## Limitations of the current shim

* Every awaited future consumes one slot on the default thread executor.
  Workloads that issue thousands of concurrent reads should configure a
  larger executor via `loop.set_default_executor(...)`.
* `get_range` is materialised eagerly into a list. Streaming async
  iteration is left for the native integration.
* `transactional_async` does not support the same decorator-style
  ergonomics as `fdb.transactional`. A decorator can be added once the
  shape of the native integration is settled.
* The shim does not interact with the legacy `event_model="asyncio"` path
  in `fdb.impl.init`; callers should leave `event_model` unset.
