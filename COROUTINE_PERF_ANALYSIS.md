# C++20 Coroutine vs ACTOR Performance Analysis

## Summary

After unified allocation (embedding CoroActor in the coroutine frame) and
variant store elimination (skipping std::variant copy for non-stream futures),
C++20 coroutines are at near-parity with ACTOR-generated code on Linux.

## Linux Benchmark Results (32-core, 3.1 GHz)

```
Benchmark         ACTOR (ns)    Coroutine (ns)    Overhead
---------         ----------    --------------    --------
DELAY/4096           203             205             ~1%
NET2/4096        1,607,113       1,709,636           ~6%
```

YIELD benchmarks show coroutines ~3% FASTER than ACTOR on both platforms.

## Where the Remaining ~6% NET2 Overhead Lives

Profiled with `perf record -g` on Linux. Two root causes:

### 1. Coroutine frame is larger than ACTOR struct (+32 bytes)

The `increment()` coroutine frame allocates from `FastAllocator<128>` (97-128B),
while the ACTOR version uses `FastAllocator<96>` (65-96B). The extra ~32 bytes
come from C++20 frame overhead:

```
ACTOR struct layout (~80B, fits in <96> bucket):
  Actor<Void> base       ~48B   (SAV vtable + callbacks + refcounts + error + wait_state)
  Parameter copies       ~12B   (TaskPriority + unsigned int*)
  State machine int       ~4B
  Alignment padding      ~16B

Coroutine frame layout (~112B, needs <128> bucket):
  CoroPromise            ~56B   (embedded CoroActor = same as Actor<Void> base)
  Parameter copies       ~12B   (same)
  AwaitableFuture temp   ~32B   (Callback prev/next + future ref + promise_type ptr)
  Compiler suspend state  ~8B   (suspend point index, padding)
  Alignment padding       ~4B
```

The key difference is the AwaitableFuture temporary (~32B). In ACTOR code,
the callback is the actor itself (via ActorCallback multiple inheritance).
In coroutine code, each `co_await` creates a separate AwaitableFuture object
that lives in the frame. This is inherent to how C++20 `co_await` works.

With 4096 `increment()` calls per NET2 iteration, that's 4096 allocations
from the larger bucket — measurable cache pressure.

### 2. Extra indirection in the resume path (~7.5% of profile)

```
Coroutine-specific overhead (not present in ACTOR profile):
  AwaitableFuture::fire()       2.53%   (callback → handle.resume())
  increment .resume entry       3.95%   (coroutine resume stub)
  AwaitableFuture::resumeImpl() 1.01%   (wait_state switch + callback removal)
                                -----
                         Total: 7.49%
```

ACTOR code avoids this because `ActorCallback::fire()` directly calls the
state machine body (`a_body1loopHead1`). Coroutine code goes through:
`fire()` → `handle.resume()` → compiler-generated resume stub → `await_resume()` → `resumeImpl()`.

## Why DELAY Shows ~1% While NET2 Shows ~6%

DELAY benchmarks create one coroutine that loops (suspend/resume in a loop).
The frame is allocated once and reused. The overhead is just the resume path.

NET2 benchmarks create 4096 fresh `increment()` coroutines per iteration.
Each one allocates a frame, runs, and deallocates. The per-frame allocation
size difference (128B vs 96B) is multiplied 4096x, making it visible.

## What Would It Take to Beat ACTOR

1. **Shrink the frame below 96 bytes** — would need to eliminate the
   AwaitableFuture temporary from the frame. Not possible with standard
   C++20 co_await semantics. Would require compiler extensions or a
   fundamentally different awaitable design.

2. **Inline the resume path** — compiler could theoretically devirtualize
   `handle.resume()` and inline through to `await_resume()`. Current
   compilers don't optimize this well for coroutines.

3. **Use SingleCallback for non-stream** — saves 8 bytes (one pointer) per
   AwaitableFuture, but unlikely to cross the 96B threshold alone.

## Optimizations Already Applied

1. **Unified allocation** — CoroActor embedded in CoroPromise. One heap
   allocation instead of two per coroutine. Eliminated ~30% overhead.

2. **Variant store elimination** — non-stream futures read directly from
   the SAV after `fire()`, skipping the `std::variant<Error, T>` copy.
   Saved one value copy per co_await resume.

3. **Fiber stack increase** — Boost fiber stack 32KB → 256KB to prevent
   stack overflow when RocksDB runs on fiber coroutines.

## Conclusion

On Linux, coroutines are within 1-6% of ACTOR performance. The remaining
gap is inherent to C++20 coroutine semantics (per-await temporaries in
the frame, indirect resume through handle). This is likely acceptable
for production use, especially since YIELD operations (the most common
suspend pattern) already show coroutines faster than ACTOR.
