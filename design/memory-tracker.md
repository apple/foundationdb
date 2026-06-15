# FDB Memory Tracker — Per-Call-Site Allocation Attribution

## Objective

Make it easier to debug memory leaks and generally to understand the
principal consumers of memory in FDB.  Specifically: add a sampled,
always-compiled, runtime-knob-controlled memory attribution mechanism
that captures a small return-address backtrace at a configurable
fraction of allocations, aggregates byte and call counts per call
site, and periodically emits the aggregates as TraceEvents for offline
`addr2line` symbolization. The mechanism must be lightweight enough to
leave on by default in production (~1% sampling) and at higher rates
(~50%) in simulation, where it must remain deterministic.

## Background

Historically memory leaks that happen in FDB production or at scale
can be challenging to debug, taking O(weeks) to resolve.
Additionally, lack of granular understanding of memory usage might
make it more challenging to reduce process memory usage in support of
ongoing fleet efficiency goals.

FDB has aggregate memory telemetry today — per-`FastAllocator` size-class
counters, total `Arena` bytes (`/flow/arena/arenaBlockBytesAllocated`), the
`g_hugeArenaMemory` atomic, and process RSS reported in
`flow/SystemMonitor.cpp`'s `MemoryMetrics` event — but no per-call-site
attribution. When memory pressure or a leak shows up in production, the
trace tells us *what size class* or *how much arena memory* but not *which
caller*, making root-causing difficult.

A partial framework already exists in `flow/include/flow/FastAlloc.h`
under the `ALLOC_INSTRUMENTATION` compile flag. It defines the right
shapes — `memSample` (pointer → backtrace hash + size), `backTraceLookup`
(hash → aggregate), a `memSample_entered` reentrancy flag — and global
`operator new`/`delete` overrides in `fdbserver/fdbserver.cpp`. But
it relies on glibc `backtrace(3)` (microseconds per call), is gated behind
a non-default compile flag, and was designed for offline analysis, not
production. In practice it is dead code: builds that turn it on are too
slow to run real workloads.

The infrastructure needed to make a lightweight version cheap is already
in place:

- Frame pointers are globally enabled
  (`cmake/ConfigureCompiler.cmake`: `-fno-omit-frame-pointer`), so a
  manual frame-pointer walk replaces `backtrace(3)` and runs in ~100 ns.
- `platform::format_backtrace` (`flow/Platform.cpp`) already emits a
  ready-to-paste `addr2line -e <bin> -p -C -f -i 0xADDR ...` command with
  the PIE load offset subtracted via `dl_iterate_phdr`. Symbolization is
  fully offline.
- `flow/SystemMonitor.cpp` already drives periodic memory TraceEvents we
  can hang the per-site dump off.

## Requirements

R0. **Cheap enough to enable in production by default.** Various following
    requirements support this.

R1. **Un-sampled hot path.** A non-sampled allocation incurs only one
    thread-local load, one decrement, and one branch. No atomics, no
    syscalls, no library calls.

R2. **Sampled CPU budget.** End-to-end overhead from sampling at 1% on
    workloads that allocate at 100K/sec must be under 0.1% CPU. Stack
    capture must take less than 200 ns for 6 frames on x86_64 and
    aarch64.

R3. **Default-on in production and simulation (post-rollout
    steady state).** Steady-state production default is 1% sampling
    (one in 100 allocations); simulation runs at 50% (one in two) so
    the tracker is exercised under fault injection. The Rollout
    section describes a phased landing where the production default
    starts at 0 and is flipped to the steady-state value after
    baseline performance testing.

R4. **Allocation-path coverage.** All three FDB-owned allocation paths
    are attributed:
    - Global `operator new` / `operator delete` (all standard overloads:
      sized, unsized, array, nothrow, aligned).
    - `FastAllocator<Size>::allocate` / `::release` for every size class.
    - `Arena` allocations, attributed at the `ArenaBlock` granularity
      (one entry per block, sized to the block's byte count).

R5. **Out-of-scope allocators.** Direct libc `malloc`/`free`,
    `posix_memalign`/`aligned_alloc`, `mmap`, and third-party allocators
    that bypass `operator new` are not attributed in v1. RocksDB
    allocations *are* covered via the `operator new` override since
    RocksDB STL containers and internal `Arena` slabs go through global
    `operator new[]`.

R6. **Determinism in simulation.** Sampling state lives entirely in
    thread-local variables and never reads `g_random` or
    `g_network->now()`. Identical workload + seed must produce identical
    aggregate counts.

R7. **Periodic reporting.** A configurable cadence (default 10 minutes
    in production, 30 s in simulation) emits one TraceEvent per
    qualifying site, where a site qualifies when its current live
    bytes exceed `MEMORY_TRACKING_REPORT_BYTES_THRESHOLD` (default
    80 MB — see Reporting for rationale). Each site event contains raw
    return-address frames; a single combined `addr2line` command for
    all qualifying sites is emitted as its own event. The dump itself
    runs in under 5 ms when ~50 sites qualify.

R8. **Zero-cost runtime symbolization.** All address-to-symbol mapping
    happens offline. The runtime never calls `backtrace_symbols`,
    `dladdr`, or any DWARF reader.

R9. **Strip-aware.** Reports work against stripped production binaries
    when separate debug info (the `.debug` sidecar this repo's release
    build already produces) is available to the offline tooling.

R10. **Off-switch.** Setting the sample-inverse knob to 0 disables
     sampling at runtime. The hot path remains a single TLS load + branch;
     no aggregation work happens.

R11. **Reentrancy safety.** No allocation made by the tracker itself
     (table grows, dump scratch space) re-enters the tracking path. A
     thread-local guard prevents re-entry.

R12. **Side-thread coverage.** Allocations from any thread are
     attributed, not only the network thread. This includes threads
     spawned by third-party libraries we do not own (RocksDB
     compaction/flush/iteration, OpenSSL background workers, etc.).
     The frame-pointer walker must terminate cleanly when it
     traverses FP-elided code such as glibc's pthread shutdown
     machinery — see "Side-thread safety" for the empirical repro
     (joshua-found `IThreadPool` segfaults) that exposed the
     original walker's crash mode.

## Design Overview

Three pieces:

1. **Sampling and capture.** Every allocation site calls a header-inlined
   `memTrackerOnAlloc(p, size)` (and matching `memTrackerOnFree(p)`).
   Both check a thread-local reentrancy flag and a thread-local
   decrementing counter; the un-sampled path is one TLS load + one
   decrement + one branch. On sample, walk the frame-pointer chain for
   4–6 return addresses (initial estimate, subject to refinement during
   development of this feature).

2. **Storage.** Two tables, both backed by `std::malloc` (which bypasses
   our `operator new` hooks):
   - A fingerprint-keyed *aggregation table*: `uint64_t → CallSite`,
     where `CallSite` carries `liveCount` / `liveBytes` (count and
     bytes of objects currently allocated at this site), `peakBytes`,
     `cumulativeAllocs` / `cumulativeBytes` (never-decremented
     lifetime totals), `forceSampledCount`, and the exemplar frames.
     Fingerprint is `fnv64(frames)`. Always present. See "Aggregation
     table" below for the full struct.
   - A pointer-keyed *live-block table* (`void* → {fingerprint, size}`)
     so `onFree` can find what `onAlloc` recorded. The pointer is the
     only key available at free time — there is no backtrace to hash —
     so the table is necessarily pointer-keyed if we want to track live
     bytes at all. Optional, gated by the `MEMORY_TRACKING_LIVE_TRACKING`
     knob (default on). When disabled, only cumulative per-site stats
     are tracked and the side table disappears entirely.

3. **Reporting.** A `memTrackerDump(int64_t bytesThreshold)` walks the
   aggregation table, emits a TraceEvent per site whose live bytes
   exceed the threshold, plus one combined `addr2line` command covering
   all qualifying sites. Called periodically from `SystemMonitor.cpp`.

Hooks:

| Path | Allocate | Free |
|---|---|---|
| Global `new`/`delete` | `MemoryTracker.cpp` (new file) | `MemoryTracker.cpp` (same file) |
| `FastAllocator<Size>` | `FastAlloc.cpp` (`::allocate`) | `FastAlloc.cpp` (`::release`) |
| `Arena` | `Arena.cpp` (`ArenaBlock::create`) | `Arena.cpp` (`ArenaBlock::destroyLeaf`) |

## Detailed Design

### Sampling and reentrancy

```cpp
// MemoryTracker.h — header-inlined hot path

extern thread_local bool gInMemTracker;
extern thread_local int  gMemTrackerCounter;
extern thread_local size_t gForceSampleBytes;  // refreshed periodically from FlowKnobs

inline void memTrackerOnAlloc(void* p, size_t n) {
    if (gInMemTracker || !p) return;
    if (--gMemTrackerCounter > 0 && n < gForceSampleBytes) return;  // un-sampled fast path
    gInMemTracker = true;
    memTrackerSampleAlloc(p, n);            // out-of-line; reseeds counter
    gInMemTracker = false;
}

inline void memTrackerOnFree(void* p) {
    if (gInMemTracker || !p) return;
    gInMemTracker = true;
    memTrackerSampleFree(p);                // no-op if p not tracked
    gInMemTracker = false;
}
```

The counter is reseeded on every sample to a small uniform random
integer with mean `INV`, so the sampling rate averages 1-in-`INV`
without aliasing to fixed allocation patterns. When `INV == 0`
(off-switch), the reseed parks the counter at a value large enough
that the un-sampled fast-path branch is taken indefinitely with no
further work.

The counter's initial value (per-thread) is chosen so the first
allocation a thread sees is always sampled, which guarantees that
test workloads with very few allocations still exercise the path.

**Force-sample-large.** The `n < gForceSampleBytes` check guarantees
that any allocation at or above the configured threshold (default 100
KB; `INT64_MAX` disables) is *always* sampled regardless of the
counter. Large allocations are rare per second so unconditional
sampling costs almost nothing in CPU, and they are often the most
interesting individual allocations regardless of frequency (caches,
buffers, big arrays). This collapses the byte-rate-vs-count-rate
distinction for the case it actually matters: rare-but-huge
allocations are no longer at risk of being missed.

`gForceSampleBytes` is a thread-local cache of
`FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES`, refreshed inside
`memTrackerSampleAlloc` (which already runs under sampled-path cost),
so the hot path reads a TLS slot rather than touching the knobs
struct.

The aggregation table per-site adds a `forceSampledCount` counter
incremented on this path; the dump emits it as `ForceSampledCount`.
Consumers compute `ForceSampledCount / CumulativeAllocs` to tell
whether a site's stats are predominantly "every alloc above the
force-sample threshold" vs "1% of allocs" — without that, two sites
with the same `CumulativeAllocs` value can mean very different
population rates.

### Stack capture

```cpp
// Cached once per thread; see "Side-thread safety" for why.
extern thread_local uintptr_t gStackLow, gStackHigh;
void initStackBoundsForThread();   // pthread_getattr_np + pthread_attr_getstack

__attribute__((no_instrument_function, noinline))
int captureStackFP(void** frames, int max) {
    if (!gStackLow) initStackBoundsForThread();
    void** fp = (void**)__builtin_frame_address(0);
    // Fallback for threads where pthread_getattr_np fails: ±8 MB
    // around the initial frame.
    uintptr_t lo = gStackLow  ? gStackLow  : (uintptr_t)fp;
    uintptr_t hi = gStackHigh ? gStackHigh : (uintptr_t)fp + (8u << 20);
    int n = 0;
    while (fp && n < max) {
        uintptr_t a = (uintptr_t)fp;
        if (a < lo || a + 16 > hi) break;            // unmapped / FP-elided upstream
        if (a & (sizeof(void*) - 1)) break;          // misaligned
        void* ra = fp[1];
        if (!ra) break;
        frames[n++] = ra;
        void** next = (void**)fp[0];
        if (next <= fp) break;                       // stack grows down
        fp = next;
    }
    return n;
}
```

Skips no frames at capture; the caller (`memTrackerSampleAlloc`) is
responsible for stripping the topmost 1–2 frames so the captured stack
starts at the real allocation site (not inside the tracker itself).

Relies on `-fno-omit-frame-pointer` already set globally
(`cmake/ConfigureCompiler.cmake`). On x86_64 and aarch64 each frame
is two adjacent words (saved FP, saved RA), so the walk is one indirect
load per frame: ~100 ns for 6 frames.

Caveats and mitigations:

- Code compiled with `-fomit-frame-pointer` (notably glibc's pthread
  shutdown / TLS-destructor machinery, and third-party static libs
  not rebuilt with project flags) does NOT terminate the walk
  cleanly. The saved-FP slot at the FP-elided boundary contains
  whatever that function happened to leave on the stack —
  uninitialized garbage, not `NULL`. The `next <= fp` sanity check
  is insufficient: garbage often satisfies it but points into
  unmapped memory, and the next `fp[1]` dereference segfaults.
  **Observed empirically:** without a bounds check, simulation
  segfaulted inside `captureStackFP` on every joshua-found seed
  that exercised `tests/fast/RandomUnitTests.toml`'s
  `IThreadPool/{NamedThread, ExplicitStop, ImplicitStop}` cases
  (the crash signature was identical: thread-pool worker exiting,
  its `FastAllocator<N>::ThreadData` destructor allocating a
  vector grow, the FP walk crossing into glibc's TLS-destructor
  cleanup and dereferencing garbage). Mitigation: cache the
  current thread's stack range once via `pthread_getattr_np` +
  `pthread_attr_getstack`, and require `fp ∈ [stackLow, stackHigh
  - 16)` aligned before dereferencing on each iteration. With the
  bounds check the walk terminates cleanly at the FDB-side
  boundary; we still get the FDB-side prefix, which is what we
  care about. See "Side-thread safety" below for the full chain
  analysis.
- Signal handlers can leave a transiently bad FP chain mid-walk.
  The same stack-bounds + `next <= fp` checks bail out of those
  cases.
- ASAN/MSAN builds may instrument the FP chain. The tracker is a
  no-op-equivalent in those builds (sampling defaults can be flipped to
  0 in CMake when sanitizers are on).

### Side-thread safety

The tracker must work on every thread that allocates, not just the
network thread. RocksDB — FDB's largest dependency — runs
significant work on its own background threads (compaction, flush,
iteration), and those code paths certainly allocate. Missing
side-thread coverage would leave a large blind spot precisely
where we expect a lot of byte volume. Side-thread coverage is a
hard requirement (see R12), not a nice-to-have.

This pulled in two non-obvious constraints that the initial draft
missed:

1. **No assumption of a single thread.** All tracker state that
   touches the slow path is either thread-local (sampling counter,
   reentrancy flag, stack-bounds cache, xorshift seed) or guarded
   by the global spinlock (aggregation table, live-block table).
   The hot path on every thread is one TLS load + one decrement +
   one branch — it does not need to know whether it is on the
   network thread, an I/O thread, a RocksDB compaction thread, or
   a thread the tracker has never seen before.

2. **Robust frame-pointer walking on threads we did not spawn.**
   FDB-spawned threads start in FDB code (FP-enabled all the way
   up). RocksDB-spawned threads start inside RocksDB and end up
   in libc's `start_thread` machinery, which is FP-elided. At
   thread *exit*, every thread (FDB- or library-spawned)
   traverses glibc's TLS-destructor invocation, also FP-elided. A
   FP-elided frame leaves the saved-FP slot uninitialized; our
   walker, if it just trusts that slot, follows garbage into
   unmapped memory and segfaults. **This is not hypothetical — we
   hit it directly.** Joshua reproduced it with three RandomUnitTests
   seeds (3288611985, 3731245491, 2219741568): every run segfaulted
   inside `captureStackFP` when an `IThreadPool` worker exited and
   its `FastAllocator<N>::ThreadData` destructor allocated a vector
   grow during slab return. Symbolizing the crash showed the walker
   had stepped from `~ThreadData` into glibc's TLS-destructor
   invocation, where the saved-FP slot was garbage. The
   stack-bounds check described under "Stack capture" above is what
   makes this safe: any walk that crosses into FP-elided code
   terminates at the stack boundary instead of dereferencing
   garbage.

The same considerations apply to threads that use `setjmp` /
`longjmp` or coroutine resumption to switch stacks — at the switch
point, the FP chain may temporarily lead into a different stack
region. The stack-bounds check correctly terminates those walks
too (the cached bounds are for the thread's *primary* stack;
walks that wander into a coroutine stack or sigaltstack simply
end early). For our use case "early termination on a non-primary
stack" is the right behavior: the captured prefix is the part on
the primary stack, which is what a human reading the dump cares
about.

### Storage

#### Live-block table

Hash table, key `void*`, value `{uint64_t fingerprint, uint32_t size}`
(16 bytes/entry). All backing memory comes from `std::malloc`
directly: this bypasses our `operator new` hooks (we override
`operator new`, not libc `malloc`), so the tracker's own allocations
cannot recurse back into the tracking path. The thread-local
`gInMemTracker` flag is the single line of defense and is sufficient
on its own; we don't need a private slab pool.

Created lazily on first sample if the `MEMORY_TRACKING_LIVE_TRACKING`
knob is set; if the knob is off, the table is never allocated and only
the aggregation table exists.

Single global `ThreadSpinLock` for v1. At 1% sampling and 100K
alloc/sec the lock fires ~1K times/sec; uncontended spinlock acquire is
~20 ns; total ~20 µs/sec ≈ 0.002% CPU. Per-thread sharding is deferred
to a follow-up if profiling on a real workload shows contention (see
Alternatives).

#### Aggregation table

Open-addressing hash, key `uint64_t fingerprint`, value:

```cpp
constexpr int MEMORY_TRACKER_MAX_FRAMES = 10;  // upper bound for the
                                               // MEMORY_TRACKING_FRAMES knob

struct CallSite {
    uint64_t fingerprint;
    int64_t  liveBytes;
    int64_t  liveCount;
    int64_t  peakBytes;
    int64_t  cumulativeAllocs;    // never decremented
    int64_t  cumulativeBytes;     // never decremented
    int64_t  forceSampledCount;   // see "Force-sample-large" in Sampling
    void*    exemplarFrames[MEMORY_TRACKER_MAX_FRAMES];
    uint8_t  exemplarFrameCount;
};
```

`fingerprint = fnv64(frames[0..n])` computed once at sample time. The
exemplar frames are stored from the first allocation that produced the
fingerprint and are never updated — different call paths that happen to
collide on a fingerprint hash are very rare with 64-bit fnv and 4–6
frames (the frame count is an initial estimate, subject to refinement
during development); if it matters we can switch to xxhash or store all
observed frames per fingerprint.

`peakBytes = max(peakBytes, liveBytes)` updated on each alloc.

### Hook sites

#### Global `operator new` / `delete`

`MemoryTracker.cpp` defines the ~12 standard global overloads — `new`,
`new[]`, `delete`, `delete[]`, sized variants, `nothrow_t` variants, and
the C++17 `std::align_val_t` overloads. Each calls `std::malloc` /
`std::free` (or `aligned_alloc` for the aligned variants) and then
`memTrackerOnAlloc`/`OnFree`. The new module's unconditional overrides
cover every binary that links flow. The legacy conditional overrides
in `fdbserver/fdbserver.cpp` are left in place; our new overrides are
wrapped in
`#if !defined(ALLOC_INSTRUMENTATION) && !defined(ALLOC_INSTRUMENTATION_STDOUT)`
so the two paths don't produce duplicate symbols when the legacy flag
is on.

#### FastAllocator

In `flow/FastAlloc.cpp`, `FastAllocator<Size>::allocate` currently
contains:

```cpp
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
    recordAllocation(p, Size);
#endif
```

This block is left untouched — it already compiles to nothing in default
builds, costs us nothing to leave, and stays available for whoever might
still wire up the old offline analysis path. Immediately after it we
add an unconditional new line:

```cpp
    memTrackerOnAlloc(p, Size);
```

Likewise in `FastAllocator<Size>::release()`, an unconditional
`memTrackerOnFree(ptr)` is added next to (not replacing) the existing
conditional `recordDeallocation(ptr)`. Size is known at compile time
from the template parameter, so no header lookup is needed.

The conditional global `operator new` / `delete` overrides in
`fdbserver/fdbserver.cpp` also stay in place. Our unconditional
overrides in `flow/MemoryTracker.cpp` are wrapped in
`#if !defined(ALLOC_INSTRUMENTATION) && !defined(ALLOC_INSTRUMENTATION_STDOUT)`
so the two paths don't produce duplicate symbols when somebody builds
with the old flag on.

#### Arena

`Arena` has no per-allocation free path — only block-level lifetime via
`ArenaBlock::create` (`flow/Arena.cpp`) and `ArenaBlock::destroyLeaf`
(`flow/Arena.cpp`). Hooks are placed there:

- `ArenaBlock::create`: after the block is allocated and before return,
  call `memTrackerOnAlloc(blockPtr, blockSizeBytes)`. The captured stack
  is whoever in user code triggered the arena to grow — typically the
  caller doing a large `new (arena) ...` that overflowed the current
  block.
- `ArenaBlock::destroyLeaf`: before returning the block to its
  underlying allocator (`FastAllocator<N>::release` or `free`), call
  `memTrackerOnFree(blockPtr)`.

This is **block-level attribution**. A single `Arena` containing many
small `new (arena) Foo` allocations is attributed to whichever calls
forced new blocks to be created, not to the `new (arena) Foo` calls
themselves. This is the right granularity for finding "who is making the
arena grow" — the more interesting question — and avoids the need for an
allocate-time hook on every bump-pointer call.

Note: The tracker's `memTrackerOnAlloc` runs inside the Arena's
allocation path, which itself runs inside callers that may already hold
locks. Because the tracker takes only its own private spinlock and
doesn't recurse into any FDB-visible state (no `g_network`, no
`g_random`, no Arena allocations), this is safe.

### Reporting

`memTrackerDump(int64_t bytesThreshold)` is called from
`flow/SystemMonitor.cpp` next to the existing `MemoryMetrics` event,
gated by a knob. The default cadence is **once every 10 minutes** in
production (knob-controlled via `MEMORY_TRACKING_REPORT_INTERVAL`);
simulation defaults to 30 s for test exercise.

Rather than logging a fixed top-N, the dump emits a `MemoryTrackerSite`
event for every site whose live bytes exceed
`MEMORY_TRACKING_REPORT_BYTES_THRESHOLD`. The default threshold is
**80 MB**, chosen as roughly 1% of the target RSS for a production
fdbserver (~8 GB). Sites smaller than that are not load-bearing for
RSS-level investigations and would only add log volume; sites above it
are the ones worth attributing. A pure top-N has the wrong shape for
this — top-50 against a process that genuinely has only three
heavyweight sites still emits 47 noise events, while a process with
hundreds of meaningful sites silently truncates at 50. A byte
threshold scales naturally to the actual distribution.

**Degraded mode.** When `MEMORY_TRACKING_LIVE_TRACKING` is `false`,
the live-block side table is not maintained, so `onFree` is a no-op
and `liveBytes` / `liveCount` / `peakBytes` are never decremented —
they end up tracking the cumulatives. The threshold filter still
works, but it now reports "any site that has ever allocated ≥
threshold bytes" rather than "any site currently holding ≥
threshold". Operators using this mode should read the dump
accordingly.

```cpp
if (FLOW_KNOBS->MEMORY_TRACKING_REPORT_INTERVAL > 0 &&
    now() - lastDump >= FLOW_KNOBS->MEMORY_TRACKING_REPORT_INTERVAL) {
    memTrackerDump(FLOW_KNOBS->MEMORY_TRACKING_REPORT_BYTES_THRESHOLD);
    lastDump = now();
}
```

Implementation: take the spinlock; copy the aggregation table values
into a local `std::vector<CallSite>` (backed by `std::malloc`; the
`gInMemTracker` reentrancy flag prevents recursion into the tracker);
release the lock; sort by `liveBytes` descending;
filter to entries with `liveBytes >= bytesThreshold`; emit one
TraceEvent per qualifying site with stats and raw addresses (no
per-site `addr2line` command):

```
TraceEvent("MemoryTrackerSite")
    .detail("Fingerprint", format("%016" PRIx64, s.fingerprint))
    .detail("LiveBytes", s.liveBytes)
    .detail("LiveCount", s.liveCount)
    .detail("PeakBytes", s.peakBytes)
    .detail("CumulativeBytes", s.cumulativeBytes)
    .detail("CumulativeAllocs", s.cumulativeAllocs)
    .detail("ForceSampledCount", s.forceSampledCount)
    .detail("Frame0", format("%p", s.exemplarFrames[0]))
    .detail("Frame1", format("%p", s.exemplarFrames[1]));
    // ... up to FrameN
```

Then emit **one** combined `addr2line`-style command for the entire
dump as its own TraceEvent (`MemoryTrackerAddrCmd`), listing every
qualifying site's exemplar frames in dump order with PIE-relative
addresses. A human or AI consumer cuts and pastes this single
command and gets every reported site's stacks resolved in one shot.

Also emit one summary event per dump:

```
TraceEvent("MemoryTrackerSummary")
    .detail("SitesTracked", aggregationTable.size())   // unique call-site fingerprints tracked
    .detail("SitesReported", qualifying.size())        // sites that crossed the byte threshold this dump
    .detail("LiveBlocks", liveMap.size())              // currently live sampled blocks
    .detail("LiveBytesTotal", liveBytesTotal)          // sum of bytes across currently live sampled blocks
    .detail("LiveBlocksTotal", liveBlocksTotal)        // running counter; equivalent to LiveBlocks above
    .detail("CumulativeAllocs", cumulativeAllocsTotal)
    .detail("CumulativeBytes", cumulativeBytesTotal)
    .detail("SamplesEmitted", samplesEmittedSinceStart)
    .detail("SampleInverse", FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE)
    .detail("ForceSampleBytes", FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES)
    .detail("ReportBytesThreshold", bytesThreshold);
```

The running totals (`LiveBytesTotal`, `LiveBlocksTotal`,
`CumulativeAllocs`, `CumulativeBytes`) are maintained as `int64_t`
globals updated under the same spinlock on every sample. They are
*sampled* totals, not population totals; to estimate population,
multiply by the sample inverse. (Force-sampled large allocations
should be deducted before multiplying — easiest to track them in
their own counter pair if this estimate matters.) `LiveBlocks` is a
snapshot of the live-block table size at dump time and is equivalent
to `LiveBlocksTotal`; both are emitted for now and one may drop in a
cleanup.

### Knobs (FlowKnobs, since clients use Arena/new too)

| Knob | Default (prod) | Default (sim) | Meaning |
|---|---|---|---|
| `MEMORY_TRACKING_SAMPLE_INVERSE` | 100 | 2 | 0=off, N=1-in-N |
| `MEMORY_TRACKING_FORCE_SAMPLE_BYTES` | 100000 | 100000 | Always sample allocations ≥ this many bytes; `INT64_MAX` disables force-sample |
| `MEMORY_TRACKING_LIVE_TRACKING` | true | true | When false, skip the pointer-keyed live-block table and report cumulative-only stats |
| `MEMORY_TRACKING_REPORT_INTERVAL` | 600.0 | 30.0 | Seconds between dumps; 0 disables |
| `MEMORY_TRACKING_REPORT_BYTES_THRESHOLD` | 80000000 | 80000000 | Sites with live bytes ≥ this are reported each dump (~1% of an 8 GB target RSS) |
| `MEMORY_TRACKING_FRAMES` | 6 | 6 | Captured stack depth (1–10) |

Buggify the inverse to 1 (sample everything) in a small fraction of
simulation runs.

### Files

New:
- `flow/MemoryTracker.cpp` — out-of-line sample paths
  (`memTrackerSampleAlloc`, `memTrackerSampleFree`), live-block table,
  aggregation table, `memTrackerDump`, `memTrackerForEachSite`, and
  global `operator new`/`delete` overloads (wrapped in
  `#if !defined(ALLOC_INSTRUMENTATION) && !defined(ALLOC_INSTRUMENTATION_STDOUT)`
  to avoid duplicate symbols against the legacy framework).
- `flow/include/flow/MemoryTracker.h` — header-inlined hot-path
  entry points (`memTrackerOnAlloc`/`OnFree`), declarations of the
  out-of-line entry points, `memTrackerDump`, `memTrackerForEachSite`.

Modified:
- `flow/FastAlloc.cpp` — add unconditional
  `memTrackerOnAlloc`/`OnFree` calls *next to* (not replacing) the
  existing conditional `recordAllocation`/`recordDeallocation` calls.
- `flow/Arena.cpp` — add hooks in `ArenaBlock::create` and
  `ArenaBlock::destroyLeaf`.
- `flow/SystemMonitor.cpp` — periodic dump call.
- `flow/Knobs.h`, `flow/Knobs.cpp` — six new FlowKnobs (see Knobs
  table above).
- `flow/CMakeLists.txt` — register `MemoryTracker.cpp`.

Reused as-is:
- `platform::format_backtrace` (`flow/Platform.cpp`).
- `platform::ImageInfo` (`flow/include/flow/Platform.h`).

Not touched:
- The `ALLOC_INSTRUMENTATION` block in `flow/include/flow/FastAlloc.h`
  and its callers in `flow/FastAlloc.cpp` (the conditional
  `recordAllocation`/`recordDeallocation` lines) and
  `fdbserver/fdbserver.cpp` (the conditional `operator new`
  overrides). All of these compile to nothing in default builds and
  stay available for whoever might still wire up the old offline
  analysis path.

### Determinism in simulation

Sampling state (`gInMemTracker`, `gMemTrackerCounter`, the xorshift
seed) lives in thread local storage. Sim2 runs single-threaded, so the sequence of
sampling decisions is fully replayable across runs with the same seed.
The tracker never reads `g_random` or `g_network->now()`, never
allocates from FDB-visible heaps, and never takes any lock that
participates in Sim2 ordering. The sample dump cadence is driven by
`now()`, but only inside `SystemMonitor.cpp` where time-driven cadence
is already deterministic in simulation.

`BUGGIFY` flips `MEMORY_TRACKING_SAMPLE_INVERSE` to 1 in some seeds so
the tracker is exercised at 100% under fault injection.

## Alternatives Considered

### A1. In-band header (store backtrace in the allocated block)

Original instinct: prepend an 8–16 byte header to each sampled
allocation containing the fingerprint, look it back up on free. Fast
free-side lookup (no hash).

Rejected because:
- `operator new` array allocations have a separate cookie; mixing in a
  header breaks `delete[]`.
- C++17 `std::align_val_t` overloads expect specific alignment that a
  prepended header would invalidate.
- `FastAllocator<Size>` uses fixed size classes; a header changes the
  effective payload size and would force a different size class for
  sampled vs. unsampled allocations, complicating `release()`.
- `Arena` allocations are often 4–16 bytes; an 8-byte header is 50–200%
  overhead on those allocations.

A side table costs one hash lookup on free in exchange for not
disturbing layout. At 1% sampling the side table is small and the
lookup is one cache line in the common case.

### A2. `backtrace(3)` / libunwind for stack capture

The existing `ALLOC_INSTRUMENTATION` framework uses glibc `backtrace(3)`,
which on first call touches the symbol table and is generally
microseconds per call. libunwind is faster (sub-microsecond) but still
slower than frame-pointer walking by an order of magnitude.

Rejected because frame pointers are already enabled globally
(`-fno-omit-frame-pointer` in `cmake/ConfigureCompiler.cmake`), and
a hand-rolled FP walk is ~100 ns for 6 frames — fast enough to leave on
in production. Falling back to libunwind is feasible if we ever need to
support a build configuration that omits frame pointers.

### A3. `--wrap=malloc` to intercept libc `malloc`/`free`

Linker `--wrap` rewrites direct calls to `malloc` into `__wrap_malloc`,
catching the long tail of allocations that bypass `operator new`
(`posix_memalign`, OpenSSL's `OPENSSL_malloc`, RocksDB's cacheline-aligned
allocators, third-party compression libraries).

Deferred from v1 because:
- FDB's own code allocates almost exclusively through `operator new`,
  `FastAllocator`, and `Arena`, all of which are covered by direct
  hooks.
- RocksDB's STL containers and internal `Arena` slabs allocate via
  `operator new[]` and are caught by the operator-new override.
- The remaining gap (aligned allocations and OpenSSL/compression) is
  small in byte volume and not currently a known source of mystery
  memory growth.
- `--wrap` requires changes at every link line and slightly complicates
  static-analysis tools that don't model the rename.

Trivial to add later if production data shows we need it.

### A4. Byte-rate sampling vs count-rate sampling

True byte-rate sampling (per-allocation Bernoulli draw with probability
proportional to size, à la jemalloc/tcmalloc heap profilers) gives
unbiased per-site byte estimates but adds per-allocation work and
complicates determinism. The hybrid we ship — count-rate for small
allocations plus unconditional sampling above
`MEMORY_TRACKING_FORCE_SAMPLE_BYTES` — captures the case byte-rate
would have caught (rare-but-huge allocations) without the extra
hot-path cost.

### A5. Per-thread aggregation tables vs single global lock

Per-thread tables merged on dump avoid all lock contention on the hot
path. With FDB's threading model (one network thread plus a small
number of I/O / RocksDB threads), the merge cost is small.

Chose single global spinlock for v1 because:
- At 1% sampling + 100K alloc/sec the lock fires only 1K times/sec.
  Uncontended spinlock acquire is ~20 ns. Total CPU cost ~20 µs/sec.
- Per-thread tables require a thread-registration mechanism (the
  tracker has to know about thread births/deaths so it can merge) and a
  thread-local pointer in TLS; non-trivial to get right under
  `pthread_create`-spawned background threads in third-party libraries
  whose lifecycle FDB doesn't own.
- Single-lock is dead simple to reason about and easy to retrofit into
  per-thread later if profiling justifies it.

### A6. Extending `ALLOC_INSTRUMENTATION` vs a new module

The existing framework has the right shapes (`memSample`,
`backTraceLookup`, `memSample_entered`). Could in principle be
generalized.

Rejected because:
- The existing framework is gated behind a compile-time flag and was
  designed for offline analysis with the slow `backtrace(3)`. Repurposing
  it requires removing the compile gate, replacing the capture path,
  changing the lock discipline, replacing the data structures, and
  adding the dump path — at which point the rewrite is cleaner as a
  fresh module.
- Two coexisting paths under different flags are confusing for
  maintainers.

The new module supersedes it. The dead code stays in place behind its
`#ifdef` for v1 and is a candidate for removal in a follow-up cleanup PR.

### A7. jemalloc's built-in heap profiler

jemalloc has a mature sampled-stack heap profiler enabled via
`MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:14`. Zero
implementation cost.

Rejected as a complete solution because:
- Covers only `malloc` and `operator new`, not `FastAllocator` or
  `Arena`.  We suspect that most of FDB's allocation byte volume goes through the latter
  two.
- Requires a custom-built jemalloc with `--enable-prof` (the FDB
  `USE_CUSTOM_JEMALLOC` build option). Default Linux distro jemalloc
  packages don't enable profiling.
- Not available on macOS / FreeBSD / Windows where jemalloc is
  optional or absent.

We could enable it *in addition* to the FDB tracker for the libc-malloc
tail. Out of scope for v1.

### A8. `__FILE__`/`__LINE__` macro instrumentation

An alternative considered: replace every `new` / `arena.allocate` /
`malloc` call with a macro that captures `__FILE__` / `__LINE__`,
storing those instead of return-address frames. Avoids the need for
`addr2line`.

Rejected because:
- File/line at the *allocator* layer is uninformative
  (always `Arena.cpp` or `FastAlloc.cpp`). To get useful attribution
  the macro would have to be applied at every *caller* site —
  thousands of `new` and `arena.allocate` call sites. This is
  invasive churn and breaks templates that allocate on behalf of
  callers (the template body sees its own `__FILE__`, not the
  instantiation's).
- Return-address frames give correct multi-frame attribution
  with no source changes. Symbolization via `addr2line` is offline
  but already supported in this repo.

## Testing Considerations

### Unit tests

A new `flow/MemoryTrackerTest.cpp` exercises:

- **Sampling correctness.** With sample inverse 1, allocate 100K
  objects in a tight loop; confirm the aggregation table has one entry
  for the test function with `cumulativeAllocs == 100K`. Free them;
  confirm `liveCount` returns to 0 and `cumulativeAllocs` stays at 100K.
- **Off-switch.** With sample inverse 0, allocate 100K objects; confirm
  the aggregation table is empty.
- **Reentrancy guard.** Force a path where the tracker itself
  allocates during sampling (e.g., aggregation table resize); confirm
  the `gInMemTracker` flag prevents re-entry into the tracking path.
- **Frame-pointer walk depth.** Allocate from increasingly deeper call
  stacks; confirm the captured frame count tracks `MEMORY_TRACKING_FRAMES`.
- **Free of un-tracked pointer.** `memTrackerOnFree` for a pointer that
  was never sampled is a no-op (correct against the live-block table
  miss).

### Simulation tests

- **Determinism.** Run `bin/fdbserver -r simulation -f tests/fast/Cycle.toml`
  with `--knob_memory_tracking_sample_inverse=2` repeatedly with the
  same `-s <seed>`. The aggregate counts (`MemoryTrackerSummary` event)
  must match across runs.
- **Coverage.** Run any small simulation test with sample inverse 1 and
  confirm `MemoryTrackerSite` events include known-heavy sites: arena
  growth from `MutationListRef` parsing, FastAlloc usage from
  `Reference<>` ref-count blocks, `operator new` from `std::shared_ptr`
  control blocks inside RocksDB-touching code paths.
- **Buggified inverse.** Exercise the tracker at inverse=1 in a
  fault-injection workload (e.g., `tests/slow/CycleWithKills.toml`).
  Confirm no crashes, no determinism failures.

### Microbenchmarks

The targets below are 5% rather than R2's 0.1% end-to-end CPU ceiling
because a tight alloc/free loop is the pessimal case: there is no real
work between hook calls to amortize the overhead against, so the
per-allocation cost shows up undiluted. R2's 0.1% applies to realistic
workloads.

- **Un-sampled hot path.** A new `bin/fdbserver -r unittests
  -f /flow/MemoryTracker/perfUnsampled` allocates and frees in a tight
  loop with sample inverse 0; compare against a baseline build with the
  hooks compiled out (`#ifdef MEMTRACKER_DISABLED`). Target: < 5%
  delta.
- **Sampled path.** Same loop with sample inverse 100; target: < 5%
  delta vs. the un-sampled baseline at 100K alloc/sec.

### Strip-aware symbolization spot-check

Build the release binary, strip it, paste an `AddrCmd` field from a
trace into a shell pointing at the matching `.debug` sidecar, confirm
symbol resolution works.

### Coverage spot-check via sentinel functions

The "did the tracker actually capture each allocation path?" check is
fully automated using sentinel functions and direct introspection of
the aggregation table — no log parsing, no `addr2line`, no eyeballing.

Add an introspection API:

```cpp
void memTrackerForEachSite(std::function<void(const CallSite&)>);
```

The unit test:

1. Sets `MEMORY_TRACKING_SAMPLE_INVERSE = 1` (sample everything) and
   resets the aggregation table.
2. Calls a sentinel function `triggerOperatorNewSentinel()` that does
   N `new int[k]` / `delete[]` pairs.
3. Calls `triggerFastAllocSentinel<32>()` that does N
   `FastAllocator<32>::allocate/release` pairs.
4. Calls `triggerArenaSentinel()` that does N arena growths large
   enough to force `ArenaBlock::create` calls.
5. Captures the address of each sentinel via its function pointer:
   `void* expected = (void*)&triggerOperatorNewSentinel;`
6. Iterates the aggregation table via `memTrackerForEachSite`, asserts
   that for each sentinel address there exists at least one site
   whose `exemplarFrames` contains an address in the
   `[&triggerXSentinel, &triggerXSentinel + reasonableSize)` range,
   and that the matching site's `cumulativeAllocs == N`.

This tests the full pipeline (sampling → capture → fingerprint →
aggregation) end-to-end. The sentinel-address technique compares raw
return addresses to function-pointer values at runtime, so it is
deterministic, fast, and works on stripped builds.

## Observability/Supportability Considerations

### Self-metrics emitted by the tracker

Every `MemoryTrackerSummary` event reports:
- `SitesTracked` — unique call-site fingerprints in the aggregation table.
- `SitesReported` — sites that crossed the byte threshold this dump.
- `LiveBlocks` / `LiveBlocksTotal` — currently live sampled blocks
  (two equivalent paths; see Reporting).
- `SamplesEmitted` — cumulative samples that produced an aggregate
  update.

These are the operator-visible knobs for "is the tracker working" and
"is the tracker hurting us".

## Rollout/Migration Considerations

### Phased rollout

1. Land code with `MEMORY_TRACKING_SAMPLE_INVERSE` defaulting to 0
   (off) but compiled in — overriding the steady-state default in the
   knobs table for this initial commit only. Verify no regression in
   cluster-level performance testing.
2. Flip the production default to 100 (1% sampling — the steady-state
   value shown in the knobs table) in a follow-up commit.
3. Confirm `MemoryTrackerSite` events flow through the trace pipeline
   end-to-end.
4. On a case by case basis, test in developer-specific performance
   cluster testing for specific use cases related to storage server
   memory use.

### Client library implications

`libfdb_c.so` and other client artifacts link `flow/`. The tracker
binary path is therefore present in client binaries linked into
embedding applications. Implications:

- Sampling defaults to 0 in client contexts. We do not have any plans
  or requirements currently to enable this for client library users.
- `operator new` overrides apply globally inside the client process,
  affecting host application allocations too. Most embedding apps will
  not enable sampling, so the cost is just the extra `if
  (gInMemTracker)` check on every `operator new`.

We will need to test client bindings to ensure that overriding operator new
does not cause problems for existing clients.

If always-compiled tracker code in the client library ends up not
working, should gate it behind a `-DENABLE_MEMORY_TRACKING_CLIENT=OFF`
CMake flag for client builds specifically.

### `ALLOC_INSTRUMENTATION` coexistence

The existing framework in `flow/include/flow/FastAlloc.h` and
its associated conditional code paths (in `flow/FastAlloc.cpp` and
`fdbserver/fdbserver.cpp`) remain in place. The new tracker
sits next to it, gated only by the inverse complement of the same
`#ifdef`s where they would otherwise produce duplicate symbols (the
`operator new` overrides). No removal is planned.

### Rollback

Setting `MEMORY_TRACKING_SAMPLE_INVERSE=0` and
`MEMORY_TRACKING_REPORT_INTERVAL=0` at runtime fully disables the
tracker without a binary rebuild. The hot-path cost reduces to the TLS
load + branch (`gInMemTracker` check) and is observably zero on
benchmarks.

If the operator-new overrides themselves need to be rolled back (e.g.,
they cause issues with a third-party library that has its own global
new override), a build flag `-DDISABLE_GLOBAL_NEW_OVERRIDE` could omit
just those overloads while keeping FastAlloc and Arena tracking. Not
included in v1; can be added if such an issue surfaces.
