# FDB Memory Tracker — Per-Call-Site Allocation Attribution

## Objective

Make it easier to debug memory leaks and generally to understand the
principle consumers of memory in FDB.  Specifically: add a sampled,
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

A partial framework already exists in `flow/include/flow/FastAlloc.h:60-112`
under the `ALLOC_INSTRUMENTATION` compile flag. It defines the right
shapes — `memSample` (pointer → backtrace hash + size), `backTraceLookup`
(hash → aggregate), a `memSample_entered` reentrancy flag — and global
`operator new`/`delete` overrides in `fdbserver/fdbserver.cpp:771-818`. But
it relies on glibc `backtrace(3)` (microseconds per call), is gated behind
a non-default compile flag, and was designed for offline analysis, not
production. In practice it is dead code: builds that turn it on are too
slow to run real workloads.

The infrastructure needed to make a lightweight version cheap is already
in place:

- Frame pointers are globally enabled
  (`cmake/ConfigureCompiler.cmake:212`: `-fno-omit-frame-pointer`), so a
  manual frame-pointer walk replaces `backtrace(3)` and runs in ~100 ns.
- `platform::format_backtrace` (`flow/Platform.cpp:3517`) already emits a
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

R3. **Default-on in production and simulation.** Production binaries ship
    with sampling enabled at 1% (one in 100 allocations). Simulation runs
    at 50% (one in two) so the tracker is exercised under fault
    injection.

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

R7. **Periodic reporting.** A configurable cadence (default 60 s in
    production, 30 s in simulation) emits one TraceEvent per top-N
    sites (default 50), each containing raw return-address frames and a
    ready-to-paste `addr2line` command. The dump itself runs in under 5
    ms for 50 sites.

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

## Design Overview

Three pieces:

1. **Sampling and capture.** Every allocation site calls a header-inlined
   `memTrackerOnAlloc(p, size)` (and matching `memTrackerOnFree(p)`).
   Both check a thread-local reentrancy flag and a thread-local
   decrementing counter; the un-sampled path is one TLS load + one
   decrement + one branch. On sample, walk the frame-pointer chain for
   4–6 return addresses.

2. **Storage.** Two tables, both backed by `std::malloc` (which bypasses
   our `operator new` hooks):
   - A fingerprint-keyed *aggregation table*
     (`uint64_t → {count, bytes, peakBytes, exemplarFrames[6]}`) where
     the fingerprint is `fnv64(frames)`. Always present.
   - A pointer-keyed *live-block table* (`void* → {fingerprint, size}`)
     so `onFree` can find what `onAlloc` recorded. The pointer is the
     only key available at free time — there is no backtrace to hash —
     so the table is necessarily pointer-keyed if we want to track live
     bytes at all. Optional, gated by the `MEMORY_TRACKING_LIVE_TRACKING`
     knob (default on). When disabled, only cumulative per-site stats
     are tracked and the side table disappears entirely.

3. **Reporting.** A `memTrackerDump(int topN)` walks the aggregation
   table sorted by live bytes, emits a TraceEvent per site with raw
   addresses and the offline `addr2line` command, called periodically
   from `SystemMonitor.cpp`.

Hooks:

| Path | Allocate | Free |
|---|---|---|
| Global `new`/`delete` | new `MemoryTracker.cpp` | same |
| `FastAllocator<Size>` | `FastAlloc.cpp:432` | `FastAlloc.cpp:509` |
| `Arena` | `Arena.cpp` `ArenaBlock::create` | `Arena.cpp` `ArenaBlock::destroyLeaf` |

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

The counter is reseeded inside `memTrackerSampleAlloc` to
`1 + (xorshift32(&seed) % (2 * INV))`, giving mean `INV` with no aliasing
to fixed allocation patterns. When `INV == 0` (off-switch), the reseed
sets the counter to `INT_MAX` so the un-sampled branch is taken
indefinitely with no further work.

`gMemTrackerCounter` starts at 1 so the first allocation per thread is
always sampled — guarantees test workloads with very few allocations still
exercise the path.

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
incremented on this path; a site's `ForceSampledFraction` in the dump
output lets consumers tell whether a site's stats are predominantly
"every alloc above 100 KB" vs "1% of allocs"  — without that, two
sites with the same `cumulativeAllocs` value can mean very different
population rates.

### Stack capture

```cpp
__attribute__((no_instrument_function, always_inline))
inline int captureStackFP(void** frames, int max) {
    void** fp = (void**)__builtin_frame_address(0);
    int n = 0;
    while (fp && n < max) {
        void* ra = fp[1];
        if (!ra) break;
        frames[n++] = ra;
        void** next = (void**)fp[0];
        if (next <= fp) break;   // sanity: stack grows down
        fp = next;
    }
    return n;
}
```

Skips no frames at capture; the caller (`memTrackerSampleAlloc`) is
responsible for stripping the topmost 1–2 frames so the captured stack
starts at the real allocation site (not inside the tracker itself).

Relies on `-fno-omit-frame-pointer` already set globally
(`cmake/ConfigureCompiler.cmake:212`). On x86_64 and aarch64 each frame
is two adjacent words (saved FP, saved RA), so the walk is one indirect
load per frame: ~100 ns for 6 frames.

Caveats and mitigations:

- Code compiled with `-fomit-frame-pointer` (third-party static libs not
  rebuilt with project flags) terminate the walk early. Acceptable: we
  still get the FDB-side prefix, which is the main thing we care about.
- Signal handlers can leave a transiently bad FP chain mid-walk; the
  `next <= fp` sanity check bails out of those.
- ASAN/MSAN builds may instrument the FP chain. The tracker is a
  no-op-equivalent in those builds (sampling defaults can be flipped to
  0 in CMake when sanitizers are on).

### Storage

#### Live-block table

Open-addressing hash, key `void*`, value `{uint64_t fingerprint, uint32_t
size}` (16 bytes/entry). Sized for ~1.5× expected live sampled
allocations to keep load factor under 0.7. Resize doubles capacity. All
backing memory comes from `std::malloc` directly: this bypasses our
`operator new` hooks (we override `operator new`, not libc `malloc`), so
the tracker's own allocations cannot recurse back into the tracking
path. The thread-local `gInMemTracker` flag is the single line of
defense and is sufficient on its own; we don't need a private slab pool.

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
struct CallSite {
    uint64_t fingerprint;
    int64_t  liveBytes;
    int64_t  liveCount;
    int64_t  peakBytes;
    int64_t  cumulativeAllocs;   // never decremented
    int64_t  cumulativeBytes;    // never decremented
    void*    exemplarFrames[6];
    uint8_t  exemplarFrameCount;
};
```

`fingerprint = fnv64(frames[0..n])` computed once at sample time. The
exemplar frames are stored from the first allocation that produced the
fingerprint and are never updated — different call paths that happen to
collide on a fingerprint hash are very rare with 64-bit fnv and 4–6
frames; if it matters we can switch to xxhash or store all observed
frames per fingerprint.

`peakBytes = max(peakBytes, liveBytes)` updated on each alloc.

### Hook sites

#### Global `operator new` / `delete`

`MemoryTracker.cpp` defines the ~12 standard global overloads — `new`,
`new[]`, `delete`, `delete[]`, sized variants, `nothrow_t` variants, and
the C++17 `std::align_val_t` overloads. Each calls `std::malloc` /
`std::free` (or `aligned_alloc` for the aligned variants) and then
`memTrackerOnAlloc`/`OnFree`. The conditional overrides in
`fdbserver/fdbserver.cpp:771-818` are deleted; the new module's
unconditional overrides cover every binary that links flow.

#### FastAllocator

`flow/FastAlloc.cpp:432` (in `FastAllocator<Size>::allocate`) currently
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

Likewise at `FastAlloc.cpp:509` in `release()`, an unconditional
`memTrackerOnFree(ptr)` is added next to (not replacing) the existing
conditional `recordDeallocation(ptr)`. Size is known at compile time
from the template parameter, so no header lookup is needed.

The conditional global `operator new` / `delete` overrides in
`fdbserver/fdbserver.cpp:771-818` also stay in place. Our unconditional
overrides in `flow/MemoryTracker.cpp` are wrapped in
`#if !defined(ALLOC_INSTRUMENTATION) && !defined(ALLOC_INSTRUMENTATION_STDOUT)`
so the two paths don't produce duplicate symbols when somebody builds
with the old flag on.

#### Arena

`Arena` has no per-allocation free path — only block-level lifetime via
`ArenaBlock::create` (`flow/Arena.cpp:420-525`) and `ArenaBlock::destroyLeaf`
(`flow/Arena.cpp:567-610`). Hooks are placed there:

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

`memTrackerDump(int topN)` is called from `flow/SystemMonitor.cpp` next
to the existing `MemoryMetrics` event, gated by a knob:

```cpp
if (FLOW_KNOBS->MEMORY_TRACKING_REPORT_INTERVAL > 0 &&
    now() - lastDump >= FLOW_KNOBS->MEMORY_TRACKING_REPORT_INTERVAL) {
    memTrackerDump(FLOW_KNOBS->MEMORY_TRACKING_TOP_N);
    lastDump = now();
}
```

Implementation: take the spinlock; copy the aggregation table values
into a local `std::vector<CallSite>` (allocated via the slab pool, not
`operator new`); release the lock; sort by `liveBytes` descending; emit
one TraceEvent per top-N site:

```
TraceEvent("MemoryTrackerSite")
    .detail("Fingerprint", format("%016" PRIx64, s.fingerprint))
    .detail("LiveBytes", s.liveBytes)
    .detail("LiveCount", s.liveCount)
    .detail("PeakBytes", s.peakBytes)
    .detail("CumulativeBytes", s.cumulativeBytes)
    .detail("CumulativeAllocs", s.cumulativeAllocs)
    .detail("Frame0", format("%p", s.exemplarFrames[0]))
    .detail("Frame1", format("%p", s.exemplarFrames[1]))
    // ... up to FrameN
    .detail("AddrCmd", platform::format_backtrace(s.exemplarFrames, s.exemplarFrameCount));
```

`format_backtrace` (`flow/Platform.cpp:3517`) is reused as-is. It
subtracts the PIE load offset and emits the absolute-path
`/usr/local/bin/llvm-addr2line -e <bin> -p -C -f -i 0xADDR ...` command,
making each TraceEvent self-contained for offline symbolization.

Also emit one summary event per dump:

```
TraceEvent("MemoryTrackerSummary")
    .detail("SitesTracked", aggregationTable.size())
    .detail("LiveBlocks", liveBlocksTotal)            // sampled live blocks across all sites
    .detail("LiveBytes", liveBytesTotal)              // sampled live bytes across all sites
    .detail("CumulativeAllocs", cumulativeAllocsTotal)
    .detail("CumulativeBytes", cumulativeBytesTotal)
    .detail("SamplesEmitted", samplesEmittedSinceStart)
    .detail("SamplesDroppedReentry", reentrantBailouts)
    .detail("SamplesDroppedTableFull", tableFullDrops)
    .detail("DumpDurationMs", dumpDurationMs)
    .detail("SampleInverse", FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE)
    .detail("ForceSampleBytes", FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES);
```

The four totals (`LiveBlocks`, `LiveBytes`, `CumulativeAllocs`,
`CumulativeBytes`) are maintained as `int64_t` globals updated under
the same spinlock on every sample. They are *sampled* totals, not
population totals; to estimate population, multiply by the sample
inverse. (Force-sampled large allocations should be deducted before
multiplying — easiest to track them in their own counter pair if this
estimate matters.)

### Knobs (FlowKnobs, since clients use Arena/new too)

| Knob | Default (prod) | Default (sim) | Meaning |
|---|---|---|---|
| `MEMORY_TRACKING_SAMPLE_INVERSE` | 100 | 2 | 0=off, N=1-in-N |
| `MEMORY_TRACKING_FORCE_SAMPLE_BYTES` | 100000 | 100000 | Always sample allocations ≥ this many bytes; `INT64_MAX` disables force-sample |
| `MEMORY_TRACKING_LIVE_TRACKING` | true | true | When false, skip the pointer-keyed live-block table and report cumulative-only stats |
| `MEMORY_TRACKING_REPORT_INTERVAL` | 60.0 | 30.0 | Seconds between dumps; 0 disables |
| `MEMORY_TRACKING_TOP_N` | 50 | 50 | Number of sites per dump |
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
- `flow/FastAlloc.cpp` (lines 432, 509) — add unconditional
  `memTrackerOnAlloc`/`OnFree` calls *next to* (not replacing) the
  existing conditional `recordAllocation`/`recordDeallocation` calls.
- `flow/Arena.cpp` — add hooks in `ArenaBlock::create` and
  `ArenaBlock::destroyLeaf`.
- `flow/SystemMonitor.cpp` — periodic dump call.
- `flow/Knobs.h`, `flow/Knobs.cpp` — six new FlowKnobs (see Knobs
  table above).
- `flow/CMakeLists.txt` — register `MemoryTracker.cpp`.

Reused as-is:
- `platform::format_backtrace` (`flow/Platform.cpp:3517`).
- `platform::ImageInfo` (`flow/include/flow/Platform.h:457-468`).

Not touched:
- The `ALLOC_INSTRUMENTATION` block in `flow/include/flow/FastAlloc.h:60-112`
  and its callers in `flow/FastAlloc.cpp` (the conditional
  `recordAllocation`/`recordDeallocation` lines) and
  `fdbserver/fdbserver.cpp:771-818` (the conditional `operator new`
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
(`-fno-omit-frame-pointer` in `cmake/ConfigureCompiler.cmake:212`), and
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

User mentioned this as an alternative: replace every `new` /
`arena.allocate` / `malloc` call with a macro that captures
`__FILE__`/`__LINE__`, store those instead of return-address frames.
Avoids the need for `addr2line`.

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

### A9. Storing the small backtrace inside the allocated block

User originally proposed this; covered under A1. Side table chosen.

## Testing Considerations

### Unit tests

A new `flow/MemoryTrackerTest.cpp` exercises:

- **Sampling correctness.** With sample inverse 1, allocate 100K
  objects in a tight loop; confirm the aggregation table has one entry
  for the test function with `cumulativeAllocs == 100K`. Free them;
  confirm `liveCount` returns to 0 and `cumulativeAllocs` stays at 100K.
- **Off-switch.** With sample inverse 0, allocate 100K objects; confirm
  the aggregation table is empty.
- **Reentrancy guard.** Force a path where the tracker's own slab pool
  has to grow during sampling; confirm no infinite recursion.
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

- **Un-sampled hot path.** A new `bin/fdbserver -r unittests
  -f /flow/MemoryTracker/perfUnsampled` allocates and frees in a tight
  loop with sample inverse 0; compare against a baseline build with the
  hooks compiled out (`#ifdef MEMTRACKER_DISABLED`). Target: < 5%
  delta.

// NOTE: I changed 1% to 5% above. This is because 1% total overhead
// (stated elsewhere as a requirement) 
// accounts for the system doing more than just allocating and freeing
// in a tight loop.  In a tight loop, the overhead can be more.

- **Sampled path.** Same loop with sample inverse 100; target: < 5%
  delta vs. the un-sampled baseline at 100K alloc/sec.

// NOTE: again 5% here.

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
- `SitesTracked` — current size of aggregation table.
- `LiveBlocks` — current size of live-block table.
- `SamplesEmitted` — cumulative samples that produced an aggregate
  update.
- `SamplesDroppedReentry` — sampled allocations skipped because the
  reentrancy guard was already set.
- `SamplesDroppedTableFull` — sampled allocations skipped because the
  slab pool ran out.
- `DumpDurationMs` — how long the dump itself took.

These are the operator-visible knobs for "is the tracker working" and
"is the tracker hurting us".

## Rollout/Migration Considerations

### Phased rollout

1. Land code with `MEMORY_TRACKING_SAMPLE_INVERSE` defaulting to 0
   (off) but compiled in. Verify no regression in cluster level
   performance testing.
2. Flip the production default to 100 (1% sampling) in a follow-up
   commit.
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

The existing framework in `flow/include/flow/FastAlloc.h:60-112` and
its associated conditional code paths (in `flow/FastAlloc.cpp` and
`fdbserver/fdbserver.cpp:771-818`) remain in place. The new tracker
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
