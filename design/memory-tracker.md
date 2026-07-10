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
(~50%) in simulation, where it must remain deterministic. (That ≤1%
overhead is the *target* that gates leaving it on; the v1 single-lock
implementation does not yet meet it at high allocation rates, so it ships
**off** by default pending a lock-sharding follow-up — see Performance and
Rollout.)

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
`operator new`/`delete` overrides (in `fdbserver/GlobalNewDelete.cpp`). But
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

R0. **Cheap enough to leave on in production by default.** Total CPU
    overhead from the tracker at the production default sampling rate must
    stay under 1% end-to-end on realistic workloads. This is a ceiling the
    design must meet; the measured/estimated overhead is reported in the
    detailed design, not here, and the ceiling is not to be relaxed to
    accommodate an implementation.

R1. **Un-sampled hot path.** A non-sampled allocation incurs only one
    thread-local load, one decrement, and one branch. A non-sampled free
    (which has no per-thread counter to gate on) incurs one relaxed read of
    a global enabled flag and one branch. That flag is written only on an
    enabled-state change, so it stays in MESI shared state and the read is a
    cached, uncontended atomic load (~12-15 cycles for an L2 cache hit) —
    never a contended or uncached atomic. No syscalls, no library calls, no
    locks on either path.

R2. **Default-on in production and simulation (post-rollout
    steady state).** Steady-state production default is 1% sampling
    (one in 100 allocations); simulation runs at 50% (one in two) so
    the tracker is exercised under fault injection. The Rollout
    section describes a phased landing where the production default
    starts at 0 and is flipped to the steady-state value after
    baseline performance testing.

R3. **Allocation-path coverage.** All three FDB-owned allocation paths
    are attributed:
    - Global `operator new` / `operator delete` (all standard overloads:
      sized, unsized, array, nothrow, aligned).
    - `FastAllocator<Size>::allocate` / `::release` for every size class.
    - `Arena` allocations, attributed at the `ArenaBlock` granularity
      (one entry per block, sized to the block's byte count).

R4. **Out-of-scope allocators.** Direct libc `malloc`/`free`,
    `posix_memalign`/`aligned_alloc`, `mmap`, and third-party allocators
    that bypass `operator new` are not attributed in v1. RocksDB
    allocations *are* covered via the `operator new` override since
    RocksDB STL containers and internal `Arena` slabs go through global
    `operator new[]`.

R5. **Determinism in simulation.** Sampling state lives entirely in
    thread-local variables and never reads `g_random` or
    `g_network->now()`. Identical workload + seed must produce identical
    aggregate counts.

R6. **Periodic reporting.** A configurable cadence emits one TraceEvent per
    qualifying site, where a site qualifies when its estimated live
    bytes exceed `MEMORY_TRACKING_REPORT_BYTES_THRESHOLD` (see Reporting
    for rationale). Each site event carries the site's
    raw return-address frames together with a ready-to-paste `addr2line`
    command (the `AddrCmd` detail) covering just that site's frames. The dump
    itself runs in under 5 ms when ~50 sites qualify.

R7. **Zero-cost runtime symbolization.** All address-to-symbol mapping
    happens offline. The runtime never calls `backtrace_symbols`,
    `dladdr`, or any DWARF reader.

R8. **Strip-aware.** Reports work against stripped production binaries
    when separate debug info (the `.debug` sidecar this repo's release
    build already produces) is available to the offline tooling.

R9. **Off-switch.** Setting the sample-inverse knob to 0 disables
     sampling at runtime. The alloc hot path remains a TLS load, decrement,
     and branch; the free hot path remains a cached atomic-flag read and a
     branch. No aggregation work, and in particular no lock acquisition,
     happens on either path when disabled.

R10. **Reentrancy safety.** No allocation made by the tracker itself
     (table grows, dump scratch space) re-enters the tracking path. A
     thread-local guard prevents re-entry.

R11. **Side-thread coverage.** Allocations from any thread are
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

1. **Sampling and capture.** Every allocation site calls a
   header-inlined `memTrackerOnAlloc(p, size)` (and matching
   `memTrackerOnFree(p)`).  Both check a thread-local reentrancy flag
   and a thread-local decrementing counter; the un-sampled path is one
   TLS load + one decrement + one branch. On sample, walk the
   frame-pointer chain for 4-6 return addresses (4-6 being initial
   estimate, subject to refinement during development of this
   feature).

2. **Storage.** Two tables, both backed by `std::malloc` (which bypasses
   our `operator new` hooks, so the tracker's own bookkeeping cannot recurse
   into the tracking path):
   - A fingerprint-keyed *aggregation table* (`fnv64(frames) → per-site
     counters`): live / peak / cumulative bytes and counts, plus exemplar
     frames for offline symbolization. Always present.
   - A pointer-keyed *live-block table* (`void* → {fingerprint, size,
     weight}`) so `onFree` can find what `onAlloc` recorded — the pointer is
     the only key available at free time. Gated by the
     `MEMORY_TRACKING_LIVE_TRACKING` knob (default on); when off, only
     cumulative per-site stats are kept.

3. **Reporting.** A `memTrackerDump(int64_t bytesThreshold)` walks the
   aggregation table, emits a TraceEvent per site whose estimated live
   bytes exceed the threshold — each carrying the site's estimated usage,
   raw sampled counters, and a ready-to-paste `addr2line` command for its
   stack — plus one summary event. Called periodically from `SystemMonitor.cpp`.

Hooks:

| Path | Allocate | Free |
|---|---|---|
| Global `new`/`delete` | `fdbserver/GlobalNewDelete.cpp` | `fdbserver/GlobalNewDelete.cpp` |
| `FastAllocator<Size>` | `FastAlloc.cpp` (`::allocate`) | `FastAlloc.cpp` (`::release`) |
| `Arena` | `Arena.cpp` (`ArenaBlock::create`) | `Arena.cpp` (`ArenaBlock::destroyLeaf`) |

## Detailed Design

### Sampling and reentrancy

```cpp
// MemoryTracker.h — header-inlined hot path

extern thread_local bool gInMemTracker;
extern thread_local int  gMemTrackerCounter;
extern thread_local size_t gForceSampleBytes;  // refreshed periodically from FlowKnobs

// Written only on an enabled-state change (rare), so it stays MESI-shared and
// reads are cached/uncontended. Lives on its own cache line.
extern std::atomic<bool> g_memTrackerEnabled;

inline void memTrackerOnAlloc(void* p, size_t n) {
    if (gInMemTracker || !p) return;
    if (--gMemTrackerCounter > 0 && n < gForceSampleBytes) return;  // un-sampled fast path
    gInMemTracker = true;
    memTrackerSampleAlloc(p, n);            // out-of-line; reseeds counter, publishes g_memTrackerEnabled
    gInMemTracker = false;
}

inline void memTrackerOnFree(void* p) {
    if (gInMemTracker || !p) return;
    if (!g_memTrackerEnabled.load(relaxed)) return;  // disabled: no lock, no table probe
    gInMemTracker = true;
    memTrackerSampleFree(p);                // no-op if p not tracked
    gInMemTracker = false;
}
```

A free has no per-thread sampling counter — every free is a candidate to
debit a live entry — so it cannot be gated the way an alloc is. Gating it on
the enabled flag instead keeps the disabled free path lock-free: without this
gate, every hooked `delete`/`FastAllocator::release`/`ArenaBlock` teardown
would acquire the global spinlock even with sampling off, defeating the
off-switch. The flag is written only when the enabled state actually changes
(published from `memTrackerSampleAlloc`, the one place that re-reads the knob),
so its cache line stays shared across cores and the hot-path read is cheap.
Runtime toggling has the same in-flight caveat as `MEMORY_TRACKING_LIVE_TRACKING`:
disabling while entries are live orphans them in the table rather than debiting
them on free.  Note that in practice we don't expect this knob to
flip; fdbserver process restarts are more typical when knobs change.

The counter is reseeded on every sample to a small uniform random
integer with mean `INV`, so the sampling rate averages 1-in-`INV`
without aliasing to fixed allocation patterns. When `INV == 0`
(off-switch) the reseed re-parks the counter at a bounded value rather than
effectively infinity, so a parked thread re-enters the lock-free slow path —
which re-reads the knob and can observe an off→on change — within a bounded
number of allocations. The bail-out does no aggregation work and takes no lock.

The counter's initial value (per-thread) is chosen so the first
allocation a thread sees is always sampled, which guarantees that
test workloads with very few allocations still exercise the path.

**Force-sample-large.** Any allocation at or above
`MEMORY_TRACKING_FORCE_SAMPLE_BYTES` (default ~100 KB) is *always* sampled
regardless of the counter; setting the knob to `-1` disables force-sampling.

Large allocations are rare per second so unconditional
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

Capture is a hand-rolled frame-pointer walk (`captureStackFP`): starting from
`__builtin_frame_address(0)`, follow the saved-FP links, recording the saved
return address at each frame, up to `MEMORY_TRACKING_FRAMES` deep. The caller
(`memTrackerSampleAlloc`) strips the topmost 1–2 tracker frames so the captured
stack starts at the real allocation site rather than inside the tracker.

The **load-bearing safety invariant** is a check on every iteration: each
candidate frame pointer must be word-aligned and lie within the current
thread's stack range — cached once per thread via `pthread_getattr_np` /
`pthread_attr_getstack`, with a bounded fallback around the initial frame if
that fails — *before* it is dereferenced. This is what lets the walk terminate
cleanly instead of chasing a garbage saved-FP into unmapped memory when it
reaches FP-elided code; see "Side-thread safety" for why that is not
hypothetical.

The walk relies on `-fno-omit-frame-pointer`, which is already set
globally (`cmake/ConfigureCompiler.cmake`). On x86_64 and aarch64 each
frame is two adjacent words (saved FP, saved RA), so the walk performs
one indirect load per frame — roughly 100 ns for 6 frames.

Caveats and mitigations:

- Code compiled with `-fomit-frame-pointer` (glibc's pthread
  shutdown / TLS-destructor machinery, third-party static libs not rebuilt
  with project flags) does not terminate the walk cleanly — the saved-FP slot
  at the boundary is uninitialized garbage, not `NULL`, and a naive `next <= fp`
  check is insufficient. The stack-bounds invariant above is the mitigation;
  see "Side-thread safety" for the empirical repro and full chain analysis.
- Signal handlers can leave a transiently bad FP chain mid-walk; the same
  bounds check bails out.
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
hard requirement (see R11), not a nice-to-have.

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

Hash table, key `void*`, value `{uint64_t fingerprint, uint64_t size,
int64_t weight}`. All backing memory comes from `std::malloc`
directly: this bypasses our `operator new` hooks (we override
`operator new`, not libc `malloc`), so the tracker's own allocations
cannot recurse back into the tracking path. The thread-local
`gInMemTracker` flag is the single line of defense and is sufficient
on its own; we don't need a private slab pool.

`size` is stored full-width (a narrower field would under-debit `liveBytes`
on free for allocations ≥ 4 GiB). `weight` is the block's estimate multiplier,
captured at sample time (≈ `SampleInverse` for a randomly-sampled block, 1 for
a force-sampled one); storing it per block lets a free debit the *estimated*
totals by exactly what its alloc credited even if the sampling knob changed in
between — see "Estimated usage" under Reporting.

The table is created lazily alongside the aggregation table on the first
sampled allocation; when live-tracking is off it stays allocated-but-empty.

Both tables are allocated once and never torn down — intentionally leaked at
process exit so `delete`s arriving during static destruction still find valid
tables, and the tracker installs no `fork` handler since fdbserver does not
fork after init.

Single global `ThreadSpinLock` for v1. Two things acquire it: sampled
allocs (~1K/sec at 1% of 100K alloc/sec; uncontended acquire ~20 ns, so
~0.002% CPU) and **every** free while live-tracking is enabled — a free
carries no backtrace, so its only key is the pointer and it must probe the
live table to learn whether that pointer was sampled. At 100K free/sec that
is ~100K lock+probe/sec, the dominant cost of the enabled steady state. Two
mitigations: (1) when the tracker is *disabled* the `g_memTrackerEnabled`
flag short-circuits the free path before the lock, so the off-switch is
genuinely free of lock work; (2) per-thread or pointer-sharded live tables to
cut enabled-state contention are deferred to a follow-up, to be sized against
the phased-rollout performance baseline before the production default is
flipped on (see Alternatives).

#### Aggregation table

Open-addressing hash keyed by `uint64_t` fingerprint. Each entry holds, for one
call site: the **estimated** population usage (live / peak / cumulative bytes
and counts, sampling-correction already applied at sample time so consumers read
them directly), the **raw** sampled counters alongside them (one increment per
observed sample, for auditing the estimate and gauging its confidence), a
force-sampled count, and the exemplar return-address frames (bounded by
`MEMORY_TRACKING_FRAMES`, capped at 10). The exact field set is in `CallSite`
(`flow/MemoryTracker.h`).

`fingerprint = fnv64(frames)`, computed once at sample time; exemplar frames are
recorded from the first allocation with that fingerprint and never updated.
Fingerprint collisions between distinct call paths are very unlikely with 64-bit
FNV over several frames; if it ever matters we can switch hashes or store all
observed frames. Live and peak fields are maintained only when live-tracking is
enabled (they stay 0 in degraded mode); the cumulative fields are always
maintained.

### Hook sites

#### Global `operator new` / `delete`

`fdbserver/GlobalNewDelete.cpp` defines the ~12 standard global overloads
— `new`, `new[]`, `delete`, `delete[]`, sized variants, `nothrow_t`
variants, and the C++17 `std::align_val_t` overloads. Each calls
`std::malloc` / `std::free` (or `posix_memalign` for the aligned
variants) and then `memTrackerOnAlloc`/`OnFree`.

These overloads live in a translation unit compiled directly into the
`fdbserver` executable — not in the `flow` static library — for two
reasons:

1. **Correct interposition.** `operator new` / `operator delete` are
   replaceable functions. A definition sitting in a static archive is
   only pulled into the link if the linker already needs some other
   symbol from that same object file; placing the overloads in an
   executable TU guarantees they win rather than relying on incidental
   archive pull-in.
2. **Client isolation.** `flow` is linked into `libfdb_c` and every
   client binding. A global-`new` override compiled into it would
   interpose the entire host process's allocator in any application
   that loads the client. `fdbserver` is a standalone executable that
   clients never link, so keeping these here confines the interposition
   to the server. (The `FastAllocator` and `Arena` hooks still compile
   into `flow`, and hence into the client, but those are ordinary direct
   calls gated on the sampling knob — not global-symbol interposition —
   and are inert when sampling is off.)

The same file also hosts the legacy `ALLOC_INSTRUMENTATION`
`recordAllocation`/`recordDeallocation` overrides; the two
implementations are selected by a single
`#if defined(ALLOC_INSTRUMENTATION) …` / `#else` / `#endif`, so exactly
one set of global operators is ever defined.


#### FastAllocator

In `flow/FastAlloc.cpp`, `FastAllocator<Size>::allocate` / `::release` get an
unconditional `memTrackerOnAlloc(p, Size)` / `memTrackerOnFree(ptr)` call added
next to (not replacing) the existing conditional `ALLOC_INSTRUMENTATION`
`recordAllocation`/`recordDeallocation` lines, which are left in place. `Size`
is a compile-time template parameter, so no size lookup is needed.

The global `operator new` / `delete` overrides (both the tracker path
and the legacy `ALLOC_INSTRUMENTATION` path) live in
`fdbserver/GlobalNewDelete.cpp`; see the "Global `operator new` /
`delete`" section above.

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
event for every site whose **estimated** live bytes exceed
`MEMORY_TRACKING_REPORT_BYTES_THRESHOLD`. The default threshold is
**80 MB**, chosen as roughly 1% of the target RSS for a production
fdbserver (~8 GB). Because the threshold is expressed in real bytes,
it is compared against the estimated (sampling-corrected) live bytes,
not the raw sampled bytes — comparing 80 MB against ~1/100th-scale
sampled bytes would qualify almost nothing. Sites smaller than that are
not load-bearing for RSS-level investigations and would only add log
volume; sites above it are the ones worth attributing. A pure top-N has
the wrong shape for this — top-50 against a process that genuinely has
only three heavyweight sites still emits 47 noise events, while a
process with hundreds of meaningful sites silently truncates at 50. A
byte threshold scales naturally to the actual distribution.

**Estimated usage.** The consumer should not have to do sampling math.
Each site reports `Est*` fields — estimated *population* usage —
alongside the raw sampled counters. The estimate weights each sampled
block by its inverse inclusion probability at sample time: a randomly
sampled block (1-in-`INV`) is weighted `INV`, a force-sampled block
(captured with certainty) is weighted 1. `EstLiveBytes = Σ size×weight`
over that site's live blocks, and likewise for cumulative and peak. The
weight is stored per live block, so a free debits the estimate by
exactly what its alloc credited even if `INV` changed in between.
Weighting at sample time (rather than scaling raw totals at report time)
also makes `EstPeakBytes` well-defined as the running max of
`EstLiveBytes`. The raw counters remain in the event for auditing the
estimate and gauging confidence (a site with few `SampledAllocs` is
noisy); the `MemoryTrackerSummary` event carries an `EstimateBasis`
caveat string. (`INV` is used as the weight rather than the exact mean
reseed gap `INV + 0.5`; the ~`0.5/INV` bias is negligible at the
production `INV` of 100.)

**Degraded mode.** When `MEMORY_TRACKING_LIVE_TRACKING` is `false`,
the live-block side table is not populated, so `onFree` is a no-op and
the per-site `liveBytes` / `liveCount` / `peakBytes` (and their `est*`
counterparts) are never incremented — they stay at 0. Only the
`Cumulative*` / `EstCumulative*` fields are meaningful in this mode, and
the dump filters and ranks on `estCumulativeBytes` accordingly, so it
reports "any site that has ever allocated ≥ threshold bytes" rather than
"any site currently holding ≥ threshold". Operators using this mode
should read the dump accordingly and ignore the (zero) live/peak fields.

Each dump snapshots the aggregation table under the spinlock into a
`std::malloc`-backed local (the `gInMemTracker` guard prevents recursion),
releases the lock, then ranks and filters by the estimated bytes and emits:

- one **`MemoryTrackerSite`** event per qualifying site: its fingerprint, the
  estimated population fields, the raw sampled counters (for auditing), and an
  `AddrCmd` detail — a ready-to-paste `addr2line` invocation for just that
  site's frames (short enough to fit under the TraceEvent string-detail cap, so
  a consumer pastes one site's `AddrCmd` and gets exactly that site's stack);
- one **`MemoryTrackerSummary`** event per dump: site counts, estimated and raw
  population totals, live-block count, samples emitted, an echo of the active
  knobs (`SampleInverse`, `ForceSampleBytes`, threshold), and an `EstimateBasis`
  caveat string.

The exact detail keys live in the `TraceEvent` call sites in
`flow/MemoryTracker.cpp`; a comment there points back to this section. The raw
running totals are maintained as globals under the same spinlock on every
sample; the `Est*` totals are their sampling-corrected counterparts, accumulated
with the per-block weight so consumers read them without post-hoc scaling.

### Knobs (FlowKnobs, since clients use Arena/new too)

Authoritative defaults live in `flow/Knobs.cpp`; this table gives the meaning
and intent, not exact literals.

| Knob | Meaning / intent |
|---|---|
| `MEMORY_TRACKING_SAMPLE_INVERSE` | 0 = off, N = sample 1-in-N. Ships off in prod during rollout; ~1% (N=100) is the steady-state target; simulation runs at 1-in-2 to exercise the path. |
| `MEMORY_TRACKING_FORCE_SAMPLE_BYTES` | Always sample allocations at or above this size (~100 KB); `-1` disables force-sampling. |
| `MEMORY_TRACKING_LIVE_TRACKING` | On by default. When off, skip the live-block table and report cumulative-only stats. |
| `MEMORY_TRACKING_REPORT_INTERVAL` | Seconds between dumps (~10 min in prod, ~30 s in sim); 0 disables reporting. |
| `MEMORY_TRACKING_REPORT_BYTES_THRESHOLD` | Report sites whose estimated bytes exceed this (prod ≈ 1% of an ~8 GB target RSS; lowered in sim so events actually fire). |
| `MEMORY_TRACKING_FRAMES` | Captured stack depth (1–10). |

Simulation uses a fixed 1-in-2 sample rate; the every-allocation path
(`inverse==1`) and the sampled/weighted path (`inverse=N>1`) are both
pinned deterministically by unit tests (`MemoryTrackerTest.cpp`), so no
buggify of the rate is needed.

### Code layout

- **Tracker core:** `flow/MemoryTracker.{cpp,h}` — hot-path inlines, out-of-line
  sample/free paths, the two tables, and the dump.
- **Global `operator new`/`delete`:** `fdbserver/GlobalNewDelete.cpp` — compiled
  only into the `fdbserver` executable (client isolation), and also home to the
  legacy `ALLOC_INSTRUMENTATION` overrides via `#if`/`#else`.
- **Allocator hooks:** unconditional `memTrackerOnAlloc`/`OnFree` calls added in
  `flow/FastAlloc.cpp` and `flow/Arena.cpp`, next to the existing
  `ALLOC_INSTRUMENTATION` lines (which are left untouched — dormant in default
  builds).
- **Dump driver / knobs:** `flow/SystemMonitor.cpp` and `flow/Knobs.{cpp,h}`.
- **Tests / bench:** `flow/MemoryTrackerTest.cpp` (unit tests) and
  `fdbserver/bench/` (`fdbserver_bench`).

Most files are picked up by the source globs; the exceptions are the
`fdbserver/bench/` subdirectory (its own `CMakeLists.txt` + an `add_subdirectory`
line) and the `forceLinkMemoryTrackerTests()` stub that `fdbserver/workloads/UnitTests.cpp`
must call so the test TU is not dropped from the link.

### Determinism in simulation

Sampling state (`gInMemTracker`, `gMemTrackerCounter`, the xorshift
seed) lives in thread local storage. Sim2 runs single-threaded, so the sequence of
sampling decisions is fully replayable across runs with the same seed.
The tracker never reads `g_random` or `g_network->now()`, never
allocates from FDB-visible heaps, and never takes any lock that
participates in Sim2 ordering. The sample dump cadence is driven by
`now()`, but only inside `SystemMonitor.cpp` where time-driven cadence
is already deterministic in simulation.

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

  These figures are back-of-the-envelope, not measured. The 100K
  alloc/sec is an assumed workload rate (real rates vary widely by
  process role and load), and the ~20 ns uncontended-acquire is a
  rough x86 ballpark that will differ across microarchitectures and
  ISAs (aarch64), and under cache pressure or NUMA effects. Both could
  be optimistic; treat the conclusion as "cheap on the sampled path
  under expected conditions," to be confirmed by the microbenchmarks
  and rollout baseline rather than as a guaranteed bound.

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

Its runtime cost is not a single well-established figure we can cite here.
jemalloc profiling samples at an average byte interval set by
`lg_prof_sample` and captures a backtrace per sample, so the overhead
scales with the allocation rate and the configured interval and is
generally characterized as low at coarse sampling intervals. We have not
benchmarked it for FDB's workload, and jemalloc's documentation does not
give a workload-independent overhead number. We therefore make no specific
performance claim; the reasons below reject it on coverage and portability
grounds regardless of its cost.

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

`flow/MemoryTrackerTest.cpp` implements these `TEST_CASE`s (all under
`/flow/MemoryTracker/`):

- **`coverage`** — sentinel functions drive one allocation each through
  `operator new`, `FastAllocator`, and `Arena`; confirms a captured call
  site contains a frame inside each sentinel. Linux-only (the FP walker is
  a no-op stub elsewhere).
- **`cumulativeIsMonotonic`** — after allocating then freeing, `liveCount`
  returns to 0 while `cumulativeAllocs` does not decrement.
- **`offSwitch`** — with sample inverse 0, no sites are recorded and the
  `g_memTrackerEnabled` flag is false (so frees skip the lock).
- **`enableAfterOff`** — flips inverse 0 → 1 *without* resetting tracker
  state; confirms sampling resumes within the bounded re-park window. This
  is the regression test for the startup / off→on activation path.
- **`freeOfUntrackedPtrIsNoop`** — `memTrackerOnFree` on a never-sampled
  pointer (and on `nullptr`) records nothing.
- **`estimateScaling`** — with a fixed inverse N and no force-sampled
  blocks, each site's `Est*` value equals N × its raw counter, and freeing
  debits the estimate symmetrically.
- **`fastAlloc32Accounting`, `arenaSmallAccounting`,
  `arenaMediumAccounting`, `arenaHugeAccounting`** — byte/block accounting
  per allocation path; the "exactly one site carries the sentinel's frames"
  assertion is the B1 double-tracking regression test (the medium and huge
  `Arena` paths are the ones that regressed).

Not yet covered by dedicated unit tests: an explicit reentrancy-guard test
and a frame-pointer-walk-depth test — both behaviors are exercised
indirectly by the tests above.

### Microbenchmarks

`fdbserver/bench/BenchMemoryTracker.cpp` (Google Benchmark; run
`bin/fdbserver_bench --benchmark_filter=memtracker`) measures the tracker's
per-operation cost. Two cases matter, both feeding R0 ("cheap enough to leave
on?"): the **off-state** cost (always-compiled hooks, sampling disabled) and
the **enabled-state** cost (hooks at the production 1% rate, and at the pessimal
every-allocation rate). Each is measured both end-to-end through global
`operator new[]`/`delete[]` and by calling `memTrackerOnAlloc`/`OnFree` directly
on a preallocated buffer (which isolates the tracker's own work, with no
allocation in the loop); a raw `malloc`/`free` loop is the tracker-free
baseline. The operator-new path only fires the tracker when the global override
(`fdbserver/GlobalNewDelete.cpp`) is linked into the binary, so this bench lives
under `fdbserver/` and its CMake compiles that override into `fdbserver_bench`;
`flow_bench` links only `flow` (no override) and would measure the unhooked
allocator.

Measured on the dev pod (AMD EPYC 9R14 / Zen 4, 32 vCPUs, 128 GB; `clang -O3`
release build), ns per alloc+free pair, projected to one second at **~2M
allocations/sec** — the storage-process allocation rate measured in the mako A/B
below. These are a point-in-time snapshot on one machine (numbers stable to
within a few percent across repeated runs), not a portable guarantee:

| Case | ns/op | @2M/s | % of one core |
|---|---|---|---|
| `malloc`/`free` (baseline, no tracker) | 7.8 | 15.6 ms/s | 1.56% |
| hooks, off | 1.6 | 3.3 ms/s | 0.33% |
| hooks, 1% sampling | 5.1 | 10.2 ms/s | 1.02% |
| hooks, every alloc (worst case, not a default) | 75 | 150 ms/s | 15% |
| `operator new`/`delete`, off | 11.4 | 22.8 ms/s | 2.28% |
| `operator new`/`delete`, 1% sampling | 14.4 | 28.8 ms/s | 2.88% |
| `operator new`/`delete`, every alloc (worst case, not a default) | 81 | 162 ms/s | 16% |

Takeaways:

- **Disabled**, the tracker adds ~1.6 ns per alloc+free pair — ~0.33% of a core
  at 2M/s. Small but no longer negligible at this rate; this is the off-switch
  cost (R9).
- **At the production 1% rate**, the tracker's own *single-threaded* work is
  ~5.1 ns/pair — ~1.0% of a core at 2M/s, i.e. right at R0's 1% ceiling, and
  that is *without* cross-thread lock contention. The cost is dominated by the
  per-free lock+probe (every free takes the global lock while live-tracking is
  on), not the 1%-of-allocs slow path.
- These are single-threaded projections and are a **floor**, not the real cost.
  The end-to-end mako A/B (storage isolated on one core, real multi-threaded
  allocation including I/O threads) measures a materially larger hit at 1%
  sampling — **−16.5% TPS on redwood, −9.9% on rocksdb** (off vs 1:100, live
  tracking on) — because all those frees contend on the one global `g_mtLock`.
  The engine with the higher baseline throughput (redwood) takes the larger
  hit, consistent with a per-free-lock cost that scales with allocation rate.
- **Conclusion:** even at this worst-case ~2M/s (a CPU-saturated storage core),
  the current single-global-lock design is *not* comfortably under R0's 1%
  ceiling when enabled at 1%. This is why the production default stays off
  (R2's phased rollout) until the live-table lock is sharded / made per-thread
  (the deferred follow-up in Storage and Alternatives); the µbench and A/B
  together size that work.
- The **every-allocation** rows are **not a proposed configuration** — they are
  an estimated worst case for buggified `inverse=1` simulation runs.

Caveat: 2M/s is a **worst-case** rate — it was measured on a deliberately
CPU-pegged storage server (the mako bench pins the storage role to ~one core at
max throughput). A production server not running flat out allocates less, so the
%-of-core figures here are upper bounds; scale linearly for lower rates or other
roles. Conversely, the single-threaded µbench excludes cross-thread lock
contention, so its per-op %-of-core is a *lower* bound — the mako A/B is the
representative end-to-end number. The real cost is bracketed between the two.

Two mako A/B harnesses drive these end-to-end numbers, both on a single-host
1/1/1 loopback cluster with the storage role CPU-pegged on tmpfs.
`contrib/mako_ab_memtracker.py` runs one build at sampling off vs on (the
−16.5% / −9.9% figures above). `contrib/mako_ab_binaries.py` compares a
vanilla-`main` fdbserver against this PR built with tracking off, isolating the
cost of the always-compiled-but-disabled code: on a shared base commit it
measured **−0.53% (redwood) / −0.10% (rocksdb)**, confirming the off-state
overhead is well within R0's 1% ceiling. Each emits a self-contained chart.js
report.

### Strip-aware symbolization spot-check

Build the release binary, strip it, paste an `AddrCmd` field from a
trace into a shell pointing at the matching `.debug` sidecar, confirm
symbol resolution works.

### Coverage spot-check via sentinel functions

The `coverage` / `*Accounting` tests verify capture without log parsing or
`addr2line`: they sample at inverse 1, drive a known number of allocations
through each path from a dedicated sentinel function, then use the
`memTrackerForEachSite` introspection API to assert that some captured site's
exemplar frames fall inside that sentinel's address range with the expected
allocation count. Comparing raw return addresses to function-pointer values is
deterministic, fast, and works on stripped builds — it exercises the full
sampling → capture → fingerprint → aggregation pipeline.

## Observability/Supportability Considerations

The `MemoryTrackerSummary` event (see Reporting) doubles as the tracker's own
health signal: site counts, live-block count, and samples-emitted answer "is the
tracker working" and "is it costing us." No separate self-metrics are added.

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

`libfdb_c.so` and other client artifacts link `flow/`, so the tracker's
`FastAllocator` and `Arena` hooks — and the out-of-line sample paths —
are present in client binaries. The **global `operator new` / `delete`
overrides are not**: they live in `fdbserver/GlobalNewDelete.cpp`, which
is compiled only into the `fdbserver` executable (a standalone binary
clients never link). Implications:

- The global-allocator interposition never reaches an embedding
  application. This removes the main client risk — a host app that has
  its own `operator new` override, or is sensitive to which allocator
  services `new`, is unaffected.
- The `FastAllocator` / `Arena` hooks that do compile into the client
  are ordinary direct calls gated on the sampling knob, not global-symbol
  interposition. Sampling defaults to 0 in client contexts, so their
  cost is just the `if (gInMemTracker)` / disabled-flag check. We have no
  plans or requirements to enable sampling for client library users.

### `ALLOC_INSTRUMENTATION` coexistence

The existing framework in `flow/include/flow/FastAlloc.h` and its
conditional callers in `flow/FastAlloc.cpp` remain in place. Its global
`operator new` / `delete` overrides now live in the `#if
defined(ALLOC_INSTRUMENTATION)` branch of `fdbserver/GlobalNewDelete.cpp`,
mutually exclusive (via `#else`) with the tracker overrides, so exactly
one set of global operators is defined. No removal is planned.

### Rollback

Setting `MEMORY_TRACKING_SAMPLE_INVERSE=0` and
`MEMORY_TRACKING_REPORT_INTERVAL=0` at runtime fully disables the
tracker without a binary rebuild. The hot-path cost reduces to the TLS
load + branch (`gInMemTracker` check) and is observably zero on
benchmarks.

If the operator-new overrides themselves need to be rolled back (e.g.,
they cause issues with a third-party library that has its own global
new override), removing them is a matter of deleting
`fdbserver/GlobalNewDelete.cpp`'s tracker branch while keeping the
FastAlloc and Arena tracking that lives in `flow`.
