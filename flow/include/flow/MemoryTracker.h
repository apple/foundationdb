/*
 * MemoryTracker.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Sampled per-call-site memory attribution.
//
// See design/memory-tracker.md for the full design.
//
// Hot path: memTrackerOnAlloc is header-inlined; when the feature is disabled
// (the common production default) it short-circuits on a single per-thread TLS
// load + branch. memTrackerOnFree is header-inlined, one relaxed read of a
// cache-line-isolated enabled flag + one branch when disabled -- no lock, no
// table probe.
//
// Sampled path delegates to memTrackerSampleAlloc / memTrackerSampleFree,
// which take a private spinlock, capture a frame-pointer-walk backtrace, and
// update two tables (aggregation by fingerprint, and an optional pointer-
// keyed live-block table).
//
// Reentrancy: the gInMemTracker thread-local guard is set to true while the
// tracker is doing its own work. Any allocator hook called recursively
// during that window observes the guard and bails out, leaving the
// underlying allocation un-tracked. Higher-level hooks (e.g. ArenaBlock::create)
// may also set this guard to suppress an inner allocator hook so the same
// block is attributed at exactly one level.

#ifndef FLOW_MEMORY_TRACKER_H
#define FLOW_MEMORY_TRACKER_H
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>

// FDB_MEMORY_TRACKER gates the whole feature at compile time; on (1) by default.
// Build with -DFDB_MEMORY_TRACKER=0 (cmake -DFDB_MEMORY_TRACKER=OFF) to compile it
// out entirely: the hooks become no-ops and the global operator new/delete override
// (fdbserver/GlobalNewDelete.cpp) is not defined, so libc++'s allocator is used.
#ifndef FDB_MEMORY_TRACKER
#define FDB_MEMORY_TRACKER 1
#endif

#if FDB_MEMORY_TRACKER

// Maximum number of stack frames the tracker can capture per sample.
// MEMORY_TRACKING_FRAMES knob controls the runtime depth (1..MEMORY_TRACKER_MAX_FRAMES).
constexpr int MEMORY_TRACKER_MAX_FRAMES = 10;

// Per-site aggregate, exposed for tests via memTrackerForEachSite.
//
// Two families of numbers are kept per site:
//   * Est* — the estimated *population* usage, i.e. what the site is really
//     costing. Each sampled block is weighted by its inverse inclusion
//     probability (≈ SampleInverse for randomly-sampled blocks, 1 for
//     force-sampled blocks) at sample time, so these already have the sampling
//     math applied — a consumer (logging, etc) reads them directly, no scaling required.
//   * the raw sampled counters (liveBytes, cumulativeAllocs, …) — the
//     uninterpreted "what we actually observed" numbers, kept for auditing the
//     estimate and gauging its confidence (few samples ⇒ noisy estimate).
struct MemoryTrackerCallSite {
	uint64_t fingerprint;

	int64_t estLiveBytes;
	int64_t estLiveCount;
	int64_t estPeakBytes;
	int64_t estCumulativeBytes;
	int64_t estCumulativeAllocs;

	int64_t liveBytes;
	int64_t liveCount;
	int64_t peakBytes;
	int64_t cumulativeAllocs;
	int64_t cumulativeBytes;
	int64_t forceSampledCount;

	void* exemplarFrames[MEMORY_TRACKER_MAX_FRAMES];
	uint8_t exemplarFrameCount;
};

extern thread_local bool gInMemTracker;
extern thread_local int gMemTrackerCounter;
extern thread_local std::size_t gForceSampleBytes;

// Set true (per thread, nest-safe via MemTrackerInternalAlloc) only around the
// tracker's OWN bookkeeping allocations — map construction / node inserts / rehash
// on the sampled path, and the reporter's scratch vectors, strings, and
// TraceEvents. The global operator new override (fdbserver/GlobalNewDelete.cpp)
// reads this: an allocation failure inside this window throws std::bad_alloc
// straight to the tracker's fail-open catch instead of invoking the installed
// std::new_handler (platform::outOfMemory, which terminates with FDB_EXIT_NO_MEM).
// So a diagnostic bookkeeping allocation that fails just drops the sample or
// report; it cannot terminate the server after the caller's real allocation has
// already succeeded.
//
// Deliberately distinct from gInMemTracker: gInMemTracker is also set by Arena
// around genuine user-owned allocations (new uint8_t[] for a large block), which
// must keep the fatal OOM semantics. Only the tracker's own metadata fails open.
extern thread_local bool gInMemTrackerAlloc;

// Test-only one-shot, read by the global operator new override
// (fdbserver/GlobalNewDelete.cpp). When set, the next tracker-internal allocation
// (gInMemTrackerAlloc active) is forced to fail there, so tests can drive the
// fail-open path through the real mallocWithNewHandler. Armed via
// memTrackerFailNextInternalAllocForTest; cleared by memTrackerResetForTest.
extern thread_local bool gMemTrackerFailNextInternalAllocForTest;
// Set true (per thread) once this thread's slow path observes sampling is off,
// so the alloc hot path then short-circuits on a single TLS load instead of
// decrementing the counter and reading gForceSampleBytes every call. Per-thread
// (not global) so an early main-thread allocation before FLOW_KNOBS is ready
// can't disable sampling on worker threads that bootstrap later. Cleared by
// memTrackerResetForTest.
extern thread_local bool gMemTrackerOff;

// Global "is the tracker enabled" flag, kept in its own cache line. Published
// once, from the first slow-path visit, and thereafter constant: the sample-
// inverse knob is read at startup only (dynamic enable/disable is a
// Non-requirement -- see design/memory-tracker.md). The flag stays in MESI
// shared state across cores, so the free hot path's relaxed read is cached:
// when disabled, a free is one read + one branch, no lock. This is the
// free-path off switch (a free has no per-thread sampling counter to gate on,
// unlike an alloc).
//
// Relaxed ordering is sufficient: a free of a sampled pointer is always
// preceded (via the pointer handoff that let the freeing thread learn the
// pointer at all) by the sampling alloc that inserted it, and that alloc set
// this flag true before inserting -- so the happens-before edge guarantees the
// freeing thread observes the flag as true.
struct alignas(64) MemTrackerEnabledFlag {
	std::atomic<bool> value{ false };
	char pad[64 - sizeof(std::atomic<bool>)];
};
extern MemTrackerEnabledFlag g_memTrackerEnabled;

// RAII suppressor: while alive, allocator hooks short-circuit. Used by code
// paths that call into a lower-level allocator (e.g. ArenaBlock wrapping
// `new uint8_t[]`) and want their explicit memTrackerOnAlloc/OnFree call to
// be the sole tracker for the block — without this guard the inner
// allocator's hook fires too and the same pointer is double-tracked under
// two different fingerprints. Nest-safe: saves and restores prev.
class MemTrackerSuppress {
	bool prev;

public:
	MemTrackerSuppress() : prev(gInMemTracker) { gInMemTracker = true; }
	~MemTrackerSuppress() { gInMemTracker = prev; }
	MemTrackerSuppress(const MemTrackerSuppress&) = delete;
	MemTrackerSuppress& operator=(const MemTrackerSuppress&) = delete;
};

// RAII: marks the enclosed region as tracker-internal bookkeeping, so a heap
// allocation failure there fails open (throws std::bad_alloc, caught by the
// tracker) rather than tripping the fatal OOM handler — see gInMemTrackerAlloc.
// Nest-safe (saves and restores prev). Distinct from MemTrackerSuppress:
// MemTrackerSuppress gates the sampling hook and is also used around real user
// allocations; this guard only changes allocation-failure behavior, and only for
// the tracker's own metadata.
class MemTrackerInternalAlloc {
	bool prev;

public:
	MemTrackerInternalAlloc() : prev(gInMemTrackerAlloc) { gInMemTrackerAlloc = true; }
	~MemTrackerInternalAlloc() { gInMemTrackerAlloc = prev; }
	MemTrackerInternalAlloc(const MemTrackerInternalAlloc&) = delete;
	MemTrackerInternalAlloc& operator=(const MemTrackerInternalAlloc&) = delete;
};

// Initialize the tracker from the current knob values. Call once, from process
// startup, AFTER all knobs are finalized and BEFORE any serving role starts (see
// fdbserver.cpp). Publishes the enabled state and arms the calling (network)
// thread from MEMORY_TRACKING_SAMPLE_INVERSE, so early startup allocations on the
// main thread cannot latch the tracker off before the knob is configured. The
// sample-inverse knob is read here (and in memTrackerResetForTest) rather than
// inferred from the first allocation; dynamic runtime enable/disable is a
// Non-requirement (see design/memory-tracker.md).
void memTrackerInit();

void memTrackerSampleAlloc(void* p, std::size_t n);
void memTrackerSampleFree(void* p);

inline void memTrackerOnAlloc(void* p, std::size_t n) {
	if (gMemTrackerOff) [[likely]] {
		return;
	}
	if (gInMemTracker || !p) {
		return;
	}
	if (--gMemTrackerCounter > 0 && n < gForceSampleBytes) {
		return;
	}
	MemTrackerSuppress _suppress;
	memTrackerSampleAlloc(p, n);
}

inline void memTrackerOnFree(void* p) {
	if (gInMemTracker || !p) {
		return;
	}
	// Cheap cache-line-shared read: when the tracker is disabled there is no
	// live-block table to debit, so skip all lock/table work.
	if (!g_memTrackerEnabled.value.load(std::memory_order_relaxed)) {
		return;
	}
	MemTrackerSuppress _suppress;
	memTrackerSampleFree(p);
}

// Periodic dump — emits one TraceEvent("MemoryTrackerSite") per site whose
// estLiveBytes (or estCumulativeBytes when MEMORY_TRACKING_LIVE_TRACKING is off)
// exceeds bytesThreshold. The threshold is compared against the sampling-corrected
// estimate, not the raw sampled bytes. Each site event carries an "AddrCmd"
// detail: a ready-to-paste addr2line invocation covering just that site's frames.
// A final TraceEvent("MemoryTrackerSummary") reports aggregate totals.
void memTrackerDump(int64_t bytesThreshold);

// Snapshot iteration for tests. The callback runs while a copy of the
// aggregation table is held; the spinlock is not held during the callback.
void memTrackerForEachSite(std::function<void(const MemoryTrackerCallSite&)> cb);

// Reset all state. Tests only — not safe for production use.
void memTrackerResetForTest();

// Test only: arm a one-shot so the next tracker-internal (bookkeeping) allocation
// is forced to fail. Unlike a throw injected at the top of the sampled path, this
// drives the real failure route: an actual map/report allocation goes through the
// global operator new (fdbserver/GlobalNewDelete.cpp), which — seeing the
// tracker-internal context — throws std::bad_alloc WITHOUT invoking the installed
// std::new_handler, and the tracker swallows it. Lets tests verify the tracker
// fails open (underlying allocation preserved, handler not invoked, tracking
// recovers afterward). Never used in production.
void memTrackerFailNextInternalAllocForTest();

#else // !FDB_MEMORY_TRACKER — compiled out: hooks are no-ops, no operator new override.

inline void memTrackerInit() {}
inline void memTrackerOnAlloc(void*, std::size_t) {}
inline void memTrackerOnFree(void*) {}
inline void memTrackerDump(int64_t) {}
inline void memTrackerResetForTest() {}
class MemTrackerSuppress {
public:
	MemTrackerSuppress() {}
	~MemTrackerSuppress() {}
	MemTrackerSuppress(const MemTrackerSuppress&) = delete;
	MemTrackerSuppress& operator=(const MemTrackerSuppress&) = delete;
};

#endif // FDB_MEMORY_TRACKER

#endif // FLOW_MEMORY_TRACKER_H
