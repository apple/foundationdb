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
// Hot path: memTrackerOnAlloc is header-inlined, one TLS load + one decrement
// + one branch on the un-sampled fast path. memTrackerOnFree is header-inlined,
// one relaxed read of a cache-line-isolated enabled flag + one branch when the
// feature is disabled (the common production default) -- no lock, no table
// probe.
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

// Maximum number of stack frames the tracker can capture per sample.
// MEMORY_TRACKING_FRAMES knob controls the runtime depth (1..MEMORY_TRACKER_MAX_FRAMES).
constexpr int MEMORY_TRACKER_MAX_FRAMES = 10;

// When sampling is disabled (MEMORY_TRACKING_SAMPLE_INVERSE <= 0) the alloc slow
// path re-parks the per-thread counter at this value rather than at INT_MAX.
// A parked thread therefore re-enters the (lock-free, knob-reading) slow path
// once every ~this-many allocations, so an off->on knob change is observed
// within a bounded window instead of after ~2^31 allocations. The cost while
// disabled is one slow-path visit (a handful of int reads, no lock) per this
// many allocations -- negligible.
constexpr int MEMORY_TRACKER_DISABLED_RESEED = 1 << 16;

// Per-site aggregate. Public so unit tests can introspect via memTrackerForEachSite.
//
// Two families of numbers are kept per site:
//   * Est* — the estimated *population* usage, i.e. what the site is really
//     costing. Each sampled block is weighted by its inverse inclusion
//     probability (≈ SampleInverse for randomly-sampled blocks, 1 for
//     force-sampled blocks) at sample time, so these already have the sampling
//     math applied — a consumer reads them directly, no scaling required.
//   * the raw sampled counters (liveBytes, cumulativeAllocs, …) — the
//     uninterpreted "what we actually observed" numbers, kept for auditing the
//     estimate and gauging its confidence (few samples ⇒ noisy estimate).
struct MemoryTrackerCallSite {
	uint64_t fingerprint;

	// Estimated population usage (sampling correction already applied).
	int64_t estLiveBytes;
	int64_t estLiveCount;
	int64_t estPeakBytes;
	int64_t estCumulativeBytes;
	int64_t estCumulativeAllocs;

	// Raw sampled counters (no correction — one increment per observed sample).
	int64_t liveBytes;
	int64_t liveCount;
	int64_t peakBytes;
	int64_t cumulativeAllocs;
	int64_t cumulativeBytes;
	int64_t forceSampledCount;

	void* exemplarFrames[MEMORY_TRACKER_MAX_FRAMES];
	uint8_t exemplarFrameCount;
};

// Thread-local state — see header comment.
extern thread_local bool gInMemTracker;
extern thread_local int gMemTrackerCounter;
extern thread_local std::size_t gForceSampleBytes;

// Global "is the tracker enabled" flag, kept in its own cache line. Written
// only when the enabled state actually changes (i.e. rarely -- on the first
// slow-path visit after a MEMORY_TRACKING_SAMPLE_INVERSE knob change), so it
// stays in MESI shared state across cores and every core's read is
// effectively L1-local. The free hot path reads it (relaxed) before touching
// the lock: when disabled, a free is one read + one branch. This is what makes
// the "off switch" genuinely free-of-cost on the free path (a free has no
// per-thread sampling counter to gate on, unlike an alloc).
//
// Relaxed ordering is sufficient: a free of a sampled pointer is always
// preceded (via the pointer handoff that let the freeing thread learn the
// pointer at all) by the sampling alloc that inserted it, and that alloc set
// this flag true before inserting -- so the happens-before edge guarantees the
// freeing thread observes the flag as true. A stale read during a runtime
// off->on/on->off toggle can at worst take/skip one unnecessary lock or orphan
// one in-flight live entry, which is within the documented runtime-toggle
// limitations (see MEMORY_TRACKING_LIVE_TRACKING in Knobs.h).
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

// Out-of-line slow paths.
void memTrackerSampleAlloc(void* p, std::size_t n);
void memTrackerSampleFree(void* p);

// Header-inlined hot path. Cost on the un-sampled path: one TLS load +
// one decrement + one branch.
inline void memTrackerOnAlloc(void* p, std::size_t n) {
	if (gInMemTracker || !p)
		return;
	if (--gMemTrackerCounter > 0 && n < gForceSampleBytes)
		return;
	gInMemTracker = true;
	memTrackerSampleAlloc(p, n);
	gInMemTracker = false;
}

inline void memTrackerOnFree(void* p) {
	if (gInMemTracker || !p)
		return;
	// Cheap cache-line-shared read: when the tracker is disabled there is no
	// live-block table to debit, so skip all lock/table work.
	if (!g_memTrackerEnabled.value.load(std::memory_order_relaxed))
		return;
	gInMemTracker = true;
	memTrackerSampleFree(p);
	gInMemTracker = false;
}

// Periodic dump — emits one TraceEvent("MemoryTrackerSite") per site whose
// liveBytes (or cumulativeBytes when MEMORY_TRACKING_LIVE_TRACKING is off)
// exceeds bytesThreshold. Each site event carries an "AddrCmd" detail: a
// ready-to-paste addr2line invocation covering just that site's frames. A
// final TraceEvent("MemoryTrackerSummary") reports aggregate totals. Called
// from SystemMonitor.
void memTrackerDump(int64_t bytesThreshold);

// Snapshot iteration for tests. The callback runs while a copy of the
// aggregation table is held; the spinlock is not held during the callback.
void memTrackerForEachSite(std::function<void(const MemoryTrackerCallSite&)> cb);

// Reset all state. Tests only — not safe for production use.
void memTrackerResetForTest();

#endif // FLOW_MEMORY_TRACKER_H
