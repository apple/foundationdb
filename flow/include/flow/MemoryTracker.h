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
// Hot path: memTrackerOnAlloc / memTrackerOnFree are header-inlined, one TLS
// load + one decrement + one branch on the un-sampled fast path.
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

#include <cstddef>
#include <cstdint>
#include <functional>

// Maximum number of stack frames the tracker can capture per sample.
// MEMORY_TRACKING_FRAMES knob controls the runtime depth (1..MEMORY_TRACKER_MAX_FRAMES).
constexpr int MEMORY_TRACKER_MAX_FRAMES = 10;

// Per-site aggregate. Public so unit tests can introspect via memTrackerForEachSite.
struct MemoryTrackerCallSite {
	uint64_t fingerprint;
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
	gInMemTracker = true;
	memTrackerSampleFree(p);
	gInMemTracker = false;
}

// Periodic dump — emits one TraceEvent("MemoryTrackerSite") per site whose
// liveBytes (or cumulativeBytes when MEMORY_TRACKING_LIVE_TRACKING is off)
// exceeds bytesThreshold, plus one TraceEvent("MemoryTrackerAddrCmd") with
// a single combined addr2line invocation covering all qualifying sites,
// plus one TraceEvent("MemoryTrackerSummary"). Called from SystemMonitor.
void memTrackerDump(int64_t bytesThreshold);

// Snapshot iteration for tests. The callback runs while a copy of the
// aggregation table is held; the spinlock is not held during the callback.
void memTrackerForEachSite(std::function<void(const MemoryTrackerCallSite&)> cb);

// Reset all state. Tests only — not safe for production use.
void memTrackerResetForTest();

#endif // FLOW_MEMORY_TRACKER_H
