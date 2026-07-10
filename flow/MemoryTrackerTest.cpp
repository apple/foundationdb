/*
 * MemoryTrackerTest.cpp
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

// Unit tests for the per-call-site memory tracker.
//
// The "coverage" test uses sentinel functions: each sentinel triggers exactly
// one allocation path (operator new, FastAllocator, Arena), and the test
// confirms that some call site in the aggregation table contains a frame
// inside that sentinel's body. We compare raw return-address values against
// function-pointer values at runtime, so this works on stripped builds with
// no symbolization.

#include "flow/Arena.h"
#include "flow/FastAlloc.h"
#include "flow/Knobs.h"
#include "flow/MemoryTracker.h"
#include "flow/UnitTest.h"

#include <climits>
#include <cstdint>
#include <cstdlib>
#include <vector>

// Force this TU to link. The TEST_CASE macro registers via a static
// initializer; in a static library, a TU containing only static initializers
// gets dropped by the linker because nothing references its symbols.
// fdbserver/workloads/UnitTests.cpp calls this function to keep the TU.
void forceLinkMemoryTrackerTests() {}

namespace {

// A sentinel is an out-of-line function that performs exactly one kind of
// allocation, then returns its own address. We use the returned address to
// recognize captured stack frames that fell inside the sentinel's body.
constexpr uintptr_t SENTINEL_FUNC_SIZE = 4096;

// Defeat clang -O3 heap elision (P0593): if the allocated pointer doesn't
// escape, the compiler is free to drop the new/delete pair entirely, which
// then never reaches our operator-new override and the test sees zero samples.
inline void escape(void* p) {
	asm volatile("" : : "r"(p) : "memory");
}

bool frameInside(void* frame, void* sentinel) {
	uintptr_t f = reinterpret_cast<uintptr_t>(frame);
	uintptr_t s = reinterpret_cast<uintptr_t>(sentinel);
	return f >= s && f < s + SENTINEL_FUNC_SIZE;
}

__attribute__((noinline)) void* triggerOperatorNewSentinel(int n, int k) {
	for (int i = 0; i < n; i++) {
		auto* p = new int[k];
		p[0] = i;
		escape(p);
		delete[] p;
	}
	return reinterpret_cast<void*>(&triggerOperatorNewSentinel);
}

__attribute__((noinline)) void* triggerFastAllocSentinel(int n) {
	for (int i = 0; i < n; i++) {
		void* p = FastAllocator<32>::allocate();
		escape(p);
		FastAllocator<32>::release(p);
	}
	return reinterpret_cast<void*>(&triggerFastAllocSentinel);
}

__attribute__((noinline)) void* triggerArenaSentinel(int n) {
	// Force ArenaBlock::create by allocating large enough chunks to exceed
	// the small-block threshold.
	for (int i = 0; i < n; i++) {
		Arena a;
		// One ~512-byte allocation per arena -> goes through allocateAndMaybeKeepalive
		// path which has the explicit Arena hook.
		auto* p = new (a) uint8_t[600];
		escape(p);
	}
	return reinterpret_cast<void*>(&triggerArenaSentinel);
}

// ---------------------------------------------------------------------------
// Accounting tests: verify byte/block counts come out right per allocation path
// and there's no double-tracking.

// Allocate n arenas (each holding one >256-byte block, exercising the
// allocateAndMaybeKeepalive / new uint8_t[] path).
__attribute__((noinline)) void* allocateArenaMediumSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[600];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaMediumSentinel);
}

// Allocate n arenas (each holding one huge block, exercising the
// reqSize >= LARGE path of ArenaBlock::create).
__attribute__((noinline)) void* allocateArenaHugeSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[100000];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaHugeSentinel);
}

// Allocate n arenas (each holding one small block via FastAllocator<128|256>).
__attribute__((noinline)) void* allocateArenaSmallSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[64];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaSmallSentinel);
}

// Allocate n FastAllocator<32> blocks; pointers retained so the test can
// release them later.
__attribute__((noinline)) void* allocateFastAlloc32Sentinel(int n, std::vector<void*>& ptrs) {
	for (int i = 0; i < n; i++) {
		void* p = FastAllocator<32>::allocate();
		escape(p);
		ptrs.push_back(p);
	}
	return reinterpret_cast<void*>(&allocateFastAlloc32Sentinel);
}

__attribute__((noinline)) void releaseFastAlloc32(std::vector<void*>& ptrs) {
	for (void* p : ptrs) {
		FastAllocator<32>::release(p);
	}
	ptrs.clear();
}

struct AccountingSummary {
	int sitesWithSentinelFrames = 0;
	int64_t cumBytesSentinel = 0;
	int64_t cumAllocsSentinel = 0;
	int64_t liveBytesSentinel = 0;
	int64_t liveCountSentinel = 0;
	int totalSites = 0;
};

AccountingSummary collectAccounting(void* sentinel) {
	AccountingSummary acc;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		acc.totalSites++;
		bool touches = false;
		for (int i = 0; i < s.exemplarFrameCount; i++) {
			if (frameInside(s.exemplarFrames[i], sentinel)) {
				touches = true;
				break;
			}
		}
		if (touches && s.cumulativeBytes > 0) {
			acc.sitesWithSentinelFrames++;
			acc.cumBytesSentinel += s.cumulativeBytes;
			acc.cumAllocsSentinel += s.cumulativeAllocs;
			acc.liveBytesSentinel += s.liveBytes;
			acc.liveCountSentinel += s.liveCount;
		}
	});
	return acc;
}

void dumpSitesForFailure(const char* tag) {
	fprintf(stderr, "[%s] dumping all tracker sites:\n", tag);
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		fprintf(stderr,
		        "  fp=%016llx liveBytes=%lld liveCount=%lld cumBytes=%lld cumAllocs=%lld frames=",
		        (unsigned long long)s.fingerprint,
		        (long long)s.liveBytes,
		        (long long)s.liveCount,
		        (long long)s.cumulativeBytes,
		        (long long)s.cumulativeAllocs);
		for (int i = 0; i < s.exemplarFrameCount; i++) {
			fprintf(stderr, "%p ", s.exemplarFrames[i]);
		}
		fprintf(stderr, "\n");
	});
}

class KnobOverride {
public:
	explicit KnobOverride(int inverse = 1) : prevInverse(FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE) {
		auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
		k->MEMORY_TRACKING_SAMPLE_INVERSE = inverse;
	}
	~KnobOverride() {
		auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
		k->MEMORY_TRACKING_SAMPLE_INVERSE = prevInverse;
	}

private:
	int prevInverse;
};

} // namespace

TEST_CASE("/flow/MemoryTracker/coverage") {
#ifndef __linux__
	// captureFramesFP is a no-op stub on non-Linux (FP walking through libc
	// can't be made reliable on macOS); tests that inspect captured frames
	// have nothing to inspect. Skip cleanly. The tracker still compiles
	// and the non-frame tests (offSwitch, freeOfUntrackedPtrIsNoop) still
	// run.
	return Void();
#endif
	// Sample everything, reset, run sentinels, check.
	KnobOverride ko;
	memTrackerResetForTest();

	void* opNew = triggerOperatorNewSentinel(50, 4);
	void* fastAlloc = triggerFastAllocSentinel(50);
	void* arena = triggerArenaSentinel(50);

	bool foundOpNew = false;
	bool foundFastAlloc = false;
	bool foundArena = false;
	int siteCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		siteCount++;
		for (int i = 0; i < s.exemplarFrameCount; i++) {
			if (frameInside(s.exemplarFrames[i], opNew)) {
				foundOpNew = true;
			}
			if (frameInside(s.exemplarFrames[i], fastAlloc)) {
				foundFastAlloc = true;
			}
			if (frameInside(s.exemplarFrames[i], arena)) {
				foundArena = true;
			}
		}
	});

	if (!foundOpNew || !foundFastAlloc || !foundArena) {
		fprintf(stderr,
		        "MemoryTracker/coverage: sites=%d opNewSentinel=%p fastAllocSentinel=%p arenaSentinel=%p\n",
		        siteCount,
		        opNew,
		        fastAlloc,
		        arena);
		fprintf(stderr,
		        "MemoryTracker/coverage: foundOpNew=%d foundFastAlloc=%d foundArena=%d\n",
		        foundOpNew,
		        foundFastAlloc,
		        foundArena);
		memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
			fprintf(stderr,
			        "  site fp=%016llx liveBytes=%lld cumAllocs=%lld frames=",
			        (unsigned long long)s.fingerprint,
			        (long long)s.liveBytes,
			        (long long)s.cumulativeAllocs);
			for (int i = 0; i < s.exemplarFrameCount; i++) {
				fprintf(stderr, "%p ", s.exemplarFrames[i]);
			}
			fprintf(stderr, "\n");
		});
	}

	ASSERT(foundOpNew);
	ASSERT(foundFastAlloc);
	ASSERT(foundArena);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/offSwitch") {
	// With sample inverse 0, no allocations should be attributed.
	auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
	int prev = k->MEMORY_TRACKING_SAMPLE_INVERSE;
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 0;

	memTrackerResetForTest();

	// Burn through the per-thread initial counter (which is 1, so the very
	// first allocation will still be sampled before the reseed observes
	// inverse==0). Then run a flurry that should NOT be tracked.
	{
		auto* warm = new int[4];
		delete[] warm;
	}
	memTrackerResetForTest(); // discard the unavoidable first sample

	for (int i = 0; i < 100; i++) {
		auto* p = new int[4];
		p[0] = i;
		delete[] p;
	}

	int siteCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite&) { siteCount++; });
	ASSERT_EQ(siteCount, 0);

	// The enabled flag gates the free hot path: with sampling off it must be
	// false, so memTrackerOnFree short-circuits before taking g_mtLock.
	ASSERT(!g_memTrackerEnabled.value.load(std::memory_order_relaxed));

	k->MEMORY_TRACKING_SAMPLE_INVERSE = prev;
	return Void();
}

TEST_CASE("/flow/MemoryTracker/enableAfterOff") {
	// Regression test for the startup/off->on activation path. A thread that
	// first hits the slow path while sampling is off must NOT be parked
	// permanently: after the knob flips on, sampling has to resume without any
	// call to memTrackerResetForTest() (which the other tests use and which
	// would mask this bug).
	auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
	int prev = k->MEMORY_TRACKING_SAMPLE_INVERSE;

	// Start from a clean, disabled state.
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 0;
	memTrackerResetForTest();

	// One allocation drives the slow path, which parks the counter at
	// MEMORY_TRACKER_DISABLED_RESEED and publishes enabled=false.
	{
		auto* warm = new int[4];
		escape(warm);
		delete[] warm;
	}
	ASSERT(!g_memTrackerEnabled.value.load(std::memory_order_relaxed));

	// Flip sampling on WITHOUT resetting tracker state. The parked counter must
	// drain within MEMORY_TRACKER_DISABLED_RESEED allocations and re-read the
	// knob, at which point sampling resumes and a site is recorded.
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 1;

	int siteCount = 0;
	for (int i = 0; i < MEMORY_TRACKER_DISABLED_RESEED + 16; i++) {
		auto* p = new int[4];
		p[0] = i;
		escape(p);
		delete[] p;
		if ((i & 0x3ff) == 0) {
			siteCount = 0;
			memTrackerForEachSite([&](const MemoryTrackerCallSite&) { siteCount++; });
			if (siteCount > 0) {
				break;
			}
		}
	}
	siteCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite&) { siteCount++; });
	ASSERT(siteCount > 0);
	ASSERT(g_memTrackerEnabled.value.load(std::memory_order_relaxed));

	k->MEMORY_TRACKING_SAMPLE_INVERSE = prev;
	memTrackerResetForTest();
	return Void();
}

TEST_CASE("/flow/MemoryTracker/freeOfUntrackedPtrIsNoop") {
	// memTrackerOnFree on a pointer the tracker never recorded must be a no-op.
	KnobOverride ko;
	memTrackerResetForTest();

	int x = 0;
	memTrackerOnFree(&x); // not in any table
	memTrackerOnFree(nullptr);

	int siteCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite&) { siteCount++; });
	ASSERT_EQ(siteCount, 0);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/cumulativeIsMonotonic") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	// liveCount must return to ~0 after we free everything we allocated;
	// cumulativeAllocs must NOT decrement.
	KnobOverride ko;
	memTrackerResetForTest();

	void* sentinel = triggerOperatorNewSentinel(100, 8);

	int64_t maxCumulative = 0;
	int64_t finalLive = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		for (int i = 0; i < s.exemplarFrameCount; i++) {
			if (frameInside(s.exemplarFrames[i], sentinel)) {
				if (s.cumulativeAllocs > maxCumulative) {
					maxCumulative = s.cumulativeAllocs;
				}
				finalLive += s.liveCount;
			}
		}
	});

	ASSERT(maxCumulative >= 100);
	ASSERT_EQ(finalLive, 0); // every alloc was paired with delete
	return Void();
}

TEST_CASE("/flow/MemoryTracker/estimateScaling") {
	// End-to-end estimate check. With a fixed inverse N > 1 and no force-sampled
	// blocks, every sample at a site carries weight N, so the site's estimated
	// usage must be *exactly* N times its raw sampled counters. This verifies
	// the reported Est* numbers without depending on which specific allocations
	// happened to be sampled. Runs on all platforms (no frame inspection).
	constexpr int N = 8;
	KnobOverride ko(N);
	memTrackerResetForTest();

	// Small allocations far below the force-sample threshold, so none
	// are force-sampled and every sampled block gets weight N.
	std::vector<int*> ptrs;
	ptrs.reserve(5000);
	for (int i = 0; i < 5000; i++) {
		auto* p = new int[4];
		escape(p);
		ptrs.push_back(p);
	}

	int checked = 0;
	int64_t rawLiveBefore = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		if (s.forceSampledCount != 0) {
			return; // ignore any incidental force-sampled site (weight 1, not N)
		}
		ASSERT_EQ(s.estCumulativeBytes, s.cumulativeBytes * N);
		ASSERT_EQ(s.estCumulativeAllocs, s.cumulativeAllocs * N);
		ASSERT_EQ(s.estLiveBytes, s.liveBytes * N);
		ASSERT_EQ(s.estLiveCount, s.liveCount * N);
		ASSERT_EQ(s.estPeakBytes, s.peakBytes * N);
		rawLiveBefore += s.liveBytes;
		checked++;
	});
	ASSERT(checked > 0);
	ASSERT(rawLiveBefore > 0);

	for (auto* p : ptrs) {
		delete[] p;
	}
	ptrs.clear();

	// Symmetric debit: the per-site scaling invariant must still hold after the
	// frees (each free debits the estimate by exactly weight×size), and the live
	// total must have dropped. We check the invariant rather than "live == 0"
	// because incidental still-live allocations (e.g. the ptrs vector's own
	// backing buffer) legitimately remain tracked.
	int64_t rawLiveAfter = 0;
	int64_t estLiveAfter = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		if (s.forceSampledCount != 0) {
			return;
		}
		ASSERT_EQ(s.estLiveBytes, s.liveBytes * N);
		rawLiveAfter += s.liveBytes;
		estLiveAfter += s.estLiveBytes;
	});
	ASSERT_EQ(estLiveAfter, rawLiveAfter * N);
	ASSERT(rawLiveAfter < rawLiveBefore); // the freed blocks were debited

	memTrackerResetForTest();
	return Void();
}

// ---------------------------------------------------------------------------
// Accounting tests. The "sites with the sentinel's frames" assertion is the
// main check here.

TEST_CASE("/flow/MemoryTracker/fastAlloc32Accounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	KnobOverride ko;
	constexpr int N = 30;

	std::vector<void*> ptrs;
	ptrs.reserve(N);

	memTrackerResetForTest();
	void* sentinel = allocateFastAlloc32Sentinel(N, ptrs);

	auto pre = collectAccounting(sentinel);
	if (pre.sitesWithSentinelFrames != 1) {
		dumpSitesForFailure("fastAlloc32Accounting/post-alloc");
	}
	ASSERT_EQ(pre.sitesWithSentinelFrames, 1);
	ASSERT_EQ(pre.cumAllocsSentinel, N);
	ASSERT_EQ(pre.liveCountSentinel, N);
	ASSERT_EQ(pre.cumBytesSentinel, int64_t(N) * 32);
	ASSERT_EQ(pre.liveBytesSentinel, pre.cumBytesSentinel);
	// Global totals are intentionally not asserted: at inverse=1 a foreign-thread
	// allocation in the window would break a strict global equality (flaky at
	// Joshua scale); the sentinel-scoped checks above pin the regression.

	releaseFastAlloc32(ptrs);

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	// Global live totals intentionally not asserted (flaky at inverse=1; see above).
	ASSERT_EQ(post.cumAllocsSentinel, N); // cumulative never decrements
	return Void();
}

TEST_CASE("/flow/MemoryTracker/arenaSmallAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	KnobOverride ko;
	constexpr int N = 30;

	std::vector<Arena> arenas;
	arenas.reserve(N);

	memTrackerResetForTest();
	void* sentinel = allocateArenaSmallSentinel(N, arenas);

	auto pre = collectAccounting(sentinel);
	if (pre.sitesWithSentinelFrames != 1) {
		dumpSitesForFailure("arenaSmallAccounting/post-alloc");
	}
	ASSERT_EQ(pre.sitesWithSentinelFrames, 1);
	ASSERT_EQ(pre.cumAllocsSentinel, N);
	ASSERT_EQ(pre.liveCountSentinel, N);
	// liveBytes == cumulativeBytes since nothing freed yet.
	ASSERT_EQ(pre.liveBytesSentinel, pre.cumBytesSentinel);
	// Global totals are intentionally not asserted: at inverse=1 a foreign-thread
	// allocation in the window would break a strict global equality (flaky at
	// Joshua scale); the sentinel-scoped checks above pin the regression.

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	// Global live totals intentionally not asserted (flaky at inverse=1; see above).
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/arenaMediumAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	// Make sure arenas aren't counted twice, once due to their direct
	// instrumentation and a second time due to their use of operator new.
	KnobOverride ko;
	constexpr int N = 30;

	std::vector<Arena> arenas;
	arenas.reserve(N);

	memTrackerResetForTest();
	void* sentinel = allocateArenaMediumSentinel(N, arenas);

	auto pre = collectAccounting(sentinel);
	if (pre.sitesWithSentinelFrames != 1) {
		dumpSitesForFailure("arenaMediumAccounting/post-alloc");
	}
	ASSERT_EQ(pre.sitesWithSentinelFrames, 1);
	ASSERT_EQ(pre.cumAllocsSentinel, N);
	ASSERT_EQ(pre.liveCountSentinel, N);
	ASSERT_EQ(pre.liveBytesSentinel, pre.cumBytesSentinel);
	// Global totals intentionally not asserted (flaky at inverse=1; see above).

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	// Global live totals intentionally not asserted (flaky at inverse=1; see above).
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/arenaHugeAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	// Again ensure arena allocations aren't counted twice.
	KnobOverride ko;
	constexpr int N = 10;

	std::vector<Arena> arenas;
	arenas.reserve(N);

	memTrackerResetForTest();
	void* sentinel = allocateArenaHugeSentinel(N, arenas);

	auto pre = collectAccounting(sentinel);
	if (pre.sitesWithSentinelFrames != 1) {
		dumpSitesForFailure("arenaHugeAccounting/post-alloc");
	}
	ASSERT_EQ(pre.sitesWithSentinelFrames, 1);
	ASSERT_EQ(pre.cumAllocsSentinel, N);
	ASSERT_EQ(pre.liveCountSentinel, N);
	ASSERT_EQ(pre.liveBytesSentinel, pre.cumBytesSentinel);
	// Global totals intentionally not asserted (flaky at inverse=1; see above).

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	// Global live totals intentionally not asserted (flaky at inverse=1; see above).
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}
