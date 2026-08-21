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
#include "flow/Platform.h"
#include "flow/UnitTest.h"

#include <climits>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <new>
#include <vector>

// Force this TU to link. The TEST_CASE macro registers via a static
// initializer; in a static library, a TU containing only static initializers
// gets dropped by the linker because nothing references its symbols.
// fdbserver/workloads/UnitTests.cpp calls this function to keep the TU.
void forceLinkMemoryTrackerTests() {}

#if FDB_MEMORY_TRACKER

namespace {

// A sentinel is an out-of-line function that performs exactly one kind of
// allocation, then returns its own address. We use the returned address to
// recognize captured stack frames that fell inside the sentinel's body.
constexpr uintptr_t SENTINEL_FUNC_SIZE = 4096;

// Defeat clang -O3 heap elision (P0593): if the allocated pointer doesn't
// escape, the compiler is free to drop the new/delete pair entirely, which
// then never reaches our operator-new override and the test sees zero samples.
void* volatile gEscapeSink;
inline void escape(void* p) {
	gEscapeSink = p;
}

bool frameInside(void* frame, void* sentinel) {
	uintptr_t f = reinterpret_cast<uintptr_t>(frame);
	uintptr_t s = reinterpret_cast<uintptr_t>(sentinel);
	return f >= s && f < s + SENTINEL_FUNC_SIZE;
}

force_noinline void* triggerOperatorNewSentinel(int n, int k) {
	for (int i = 0; i < n; i++) {
		auto* p = new int[k];
		p[0] = i;
		escape(p);
		delete[] p;
	}
	return reinterpret_cast<void*>(&triggerOperatorNewSentinel);
}

force_noinline void* triggerFastAllocSentinel(int n) {
	for (int i = 0; i < n; i++) {
		void* p = FastAllocator<32>::allocate();
		escape(p);
		FastAllocator<32>::release(p);
	}
	return reinterpret_cast<void*>(&triggerFastAllocSentinel);
}

force_noinline void* triggerArenaSentinel(int n) {
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
force_noinline void* allocateArenaMediumSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[600];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaMediumSentinel);
}

force_noinline void* allocateArenaHugeSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[100000];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaHugeSentinel);
}

force_noinline void* allocateArenaSmallSentinel(int n, std::vector<Arena>& arenas) {
	for (int i = 0; i < n; i++) {
		arenas.emplace_back();
		auto* p = new (arenas.back()) uint8_t[64];
		escape(p);
	}
	return reinterpret_cast<void*>(&allocateArenaSmallSentinel);
}

force_noinline void* allocateOperatorNewSentinel(int n, int k, std::vector<int*>& ptrs) {
	for (int i = 0; i < n; i++) {
		auto* p = new int[k];
		escape(p);
		ptrs.push_back(p);
	}
	return reinterpret_cast<void*>(&allocateOperatorNewSentinel);
}

force_noinline void* allocateFastAlloc32Sentinel(int n, std::vector<void*>& ptrs) {
	for (int i = 0; i < n; i++) {
		void* p = FastAllocator<32>::allocate();
		escape(p);
		ptrs.push_back(p);
	}
	return reinterpret_cast<void*>(&allocateFastAlloc32Sentinel);
}

force_noinline void releaseFastAlloc32(std::vector<void*>& ptrs) {
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
	// With sample inverse 0, no allocations are attributed. memTrackerResetForTest
	// arms this thread's off-latch straight from the knob (as memTrackerInit does
	// at startup), so even the first allocation short-circuits.
	auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
	int prev = k->MEMORY_TRACKING_SAMPLE_INVERSE;
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 0;

	memTrackerResetForTest();

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

TEST_CASE("/flow/MemoryTracker/operatorNewHonorsNewHandler") {
	// The global operator new override (fdbserver/GlobalNewDelete.cpp) must run
	// the std::new_handler retry loop so an allocation failure reaches FDB's OOM
	// path instead of throwing straight past it. (Where the override isn't linked,
	// the standard library's operator new provides the same contract, so this
	// still passes.)
	static bool handlerRan;
	handlerRan = false;
	std::new_handler prev = std::set_new_handler([]() {
		handlerRan = true;
		throw std::bad_alloc(); // break the retry loop
	});

	bool caught = false;
	try {
		// volatile so the compiler can't fold the size and warn (-Walloc-size); malloc
		// reliably fails for SIZE_MAX, driving the handler loop.
		volatile std::size_t huge = std::numeric_limits<std::size_t>::max();
		void* p = ::operator new(huge);
		escape(p);
	} catch (const std::bad_alloc&) {
		caught = true;
	}
	std::set_new_handler(prev);

	ASSERT(handlerRan);
	ASSERT(caught);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/samplingRate") {
	// The reseed gap is uniform on [1, 2N-1] (mean N), so at inverse N the sampled
	// fraction should be ~1/N. Wide bounds keep it non-flaky across RNG state.
	constexpr int N = 10;
	constexpr int ALLOCS = 200000;
	KnobOverride ko(N);
	memTrackerResetForTest();

	std::vector<int*> ptrs;
	ptrs.reserve(ALLOCS);
	for (int i = 0; i < ALLOCS; i++) {
		auto* p = new int[4];
		escape(p);
		ptrs.push_back(p);
	}

	int64_t sampled = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		if (s.forceSampledCount == 0) {
			sampled += s.cumulativeAllocs;
		}
	});

	for (auto* p : ptrs) {
		delete[] p;
	}
	ptrs.clear();

	double frac = double(sampled) / ALLOCS;
	ASSERT(frac > 0.06 && frac < 0.15); // expect ~0.1
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
	// Verify the huge Arena-block path (reqSize >= LARGE), including that a block is
	// tracked once (not double-counted by both the explicit Arena hook and the inner
	// operator new[]). This is the deepest tracked call chain, and under a real
	// (non-simulation) network its frame-pointer backtrace is unreliable — the
	// best-effort walker may, run to run and even alloc to alloc, fail to climb to
	// the test's frame or attribute the blocks to varying fingerprints. So instead of
	// the sentinel-frame approach the other *Accounting tests use, we identify the
	// huge blocks by their unmistakable ~100 KB size signature: the recorded size is
	// correct regardless of which frames were captured, and no incidental or
	// foreign-thread allocation comes anywhere near this large.
	KnobOverride ko;
	constexpr int N = 10;
	constexpr int64_t BLOCK = 100000;
	constexpr int64_t HUGE_MIN = 90000; // mean bytes/alloc of a huge site; nothing else is this big

	std::vector<Arena> arenas;
	arenas.reserve(N);

	memTrackerResetForTest();
	allocateArenaHugeSentinel(N, arenas);

	int64_t cumAllocs = 0, cumBytes = 0, liveBytes = 0, liveCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		if (s.cumulativeAllocs > 0 && s.cumulativeBytes / s.cumulativeAllocs >= HUGE_MIN) {
			cumAllocs += s.cumulativeAllocs;
			cumBytes += s.cumulativeBytes;
			liveBytes += s.liveBytes;
			liveCount += s.liveCount;
		}
	});
	// Exactly N huge blocks, each tracked once (double-tracking would show as 2N),
	// all currently live, with live bytes == cumulative bytes (nothing freed yet).
	ASSERT_EQ(cumAllocs, N);
	ASSERT_EQ(liveCount, N);
	ASSERT_EQ(liveBytes, cumBytes);
	ASSERT(cumBytes >= int64_t(N) * BLOCK);

	arenas.clear();

	int64_t cumAllocsPost = 0, liveBytesPost = 0, liveCountPost = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite& s) {
		if (s.cumulativeAllocs > 0 && s.cumulativeBytes / s.cumulativeAllocs >= HUGE_MIN) {
			cumAllocsPost += s.cumulativeAllocs;
			liveBytesPost += s.liveBytes;
			liveCountPost += s.liveCount;
		}
	});
	// After freeing, the huge blocks are debited: live returns to 0, cumulative persists.
	ASSERT_EQ(liveBytesPost, 0);
	ASSERT_EQ(liveCountPost, 0);
	ASSERT_EQ(cumAllocsPost, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/operatorNewAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	KnobOverride ko;
	constexpr int N = 30;
	constexpr int K = 8; // new int[8] -> 32 bytes; int is trivial so no array cookie

	std::vector<int*> ptrs;
	ptrs.reserve(N);

	memTrackerResetForTest();
	void* sentinel = allocateOperatorNewSentinel(N, K, ptrs);

	auto pre = collectAccounting(sentinel);
	if (pre.sitesWithSentinelFrames != 1) {
		dumpSitesForFailure("operatorNewAccounting/post-alloc");
	}
	ASSERT_EQ(pre.sitesWithSentinelFrames, 1);
	ASSERT_EQ(pre.cumAllocsSentinel, N);
	ASSERT_EQ(pre.liveCountSentinel, N);
	ASSERT_EQ(pre.cumBytesSentinel, static_cast<int64_t>(N) * K * static_cast<int64_t>(sizeof(int)));
	ASSERT_EQ(pre.liveBytesSentinel, pre.cumBytesSentinel);
	// Global totals intentionally not asserted (flaky at inverse=1; see above).

	for (auto* p : ptrs) {
		delete[] p;
	}
	ptrs.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	// Global live totals intentionally not asserted (flaky at inverse=1; see above).
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/failOpenOnMetadataAllocFailure") {
	// The tracker must fail open: if one of its own bookkeeping allocations fails,
	// the underlying user allocation still succeeds, the process is NOT terminated
	// through the installed std::new_handler, and tracking recovers on this thread
	// afterward (the reentrancy guards are restored, not leaked). Runs on all
	// platforms — no frame inspection.
	//
	// Unlike a throw injected at the top of the sampled path, this drives the real
	// failure route: the one-shot forces an actual tracker-internal allocation
	// (map growth) through the global operator new override, which — seeing the
	// tracker-internal context — throws std::bad_alloc WITHOUT running the handler.
	KnobOverride ko; // inverse = 1: sample every allocation
	memTrackerResetForTest();

	// Install a new_handler that records if it ran. The tracker-internal fail-open
	// path must never reach it; if it does (bug), we still break the retry loop so
	// the test fails cleanly instead of spinning or aborting via the real
	// platform::outOfMemory handler.
	static bool handlerRan;
	handlerRan = false;
	std::new_handler prevHandler = std::set_new_handler([]() {
		handlerRan = true;
		throw std::bad_alloc();
	});

	// Arm the one-shot; the next tracker-internal allocation is forced to fail
	// inside mallocWithNewHandler, which throws bad_alloc (fail open) and the
	// tracker swallows it. The user's new[] must observe none of this.
	//
	// escape(p) is essential: at -O3 clang elides a new/delete pair whose pointer
	// never escapes (P0593), so without it the allocation never reaches the global
	// operator new, the sampler never runs, and the one-shot is never consumed — the
	// test would pass without exercising anything (see the consumption assert below).
	memTrackerFailNextInternalAllocForTest();
	int* p = new int[4];
	ASSERT(p != nullptr);
	p[0] = 42;
	escape(p);
	int observed = p[0];
	delete[] p;

	std::set_new_handler(prevHandler);

	// The one-shot must have been consumed by a real tracker-internal allocation;
	// if it's still armed, no tracker metadata allocation went through the global
	// operator new and the fail-open path was never exercised (the test would then
	// pass trivially — exactly the weakness this rewrite fixes).
	ASSERT(!gMemTrackerFailNextInternalAllocForTest);
	ASSERT_EQ(observed, 42);
	ASSERT(!handlerRan); // fail-open path must bypass the fatal OOM handler

	// Tracking must still work after the injected failure.
	memTrackerResetForTest();
	std::vector<int*> ptrs;
	ptrs.reserve(8);
	for (int i = 0; i < 8; i++) {
		auto* q = new int[4];
		escape(q);
		ptrs.push_back(q);
	}
	int siteCount = 0;
	memTrackerForEachSite([&](const MemoryTrackerCallSite&) { siteCount++; });
	for (auto* q : ptrs) {
		delete[] q;
	}
	ASSERT(siteCount > 0);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/initEnablesFromKnob") {
	// Exercises the *production* enablement path (memTrackerInit), not the test-only
	// reset — the reviewer noted that memTrackerResetForTest masks the startup race.
	// memTrackerInit must set the global enabled flag and this thread's fast-path
	// off-latch straight from MEMORY_TRACKING_SAMPLE_INVERSE, and (the regression)
	// must re-arm a thread that a prior off-configuration had already latched off.
	auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
	int prev = k->MEMORY_TRACKING_SAMPLE_INVERSE;

	// Sampling off: init publishes disabled and latches this thread off. This is the
	// state an early startup allocation used to get stuck in before init existed.
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 0;
	memTrackerInit();
	ASSERT(!g_memTrackerEnabled.value.load(std::memory_order_relaxed));
	ASSERT(gMemTrackerOff); // alloc hot path short-circuits

	// Knobs now configured with sampling on: init must re-arm THIS thread. The bug
	// was that nothing re-armed the network thread once it latched off before the
	// knobs were ready, so sampling stayed dead for the life of the process.
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 8;
	memTrackerInit();
	ASSERT(g_memTrackerEnabled.value.load(std::memory_order_relaxed));
	ASSERT(!gMemTrackerOff); // re-armed: alloc hot path now reaches the sampler

	// And the reverse transition: a subsequent off-configuration re-latches it.
	k->MEMORY_TRACKING_SAMPLE_INVERSE = 0;
	memTrackerInit();
	ASSERT(!g_memTrackerEnabled.value.load(std::memory_order_relaxed));
	ASSERT(gMemTrackerOff);

	k->MEMORY_TRACKING_SAMPLE_INVERSE = prev;
	memTrackerInit(); // restore tracker state from the baseline knob for later tests
	return Void();
}

#endif // FDB_MEMORY_TRACKER
