/*
 * MemoryTrackerTest.cpp — unit tests for the per-call-site memory tracker.
 *
 * The "coverage" test uses sentinel functions: each sentinel triggers exactly
 * one allocation path (operator new, FastAllocator, Arena), and the test
 * confirms that some call site in the aggregation table contains a frame
 * inside that sentinel's body. We compare raw return-address values against
 * function-pointer values at runtime, so this works on stripped builds with
 * no symbolization.
 */

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
// and there's no double-tracking. The Arena medium/huge tests are the explicit
// regression tests for B1 — pre-fix they see two sites with the sentinel's
// frames (operator new[] hook + explicit Arena hook); post-fix only one.

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
	for (void* p : ptrs)
		FastAllocator<32>::release(p);
	ptrs.clear();
}

struct AccountingSummary {
	int sitesWithSentinelFrames = 0;
	int64_t cumBytesSentinel = 0;
	int64_t cumAllocsSentinel = 0;
	int64_t liveBytesSentinel = 0;
	int64_t liveCountSentinel = 0;
	int64_t totalCumBytes = 0;
	int64_t totalLiveBytes = 0;
	int64_t totalLiveCount = 0;
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
		acc.totalCumBytes += s.cumulativeBytes;
		acc.totalLiveBytes += s.liveBytes;
		acc.totalLiveCount += s.liveCount;
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
		for (int i = 0; i < s.exemplarFrameCount; i++)
			fprintf(stderr, "%p ", s.exemplarFrames[i]);
		fprintf(stderr, "\n");
	});
}

class KnobOverride {
public:
	KnobOverride() : prevInverse(FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE) {
		auto* k = const_cast<FlowKnobs*>(FLOW_KNOBS);
		k->MEMORY_TRACKING_SAMPLE_INVERSE = 1;
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
			if (frameInside(s.exemplarFrames[i], opNew))
				foundOpNew = true;
			if (frameInside(s.exemplarFrames[i], fastAlloc))
				foundFastAlloc = true;
			if (frameInside(s.exemplarFrames[i], arena))
				foundArena = true;
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

	k->MEMORY_TRACKING_SAMPLE_INVERSE = prev;
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
				if (s.cumulativeAllocs > maxCumulative)
					maxCumulative = s.cumulativeAllocs;
				finalLive += s.liveCount;
			}
		}
	});

	ASSERT(maxCumulative >= 100);
	ASSERT_EQ(finalLive, 0); // every alloc was paired with delete
	return Void();
}

// ---------------------------------------------------------------------------
// Accounting tests. The "sites with the sentinel's frames" assertion is the
// load-bearing check: if more than one site has the sentinel's frames, two
// hooks fired on the same allocation (regression of B1).

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
	// No accounting bytes outside the sentinel-touching site.
	ASSERT_EQ(pre.totalCumBytes, pre.cumBytesSentinel);
	ASSERT_EQ(pre.totalLiveBytes, pre.liveBytesSentinel);

	releaseFastAlloc32(ptrs);

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	ASSERT_EQ(post.totalLiveBytes, 0);
	ASSERT_EQ(post.totalLiveCount, 0);
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
	// No accounting bytes outside the sentinel-touching site.
	ASSERT_EQ(pre.totalCumBytes, pre.cumBytesSentinel);
	ASSERT_EQ(pre.totalLiveBytes, pre.liveBytesSentinel);

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	ASSERT_EQ(post.totalLiveBytes, 0);
	ASSERT_EQ(post.totalLiveCount, 0);
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/arenaMediumAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	// B1 regression test. Pre-fix this fails because the >256 ArenaBlock
	// path triggers BOTH the explicit memTrackerOnAlloc hook AND the
	// global operator new[] override (via allocateAndMaybeKeepalive's
	// `new uint8_t[]`), producing two sites with the sentinel's frames
	// and double-tracked totals.
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
	ASSERT_EQ(pre.totalCumBytes, pre.cumBytesSentinel);
	ASSERT_EQ(pre.totalLiveBytes, pre.liveBytesSentinel);

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	ASSERT_EQ(post.totalLiveBytes, 0);
	ASSERT_EQ(post.totalLiveCount, 0);
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}

TEST_CASE("/flow/MemoryTracker/arenaHugeAccounting") {
#ifndef __linux__
	return Void(); // see /coverage for rationale
#endif
	// B1 regression test for the huge path of ArenaBlock::create.
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
	ASSERT_EQ(pre.totalCumBytes, pre.cumBytesSentinel);
	ASSERT_EQ(pre.totalLiveBytes, pre.liveBytesSentinel);

	arenas.clear();

	auto post = collectAccounting(sentinel);
	ASSERT_EQ(post.liveBytesSentinel, 0);
	ASSERT_EQ(post.liveCountSentinel, 0);
	ASSERT_EQ(post.totalLiveBytes, 0);
	ASSERT_EQ(post.totalLiveCount, 0);
	ASSERT_EQ(post.cumAllocsSentinel, N);
	return Void();
}
