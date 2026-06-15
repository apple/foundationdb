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
