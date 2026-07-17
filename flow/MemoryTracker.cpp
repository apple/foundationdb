/*
 * MemoryTracker.cpp
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

// Implementation of the sampled per-call-site memory tracker.
// See design/memory-tracker.md and flow/include/flow/MemoryTracker.h.

#include "flow/MemoryTracker.h"

#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/ThreadPrimitives.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#endif

// Thread-local sampling state.
// gMemTrackerCounter starts at 1 so the first allocation per thread is
// sampled (and the slow path then reseeds from the knob).
// gForceSampleBytes initialized to ~0 so force-sample never fires before we've
// loaded the knob value at least once.
thread_local bool gInMemTracker = false;
thread_local int gMemTrackerCounter = 1;
thread_local std::size_t gForceSampleBytes = static_cast<std::size_t>(-1);

// Definition of the cache-line-isolated enabled flag declared in the header.
MemTrackerEnabledFlag g_memTrackerEnabled;
// Same initial seed for every thread; cheap and adequate. Threads in
// production start at different times and call into the slow path at
// uncorrelated rates, so any phase correlation washes out within the
// first handful of samples. If profiling ever shows correlated bursts
// at startup, mix in a thread-id-derived value here.
thread_local uint32_t gMemTrackerSeed = 0x9E3779B9u;

namespace {

// FNV-1a 64-bit over the captured frame array.
uint64_t fnv64(const void* data, std::size_t len) {
	uint64_t h = 0xcbf29ce484222325ULL;
	const auto* p = static_cast<const std::uint8_t*>(data);
	for (std::size_t i = 0; i < len; i++) {
		h ^= p[i];
		h *= 0x100000001b3ULL;
	}
	return h;
}

inline std::uint32_t xorshift32(std::uint32_t& s) {
	std::uint32_t x = s ? s : 0x9E3779B9u;
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	s = x;
	return x;
}

// Per-thread stack bounds, populated lazily on first use of captureFramesFP.
// Used by captureFramesFP to terminate the FP walk when it crosses into
// FP-elided code (notably glibc's pthread shutdown / TLS-destructor
// machinery). Without this guard, the FP-elided frame leaves an
// uninitialized saved-FP slot and the walk dereferences garbage. See
// design/memory-tracker.md, "Side-thread safety".
//
// You've heard of optimistic concurrency control in database systems? This is
// basically *optimistic segfault avoidance* to enable fast stack unwinding.
// Caveat: the heuristics here aren't perfect. They seem pretty effective
// so far.
thread_local uintptr_t gStackLow = 0;
thread_local uintptr_t gStackHigh = 0;

#ifdef __linux__

void initStackBoundsForThread() {
	pthread_attr_t attr;
	if (pthread_getattr_np(pthread_self(), &attr) == 0) {
		void* base = nullptr;
		size_t size = 0;
		if (pthread_attr_getstack(&attr, &base, &size) == 0) {
			gStackLow = reinterpret_cast<uintptr_t>(base);
			gStackHigh = gStackLow + size;
		}
		pthread_attr_destroy(&attr);
	}
}

// Manual frame-pointer walk. Captures the return-address chain starting at
// the caller of this function (and up). Relies on -fno-omit-frame-pointer.
// Annotated noinline + no_instrument_function so the compiler can't fold the
// frame chain in unexpected ways.
//
// Bounds the walk by the current thread's stack range so that crossing into
// FP-elided code (which leaves the saved-FP slot uninitialized rather than
// NULL) terminates cleanly instead of dereferencing garbage (see above).
__attribute__((no_instrument_function, noinline)) int captureFramesFP(void** out, int max) {
	if (!gStackLow) {
		initStackBoundsForThread();
	}
	void** fp = static_cast<void**>(__builtin_frame_address(0));
	// Fallback for threads where pthread_getattr_np failed: ±8 MB around
	// the initial frame.
	// Caveat: this may need to be constrained more tightly to deal with
	// smaller stacks.
	uintptr_t lo = gStackLow ? gStackLow : reinterpret_cast<uintptr_t>(fp);
	uintptr_t hi = gStackHigh ? gStackHigh : reinterpret_cast<uintptr_t>(fp) + (8u << 20);
	int n = 0;
	while (fp && n < max) {
		uintptr_t a = reinterpret_cast<uintptr_t>(fp);
		// Reject out-of-stack or misaligned fp before dereferencing.
		if (a < lo || a + 16 > hi) {
			break;
		}
		if (a & (sizeof(void*) - 1)) {
			break;
		}
		void* ra = fp[1];
		if (!ra) {
			break;
		}
		out[n++] = ra;
		void** next = static_cast<void**>(fp[0]);
		// Sanity: stack grows down, so each next frame address must be larger.
		if (next <= fp) {
			break;
		}
		fp = next;
	}
	return n;
}

#else // !__linux__

// macOS / non-Linux: stack walking is unreliable here (system runtime
// has -fomit-frame-pointer in places we can't avoid, and pthread_getattr_np
// is Linux-specific). FDB is required to compile on macOS but is not run
// in production there, so we just no-op the walker. The rest of the
// tracker still compiles and runs; per-call-site reports will simply
// lack stack attribution.
__attribute__((no_instrument_function, noinline)) int captureFramesFP(void**, int) {
	return 0;
}

#endif // __linux__

// TODO: when memory tracking is enabled, regardless of the sampling
// rate, every deallocation has to acquire this mutex. At O(1M)
// frees/second this is noticeable overhead.  This could be sped up by
// sharding the mutex and the global state protected by the mutex
// (the global state is the 2 maps and ~10 scalars defined below).
// Presumably hash of the malloc buffer address itself could direct
// the sharding.  Consumers of the global state would have to merge
// across shards but that is intended only to be the periodic log
// reporter so making it do costly work is fine since it only runs
// O(1/minute).

ThreadSpinLock g_mtLock;

struct LiveEntry {
	std::uint64_t fingerprint;
	std::uint64_t size;
	std::int64_t weight; // inverse inclusion probability at sample time (≈ SampleInverse, or 1 if
	                     // force-sampled); estimated contribution of this block is size * weight.
	                     // Stored so free debits the estimate by exactly what alloc credited, even
	                     // if the sampling knob changed in between.
};

// Lazily-constructed maps. Allocated under the spinlock the first time we
// reach the sampled path. Heap allocations from the maps' internals go
// through our overridden operator new, which short-circuits (gInMemTracker
// is true on the sampled path) and falls through to std::malloc — so map
// growth never recurses into tracking.
std::unordered_map<std::uint64_t, MemoryTrackerCallSite>* g_aggMap = nullptr;
std::unordered_map<std::uintptr_t, LiveEntry>* g_liveMap = nullptr;

// Sampled-totals (i.e. across what we actually saw, not population estimates).
std::int64_t g_liveBytesTotal = 0;
std::int64_t g_liveBlocksTotal = 0;
std::int64_t g_cumulativeBytesTotal = 0;
std::int64_t g_cumulativeAllocsTotal = 0;
std::int64_t g_samplesEmitted = 0;

// Estimated population totals (sampling correction applied; see LiveEntry::weight).
std::int64_t g_estLiveBytesTotal = 0;
std::int64_t g_estLiveBlocksTotal = 0;
std::int64_t g_estCumulativeBytesTotal = 0;
std::int64_t g_estCumulativeAllocsTotal = 0;

void ensureMaps() {
	if (!g_aggMap) {
		g_aggMap = new std::unordered_map<std::uint64_t, MemoryTrackerCallSite>();
	}
	if (!g_liveMap) {
		g_liveMap = new std::unordered_map<std::uintptr_t, LiveEntry>();
	}
}

} // namespace

void memTrackerSampleAlloc(void* p, std::size_t n) {
	int inverse = 0;
	int frames = 6;
	bool liveTracking = true;
	if (FLOW_KNOBS) {
		inverse = FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE;
		frames = FLOW_KNOBS->MEMORY_TRACKING_FRAMES;
		liveTracking = FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING;
		gForceSampleBytes = static_cast<std::size_t>(FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES);
	}
	if (frames < 1) {
		frames = 1;
	}
	if (frames > MEMORY_TRACKER_MAX_FRAMES) {
		frames = MEMORY_TRACKER_MAX_FRAMES;
	}
	// Bound the reseed's `2 * inverse` arithmetic to int range for absurd knob
	// values; 1-in-256M sampling is already effectively off.
	if (inverse > (1 << 28)) {
		inverse = 1 << 28;
	}

	// Publish the enabled state for the free hot path's gate. Store only on an
	// actual change so the flag's cache line stays in MESI shared state (see
	// MemTrackerEnabledFlag in the header). Published once, since the sample knob
	// is read at startup only and not changed at runtime.
	bool enabled = (inverse > 0);
	if (g_memTrackerEnabled.value.load(std::memory_order_relaxed) != enabled) {
		g_memTrackerEnabled.value.store(enabled, std::memory_order_relaxed);
	}

	if (inverse <= 0) {
		// Off. The knob is startup-only, so park the counter effectively forever
		// (INT_MAX) and stop force-sampling; this thread stays on the fast path.
		gMemTrackerCounter = INT_MAX;
		gForceSampleBytes = static_cast<std::size_t>(-1);
		return;
	}
	if (inverse == 1) {
		// Sample every allocation — keep counter at 1 so the next decrement
		// drops it to 0 and re-enters the slow path. Bypass the random
		// reseed below, which would otherwise leave counter==2 half the
		// time and cause us to miss every other allocation.
		gMemTrackerCounter = 1;
	} else if (gMemTrackerCounter <= 0) {
		// The random countdown actually expired: draw a fresh gap uniformly from
		// [1, 2*inverse-1], whose mean is exactly `inverse` — so the 1-in-inverse
		// sampling rate is unbiased and the integer `weight` below is exact.
		std::uint32_t r = xorshift32(gMemTrackerSeed);
		gMemTrackerCounter = 1 + static_cast<int>(r % static_cast<std::uint32_t>(2 * inverse - 1));
	}

	bool isForceSampled = (n >= gForceSampleBytes);

	// Weight = inverse inclusion probability of this sample, i.e. how many
	// allocations in the population it stands in for. A randomly-sampled block
	// (1-in-inverse) represents ~inverse allocations; a force-sampled block was
	// captured with certainty and represents only itself. The reseed draws
	// uniformly from [1, 2*inverse-1] (mean exactly `inverse`), so this integer
	// weight matches the true mean sampling gap with no bias.
	std::int64_t weight = (isForceSampled || inverse <= 1) ? 1 : inverse;

	// Capture frames; skip the topmost two (this function and captureFramesFP
	// itself) so the recorded stack starts at the caller of memTrackerOnAlloc.
	// The strip count of 2 assumes memTrackerOnAlloc is inlined into its
	// caller (it's declared `inline` and the body is trivial). Production
	// builds run at -O3 and the inliner cooperates; at -O0 the inline hint
	// can be ignored and the recorded stack starts one frame too deep
	// (frame 0 = memTrackerOnAlloc body rather than the user's
	// allocation site). Acceptable: -O0 builds are not load-bearing for
	// memory attribution; the off-by-one is harmless for that workflow.
	void* tmp[MEMORY_TRACKER_MAX_FRAMES + 4];
	int captured = captureFramesFP(tmp, frames + 2);
	int kept = 0;
	void* keep[MEMORY_TRACKER_MAX_FRAMES];
	for (int i = 2; i < captured && kept < frames; i++) {
		keep[kept++] = tmp[i];
	}

	std::uint64_t fp = (kept == 0) ? 0 : fnv64(keep, static_cast<std::size_t>(kept) * sizeof(void*));

	ThreadSpinLockHolder lk(g_mtLock);
	ensureMaps();

	auto& site = (*g_aggMap)[fp];
	if (site.fingerprint == 0 && site.cumulativeAllocs == 0) {
		site.fingerprint = fp;
		site.exemplarFrameCount = static_cast<std::uint8_t>(kept);
		for (int i = 0; i < kept; i++) {
			site.exemplarFrames[i] = keep[i];
		}
	}
	auto nBytes = static_cast<std::int64_t>(n);
	auto estBytes = nBytes * weight;

	site.cumulativeAllocs += 1;
	site.cumulativeBytes += nBytes;
	site.estCumulativeAllocs += weight;
	site.estCumulativeBytes += estBytes;
	if (isForceSampled) {
		site.forceSampledCount += 1;
	}

	if (liveTracking) {
		auto key = reinterpret_cast<std::uintptr_t>(p);
		// If a stale entry already exists for this address — its free was
		// suppressed (e.g. during memTrackerDump or a memTrackerForEachSite
		// callback) so it was never debited — debit it now before overwriting.
		// Otherwise its live credit leaks permanently once the address is reused
		// and live totals creep upward over long uptime.
		auto stale = g_liveMap->find(key);
		if (stale != g_liveMap->end()) {
			const LiveEntry& old = stale->second;
			auto oldBytes = static_cast<std::int64_t>(old.size);
			auto oldEst = oldBytes * old.weight;
			if (g_aggMap) {
				auto oldSite = g_aggMap->find(old.fingerprint);
				if (oldSite != g_aggMap->end()) {
					oldSite->second.liveBytes -= oldBytes;
					oldSite->second.liveCount -= 1;
					oldSite->second.estLiveBytes -= oldEst;
					oldSite->second.estLiveCount -= old.weight;
				}
			}
			g_liveBytesTotal -= oldBytes;
			g_liveBlocksTotal -= 1;
			g_estLiveBytesTotal -= oldEst;
			g_estLiveBlocksTotal -= old.weight;
		}

		site.liveBytes += nBytes;
		site.liveCount += 1;
		if (site.liveBytes > site.peakBytes) {
			site.peakBytes = site.liveBytes;
		}
		site.estLiveBytes += estBytes;
		site.estLiveCount += weight;
		if (site.estLiveBytes > site.estPeakBytes) {
			site.estPeakBytes = site.estLiveBytes;
		}
		(*g_liveMap)[key] = LiveEntry{ fp, static_cast<std::uint64_t>(n), weight };
		g_liveBytesTotal += nBytes;
		g_liveBlocksTotal += 1;
		g_estLiveBytesTotal += estBytes;
		g_estLiveBlocksTotal += weight;
	}
	g_cumulativeBytesTotal += nBytes;
	g_cumulativeAllocsTotal += 1;
	g_estCumulativeBytesTotal += estBytes;
	g_estCumulativeAllocsTotal += weight;
	g_samplesEmitted += 1;
}

void memTrackerSampleFree(void* p) {
	bool liveTracking = FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING : true;
	if (!liveTracking) {
		return;
	}

	ThreadSpinLockHolder lk(g_mtLock);
	if (!g_liveMap) {
		return;
	}
	auto it = g_liveMap->find(reinterpret_cast<std::uintptr_t>(p));
	if (it == g_liveMap->end()) {
		return;
	}

	LiveEntry e = it->second;
	g_liveMap->erase(it);

	auto eBytes = static_cast<std::int64_t>(e.size);
	auto eEstBytes = eBytes * e.weight;
	if (g_aggMap) {
		auto sit = g_aggMap->find(e.fingerprint);
		if (sit != g_aggMap->end()) {
			sit->second.liveBytes -= eBytes;
			sit->second.liveCount -= 1;
			sit->second.estLiveBytes -= eEstBytes;
			sit->second.estLiveCount -= e.weight;
		}
	}
	g_liveBytesTotal -= eBytes;
	g_liveBlocksTotal -= 1;
	g_estLiveBytesTotal -= eEstBytes;
	g_estLiveBlocksTotal -= e.weight;
}

void memTrackerForEachSite(std::function<void(const MemoryTrackerCallSite&)> cb) {
	// Suppress for the entire call so callbacks that allocate (e.g.
	// fprintf or std::vector growth in test failure paths) don't
	// re-enter the tracker and pollute the agg map mid-iteration.
	MemTrackerSuppress _suppress;
	std::vector<MemoryTrackerCallSite> snapshot;
	{
		ThreadSpinLockHolder lk(g_mtLock);
		if (g_aggMap) {
			snapshot.reserve(g_aggMap->size());
			for (auto& kv : *g_aggMap) {
				snapshot.push_back(kv.second);
			}
		}
	}
	for (auto& s : snapshot) {
		cb(s);
	}
}

void memTrackerResetForTest() {
	MemTrackerSuppress _suppress;
	{
		ThreadSpinLockHolder lk(g_mtLock);
		if (g_aggMap) {
			g_aggMap->clear();
		}
		if (g_liveMap) {
			g_liveMap->clear();
		}
		g_liveBytesTotal = 0;
		g_liveBlocksTotal = 0;
		g_cumulativeBytesTotal = 0;
		g_cumulativeAllocsTotal = 0;
		g_samplesEmitted = 0;
		g_estLiveBytesTotal = 0;
		g_estLiveBlocksTotal = 0;
		g_estCumulativeBytesTotal = 0;
		g_estCumulativeAllocsTotal = 0;
	}
	// Force the next allocation on this thread to take the slow path so it
	// re-reads the (possibly just-changed) sample-inverse knob. Without this, a
	// prior disabled run that parked the counter would keep the fast path
	// skipping samples until it drained.
	gMemTrackerCounter = 1;
	gForceSampleBytes = static_cast<std::size_t>(-1);
	// Clear the enabled flag; the next slow-path visit republishes it from the
	// current knob value.
	g_memTrackerEnabled.value.store(false, std::memory_order_relaxed);
}

void memTrackerDump(int64_t bytesThreshold) {
	MemTrackerSuppress _suppress;

	std::vector<MemoryTrackerCallSite> sites;
	int aggSize = 0;
	int liveSize = 0;
	std::int64_t liveBytesTotalSnap = 0;
	std::int64_t liveBlocksTotalSnap = 0;
	std::int64_t cumBytesSnap = 0;
	std::int64_t cumAllocsSnap = 0;
	std::int64_t samplesEmittedSnap = 0;
	std::int64_t estLiveBytesTotalSnap = 0;
	std::int64_t estLiveBlocksTotalSnap = 0;
	std::int64_t estCumBytesSnap = 0;
	std::int64_t estCumAllocsSnap = 0;
	{
		ThreadSpinLockHolder lk(g_mtLock);
		if (g_aggMap) {
			sites.reserve(g_aggMap->size());
			for (auto& kv : *g_aggMap) {
				sites.push_back(kv.second);
			}
			aggSize = static_cast<int>(g_aggMap->size());
		}
		liveSize = g_liveMap ? static_cast<int>(g_liveMap->size()) : 0;
		liveBytesTotalSnap = g_liveBytesTotal;
		liveBlocksTotalSnap = g_liveBlocksTotal;
		cumBytesSnap = g_cumulativeBytesTotal;
		cumAllocsSnap = g_cumulativeAllocsTotal;
		samplesEmittedSnap = g_samplesEmitted;
		estLiveBytesTotalSnap = g_estLiveBytesTotal;
		estLiveBlocksTotalSnap = g_estLiveBlocksTotal;
		estCumBytesSnap = g_estCumulativeBytesTotal;
		estCumAllocsSnap = g_estCumulativeAllocsTotal;
	}

	bool liveTracking = FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING : true;
	// Rank and threshold on the *estimated* usage, since that is the real
	// per-site cost the report is about; the threshold knob is expressed in
	// real bytes (~1% of target RSS), not sampled bytes.
	// std::sort is unstable and unordered_map iteration is bucket-order, so
	// MemoryTrackerSite events for sites with tied byte values may appear in
	// different orders across same-seed sim2 runs. The R5 determinism
	// requirement is on aggregate counts, not event ordering — those are
	// unaffected — so we don't pay for stable_sort here.
	auto byLive = [](const MemoryTrackerCallSite& a, const MemoryTrackerCallSite& b) {
		return a.estLiveBytes > b.estLiveBytes;
	};
	auto byCum = [](const MemoryTrackerCallSite& a, const MemoryTrackerCallSite& b) {
		return a.estCumulativeBytes > b.estCumulativeBytes;
	};
	if (liveTracking) {
		std::sort(sites.begin(), sites.end(), byLive);
	} else {
		std::sort(sites.begin(), sites.end(), byCum);
	}

	// Filter: a site qualifies when its estimated currently-live bytes (or
	// estimated cumulative bytes in degraded mode) exceed the threshold. Sites
	// are already sorted descending, so we can stop at the first non-qualifier.
	std::vector<MemoryTrackerCallSite> qualifying;
	qualifying.reserve(sites.size());
	for (const auto& s : sites) {
		int64_t v = liveTracking ? s.estLiveBytes : s.estCumulativeBytes;
		if (v < bytesThreshold) {
			break;
		}
		qualifying.push_back(s);
	}

	// Build addr2line prefix once per dump. Built directly here rather
	// than via platform::format_backtrace, which deliberately drops index
	// 0 of its input (its single-site use case treats that as the helper's
	// caller); we want every captured frame including the leaf.
	std::string addrCmdPrefix;
	uintptr_t pieOffset = 0;
	if (!qualifying.empty()) {
		platform::ImageInfo img = platform::getImageInfo();
#ifdef __clang__
		const char* addr2lineTool = "/usr/local/bin/llvm-addr2line";
#else
		const char* addr2lineTool = "/usr/bin/addr2line";
#endif
		addrCmdPrefix = format("%s -e %s -p -C -f -i", addr2lineTool, img.symbolFileName.c_str());
		pieOffset = reinterpret_cast<uintptr_t>(img.offset);
	}

	for (const auto& s : qualifying) {
		std::string addrCmd = addrCmdPrefix;
		for (int i = 0; i < s.exemplarFrameCount; i++) {
			uintptr_t pieRelative = reinterpret_cast<uintptr_t>(s.exemplarFrames[i]) - pieOffset;
			addrCmd += format(" 0x%lx", pieRelative);
		}
		TraceEvent("MemoryTrackerSite")
		    .detail("Fingerprint", format("%016llx", static_cast<unsigned long long>(s.fingerprint)))
		    .detail("EstLiveBytes", s.estLiveBytes)
		    .detail("EstLiveCount", s.estLiveCount)
		    .detail("EstPeakBytes", s.estPeakBytes)
		    .detail("EstCumulativeBytes", s.estCumulativeBytes)
		    .detail("EstCumulativeAllocs", s.estCumulativeAllocs)
		    .detail("LiveBytes", s.liveBytes)
		    .detail("LiveCount", s.liveCount)
		    .detail("PeakBytes", s.peakBytes)
		    .detail("CumulativeBytes", s.cumulativeBytes)
		    .detail("CumulativeAllocs", s.cumulativeAllocs)
		    .detail("ForceSampledCount", s.forceSampledCount)
		    .detail("AddrCmd", addrCmd);
	}

	TraceEvent("MemoryTrackerSummary")
	    .detail("SitesTracked", aggSize)
	    .detail("SitesReported", static_cast<int>(qualifying.size()))
	    .detail("EstLiveBytesTotal", estLiveBytesTotalSnap)
	    .detail("EstLiveBlocksTotal", estLiveBlocksTotalSnap)
	    .detail("EstCumulativeBytes", estCumBytesSnap)
	    .detail("EstCumulativeAllocs", estCumAllocsSnap)
	    .detail("LiveBlocks", liveSize)
	    .detail("LiveBytesTotal", liveBytesTotalSnap)
	    .detail("LiveBlocksTotal", liveBlocksTotalSnap)
	    .detail("CumulativeAllocs", cumAllocsSnap)
	    .detail("CumulativeBytes", cumBytesSnap)
	    .detail("SamplesEmitted", samplesEmittedSnap)
	    .detail("SampleInverse", FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE : 0)
	    .detail("ForceSampleBytes",
	            FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES : static_cast<std::int64_t>(-1))
	    .detail("ReportBytesThreshold", bytesThreshold)
	    // Caveat: Est* values are statistical estimates. Each randomly-sampled block is
	    // scaled by SampleInverse; force-sampled blocks (>= ForceSampleBytes) count once.
	    // Accuracy improves with SamplesEmitted; a site with few samples is noisy.
	    .detail("EstimateBasis", "Est*=sampled*SampleInverse; force-sampled weight 1; statistical estimate");
}

// The global operator new / operator delete replacements that route through
// memTrackerOnAlloc/OnFree live in fdbserver/GlobalNewDelete.cpp, not here, so
// the interposition is confined to the fdbserver executable and never ships in
// libfdb_c / client bindings. This TU provides only the tracker machinery those
// overrides (and the FastAllocator / ArenaBlock hooks) call into.
