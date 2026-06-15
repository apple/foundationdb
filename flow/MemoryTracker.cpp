/*
 * MemoryTracker.cpp — implementation of the sampled per-call-site memory tracker.
 * See design/memory-tracker.md and flow/include/flow/MemoryTracker.h.
 */

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
#include <pthread.h>
#include <unordered_map>
#include <vector>

// Thread-local sampling state.
// gMemTrackerCounter starts at 1 so the first allocation per thread is
// sampled (and the slow path then reseeds from the knob).
// gForceSampleBytes initialized to ~0 so force-sample never fires before we've
// loaded the knob value at least once.
thread_local bool gInMemTracker = false;
thread_local int gMemTrackerCounter = 1;
thread_local std::size_t gForceSampleBytes = static_cast<std::size_t>(-1);
// Same initial seed for every thread; cheap and adequate. Threads in
// production start at different times and call into the slow path at
// uncorrelated rates, so any phase correlation washes out within the
// first handful of samples. If profiling ever shows correlated bursts
// at startup, mix in a thread-id-derived value here.
thread_local uint32_t gMemTrackerSeed = 0x9E3779B9u;

namespace {

// FNV-1a 64-bit. Produces a 64-bit fingerprint over the captured frame array.
uint64_t fnv64(const void* data, std::size_t len) {
	uint64_t h = 0xcbf29ce484222325ULL;
	const std::uint8_t* p = static_cast<const std::uint8_t*>(data);
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
// NULL) terminates cleanly instead of dereferencing garbage.
__attribute__((no_instrument_function, noinline)) int captureFramesFP(void** out, int max) {
	if (!gStackLow)
		initStackBoundsForThread();
	void** fp = static_cast<void**>(__builtin_frame_address(0));
	// Fallback for threads where pthread_getattr_np failed: ±8 MB around
	// the initial frame.
	uintptr_t lo = gStackLow ? gStackLow : reinterpret_cast<uintptr_t>(fp);
	uintptr_t hi = gStackHigh ? gStackHigh : reinterpret_cast<uintptr_t>(fp) + (8u << 20);
	int n = 0;
	while (fp && n < max) {
		uintptr_t a = reinterpret_cast<uintptr_t>(fp);
		// Reject out-of-stack or misaligned fp before dereferencing.
		if (a < lo || a + 16 > hi)
			break;
		if (a & (sizeof(void*) - 1))
			break;
		void* ra = fp[1];
		if (!ra)
			break;
		out[n++] = ra;
		void** next = static_cast<void**>(fp[0]);
		// Sanity: stack grows down, so each next frame address must be larger.
		if (next <= fp)
			break;
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

ThreadSpinLock g_mtLock;

struct LiveEntry {
	std::uint64_t fingerprint;
	std::uint32_t size;
	bool forceSampled;
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

void ensureMaps() {
	if (!g_aggMap)
		g_aggMap = new std::unordered_map<std::uint64_t, MemoryTrackerCallSite>();
	if (!g_liveMap)
		g_liveMap = new std::unordered_map<std::uintptr_t, LiveEntry>();
}

} // namespace

void memTrackerSampleAlloc(void* p, std::size_t n) {
	// Refresh thread-local cache from knobs every time we hit the slow path.
	int inverse = 0;
	int frames = 6;
	bool liveTracking = true;
	if (FLOW_KNOBS) {
		inverse = FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE;
		frames = FLOW_KNOBS->MEMORY_TRACKING_FRAMES;
		liveTracking = FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING;
		gForceSampleBytes = static_cast<std::size_t>(FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES);
	}
	if (frames < 1)
		frames = 1;
	if (frames > MEMORY_TRACKER_MAX_FRAMES)
		frames = MEMORY_TRACKER_MAX_FRAMES;

	// Reseed the counter for the un-sampled fast path.
	if (inverse <= 0) {
		gMemTrackerCounter = INT_MAX;
		// Sampling is off — but we were called because the caller forced
		// the slow path (counter underflowed from initial 1, or n was big
		// before gForceSampleBytes had been refreshed). Just bail.
		return;
	}
	if (inverse == 1) {
		// Sample every allocation — keep counter at 1 so the next decrement
		// drops it to 0 and re-enters the slow path. Bypass the random
		// reseed below, which would otherwise leave counter==2 half the
		// time and cause us to miss every other allocation.
		gMemTrackerCounter = 1;
	} else {
		std::uint32_t r = xorshift32(gMemTrackerSeed);
		gMemTrackerCounter = 1 + static_cast<int>(r % static_cast<std::uint32_t>(2 * inverse));
	}

	bool isForceSampled = (n >= gForceSampleBytes);

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
		// First insertion — initialize the exemplar frames. We use these for
		// the offline addr2line command in the dump.
		site.fingerprint = fp;
		site.exemplarFrameCount = static_cast<std::uint8_t>(kept);
		for (int i = 0; i < kept; i++)
			site.exemplarFrames[i] = keep[i];
	}
	site.cumulativeAllocs += 1;
	site.cumulativeBytes += static_cast<std::int64_t>(n);
	if (isForceSampled)
		site.forceSampledCount += 1;

	if (liveTracking) {
		site.liveBytes += static_cast<std::int64_t>(n);
		site.liveCount += 1;
		if (site.liveBytes > site.peakBytes)
			site.peakBytes = site.liveBytes;
		(*g_liveMap)[reinterpret_cast<std::uintptr_t>(p)] =
		    LiveEntry{ fp, static_cast<std::uint32_t>(n), isForceSampled };
		g_liveBytesTotal += static_cast<std::int64_t>(n);
		g_liveBlocksTotal += 1;
	}
	g_cumulativeBytesTotal += static_cast<std::int64_t>(n);
	g_cumulativeAllocsTotal += 1;
	g_samplesEmitted += 1;
}

void memTrackerSampleFree(void* p) {
	bool liveTracking = FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING : true;
	if (!liveTracking)
		return; // we never recorded the alloc, nothing to undo

	ThreadSpinLockHolder lk(g_mtLock);
	if (!g_liveMap)
		return;
	auto it = g_liveMap->find(reinterpret_cast<std::uintptr_t>(p));
	if (it == g_liveMap->end())
		return; // un-tracked pointer, no-op

	LiveEntry e = it->second;
	g_liveMap->erase(it);

	if (g_aggMap) {
		auto sit = g_aggMap->find(e.fingerprint);
		if (sit != g_aggMap->end()) {
			sit->second.liveBytes -= static_cast<std::int64_t>(e.size);
			sit->second.liveCount -= 1;
		}
	}
	g_liveBytesTotal -= static_cast<std::int64_t>(e.size);
	g_liveBlocksTotal -= 1;
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
			for (auto& kv : *g_aggMap)
				snapshot.push_back(kv.second);
		}
	}
	for (auto& s : snapshot)
		cb(s);
}

void memTrackerResetForTest() {
	gInMemTracker = true;
	{
		ThreadSpinLockHolder lk(g_mtLock);
		if (g_aggMap)
			g_aggMap->clear();
		if (g_liveMap)
			g_liveMap->clear();
		g_liveBytesTotal = 0;
		g_liveBlocksTotal = 0;
		g_cumulativeBytesTotal = 0;
		g_cumulativeAllocsTotal = 0;
		g_samplesEmitted = 0;
	}
	// Force the next allocation on this thread to take the slow path so it
	// re-reads the (possibly just-changed) sample-inverse knob. Without this,
	// a prior off-switch run that stored INT_MAX into the counter would keep
	// the fast path skipping samples for the rest of the run.
	gMemTrackerCounter = 1;
	gForceSampleBytes = static_cast<std::size_t>(-1);
	gInMemTracker = false;
}

void memTrackerDump(int64_t bytesThreshold) {
	gInMemTracker = true;

	std::vector<MemoryTrackerCallSite> sites;
	int aggSize = 0;
	int liveSize = 0;
	std::int64_t liveBytesTotalSnap = 0;
	std::int64_t liveBlocksTotalSnap = 0;
	std::int64_t cumBytesSnap = 0;
	std::int64_t cumAllocsSnap = 0;
	std::int64_t samplesEmittedSnap = 0;
	{
		ThreadSpinLockHolder lk(g_mtLock);
		if (g_aggMap) {
			sites.reserve(g_aggMap->size());
			for (auto& kv : *g_aggMap)
				sites.push_back(kv.second);
			aggSize = static_cast<int>(g_aggMap->size());
		}
		liveSize = g_liveMap ? static_cast<int>(g_liveMap->size()) : 0;
		liveBytesTotalSnap = g_liveBytesTotal;
		liveBlocksTotalSnap = g_liveBlocksTotal;
		cumBytesSnap = g_cumulativeBytesTotal;
		cumAllocsSnap = g_cumulativeAllocsTotal;
		samplesEmittedSnap = g_samplesEmitted;
	}

	bool liveTracking = FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_LIVE_TRACKING : true;
	// std::sort is unstable and unordered_map iteration is bucket-order, so
	// MemoryTrackerSite events for sites with tied byte values may appear in
	// different orders across same-seed sim2 runs. The R6 determinism
	// requirement is on aggregate counts, not event ordering — those are
	// unaffected — so we don't pay for stable_sort here.
	auto byLive = [](const MemoryTrackerCallSite& a, const MemoryTrackerCallSite& b) {
		return a.liveBytes > b.liveBytes;
	};
	auto byCum = [](const MemoryTrackerCallSite& a, const MemoryTrackerCallSite& b) {
		return a.cumulativeBytes > b.cumulativeBytes;
	};
	if (liveTracking) {
		std::sort(sites.begin(), sites.end(), byLive);
	} else {
		std::sort(sites.begin(), sites.end(), byCum);
	}

	// Filter: a site qualifies when its currently-live bytes (or cumulative
	// bytes in degraded mode) exceed the threshold. Sites are already sorted
	// descending, so we can stop at the first non-qualifier.
	std::vector<MemoryTrackerCallSite> qualifying;
	qualifying.reserve(sites.size());
	for (const auto& s : sites) {
		int64_t v = liveTracking ? s.liveBytes : s.cumulativeBytes;
		if (v < bytesThreshold)
			break;
		qualifying.push_back(s);
	}

	for (const auto& s : qualifying) {
		TraceEvent ev("MemoryTrackerSite");
		ev.detail("Fingerprint", format("%016llx", static_cast<unsigned long long>(s.fingerprint)));
		ev.detail("LiveBytes", s.liveBytes);
		ev.detail("LiveCount", s.liveCount);
		ev.detail("PeakBytes", s.peakBytes);
		ev.detail("CumulativeBytes", s.cumulativeBytes);
		ev.detail("CumulativeAllocs", s.cumulativeAllocs);
		ev.detail("ForceSampledCount", s.forceSampledCount);
		for (int f = 0; f < s.exemplarFrameCount; f++) {
			ev.detail(format("Frame%d", f).c_str(), format("%p", s.exemplarFrames[f]));
		}
	}

	// Single combined addr2line invocation covering every qualifying site's
	// frames in dump order, leaf-to-root within each site. Cut-and-paste
	// resolves all stacks at once; resolved lines map back to sites positionally
	// (FramesPerSite per site). Built directly so we keep frame 0 (the actual
	// allocation site) — platform::format_backtrace drops index 0 by design,
	// which is wrong for this use case.
	if (!qualifying.empty()) {
		platform::ImageInfo img = platform::getImageInfo();
#ifdef __clang__
		const char* addr2lineTool = "/usr/local/bin/llvm-addr2line";
#else
		const char* addr2lineTool = "/usr/bin/addr2line";
#endif
		std::string cmd = format("%s -e %s -p -C -f -i", addr2lineTool, img.symbolFileName.c_str());
		int totalFrames = 0;
		for (const auto& s : qualifying) {
			for (int i = 0; i < s.exemplarFrameCount; i++) {
				uintptr_t pieRelative =
				    reinterpret_cast<uintptr_t>(s.exemplarFrames[i]) - reinterpret_cast<uintptr_t>(img.offset);
				cmd += format(" 0x%lx", pieRelative);
				totalFrames++;
			}
		}
		TraceEvent("MemoryTrackerAddrCmd")
		    .detail("Sites", qualifying.size())
		    .detail("FramesPerSite", FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_FRAMES : 6)
		    .detail("TotalFrames", totalFrames)
		    .detail("AddrCmd", cmd);
	}

	TraceEvent("MemoryTrackerSummary")
	    .detail("SitesTracked", aggSize)
	    .detail("SitesReported", static_cast<int>(qualifying.size()))
	    .detail("LiveBlocks", liveSize)
	    .detail("LiveBytesTotal", liveBytesTotalSnap)
	    .detail("LiveBlocksTotal", liveBlocksTotalSnap)
	    .detail("CumulativeAllocs", cumAllocsSnap)
	    .detail("CumulativeBytes", cumBytesSnap)
	    .detail("SamplesEmitted", samplesEmittedSnap)
	    .detail("SampleInverse", FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_SAMPLE_INVERSE : 0)
	    .detail("ForceSampleBytes",
	            FLOW_KNOBS ? FLOW_KNOBS->MEMORY_TRACKING_FORCE_SAMPLE_BYTES : static_cast<std::int64_t>(-1))
	    .detail("ReportBytesThreshold", bytesThreshold);

	gInMemTracker = false;
}

// Global operator new/delete overrides. We override the standard set so every
// allocation flowing through global new is observable. Wrapped in
// !ALLOC_INSTRUMENTATION to avoid duplicate symbols against the legacy framework
// in fdbserver/fdbserver.cpp, which is gated on the same flag.
#if !defined(ALLOC_INSTRUMENTATION) && !defined(ALLOC_INSTRUMENTATION_STDOUT)

void* operator new(std::size_t n) {
	void* p = std::malloc(n);
	if (!p)
		throw std::bad_alloc();
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n) {
	void* p = std::malloc(n);
	if (!p)
		throw std::bad_alloc();
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
	void* p = std::malloc(n);
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p, const std::nothrow_t&) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
	void* p = std::malloc(n);
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

// C++17 over-aligned new/delete.
void* operator new(std::size_t n, std::align_val_t a) {
	void* p = nullptr;
	if (posix_memalign(&p, static_cast<std::size_t>(a), n) != 0)
		throw std::bad_alloc();
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n, std::align_val_t a) {
	void* p = nullptr;
	if (posix_memalign(&p, static_cast<std::size_t>(a), n) != 0)
		throw std::bad_alloc();
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

#endif // !ALLOC_INSTRUMENTATION
