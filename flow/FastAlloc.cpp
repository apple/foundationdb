/*
 * FastAlloc.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/FastAlloc.h"

#include "flow/ThreadPrimitives.h"
#include "flow/Trace.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/UnitTest.h"
#include "crc32/crc32c.h"
#include "flow/flow.h"

#include <atomic>
#include <cstdint>
#include <unordered_map>

#ifdef WIN32
#include <windows.h>
#undef min
#undef max
#endif

#ifdef __linux__
#include <sys/mman.h>
#include <linux/mman.h>
#endif

#ifdef __FreeBSD__
#include <sys/mman.h>
#endif

#define FAST_ALLOCATOR_DEBUG 0

#ifdef _MSC_VER
// warning 4073 warns about "initializers put in library initialization area", which is our intent
#pragma warning(disable : 4073)
#pragma init_seg(lib)
#define INIT_SEG
#elif defined(__INTEL_COMPILER)
// intel compiler ignored INIT_SEG for thread local variables
#define INIT_SEG
#elif defined(__GNUG__)
#ifdef __linux__
#define INIT_SEG __attribute__((init_priority(1000)))
#elif defined(__FreeBSD__)
#define INIT_SEG __attribute__((init_priority(1000)))
#elif defined(__APPLE__)
#pragma message "init_priority is not supported on this platform; will this be a problem?"
#define INIT_SEG
#else
#error Where am I?
#endif
#else
#error Port me? (init_seg(lib))
#endif

template <int Size>
INIT_SEG thread_local typename FastAllocator<Size>::ThreadDataInit FastAllocator<Size>::threadDataInit;

template <int Size>
typename FastAllocator<Size>::ThreadData& FastAllocator<Size>::threadData() noexcept {
	static thread_local ThreadData threadData;
	return threadData;
}

#ifdef VALGRIND
template <int Size>
unsigned long FastAllocator<Size>::vLock = 1;

// valgrindPrecise controls some extra instrumentation that causes valgrind to run more slowly but give better
// diagnostics. Set the environment variable FDB_VALGRIND_PRECISE to enable. valgrindPrecise must never change the
// behavior of the program itself, so when you find a memory error in simulation without valgrindPrecise enabled, you
// can rerun it with FDB_VALGRIND_PRECISE set, make yourself a coffee, and come back to a nicer diagnostic (you probably
// want to pass --track-origins=yes to valgrind as well!)
//
// Currently valgrindPrecise replaces FastAllocator::allocate with malloc, and FastAllocator::release with free.
// This improves diagnostics for fast-allocated memory. The main thing it improves is the case where you free a buffer
// and then allocate a buffer again - with FastAllocator you'll get the same buffer back, and so uses of the freed
// pointer either won't be noticed or will be counted as use of uninitialized memory instead of use after free.
//
// valgrindPrecise also enables extra instrumentation for Arenas, so you can
// catch things like buffer overflows in arena-allocated memory more easily
// (valgrind otherwise wouldn't know that memory used for Arena bookkeeping
// should only be accessed within certain Arena routines.) Unfortunately the
// current Arena contract requires some allocations to be adjacent, so we can't
// insert redzones between arena allocations, but we can at least catch buffer
// overflows if it's the most recently allocated memory from an Arena.
bool valgrindPrecise() {
	static bool result = std::getenv("FDB_VALGRIND_PRECISE");
	return result;
}
#endif

template <int Size>
void* FastAllocator<Size>::freelist = nullptr;

std::atomic<int64_t> g_hugeArenaMemory(0);

double hugeArenaLastLogged = 0;
std::map<std::string, std::pair<int, int64_t>> hugeArenaTraces;

void hugeArenaSample(int size) {
	if (TraceEvent::isNetworkThread()) {
		auto& info = hugeArenaTraces[platform::get_backtrace()];
		info.first++;
		info.second += size;
		if (now() - hugeArenaLastLogged > FLOW_KNOBS->HUGE_ARENA_LOGGING_INTERVAL) {
			for (auto& it : hugeArenaTraces) {
				TraceEvent("HugeArenaSample")
				    .detail("Count", it.second.first)
				    .detail("Size", it.second.second)
				    .detail("Backtrace", it.first);
			}
			hugeArenaLastLogged = now();
			hugeArenaTraces.clear();
		}
	}
}

#ifdef ALLOC_INSTRUMENTATION
INIT_SEG std::map<const char*, AllocInstrInfo> allocInstr;
INIT_SEG std::unordered_map<int64_t, std::pair<uint32_t, size_t>> memSample;
INIT_SEG std::unordered_map<uint32_t, BackTraceAccount> backTraceLookup;
INIT_SEG ThreadSpinLock memLock;
const size_t SAMPLE_BYTES = 1e7;
template <int Size>
volatile int32_t FastAllocator<Size>::pageCount;
thread_local bool memSample_entered = false;
#endif

#ifdef ALLOC_INSTRUMENTATION_STDOUT
thread_local bool inRecordAllocation = false;
#endif

void recordAllocation(void* ptr, size_t size) {
#ifdef ALLOC_INSTRUMENTATION_STDOUT
	if (inRecordAllocation)
		return;
	inRecordAllocation = true;
	std::string trace = platform::get_backtrace();
	printf("Alloc\t%p\t%d\t%s\n", ptr, size, trace.c_str());
	inRecordAllocation = false;
#endif
#ifdef ALLOC_INSTRUMENTATION
	if (memSample_entered)
		return;
	memSample_entered = true;

	if (((double)rand()) / RAND_MAX < ((double)size) / SAMPLE_BYTES) {
		void* buffer[100];
#if defined(__linux__)
		int nptrs = backtrace(buffer, 100);
#elif defined(_WIN32)
		// We could be using fourth parameter to get a hash, but we'll do this
		//  in a unified way between platforms
		int nptrs = CaptureStackBackTrace(1, 100, buffer, nullptr);
#else
#error Instrumentation not supported on this platform
#endif

		uint32_t a = 0;
		if (nptrs > 0) {
			a = crc32c_append(0xfdbeefdb, reinterpret_cast<uint8_t*>(buffer), nptrs * sizeof(void*));
		}

		double countDelta = std::max(1.0, ((double)SAMPLE_BYTES) / size);
		size_t sizeDelta = std::max(SAMPLE_BYTES, size);
		ThreadSpinLockHolder holder(memLock);
		auto it = backTraceLookup.find(a);
		if (it == backTraceLookup.end()) {
			auto& bt = backTraceLookup[a];
			bt.backTrace = new std::vector<void*>();
			for (int j = 0; j < nptrs; j++) {
				bt.backTrace->push_back(buffer[j]);
			}
			bt.totalSize = sizeDelta;
			bt.count = countDelta;
			bt.sampleCount = 1;
		} else {
			it->second.totalSize += sizeDelta;
			it->second.count += countDelta;
			it->second.sampleCount++;
		}
		memSample[(int64_t)ptr] = std::make_pair(a, size);
	}
	memSample_entered = false;
#endif
}

void recordDeallocation(void* ptr) {
#ifdef ALLOC_INSTRUMENTATION_STDOUT
	if (inRecordAllocation)
		return;
	printf("Dealloc\t%p\n", ptr);
	inRecordAllocation = false;
#endif
#ifdef ALLOC_INSTRUMENTATION
	if (memSample_entered) // could this lead to deallocations not being recorded?
		return;
	memSample_entered = true;
	{
		ThreadSpinLockHolder holder(memLock);

		auto it = memSample.find((int64_t)ptr);
		if (it == memSample.end()) {
			memSample_entered = false;
			return;
		}
		auto bti = backTraceLookup.find(it->second.first);
		ASSERT(bti != backTraceLookup.end());

		size_t sizeDelta = std::max(SAMPLE_BYTES, it->second.second);
		double countDelta = std::max(1.0, ((double)SAMPLE_BYTES) / it->second.second);

		bti->second.totalSize -= sizeDelta;
		bti->second.count -= countDelta;
		bti->second.sampleCount--;
		memSample.erase(it);
	}
	memSample_entered = false;
#endif
}

template <int Size>
struct FastAllocator<Size>::GlobalData {
	CRITICAL_SECTION mutex;
	std::vector<void*> magazines; // These magazines are always exactly magazine_size ("full")
	std::vector<std::pair<int, void*>>
	    partial_magazines; // Magazines that are not "full" and their counts.  Only created by releaseThreadMagazines().
	std::atomic<long long> totalMemory;
	long long partialMagazineUnallocatedMemory;
	std::atomic<long long> activeThreads;
	GlobalData() : totalMemory(0), partialMagazineUnallocatedMemory(0), activeThreads(0) {
		InitializeCriticalSection(&mutex);
	}
};

template <int Size>
long long FastAllocator<Size>::getTotalMemory() {
	return globalData()->totalMemory.load();
}

// This does not include memory held by various threads that's available for allocation
template <int Size>
long long FastAllocator<Size>::getApproximateMemoryUnused() {
	EnterCriticalSection(&globalData()->mutex);
	long long unused =
	    globalData()->magazines.size() * magazine_size * Size + globalData()->partialMagazineUnallocatedMemory;
	LeaveCriticalSection(&globalData()->mutex);
	return unused;
}

template <int Size>
long long FastAllocator<Size>::getActiveThreads() {
	return globalData()->activeThreads.load();
}

#if FAST_ALLOCATOR_DEBUG
static int64_t getSizeCode(int i) {
	switch (i) {
	case 16:
		return 1;
	case 32:
		return 2;
	case 64:
		return 3;
	case 96:
		return 4;
	case 128:
		return 5;
	case 256:
		return 6;
	case 512:
		return 7;
	case 1024:
		return 8;
	case 2048:
		return 9;
	case 4096:
		return 10;
	case 8192:
		return 11;
	default:
		return 12;
	}
}
#endif

namespace keepalive_allocator {

namespace detail {

std::set<void*> g_allocatedSet;
std::set<void*> g_freedSet;
std::vector<std::pair<const uint8_t*, int>> g_wipedSet;
bool g_active = false;

} // namespace detail

ActiveScope::ActiveScope() {
	// no nested scopes allowed
	ASSERT(!detail::g_active);
	ASSERT(detail::g_allocatedSet.empty());
	ASSERT(detail::g_freedSet.empty());
	ASSERT(detail::g_wipedSet.empty());
	detail::g_active = true;
	// As of writing, TraceEvent uses eventname-based throttling keyed by Standalone<StringRef>,
	// which uses Arena and stays allocated after scope.
	// Therefore, we disable allocation tracing (e.g. hugeArenaSample()) while this scope is active.
	g_allocation_tracing_disabled++;
}

ActiveScope::~ActiveScope() {
	ASSERT_ABORT(detail::g_active);
	ASSERT_ABORT(detail::g_allocatedSet == detail::g_freedSet);
	g_allocation_tracing_disabled--;
	for (auto memory : detail::g_allocatedSet) {
		delete[] static_cast<uint8_t*>(memory);
	}
	detail::g_allocatedSet.clear();
	detail::g_freedSet.clear();
	detail::g_wipedSet.clear();
	detail::g_active = false;
}

void* allocate(size_t size) {
	ASSERT_ABORT(detail::g_active);
	auto ptr = new uint8_t[size];
	auto [_, inserted] = detail::g_allocatedSet.insert(ptr);
	ASSERT_ABORT(inserted); // no duplicates
	return ptr;
}

void invalidate(void* ptr) {
	ASSERT_ABORT(detail::g_active);
	ASSERT_ABORT(detail::g_allocatedSet.contains(ptr));
	ASSERT_ABORT(!detail::g_freedSet.contains(ptr));
	detail::g_freedSet.insert(ptr);
}

void trackWipedArea(const uint8_t* begin, int size) {
	ASSERT_ABORT(detail::g_active);
	detail::g_wipedSet.emplace_back(begin, size);
}

std::vector<std::pair<const uint8_t*, int>> const& getWipedAreaSet() {
	ASSERT_ABORT(detail::g_active);
	return detail::g_wipedSet;
}

} // namespace keepalive_allocator

template <int Size>
void* FastAllocator<Size>::allocate() {
	if (keepalive_allocator::isActive()) [[unlikely]]
		return keepalive_allocator::allocate(Size);

#if defined(USE_GPERFTOOLS) || defined(ADDRESS_SANITIZER)
	// Some usages of FastAllocator require 4096 byte alignment.
	return aligned_alloc(Size >= 4096 ? 4096 : alignof(void*), Size);
#endif

#if VALGRIND
	if (valgrindPrecise()) {
		// Some usages of FastAllocator require 4096 byte alignment
		return aligned_alloc(Size >= 4096 ? 4096 : alignof(void*), Size);
	}
#endif

#if FASTALLOC_THREAD_SAFE
	ThreadData& thr = threadData();
	if (!thr.freelist) {
		ASSERT(thr.count == 0);
		if (thr.alternate) {
			thr.freelist = thr.alternate;
			thr.alternate = nullptr;
			thr.count = magazine_size;
		} else {
			getMagazine();
		}
	}
	--thr.count;
	void* p = thr.freelist;
#if VALGRIND
	VALGRIND_MAKE_MEM_DEFINED(p, sizeof(void*));
#endif
	thr.freelist = *(void**)p;
	ASSERT(!thr.freelist == (thr.count == 0)); // freelist is empty if and only if count is 0
	// check( p, true );
#else
	void* p = freelist;
	if (!p)
		getMagazine();
#if VALGRIND
	VALGRIND_MAKE_MEM_DEFINED(p, sizeof(void*));
#endif
	freelist = *(void**)p;
#endif
#if VALGRIND
	VALGRIND_MALLOCLIKE_BLOCK(p, Size, 0, 0);
#endif
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
	recordAllocation(p, Size);
#endif
	return p;
}

template <int Size>
void FastAllocator<Size>::release(void* ptr) {
	if (keepalive_allocator::isActive()) [[unlikely]]
		return keepalive_allocator::invalidate(ptr);

#if defined(USE_GPERFTOOLS) || defined(ADDRESS_SANITIZER)
	return aligned_free(ptr);
#endif

#if VALGRIND
	if (valgrindPrecise()) {
		return aligned_free(ptr);
	}
#endif

#if FASTALLOC_THREAD_SAFE
	ThreadData& thr = threadData();
	if (thr.count == magazine_size) {
		if (thr.alternate) // Two full magazines, return one
			releaseMagazine(thr.alternate);
		thr.alternate = thr.freelist;
		thr.freelist = nullptr;
		thr.count = 0;
	}

	ASSERT(!thr.freelist == (thr.count == 0)); // freelist is empty if and only if count is 0

#if VALGRIND
	VALGRIND_MAKE_MEM_DEFINED(ptr, sizeof(void*));
#endif
	++thr.count;
	*(void**)ptr = thr.freelist;
	// check(ptr, false);
	thr.freelist = ptr;
#else
	*(void**)ptr = freelist;
	freelist = ptr;
#endif

#if VALGRIND
	VALGRIND_FREELIKE_BLOCK(ptr, 0);
#endif
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
	recordDeallocation(ptr);
#endif
}

template <int Size>
void FastAllocator<Size>::check(void* ptr, bool alloc) {
#if FAST_ALLOCATOR_DEBUG
	// if (ptr == (void*)0x400200180)
	//	printf("%c%p\n", alloc?'+':'-', ptr);

	// Check for pointers that aren't part of this FastAllocator
	if (ptr < (void*)(((getSizeCode(Size) << 11) + 0) * magazine_size * Size) ||
	    ptr > (void*)(((getSizeCode(Size) << 11) + 4000) * magazine_size * Size) || (int64_t(ptr) & (Size - 1))) {
		printf("Bad ptr: %p\n", ptr);
		abort();
	}

	// Redundant freelist pointers to detect outright smashing of the freelist
	if (alloc) {
		if (*((void**)ptr + 1) != *(void**)ptr) {
			printf("Freelist corruption? %p %p\n", *(void**)ptr, *((void**)ptr + 1));
			abort();
		}
		*((void**)ptr + 1) = (void*)0;
	} else {
		*((void**)ptr + 1) = *(void**)ptr;
	}

	// Track allocated/free status in a completely separate data structure to detect double frees
	int i = (int)((int64_t)ptr - ((getSizeCode(Size) << 11) + 0) * magazine_size * Size) / Size;
	static std::vector<bool> isFreed;
	if (!alloc) {
		if (i + 1 > isFreed.size())
			isFreed.resize(i + 1, false);
		if (isFreed[i]) {
			printf("Double free: %p\n", ptr);
			abort();
		}
		isFreed[i] = true;
	} else {
		if (i + 1 > isFreed.size()) {
			printf("Allocate beyond end: %p\n", ptr);
			abort();
		}
		if (!isFreed[i]) {
			printf("Allocate non-freed: %p\n", ptr);
			abort();
		}
		isFreed[i] = false;
	}
#endif
}

template <int Size>
FastAllocator<Size>::ThreadData::ThreadData() {
	globalData()->activeThreads.fetch_add(1);
	freelist = nullptr;
	alternate = nullptr;
	count = 0;
}

template <int Size>
void FastAllocator<Size>::getMagazine() {
	ThreadData& thr = threadData();
	ASSERT(!thr.freelist && !thr.alternate && thr.count == 0);

	EnterCriticalSection(&globalData()->mutex);
	if (globalData()->magazines.size()) {
		void* m = globalData()->magazines.back();
		globalData()->magazines.pop_back();
		LeaveCriticalSection(&globalData()->mutex);
		thr.freelist = m;
		thr.count = magazine_size;
		return;
	} else if (globalData()->partial_magazines.size()) {
		std::pair<int, void*> p = globalData()->partial_magazines.back();
		globalData()->partial_magazines.pop_back();
		globalData()->partialMagazineUnallocatedMemory -= p.first * Size;
		LeaveCriticalSection(&globalData()->mutex);
		thr.freelist = p.second;
		thr.count = p.first;
		return;
	}
	globalData()->totalMemory.fetch_add(magazine_size * Size);
	LeaveCriticalSection(&globalData()->mutex);

// Allocate a new page of data from the system allocator
#ifdef ALLOC_INSTRUMENTATION
	interlockedIncrement(&pageCount);
#endif

	void** block = nullptr;
#if FAST_ALLOCATOR_DEBUG
#ifdef WIN32
	static int alt = 0;
	alt++;
	block = (void**)VirtualAllocEx(GetCurrentProcess(),
	                               (void*)(((getSizeCode(Size) << 11) + alt) * magazine_size * Size),
	                               magazine_size * Size,
	                               MEM_COMMIT | MEM_RESERVE,
	                               PAGE_READWRITE);
#else
	static int alt = 0;
	alt++;
	void* desiredBlock = (void*)(((getSizeCode(Size) << 11) + alt) * magazine_size * Size);
	block =
	    (void**)mmap(desiredBlock, magazine_size * Size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	ASSERT(block == desiredBlock);
#endif
#else
	// FIXME: We should be able to allocate larger magazine sizes here if we
	// detect that the underlying system supports hugepages.  Using hugepages
	// with smaller-than-2MiB magazine sizes strands memory.  See issue #909.
#if !DEBUG_DETERMINISM
	if (FLOW_KNOBS && g_allocation_tracing_disabled == 0 &&
	    nondeterministicRandom()->random01() < (magazine_size * Size) / FLOW_KNOBS->FAST_ALLOC_LOGGING_BYTES) {
		++g_allocation_tracing_disabled;
		TraceEvent("GetMagazineSample").detail("Size", Size).backtrace();
		--g_allocation_tracing_disabled;
	}
#endif
#ifdef VALGRIND
	const bool includeGuardPages = false;
#else
	const bool includeGuardPages = true;
#endif
	block = (void**)::allocate(magazine_size * Size, /*allowLargePages*/ false, includeGuardPages);
#endif

	// void** block = new void*[ magazine_size * PSize ];
	for (int i = 0; i < magazine_size - 1; i++) {
		block[i * PSize + 1] = block[i * PSize] = &block[(i + 1) * PSize];
		check(&block[i * PSize], false);
	}

	block[(magazine_size - 1) * PSize + 1] = block[(magazine_size - 1) * PSize] = nullptr;
	check(&block[(magazine_size - 1) * PSize], false);
	thr.freelist = block;
	thr.count = magazine_size;
}
template <int Size>
void FastAllocator<Size>::releaseMagazine(void* mag) {
	EnterCriticalSection(&globalData()->mutex);
	globalData()->magazines.push_back(mag);
	LeaveCriticalSection(&globalData()->mutex);
}
template <int Size>
FastAllocator<Size>::ThreadData::~ThreadData() {
	EnterCriticalSection(&globalData()->mutex);
	if (freelist) {
		ASSERT_ABORT(count > 0 && count <= magazine_size);
		globalData()->partial_magazines.emplace_back(count, freelist);
		globalData()->partialMagazineUnallocatedMemory += count * Size;
	}
	if (alternate) {
		globalData()->magazines.push_back(alternate);
	}
	globalData()->activeThreads.fetch_add(-1);
	LeaveCriticalSection(&globalData()->mutex);

	count = 0;
	alternate = nullptr;
	freelist = nullptr;
}

int64_t getTotalUnusedAllocatedMemory() {
	int64_t unusedMemory = 0;

	unusedMemory += FastAllocator<16>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<32>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<64>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<96>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<128>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<256>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<512>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<1024>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<2048>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<4096>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<8192>::getApproximateMemoryUnused();
	unusedMemory += FastAllocator<16384>::getApproximateMemoryUnused();

	return unusedMemory;
}

template class FastAllocator<16>;
template class FastAllocator<32>;
template class FastAllocator<64>;
template class FastAllocator<96>;
template class FastAllocator<128>;
template class FastAllocator<256>;
template class FastAllocator<512>;
template class FastAllocator<1024>;
template class FastAllocator<2048>;
template class FastAllocator<4096>;
template class FastAllocator<8192>;
template class FastAllocator<16384>;

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
TEST_CASE("/jemalloc/4k_aligned_usable_size") {
	void* ptr;
	try {
		// Check that we can allocate 4k aligned up to 16k with no internal
		// fragmentation
		for (int i = 1; i < 4; ++i) {
			ptr = aligned_alloc(4096, i * 4096);
			ASSERT_EQ(malloc_usable_size(ptr), i * 4096);
			aligned_free(ptr);
			ptr = nullptr;
		}
		// Also check that we can allocate magazines with no internal
		// fragmentation, should we decide to do that.
		ptr = aligned_alloc(4096, kFastAllocMagazineBytes);
		ASSERT_EQ(malloc_usable_size(ptr), kFastAllocMagazineBytes);
		aligned_free(ptr);
		ptr = nullptr;
	} catch (...) {
		aligned_free(ptr);
		throw;
	}
	return Void();
}
#endif
