/*
 * FastAlloc.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_FASTALLOC_H
#define FLOW_FASTALLOC_H
#pragma once

#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/config.h"

// ALLOC_INSTRUMENTATION_STDOUT enables non-sampled logging of all allocations and deallocations to stdout to be
// processed by tools/alloc_instrumentation.py
//#define ALLOC_INSTRUMENTATION_STDOUT ENABLED(NOT_IN_CLEAN)

//#define ALLOC_INSTRUMENTATION ENABLED(NOT_IN_CLEAN)
// The form "(1==1)" in this context is used to satisfy both clang and vc++ with a single syntax.  Clang rejects "1" and
// vc++ rejects "true".
#define FASTALLOC_THREAD_SAFE (FLOW_THREAD_SAFE || (1 == 1))

#if VALGRIND
#include <drd.h>
#include <memcheck.h>
bool valgrindPrecise();
#endif

#include "flow/Hash3.h"

#include <assert.h>
#include <atomic>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <unordered_map>

#if defined(ALLOC_INSTRUMENTATION) && defined(__linux__)
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#endif

#ifdef ALLOC_INSTRUMENTATION
#include <map>
#include <algorithm>
#include "flow/ThreadPrimitives.h"
struct AllocInstrInfo {
	int64_t allocCount;
	int64_t deallocCount;
	int64_t maxAllocated;
	inline void alloc(int64_t count = 1) {
		allocCount += count;
		maxAllocated = std::max(allocCount - deallocCount, maxAllocated);
	}
	inline void dealloc(int64_t count = 1) { deallocCount += count; }
};
extern std::map<const char*, AllocInstrInfo> allocInstr;
#define INSTRUMENT_ALLOCATE(name) (allocInstr[(name)].alloc())
#define INSTRUMENT_RELEASE(name) (allocInstr[(name)].dealloc())

// extern std::map<uint32_t, uint64_t> stackAllocations;

// maps from an address to the hash of the backtrace and the size of the alloction
extern std::unordered_map<int64_t, std::pair<uint32_t, size_t>> memSample;

struct BackTraceAccount {
	double count;
	size_t sampleCount;
	size_t totalSize;
	std::vector<void*>* backTrace;
};
// maps from a hash of a backtrace to a backtrace and the total size of data currently allocated from this stack
extern std::unordered_map<uint32_t, BackTraceAccount> backTraceLookup;

extern ThreadSpinLock memLock;
extern thread_local bool memSample_entered;
extern const size_t SAMPLE_BYTES;

#else
#define INSTRUMENT_ALLOCATE(name)
#define INSTRUMENT_RELEASE(name)
#endif

#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
void recordAllocation(void* ptr, size_t size);
void recordDeallocation(void* ptr);
#endif

inline constexpr auto kFastAllocMagazineBytes = 128 << 10;

template <int Size>
class FastAllocator {
public:
	[[nodiscard]] static void* allocate();
	static void release(void* ptr);
	static void check(void* ptr, bool alloc);

	static long long getTotalMemory();
	static long long getApproximateMemoryUnused();
	static long long getActiveThreads();

#ifdef ALLOC_INSTRUMENTATION
	static volatile int32_t pageCount;
#endif

	FastAllocator() = delete;

private:
#ifdef VALGRIND
	static unsigned long vLock;
#endif

	static const int magazine_size = kFastAllocMagazineBytes / Size;
	static const int PSize = Size / sizeof(void*);
	struct GlobalData;
	struct ThreadData {
		void* freelist;
		int count; // there are count items on freelist
		void* alternate; // alternate is either a full magazine, or an empty one
		ThreadData();
		~ThreadData();
	};
	struct ThreadDataInit {
		ThreadDataInit() { threadData(); }
	};
	// Used to try to initialize threadData as early as possible. It's still
	// possible that a static thread local variable (that owns fast-allocated
	// memory) could be constructed before threadData, in which case threadData
	// would be destroyed by the time that variable's destructor attempts to free.
	// This is undefined behavior if this happens, which is why we want to
	// initialize threadData as early as possible.
	static thread_local ThreadDataInit threadDataInit;
	// Used to access threadData. Returning a reference to a function-level
	// static guarantees that threadData will be constructed before it's
	// accessed here. Furthermore, if accessing threadData from a static thread
	// local variable's constructor, this guarantees that threadData will
	// outlive this object, since destruction order is the reverse of
	// construction order.
	static ThreadData& threadData() noexcept;
	static GlobalData* globalData() noexcept {
#ifdef VALGRIND
		ANNOTATE_RWLOCK_ACQUIRED(vLock, 1);
#endif
		static GlobalData* data = new GlobalData(); // This is thread-safe as of c++11 (VS 2015, gcc 4.8, clang 3.3)

#ifdef VALGRIND
		ANNOTATE_RWLOCK_RELEASED(vLock, 1);
#endif

		return data;
	}
	static void* freelist;

	static void getMagazine();
	static void releaseMagazine(void*);
};

extern std::atomic<int64_t> g_hugeArenaMemory;
void hugeArenaSample(int size);
void releaseAllThreadMagazines();
int64_t getTotalUnusedAllocatedMemory();

inline constexpr int nextFastAllocatedSize(int x) {
	assert(x > 0 && x <= 8192);
	if (x <= 16)
		return 16;
	else if (x <= 32)
		return 32;
	else if (x <= 64)
		return 64;
	else if (x <= 96)
		return 96;
	else if (x <= 128)
		return 128;
	else if (x <= 256)
		return 256;
	else if (x <= 512)
		return 512;
	else if (x <= 1024)
		return 1024;
	else if (x <= 2048)
		return 2048;
	else if (x <= 4096)
		return 4096;
	else
		return 8192;
}

template <class Object>
class FastAllocated {
public:
	[[nodiscard]] static void* operator new(size_t s) {
		if (s != sizeof(Object))
			abort();
		INSTRUMENT_ALLOCATE(typeid(Object).name());

		if constexpr (sizeof(Object) <= 256) {
			void* p = FastAllocator < sizeof(Object) <= 64 ? 64 : nextFastAllocatedSize(sizeof(Object)) > ::allocate();
			return p;
		} else {
			void* p = new uint8_t[nextFastAllocatedSize(sizeof(Object))];
			return p;
		}
	}

	static void operator delete(void* s) {
		INSTRUMENT_RELEASE(typeid(Object).name());

		if constexpr (sizeof(Object) <= 256) {
			FastAllocator<sizeof(Object) <= 64 ? 64 : nextFastAllocatedSize(sizeof(Object))>::release(s);
		} else {
			delete[] reinterpret_cast<uint8_t*>(s);
		}
	}
	// Redefine placement new so you can still use it
	static void* operator new(size_t, void* p) { return p; }
	static void operator delete(void*, void*) {}
};

[[nodiscard]] inline void* allocateFast(int size) {
	if (size <= 16)
		return FastAllocator<16>::allocate();
	if (size <= 32)
		return FastAllocator<32>::allocate();
	if (size <= 64)
		return FastAllocator<64>::allocate();
	if (size <= 96)
		return FastAllocator<96>::allocate();
	if (size <= 128)
		return FastAllocator<128>::allocate();
	if (size <= 256)
		return FastAllocator<256>::allocate();
	return new uint8_t[size];
}

inline void freeFast(int size, void* ptr) {
	if (size <= 16)
		return FastAllocator<16>::release(ptr);
	if (size <= 32)
		return FastAllocator<32>::release(ptr);
	if (size <= 64)
		return FastAllocator<64>::release(ptr);
	if (size <= 96)
		return FastAllocator<96>::release(ptr);
	if (size <= 128)
		return FastAllocator<128>::release(ptr);
	if (size <= 256)
		return FastAllocator<256>::release(ptr);
	delete[](uint8_t*) ptr;
}

// Allocate a block of memory aligned to 4096 bytes. Size must be a multiple of
// 4096. Guaranteed not to return null. Use freeFast4kAligned to free.
[[nodiscard]] inline void* allocateFast4kAligned(int size) {
#if !defined(USE_JEMALLOC)
	// Use FastAllocator for sizes it supports to avoid internal fragmentation in some implementations of aligned_alloc
	if (size <= 4096)
		return FastAllocator<4096>::allocate();
	if (size <= 8192)
		return FastAllocator<8192>::allocate();
	if (size <= 16384)
		return FastAllocator<16384>::allocate();
#endif
	auto* result = aligned_alloc(4096, size);
	if (result == nullptr) {
		platform::outOfMemory();
	}
	return result;
}

// Free a pointer returned from allocateFast4kAligned(size)
inline void freeFast4kAligned(int size, void* ptr) {
#if !defined(USE_JEMALLOC)
	// Sizes supported by FastAllocator must be release via FastAllocator
	if (size <= 4096)
		return FastAllocator<4096>::release(ptr);
	if (size <= 8192)
		return FastAllocator<8192>::release(ptr);
	if (size <= 16384)
		return FastAllocator<16384>::release(ptr);
#endif
	aligned_free(ptr);
}

#endif
