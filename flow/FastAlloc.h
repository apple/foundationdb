/*
 * FastAlloc.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

// ALLOC_INSTRUMENTATION_STDOUT enables non-sampled logging of all allocations and deallocations to stdout to be processed by tools/alloc_instrumentation.py
//#define ALLOC_INSTRUMENTATION_STDOUT ENABLED(NOT_IN_CLEAN)

//#define ALLOC_INSTRUMENTATION ENABLED(NOT_IN_CLEAN)
// The form "(1==1)" in this context is used to satisfy both clang and vc++ with a single syntax.  Clang rejects "1" and vc++ rejects "true".
#define FASTALLOC_THREAD_SAFE (FLOW_THREAD_SAFE || (1==1))

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
	inline void alloc(int64_t count=1) {
		allocCount += count;
		maxAllocated = std::max(allocCount-deallocCount,maxAllocated);
	}
	inline void dealloc(int64_t count=1) {
		deallocCount += count;
	}
};
extern std::map<const char*, AllocInstrInfo> allocInstr;
#define INSTRUMENT_ALLOCATE(name) (allocInstr[(name)].alloc())
#define INSTRUMENT_RELEASE(name) (allocInstr[(name)].dealloc())

//extern std::map<uint32_t, uint64_t> stackAllocations;

// maps from an address to the hash of the backtrace and the size of the alloction
extern std::unordered_map<int64_t, std::pair<uint32_t, size_t>> memSample;

struct BackTraceAccount {
	double count;
	size_t sampleCount;
	size_t totalSize;
	std::vector<void*> *backTrace;
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
void recordAllocation( void *ptr, size_t size );
void recordDeallocation( void *ptr );
#endif

template <int Size>
class FastAllocator {
public:
	[[nodiscard]] static void* allocate();
	static void release(void* ptr);
	static void check( void* ptr, bool alloc );

	static long long getTotalMemory();
	static long long getApproximateMemoryUnused();
	static long long getActiveThreads();

	static void releaseThreadMagazines();

#ifdef ALLOC_INSTRUMENTATION
	static volatile int32_t pageCount;
#endif

	FastAllocator()=delete;
private:
#ifdef VALGRIND
	static unsigned long vLock;
#endif

	static const int magazine_size = (128<<10) / Size;
	static const int PSize = Size / sizeof(void*);
	struct GlobalData;
	struct ThreadData {
		void* freelist;
		int count;		  // there are count items on freelist
		void* alternate;  // alternate is either a full magazine, or an empty one
	};
	static thread_local ThreadData threadData;
	static thread_local bool threadInitialized;
	static GlobalData* globalData() noexcept {
#ifdef VALGRIND
		ANNOTATE_RWLOCK_ACQUIRED(vLock, 1);
#endif
		static GlobalData *data = new GlobalData(); // This is thread-safe as of c++11 (VS 2015, gcc 4.8, clang 3.3)

#ifdef VALGRIND
		ANNOTATE_RWLOCK_RELEASED(vLock, 1);
#endif

		return data;
	}
	static void* freelist;

	static void initThread();
	static void getMagazine();
	static void releaseMagazine(void*);
};

extern std::atomic<int64_t> g_hugeArenaMemory;
void hugeArenaSample(int size);
void releaseAllThreadMagazines();
int64_t getTotalUnusedAllocatedMemory();
void setFastAllocatorThreadInitFunction( void (*)() );  // The given function will be called at least once in each thread that allocates from a FastAllocator.  Currently just one such function is tracked.

inline constexpr int nextFastAllocatedSize(int x) {
	// Based on http://jemalloc.net/jemalloc.3.html#size_classes
	assert(0 < x && x <= 8192);
	if (x <= 8) return 8;
	if (x <= 16) return 16;
	if (x <= 32) return 32;
	if (x <= 48) return 48;
	if (x <= 64) return 64;
	if (x <= 80) return 80;
	if (x <= 96) return 96;
	if (x <= 112) return 112;
	if (x <= 128) return 128;
	if (x <= 160) return 160;
	if (x <= 192) return 192;
	if (x <= 224) return 224;
	if (x <= 256) return 256;
	if (x <= 320) return 320;
	if (x <= 384) return 384;
	if (x <= 448) return 448;
	if (x <= 512) return 512;
	if (x <= 640) return 640;
	if (x <= 768) return 768;
	if (x <= 896) return 896;
	if (x <= 1024) return 1024;
	if (x <= 1280) return 1280;
	if (x <= 1536) return 1536;
	if (x <= 1792) return 1792;
	if (x <= 2048) return 2048;
	if (x <= 2560) return 2560;
	if (x <= 3072) return 3072;
	if (x <= 3584) return 3584;
	if (x <= 4096) return 4096;
	if (x <= 5 * 1024) return 5 * 1024;
	if (x <= 6 * 1024) return 6 * 1024;
	if (x <= 7 * 1024) return 7 * 1024;
	if (x <= 8 * 1024) return 8 * 1024;
	return x;
}

// allocate, free, and sized free

// Obtain a heap allocation of |size| bytes. |size| must be > 0
[[nodiscard]] void* allocateFast(int size) noexcept;
// |size| must be as passed to allocateFast to obtain |ptr|. |ptr| must not be null.
void freeFast(int size, void* ptr) noexcept;
// |ptr| must have been returned by allocateFast. Prefer the overload accepting |size| when possible. |ptr| must not be
// null.
void freeFast(void* ptr) noexcept;

// aligned allocation

// |align| must be a power of 2, |size| must be a multiple of align
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept;
// |size| and |align| must be as passed to aligneAllocateFast to obtain |ptr|. |ptr| must not be null.
void alignedFreeFast(void* ptr, int align, int size) noexcept;

template <class Object>
class FastAllocated {
public:
	[[nodiscard]] static void* operator new(size_t s) {
		ASSERT(s == sizeof(Object));
		INSTRUMENT_ALLOCATE(typeid(Object).name());
		return alignedAllocateFast(alignof(Object), sizeof(Object));
	}

	static void operator delete(void* s) {
		INSTRUMENT_RELEASE(typeid(Object).name());
		return alignedFreeFast(s, alignof(Object), sizeof(Object));
	}
	// Redefine placement new so you can still use it
	static void* operator new(size_t, void* p) { return p; }
	static void operator delete(void*, void*) {}
};

#endif
