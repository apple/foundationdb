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

// ALLOC_INSTRUMENTATION_STDOUT enables non-sampled logging of all allocations and deallocations to stdout to be processed by scripts/alloc.pl
//#define ALLOC_INSTRUMENTATION_STDOUT ENABLED(NOT_IN_CLEAN)

//#define ALLOC_INSTRUMENTATION ENABLED(NOT_IN_CLEAN)
// The form "(1==1)" in this context is used to satisfy both clang and vc++ with a single syntax.  Clang rejects "1" and vc++ rejects "true".
#define FASTALLOC_THREAD_SAFE (FLOW_THREAD_SAFE || (1==1))

#if VALGRIND
#include <drd.h>
#include <memcheck.h>
#endif

#include "flow/Hash3.h"

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
	static void* allocate();
	static void release(void* ptr);
	static void check( void* ptr, bool alloc );

	static long long getTotalMemory();
	static long long getApproximateMemoryUnused();
	static long long getActiveThreads();

	static void releaseThreadMagazines();

#ifdef ALLOC_INSTRUMENTATION
	static volatile int32_t pageCount;
#endif

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
	static GlobalData* globalData() {
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

	FastAllocator();  // not implemented
	static void initThread();
	static void getMagazine();   
	static void releaseMagazine(void*);
};

void releaseAllThreadMagazines();
int64_t getTotalUnusedAllocatedMemory();
void setFastAllocatorThreadInitFunction( void (*)() );  // The given function will be called at least once in each thread that allocates from a FastAllocator.  Currently just one such function is tracked.

template<int X>
class NextPowerOfTwo {
	static const int A = X-1;
	static const int B = A | (A>>1);
	static const int C = B | (B>>2);
	static const int D = C | (C>>4);
	static const int E = D | (D>>8);
	static const int F = E | (E>>16);
public:
	static const int Result = F+1;
};

template <class Object>
class FastAllocated {
public:
	static void* operator new(size_t s) {
		if (s != sizeof(Object)) abort();
		INSTRUMENT_ALLOCATE(typeid(Object).name());
		void* p = FastAllocator<sizeof(Object)<=64 ? 64 : NextPowerOfTwo<sizeof(Object)>::Result>::allocate();		
		return p;
	}

	static void operator delete(void* s) {
		INSTRUMENT_RELEASE(typeid(Object).name());
		FastAllocator<sizeof(Object)<=64 ? 64 : NextPowerOfTwo<sizeof(Object)>::Result>::release(s);
	}
	// Redefine placement new so you can still use it
	static void* operator new( size_t, void* p ) { return p; }
	static void operator delete( void*, void* ) { }
};

static void* allocateFast(int size) {
	if (size <= 16) return FastAllocator<16>::allocate();
	if (size <= 32) return FastAllocator<32>::allocate();
	if (size <= 64) return FastAllocator<64>::allocate();
	if (size <= 128) return FastAllocator<128>::allocate();
	if (size <= 256) return FastAllocator<256>::allocate();
	if (size <= 512) return FastAllocator<512>::allocate();
	return new uint8_t[size];
}

static void freeFast(int size, void* ptr) {
	if (size <= 16) return FastAllocator<16>::release(ptr);
	if (size <= 32) return FastAllocator<32>::release(ptr);
	if (size <= 64) return FastAllocator<64>::release(ptr);
	if (size <= 128) return FastAllocator<128>::release(ptr);
	if (size <= 256) return FastAllocator<256>::release(ptr);
	if (size <= 512) return FastAllocator<512>::release(ptr);
	delete[](uint8_t*)ptr;
}

#endif
