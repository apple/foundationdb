/*
 * FastAlloc.cpp
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

#include "FastAlloc.h"

#include "ThreadPrimitives.h"
#include "Trace.h"
#include "Error.h"

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

#define FAST_ALLOCATOR_DEBUG 0

#ifdef _MSC_VER
// warning 4073 warns about "initializers put in library initialization area", which is our intent
#pragma warning (disable: 4073)
#pragma init_seg(lib)
#define INIT_SEG
#elif defined(__GNUG__)
#ifdef __linux__
#define INIT_SEG __attribute__ ((init_priority (1000)))
#elif defined(__APPLE__)
#pragma message "init_priority is not supported on this platform; will this be a problem?"
#define INIT_SEG
#else
#error Where am I?
#endif
#else
#error Port me? (init_seg(lib))
#endif

template<int Size>
INIT_SEG thread_local typename FastAllocator<Size>::ThreadData FastAllocator<Size>::threadData;

#ifdef VALGRIND
template<int Size>
unsigned long FastAllocator<Size>::vLock = 1;
#endif

template<int Size>
void* FastAllocator<Size>::freelist = 0;

typedef void (*ThreadInitFunction)();

ThreadInitFunction threadInitFunction = 0;  // See ThreadCleanup.cpp in the C binding
void setFastAllocatorThreadInitFunction( ThreadInitFunction f ) { 
	ASSERT( !threadInitFunction );
	threadInitFunction = f; 
}

#ifdef ALLOC_INSTRUMENTATION
INIT_SEG std::map<const char*, AllocInstrInfo> allocInstr;
INIT_SEG std::unordered_map<int64_t, std::pair<uint32_t, size_t>> memSample;
INIT_SEG std::unordered_map<uint32_t, BackTraceAccount> backTraceLookup;
INIT_SEG ThreadSpinLock memLock;
const size_t SAMPLE_BYTES = 1e7;
template<int Size>
volatile int32_t FastAllocator<Size>::pageCount;
thread_local bool memSample_entered = false;
#endif

void recordAllocation( void *ptr, size_t size ) {
#ifdef ALLOC_INSTRUMENTATION_STDOUT
	std::string trace = platform::get_backtrace();
	printf("Alloc\t%p\t%d\t%s\n", ptr, size, platform::get_backtrace().c_str());
#endif
#ifdef ALLOC_INSTRUMENTATION
	if( memSample_entered )
		return;
	memSample_entered = true;

	if(((double)rand()) / RAND_MAX < ((double)size) / SAMPLE_BYTES) {
		void *buffer[100];
#if defined(__linux__)
		int nptrs = backtrace( buffer, 100 );
#elif defined(_WIN32)
		// We could be using fourth parameter to get a hash, but we'll do this
		//  in a unified way between platforms
		int nptrs = CaptureStackBackTrace( 1, 100, buffer, NULL );
#else
#error Instrumentation not supported on this platform
#endif

		if( nptrs > 0 ) {
			uint32_t a = 0, b = 0;
			hashlittle2( buffer, nptrs * sizeof(void *), &a, &b );

			{
				double countDelta = std::max(1.0, ((double)SAMPLE_BYTES) / size);
				size_t sizeDelta = std::max(SAMPLE_BYTES, size);
				ThreadSpinLockHolder holder( memLock );
				auto it = backTraceLookup.find( a );
				if( it == backTraceLookup.end() ) {
					auto& bt = backTraceLookup[ a ];
					bt.backTrace = new std::vector<void*>();
					for (int j = 0; j < nptrs; j++) {
						bt.backTrace->push_back( buffer[j] );
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
		}
	}
	memSample_entered = false;
#endif
}

void recordDeallocation( void *ptr ) {
#ifdef ALLOC_INSTRUMENTATION_STDOUT
	printf("Dealloc\t%p\n", ptr);
#endif
#ifdef ALLOC_INSTRUMENTATION
	if( memSample_entered ) // could this lead to deallocations not being recorded?
		return;
	memSample_entered = true;
	{
		ThreadSpinLockHolder holder( memLock );

		auto it = memSample.find( (int64_t)ptr );
		if( it == memSample.end() ) {
			memSample_entered = false;
			return;
		}
		auto bti = backTraceLookup.find( it->second.first );
		ASSERT( bti != backTraceLookup.end() );

		size_t sizeDelta = std::max(SAMPLE_BYTES, it->second.second);
		double countDelta = std::max(1.0, ((double)SAMPLE_BYTES) / it->second.second);

		bti->second.totalSize -= sizeDelta;
		bti->second.count -= countDelta;
		bti->second.sampleCount--;
		memSample.erase( it );
	}
	memSample_entered = false;
#endif
}

template <int Size>
struct FastAllocator<Size>::GlobalData {
	CRITICAL_SECTION mutex;
	std::vector<void*> magazines;   // These magazines are always exactly magazine_size ("full")
	std::vector<std::pair<int, void*>> partial_magazines;  // Magazines that are not "full" and their counts.  Only created by releaseThreadMagazines().
	long long memoryUsed;
	GlobalData() : memoryUsed(0) { 
		InitializeCriticalSection(&mutex);
	}
};

template <int Size>
long long FastAllocator<Size>::getMemoryUsed() {
	return globalData()->memoryUsed;
}

template <int Size>
long long FastAllocator<Size>::getMemoryUnused() {
	return globalData()->magazines.size() * magazine_size * Size;
}

static int64_t getSizeCode(int i) {
	switch (i) {
		case 16: return 1;
		case 32: return 2;
		case 64: return 3;
		case 128: return 4;
		case 256: return 5;
		case 512: return 6;
		case 1024: return 7;
		case 2048: return 8;
		case 4096: return 9;
		default: return 10;
	}
}

template<int Size>
void *FastAllocator<Size>::allocate() {
#if FASTALLOC_THREAD_SAFE
	ThreadData& thr = threadData;
	if (!thr.freelist) {
		if (thr.alternate) {
			thr.freelist = thr.alternate;
			thr.alternate = 0;
			thr.count = magazine_size;
		} else
			getMagazine();
	}
	--thr.count;
	void* p = thr.freelist;
#if VALGRIND
	VALGRIND_MAKE_MEM_DEFINED(p, sizeof(void*));
#endif
	thr.freelist = *(void**)p;
	//check( p, true );
#else
	void* p = freelist;
	if (!p) getMagazine();
#if VALGRIND
	VALGRIND_MAKE_MEM_DEFINED(p, sizeof(void*));
#endif
	freelist = *(void**)p;
#endif
#if VALGRIND
	VALGRIND_MALLOCLIKE_BLOCK( p, Size, 0, 0 );
#endif
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
	recordAllocation(p, Size);
#endif
	return p;
}

template<int Size>
void FastAllocator<Size>::release(void *ptr) {
#if FASTALLOC_THREAD_SAFE
	ThreadData& thr = threadData;
	if (thr.count == magazine_size) {
		if (thr.alternate)		// Two full magazines, return one
			releaseMagazine( thr.alternate );
		thr.alternate = thr.freelist;
		thr.freelist = 0;
		thr.count = 0;
	}
	++thr.count;
	*(void**)ptr = thr.freelist;
	//check(ptr, false);
	thr.freelist = ptr;
#else
	*(void**)ptr = freelist;
	freelist = ptr;
#endif

#if VALGRIND
	VALGRIND_FREELIKE_BLOCK( ptr, 0 );
#endif
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)
	recordDeallocation( ptr );
#endif
}

template <int Size>
void FastAllocator<Size>::check(void* ptr, bool alloc) {
#if FAST_ALLOCATOR_DEBUG
	//if (ptr == (void*)0x400200180)
	//	printf("%c%p\n", alloc?'+':'-', ptr);

	// Check for pointers that aren't part of this FastAllocator
	if (ptr < (void*)(((getSizeCode(Size)<<11) + 0) * magazine_size*Size) ||
		ptr > (void*)(((getSizeCode(Size)<<11) + 4000) * magazine_size*Size) ||
		(int64_t(ptr)&(Size-1)))
	{
		printf("Bad ptr: %p\n", ptr);
		abort();
	}

	// Redundant freelist pointers to detect outright smashing of the freelist
	if (alloc) {
		if ( *((void**)ptr+1) != *(void**)ptr ) {
			printf("Freelist corruption? %p %p\n", *(void**)ptr, *((void**)ptr+1));
			abort();
		}
		*((void**)ptr+1) = (void*)0;
	} else {
		*((void**)ptr+1) = *(void**)ptr;
	}

	// Track allocated/free status in a completely separate data structure to detect double frees
	int i = (int)((int64_t)ptr - ((getSizeCode(Size)<<11) + 0) * magazine_size*Size) / Size;
	static std::vector<bool> isFreed;
	if (!alloc) {
		if (i+1 > isFreed.size())
			isFreed.resize(i+1, false);
		if (isFreed[i]) {
			printf("Double free: %p\n", ptr);
			abort();
		}
		isFreed[i] = true;
	} else {
		if (i+1 > isFreed.size()) {
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
void FastAllocator<Size>::getMagazine() {
	if (threadInitFunction) threadInitFunction();
	EnterCriticalSection(&globalData()->mutex);
	if (globalData()->magazines.size()) {
		void* m = globalData()->magazines.back();
		globalData()->magazines.pop_back();
		LeaveCriticalSection(&globalData()->mutex);
		threadData.freelist = m;
		threadData.count = magazine_size;
		return;
	} else if (globalData()->partial_magazines.size()) {
		std::pair<int, void*> p = globalData()->partial_magazines.back();
		globalData()->partial_magazines.pop_back();
		LeaveCriticalSection(&globalData()->mutex);
		threadData.freelist = p.second;
		threadData.count = p.first;
		return;
	}
	globalData()->memoryUsed += magazine_size*Size;
	LeaveCriticalSection(&globalData()->mutex);

	// Allocate a new page of data from the system allocator
	#ifdef ALLOC_INSTRUMENTATION
	interlockedIncrement(&pageCount);
	#endif

	void** block = 0;
#if FAST_ALLOCATOR_DEBUG
#ifdef WIN32
	static int alt = 0; alt++;
	block = (void**)VirtualAllocEx( GetCurrentProcess(), 
									(void*)( ((getSizeCode(Size)<<11) + alt) * magazine_size*Size), magazine_size*Size, MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE );
#else
	static int alt = 0; alt++;
	void* desiredBlock = (void*)( ((getSizeCode(Size)<<11) + alt) * magazine_size*Size);
	block = (void**)mmap( desiredBlock, magazine_size*Size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0 );
	ASSERT( block == desiredBlock );
#endif
#else
	block = (void **)::allocate(magazine_size * Size, true);
#endif

	//void** block = new void*[ magazine_size * PSize ];
	for(int i=0; i<magazine_size-1; i++) {
		block[i*PSize+1] = block[i*PSize] = &block[(i+1)*PSize];
		check( &block[i*PSize], false );
	}
		
	block[(magazine_size-1)*PSize+1] = block[(magazine_size-1)*PSize] = 0;
	check( &block[(magazine_size-1)*PSize], false );
	threadData.freelist = block;
	threadData.count = magazine_size;
}
template <int Size>
void FastAllocator<Size>::releaseMagazine(void* mag) {
	EnterCriticalSection(&globalData()->mutex);
	globalData()->magazines.push_back(mag);
	LeaveCriticalSection(&globalData()->mutex);
}
template <int Size>
void FastAllocator<Size>::releaseThreadMagazines() {
	ThreadData& thr = threadData;

	if (thr.freelist || thr.alternate) {
		EnterCriticalSection(&globalData()->mutex);
		if (thr.freelist) globalData()->partial_magazines.push_back( std::make_pair(thr.count, thr.freelist) );
		if (thr.alternate) globalData()->magazines.push_back(thr.alternate);
		LeaveCriticalSection(&globalData()->mutex);
	}
	thr.count = 0;
	thr.alternate = 0;
	thr.freelist = 0;
}

void releaseAllThreadMagazines() {
	FastAllocator<16>::releaseThreadMagazines();
	FastAllocator<32>::releaseThreadMagazines();
	FastAllocator<64>::releaseThreadMagazines();
	FastAllocator<128>::releaseThreadMagazines();
	FastAllocator<256>::releaseThreadMagazines();
	FastAllocator<512>::releaseThreadMagazines();
	FastAllocator<1024>::releaseThreadMagazines();
	FastAllocator<2048>::releaseThreadMagazines();
	FastAllocator<4096>::releaseThreadMagazines();
}

template class FastAllocator<16>;
template class FastAllocator<32>;
template class FastAllocator<64>;
template class FastAllocator<128>;
template class FastAllocator<256>;
template class FastAllocator<512>;
template class FastAllocator<1024>;
template class FastAllocator<2048>;
template class FastAllocator<4096>;

