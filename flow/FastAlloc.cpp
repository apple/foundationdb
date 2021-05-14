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

#include "flow/FastAlloc.h"
#include "flow/Platform.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"
#include "flow/network.h"

#ifdef VALGRIND
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

#ifndef USE_JEMALLOC

void dumpHeapProfile(const char* file, const char* msg) {
	TraceEvent("HeapProfileNotSupported");
}
void traceHeapMetrics() {}
[[nodiscard]] void* allocateFast(int size) noexcept {
	return malloc(size);
}
void freeFast(void* ptr, int size) noexcept {
	return free(ptr);
}
void freeFast(void* ptr) noexcept {
	return free(ptr);
}

#if defined(_WIN32)
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
	auto* result = _aligned_malloc(size, align);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
void alignedFreeFast(void* ptr) noexcept {
	return _aligned_free(ptr);
}
#else
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
	// aligned_alloc isn't always available
	void* ptr = nullptr;
	posix_memalign(&ptr, align, size);
	if (!ptr) {
		platform::outOfMemory();
	}
	return ptr;
}
void alignedFreeFast(void* ptr) noexcept {
	return free(ptr);
}
#endif

#else

#include "jemalloc/jemalloc.h"
const char* je_malloc_conf = "prof:true";
void dumpHeapProfile(const char* file, const char* msg) {
	double before = g_network->timer();
	je_mallctl("prof.dump", nullptr, nullptr, &file, sizeof(file));
	TraceEvent("HeapProfile").detail("FileName", file).detail("Msg", msg).detail("Delay", g_network->timer() - before);
}
void traceHeapMetrics() {
	// Force cached stats to update
	je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
	size_t epoch = 0;
	je_mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));

	size_t sz, allocated, active, metadata, resident, mapped;
	sz = sizeof(size_t);
	if (je_mallctl("stats.allocated", &allocated, &sz, nullptr, 0) == 0 &&
	    je_mallctl("stats.active", &active, &sz, nullptr, 0) == 0 &&
	    je_mallctl("stats.metadata", &metadata, &sz, nullptr, 0) == 0 &&
	    je_mallctl("stats.resident", &resident, &sz, nullptr, 0) == 0 &&
	    je_mallctl("stats.mapped", &mapped, &sz, nullptr, 0) == 0) {
		TraceEvent("HeapMetrics")
		    .detail("Allocated", allocated)
		    .detail("Active", active)
		    .detail("Metadata", metadata)
		    .detail("Resident", resident)
		    .detail("Mapped", mapped);
	}
}
[[nodiscard]] void* allocateFast(int size) noexcept {
	auto* result = je_mallocx(size, /*flags*/ 0);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
void freeFast(void* ptr, int size) noexcept {
	return je_sdallocx(ptr, size, /*flags*/ 0);
}
void freeFast(void* ptr) noexcept {
	return je_dallocx(ptr, /*flags*/ 0);
}
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
	auto* result = je_aligned_alloc(align, size);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
void alignedFreeFast(void* ptr) noexcept {
	return je_free(ptr);
}

TEST_CASE("/FastAlloc/4096-aligned-allocation-no-internal-fragmentation") {
	auto* p = alignedAllocateFast(4096, 4096);
	ASSERT(je_sallocx(p, /*flags*/ 0) == 4096);
	alignedFreeFast(p);
	p = alignedAllocateFast(4096, 4096 * 2);
	ASSERT(je_sallocx(p, /*flags*/ 0) == 4096 * 2);
	alignedFreeFast(p);
	p = alignedAllocateFast(4096, 4096 * 4);
	ASSERT(je_sallocx(p, /*flags*/ 0) == 4096 * 4);
	alignedFreeFast(p);
	return Void();
}

#endif
