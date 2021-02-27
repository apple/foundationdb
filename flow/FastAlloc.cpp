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
#include "flow/flow.h"

#if defined(_WIN32) || defined(USE_SANITIZER) || defined(USE_VALGRIND) || defined(__FreeBSD__)

void dumpHeapProfile() {
	TraceEvent("HeapProfileNotSupported");
}
void traceHeapStats() {}
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
	return _aligned_malloc(size, align);
	return aligned_alloc(align, size);
}
void alignedFreeFast(void* ptr, int align, int size) noexcept {
	return aligned_free(ptr);
}
#else
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
#if defined(HAS_ALIGNED_ALLOC)
	return aligned_alloc(align, size);
#else
	void* ptr = nullptr;
	posix_memalign(&ptr, alignment, size);
	return ptr;
#endif
}
void alignedFreeFast(void* ptr, int align, int size) noexcept {
	return aligned_free(ptr);
}
#endif

#else

#include "jemalloc/jemalloc.h"
const char* je_malloc_conf = "prof:true";
void dumpHeapProfile() {
	std::string fileName = "fdb_heap_profile_" + deterministicRandom()->randomUniqueID().toString() + ".heap";
	const char* name = fileName.c_str();
	je_mallctl("prof.dump", nullptr, nullptr, &name, sizeof(name));
	TraceEvent("HeapProfile").detail("FileName", name);
}
void traceHeapStats() {
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
		TraceEvent("HeapStats")
		    .detail("Allocated", allocated)
		    .detail("Active", active)
		    .detail("Metadata", metadata)
		    .detail("Resident", resident)
		    .detail("Mapped", mapped);
	}
}
[[nodiscard]] void* allocateFast(int size) noexcept {
	return je_mallocx(size, /*flags*/ 0);
}
void freeFast(void* ptr, int size) noexcept {
	return je_sdallocx(ptr, size, /*flags*/ 0);
}
void freeFast(void* ptr) noexcept {
	return je_dallocx(ptr, /*flags*/ 0);
}
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
	return je_mallocx(size, /*flags*/ MALLOCX_ALIGN(align));
}
void alignedFreeFast(void* ptr, int align, int size) noexcept {
	return je_sdallocx(ptr, size, /*flags*/ MALLOCX_ALIGN(align));
}
#endif
