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

#if VALGRIND
#include <drd.h>
#include <memcheck.h>
bool valgrindPrecise();
#endif

#include "flow/Trace.h"
#include "flow/Error.h"
#include <cstdint>
#include <cstddef>

inline constexpr int nextFastAllocatedSize(int x) {
	// Based on http://jemalloc.net/jemalloc.3.html#size_classes
	ASSERT(0 < x && x <= 8192);
	if (x <= 8)
		return 8;
	if (x <= 16)
		return 16;
	if (x <= 32)
		return 32;
	if (x <= 48)
		return 48;
	if (x <= 64)
		return 64;
	if (x <= 80)
		return 80;
	if (x <= 96)
		return 96;
	if (x <= 112)
		return 112;
	if (x <= 128)
		return 128;
	if (x <= 160)
		return 160;
	if (x <= 192)
		return 192;
	if (x <= 224)
		return 224;
	if (x <= 256)
		return 256;
	if (x <= 320)
		return 320;
	if (x <= 384)
		return 384;
	if (x <= 448)
		return 448;
	if (x <= 512)
		return 512;
	if (x <= 640)
		return 640;
	if (x <= 768)
		return 768;
	if (x <= 896)
		return 896;
	if (x <= 1024)
		return 1024;
	if (x <= 1280)
		return 1280;
	if (x <= 1536)
		return 1536;
	if (x <= 1792)
		return 1792;
	if (x <= 2048)
		return 2048;
	if (x <= 2560)
		return 2560;
	if (x <= 3072)
		return 3072;
	if (x <= 3584)
		return 3584;
	if (x <= 4096)
		return 4096;
	if (x <= 5 * 1024)
		return 5 * 1024;
	if (x <= 6 * 1024)
		return 6 * 1024;
	if (x <= 7 * 1024)
		return 7 * 1024;
	if (x <= 8 * 1024)
		return 8 * 1024;
	return x;
}

void traceHeapMetrics();

// allocate, free, and sized free

// Obtain a heap allocation of |size| bytes. |size| must be > 0.
[[nodiscard]] inline void* allocateFast(int size) noexcept;
// |size| must be as passed to allocateFast to obtain |ptr|. |ptr| must not be null.
inline void freeFast(void* ptr, int size) noexcept;
// |ptr| must have been returned by allocateFast. Prefer the overload accepting |size| when possible. |ptr| must not be
// null.
inline void freeFast(void* ptr) noexcept;

// aligned allocation

// |align| must be a power of 2, and |size| must be a multiple of align. |align| must be at least sizeof(void*).
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept;
// |ptr| must have been returned by alignedAllocateFast. |ptr| may be be null.
inline void alignedFreeFast(void* ptr) noexcept;

template <class Object>
class FastAllocated {
public:
	[[nodiscard]] static void* operator new(std::size_t s) {
		ASSERT(s == sizeof(Object));
		return allocateFast(sizeof(Object));
	}

	static void operator delete(void* s) { return freeFast(s, sizeof(Object)); }
	// Redefine placement new so you can still use it
	static void* operator new(std::size_t, void* p) { return p; }
	static void operator delete(void*, void*) {}
};

//////////////////// Implementation ////////////////////

#ifndef USE_JEMALLOC

inline void traceHeapMetrics() {}
[[nodiscard]] inline void* allocateFast(int size) noexcept {
	return malloc(size);
}
inline void freeFast(void* ptr, int size) noexcept {
	return free(ptr);
}
inline void freeFast(void* ptr) noexcept {
	return free(ptr);
}

#if defined(_WIN32)
[[nodiscard]] inline void* alignedAllocateFast(int align, int size) noexcept {
	auto* result = _aligned_malloc(size, align);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
inline void alignedFreeFast(void* ptr) noexcept {
	return _aligned_free(ptr);
}
#else
[[nodiscard]] inline void* alignedAllocateFast(int align, int size) noexcept {
	// aligned_alloc isn't always available
	void* ptr = nullptr;
	posix_memalign(&ptr, align, size);
	if (!ptr) {
		platform::outOfMemory();
	}
	return ptr;
}
inline void alignedFreeFast(void* ptr) noexcept {
	return free(ptr);
}
#endif

#else

#include "jemalloc/jemalloc.h"

inline void traceHeapMetrics() {
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
[[nodiscard]] inline void* allocateFast(int size) noexcept {
	auto* result = je_mallocx(size, /*flags*/ 0);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
inline void freeFast(void* ptr, int size) noexcept {
	return je_sdallocx(ptr, size, /*flags*/ 0);
}
inline void freeFast(void* ptr) noexcept {
	return je_dallocx(ptr, /*flags*/ 0);
}
[[nodiscard]] inline void* alignedAllocateFast(int align, int size) noexcept {
	auto* result = je_aligned_alloc(align, size);
	if (!result) {
		platform::outOfMemory();
	}
	return result;
}
inline void alignedFreeFast(void* ptr) noexcept {
	return je_free(ptr);
}

#endif

#endif
