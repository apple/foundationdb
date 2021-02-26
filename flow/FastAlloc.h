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

#include "flow/Error.h"
#include <cstdint>
#include <cstddef>

inline constexpr int nextFastAllocatedSize(int x) {
	// Based on http://jemalloc.net/jemalloc.3.html#size_classes
	ASSERT(0 < x && x <= 8192);
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
void freeFast(void* ptr, int size) noexcept;
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
	[[nodiscard]] static void* operator new(std::size_t s) {
		ASSERT(s == sizeof(Object));
		if constexpr (alignof(Object) <= sizeof(void*)) {
			return allocateFast(sizeof(Object));
		} else {
			return alignedAllocateFast(alignof(Object), sizeof(Object));
		}
	}

	static void operator delete(void* s) {
		if constexpr (alignof(Object) <= sizeof(void*)) {
			return freeFast(s, sizeof(Object));
		} else {
			return alignedFreeFast(s, alignof(Object), sizeof(Object));
		}
	}
	// Redefine placement new so you can still use it
	static void* operator new(std::size_t, void* p) { return p; }
	static void operator delete(void*, void*) {}
};

#endif
