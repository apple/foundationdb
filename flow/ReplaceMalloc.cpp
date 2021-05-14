/*
 * ReplaceMalloc.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#if defined(USE_JEMALLOC)

#include <cstdlib>
#include <exception>
#include <new>

#include "jemalloc/jemalloc.h"

void* operator new(std::size_t sz) {
	if (void* ptr = je_malloc(sz))
		return ptr;
	throw std::bad_alloc{};
}
void* operator new[](std::size_t sz) {
	if (void* ptr = je_malloc(sz))
		return ptr;
	throw std::bad_alloc{};
}
void* operator new(std::size_t sz, std::align_val_t align) {
	if (void* ptr = je_aligned_alloc(align, sz))
		return ptr;
	throw std::bad_alloc{};
}
void* operator new[](std::size_t sz, std::align_val_t align) {
	if (void* ptr = je_aligned_alloc(align, sz))
		return ptr;
	throw std::bad_alloc{};
}
void* operator new(std::size_t sz, std::no_throw_t const&) {
	return je_malloc(sz);
}
void operator delete(void* ptr) noexcept {
	je_free(ptr);
}
void operator delete[](void* ptr) noexcept {
	je_free(ptr);
}
void operator delete(void* ptr, size_t sz) noexcept {
	if (ptr) {
		je_sdallocx(ptr, sz, /*flags*/ 0);
	}
}

// Replace minimal set of functions for glibc: https://www.gnu.org/software/libc/manual/html_node/Replacing-malloc.html
extern "C" void* malloc(size_t sz) {
	return je_malloc(sz);
}
extern "C" void free(void* p) {
	return je_free(p);
}
extern "C" void* calloc(size_t n, size_t sz) {
	return je_calloc(n, sz);
}
extern "C" void* realloc(void* ptr, size_t size) {
	return je_realloc(ptr, size);
}
extern "C" void* aligned_alloc(size_t alignment, size_t size) {
	return je_aligned_alloc(alignment, size);
}
extern "C" size_t malloc_usable_size(void* ptr) {
	return je_malloc_usable_size(ptr);
}
extern "C" void* memalign(size_t alignment, size_t size) {
	return je_memalign(alignment, size);
}
extern "C" int posix_memalign(void** memptr, size_t alignment, size_t size) {
	return je_posix_memalign(memptr, alignment, size);
}
// jemalloc doesn't have pvalloc?
extern "C" void* valloc(size_t size) {
	return je_valloc(size);
}
extern "C" void* __libc_memalign(size_t alignment, size_t size) {
	return je_memalign(alignment, size);
}

#endif
