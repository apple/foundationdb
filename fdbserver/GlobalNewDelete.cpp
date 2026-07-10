/*
 * GlobalNewDelete.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

// Process-wide replacements for the global operator new / operator delete set,
// owned by the fdbserver binary.
//
// These live here, in a translation unit compiled directly into the fdbserver
// executable, rather than in the `flow` static library, for two reasons:
//
//  1. Correctness of interposition. operator new / operator delete are
//     replaceable functions; a definition sitting in a static archive is only
//     pulled into the link if the linker already needs some other symbol from
//     that same object file. Placing them in an executable TU guarantees the
//     replacements are part of the final link instead of relying on incidental
//     archive pull-in.
//
//  2. Client isolation. `flow` is linked into libfdb_c and every client
//     binding; a global-new override compiled into it would interpose the
//     entire host process's allocator in any application that loads the client.
//     fdbserver is a standalone executable that clients never link, so keeping
//     these here confines the interposition to the server.
//
// Exactly one implementation is compiled, chosen by the same ALLOC_INSTRUMENTATION
// flags the legacy accounting framework uses (so the two never define the global
// operators twice):
//
//   - ALLOC_INSTRUMENTATION[_STDOUT] on  -> legacy FastAlloc accounting hooks.
//   - otherwise                          -> the sampled per-call-site memory
//                                           tracker (flow/MemoryTracker.*).

#include <cstdlib>
#include <new>

// TODO: the old ALLOC_INSTRUMENTATION doesn't seem to be usable at
// scale. Consider deleting it.
#if defined(ALLOC_INSTRUMENTATION) || defined(ALLOC_INSTRUMENTATION_STDOUT)

#include "flow/FastAlloc.h"

void* operator new(std::size_t size) {
	void* p = malloc(size);
	if (!p) {
		throw std::bad_alloc();
	}
	recordAllocation(p, size);
	return p;
}
void operator delete(void* ptr) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// scalar, nothrow new and it matching delete
void* operator new(std::size_t size, const std::nothrow_t&) throw() {
	void* p = malloc(size);
	recordAllocation(p, size);
	return p;
}
void operator delete(void* ptr, const std::nothrow_t&) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// array throwing new and matching delete[]
void* operator new[](std::size_t size) {
	void* p = malloc(size);
	if (!p) {
		throw std::bad_alloc();
	}
	recordAllocation(p, size);
	return p;
}
void operator delete[](void* ptr) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

// array, nothrow new and matching delete[]
void* operator new[](std::size_t size, const std::nothrow_t&) throw() {
	void* p = malloc(size);
	recordAllocation(p, size);
	return p;
}
void operator delete[](void* ptr, const std::nothrow_t&) throw() {
	recordDeallocation(ptr);
	free(ptr);
}

#else // sampled memory tracker, see design/memory-tracker.md

#include "flow/MemoryTracker.h"

void* operator new(std::size_t n) {
	void* p = std::malloc(n);
	if (!p) {
		throw std::bad_alloc();
	}
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n) {
	void* p = std::malloc(n);
	if (!p) {
		throw std::bad_alloc();
	}
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
	void* p = std::malloc(n);
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p, const std::nothrow_t&) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
	void* p = std::malloc(n);
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

// C++17 over-aligned new/delete.
void* operator new(std::size_t n, std::align_val_t a) {
	void* p = nullptr;
	if (posix_memalign(&p, static_cast<std::size_t>(a), n) != 0) {
		throw std::bad_alloc();
	}
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete(void* p, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

void* operator new[](std::size_t n, std::align_val_t a) {
	void* p = nullptr;
	if (posix_memalign(&p, static_cast<std::size_t>(a), n) != 0) {
		throw std::bad_alloc();
	}
	memTrackerOnAlloc(p, n);
	return p;
}
void operator delete[](void* p, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
	memTrackerOnFree(p);
	std::free(p);
}

#endif // ALLOC_INSTRUMENTATION
