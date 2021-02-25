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
[[nodiscard]] void* allocateFast(int size) noexcept {
	return malloc(size);
}
void freeFast(int size, void* ptr) noexcept {
	return free(ptr);
}
void freeFast(void* ptr) noexcept {
	return free(ptr);
}
[[nodiscard]] void* alignedAllocateFast(int align, int size) noexcept {
	return aligned_alloc(align, size);
}
void alignedFreeFast(void* ptr, int align, int size) noexcept {
	return free(ptr);
}
#else
#include "jemalloc/jemalloc.h"
const char* je_malloc_conf = "prof:true";
void dumpHeapProfile() {
	std::string fileName = "fdb_heap_profile_" + deterministicRandom()->randomUniqueID().toString() + ".heap";
	const char* name = fileName.c_str();
	je_mallctl("prof.dump", nullptr, nullptr, &name, sizeof(name));
	TraceEvent("HeapProfile").detail("FileName", name);
}
[[nodiscard]] void* allocateFast(int size) noexcept {
	return je_mallocx(size, /*flags*/ 0);
}
void freeFast(int size, void* ptr) noexcept {
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
