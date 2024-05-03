/*
 * ArenaAllocator.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_ARENA_ALLOCATOR_H
#define FLOW_ARENA_ALLOCATOR_H
#pragma once

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include <functional>
#include <type_traits>
#include <variant>

template <class T>
class ArenaAllocator {
	Arena* arenaPtr;

	Arena& arena() noexcept { return *arenaPtr; }

public:
	using pointer = T*;
	using const_pointer = const T*;
	using reference = T&;
	using const_reference = const T&;
	using void_pointer = void*;
	using const_void_pointer = const void*;
	using self_type = ArenaAllocator<T>;
	using size_type = size_t;
	using value_type = T;
	using difference_type = typename std::pointer_traits<pointer>::difference_type;

	// Unfortunately this needs to exist due to STL's internal use of Allocator() in internal coding
	ArenaAllocator() noexcept : arenaPtr(nullptr) {}

	ArenaAllocator(Arena& arena) noexcept : arenaPtr(&arena) {}

	ArenaAllocator(const self_type& other) noexcept = default;

	// Rebind constructor does not modify
	template <class U>
	ArenaAllocator(const ArenaAllocator<U>& other) noexcept : arenaPtr(other.arenaPtr) {}

	ArenaAllocator& operator=(const self_type& other) noexcept = default;

	ArenaAllocator(self_type&& other) noexcept = default;

	ArenaAllocator& operator=(self_type&& other) noexcept = default;

	T* allocate(size_t n) {
		if (!arenaPtr)
			throw bad_allocator();
		return new (arena()) T[n];
	}

	void deallocate(T*, size_t) noexcept {}

	bool operator==(const self_type& other) const noexcept { return arenaPtr == other.arenaPtr; }

	bool operator!=(const self_type& other) const noexcept { return !(*this == other); }

	template <class U>
	struct rebind {
		using other = ArenaAllocator<U>;
	};

	using is_always_equal = std::false_type;
	using propagate_on_container_copy_assignment = std::true_type;
	using propagate_on_container_move_assignment = std::true_type;
	using propagate_on_container_swap = std::true_type;
};

#endif /*FLOW_ARENA_ALLOCATOR_H*/
