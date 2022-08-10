/*
 * Deque.h
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

#ifndef FLOW_DEQUE_H
#define FLOW_DEQUE_H
#pragma once

#include "flow/Platform.h"
#include <stdexcept>

template <class T>
class Deque {
	// Double ended queue implemented using circular array (and on-demand reallocation, like std::vector)
	// Interface similar to std::deque, but incomplete (also reallocation invalidates all iterators like std::vector)
	// Capacity is limited to 2^32-1 items even in 64 bit

public:
	typedef T value_type;
	typedef T& reference;
	typedef T const& const_reference;
	typedef int32_t difference_type;
	typedef uint32_t size_type;

	Deque() : arr(0), begin(0), end(0), mask(-1) {}

	// TODO: iterator construction, other constructors
	Deque(Deque const& r) : arr(nullptr), begin(0), end(r.size()), mask(r.mask) {
		if (r.capacity() > 0) {
			arr = (T*)aligned_alloc(std::max(__alignof(T), sizeof(void*)), capacity() * sizeof(T));
			if (arr == nullptr) {
				platform::outOfMemory();
			}
		}
		ASSERT(capacity() >= end || end == 0);
		if (r.end < r.capacity()) {
			std::copy(r.arr + r.begin, r.arr + r.begin + r.size(), arr);
		} else {
			// r.begin is always < capacity(), and r.end is always >= r.begin.  Mask is used for wrapping r.end.
			// but if r.end >= r.capacity(), the deque wraps around so the
			// copy must be performed in two parts
			auto partTwo = std::copy(r.arr + r.begin, r.arr + r.capacity(), arr);
			std::copy(r.arr, r.arr + (r.end & r.mask), partTwo);
		}
	}

	void operator=(Deque const& r) {
		cleanup();

		arr = nullptr;
		begin = 0;
		end = r.size();
		mask = r.mask;
		if (r.capacity() > 0) {
			arr = (T*)aligned_alloc(std::max(__alignof(T), sizeof(void*)), capacity() * sizeof(T));
			if (arr == nullptr) {
				platform::outOfMemory();
			}
		}
		ASSERT(capacity() >= end || end == 0);
		if (r.end < r.capacity()) {
			std::copy(r.arr + r.begin, r.arr + r.begin + r.size(), arr);
		} else {
			// r.begin is always < capacity(), and r.end is always >= r.begin.  Mask is used for wrapping r.end.
			// but if r.end >= r.capacity(), the deque wraps around so the
			// copy must be performed in two parts
			auto partTwo = std::copy(r.arr + r.begin, r.arr + r.capacity(), arr);
			std::copy(r.arr, r.arr + (r.end & r.mask), partTwo);
		}
	}

	Deque(Deque&& r) noexcept : arr(r.arr), begin(r.begin), end(r.end), mask(r.mask) {
		r.arr = nullptr;
		r.begin = r.end = 0;
		r.mask = -1;
	}

	void operator=(Deque&& r) noexcept {
		cleanup();

		begin = r.begin;
		end = r.end;
		mask = r.mask;
		arr = r.arr;

		r.arr = nullptr;
		r.begin = r.end = 0;
		r.mask = -1;
	}

	bool operator==(const Deque& r) const {
		if (size() != r.size())
			return false;
		for (uint32_t i = 0; i < size(); i++)
			if ((*this)[i] != r[i])
				return false;
		return true;
	}
	bool operator!=(const Deque& r) const { return !(*this == r); }

	~Deque() { cleanup(); }

	void push_back(const T& val) {
		if (full())
			grow();
		new (&arr[end & mask]) T(val);
		end++;
	}

	template <class... U>
	reference emplace_back(U&&... val) {
		if (full())
			grow();
		new (&arr[end & mask]) T(std::forward<U>(val)...);
		reference result = arr[end & mask];
		end++;
		return result;
	}

	void pop_back() {
		ASSERT(!empty());
		end--;
		arr[end & mask].~T();
	}

	void pop_front() {
		ASSERT(!empty());
		arr[begin].~T();
		if (begin == mask) {
			begin -= mask;
			end -= mask + 1;
		} else
			begin++;
	}

	void clear() {
		for (uint32_t i = begin; i != end; i++)
			arr[i & mask].~T();
		begin = end = 0;
	}

	size_type size() const { return end - begin; }
	bool empty() const { return end == begin; }
	size_type capacity() const { return mask + 1; }
	size_type max_size() const {
		return 1 << 30;
	} // All the logic should work at size 2^32, but size() can't return it, and callers might break, and there might be
	  // bugs...

	T& front() { return arr[begin]; }
	T const& front() const { return arr[begin]; }
	T& back() { return arr[(end - 1) & mask]; }
	T const& back() const { return arr[(end - 1) & mask]; }

	T& operator[](int i) { return arr[(begin + i) & mask]; }
	T const& operator[](int i) const { return arr[(begin + i) & mask]; }

	T& at(int i) {
		if (i < 0 || i >= end - begin)
			throw std::out_of_range("requires 0 <= i < size");
		return (*this)[i];
	}
	T const& at(int i) const {
		if (i < 0 || i >= end - begin)
			throw std::out_of_range("requires 0 <= i < size");
		return (*this)[i];
	}

private:
	T* arr;
	uint32_t begin, end, mask;

	bool full() const { return end == begin + mask + 1; }
	void grow() {
		// This doubles capacity (or makes it at least 8), and arbitrarily moves begin to be 0

		size_t mp1 = arr ? size_t(mask) + 1 : 4;
		size_t newSize = mp1 * 2;
		if (newSize > max_size())
			throw std::bad_alloc();
		// printf("Growing to %lld (%u-%u mask %u)\n", (long long)newSize, begin, end, mask);
		T* newArr = (T*)aligned_alloc(std::max(__alignof(T), sizeof(void*)),
		                              newSize * sizeof(T)); // SOMEDAY: FastAllocator
		if (newArr == nullptr) {
			platform::outOfMemory();
		}
		for (int i = begin; i != end; i++) {
			try {
				new (&newArr[i - begin]) T(std::move_if_noexcept(arr[i & mask]));
			} catch (...) {
				cleanup(newArr, i - begin);
				throw;
			}
		}
		for (int i = begin; i != end; i++) {
			static_assert(std::is_nothrow_destructible_v<T>);
			arr[i & mask].~T();
		}
		aligned_free(arr);
		arr = newArr;
		end -= begin;
		begin = 0;
		mask = uint32_t(newSize - 1);
	}

	static void cleanup(T* data, size_t size) noexcept {
		for (int i = 0; i < size; ++i) {
			data[i].~T();
		}
		aligned_free(data);
	}

	void cleanup() noexcept {
		for (int i = begin; i != end; i++)
			arr[i & mask].~T();
		if (arr)
			aligned_free(arr);
	}
};

#endif
