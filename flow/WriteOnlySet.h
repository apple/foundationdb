/*
 * WriteOnlySet.cpp
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

#pragma once
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include <boost/lockfree/queue.hpp>

template <class T, class IndexType, IndexType CAPACITY>
class WriteOnlySet {
public:
	// The type we use for lookup into the set. Gets assigned during insert
	using Index = IndexType;
	// For now we use a fixed size capacity
	constexpr static Index npos = std::numeric_limits<Index>::max();

	explicit WriteOnlySet();
	WriteOnlySet(const WriteOnlySet&) = delete;
	WriteOnlySet& operator=(const WriteOnlySet&) = delete;

	// Returns the number of elements at the time of calling. Keep in mind that this is a lockfree data structure, so
	// the actual size might change anytime after or even during the call. This function only guarantees that the size
	// was whatever the method returns at one point between the start and the end of the function call. The safest way
	// to handle this is by assuming that this returns an estimate.
	unsigned size();

	Index insert(const Reference<T>& lineage);
	void erase(Index idx);
	std::vector<Reference<T>> copy();

private:
	static constexpr uintptr_t FREE = 0b1;
	static constexpr uintptr_t LOCK = 0b10;
	std::atomic<Index> _size = 0;
	std::vector<std::atomic<std::uintptr_t>> _set;
	static_assert(std::atomic<Index>::is_always_lock_free, "Index type can't be used as a lock-free type");
	static_assert(std::atomic<Index>::is_always_lock_free, "uintptr_t can't be used as a lock-free type");
	boost::lockfree::queue<Index, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeQueue;
	boost::lockfree::queue<T*, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeList;
};

template <class T, class IndexType, IndexType CAPACITY>
WriteOnlySet<T, IndexType, CAPACITY>::WriteOnlySet() : _set(CAPACITY) {
	// insert the free indexes in reverse order
	for (unsigned i = CAPACITY; i > 0; --i) {
		freeQueue.push(i - 1);
		_set[i] = uintptr_t(FREE);
	}
}

template <class T, class IndexType, IndexType CAPACITY>
std::vector<Reference<T>> WriteOnlySet<T, IndexType, CAPACITY>::copy() {
	std::vector<Reference<T>> result;
	for (int i = 0; i < CAPACITY; ++i) {
		auto ptr = _set[i].load();
		if ((ptr & FREE) != 0) {
			ASSERT((ptr & LOCK) == 0);
			if (_set[i].compare_exchange_strong(ptr, ptr | LOCK)) {
				T* entry = reinterpret_cast<T*>(ptr);
				ptr |= LOCK;
				entry->addref();
				// we try to unlock now. If this element was removed while we incremented the refcount, the element will
				// end up in the freeList, so we will decrement later.
				_set[i].compare_exchange_strong(ptr, ptr ^ LOCK);
				result.emplace_back(entry);
			}
		}
	}
	// after we're done we need to clean up all objects that contented on a lock. This won't be perfect (as some thread
	// might not yet added the object to the free list), but whatever we don't get now we'll clean up in the next
	// iteration
	freeList.consume_all([](auto toClean) { toClean->delref(); });
	return result;
}

class ActorLineage;
extern template class WriteOnlySet<ActorLineage, unsigned, 1024>;
