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
	constexpr static IndexType capacity = CAPACITY;

	explicit WriteOnlySet();
	WriteOnlySet(const WriteOnlySet&) = delete;
	WriteOnlySet& operator=(const WriteOnlySet&) = delete;

	// Returns the number of elements at the time of calling. Keep in mind that this is a lockfree data structure, so
	// the actual size might change anytime after or even during the call. This function only guarantees that the size
	// was whatever the method returns at one point between the start and the end of the function call. The safest way
	// to handle this is by assuming that this returns an estimate.
	unsigned size();

	Index insert(const Reference<T>& lineage);
	bool erase(Index idx);
	std::vector<Reference<T>> copy();

private:
	bool eraseImpl(Index idx);

	static constexpr uintptr_t LOCK = 0b1;
	std::atomic<Index> _size = 0;
	std::vector<std::atomic<std::uintptr_t>> _set;
	static_assert(std::atomic<Index>::is_always_lock_free, "Index type can't be used as a lock-free type");
	static_assert(std::atomic<Index>::is_always_lock_free, "uintptr_t can't be used as a lock-free type");
	boost::lockfree::queue<Index, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeQueue;
	boost::lockfree::queue<T*, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeList;
};

class ActorLineage;
extern template class WriteOnlySet<ActorLineage, unsigned, 1024>;

using ActorLineageSet = WriteOnlySet<ActorLineage, unsigned, 1024>;
