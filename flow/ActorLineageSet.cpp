/*
 * ActorLineageSet.cpp
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

#include "flow/flow.h"
#include <boost/lockfree/queue.hpp>

class ActorLineageSet {
public:
	// The type we use for lookup into the set. Gets assigned during insert
	using Index = unsigned;
	// For now we use a fixed size capacity
	constexpr static Index CAPACITY = 1024;
	constexpr static Index npos = std::numeric_limits<Index>::max();

	explicit ActorLineageSet();
	ActorLineageSet(const ActorLineageSet&) = delete;
	ActorLineageSet& operator=(const ActorLineageSet&) = delete;

	// Returns the number of elements at the time of calling. Keep in mind that this is a lockfree data structure, so
	// the actual size might change anytime after or even during the call. This function only guarantees that the size
	// was whatever the method returns at one point between the start and the end of the function call. The safest way
	// to handle this is by assuming that this returns an estimate.
	unsigned size();

	Index insert(const Reference<ActorLineage>& lineage);
	void erase(Index idx);
	std::vector<Reference<ActorLineage>> copy();

private:
	static constexpr uintptr_t FREE = 0b1;
	static constexpr uintptr_t LOCK = 0b10;
	std::atomic<unsigned> _size = 0;
	std::vector<std::atomic<std::uintptr_t>> _set;
	boost::lockfree::queue<Index, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeQueue;
	boost::lockfree::queue<ActorLineage*, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>>
	    freeList;
};

ActorLineageSet::ActorLineageSet() {
	// insert the free indexes in reverse order
	for (unsigned i = CAPACITY; i > 0; --i) {
		freeQueue.push(i - 1);
		_set[i] = uintptr_t(1);
	}
}

std::vector<Reference<ActorLineage>> ActorLineageSet::copy() {
	std::vector<Reference<ActorLineage>> result;
	for (int i = 0; i < CAPACITY; ++i) {
		auto ptr = _set[i].load();
		if ((ptr & FREE) != 0) {
			ASSERT((ptr & LOCK) == 0);
			if (_set[i].compare_exchange_strong(ptr, ptr | LOCK)) {
				ActorLineage* entry = reinterpret_cast<ActorLineage*>(ptr);
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
	ActorLineage* toClean;
	while (freeList.pop(toClean)) {
		toClean->delref();
	}
	return result;
}

ActorLineageSet::Index ActorLineageSet::insert(const Reference<ActorLineage>& lineage) {
	Index res;
	if (!freeQueue.pop(res)) {
		TraceEvent(SevWarnAlways, "NoCapacityInActorLineageSet");
		return npos;
	}
	ASSERT(_set[res].load() & FREE);
	auto ptr = reinterpret_cast<uintptr_t>(lineage.getPtr());
	lineage->addref();
	_set[res].store(ptr);
	return res;
}

void ActorLineageSet::erase(Index idx) {
	while (true) {
		auto ptr = _set[idx].load();
		if (ptr & LOCK) {
			_set[idx].store(FREE);
			freeList.push(reinterpret_cast<ActorLineage*>(ptr ^ LOCK));
			return;
		} else {
			if (_set[idx].compare_exchange_strong(ptr, FREE)) {
				reinterpret_cast<ActorLineage*>(ptr)->delref();
				return;
			}
		}
	}
}