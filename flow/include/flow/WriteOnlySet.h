/*
 * WriteOnlySet.h
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

#pragma once
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include <boost/lockfree/queue.hpp>

#ifdef ENABLE_SAMPLING
/**
 * This is a Write-Only set that supports copying the whole content. This data structure is lock-free and allows a user
 * to insert and remove objects up to a given capacity (passed by a template).
 *
 * Template parameters:
 * \param T The type to store.
 * \param IndexType The type used as an index
 * \param CAPACITY The maximum number of object this structure can store (if a user tries to store more, insert will
 *                 fail gracefully)
 * \pre T implements `void addref() const` and `void delref() const`
 * \pre IndexType must have a copy constructor
 * \pre IndexType must have a trivial assignment operator
 * \pre IndexType must have a trivial destructor
 * \pre IndexType can be used as an index into a std::vector
 */
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
	WriteOnlySet(WriteOnlySet&&) = delete;
	WriteOnlySet& operator=(const WriteOnlySet&) = delete;
	WriteOnlySet& operator=(WriteOnlySet&&) = delete;

	/**
	 * Attempts to insert \p lineage into the set. This method can fail if the set is full (its size is equal to its
	 * capacity). Calling insert on a full set is safe but the method will return \ref npos if the operation fails.
	 *
	 * \param lineage A reference to the object the user wants to insert.
	 * \ret An index that can later be used to erase the value again or \ref npos if the insert failed.
	 * \pre lineage.getPtr() % 2 == 0 (the memory for lineage has to be at least 2 byte aligned)
	 */
	[[nodiscard]] Index insert(const Reference<T>& lineage);

	/**
	 * Erases the object associated with \p idx from the set.
	 *
	 * \ret Whether the reference count was decremented. Usually the return value is only interesting for testing and
	 *      benchmarking purposes and will in most cases be ignored. If \ref delref wasn't called, it will be called
	 *      later. Note that at the time the return value is checked, \ref delref might already have been called.
	 */
	bool erase(Index idx);

	/**
	 * Replaces the object associated with \p idx with \p lineage.
	 *
	 * \ret Whether the reference count of the replaced object was decremented. Usually the return value is only
	 *      interesting for testing and benchmarking purposes and will in most cases be ignored. If \ref delref
	 *      wasn't called, it will be called later. Note that at the time the return value is checked, \ref delref
	 *      might already have been called.
	 */
	bool replace(Index idx, const Reference<T>& lineage);

	/**
	 * Copies all elements that are stored in the set into a vector. This copy operation does NOT provide a snapshot of
	 * the data structure. The contract is weak:
	 * - All object that were in the set before copy is called and weren't removed until after copy returned are
	 *   guaranteed to be in the result.
	 * - Any object that was inserted while copy is running might be in the result.
	 * - Any object that was erased while copy is running might be in the result.
	 */
	std::vector<Reference<T>> copy();

protected:
	// the implementation of erase -- the wrapper just makes the function a bit more readable.
	bool eraseImpl(Index idx);

	// the last bit of a pointer within the set is used like a boolean and true means that the object is locked. Locking
	// an object is only relevant for memory management. A locked pointer can still be erased from the set, but the
	// erase won't call delref on the object. Instead it will push the pointer into the \ref freeList and copy will call
	// delref later.
	static constexpr uintptr_t LOCK = 0b1;

	// The actual memory
	std::vector<std::atomic<std::uintptr_t>> _set;
	static_assert(std::atomic<Index>::is_always_lock_free, "Index type can't be used as a lock-free type");
	static_assert(std::atomic<uintptr_t>::is_always_lock_free, "uintptr_t can't be used as a lock-free type");

	// The freeQueue. On creation all indexes (0..capacity-1) are pushed into this queue. On insert one element from
	// this queue is consumed and the resulting number is used as an index into the set. On erase the index is given
	// back to the freeQueue.
	boost::lockfree::queue<Index, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeQueue;

	// The freeList is used for memory management. Generally copying a shared pointer can't be done in a lock-free way.
	// Instead, when we copy the data structure we first copy the address, then attempt to set the last bit to 1 and
	// only if that succeeds we will increment the reference count. Whenever we attempt to remove an object
	// in \ref erase we remove the object from the set (using an atomic compare and swap) and only decrement the
	// reference count if the last bit is 0. If it's not we'll push the pointer into this free list.
	// \ref copy will consume all elements from this freeList each time it runs and decrements the refcount for each
	// element.
	boost::lockfree::queue<T*, boost::lockfree::fixed_sized<true>, boost::lockfree::capacity<CAPACITY>> freeList;
};

/**
 * Provides a thread safe, lock-free write only variable.
 *
 * Template parameters:
 * \param T The type to store.
 * \param IndexType The type used as an index
 * \pre T implements `void addref() const` and `void delref() const`
 * \pre IndexType must have a copy constructor
 * \pre IndexType must have a trivial assignment operator
 * \pre IndexType must have a trivial destructor
 * \pre IndexType can be used as an index into a std::vector
 */
template <class T, class IndexType>
class WriteOnlyVariable : private WriteOnlySet<T, IndexType, 1> {
public:
	explicit WriteOnlyVariable();

	/**
	 * Returns a copied reference to the stored variable.
	 */
	Reference<T> get();

	/**
	 * Replaces the variable with \p lineage. \p lineage is permitted to be an invalid pointer.
	 *
	 * \ret Whether the reference count of the replaced object was decremented. Note that if the reference being
	 * replaced is invalid, this function will always return false. If \ref delref wasn't called and the reference was
	 * valid, it will be called later. Note that at the time the return value is checked, \ref delref might already have
	 *      been called.
	 */
	bool replace(const Reference<T>& element);
};

class ActorLineage;
extern template class WriteOnlySet<ActorLineage, unsigned, 1024>;

using ActorLineageSet = WriteOnlySet<ActorLineage, unsigned, 1024>;
#endif
