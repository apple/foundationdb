/*
 * WriteOnlySet.actor.cpp
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

#include "flow/DeterministicRandom.h"
#include "flow/WriteOnlySet.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"

#include <chrono>
#include <random>
#include "flow/actorcompiler.h" // has to be last include

#ifdef ENABLE_SAMPLING
template <class T, class IndexType, IndexType CAPACITY>
auto WriteOnlySet<T, IndexType, CAPACITY>::insert(const Reference<T>& lineage) -> Index {
	Index res;
	if (!freeQueue.pop(res)) {
		TraceEvent(SevWarnAlways, "NoCapacityInWriteOnlySet");
		return npos;
	}
	ASSERT(_set[res].load() == 0);
	auto ptr = reinterpret_cast<uintptr_t>(lineage.getPtr());
	ASSERT((ptr % 2) == 0); // this needs to be at least 2-byte aligned
	ASSERT(ptr != 0);
	lineage->addref();
	_set[res].store(ptr);
	return res;
}

template <class T, class IndexType, IndexType CAPACITY>
bool WriteOnlySet<T, IndexType, CAPACITY>::eraseImpl(Index idx) {
	while (true) {
		auto ptr = _set[idx].load();
		if (ptr & LOCK) {
			_set[idx].store(0);
			freeList.push(reinterpret_cast<T*>(ptr ^ LOCK));
			return false;
		} else {
			if (_set[idx].compare_exchange_strong(ptr, 0)) {
				reinterpret_cast<T*>(ptr)->delref();
				return true;
			}
		}
	}
}

template <class T, class IndexType, IndexType CAPACITY>
bool WriteOnlySet<T, IndexType, CAPACITY>::erase(Index idx) {
	ASSERT(idx >= 0 && idx < CAPACITY);
	auto res = eraseImpl(idx);
	ASSERT(freeQueue.push(idx));
	return res;
}

template <class T, class IndexType, IndexType CAPACITY>
bool WriteOnlySet<T, IndexType, CAPACITY>::replace(Index idx, const Reference<T>& lineage) {
	auto lineagePtr = reinterpret_cast<uintptr_t>(lineage.getPtr());
	if (lineage.isValid()) {
		lineage->addref();
	}
	ASSERT((lineagePtr % 2) == 0); // this needs to be at least 2-byte aligned

	while (true) {
		auto ptr = _set[idx].load();
		if (ptr & LOCK) {
			_set[idx].store(lineagePtr);
			ASSERT(freeList.push(reinterpret_cast<T*>(ptr ^ LOCK)));
			return false;
		} else {
			if (_set[idx].compare_exchange_strong(ptr, lineagePtr)) {
				if (ptr) {
					reinterpret_cast<T*>(ptr)->delref();
				}
				return ptr != 0;
			}
		}
	}
}

template <class T, class IndexType, IndexType CAPACITY>
WriteOnlySet<T, IndexType, CAPACITY>::WriteOnlySet() : _set(CAPACITY) {
	// insert the free indexes in reverse order
	for (unsigned i = CAPACITY; i > 0; --i) {
		freeQueue.push(i - 1);
		std::atomic_init(&_set[i - 1], uintptr_t(0));
	}
}

template <class T, class IndexType, IndexType CAPACITY>
std::vector<Reference<T>> WriteOnlySet<T, IndexType, CAPACITY>::copy() {
	std::vector<Reference<T>> result;
	for (int i = 0; i < CAPACITY; ++i) {
		auto ptr = _set[i].load();
		if (ptr) {
			ASSERT((ptr & LOCK) == 0); // if we lock something we need to immediately unlock after we're done copying
			// We attempt lock so this won't get deleted. We will try this only once, if the other thread removed the
			// object from the set between the previews lines and now, we just won't make it part of the result.
			if (_set[i].compare_exchange_strong(ptr, ptr | LOCK)) {
				T* entry = reinterpret_cast<T*>(ptr);
				ptr |= LOCK;
				entry->addref();
				// we try to unlock now. If this element was removed while we incremented the refcount, the element will
				// end up in the freeList, so we will decrement later.
				_set[i].compare_exchange_strong(ptr, ptr ^ LOCK);
				result.push_back(Reference(entry));
			}
		}
	}
	// after we're done we need to clean up all objects that contented on a lock. This won't be perfect (as some thread
	// might not yet added the object to the free list), but whatever we don't get now we'll clean up in the next
	// iteration
	freeList.consume_all([](auto toClean) { toClean->delref(); });
	return result;
}

template <class T, class IndexType>
WriteOnlyVariable<T, IndexType>::WriteOnlyVariable() : WriteOnlySet<T, IndexType, 1>() {}

template <class T, class IndexType>
Reference<T> WriteOnlyVariable<T, IndexType>::get() {
	auto result = WriteOnlySet<T, IndexType, 1>::copy();
	return result.size() ? result.at(0) : Reference<T>();
}

template <class T, class IndexType>
bool WriteOnlyVariable<T, IndexType>::replace(const Reference<T>& element) {
	return WriteOnlySet<T, IndexType, 1>::replace(0, element);
}

// Explicit instantiation
template class WriteOnlySet<ActorLineage, unsigned, 1024>;
template class WriteOnlyVariable<ActorLineage, unsigned>;

// testing code
namespace {

// Some statistics
std::atomic<unsigned long> instanceCounter = 0;
std::atomic<unsigned long> numInserts = 0;
std::atomic<unsigned long> numErase = 0;
std::atomic<unsigned long> numLockedErase = 0;
std::atomic<unsigned long> numCopied = 0;

// A simple object that counts the number of its instances. This is used to detect memory leaks.
struct TestObject {
	mutable std::atomic<unsigned> _refCount = 1;
	TestObject() { instanceCounter.fetch_add(1); }
	void delref() const {
		if (--_refCount == 0) {
			delete this;
			--instanceCounter;
		}
	}
	void addref() const { ++_refCount; }
};

using TestSet = WriteOnlySet<TestObject, unsigned, 128>;
using Clock = std::chrono::steady_clock;

// An actor that can join a set of threads in an async way.
ACTOR Future<Void> threadjoiner(std::shared_ptr<std::vector<std::thread>> threads, std::shared_ptr<TestSet> set) {
	loop {
		wait(delay(0.1));
		for (unsigned i = 0;;) {
			if (threads->size() == i) {
				break;
			}
			auto& t = (*threads)[i];
			if (t.joinable()) {
				t.join();
				if (i + 1 < threads->size()) {
					std::swap(*threads->rbegin(), (*threads)[i]);
				}
				threads->pop_back();
			} else {
				++i;
			}
		}
		if (threads->empty()) {
			set->copy();
			ASSERT(instanceCounter.load() == 0);
			return Void();
		}
	}
}

// occasionally copy the contents of the past set.
void testCopier(std::shared_ptr<TestSet> set, std::chrono::seconds runFor) {
	auto start = Clock::now();
	while (true) {
		if (Clock::now() - start > runFor) {
			return;
		}
		auto copy = set->copy();
		numCopied.fetch_add(copy.size());
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

// In a loop adds and removes a set of objects to the set
void writer(std::shared_ptr<TestSet> set, std::chrono::seconds runFor) {
	auto start = Clock::now();
	std::random_device rDev;
	DeterministicRandom rnd(rDev());
	while (true) {
		unsigned inserts = 0, erases = 0;
		if (Clock::now() - start > runFor) {
			return;
		}
		std::vector<TestSet::Index> positions;
		for (int i = 0; i < rnd.randomInt(1, 101); ++i) {
			Reference<TestObject> o(new TestObject());
			auto pos = set->insert(o);
			if (pos == TestSet::npos) {
				// could not insert -- ignore
				break;
			}
			++inserts;
			ASSERT(pos < TestSet::capacity);
			positions.push_back(pos);
		}
		rnd.randomShuffle(positions);
		for (auto p : positions) {
			if (!set->erase(p)) {
				++numLockedErase;
			}
			++erases;
		}
		numInserts.fetch_add(inserts);
		numErase.fetch_add(erases);
		ASSERT(inserts == erases);
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

// This unit test creates 5 writer threads and one copier thread.
TEST_CASE("/flow/WriteOnlySet") {
	if (g_network->isSimulated()) {
		// This test is not deterministic, so we shouldn't run it in simulation
		return Void();
	}
	auto set = std::make_shared<TestSet>();
	auto threads = std::make_shared<std::vector<std::thread>>();
	std::chrono::seconds runFor(10);
	for (int i = 0; i < 5; ++i) {
		threads->emplace_back([set, runFor]() { writer(set, runFor); });
	}
	threads->emplace_back([set, runFor]() { testCopier(set, runFor); });
	wait(threadjoiner(threads, set));
	TraceEvent("WriteOnlySetTestResult")
	    .detail("Inserts", numInserts.load())
	    .detail("Erases", numErase.load())
	    .detail("Copies", numCopied.load())
	    .detail("LockedErase", numLockedErase.load());
	return Void();
}
} // namespace
#endif
