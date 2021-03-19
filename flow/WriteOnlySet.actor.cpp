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

#include "flow/DeterministicRandom.h"
#include "flow/WriteOnlySet.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"

#include <chrono>
#include <random>
#include "flow/actorcompiler.h" // has to be last include

template <class T, class IndexType, IndexType CAPACITY>
auto WriteOnlySet<T, IndexType, CAPACITY>::insert(const Reference<T>& lineage) -> Index {
	Index res;
	if (!freeQueue.pop(res)) {
		TraceEvent(SevWarnAlways, "NoCapacityInWriteOnlySet");
		return npos;
	}
	ASSERT(_set[res].load() & FREE);
	auto ptr = reinterpret_cast<uintptr_t>(lineage.getPtr());
	ASSERT((ptr % 4) == 0); // this needs to be at least 4-byte aligned
	ASSERT((ptr & FREE) == 0 && (ptr & LOCK) == 0);
	lineage->addref();
	_set[res].store(ptr);
	return res;
}

template <class T, class IndexType, IndexType CAPACITY>
void WriteOnlySet<T, IndexType, CAPACITY>::erase(Index idx) {
	while (true) {
		auto ptr = _set[idx].load();
		if (ptr & LOCK) {
			_set[idx].store(FREE);
			freeList.push(reinterpret_cast<T*>(ptr ^ LOCK));
			return;
		} else {
			if (_set[idx].compare_exchange_strong(ptr, FREE)) {
				reinterpret_cast<T*>(ptr)->delref();
				return;
			}
		}
	}
}

// Explicit instantiation
template class WriteOnlySet<ActorLineage, unsigned, 1024>;

// testing code
namespace {

std::atomic<unsigned long> instanceCounter = 0;
constexpr double iteration_frequency = 10.0;

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

void testCopier(std::shared_ptr<TestSet> set, std::chrono::seconds runFor) {
	auto start = Clock::now();
	while (true) {
		if (Clock::now() - start > runFor) {
			return;
		}
		auto copy = set->copy();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

void writer(std::shared_ptr<TestSet> set, std::chrono::seconds runFor) {
	auto start = Clock::now();
	std::random_device rDev;
	DeterministicRandom rnd(rDev());
	while (true) {
		if (Clock::now() - start > runFor) {
			return;
		}
		std::vector<TestSet::Index> positions;
		for (int i = 0; i < rnd.randomInt(1, 101); ++i) {
			positions.push_back(set->insert(Reference<TestObject>(new TestObject())));
		}
		rnd.randomShuffle(positions);
		for (auto p : positions) {
			set->erase(p);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

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
	return Void();
}
} // namespace