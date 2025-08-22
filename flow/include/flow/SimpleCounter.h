/*
 * SimpleCounter.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_SIMPLECOUNTER_H
#define FLOW_SIMPLECOUNTER_H
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "flow/Error.h"

// SimpleCounter metrics class for atomic counters of int64_t or
// double.  Example usage:
//
// static SimpleCounter<int64_t> *foo = SimpleCounter::makeCounter("/mymodule/foo");
//
// if (...) {
//   // condition of interest
//   foo->increment(1);
//   ...
// }
//
// This class is thread safe, i.e. can be used by code which does limited-scope
// synchronous work in side threads, but is intended to generally be very
// light weight.
//
// FIXME: periodically log the counters, and give the name of the TraceEvent
// where they can be found.
//
// More background: https://quip-apple.com/PyfZA6Qkbc7w
//
// Caveat: if you allocate two different counters with the same name, they will
// accumulate updates independently. You probably don't want to do that.
//
// FIXME: add support for metric labels.  This needs some type of
// variadic template to let callers specify a set of <type, name> tuples
// for each label dimension.  Alternatively, make all label types be
// strings and have users specify a set of 0 or more label dimension names.
//
// FIXME: add support for gauges.

template <class T>
class SimpleCounter {
	static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, double>, "T must be int64_t or double");

private:
	SimpleCounter(std::string_view n) : value(T(0)), name_(n) {}

	// Not copyable or movable.
	SimpleCounter(const SimpleCounter&) = delete;
	SimpleCounter& operator=(const SimpleCounter&) = delete;
	SimpleCounter(SimpleCounter&&) = delete;
	SimpleCounter& operator=(SimpleCounter&&) = delete;

private:
	std::atomic<T> value;
	std::string name_;

	static inline std::mutex mutex; // protects 3 static values below
	// Track all counters of type T.  This isn't a std::vector because
	// 1) it's static, and 2) we want no dependency on order of initialization
	// of static objects.  These members are not objects, they are statically
	// initialized plain old data.  Thus we can count on them being initialized
	// from the start of the process.
	static inline int numCounters{ 0 };
	static inline int allCountersSize{ 0 };
	static inline SimpleCounter<T>** allCounters{ nullptr };

public:
	// Defined in template instantiations below.
	void increment(T delta);

	T get(void) { return value.load(); }

	const std::string& name(void) { return name_; }

	static SimpleCounter<T>* makeCounter(std::string_view name) {
		SimpleCounter<T>* rv = new SimpleCounter<T>(name);

		std::lock_guard<std::mutex> lock(mutex);
		ASSERT(numCounters <= allCountersSize);
		if (numCounters == allCountersSize) {
			if (allCountersSize == 0) {
				allCountersSize = 64;
			} else {
				allCountersSize *= 2;
			}
			// printf("allCountersSize now [%d]; numCounters now [%d]\n", allCountersSize, numCounters);
			allCounters =
			    static_cast<SimpleCounter<T>**>(realloc(allCounters, allCountersSize * sizeof(SimpleCounter<T>*)));
		}
		ASSERT(numCounters < allCountersSize);
		allCounters[numCounters++] = rv;

		return rv;
	}

	static std::vector<SimpleCounter<T>*> getCounters() {
		std::vector<SimpleCounter<T>*> rv;
		std::lock_guard<std::mutex> lock(mutex);
		rv.reserve(numCounters);
		for (int i = 0; i < numCounters; i++) {
			rv.push_back(allCounters[i]);
		}
		return rv;
	}

	// FIXME: actually use this interface to log the metrics somewhere.
};

template <>
void SimpleCounter<int64_t>::increment(int64_t delta) {
	value.fetch_add(delta, std::memory_order_relaxed);
}

template <>
void SimpleCounter<double>::increment(double delta) {
	double old = value.load();
	while (!value.compare_exchange_weak(old, old + delta)) {
		;
	}
}

#endif
