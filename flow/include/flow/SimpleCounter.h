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
#include <vector>

#include "flow/Error.h"
#include "flow/Trace.h"

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
// light weight.  `makeCounter` can be called in constructors of global objects.
//
// If you want to use hierarchical metric names (e.g., '/'-separated
// components), please use ALL LOWER CASE METRIC NAMES AS PER THE EXAMPLE ABOVE.
// This enables the implementation to smuggle path component
// separators into the trace output by replacing path separaters like '/' with
// carefully chosen capital letters.  This obtains compatibility with current FDB
// "field name" naming conventions.
//
// In the future we might replace '/' with '_' to obtain Prometheus-compatible
// metric names that don't actually look terrible.
//
// If you don't want to use hierarchical metric names, then your counter
// names should be ReallyVerboseConcatenatedNamesWithCaps and must be globally
// unique.
//
// SimpleCounter<T>* returned by `makeCounter` are intended to live for the
// duration of the process, i.e. they are not intended to be freed/destroyed.
//
// Counters are periodically logged as "SimpleCounters".
//
// More background: https://quip-apple.com/PyfZA6Qkbc7w
//
// Caveat: if you allocate two different counters with the same name, they will
// accumulate updates independently. You probably don't want to do that.
//
// FIXME: add support for metric labels.  This can be done by letting the
// template take 0 or more additional string typed arguments which represent
// label dimension names. The increment() API below would require that the
// same number of string-valued arguments (or arguments convertable to string)
// be provided and would remember those as labels.

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

	// Protects the static object returned by counters() below.
	// https://chatgpt.com/share/68acec3c-21d4-800b-b315-ff6fc45ec806
	// explains why this is necessary.
	static inline std::mutex& mutex() {
		static std::mutex m;
		return m;
	}
	static inline std::vector<SimpleCounter<T>*>& counters() {
		static std::vector<SimpleCounter<T>*> v;
		return v;
	}

public:
	// Defined in template instantiations below.
	inline void increment(T delta);

	inline T get(void) const { return value.load(); }

	inline const std::string& name(void) const { return name_; }

	static inline SimpleCounter<T>* makeCounter(std::string_view name) {
		SimpleCounter<T>* rv = new SimpleCounter<T>(name);

		std::lock_guard<std::mutex> lock(mutex());
		std::vector<SimpleCounter<T>*>& v = counters();
		v.push_back(rv);

		return rv;
	}

	static inline std::vector<SimpleCounter<T>*> getCounters() {
		std::vector<SimpleCounter<T>*> rv;
		std::lock_guard<std::mutex> lock(mutex());
		rv = counters();
		return rv;
	}
};

template <>
inline void SimpleCounter<int64_t>::increment(int64_t delta) {
	value.fetch_add(delta, std::memory_order_relaxed);
}

// Newer versions of C++ allow `fetch_add` on double, but older
// versions don't. This is not expected to cause performance issues in
// practice.
template <>
inline void SimpleCounter<double>::increment(double delta) {
	double old = value.load();
	while (!value.compare_exchange_weak(old, old + delta)) {
		;
	}
}

void simpleCounterReport(Severity severity = SevInfo);

#endif
