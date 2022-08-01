/*
 * IndexedSet.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_TREEBENCHMARK_H
#define FLOW_TREEBENCHMARK_H
#pragma once

#include <map>
#include <random>

#include "flow/flow.h"

struct opTimer {
	double start = timer();
	char const* name;
	int opCount;

	opTimer(char const* name, int opCount) : name(name), opCount(opCount) {}

	~opTimer() { printf("%s: %0.1f Kop/s\n", name, (opCount / 1000.0) / (timer() - start)); }
};

template <typename F, typename T>
void timedRun(char const* name, T& t, F f) {
	opTimer timer(name, t.size());
	for (auto& i : t) {
		f(i);
	}
}

template <typename K>
struct MapHarness {
	using map = std::map<K, int>;
	using key_type = K;

	struct result {
		typename map::const_iterator it;

		result(typename map::const_iterator it) : it(it) {}

		result& operator++() {
			it++;
			return *this;
		}

		const K& operator*() const { return (*it).first; }

		const K& operator->() const { return it->first; }

		bool operator==(result const& k) const { return it == k.it; }
		bool operator!=(result const& k) const { return it != k.it; }
	};

	map s;

	void insert(K const& k) { s.insert(std::pair<K, int>(k, 1)); }
	result find(K const& k) const { return result(s.find(k)); }
	result not_found() const { return result(s.end()); }
	result begin() const { return result(s.begin()); }
	result end() const { return result(s.end()); }
	result lower_bound(K const& k) const { return result(s.lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

template <typename T, typename F>
void treeBenchmark(T& tree, F generateKey) {
	std::mt19937_64 urng(deterministicRandom()->randomUInt32());

	using key = typename T::key_type;

	int keyCount = 1000000;

	std::vector<key> keys;
	for (int i = 0; i < keyCount; i++) {
		keys.push_back(generateKey());
	}

	timedRun("insert", keys, [&tree](key const& k) { tree.insert(k); });
	timedRun("find", keys, [&tree](key const& k) { ASSERT(tree.find(k) != tree.not_found()); });
	timedRun("lower_bound", keys, [&tree](key const& k) { ASSERT(tree.lower_bound(k) != tree.not_found()); });
	timedRun("upper_bound", keys, [&tree](key const& k) { tree.upper_bound(k); });

	std::sort(keys.begin(), keys.end());
	keys.resize(std::unique(keys.begin(), keys.end()) - keys.begin());

	auto iter = tree.lower_bound(*keys.begin());
	timedRun("scan", keys, [&iter](key const& k) {
		ASSERT(k == *iter);
		++iter;
	});
	ASSERT(iter == tree.end());

	timedRun("find (sorted)", keys, [&tree](key const& k) { ASSERT(tree.find(k) != tree.end()); });

	std::shuffle(keys.begin(), keys.end(), urng);

	timedRun("erase", keys, [&tree](key const& k) { tree.erase(k); });
	ASSERT(tree.begin() == tree.end());
}

static inline StringRef randomStr(Arena& arena) {
	size_t keySz = 100;
	return StringRef(arena, deterministicRandom()->randomAlphaNumeric(keySz));
}

static inline int randomInt() {
	return deterministicRandom()->randomInt(0, INT32_MAX);
}

#endif // FLOW_TREEBENCHMARK_H
