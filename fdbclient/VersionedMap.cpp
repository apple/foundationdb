/*
 * VersionedMap.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/VersionedMap.h"
#include "flow/TreeBenchmark.h"
#include "flow/UnitTest.h"

template <typename K>
struct VersionedMapHarness {
	using map = VersionedMap<K, int>;
	using key_type = K;

	struct result {
		typename map::iterator it;

		result(typename map::iterator it) : it(it) {}

		result& operator++() {
			++it;
			return *this;
		}

		const K& operator*() const { return it.key(); }

		const K& operator->() const { return it.key(); }

		bool operator==(result const& k) const { return it == k.it; }
		bool operator!=(result const& k) const { return !(*this == k); }
	};

	map s;

	void insert(K const& k) { s.insert(k, 1); }
	result find(K const& k) const { return result(s.atLatest().find(k)); }
	result not_found() const { return result(s.atLatest().end()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(s.atLatest().end()); }
	result lower_bound(K const& k) const { return result(s.atLatest().lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.atLatest().upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

TEST_CASE("performance/map/int/VersionedMap") {
	VersionedMapHarness<int> tree;

	treeBenchmark(tree, *randomInt);

	return Void();
}

TEST_CASE("performance/map/StringRef/VersionedMap") {
	Arena arena;
	VersionedMapHarness<StringRef> tree;

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

static void printKOps(const char* name, int64_t opCount, double elapsed) {
	printf("%s: %0.3f Kop/s\n", name, (opCount / 1000.0) / elapsed);
}

TEST_CASE("performance/multiversion/map/int/VersionedMap") {
	constexpr int initialKeys = 20000;
	constexpr int versions = 50000;
	constexpr int retainedVersions = 5000;
	constexpr int forgetEvery = 1000;
	constexpr int scanWidth = 8;

	VersionedMap<int, int> map;
	map.createNewVersion(1);

	double populateElapsed = 0;
	double createElapsed = 0;
	double latestInsertElapsed = 0;
	double latestEraseElapsed = 0;
	double historicalFindElapsed = 0;
	double historicalLowerBoundElapsed = 0;
	double historicalScanElapsed = 0;
	double compactForgetElapsed = 0;

	int64_t populateOps = initialKeys;
	int64_t createOps = 0;
	int64_t latestInsertOps = 0;
	int64_t latestEraseOps = 0;
	int64_t historicalFindOps = 0;
	int64_t historicalLowerBoundOps = 0;
	int64_t historicalScanOps = 0;
	int64_t compactForgetOps = 0;

	double start = timer();
	for (int key = 0; key < initialKeys; ++key) {
		map.insert(key, key);
	}
	populateElapsed += timer() - start;

	for (int version = 2; version <= versions; ++version) {
		start = timer();
		map.createNewVersion(version);
		createElapsed += timer() - start;
		++createOps;

		start = timer();
		map.insert(version % initialKeys, version);
		map.insert(initialKeys + version, version);
		latestInsertElapsed += timer() - start;
		latestInsertOps += 2;

		if (version > retainedVersions + 1) {
			start = timer();
			map.erase(initialKeys + version - retainedVersions);
			latestEraseElapsed += timer() - start;
			++latestEraseOps;
		}

		Version readVersion = std::max<Version>(map.getOldestVersion(), version - retainedVersions / 2);
		readVersion = std::max<Version>(readVersion, 1);

		start = timer();
		auto historicalFind = map.at(readVersion).find(int(readVersion % initialKeys));
		ASSERT(historicalFind);
		historicalFindElapsed += timer() - start;
		++historicalFindOps;

		start = timer();
		auto historicalLower = map.at(readVersion).lower_bound(int((readVersion * 17) % initialKeys));
		ASSERT(historicalLower);
		historicalLowerBoundElapsed += timer() - start;
		++historicalLowerBoundOps;

		start = timer();
		auto scan = map.at(readVersion).lower_bound(int((readVersion * 31) % (initialKeys - scanWidth)));
		for (int i = 0; i < scanWidth; ++i) {
			ASSERT(scan);
			++scan;
			++historicalScanOps;
		}
		historicalScanElapsed += timer() - start;

		if (version > retainedVersions && version % forgetEvery == 0) {
			Version cutoff = version - retainedVersions;
			start = timer();
			map.compact(cutoff);
			map.forgetVersionsBefore(cutoff);
			compactForgetElapsed += timer() - start;
			compactForgetOps += 2;
		}
	}

	printKOps("multiversion/populate", populateOps, populateElapsed);
	printKOps("multiversion/create_version", createOps, createElapsed);
	printKOps("multiversion/latest_insert", latestInsertOps, latestInsertElapsed);
	printKOps("multiversion/latest_erase", latestEraseOps, latestEraseElapsed);
	printKOps("multiversion/historical_find", historicalFindOps, historicalFindElapsed);
	printKOps("multiversion/historical_lower_bound", historicalLowerBoundOps, historicalLowerBoundElapsed);
	printKOps("multiversion/historical_scan", historicalScanOps, historicalScanElapsed);
	printKOps("multiversion/compact_forget", compactForgetOps, compactForgetElapsed);

	return Void();
}

void forceLinkVersionedMapTests() {}
