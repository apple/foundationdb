/*
 * FeistelPermutation.cpp
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

#include <algorithm>
#include <array>
#include <cmath>
#include <map>
#include <numeric>
#include <vector>

#include "flow/DeterministicRandom.h"
#include "flow/FeistelPermutation.h"
#include "flow/UnitTest.h"

TEST_CASE("/flow/FeistelPermutation/bijection") {
	for (uint64_t const n : { 1, 2, 3, 4, 5, 17, 20, 21, 1000, 1024, 4097 }) {
		FeistelPermutation const perm(n, 0x9c5b);
		std::vector<bool> seen(n, false);
		for (uint64_t i = 0; i < n; i++) {
			uint64_t const p = perm(i);
			ASSERT(p < n);
			ASSERT(!seen[p]);
			seen[p] = true;
		}
	}

	// The same seed gives the same order and a different seed a different one, for both the ranked small-domain path
	// and the Feistel path
	for (uint64_t const n : { 20, 1000 }) {
		FeistelPermutation const a(n, 1), b(n, 1), c(n, 2);
		bool differs = false;
		for (uint64_t i = 0; i < n; i++) {
			ASSERT_EQ(a(i), b(i));
			differs |= a(i) != c(i);
		}
		ASSERT(differs);
	}
	return Void();
}

namespace {

// Every ordering of a small domain should come up about equally often across seeds
template <uint64_t Length>
void checkFeistelPermutationDistribution() {
	constexpr uint64_t sampleCount = 5000;
	std::map<std::array<uint64_t, Length>, uint64_t> orderingCounts;
	std::array<uint64_t, Length> order{};
	std::iota(order.begin(), order.end(), uint64_t{ 0 });
	do {
		orderingCounts[order] = 0;
	} while (std::next_permutation(order.begin(), order.end()));

	DeterministicRandom seedRandom(0x9c5b);
	for (uint64_t i = 0; i < sampleCount; ++i) {
		FeistelPermutation const perm(Length, seedRandom.randomUInt64());
		for (uint64_t j = 0; j < Length; ++j) {
			order[j] = perm(j);
		}
		++orderingCounts[order];
	}

	uint64_t const expectedCount = sampleCount / orderingCounts.size();
	// 5 binomial standard deviations, so the bound doesn't trip on noise as the number of orderings grows
	double const bucketProbability = 1.0 / orderingCounts.size();
	auto const errorMargin =
	    static_cast<uint64_t>(5.0 * std::sqrt(sampleCount * bucketProbability * (1.0 - bucketProbability)));
	for (auto const& [ordering, count] : orderingCounts) {
		ASSERT_GT(count, expectedCount - errorMargin);
		ASSERT_LT(count, expectedCount + errorMargin);
	}
}

} // namespace

TEST_CASE("/flow/FeistelPermutation/distribution") {
	checkFeistelPermutationDistribution<3>();
	checkFeistelPermutationDistribution<4>();
	return Void();
}
