/*
 * FeistelPermutation.h
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

#pragma once

#include "flow/Error.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <numeric>

// A deterministic pseudo-random bijection on [0, n) using O(1) memory. Small domains use seed-ranked permutations;
// larger domains use cycle walking to keep the Feistel permutation's outputs inside [0, n).
class FeistelPermutation {
public:
	FeistelPermutation(uint64_t n, uint64_t seed) : n_(n) {
		ASSERT(n > 0);
		if (n <= maxSmallDomain) {
			uint64_t const permutationCount = factorial(n);
			uint64_t const rejectionThreshold = (uint64_t{ 0 } - permutationCount) % permutationCount;
			uint64_t value = mix(seed);
			while (value < rejectionThreshold) {
				seed += splitmixGamma;
				value = mix(seed);
			}
			uint64_t rank = value % permutationCount;
			std::array<uint64_t, maxSmallDomain> available{};
			std::iota(available.begin(), available.begin() + n_, uint64_t{ 0 });
			for (uint64_t position = 0; position < n_; ++position) {
				uint64_t const remaining = n_ - position;
				uint64_t const divisor = factorial(remaining - 1);
				uint64_t const digit = rank / divisor;
				rank %= divisor;
				smallPermutation_[position] = available[digit];
				std::copy(available.begin() + digit + 1, available.begin() + remaining, available.begin() + digit);
			}
			return;
		}
		int bits = 2;
		while (bits < 64 && (uint64_t{ 1 } << bits) < n) {
			bits += 2;
		}
		halfBits_ = bits / 2;
		halfMask_ = (uint64_t{ 1 } << halfBits_) - 1;
		std::ranges::generate(roundKeys_, [&seed] { return seed = mix(seed); });
	}

	uint64_t operator()(uint64_t i) const {
		ASSERT(i < n_);
		if (n_ <= maxSmallDomain) {
			return smallPermutation_[i];
		}
		uint64_t x = encrypt(i);
		while (x >= n_) {
			x = encrypt(x);
		}
		return x;
	}

private:
	// 20! fits in uint64_t; rank small permutations to avoid tiny Feistel domains.
	static constexpr uint64_t maxSmallDomain = 20;

	static uint64_t factorial(uint64_t n) {
		uint64_t result = 1;
		for (uint64_t i = 2; i <= n; ++i) {
			result *= i;
		}
		return result;
	}

	// Standard splitmix64 constants
	static constexpr uint64_t splitmixGamma = 0x9e3779b97f4a7c15ULL;
	static constexpr uint64_t splitmixMul1 = 0xbf58476d1ce4e5b9ULL;
	static constexpr uint64_t splitmixMul2 = 0x94d049bb133111ebULL;

	// splitmix64 finalizer
	static uint64_t mix(uint64_t x) {
		x += splitmixGamma;
		x = (x ^ (x >> 30)) * splitmixMul1;
		x = (x ^ (x >> 27)) * splitmixMul2;
		return x ^ (x >> 31);
	}

	uint64_t encrypt(uint64_t x) const {
		uint64_t l = x >> halfBits_;
		uint64_t r = x & halfMask_;
		for (uint64_t const k : roundKeys_) {
			uint64_t const f = mix(r ^ k) & halfMask_;
			uint64_t const newR = l ^ f;
			l = r;
			r = newR;
		}
		return (l << halfBits_) | r;
	}

	uint64_t n_;
	std::array<uint64_t, maxSmallDomain> smallPermutation_{};
	int halfBits_{ 0 };
	uint64_t halfMask_{ 0 };
	std::array<uint64_t, 4> roundKeys_{};
};
