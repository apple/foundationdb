/*
 * native_latency.hpp
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

#ifndef MAKO_NATIVE_LATENCY_HPP
#define MAKO_NATIVE_LATENCY_HPP

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <optional>

namespace mako {

// Each worker owns its counters. The stats process reads them across fork, without
// touching the worker's private DDSketch, at about 1% relative bucket resolution.
class alignas(64) NativeLatencyHistogram {
public:
	static constexpr int bucketCount = 64 * 32 + 1;
	static constexpr int operationCount = 2;

	NativeLatencyHistogram() noexcept {
		for (auto& operation : buckets) {
			for (auto& bucket : operation) {
				bucket.store(0, std::memory_order_relaxed);
			}
		}
	}

	void add(int operation, uint64_t microseconds) noexcept {
		if (operation < 0 || operation >= operationCount) {
			return;
		}
		const auto index = microseconds == 0
		                       ? 0
		                       : std::min(bucketCount - 1, 1 + static_cast<int>(std::log2(microseconds) * 64));
		buckets[operation][index].fetch_add(1, std::memory_order_relaxed);
	}

	uint64_t bucket(int operation, int index) const noexcept {
		return buckets[operation][index].load(std::memory_order_relaxed);
	}

private:
	std::array<std::array<std::atomic<uint64_t>, bucketCount>, operationCount> buckets;
};

class NativeLatencySnapshot {
public:
	void merge(const NativeLatencyHistogram& histogram) noexcept {
		for (int op = 0; op < NativeLatencyHistogram::operationCount; ++op) {
			for (int index = 0; index < NativeLatencyHistogram::bucketCount; ++index) {
				const auto count = histogram.bucket(op, index);
				buckets[op][index] += count;
				counts[op] += count;
			}
		}
	}

	uint64_t samples(int operation) const noexcept { return counts[operation]; }

	std::optional<uint64_t> percentile(int operation, double quantile) const noexcept {
		if (counts[operation] == 0) {
			return std::nullopt;
		}
		const auto rank = static_cast<uint64_t>(quantile * (counts[operation] - 1));
		auto encountered = uint64_t{};
		for (int index = 0; index < NativeLatencyHistogram::bucketCount; ++index) {
			encountered += buckets[operation][index];
			if (encountered > rank) {
				return index == 0 ? 0 : static_cast<uint64_t>(std::round(std::exp2((index - 1) / 64.0)));
			}
		}
		return std::nullopt;
	}

private:
	std::array<std::array<uint64_t, NativeLatencyHistogram::bucketCount>, NativeLatencyHistogram::operationCount>
	    buckets{};
	std::array<uint64_t, NativeLatencyHistogram::operationCount> counts{};
};

} // namespace mako

#endif /* MAKO_NATIVE_LATENCY_HPP */
