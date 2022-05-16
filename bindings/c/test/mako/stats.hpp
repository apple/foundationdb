/*
 * stats.hpp
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

#ifndef MAKO_STATS_HPP
#define MAKO_STATS_HPP

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <list>
#include <new>
#include <utility>
#include "fdbclient/rapidjson/document.h"
#include "fdbclient/rapidjson/rapidjson.h"
#include "fdbclient/rapidjson/stringbuffer.h"
#include "fdbclient/rapidjson/writer.h"
#include "operations.hpp"
#include "time.hpp"

namespace mako {

/* rough cap on the number of samples to avoid OOM hindering benchmark */
constexpr const size_t SAMPLE_CAP = 2000000;

/* size of each block to get detailed latency for each operation */
constexpr const size_t LAT_BLOCK_SIZE = 4093;

/* hard cap on the number of sample blocks = 488 */
constexpr const size_t MAX_LAT_BLOCKS = SAMPLE_CAP / LAT_BLOCK_SIZE;

/* memory block allocated to each operation when collecting detailed latency */
class LatencySampleBlock {
	uint64_t samples[LAT_BLOCK_SIZE]{
		0,
	};
	uint64_t index{ 0 };

public:
	LatencySampleBlock() noexcept = default;
	bool full() const noexcept { return index >= LAT_BLOCK_SIZE; }
	void put(timediff_t td) {
		assert(!full());
		samples[index++] = toIntegerMicroseconds(td);
	}
	// return {data block, number of samples}
	std::pair<uint64_t const*, size_t> data() const noexcept { return { samples, index }; }
};

/*
    Collect sampled latencies according to DDSketch paper:
    https://arxiv.org/pdf/1908.10693.pdf
 */
class DDSketch {
private:
	double errorGuarantee;
	std::vector<uint64_t> buckets;
	uint64_t minValue, maxValue, populationSize, zeroPopulationSize;
	double gamma;
	int offset;
	constexpr static double EPSILON = 1e-18;

	int getIdx(uint64_t sample) const noexcept { return ceil(log(sample) / log(gamma)); }

	double getVal(int idx) const noexcept { return (2.0 * pow(gamma, idx)) / (1 + gamma); }

public:
	DDSketch(double err = 0.05)
	  : errorGuarantee(err), minValue(std::numeric_limits<uint64_t>::max()),
	    maxValue(std::numeric_limits<uint64_t>::min()), populationSize(0), zeroPopulationSize(0),
	    gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)), offset(getIdx(1.0 / EPSILON)) {
		buckets.resize(2 * offset, 0);
	}

	uint64_t getPopulationSize() { return populationSize; }

	void add(uint64_t sample) {
		if (sample <= EPSILON) {
			zeroPopulationSize++;
		} else {
			int idx = getIdx(sample);
			assert(idx >= 0 && idx < int(buckets.size()));
			buckets[idx]++;
		}
		populationSize++;
		maxValue = std::max(maxValue, sample);
		minValue = std::min(minValue, sample);
	}

	double percentile(double percentile) {
		assert(percentile >= 0 && percentile <= 1);

		if (populationSize == 0) {
			return 0;
		}
		uint64_t targetPercentilePopulation = percentile * (populationSize - 1);
		// Now find the tPP-th (0-indexed) element
		if (targetPercentilePopulation < zeroPopulationSize) {
			return 0;
		}

		int index = -1;
		bool found = false;
		if (percentile <= 0.5) { // count up
			uint64_t count = zeroPopulationSize;
			for (size_t i = 0; i < buckets.size(); i++) {
				if (targetPercentilePopulation < count + buckets[i]) {
					// count + buckets[i] = # of numbers so far (from the rightmost to
					// this bucket, inclusive), so if target is in this bucket, it should
					// means tPP < cnt + bck[i]
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		} else { // and count down
			uint64_t count = 0;
			for (size_t i = buckets.size() - 1; i >= 0; i--) {
				if (targetPercentilePopulation + count + buckets[i] >= populationSize) {
					// cnt + bkt[i] is # of numbers to the right of this bucket (incl.),
					// so if target is not in this bucket (i.e., to the left of this
					// bucket), it would be as right as the left bucket's rightmost
					// number, so we would have tPP + cnt + bkt[i] < total population (tPP
					// is 0-indexed), that means target is in this bucket if this
					// condition is not satisfied.
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		}
		assert(found);
		return getVal(index);
	}

	uint64_t min() const { return minValue; }
	uint64_t max() const { return maxValue; }

	void serialize(rapidjson::Writer<rapidjson::StringBuffer>& writer) const {
		writer.StartObject();

		writer.String("errorGuarantee");
		writer.Double(errorGuarantee);
		writer.String("minValue");
		writer.Uint64(minValue);
		writer.String("maxValue");
		writer.Uint64(maxValue);
		writer.String("populationSize");
		writer.Uint64(populationSize);
		writer.String("zeroPopulationSize");
		writer.Uint64(zeroPopulationSize);
		writer.String("gamma");
		writer.Double(gamma);
		writer.String("offset");
		writer.Int(offset);
		writer.String("buckets");
		writer.StartArray();
		for (auto b : buckets) {
			writer.Uint64(b);
		}
		writer.EndArray();

		writer.EndObject();
	}

	void deserialize(const rapidjson::Value& obj) {
		errorGuarantee = obj["errorGuarantee"].GetDouble();
		minValue = obj["minValue"].GetUint64();
		maxValue = obj["maxValue"].GetUint64();
		populationSize = obj["populationSize"].GetUint64();
		zeroPopulationSize = obj["zeroPopulationSize"].GetUint64();
		gamma = obj["gamma"].GetDouble();
		offset = obj["offset"].GetInt();
		auto jsonBuckets = obj["buckets"].GetArray();
		uint64_t idx = 0;
		for (auto it = jsonBuckets.Begin(); it != jsonBuckets.End(); it++) {
			buckets[idx] = it->GetUint64();
			idx++;
		}
	}

	void merge(const DDSketch& other) {
		// what to do if we have different errorGurantees?
		maxValue = std::max(maxValue, other.maxValue);
		minValue = std::min(minValue, other.minValue);
		populationSize += other.populationSize;
		zeroPopulationSize += other.zeroPopulationSize;
		for (uint32_t i = 0; i < buckets.size(); i++) {
			buckets[i] += other.buckets[i];
		}
	}
};

/* collect sampled latencies until OOM is hit */
class LatencySampleBin {
	DDSketch sketch;

public:
	void put(timediff_t td) {
		const auto latency_us = toIntegerMicroseconds(td);
		sketch.add(latency_us);
	}

	// iterate & apply for each block user function void(uint64_t const*, size_t)
	void writeToFile(const std::string& filename) const {
		rapidjson::StringBuffer ss;
		rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
		sketch.serialize(writer);
		std::ofstream f(filename);
		f << ss.GetString();
		// std::cout << ss.GetString();
	}
};

class alignas(64) ThreadStatistics {
	uint64_t conflicts;
	uint64_t total_errors;
	uint64_t ops[MAX_OP];
	uint64_t errors[MAX_OP];
	uint64_t latency_samples[MAX_OP];
	uint64_t latency_us_total[MAX_OP];
	uint64_t latency_us_min[MAX_OP];
	uint64_t latency_us_max[MAX_OP];

public:
	ThreadStatistics() noexcept {
		memset(this, 0, sizeof(ThreadStatistics));
		memset(latency_us_min, 0xff, sizeof(latency_us_min));
	}

	ThreadStatistics(const ThreadStatistics& other) noexcept = default;
	ThreadStatistics& operator=(const ThreadStatistics& other) noexcept = default;

	uint64_t getConflictCount() const noexcept { return conflicts; }

	uint64_t getOpCount(int op) const noexcept { return ops[op]; }

	uint64_t getErrorCount(int op) const noexcept { return errors[op]; }

	uint64_t getTotalErrorCount() const noexcept { return total_errors; }

	uint64_t getLatencySampleCount(int op) const noexcept { return latency_samples[op]; }

	uint64_t getLatencyUsTotal(int op) const noexcept { return latency_us_total[op]; }

	uint64_t getLatencyUsMin(int op) const noexcept { return latency_us_min[op]; }

	uint64_t getLatencyUsMax(int op) const noexcept { return latency_us_max[op]; }

	// with 'this' as final aggregation, factor in 'other'
	void combine(const ThreadStatistics& other) {
		conflicts += other.conflicts;
		for (auto op = 0; op < MAX_OP; op++) {
			ops[op] += other.ops[op];
			errors[op] += other.errors[op];
			total_errors += other.errors[op];
			latency_samples[op] += other.latency_samples[op];
			latency_us_total[op] += other.latency_us_total[op];
			if (latency_us_min[op] > other.latency_us_min[op])
				latency_us_min[op] = other.latency_us_min[op];
			if (latency_us_max[op] < other.latency_us_max[op])
				latency_us_max[op] = other.latency_us_max[op];
		}
	}

	void incrConflictCount() noexcept { conflicts++; }

	// non-commit write operations aren't measured for time.
	void incrOpCount(int op) noexcept { ops[op]++; }

	void incrErrorCount(int op) noexcept {
		total_errors++;
		errors[op]++;
	}

	void addLatency(int op, timediff_t diff) noexcept {
		const auto latency_us = toIntegerMicroseconds(diff);
		latency_samples[op]++;
		latency_us_total[op] += latency_us;
		if (latency_us_min[op] > latency_us)
			latency_us_min[op] = latency_us;
		if (latency_us_max[op] < latency_us)
			latency_us_max[op] = latency_us;
	}
};

using LatencySampleBinArray = std::array<LatencySampleBin, MAX_OP>;

} // namespace mako

#endif /* MAKO_STATS_HPP */
