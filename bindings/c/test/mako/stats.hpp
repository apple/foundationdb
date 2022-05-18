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
#include "operations.hpp"
#include "time.hpp"
#include "ddsketch.hpp"
#include "fdbclient/rapidjson/document.h"
#include "fdbclient/rapidjson/rapidjson.h"
#include "fdbclient/rapidjson/stringbuffer.h"
#include "fdbclient/rapidjson/writer.h"

namespace mako {

class DDSketchMako : public DDSketch<uint64_t> {
public:
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
		writer.String("sum");
		writer.Uint64(sum);

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
		sum = obj["sum"].GetUint64();

		auto jsonBuckets = obj["buckets"].GetArray();
		uint64_t idx = 0;
		for (auto it = jsonBuckets.Begin(); it != jsonBuckets.End(); it++) {
			buckets[idx] = it->GetUint64();
			idx++;
		}
	}
};
class LatencySampleBin {
	DDSketchMako sketch;

public:
	void put(timediff_t td) {
		const auto latency_us = toIntegerMicroseconds(td);
		sketch.addSample(latency_us);
	}

	// iterate & apply for each block user function void(uint64_t const*, size_t)
	void writeToFile(const std::string& filename) const {
		rapidjson::StringBuffer ss;
		rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
		sketch.serialize(writer);
		std::ofstream f(filename);
		f << ss.GetString();
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
