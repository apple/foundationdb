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
#include <istream>
#include <limits>
#include <list>
#include <new>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include "mako/mako.hpp"
#include "operations.hpp"
#include "time.hpp"
#include "ddsketch.hpp"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <iostream>
#include <sstream>
#include <vector>

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

inline void populateArray(std::array<uint64_t, MAX_OP>& arr,
                          rapidjson::GenericArray<true, rapidjson::GenericValue<rapidjson::UTF8<>>>& json) {
	uint64_t idx = 0;
	for (auto it = json.Begin(); it != json.End(); it++) {
		arr[idx] = it->GetUint64();
		idx++;
	}
}

class alignas(64) ThreadStatistics {
	uint64_t conflicts;
	uint64_t total_errors;
	std::array<uint64_t, MAX_OP> ops;
	std::array<uint64_t, MAX_OP> errors;
	std::array<uint64_t, MAX_OP> latency_samples;
	std::array<uint64_t, MAX_OP> latency_us_total;
	std::unordered_map<int, DDSketchMako> sketches;
	std::string txn_name;

public:
	ThreadStatistics() noexcept {
		memset(this, 0, sizeof(ThreadStatistics));
		sketches = std::unordered_map<int, DDSketchMako>{};
		txn_name = std::string{};
	}

	ThreadStatistics(const std::string& test_name) : txn_name{ test_name } {}

	ThreadStatistics(const Arguments& args) noexcept {
		memset(this, 0, sizeof(ThreadStatistics));
		txn_name = args.txn_name;
		sketches = std::unordered_map<int, DDSketchMako>{};
		for (int op = 0; op < MAX_OP; op++) {
			if (args.txnspec.ops[op][OP_COUNT] > 0) {
				// This will allocate and initialize an empty sketch
				sketches[op];
			}
		}
		// These always need to be created since they are reported
		sketches[OP_COMMIT];
		sketches[OP_TRANSACTION];
	}

	ThreadStatistics(const ThreadStatistics& other) = default;
	ThreadStatistics& operator=(const ThreadStatistics& other) = default;

	const std::string& getTestName() const noexcept { return txn_name; }

	uint64_t getConflictCount() const noexcept { return conflicts; }

	uint64_t getOpCount(int op) const noexcept { return ops[op]; }

	uint64_t getErrorCount(int op) const noexcept { return errors[op]; }

	uint64_t getTotalErrorCount() const noexcept { return total_errors; }

	uint64_t getLatencySampleCount(int op) const noexcept { return latency_samples[op]; }

	uint64_t getLatencyUsTotal(int op) const noexcept { return latency_us_total[op]; }

	uint64_t getLatencyUsMin(int op) const noexcept {
		auto it = sketches.find(op);
		if (it != sketches.end()) {
			return it->second.min();
		}
		return 0;
	}

	uint64_t getLatencyUsMax(int op) const noexcept {
		auto it = sketches.find(op);
		if (it != sketches.end()) {
			return it->second.max();
		}
		return 0;
	}

	uint64_t percentile(int op, double quantile) { return sketches[op].percentile(quantile); }

	uint64_t mean(int op) const noexcept {
		auto it = sketches.find(op);
		if (it != sketches.end()) {
			return it->second.mean();
		}
		return 0;
	}

	// with 'this' as final aggregation, factor in 'other'
	void combine(const ThreadStatistics& other) {
		conflicts += other.conflicts;
		for (auto op = 0; op < MAX_OP; op++) {
			auto it = other.sketches.find(op);
			if (it != other.sketches.end()) {
				sketches[op].mergeWith(it->second);
			}
			ops[op] += other.ops[op];
			errors[op] += other.errors[op];
			total_errors += other.errors[op];
			latency_samples[op] += other.latency_samples[op];
			latency_us_total[op] += other.latency_us_total[op];
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
		if (sketches.find(op) != sketches.end()) {
			sketches[op].addSample(latency_us);
		}
		latency_us_total[op] += latency_us;
	}

	void writeToFile(const std::string& filename, int op) const {
		auto it = sketches.find(op);
		if (it != sketches.end()) {
			rapidjson::StringBuffer ss;
			rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
			it->second.serialize(writer);
			std::ofstream f(filename);
			f << ss.GetString();
		}
	}

	void updateLatencies(const std::unordered_map<int, DDSketchMako>& other_sketches) {
		sketches = std::move(other_sketches);
	}

	void serializeFields(rapidjson::Writer<rapidjson::StringBuffer>& writer) {
		writer.String(txn_name.c_str());
		writer.StartObject();
		writer.String("conflicts");
		writer.Uint64(conflicts);
		writer.String("total_errors");
		writer.Uint64(total_errors);

		writer.String("ops");
		writer.StartArray();
		for (auto op = 0; op < MAX_OP; op++) {
			writer.Uint64(ops[op]);
		}
		writer.EndArray();

		writer.String("errors");
		writer.StartArray();
		for (auto op = 0; op < MAX_OP; op++) {
			writer.Uint64(errors[op]);
		}
		writer.EndArray();

		writer.String("latency_samples");
		writer.StartArray();
		for (auto op = 0; op < MAX_OP; op++) {
			writer.Uint64(latency_samples[op]);
		}
		writer.EndArray();

		writer.String("latency_us_total");
		writer.StartArray();
		for (auto op = 0; op < MAX_OP; op++) {
			writer.Uint64(latency_us_total[op]);
		}
		writer.EndArray();

		for (auto op = 0; op < MAX_OP; op++) {
			if (sketches.find(op) != sketches.end()) {
				std::string op_name = getOpName(op);
				writer.String(op_name.c_str());
				sketches[op].serialize(writer);
			}
		}
		writer.EndObject();
	}

	void serialize(rapidjson::Writer<rapidjson::StringBuffer>& writer) {
		writer.StartObject();
		serializeFields(writer);
		writer.EndObject();
	}

	void deserialize(const rapidjson::Value& obj, const std::string& test_name) {
		txn_name = test_name;
		conflicts = obj["conflicts"].GetUint64();
		total_errors = obj["total_errors"].GetUint64();

		auto jsonOps = obj["ops"].GetArray();
		auto jsonErrors = obj["errors"].GetArray();
		auto jsonLatencySamples = obj["latency_samples"].GetArray();
		auto jsonLatencyUsTotal = obj["latency_us_total"].GetArray();

		populateArray(ops, jsonOps);
		populateArray(errors, jsonErrors);
		populateArray(latency_samples, jsonLatencySamples);
		populateArray(latency_us_total, jsonLatencyUsTotal);
		for (int op = 0; op < MAX_OP; op++) {
			if (latency_samples[op] > 0) {
				const std::string op_name = getOpName(op);
				sketches[op].deserialize(obj[op_name.c_str()]);
			}
		}
	}
	friend std::ofstream& operator<<(std::ofstream& os, ThreadStatistics& stats);
};

inline std::ofstream& operator<<(std::ofstream& os, ThreadStatistics& stats) {
	rapidjson::StringBuffer ss;
	rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
	stats.serialize(writer);
	os << ss.GetString();
	return os;
}

/*
    This class will hold all of the statistics information of mako runs in
    in a map. The map is of the form:
        "transaction name" -> stats data
    where "transaction name" is the argument to --transaction when mako was ran.

    For example: grvgr16:1024, g8iu, etc...
*/
class TestStatisticsCollection {
	std::unordered_map<std::string, ThreadStatistics> sketches;

public:
	TestStatisticsCollection() {}
	void add(const ThreadStatistics& stats) { sketches[stats.getTestName()] = stats; }

	void combine(const TestStatisticsCollection& other) {
		for (auto& p : other.sketches) {
			auto it = sketches.find(p.first);
			if (it != sketches.end()) {
				sketches[p.first].combine(p.second);
			} else {
				sketches[p.first] = p.second;
			}
		}
	}

	// These two methods allow us to use ranged based for loops
	std::unordered_map<std::string, ThreadStatistics>::iterator begin() { return sketches.begin(); }
	std::unordered_map<std::string, ThreadStatistics>::iterator end() { return sketches.end(); }

	// Same as above, but for const
	std::unordered_map<std::string, ThreadStatistics>::const_iterator cbegin() { return sketches.cbegin(); }
	std::unordered_map<std::string, ThreadStatistics>::const_iterator cend() { return sketches.cend(); }

	// These are necessary to make I/O with streams work nicely
	friend std::ofstream& operator<<(std::ofstream& os, TestStatisticsCollection& collection);
	friend std::ifstream& operator>>(std::ifstream& is, TestStatisticsCollection& collection);
};

inline std::ifstream& operator>>(std::ifstream& is, TestStatisticsCollection& collection) {
	std::stringstream buffer;
	buffer << is.rdbuf();
	rapidjson::Document doc;
	doc.Parse(buffer.str().c_str());

	for (auto& m : doc.GetObject()) {
		ThreadStatistics tmp;
		tmp.deserialize(m.value, m.name.GetString());
		collection.sketches[m.name.GetString()] = std::move(tmp);
	}
	return is;
}

inline std::ofstream& operator<<(std::ofstream& os, TestStatisticsCollection& collection) {
	rapidjson::StringBuffer ss;
	rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
	writer.StartObject();
	for (auto& p : collection.sketches) {
		p.second.serializeFields(writer);
	}
	writer.EndObject();
	os << ss.GetString();
	return os;
}

} // namespace mako

#endif /* MAKO_STATS_HPP */
