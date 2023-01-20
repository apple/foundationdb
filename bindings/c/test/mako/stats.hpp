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

class alignas(64) WorkflowStatistics {
	uint64_t conflicts{ 0 };
	uint64_t total_errors{ 0 };
	uint64_t total_timeouts{ 0 };
	std::array<uint64_t, MAX_OP> ops;
	std::array<uint64_t, MAX_OP> errors;
	std::array<uint64_t, MAX_OP> timeouts;
	std::array<uint64_t, MAX_OP> latency_samples;
	std::array<uint64_t, MAX_OP> latency_us_total;
	std::vector<DDSketchMako> sketches;

public:
	WorkflowStatistics() noexcept {
		std::fill(ops.begin(), ops.end(), 0);
		std::fill(errors.begin(), errors.end(), 0);
		std::fill(timeouts.begin(), timeouts.end(), 0);
		std::fill(latency_samples.begin(), latency_samples.end(), 0);
		std::fill(latency_us_total.begin(), latency_us_total.end(), 0);
		sketches.resize(MAX_OP);
	}

	WorkflowStatistics(const WorkflowStatistics& other) = default;
	WorkflowStatistics& operator=(const WorkflowStatistics& other) = default;

	uint64_t getConflictCount() const noexcept { return conflicts; }

	uint64_t getOpCount(int op) const noexcept { return ops[op]; }

	uint64_t getErrorCount(int op) const noexcept { return errors[op]; }

	uint64_t getTimeoutCount(int op) const noexcept { return timeouts[op]; }

	uint64_t getTotalErrorCount() const noexcept { return total_errors; }

	uint64_t getTotalTimeoutCount() const noexcept { return total_timeouts; }

	uint64_t getLatencySampleCount(int op) const noexcept { return latency_samples[op]; }

	uint64_t getLatencyUsTotal(int op) const noexcept { return latency_us_total[op]; }

	uint64_t getLatencyUsMin(int op) const noexcept { return sketches[op].min(); }

	uint64_t getLatencyUsMax(int op) const noexcept { return sketches[op].max(); }

	uint64_t percentile(int op, double quantile) { return sketches[op].percentile(quantile); }

	uint64_t mean(int op) const noexcept { return sketches[op].mean(); }

	// with 'this' as final aggregation, factor in 'other'
	void combine(const WorkflowStatistics& other) {
		conflicts += other.conflicts;
		for (auto op = 0; op < MAX_OP; op++) {
			sketches[op].mergeWith(other.sketches[op]);
			ops[op] += other.ops[op];
			errors[op] += other.errors[op];
			timeouts[op] += other.timeouts[op];
			total_errors += other.errors[op];
			total_timeouts += other.timeouts[op];
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

	void incrTimeoutCount(int op) noexcept {
		total_timeouts++;
		timeouts[op]++;
	}

	void addLatency(int op, timediff_t diff) noexcept {
		const auto latency_us = toIntegerMicroseconds(diff);
		latency_samples[op]++;
		sketches[op].addSample(latency_us);
		latency_us_total[op] += latency_us;
	}

	void writeToFile(const std::string& filename, int op) const {
		rapidjson::StringBuffer ss;
		rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
		sketches[op].serialize(writer);
		std::ofstream f(filename);
		f << ss.GetString();
	}

	void updateLatencies(const std::vector<DDSketchMako> other_sketches) { sketches = other_sketches; }

	friend std::ofstream& operator<<(std::ofstream& os, WorkflowStatistics& stats);
	friend std::ifstream& operator>>(std::ifstream& is, WorkflowStatistics& stats);
};

inline std::ofstream& operator<<(std::ofstream& os, WorkflowStatistics& stats) {
	rapidjson::StringBuffer ss;
	rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
	writer.StartObject();
	writer.String("conflicts");
	writer.Uint64(stats.conflicts);
	writer.String("total_errors");
	writer.Uint64(stats.total_errors);
	writer.String("total_timeouts");
	writer.Uint64(stats.total_timeouts);

	writer.String("ops");
	writer.StartArray();
	for (auto op = 0; op < MAX_OP; op++) {
		writer.Uint64(stats.ops[op]);
	}
	writer.EndArray();

	writer.String("errors");
	writer.StartArray();
	for (auto op = 0; op < MAX_OP; op++) {
		writer.Uint64(stats.errors[op]);
	}
	writer.EndArray();

	writer.String("timeouts");
	writer.StartArray();
	for (auto op = 0; op < MAX_OP; op++) {
		writer.Uint64(stats.timeouts[op]);
	}
	writer.EndArray();

	writer.String("latency_samples");
	writer.StartArray();
	for (auto op = 0; op < MAX_OP; op++) {
		writer.Uint64(stats.latency_samples[op]);
	}
	writer.EndArray();

	writer.String("latency_us_total");
	writer.StartArray();
	for (auto op = 0; op < MAX_OP; op++) {
		writer.Uint64(stats.latency_us_total[op]);
	}
	writer.EndArray();

	for (auto op = 0; op < MAX_OP; op++) {
		if (stats.sketches[op].getPopulationSize() > 0) {
			std::string op_name = getOpName(op);
			writer.String(op_name.c_str());
			stats.sketches[op].serialize(writer);
		}
	}
	writer.EndObject();
	os << ss.GetString();
	return os;
}

inline void populateArray(std::array<uint64_t, MAX_OP>& arr,
                          rapidjson::GenericArray<false, rapidjson::GenericValue<rapidjson::UTF8<>>>& json) {
	uint64_t idx = 0;
	for (auto it = json.Begin(); it != json.End(); it++) {
		arr[idx] = it->GetUint64();
		idx++;
	}
}

inline std::ifstream& operator>>(std::ifstream& is, WorkflowStatistics& stats) {
	std::stringstream buffer;
	buffer << is.rdbuf();
	rapidjson::Document doc;
	doc.Parse(buffer.str().c_str());
	stats.conflicts = doc["conflicts"].GetUint64();
	stats.total_errors = doc["total_errors"].GetUint64();
	stats.total_timeouts = doc["total_timeouts"].GetUint64();

	auto jsonOps = doc["ops"].GetArray();
	auto jsonErrors = doc["errors"].GetArray();
	auto jsonTimeouts = doc["timeouts"].GetArray();
	auto jsonLatencySamples = doc["latency_samples"].GetArray();
	auto jsonLatencyUsTotal = doc["latency_us_total"].GetArray();

	populateArray(stats.ops, jsonOps);
	populateArray(stats.errors, jsonErrors);
	populateArray(stats.timeouts, jsonTimeouts);
	populateArray(stats.latency_samples, jsonLatencySamples);
	populateArray(stats.latency_us_total, jsonLatencyUsTotal);
	for (int op = 0; op < MAX_OP; op++) {
		const std::string op_name = getOpName(op);
		stats.sketches[op].deserialize(doc[op_name.c_str()]);
	}

	return is;
}

class alignas(64) ThreadStatistics {
	double cpu_utilization{ 0.0 };

public:
	ThreadStatistics() noexcept {}

	ThreadStatistics(const ThreadStatistics& other) = default;
	ThreadStatistics& operator=(const ThreadStatistics& other) = default;

	double getCPUUtilization() { return cpu_utilization; }
	void setCPUUtilization(double cpu_utilization) { this->cpu_utilization = cpu_utilization; }

	void combine(const ThreadStatistics& other) { cpu_utilization += other.cpu_utilization; }
};

class alignas(64) ProcessStatistics {
	double total_cpu_utilization{ 0.0 };
	double fdb_network_cpu_utilization{ 0.0 };

public:
	ProcessStatistics() noexcept {}

	ProcessStatistics(const ProcessStatistics& other) = default;
	ProcessStatistics& operator=(const ProcessStatistics& other) = default;

	double getTotalCPUUtilization() { return total_cpu_utilization; }
	double getFDBNetworkCPUUtilization() { return fdb_network_cpu_utilization; }

	void setCPUUtilization(double total_cpu_utilization) { this->total_cpu_utilization = total_cpu_utilization; }
	void setFDBNetworkCPUUtilization(double fdb_network_cpu_utilization) {
		this->fdb_network_cpu_utilization = fdb_network_cpu_utilization;
	}

	void combine(const ProcessStatistics& other) {
		total_cpu_utilization += other.total_cpu_utilization;
		fdb_network_cpu_utilization += other.fdb_network_cpu_utilization;
	}
};

} // namespace mako

#endif /* MAKO_STATS_HPP */
