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
#include "flow/Platform.h"
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

enum TimerKind { THREAD, PROCESS };

class CPUUtilizationTimer {
	steady_clock::time_point timepoint_start;
	steady_clock::time_point timepoint_end;
	double cpu_time_start{ 0.0 };
	double cpu_time_end{ 0.0 };
	TimerKind kind;

public:
	CPUUtilizationTimer(TimerKind kind) : kind(kind) {}
	void start() {
		timepoint_start = steady_clock::now();
		cpu_time_start = (kind == THREAD) ? getProcessorTimeThread() : getProcessorTimeProcess();
	}
	void end() {
		timepoint_end = steady_clock::now();
		cpu_time_end = (kind == THREAD) ? getProcessorTimeThread() : getProcessorTimeProcess();
	}
	double getCPUUtilization() const {
		return (cpu_time_end - cpu_time_start) / toDoubleSeconds(timepoint_end - timepoint_start) * 100.;
	}

	double getCPUTime() const { return cpu_time_end - cpu_time_start; }

	double getTotalDuration() const { return toDoubleSeconds(timepoint_end - timepoint_start); }
};

class alignas(64) ThreadStatistics {
	CPUUtilizationTimer thread_timer;

public:
	ThreadStatistics() : thread_timer(THREAD) {}

	void startThreadTimer() { thread_timer.start(); }
	void endThreadTimer() { thread_timer.end(); }
	double getThreadCPUUtilization() const { return thread_timer.getCPUUtilization(); }
	double getCPUTime() const { return thread_timer.getCPUTime(); }
	double getTotalDuration() const { return thread_timer.getTotalDuration(); }
};

class alignas(64) ProcessStatistics {
	CPUUtilizationTimer process_timer;
	CPUUtilizationTimer fdb_network_timer;

public:
	ProcessStatistics() : process_timer(PROCESS), fdb_network_timer(THREAD) {}
	void startProcessTimer() { process_timer.start(); }
	void endProcessTimer() { process_timer.end(); }

	void startFDBNetworkTimer() { fdb_network_timer.start(); }
	void endFDBNetworkTimer() { fdb_network_timer.end(); }

	double getProcessCPUUtilization() const { return process_timer.getCPUUtilization(); }
	double getFDBNetworkCPUUtilization() const { return fdb_network_timer.getCPUUtilization(); }

	double getProcessCPUTime() const { return process_timer.getCPUTime(); }
	double getFDBNetworkCPUTime() const { return fdb_network_timer.getCPUTime(); }

	double getProcessTotalDuration() const { return process_timer.getTotalDuration(); }
	double getFDBNetworkTotalDuration() const { return fdb_network_timer.getTotalDuration(); }
};

} // namespace mako

#endif /* MAKO_STATS_HPP */
