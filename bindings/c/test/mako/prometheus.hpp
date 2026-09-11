/*
 * prometheus.hpp
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

#ifndef MAKO_PROMETHEUS_HPP
#define MAKO_PROMETHEUS_HPP

#include "native_latency.hpp"
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

namespace mako {

inline std::string prometheusWorkloadName(const char* name) {
	if (!name) {
		return "run";
	}
	const auto value = std::string_view(name);
	if (value.empty() || value.size() > 64) {
		return "run";
	}
	for (const auto character : value) {
		if (!((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
		      (character >= '0' && character <= '9') || character == '-' || character == '_')) {
			return "run";
		}
	}
	return std::string(value);
}

inline std::string renderPrometheusMetrics(const NativeCountersSnapshot& counters,
                                           const NativeLatencySnapshot& latency,
                                           std::string_view job,
                                           std::string_view workload,
                                           int key_length,
                                           int value_length,
                                           int num_processes,
                                           int num_threads,
                                           int sampling,
                                           double run_start_seconds) {
	std::ostringstream out;
	out << std::setprecision(12);
	const auto label = "{mako_job=\"" + std::string(job) + "\",workload=\"" + std::string(workload) + "\"}";
	const auto emit = [&](std::string_view name, std::string_view description, std::string_view type, auto value) {
		out << "# HELP " << name << ' ' << description << '\n';
		out << "# TYPE " << name << ' ' << type << '\n';
		out << name << label << ' ' << value << '\n';
	};

	emit("fdb_mako_transactions_total",
	     "Completed Mako workload iterations",
	     "counter",
	     counters.completed_transactions);
	emit("fdb_mako_attempted_transactions_total",
	     "Started Mako workload transaction attempts, including retries",
	     "counter",
	     counters.attempted_transactions);
	emit("fdb_mako_read_operations_total", "Completed GET and UPDATE API steps", "counter", counters.reads);
	emit("fdb_mako_write_operations_total", "Completed UPDATE set API steps before commit", "counter", counters.writes);
	emit("fdb_mako_commits_total", "Successful FDB commits", "counter", counters.commits);
	emit("fdb_mako_conflicts_total", "FDB transaction conflicts observed by Mako", "counter", counters.conflicts);
	emit("fdb_mako_read_logical_bytes_total",
	     "Estimated GET and UPDATE logical value bytes",
	     "counter",
	     counters.reads * static_cast<uint64_t>(value_length));
	emit("fdb_mako_write_logical_bytes_total",
	     "Estimated UPDATE logical key and value bytes",
	     "counter",
	     counters.writes * static_cast<uint64_t>(key_length + value_length));
	emit("fdb_mako_errors_total", "Mako operation errors excluding conflicts and timeouts", "counter", counters.errors);
	emit("fdb_mako_timeouts_total", "Mako operation timeouts", "counter", counters.timeouts);
	emit("fdb_mako_configured_worker_processes", "Configured Mako worker processes", "gauge", num_processes);
	emit("fdb_mako_configured_worker_threads_per_process",
	     "Configured Mako worker threads per process",
	     "gauge",
	     num_threads);
	emit("fdb_mako_configured_worker_threads",
	     "Configured Mako worker threads total",
	     "gauge",
	     num_processes * num_threads);
	emit("fdb_mako_sampling_rate", "One latency observation per N completed transactions", "gauge", sampling);
	emit("fdb_mako_run_start_time_seconds", "Mako run start UNIX timestamp", "gauge", run_start_seconds);
	emit("fdb_mako_active", "Mako native metrics endpoint is serving this workload", "gauge", 1);

	out << "# HELP fdb_mako_latency_seconds Sampled successful client-side API latency\n";
	out << "# TYPE fdb_mako_latency_seconds histogram\n";
	for (int op = 0; op < NativeLatencyHistogram::operationCount; ++op) {
		const auto operation = op == 0 ? "GET" : "COMMIT";
		const auto histogram_labels = "{mako_job=\"" + std::string(job) + "\",workload=\"" + std::string(workload) +
		                              "\",operation=\"" + operation;
		auto cumulative = latency.bucket(op, 0);
		out << "fdb_mako_latency_seconds_bucket" << histogram_labels << "\",le=\"0\"} " << cumulative << '\n';
		auto last_bucket = 0;
		// Four internal buckets are about 4.4% wide between 100us and 1s.
		// The coarser tails keep the exported series bounded to 250 per operation.
		const auto emitBucket = [&](int index) {
			for (int bucket = last_bucket + 1; bucket <= index; ++bucket) {
				cumulative += latency.bucket(op, bucket);
			}
			const auto upper_seconds = std::exp2(index / 64.0) / 1'000'000;
			out << "fdb_mako_latency_seconds_bucket" << histogram_labels << "\",le=\"" << upper_seconds << "\"} "
			    << cumulative << '\n';
			last_bucket = index;
		};
		for (int index = 32; index <= 416; index += 32) {
			emitBucket(index);
		}
		for (int index = 426; index <= 1278; index += 4) {
			emitBucket(index);
		}
		for (int index = 1310; index < NativeLatencyHistogram::bucketCount - 1; index += 32) {
			emitBucket(index);
		}
		out << "fdb_mako_latency_seconds_bucket" << histogram_labels << "\",le=\"+Inf\"} " << latency.samples(op)
		    << '\n';
		out << "fdb_mako_latency_seconds_sum" << histogram_labels << "\"} "
		    << latency.totalMicroseconds(op) / 1'000'000.0 << '\n';
		out << "fdb_mako_latency_seconds_count" << histogram_labels << "\"} " << latency.samples(op) << '\n';
	}
	return out.str();
}

} // namespace mako

#endif /* MAKO_PROMETHEUS_HPP */
