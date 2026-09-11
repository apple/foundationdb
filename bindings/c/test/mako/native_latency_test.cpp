/*
 * native_latency_test.cpp
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

#include "prometheus.hpp"
#include "prometheus_server.hpp"

#include <cstdio>
#include <cstdlib>
#include <initializer_list>
#include <limits>
#include <new>
#include <sstream>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace mako;

void check(bool condition, const char* message) {
	if (!condition) {
		std::fprintf(stderr, "%s\n", message);
		std::abort();
	}
}

int main() {
	NativeLatencyHistogram edges;
	NativeLatencySnapshot empty;
	check(!empty.percentile(0, 0.999), "empty histogram must not report a percentile");
	for (const auto value : { uint64_t{ 0 },
	                          uint64_t{ 1 },
	                          uint64_t{ 2 },
	                          uint64_t{ 1 } << 31,
	                          uint64_t{ 1 } << 32,
	                          std::numeric_limits<uint64_t>::max() }) {
		edges.add(0, value);
	}
	NativeLatencySnapshot edge_snapshot;
	edge_snapshot.merge(edges);
	check(edge_snapshot.samples(0) == 6, "all bounded histogram samples must be counted");
	check(*edge_snapshot.percentile(0, 0) == 0, "zero latency must retain its own bucket");
	check(*edge_snapshot.percentile(0, 1) >= (uint64_t{ 1 } << 31), "large latency must be bounded safely");

	const auto size = 2 * sizeof(NativeLatencyHistogram);
	auto* shared = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	check(shared != MAP_FAILED, "could not map shared histograms");
	auto* histograms = static_cast<NativeLatencyHistogram*>(shared);
	new (&histograms[0]) NativeLatencyHistogram();
	new (&histograms[1]) NativeLatencyHistogram();
	const auto counter_size = 2 * sizeof(NativeCounters);
	auto* counter_memory = mmap(nullptr, counter_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	check(counter_memory != MAP_FAILED, "could not map shared counters");
	auto* counters = static_cast<NativeCounters*>(counter_memory);
	new (&counters[0]) NativeCounters();
	new (&counters[1]) NativeCounters();
	check(reinterpret_cast<uintptr_t>(&histograms[1]) % alignof(NativeLatencyHistogram) == 0,
	      "adjacent worker histograms must stay aligned");

	const auto child = fork();
	check(child >= 0, "could not fork histogram writer");
	if (child == 0) {
		for (int i = 0; i < 3000; ++i) {
			histograms[1].add(0, 100);
			histograms[1].add(1, 10000);
			counters[1].attempted_transactions.fetch_add(2);
			counters[1].completed_transactions.fetch_add(1);
			counters[1].reads.fetch_add(10);
			counters[1].writes.fetch_add(1);
			counters[1].commits.fetch_add(1);
			counters[1].conflicts.fetch_add(1);
		}
		_exit(0);
	}

	auto previous = uint64_t{};
	for (int i = 0; i < 10000; ++i) {
		NativeLatencySnapshot current;
		current.merge(histograms[1]);
		check(current.samples(0) >= previous, "cross-process sample counts must not decrease");
		previous = current.samples(0);
		if (previous == 3000) {
			break;
		}
		usleep(100);
	}
	int status = 0;
	check(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
	      "child writer must finish successfully");
	NativeLatencySnapshot combined;
	combined.merge(histograms[0]);
	combined.merge(histograms[1]);
	check(combined.samples(0) == 3000 && combined.samples(1) == 3000, "adjacent worker histograms must not overlap");
	check(combined.totalMicroseconds(0) == 300000 && combined.totalMicroseconds(1) == 30000000,
	      "sampled latency sums must be accurate");
	NativeCountersSnapshot total;
	total.merge(counters[0]);
	total.merge(counters[1]);
	check(total.completed_transactions == 3000 && total.attempted_transactions == 6000 && total.reads == 30000 &&
	          total.writes == 3000 && total.commits == 3000 && total.conflicts == 3000,
	      "cross-process counters must count workload operations");
	const auto rendered = renderPrometheusMetrics(total, combined, "test-job", "benchmark-g9u1", 32, 100, 2, 4, 1, 42);
	check(rendered.find("fdb_mako_read_operations_total{mako_job=\"test-job\",workload=\"benchmark-g9u1\"} 30000") !=
	          std::string::npos,
	      "read metrics must carry bounded run labels");
	check(rendered.find("fdb_mako_write_logical_bytes_total{mako_job=\"test-job\",workload=\"benchmark-g9u1\"} "
	                    "396000") != std::string::npos,
	      "logical bytes must reflect the configured key and value sizes");
	check(rendered.find("fdb_mako_conflicts_total{mako_job=\"test-job\",workload=\"benchmark-g9u1\"} 3000") !=
	          std::string::npos,
	      "conflicts must remain separate from errors");
	check(rendered.find("fdb_mako_configured_worker_threads_per_process{mako_job=\"test-job\","
	                    "workload=\"benchmark-g9u1\"} 4") != std::string::npos,
	      "thread configuration must be exposed");
	check(rendered.find("fdb_mako_active{mako_job=\"test-job\",workload=\"benchmark-g9u1\"} 1") != std::string::npos,
	      "active gauge must use the same run labels");
	check(rendered.find("fdb_mako_latency_seconds_bucket{mako_job=\"test-job\",workload=\"benchmark-g9u1\","
	                    "operation=\"GET\",le=\"+Inf\"} 3000") != std::string::npos,
	      "histogram +Inf count must equal samples");
	check(rendered.find("fdb_mako_latency_seconds_sum{mako_job=\"test-job\",workload=\"benchmark-g9u1\","
	                    "operation=\"GET\"} 0.3") != std::string::npos,
	      "histogram sum must use seconds");
	check(rendered.find("le=\"+Inf\"") != std::string::npos && rendered.size() < 100000,
	      "histogram exposition must have bounded size");
	std::istringstream histogram_lines(rendered);
	auto line = std::string{};
	auto bucket_count = 0;
	auto previous_count = uint64_t{};
	auto previous_upper = -1.0;
	while (std::getline(histogram_lines, line)) {
		if (line.rfind("fdb_mako_latency_seconds_bucket{mako_job=\"test-job\",workload=\"benchmark-g9u1\","
		               "operation=\"GET\"",
		               0) != 0) {
			continue;
		}
		const auto le = line.substr(line.find("le=\"") + 4);
		if (le.rfind("+Inf", 0) != 0) {
			const auto upper = std::stod(le);
			check(upper > previous_upper, "histogram bucket bounds must be increasing");
			previous_upper = upper;
		}
		const auto count = std::stoull(line.substr(line.find("} ") + 2));
		check(count >= previous_count, "cumulative histogram buckets must not decrease");
		previous_count = count;
		bucket_count++;
	}
	check(bucket_count == 253 && previous_count == 3000, "histogram needs 251 finite bins, zero, and +Inf");
	check(prometheusWorkloadName("job;bad") == "run" && prometheusWorkloadName("job-valid_1") == "job-valid_1",
	      "metric labels must be bounded and safe");
	NativeMetricsServer server(0, [&rendered]() { return rendered; });
	const auto request = [&server](const char* path) {
		boost::asio::ip::tcp::iostream stream;
		stream.connect("127.0.0.1", std::to_string(server.port()));
		check(stream.good(), "could not connect to native Mako metrics endpoint");
		stream << "GET " << path << " HTTP/1.1\r\nHost: localhost\r\n\r\n" << std::flush;
		std::ostringstream response;
		response << stream.rdbuf();
		return response.str();
	};
	const auto http_metrics = request("/metrics");
	check(http_metrics.find("HTTP/1.1 200 OK") != std::string::npos &&
	          http_metrics.find("fdb_mako_latency_seconds_bucket") != std::string::npos,
	      "native /metrics must serve histogram data over HTTP");
	check(request("/other").find("HTTP/1.1 404 Not Found") != std::string::npos,
	      "native metrics endpoint must reject other paths");
	auto occupied_port_rejected = false;
	try {
		NativeMetricsServer duplicate(server.port(), []() { return std::string{}; });
	} catch (const boost::system::system_error&) {
		occupied_port_rejected = true;
	}
	check(occupied_port_rejected, "native metrics bind conflict must fail visibly");
	for (const auto quantile : { 0.5, 0.9, 0.99, 0.999 }) {
		const auto get = *combined.percentile(0, quantile);
		const auto commit = *combined.percentile(1, quantile);
		check(get >= 99 && get <= 101, "GET latency percentile outside histogram accuracy");
		check(commit >= 9900 && commit <= 10100, "COMMIT latency percentile outside histogram accuracy");
	}
	check(munmap(shared, size) == 0, "could not unmap shared histograms");
	check(munmap(counter_memory, counter_size) == 0, "could not unmap shared counters");
}
