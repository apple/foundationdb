/*
 * PrometheusMetrics.cpp
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

#include "flow/PrometheusMetrics.h"
#include "flow/Error.h"

std::string sanitizePrometheusName(const std::string& name) {
	std::string result;
	result.reserve(name.size());
	for (size_t i = 0; i < name.size(); ++i) {
		char c = name[i];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == ':' ||
		    (c >= '0' && c <= '9' && i > 0)) {
			result += c;
		} else if (c == '.' || c == '-' || c == '/' || c == ' ') {
			result += '_';
		} else if (c >= '0' && c <= '9' && i == 0) {
			result += '_';
			result += c;
		} else {
			result += '_';
		}
	}
	return result;
}

std::string formatLabels(const std::vector<OTEL::Attribute>& attributes) {
	if (attributes.empty())
		return "";
	std::ostringstream oss;
	oss << "{";
	for (size_t i = 0; i < attributes.size(); ++i) {
		if (i > 0)
			oss << ",";
		oss << attributes[i].key << "=\"" << attributes[i].value << "\"";
	}
	oss << "}";
	return oss.str();
}

std::string formatPrometheusMetrics() {
	std::ostringstream output;

	// SimpleCounter metrics (int64_t)
	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();
	for (auto* counter : intCounters) {
		std::string name = hierarchicalToPrometheus(counter->name());
		output << "# TYPE " << name << " counter\n";
		output << name << " " << counter->get() << "\n";
	}

	// SimpleCounter metrics (double)
	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();
	for (auto* counter : doubleCounters) {
		std::string name = hierarchicalToPrometheus(counter->name());
		output << "# TYPE " << name << " counter\n";
		output << name << " " << counter->get() << "\n";
	}

	// OTEL metrics from MetricCollection
	MetricCollection* metrics = MetricCollection::getMetricCollection();
	if (metrics != nullptr) {
		// OTEL Sums -> Prometheus counters
		for (const auto& [uid, sum] : metrics->sumMap) {
			std::string name = sanitizePrometheusName(sum.name);
			ASSERT(!name.empty());
			output << "# TYPE " << name << " counter\n";
			for (const auto& point : sum.points) {
				std::string labels = formatLabels(point.attributes);
				if (std::holds_alternative<int64_t>(point.val)) {
					output << name << labels << " " << std::get<int64_t>(point.val) << "\n";
				} else {
					output << name << labels << " " << std::get<double>(point.val) << "\n";
				}
			}
		}

		// OTEL Gauges -> Prometheus gauges
		for (const auto& [uid, gauge] : metrics->gaugeMap) {
			std::string name = sanitizePrometheusName(gauge.name);
			ASSERT(!name.empty());
			output << "# TYPE " << name << " gauge\n";
			for (const auto& point : gauge.points) {
				std::string labels = formatLabels(point.attributes);
				if (std::holds_alternative<int64_t>(point.val)) {
					output << name << labels << " " << std::get<int64_t>(point.val) << "\n";
				} else {
					output << name << labels << " " << std::get<double>(point.val) << "\n";
				}
			}
		}

		// OTEL Histograms -> Prometheus summary (count, sum, min, max)
		for (const auto& [uid, hist] : metrics->histMap) {
			std::string name = sanitizePrometheusName(hist.name);
			ASSERT(!name.empty());
			output << "# TYPE " << name << " summary\n";
			for (const auto& point : hist.points) {
				std::string labels = formatLabels(point.attributes);
				output << name << "_count" << labels << " " << point.count << "\n";
				output << name << "_sum" << labels << " " << point.sum << "\n";
				output << name << "_min" << labels << " " << point.min << "\n";
				output << name << "_max" << labels << " " << point.max << "\n";
			}
		}
	}

	return output.str();
}
