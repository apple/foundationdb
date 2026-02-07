/*
 * PrometheusMetrics.h
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

#ifndef FLOW_PROMETHEUS_METRICS_H
#define FLOW_PROMETHEUS_METRICS_H
#pragma once

#include <string>
#include <sstream>
#include "flow/SimpleCounter.h"

// Defined in SimpleCounter.cpp
std::string hierarchicalToPrometheus(const std::string& input);
bool isValidPrometheusMetricName(std::string_view name);

// Formats all SimpleCounter metrics in Prometheus text exposition format.
inline std::string formatPrometheusMetrics() {
	std::ostringstream output;

	std::vector<SimpleCounter<int64_t>*> intCounters = SimpleCounter<int64_t>::getCounters();
	for (auto* counter : intCounters) {
		std::string name = hierarchicalToPrometheus(counter->name());
		output << name << " " << counter->get() << "\n";
	}

	std::vector<SimpleCounter<double>*> doubleCounters = SimpleCounter<double>::getCounters();
	for (auto* counter : doubleCounters) {
		std::string name = hierarchicalToPrometheus(counter->name());
		output << name << " " << counter->get() << "\n";
	}

	return output.str();
}

#endif
