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
#include <algorithm>
#include <variant>
#include "flow/SimpleCounter.h"
#include "flow/TDMetric.actor.h"
#include "flow/OTELMetrics.h"

// Defined in SimpleCounter.cpp
std::string hierarchicalToPrometheus(const std::string& input);
bool isValidPrometheusMetricName(std::string_view name);

// Sanitize a metric name for Prometheus compatibility.
std::string sanitizePrometheusName(const std::string& name);

// Format OTEL attributes as Prometheus labels string: {key1="val1",key2="val2"}
std::string formatLabels(const std::vector<OTEL::Attribute>& attributes);

// Formats all available metrics in Prometheus text exposition format.
// Includes: SimpleCounter metrics, OTEL Sums (counters), OTEL Gauges, and OTEL Histograms.
std::string formatPrometheusMetrics();

#endif
