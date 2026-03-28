/*
 * ClusterHealthMonitor.cpp
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

#include <algorithm>
#include <utility>

#include "fmt/format.h"
#include "fdbserver/core/WorkerEvents.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"

#include "ClusterHealthMonitor.h"

namespace cluster_health {

namespace {

Future<Level> fetchSpaceLevel(std::vector<WorkerDetails> const& workers,
                              std::string const& eventName,
                              std::string const& availableBytesField,
                              std::string const& totalBytesField,
                              double interventionThreshold,
                              double criticalInterventionThreshold,
                              char const* failureTraceEventName) {
	auto eventsAndErrors = co_await latestEventOnWorkers(workers, eventName);
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, _errors] = eventsAndErrors.get();
	if (events.empty()) {
		co_return Level::METRICS_MISSING;
	}

	double minAvailableSpaceRatio = 1.0;

	try {
		for (auto const& [address, traceEvent] : events) {
			(void)address;
			double available = traceEvent.getDouble(availableBytesField);
			double total = std::max(1.0, traceEvent.getDouble(totalBytesField));
			minAvailableSpaceRatio = std::min(minAvailableSpaceRatio, available / total);
		}

		if (minAvailableSpaceRatio < criticalInterventionThreshold) {
			co_return Level::CRITICAL_INTERVENTION_REQUIRED;
		}
		if (minAvailableSpaceRatio < interventionThreshold) {
			co_return Level::INTERVENTION_REQUIRED;
		}
		co_return Level::HEALTHY;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, failureTraceEventName).error(e);
		co_return Level::METRICS_MISSING;
	}
}

} // namespace

uint8_t levelToInt(Level level) {
	switch (level) {
	case Level::HEALTHY:
		return 100;
	case Level::CRITICAL_INTERVENTION_REQUIRED:
		return 75;
	case Level::INTERVENTION_REQUIRED:
		return 50;
	case Level::SELF_HEALING:
		return 25;
	case Level::METRICS_MISSING:
	case Level::OUTAGE:
		return 0;
	}

	UNREACHABLE();
}

std::strong_ordering operator<=>(Level lhs, Level rhs) {
	return levelToInt(lhs) <=> levelToInt(rhs);
}

std::string_view levelToStr(Level level) {
	switch (level) {
	case Level::OUTAGE:
		return "Outage";
	case Level::CRITICAL_INTERVENTION_REQUIRED:
		return "CriticalInterventionRequired";
	case Level::INTERVENTION_REQUIRED:
		return "InterventionRequired";
	case Level::SELF_HEALING:
		return "SelfHealing";
	case Level::METRICS_MISSING:
		return "MetricsMissing";
	case Level::HEALTHY:
		return "Healthy";
	}

	UNREACHABLE();
}

StorageSpaceFactor::StorageSpaceFactor(double interventionThreshold, double criticalInterventionThreshold)
  : interventionThreshold(interventionThreshold), criticalInterventionThreshold(criticalInterventionThreshold) {}

std::string_view StorageSpaceFactor::getName() const {
	return "StorageSpace";
}

Future<Level> StorageSpaceFactor::fetchLevel(std::vector<WorkerDetails> const& workers) {
	co_return co_await fetchSpaceLevel(workers,
	                                   "StorageMetrics",
	                                   "KvstoreBytesAvailable",
	                                   "KvstoreBytesTotal",
	                                   interventionThreshold,
	                                   criticalInterventionThreshold,
	                                   "StorageSpaceFactorFetchFailed");
}

TLogSpaceFactor::TLogSpaceFactor(double interventionThreshold, double criticalInterventionThreshold)
  : interventionThreshold(interventionThreshold), criticalInterventionThreshold(criticalInterventionThreshold) {}

std::string_view TLogSpaceFactor::getName() const {
	return "TLogSpace";
}

Future<Level> TLogSpaceFactor::fetchLevel(std::vector<WorkerDetails> const& workers) {
	co_return co_await fetchSpaceLevel(workers,
	                                   "TLogMetrics",
	                                   "QueueDiskBytesAvailable",
	                                   "QueueDiskBytesTotal",
	                                   interventionThreshold,
	                                   criticalInterventionThreshold,
	                                   "TLogSpaceFactorFetchFailed");
}

void Monitor::setWorkers(std::vector<WorkerDetails> workers) {
	this->workers = std::move(workers);
}

Future<Void> Monitor::run() {
	Future<Void> timer = Void();
	while (true) {
		co_await timer;
		timer = delay(5.0);

		std::vector<Future<Level>> levelFutures;
		levelFutures.reserve(factors.size());
		for (auto const& factor : factors) {
			levelFutures.push_back(factor->fetchLevel(workers));
		}
		co_await waitForAll(levelFutures);

		Optional<int> limitingIndex;
		Level limitingLevel = Level::HEALTHY;
		TraceEvent traceEvent("ClusterHealthMetric");
		for (int i = 0; i < factors.size(); ++i) {
			Level level = levelFutures[i].get();
			traceEvent.detail(fmt::format("Factor{}", factors[i]->getName()), levelToStr(level));
			if (!limitingIndex.present() || level < limitingLevel) {
				limitingIndex = i;
				limitingLevel = level;
			}
		}

		traceEvent.detail("Aggregate", levelToStr(limitingLevel));
		if (limitingIndex.present()) {
			traceEvent.detail("LimitingFactor", factors[*limitingIndex]->getName());
		}
	}
}

} // namespace cluster_health
