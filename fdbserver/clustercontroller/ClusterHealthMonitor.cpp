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
#include "flow/UnitTest.h"
#include "flow/genericactors.actor.h"

#include "ClusterHealthMonitor.h"

namespace cluster_health {

namespace {

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

Future<Level> fetchSpaceLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                              std::string const& eventName,
                              std::string const& availableBytesField,
                              std::string const& totalBytesField,
                              double interventionThreshold,
                              double criticalInterventionThreshold,
                              char const* failureTraceEventName) {
	auto eventsAndErrors = co_await workerEventProvider->getLatestEvents(eventName);
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

TraceEventFields makeSpaceMetrics(std::string const& availableBytesField,
                                  std::string const& totalBytesField,
                                  double availableBytes,
                                  double totalBytes) {
	TraceEventFields traceEventFields;
	traceEventFields.addField(availableBytesField, std::to_string(availableBytes));
	traceEventFields.addField(totalBytesField, std::to_string(totalBytes));
	return traceEventFields;
}

LatestWorkerEvents makeLatestWorkerEvents(TraceEventFields traceEventFields) {
	WorkerEvents events;
	events.emplace(NetworkAddress(IPAddress(0x01010101), 1), std::move(traceEventFields));
	return LatestWorkerEvents(std::make_pair(std::move(events), std::set<std::string>{}));
}

class MovingDataMetricsBuilder {
	TraceEventFields traceEventFields;

	MovingDataMetricsBuilder& setField(char const* fieldName, int64_t value) {
		traceEventFields.addField(fieldName, std::to_string(value));
		return *this;
	}

public:
	MovingDataMetricsBuilder() {
		inQueue(0).inFlight(0).priorityTeamUnhealthy(0).priorityTeam2Left(0).priorityTeam1Left(0).priorityTeam0Left(0);
	}

	MovingDataMetricsBuilder& inQueue(int64_t value) { return setField("InQueue", value); }
	MovingDataMetricsBuilder& inFlight(int64_t value) { return setField("InFlight", value); }
	MovingDataMetricsBuilder& priorityTeamUnhealthy(int64_t value) { return setField("PriorityTeamUnhealthy", value); }
	MovingDataMetricsBuilder& priorityTeam2Left(int64_t value) { return setField("PriorityTeam2Left", value); }
	MovingDataMetricsBuilder& priorityTeam1Left(int64_t value) { return setField("PriorityTeam1Left", value); }
	MovingDataMetricsBuilder& priorityTeam0Left(int64_t value) { return setField("PriorityTeam0Left", value); }

	TraceEventFields build() const { return traceEventFields; }
};

} // namespace

void WorkerEventProvider::setWorkers(std::vector<WorkerDetails> workers) {
	this->workers = std::move(workers);
}

Future<LatestWorkerEvents> WorkerEventProvider::getLatestEvents(std::string const& eventName) const {
	return latestEventOnWorkers(workers, eventName);
}

void FakeWorkerEventProvider::setLatestEvents(std::string eventName, LatestWorkerEvents latestEvents) {
	latestEventsByName[std::move(eventName)] = std::move(latestEvents);
}

Future<LatestWorkerEvents> FakeWorkerEventProvider::getLatestEvents(std::string const& eventName) const {
	auto it = latestEventsByName.find(eventName);
	if (it == latestEventsByName.end()) {
		return LatestWorkerEvents();
	}
	return it->second;
}

StorageSpaceFactor::StorageSpaceFactor(double interventionThreshold, double criticalInterventionThreshold)
  : interventionThreshold(interventionThreshold), criticalInterventionThreshold(criticalInterventionThreshold) {}

std::string_view StorageSpaceFactor::getName() const {
	return "StorageSpace";
}

Future<Level> StorageSpaceFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) {
	co_return co_await fetchSpaceLevel(workerEventProvider,
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

Future<Level> TLogSpaceFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) {
	co_return co_await fetchSpaceLevel(workerEventProvider,
	                                   "TLogMetrics",
	                                   "QueueDiskBytesAvailable",
	                                   "QueueDiskBytesTotal",
	                                   interventionThreshold,
	                                   criticalInterventionThreshold,
	                                   "TLogSpaceFactorFetchFailed");
}

std::string_view StorageReplicationFactor::getName() const {
	return "StorageReplication";
}

Future<Level> StorageReplicationFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) {
	auto eventsAndErrors = co_await workerEventProvider->getLatestEvents("MovingData");
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, _errors] = eventsAndErrors.get();
	if (events.empty()) {
		co_return Level::METRICS_MISSING;
	}

	try {
		int64_t queuedOrInFlightRepairMoves = 0;
		int64_t zeroReplicaTeams = 0;
		for (auto const& [address, traceEvent] : events) {
			(void)address;
			int64_t inQueue = traceEvent.getInt64("InQueue");
			int64_t inFlight = traceEvent.getInt64("InFlight");
			int64_t priorityTeamUnhealthy = traceEvent.getInt64("PriorityTeamUnhealthy");
			int64_t priorityTeam2Left = traceEvent.getInt64("PriorityTeam2Left");
			int64_t priorityTeam1Left = traceEvent.getInt64("PriorityTeam1Left");
			int64_t priorityTeam0Left = traceEvent.getInt64("PriorityTeam0Left");

			zeroReplicaTeams += priorityTeam0Left;
			if (inQueue > 0 || inFlight > 0) {
				queuedOrInFlightRepairMoves +=
				    priorityTeamUnhealthy + priorityTeam2Left + priorityTeam1Left + priorityTeam0Left;
			}
		}

		if (zeroReplicaTeams > 0) {
			co_return Level::OUTAGE;
		}
		if (queuedOrInFlightRepairMoves > 0) {
			co_return Level::SELF_HEALING;
		}
		co_return Level::HEALTHY;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "StorageReplicationFactorFetchFailed").error(e);
		co_return Level::METRICS_MISSING;
	}
}

Monitor::Monitor(std::vector<std::unique_ptr<IFactor>>&& factors,
                 Reference<IWorkerEventProvider const> workerEventProvider)
  : factors(std::move(factors)), workerEventProvider(workerEventProvider) {}

Future<Void> Monitor::run() {
	Future<Void> timer = Void();
	while (true) {
		co_await timer;
		timer = delay(5.0);

		std::vector<Future<Level>> levelFutures;
		levelFutures.reserve(factors.size());
		for (auto const& factor : factors) {
			levelFutures.push_back(factor->fetchLevel(workerEventProvider));
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

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/StorageSpaceFactor") {
	StorageSpaceFactor factor(/*interventionThreshold=*/0.20, /*criticalInterventionThreshold=*/0.10);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 50, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 15, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::INTERVENTION_REQUIRED);

	provider->setLatestEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 5, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents("StorageMetrics", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/TLogSpaceFactor") {
	TLogSpaceFactor factor(/*interventionThreshold=*/0.20, /*criticalInterventionThreshold=*/0.10);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 50, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 15, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::INTERVENTION_REQUIRED);

	provider->setLatestEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 5, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents("TLogMetrics", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/StorageReplicationFactor") {
	StorageReplicationFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().build()));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inQueue(1).priorityTeamUnhealthy(1).build()));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inFlight(1).priorityTeam1Left(1).build()));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inQueue(1).priorityTeam0Left(1).build()));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::OUTAGE);

	provider->setLatestEvents("MovingData", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

} // namespace cluster_health
