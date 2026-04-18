/*
 * ClusterHealthMonitorTesting.cpp
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

#include <map>

#include "fdbserver/core/RecoveryState.h"
#include "flow/UnitTest.h"

#include "ClusterHealthIFactor.h"
#include "ClusterHealthMonitor.h"

namespace cluster_health {

namespace {

class FakeWorkerEventProvider final : public IWorkerEventProvider, public ReferenceCounted<FakeWorkerEventProvider> {
	Optional<RecoveryState> recoveryState;
	bool storageTeamOneReplicaLeftIsCritical = false;
	std::map<std::string, LatestWorkerEvents> latestEventsByName;
	std::map<std::string, LatestWorkerEvents> latestRatekeeperEventsByName;
	std::map<std::string, LatestWorkerEvents> latestDataDistributorEventsByName;
	std::map<std::string, LatestWorkerEvents> latestStorageServerEventsByName;
	std::map<std::string, LatestWorkerEvents> latestTLogEventsByName;

public:
	void addref() const override { ReferenceCounted<FakeWorkerEventProvider>::addref(); }
	void delref() const override { ReferenceCounted<FakeWorkerEventProvider>::delref(); }

	void setLatestEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	void setRecoveryState(RecoveryState recoveryState) { this->recoveryState = recoveryState; }

	void setStorageTeamOneReplicaLeftIsCritical(bool storageTeamOneReplicaLeftIsCritical) {
		this->storageTeamOneReplicaLeftIsCritical = storageTeamOneReplicaLeftIsCritical;
	}

	void setLatestRatekeeperEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestRatekeeperEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	void setLatestDataDistributorEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestDataDistributorEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	void setLatestStorageServerEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestStorageServerEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	void setLatestTLogEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestTLogEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	Future<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const override {
		auto it = latestEventsByName.find(eventName);
		if (it == latestEventsByName.end()) {
			return LatestWorkerEvents();
		}
		return it->second;
	}

	Optional<RecoveryState> getRecoveryState() const override { return recoveryState; }

	bool shouldTreatStorageTeamOneReplicaLeftAsCritical() const override { return storageTeamOneReplicaLeftIsCritical; }

	Future<LatestWorkerEvents> getLatestRatekeeperEvents(std::string const& eventName) const override {
		auto it = latestRatekeeperEventsByName.find(eventName);
		if (it != latestRatekeeperEventsByName.end()) {
			return it->second;
		}
		return getLatestEvents(eventName);
	}

	Future<LatestWorkerEvents> getLatestDataDistributorEvents(std::string const& eventName) const override {
		auto it = latestDataDistributorEventsByName.find(eventName);
		if (it != latestDataDistributorEventsByName.end()) {
			return it->second;
		}
		return getLatestEvents(eventName);
	}

	Future<LatestWorkerEvents> getLatestStorageServerEvents(std::string const& eventName) const override {
		auto it = latestStorageServerEventsByName.find(eventName);
		if (it == latestStorageServerEventsByName.end()) {
			return LatestWorkerEvents();
		}
		return it->second;
	}

	Future<LatestWorkerEvents> getLatestTLogEvents(std::string const& eventName) const override {
		auto it = latestTLogEventsByName.find(eventName);
		if (it == latestTLogEventsByName.end()) {
			return LatestWorkerEvents();
		}
		return it->second;
	}
};

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

LatestWorkerEvents makeLatestWorkerEvents(WorkerEvents events) {
	return LatestWorkerEvents(std::make_pair(std::move(events), std::set<std::string>{}));
}

LatestWorkerEvents makeLatestWorkerEvents(NetworkAddress address, TraceEventFields traceEventFields) {
	WorkerEvents events;
	events.emplace(address, std::move(traceEventFields));
	return LatestWorkerEvents(std::make_pair(std::move(events), std::set<std::string>{}));
}

LatestWorkerEvents makeLatestWorkerEvents(WorkerEvents events, std::set<std::string> errors) {
	return LatestWorkerEvents(std::make_pair(std::move(events), std::move(errors)));
}

class MovingDataMetricsBuilder {
	TraceEventFields traceEventFields;

	MovingDataMetricsBuilder& setField(char const* fieldName, int64_t value) {
		traceEventFields.addField(fieldName, std::to_string(value));
		return *this;
	}

	TraceEventFields buildWithDefault(char const* fieldName, int64_t defaultValue, TraceEventFields fields) const {
		std::string value;
		if (!fields.tryGetValue(fieldName, value)) {
			fields.addField(fieldName, std::to_string(defaultValue));
		}
		return fields;
	}

public:
	MovingDataMetricsBuilder& inQueue(int64_t value) { return setField("InQueue", value); }
	MovingDataMetricsBuilder& inFlight(int64_t value) { return setField("InFlight", value); }
	MovingDataMetricsBuilder& priorityTeamUnhealthy(int64_t value) { return setField("PriorityTeamUnhealthy", value); }
	MovingDataMetricsBuilder& priorityTeam2Left(int64_t value) { return setField("PriorityTeam2Left", value); }
	MovingDataMetricsBuilder& priorityTeam1Left(int64_t value) { return setField("PriorityTeam1Left", value); }
	MovingDataMetricsBuilder& priorityTeam0Left(int64_t value) { return setField("PriorityTeam0Left", value); }

	TraceEventFields build() const {
		TraceEventFields fields = traceEventFields;
		fields = buildWithDefault("InQueue", 0, std::move(fields));
		fields = buildWithDefault("InFlight", 0, std::move(fields));
		fields = buildWithDefault("PriorityTeamUnhealthy", 0, std::move(fields));
		fields = buildWithDefault("PriorityTeam2Left", 0, std::move(fields));
		fields = buildWithDefault("PriorityTeam1Left", 0, std::move(fields));
		fields = buildWithDefault("PriorityTeam0Left", 0, std::move(fields));
		return fields;
	}
};

TraceEventFields makeProcessErrorMetrics(std::string const& type, std::string const& error) {
	TraceEventFields traceEventFields;
	traceEventFields.addField("Type", type);
	traceEventFields.addField("Error", error);
	return traceEventFields;
}

TraceEventFields makeRkUpdateMetrics(double releasedTps, double tpsLimit) {
	TraceEventFields traceEventFields;
	traceEventFields.addField("ReleasedTPS", std::to_string(releasedTps));
	traceEventFields.addField("TPSLimit", std::to_string(tpsLimit));
	return traceEventFields;
}

} // namespace

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/StorageSpaceFactor") {
	StorageSpaceFactor factor(/*interventionThreshold=*/0.20, /*criticalInterventionThreshold=*/0.10);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestStorageServerEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 50, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestStorageServerEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 15, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::INTERVENTION_REQUIRED);

	provider->setLatestStorageServerEvents(
	    "StorageMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 5, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	WorkerEvents mixedRoleEvents;
	mixedRoleEvents.emplace(NetworkAddress(IPAddress(0x01010101), 1), TraceEventFields());
	mixedRoleEvents.emplace(NetworkAddress(IPAddress(0x02020202), 2),
	                        makeSpaceMetrics("KvstoreBytesAvailable", "KvstoreBytesTotal", 50, 100));
	provider->setLatestStorageServerEvents("StorageMetrics", makeLatestWorkerEvents(std::move(mixedRoleEvents)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestStorageServerEvents("StorageMetrics", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/TLogSpaceFactor") {
	TLogSpaceFactor factor(/*interventionThreshold=*/0.20, /*criticalInterventionThreshold=*/0.10);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestTLogEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 50, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestTLogEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 15, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::INTERVENTION_REQUIRED);

	provider->setLatestTLogEvents(
	    "TLogMetrics",
	    makeLatestWorkerEvents(makeSpaceMetrics("QueueDiskBytesAvailable", "QueueDiskBytesTotal", 5, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestTLogEvents("TLogMetrics", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/StorageReplicationFactor") {
	StorageReplicationFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().build()));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inQueue(1).priorityTeamUnhealthy(1).build()));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inFlight(1).priorityTeam1Left(1).build()));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setStorageTeamOneReplicaLeftIsCritical(true);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents(
	    "MovingData", makeLatestWorkerEvents(MovingDataMetricsBuilder().inQueue(1).priorityTeam0Left(1).build()));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::OUTAGE);

	WorkerEvents staleWorkerEvents;
	staleWorkerEvents.emplace(NetworkAddress(IPAddress(0x01010101), 1),
	                          MovingDataMetricsBuilder().inQueue(1).priorityTeam0Left(1).build());
	staleWorkerEvents.emplace(NetworkAddress(IPAddress(0x02020202), 2), MovingDataMetricsBuilder().build());
	provider->setLatestEvents("MovingData", makeLatestWorkerEvents(std::move(staleWorkerEvents)));
	provider->setLatestDataDistributorEvents(
	    "MovingData",
	    makeLatestWorkerEvents(NetworkAddress(IPAddress(0x02020202), 2), MovingDataMetricsBuilder().build()));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("MovingData", LatestWorkerEvents());
	provider->setLatestDataDistributorEvents("MovingData", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/RecoveryStateFactor") {
	RecoveryStateFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setRecoveryState(RecoveryState::FULLY_RECOVERED);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setRecoveryState(RecoveryState::ACCEPTING_COMMITS);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setRecoveryState(RecoveryState::ALL_LOGS_RECRUITED);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setRecoveryState(RecoveryState::RECOVERY_TRANSACTION);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::OUTAGE);

	provider->setRecoveryState(RecoveryState::FULLY_RECOVERED);
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	auto missingProvider = makeReference<FakeWorkerEventProvider>();
	level = co_await factor.fetchLevel(missingProvider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/ProcessErrorsFactor") {
	ProcessErrorsFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("", makeLatestWorkerEvents(makeProcessErrorMetrics("OpenClusterIdError", "io_error")));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	WorkerEvents emptyLatestErrors;
	emptyLatestErrors.emplace(NetworkAddress(IPAddress(0x01010101), 1), TraceEventFields());
	provider->setLatestEvents("", makeLatestWorkerEvents(std::move(emptyLatestErrors)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	WorkerEvents partiallyFailedLatestErrors;
	partiallyFailedLatestErrors.emplace(NetworkAddress(IPAddress(0x01010101), 1), TraceEventFields());
	partiallyFailedLatestErrors.emplace(NetworkAddress(IPAddress(0x02020202), 2), TraceEventFields());
	provider->setLatestEvents("", makeLatestWorkerEvents(std::move(partiallyFailedLatestErrors), { "2.2.2.2:2" }));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("", makeLatestWorkerEvents(WorkerEvents(), { "1.1.1.1:1" }));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);

	provider->setLatestEvents("", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/RkThrottlingFactor") {
	RkThrottlingFactor factor(/*criticalTpsLimitToReleasedTpsRatioThreshold=*/1.20);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(100, 125)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(100, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(0, 100)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(0, 0)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::OUTAGE);

	WorkerEvents staleWorkerEvents;
	staleWorkerEvents.emplace(NetworkAddress(IPAddress(0x01010101), 1), makeRkUpdateMetrics(100, 0));
	staleWorkerEvents.emplace(NetworkAddress(IPAddress(0x02020202), 2), makeRkUpdateMetrics(100, 200));
	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(std::move(staleWorkerEvents)));
	provider->setLatestRatekeeperEvents(
	    "RkUpdate", makeLatestWorkerEvents(NetworkAddress(IPAddress(0x02020202), 2), makeRkUpdateMetrics(100, 200)));
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("RkUpdate", LatestWorkerEvents());
	provider->setLatestRatekeeperEvents("RkUpdate", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider, TrackCodeProbes::False);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

} // namespace cluster_health

void forceLinkClusterHealthMonitorTests() {}
