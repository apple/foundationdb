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
	std::map<std::string, LatestWorkerEvents> latestEventsByName;

public:
	void addref() const override { ReferenceCounted<FakeWorkerEventProvider>::addref(); }
	void delref() const override { ReferenceCounted<FakeWorkerEventProvider>::delref(); }

	void setLatestEvents(std::string eventName, LatestWorkerEvents latestEvents) {
		latestEventsByName[std::move(eventName)] = std::move(latestEvents);
	}

	Future<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const override {
		auto it = latestEventsByName.find(eventName);
		if (it == latestEventsByName.end()) {
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

TraceEventFields makeRecoveryStateMetrics(int statusCode) {
	TraceEventFields traceEventFields;
	traceEventFields.addField("StatusCode", std::to_string(statusCode));
	return traceEventFields;
}

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

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/RecoveryStateFactor") {
	RecoveryStateFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("MasterRecoveryState",
	                          makeLatestWorkerEvents(makeRecoveryStateMetrics(RecoveryStatus::fully_recovered)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("MasterRecoveryState",
	                          makeLatestWorkerEvents(makeRecoveryStateMetrics(RecoveryStatus::accepting_commits)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setLatestEvents("MasterRecoveryState",
	                          makeLatestWorkerEvents(makeRecoveryStateMetrics(RecoveryStatus::all_logs_recruited)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::SELF_HEALING);

	provider->setLatestEvents("MasterRecoveryState",
	                          makeLatestWorkerEvents(makeRecoveryStateMetrics(RecoveryStatus::recovery_transaction)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::OUTAGE);

	provider->setLatestEvents("MasterRecoveryState", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/ProcessErrorsFactor") {
	ProcessErrorsFactor factor;
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("", makeLatestWorkerEvents(makeProcessErrorMetrics("OpenClusterIdError", "io_error")));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents("", makeLatestWorkerEvents(WorkerEvents()));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

TEST_CASE("/fdbserver/clustercontroller/ClusterHealthMonitor/RkThrottlingFactor") {
	RkThrottlingFactor factor(/*criticalTpsLimitToReleasedTpsRatioThreshold=*/0.50);
	auto provider = makeReference<FakeWorkerEventProvider>();
	Level level;

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(100, 25)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(100, 40)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::CRITICAL_INTERVENTION_REQUIRED);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(0, 100)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::HEALTHY);

	provider->setLatestEvents("RkUpdate", makeLatestWorkerEvents(makeRkUpdateMetrics(0, 0)));
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::OUTAGE);

	provider->setLatestEvents("RkUpdate", LatestWorkerEvents());
	level = co_await factor.fetchLevel(provider);
	ASSERT_EQ(level, Level::METRICS_MISSING);
}

} // namespace cluster_health

void forceLinkClusterHealthMonitorTests() {}
