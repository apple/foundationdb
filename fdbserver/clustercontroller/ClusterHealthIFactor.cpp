/*
 * ClusterHealthIFactor.cpp
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
#include <limits>

#include "fdbserver/core/RecoveryState.h"
#include "flow/Trace.h"

#include "ClusterHealthIFactor.h"
#include "ClusterHealthMonitor.h"

namespace cluster_health {

namespace {

WorkerEvents filterEmptyEvents(WorkerEvents const& events) {
	WorkerEvents filteredEvents;
	for (auto const& [address, traceEvent] : events) {
		if (traceEvent.size() > 0) {
			filteredEvents.emplace(address, traceEvent);
		}
	}
	return filteredEvents;
}

Future<Level> fetchSpaceLevel(LatestWorkerEvents eventsAndErrors,
                              std::string const& availableBytesField,
                              std::string const& totalBytesField,
                              double interventionThreshold,
                              double criticalInterventionThreshold,
                              char const* failureTraceEventName) {
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, _errors] = eventsAndErrors.get();
	WorkerEvents filteredEvents = filterEmptyEvents(events);
	if (filteredEvents.empty()) {
		co_return Level::METRICS_MISSING;
	}

	double minAvailableSpaceRatio = 1.0;

	try {
		for (auto const& [address, traceEvent] : filteredEvents) {
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

StorageSpaceFactor::StorageSpaceFactor(double interventionThreshold, double criticalInterventionThreshold)
  : interventionThreshold(interventionThreshold), criticalInterventionThreshold(criticalInterventionThreshold) {}

std::string_view StorageSpaceFactor::getName() const {
	return "StorageSpace";
}

Future<Level> StorageSpaceFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                             TrackCodeProbes trackCodeProbes) {
	Level level = co_await fetchSpaceLevel(co_await workerEventProvider->getLatestStorageServerEvents("StorageMetrics"),
	                                       "KvstoreBytesAvailable",
	                                       "KvstoreBytesTotal",
	                                       interventionThreshold,
	                                       criticalInterventionThreshold,
	                                       "StorageSpaceFactorFetchFailed");
	CODE_PROBE(trackCodeProbes && level == Level::HEALTHY, "ClusterHealth StorageSpaceFactor returns HEALTHY");
	CODE_PROBE(trackCodeProbes && level == Level::INTERVENTION_REQUIRED,
	           "ClusterHealth StorageSpaceFactor returns INTERVENTION_REQUIRED");
	CODE_PROBE(trackCodeProbes && level == Level::CRITICAL_INTERVENTION_REQUIRED,
	           "ClusterHealth StorageSpaceFactor returns CRITICAL_INTERVENTION_REQUIRED");
	co_return level;
}

TLogSpaceFactor::TLogSpaceFactor(double interventionThreshold, double criticalInterventionThreshold)
  : interventionThreshold(interventionThreshold), criticalInterventionThreshold(criticalInterventionThreshold) {}

std::string_view TLogSpaceFactor::getName() const {
	return "TLogSpace";
}

Future<Level> TLogSpaceFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                          TrackCodeProbes trackCodeProbes) {
	Level level = co_await fetchSpaceLevel(co_await workerEventProvider->getLatestTLogEvents("TLogMetrics"),
	                                       "QueueDiskBytesAvailable",
	                                       "QueueDiskBytesTotal",
	                                       interventionThreshold,
	                                       criticalInterventionThreshold,
	                                       "TLogSpaceFactorFetchFailed");
	CODE_PROBE(trackCodeProbes && level == Level::HEALTHY, "ClusterHealth TLogSpaceFactor returns HEALTHY");
	CODE_PROBE(trackCodeProbes && level == Level::INTERVENTION_REQUIRED,
	           "ClusterHealth TLogSpaceFactor returns INTERVENTION_REQUIRED");
	CODE_PROBE(trackCodeProbes && level == Level::CRITICAL_INTERVENTION_REQUIRED,
	           "ClusterHealth TLogSpaceFactor returns CRITICAL_INTERVENTION_REQUIRED");
	co_return level;
}

std::string_view StorageReplicationFactor::getName() const {
	return "StorageReplication";
}

Future<Level> StorageReplicationFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                                   TrackCodeProbes trackCodeProbes) {
	auto eventsAndErrors = co_await workerEventProvider->getLatestDataDistributorEvents("MovingData");
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, _errors] = eventsAndErrors.get();
	WorkerEvents filteredEvents = filterEmptyEvents(events);
	if (filteredEvents.empty()) {
		co_return Level::METRICS_MISSING;
	}

	try {
		int64_t queuedOrInFlightRepairMoves = 0;
		int64_t zeroReplicaTeams = 0;
		int64_t oneReplicaTeams = 0;
		for (auto const& [address, traceEvent] : filteredEvents) {
			(void)address;
			int64_t inQueue = traceEvent.getInt64("InQueue");
			int64_t inFlight = traceEvent.getInt64("InFlight");
			int64_t priorityTeamUnhealthy = traceEvent.getInt64("PriorityTeamUnhealthy");
			int64_t priorityTeam2Left = traceEvent.getInt64("PriorityTeam2Left");
			int64_t priorityTeam1Left = traceEvent.getInt64("PriorityTeam1Left");
			int64_t priorityTeam0Left = traceEvent.getInt64("PriorityTeam0Left");

			zeroReplicaTeams += priorityTeam0Left;
			oneReplicaTeams += priorityTeam1Left;
			if (inQueue > 0 || inFlight > 0) {
				queuedOrInFlightRepairMoves +=
				    priorityTeamUnhealthy + priorityTeam2Left + priorityTeam1Left + priorityTeam0Left;
			}
		}

		if (zeroReplicaTeams > 0) {
			CODE_PROBE(trackCodeProbes, "ClusterHealth StorageReplicationFactor returns OUTAGE");
			co_return Level::OUTAGE;
		}
		if (oneReplicaTeams > 0 && workerEventProvider->shouldTreatStorageTeamOneReplicaLeftAsCritical()) {
			CODE_PROBE(trackCodeProbes,
			           "ClusterHealth StorageReplicationFactor returns CRITICAL_INTERVENTION_REQUIRED");
			co_return Level::CRITICAL_INTERVENTION_REQUIRED;
		}
		if (queuedOrInFlightRepairMoves > 0) {
			CODE_PROBE(trackCodeProbes, "ClusterHealth StorageReplicationFactor returns SELF_HEALING");
			co_return Level::SELF_HEALING;
		}
		CODE_PROBE(trackCodeProbes, "ClusterHealth StorageReplicationFactor returns HEALTHY");
		co_return Level::HEALTHY;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "StorageReplicationFactorFetchFailed").error(e);
		co_return Level::METRICS_MISSING;
	}
}

std::string_view RecoveryStateFactor::getName() const {
	return "RecoveryState";
}

Future<Level> RecoveryStateFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                              TrackCodeProbes trackCodeProbes) {
	Optional<RecoveryState> recoveryState = workerEventProvider->getRecoveryState();
	if (!recoveryState.present()) {
		co_return Level::METRICS_MISSING;
	}

	Level level = Level::HEALTHY;
	if (recoveryState.get() < RecoveryState::ACCEPTING_COMMITS) {
		level = Level::OUTAGE;
	} else if (recoveryState.get() < RecoveryState::FULLY_RECOVERED) {
		level = Level::SELF_HEALING;
	}

	CODE_PROBE(trackCodeProbes && level == Level::OUTAGE, "ClusterHealth RecoveryStateFactor returns OUTAGE");
	CODE_PROBE(trackCodeProbes && level == Level::SELF_HEALING,
	           "ClusterHealth RecoveryStateFactor returns SELF_HEALING");
	CODE_PROBE(trackCodeProbes && level == Level::HEALTHY, "ClusterHealth RecoveryStateFactor returns HEALTHY");
	co_return level;
}

std::string_view ProcessErrorsFactor::getName() const {
	return "ProcessErrors";
}

Future<Level> ProcessErrorsFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                              TrackCodeProbes trackCodeProbes) {
	auto eventsAndErrors = co_await workerEventProvider->getLatestEvents("");
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, errors] = eventsAndErrors.get();
	WorkerEvents filteredEvents = filterEmptyEvents(events);
	if (filteredEvents.empty()) {
		bool const hadSuccessfulRequest = events.size() > errors.size();
		CODE_PROBE(trackCodeProbes && hadSuccessfulRequest, "ClusterHealth ProcessErrorsFactor returns HEALTHY");
		co_return hadSuccessfulRequest ? Level::HEALTHY : Level::METRICS_MISSING;
	}
	CODE_PROBE(trackCodeProbes, "ClusterHealth ProcessErrorsFactor returns CRITICAL_INTERVENTION_REQUIRED");
	co_return Level::CRITICAL_INTERVENTION_REQUIRED;
}

RkThrottlingFactor::RkThrottlingFactor(double criticalTpsLimitToReleasedTpsRatioThreshold)
  : criticalTpsLimitToReleasedTpsRatioThreshold(criticalTpsLimitToReleasedTpsRatioThreshold) {}

std::string_view RkThrottlingFactor::getName() const {
	return "RkThrottling";
}

Future<Level> RkThrottlingFactor::fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider,
                                             TrackCodeProbes trackCodeProbes) {
	auto eventsAndErrors = co_await workerEventProvider->getLatestRatekeeperEvents("RkUpdate");
	if (!eventsAndErrors.present()) {
		co_return Level::METRICS_MISSING;
	}

	auto const& [events, _errors] = eventsAndErrors.get();
	WorkerEvents filteredEvents = filterEmptyEvents(events);
	if (filteredEvents.empty()) {
		co_return Level::METRICS_MISSING;
	}

	try {
		double minTpsLimitToReleasedTpsRatio = std::numeric_limits<double>::infinity();
		for (auto const& [address, traceEvent] : filteredEvents) {
			(void)address;
			double tpsLimit = traceEvent.getDouble("TPSLimit");
			if (tpsLimit == 0.0) {
				CODE_PROBE(trackCodeProbes, "ClusterHealth RkThrottlingFactor returns OUTAGE");
				co_return Level::OUTAGE;
			}

			double releasedTps = traceEvent.getDouble("ReleasedTPS");
			if (releasedTps == 0.0) {
				continue;
			}

			minTpsLimitToReleasedTpsRatio = std::min(minTpsLimitToReleasedTpsRatio, tpsLimit / releasedTps);
		}

		if (minTpsLimitToReleasedTpsRatio < criticalTpsLimitToReleasedTpsRatioThreshold) {
			CODE_PROBE(trackCodeProbes, "ClusterHealth RkThrottlingFactor returns CRITICAL_INTERVENTION_REQUIRED");
			co_return Level::CRITICAL_INTERVENTION_REQUIRED;
		}
		CODE_PROBE(trackCodeProbes, "ClusterHealth RkThrottlingFactor returns HEALTHY");
		co_return Level::HEALTHY;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "RkThrottlingFactorFetchFailed").error(e);
		co_return Level::METRICS_MISSING;
	}
}

} // namespace cluster_health
