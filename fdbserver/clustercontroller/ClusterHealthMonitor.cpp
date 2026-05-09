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

#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/WorkerEvents.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"

#include "ClusterHealthIFactor.h"
#include "ClusterHealthMonitor.h"

namespace cluster_health {

namespace {

AsyncResult<LatestWorkerEvents> latestEventOnWorker(WorkerInterface worker, std::string eventName) {
	try {
		EventLogRequest req = eventName.empty() ? EventLogRequest() : EventLogRequest(Standalone<StringRef>(eventName));
		ErrorOr<TraceEventFields> traceEvent =
		    co_await errorOr(timeoutError(worker.eventLogRequest.getReply(req), 2.0));

		WorkerEvents results;
		std::set<std::string> failed;
		if (traceEvent.isError()) {
			failed.insert(worker.address().toString());
			results[worker.address()] = TraceEventFields();
		} else {
			results[worker.address()] = traceEvent.get();
		}
		co_return std::make_pair(std::move(results), std::move(failed));
	} catch (Error& e) {
		ASSERT(e.code() ==
		       error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
		throw;
	}
}

template <class Interface>
AsyncResult<LatestWorkerEvents> latestEventOnInterfaces(
    std::vector<Interface> interfaces,
    std::unordered_map<NetworkAddress, WorkerInterface> addressWorkers,
    std::string eventName) {
	try {
		std::vector<Future<ErrorOr<TraceEventFields>>> eventTraces;
		std::vector<NetworkAddress> addresses;
		std::set<std::string> failed;
		WorkerEvents results;

		for (auto const& interf : interfaces) {
			auto workerIt = addressWorkers.find(interf.address());
			if (workerIt == addressWorkers.end()) {
				failed.insert(interf.address().toString());
				results[interf.address()] = TraceEventFields();
				continue;
			}

			addresses.push_back(interf.address());
			eventTraces.push_back(
			    errorOr(timeoutError(workerIt->second.eventLogRequest.getReply(EventLogRequest(
			                             Standalone<StringRef>(interf.id().toString() + "/" + eventName))),
			                         2.0)));
		}

		co_await waitForAll(eventTraces);

		for (int i = 0; i < eventTraces.size(); ++i) {
			ErrorOr<TraceEventFields> const& traceEvent = eventTraces[i].get();
			if (traceEvent.isError()) {
				failed.insert(addresses[i].toString());
				results[addresses[i]] = TraceEventFields();
			} else {
				results[addresses[i]] = traceEvent.get();
			}
		}

		co_return std::make_pair(std::move(results), std::move(failed));
	} catch (Error& e) {
		ASSERT(e.code() ==
		       error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
		throw;
	}
}

uint8_t levelToInt(Level level) {
	switch (level) {
	case Level::HEALTHY:
		return 100;
	case Level::SELF_HEALING:
		return 80;
	case Level::INTERVENTION_REQUIRED:
		return 60;
	case Level::CRITICAL_INTERVENTION_REQUIRED:
		return 40;
	case Level::METRICS_MISSING:
		return 20;
	case Level::OUTAGE:
		return 0;
	}

	UNREACHABLE();
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

} // namespace

void WorkerEventProvider::setWorkers(std::vector<WorkerDetails> workers) {
	this->workers = std::move(workers);
}

void WorkerEventProvider::setRecoveryState(RecoveryState recoveryState) {
	this->recoveryState = recoveryState;
}

void WorkerEventProvider::setStorageTeamOneReplicaLeftIsCritical(bool storageTeamOneReplicaLeftIsCritical) {
	this->storageTeamOneReplicaLeftIsCritical = storageTeamOneReplicaLeftIsCritical;
}

void WorkerEventProvider::setRatekeeperWorker(Optional<WorkerInterface> ratekeeperWorker) {
	this->ratekeeperWorker = std::move(ratekeeperWorker);
}

void WorkerEventProvider::setDataDistributorWorker(Optional<WorkerInterface> dataDistributorWorker) {
	this->dataDistributorWorker = std::move(dataDistributorWorker);
}

void WorkerEventProvider::setStorageServers(std::vector<StorageServerInterface> storageServers) {
	this->storageServers = std::move(storageServers);
}

void WorkerEventProvider::setTLogs(std::vector<TLogInterface> tlogs) {
	this->tlogs = std::move(tlogs);
}

Optional<RecoveryState> WorkerEventProvider::getRecoveryState() const {
	return recoveryState;
}

bool WorkerEventProvider::shouldTreatStorageTeamOneReplicaLeftAsCritical() const {
	return storageTeamOneReplicaLeftIsCritical;
}

AsyncResult<LatestWorkerEvents> WorkerEventProvider::getLatestEvents(std::string const& eventName) const {
	return latestEventOnWorkers(workers, eventName);
}

AsyncResult<LatestWorkerEvents> WorkerEventProvider::getLatestRatekeeperEvents(std::string const& eventName) const {
	if (!ratekeeperWorker.present()) {
		co_return LatestWorkerEvents();
	}
	co_return co_await latestEventOnWorker(ratekeeperWorker.get(), eventName);
}

AsyncResult<LatestWorkerEvents> WorkerEventProvider::getLatestDataDistributorEvents(
    std::string const& eventName) const {
	if (!dataDistributorWorker.present()) {
		co_return LatestWorkerEvents();
	}
	co_return co_await latestEventOnWorker(dataDistributorWorker.get(), eventName);
}

AsyncResult<LatestWorkerEvents> WorkerEventProvider::getLatestStorageServerEvents(std::string const& eventName) const {
	std::unordered_map<NetworkAddress, WorkerInterface> addressWorkers;
	addressWorkers.reserve(workers.size());
	for (auto const& worker : workers) {
		addressWorkers.emplace(worker.interf.address(), worker.interf);
	}
	return latestEventOnInterfaces(storageServers, std::move(addressWorkers), eventName);
}

AsyncResult<LatestWorkerEvents> WorkerEventProvider::getLatestTLogEvents(std::string const& eventName) const {
	std::unordered_map<NetworkAddress, WorkerInterface> addressWorkers;
	addressWorkers.reserve(workers.size());
	for (auto const& worker : workers) {
		addressWorkers.emplace(worker.interf.address(), worker.interf);
	}
	return latestEventOnInterfaces(tlogs, std::move(addressWorkers), eventName);
}

Monitor::Monitor(std::vector<std::unique_ptr<IFactor>>&& factors,
                 Reference<IWorkerEventProvider const> workerEventProvider)
  : factors(std::move(factors)), workerEventProvider(workerEventProvider) {}

Future<Void> Monitor::run() {
	if (!SERVER_KNOBS->CLUSTER_HEALTH_METRIC_ENABLE) {
		co_return;
	}

	Future<Void> timer = Void();
	while (true) {
		co_await timer;
		timer = delay(SERVER_KNOBS->CLUSTER_HEALTH_METRIC_POLL_INTERVAL);

		std::vector<Future<Level>> levelFutures;
		levelFutures.reserve(factors.size());
		for (auto const& factor : factors) {
			levelFutures.push_back(factor->fetchLevel(workerEventProvider, TrackCodeProbes::True));
		}
		co_await waitForAll(levelFutures);

		Optional<int> limitingIndex;
		Level limitingLevel = Level::HEALTHY;
		TraceEvent traceEvent("ClusterHealthMetric");
		for (int i = 0; i < factors.size(); ++i) {
			Level level = levelFutures[i].get();
			traceEvent.detail(fmt::format("Factor{}", factors[i]->getName()), levelToStr(level));
			if (!limitingIndex.present() || levelToInt(level) < levelToInt(limitingLevel)) {
				limitingIndex = i;
				limitingLevel = level;
			}
		}

		traceEvent.detail("Aggregate", levelToStr(limitingLevel));
		traceEvent.detail("AggregateValue", levelToInt(limitingLevel));
		if (limitingIndex.present() && limitingLevel != Level::HEALTHY) {
			traceEvent.detail("LimitingFactor", factors[*limitingIndex]->getName());
		}
	}
}

Monitor Monitor::create(Reference<IWorkerEventProvider const> workerEventProvider) {
	std::vector<std::unique_ptr<IFactor>> factors;
	factors.push_back(
	    std::make_unique<StorageSpaceFactor>(SERVER_KNOBS->CLUSTER_HEALTH_METRIC_STORAGE_INTERVENTION_THRESHOLD,
	                                         SERVER_KNOBS->CLUSTER_HEALTH_METRIC_STORAGE_CRITICAL_THRESHOLD));
	factors.push_back(std::make_unique<TLogSpaceFactor>(SERVER_KNOBS->CLUSTER_HEALTH_METRIC_TLOG_INTERVENTION_THRESHOLD,
	                                                    SERVER_KNOBS->CLUSTER_HEALTH_METRIC_TLOG_CRITICAL_THRESHOLD));
	factors.push_back(std::make_unique<StorageReplicationFactor>());
	factors.push_back(std::make_unique<RecoveryStateFactor>());
	factors.push_back(std::make_unique<ProcessErrorsFactor>());
	factors.push_back(std::make_unique<RkThrottlingFactor>(
	    SERVER_KNOBS->CLUSTER_HEALTH_METRIC_RK_CRITICAL_RELEASED_TPS_RATIO_THRESHOLD));
	return Monitor(std::move(factors), workerEventProvider);
}

} // namespace cluster_health
