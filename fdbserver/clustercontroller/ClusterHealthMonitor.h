/*
 * ClusterHealthMonitor.h
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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ClusterHealthIFactor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/TLogInterface.h"
#include "fdbserver/core/WorkerEvents.h"
#include "flow/flow.h"

namespace cluster_health {

enum class Level {
	OUTAGE,
	CRITICAL_INTERVENTION_REQUIRED,
	INTERVENTION_REQUIRED,
	SELF_HEALING,
	METRICS_MISSING,
	HEALTHY
};

using LatestWorkerEvents = Optional<std::pair<WorkerEvents, std::set<std::string>>>;

// Abstracts access to health-monitor inputs so factors can be evaluated in production and tests.
class IWorkerEventProvider {
public:
	virtual ~IWorkerEventProvider() = default;
	virtual void addref() const = 0;
	virtual void delref() const = 0;
	virtual Optional<RecoveryState> getRecoveryState() const = 0;
	virtual bool shouldTreatStorageTeamOneReplicaLeftAsCritical() const = 0;
	virtual AsyncResult<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const = 0;
	virtual AsyncResult<LatestWorkerEvents> getLatestRatekeeperEvents(std::string const& eventName) const = 0;
	virtual AsyncResult<LatestWorkerEvents> getLatestDataDistributorEvents(std::string const& eventName) const = 0;
	virtual AsyncResult<LatestWorkerEvents> getLatestStorageServerEvents(std::string const& eventName) const = 0;
	virtual AsyncResult<LatestWorkerEvents> getLatestTLogEvents(std::string const& eventName) const = 0;
};

// Production event provider backed by worker event-log RPCs.
class WorkerEventProvider final : public IWorkerEventProvider, public ReferenceCounted<WorkerEventProvider> {
	std::vector<WorkerDetails> workers;
	Optional<RecoveryState> recoveryState;
	bool storageTeamOneReplicaLeftIsCritical = false;
	Optional<WorkerInterface> ratekeeperWorker;
	Optional<WorkerInterface> dataDistributorWorker;
	std::vector<StorageServerInterface> storageServers;
	std::vector<TLogInterface> tlogs;

public:
	void addref() const override { ReferenceCounted<WorkerEventProvider>::addref(); }
	void delref() const override { ReferenceCounted<WorkerEventProvider>::delref(); }
	void setWorkers(std::vector<WorkerDetails> workers);
	void setRecoveryState(RecoveryState recoveryState);
	void setStorageTeamOneReplicaLeftIsCritical(bool storageTeamOneReplicaLeftIsCritical);
	void setRatekeeperWorker(Optional<WorkerInterface> ratekeeperWorker);
	void setDataDistributorWorker(Optional<WorkerInterface> dataDistributorWorker);
	void setStorageServers(std::vector<StorageServerInterface> storageServers);
	void setTLogs(std::vector<TLogInterface> tlogs);
	Optional<RecoveryState> getRecoveryState() const override;
	bool shouldTreatStorageTeamOneReplicaLeftAsCritical() const override;
	AsyncResult<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const override;
	AsyncResult<LatestWorkerEvents> getLatestRatekeeperEvents(std::string const& eventName) const override;
	AsyncResult<LatestWorkerEvents> getLatestDataDistributorEvents(std::string const& eventName) const override;
	AsyncResult<LatestWorkerEvents> getLatestStorageServerEvents(std::string const& eventName) const override;
	AsyncResult<LatestWorkerEvents> getLatestTLogEvents(std::string const& eventName) const override;
};

// Periodically evaluates factors and logs the aggregate cluster-health metric.
class Monitor {
	std::vector<std::unique_ptr<IFactor>> factors;
	Reference<IWorkerEventProvider const> workerEventProvider;

	Monitor(std::vector<std::unique_ptr<IFactor>>&& factors, Reference<IWorkerEventProvider const> workerEventProvider);

public:
	explicit(false) Monitor(Monitor&&) noexcept = default;
	Monitor& operator=(Monitor&&) noexcept = default;

	static Monitor create(Reference<IWorkerEventProvider const> workerEventProvider);
	Future<Void> run();
};

} // namespace cluster_health
