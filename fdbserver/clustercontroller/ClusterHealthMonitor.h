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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

class IWorkerEventProvider {
public:
	virtual ~IWorkerEventProvider() = default;
	virtual void addref() const = 0;
	virtual void delref() const = 0;
	virtual Future<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const = 0;
};

class WorkerEventProvider final : public IWorkerEventProvider, public ReferenceCounted<WorkerEventProvider> {
	std::vector<WorkerDetails> workers;

public:
	void addref() const override { ReferenceCounted<WorkerEventProvider>::addref(); }
	void delref() const override { ReferenceCounted<WorkerEventProvider>::delref(); }
	void setWorkers(std::vector<WorkerDetails> workers);
	Future<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const override;
};

class FakeWorkerEventProvider final : public IWorkerEventProvider, public ReferenceCounted<FakeWorkerEventProvider> {
	std::map<std::string, LatestWorkerEvents> latestEventsByName;

public:
	void addref() const override { ReferenceCounted<FakeWorkerEventProvider>::addref(); }
	void delref() const override { ReferenceCounted<FakeWorkerEventProvider>::delref(); }
	void setLatestEvents(std::string eventName, LatestWorkerEvents latestEvents);
	Future<LatestWorkerEvents> getLatestEvents(std::string const& eventName) const override;
};

class IFactor {
public:
	virtual ~IFactor() = default;
	virtual std::string_view getName() const = 0;

	virtual Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) = 0;
};

class StorageSpaceFactor : public IFactor {
	double interventionThreshold;
	double criticalInterventionThreshold;

public:
	StorageSpaceFactor(double interventionThreshold, double criticalInterventionThreshold);

	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) override;
};

class TLogSpaceFactor : public IFactor {
	double interventionThreshold;
	double criticalInterventionThreshold;

public:
	TLogSpaceFactor(double interventionThreshold, double criticalInterventionThreshold);

	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider) override;
};

class Monitor {
	std::vector<std::unique_ptr<IFactor>> factors;
	Reference<IWorkerEventProvider const> workerEventProvider;

public:
	explicit Monitor(Reference<IWorkerEventProvider const>);
	Future<Void> run();
};

} // namespace cluster_health
