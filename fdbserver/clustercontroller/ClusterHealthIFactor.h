/*
 * ClusterHealthIFactor.h
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

#include <string_view>

#include "flow/BooleanParam.h"
#include "flow/flow.h"

namespace cluster_health {

enum class Level;
class IWorkerEventProvider;
FDB_BOOLEAN_PARAM(TrackCodeProbes);

class IFactor {
public:
	virtual ~IFactor() = default;
	virtual std::string_view getName() const = 0;
	virtual Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) = 0;
};

// Evaluates storage-server free-space pressure from StorageMetrics events.
class StorageSpaceFactor final : public IFactor {
	double interventionThreshold;
	double criticalInterventionThreshold;

public:
	StorageSpaceFactor(double interventionThreshold, double criticalInterventionThreshold);
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

// Evaluates TLog queue-disk free-space pressure from TLogMetrics events.
class TLogSpaceFactor final : public IFactor {
	double interventionThreshold;
	double criticalInterventionThreshold;

public:
	TLogSpaceFactor(double interventionThreshold, double criticalInterventionThreshold);
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

// Evaluates whether data distribution is restoring or has lost storage replication.
class StorageReplicationFactor final : public IFactor {
public:
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

// Evaluates cluster recovery progress from cluster controller state.
class RecoveryStateFactor final : public IFactor {
public:
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

// Evaluates whether any worker is currently reporting a latest process error.
class ProcessErrorsFactor final : public IFactor {
public:
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

// Evaluates ratekeeper throttling severity from RkUpdate events.
class RkThrottlingFactor final : public IFactor {
	double criticalTpsLimitToReleasedTpsRatioThreshold;

public:
	explicit RkThrottlingFactor(double criticalTpsLimitToReleasedTpsRatioThreshold);
	std::string_view getName() const override;
	Future<Level> fetchLevel(Reference<IWorkerEventProvider const> workerEventProvider, TrackCodeProbes) override;
};

} // namespace cluster_health
