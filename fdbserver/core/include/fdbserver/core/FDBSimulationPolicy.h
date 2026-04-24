/*
 * FDBSimulationPolicy.h
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

#ifndef FDBSERVER_CORE_FDBSIMULATIONPOLICY_H
#define FDBSERVER_CORE_FDBSIMULATIONPOLICY_H
#pragma once

#include "fdbclient/DatabaseConfiguration.h"

struct FDBSimulationPolicyState {
	int desiredCoordinators = 1;
	Reference<IReplicationPolicy> storagePolicy;
	Reference<IReplicationPolicy> tLogPolicy;
	int32_t tLogWriteAntiQuorum = 0;
	Optional<Standalone<StringRef>> primaryDcId;
	Reference<IReplicationPolicy> remoteTLogPolicy;
	int32_t usableRegions = 1;
	bool allowLogSetKills = true;
	Optional<Standalone<StringRef>> remoteDcId;
	bool hasSatelliteReplication = false;
	Reference<IReplicationPolicy> satelliteTLogPolicy;
	Reference<IReplicationPolicy> satelliteTLogPolicyFallback;
	int32_t satelliteTLogWriteAntiQuorum = 0;
	int32_t satelliteTLogWriteAntiQuorumFallback = 0;
	std::vector<Optional<Standalone<StringRef>>> primarySatelliteDcIds;
	std::vector<Optional<Standalone<StringRef>>> remoteSatelliteDcIds;
	bool allowStorageMigrationTypeChange = false;
};

void installFDBSimulationPolicy();
FDBSimulationPolicyState& fdbSimulationPolicyState();
void updateFDBSimulationPolicy(DatabaseConfiguration const& configuration, bool restartingTest);
void setFDBSimulationPolicyRemoteTLogPolicy(Reference<IReplicationPolicy> remoteTLogPolicy);

#endif
