/*
 * SimulatedCluster.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_SIMULATEDCLUSTER_H
#define FDBSERVER_SIMULATEDCLUSTER_H
#pragma once

#include <string>
#include <cstdint>

#include "fdbclient/DatabaseConfiguration.h"
#include "flow/Optional.h"

// The function at present is only called through "fdbserver -r simulation"
void simulationSetupAndRun(std::string const& dataFolder,
                           const char* const& testFile,
                           bool const& rebooting,
                           bool const& restoring,
                           std::string const& whitelistBinPath);

enum class SimulationStorageEngine : uint8_t {
	SSD = 0,
	MEMORY = 1,
	RADIX_TREE = 2,
	REDWOOD = 3,
	ROCKSDB = 4,
	SHARDED_ROCKSDB = 5,
	SIMULATION_STORAGE_ENGINE_INVALID_VALUE
};

class BasicTestConfig {
public:
	int minimumReplication = 0;
	int logAntiQuorum = -1;
	// Set true to simplify simulation configs for easier debugging
	bool simpleConfig = false;
	// set to true to force a single region config
	bool singleRegion = false;
	Optional<int> desiredTLogCount, commitProxyCount, grvProxyCount, resolverCount, machineCount, coordinators;
	Optional<SimulationStorageEngine> storageEngineType;
	// ASAN uses more memory, so adding too many machines can cause OOMs. Tests can set this if they need to lower
	// machineCount specifically for ASAN. Only has an effect if `machineCount` is set and this is an ASAN build.
	Optional<int> asanMachineCount;
};

struct BasicSimulationConfig {
	// simulation machine layout
	int datacenters;
	int replication_type;
	int machine_count; // Total, not per DC.
	int processes_per_machine;

	DatabaseConfiguration db;
};

BasicSimulationConfig generateBasicSimulationConfig(const BasicTestConfig& testConfig);
#endif
