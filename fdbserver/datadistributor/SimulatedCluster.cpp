/*
 * SimulatedCluster.cpp
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

#include "fdbserver/datadistributor/SimulatedCluster.h"

#include <algorithm>
#include <map>
#include <string>

#include "fdbclient/GenericManagementAPI.h"

namespace {

std::string getRedundancyMode(const BasicTestConfig& testConfig) {
	if (testConfig.minimumReplication >= 3) {
		return "triple";
	}
	if (testConfig.minimumReplication == 2) {
		return "double";
	}
	return "single";
}

void applyConfigurationString(DatabaseConfiguration& db, const std::string& configMode) {
	std::map<std::string, std::string> configEntries;
	const auto result = buildConfiguration(configMode, configEntries);
	ASSERT(result != ConfigurationResult::NO_OPTIONS_PROVIDED);
	for (const auto& [configKey, configValue] : configEntries) {
		db.set(configKey, configValue);
	}
}

} // namespace

BasicSimulationConfig generateBasicSimulationConfig(const BasicTestConfig& testConfig) {
	BasicSimulationConfig config;

	config.datacenters =
	    testConfig.singleRegion || testConfig.simpleConfig ? 1 : std::max(1, testConfig.minimumReplication);
	config.replication_type = std::max(1, testConfig.minimumReplication);
	config.processes_per_machine = 1;

	applyConfigurationString(config.db, getRedundancyMode(testConfig));

	if (testConfig.simpleConfig) {
		config.db.desiredTLogCount = 1;
		config.db.commitProxyCount = 1;
		config.db.grvProxyCount = 1;
		config.db.resolverCount = 1;
	}

	if (testConfig.desiredTLogCount.present()) {
		config.db.desiredTLogCount = testConfig.desiredTLogCount.get();
	}
	if (testConfig.commitProxyCount.present()) {
		config.db.commitProxyCount = testConfig.commitProxyCount.get();
	}
	if (testConfig.grvProxyCount.present()) {
		config.db.grvProxyCount = testConfig.grvProxyCount.get();
	}
	if (testConfig.resolverCount.present()) {
		config.db.resolverCount = testConfig.resolverCount.get();
	}

	config.db.storageTeamSize = std::max(1, testConfig.minimumReplication);
	if (testConfig.logAntiQuorum != -1) {
		config.db.tLogWriteAntiQuorum = testConfig.logAntiQuorum;
	}

	if (config.datacenters == 1) {
		config.db.usableRegions = 1;
	}

	const int defaultMachineCount = std::max(config.db.storageTeamSize, 3) * config.datacenters;
	config.machine_count = testConfig.machineCount.present() ? testConfig.machineCount.get() : defaultMachineCount;
	config.machine_count = std::max(config.machine_count, config.db.storageTeamSize);

	return config;
}
