/*
 * FDBSimulationPolicy.cpp
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

#include "fdbserver/core/FDBSimulationPolicy.h"

#include <algorithm>
#include <set>
#include <vector>

#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"

FDBExtraDatabaseMode stringToFDBExtraDatabaseMode(const std::string& databaseMode) {
	if (databaseMode == "Disabled") {
		return FDBExtraDatabaseMode::Disabled;
	} else if (databaseMode == "LocalOrSingle") {
		return FDBExtraDatabaseMode::LocalOrSingle;
	} else if (databaseMode == "Single") {
		return FDBExtraDatabaseMode::Single;
	} else if (databaseMode == "Local") {
		return FDBExtraDatabaseMode::Local;
	} else if (databaseMode == "Multiple") {
		return FDBExtraDatabaseMode::Multiple;
	} else {
		TraceEvent(SevError, "UnknownExtraDatabaseMode").detail("DatabaseMode", databaseMode);
		ASSERT(false);
		throw internal_error();
	}
}

namespace {

FDBSimulationPolicyState policyState;

class FDBSimulationPolicy final : public ISimulationPolicy {
public:
	bool datacenterDead(Optional<Standalone<StringRef>> dcId,
	                    std::vector<ProcessInfo*> const& allProcesses) const override {
		if (!dcId.present()) {
			return false;
		}

		LocalityGroup primaryProcessesDead;
		std::vector<LocalityData> primaryLocalitiesLeft;

		for (auto processInfo : allProcesses) {
			if (!processInfo->isSpawnedKVProcess() && processInfo->isAvailableClass() &&
			    processInfo->locality.dcId() == dcId) {
				if (processInfo->isExcluded() || processInfo->isCleared() || !processInfo->isAvailable()) {
					primaryProcessesDead.add(processInfo->locality);
				} else {
					primaryLocalitiesLeft.push_back(processInfo->locality);
				}
			}
		}

		std::vector<LocalityData> badCombo;
		auto const& state = fdbSimulationPolicyState();

		bool primaryTLogsDead = state.tLogWriteAntiQuorum ? !validateAllCombinations(badCombo,
		                                                                             primaryProcessesDead,
		                                                                             state.tLogPolicy,
		                                                                             primaryLocalitiesLeft,
		                                                                             state.tLogWriteAntiQuorum,
		                                                                             false)
		                                                  : primaryProcessesDead.validate(state.tLogPolicy);
		if (state.usableRegions > 1 && state.remoteTLogPolicy && !primaryTLogsDead) {
			primaryTLogsDead = primaryProcessesDead.validate(state.remoteTLogPolicy);
		}
		return primaryTLogsDead || primaryProcessesDead.validate(state.storagePolicy);
	}

	bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses,
	                      std::vector<ProcessInfo*> const& deadProcesses,
	                      KillType kt,
	                      KillType* newKillType) const override {
		auto const& state = fdbSimulationPolicyState();
		bool canSurvive = true;
		int nQuorum = ((state.desiredCoordinators + 1) / 2) * 2 - 1;

		KillType newKt = kt;
		if ((kt == KillType::KillInstantly) || (kt == KillType::InjectFaults) || (kt == KillType::FailDisk) ||
		    (kt == KillType::RebootAndDelete) || (kt == KillType::RebootProcessAndDelete)) {
			LocalityGroup primaryProcessesLeft, primaryProcessesDead;
			LocalityGroup primarySatelliteProcessesLeft, primarySatelliteProcessesDead;
			LocalityGroup remoteProcessesLeft, remoteProcessesDead;
			LocalityGroup remoteSatelliteProcessesLeft, remoteSatelliteProcessesDead;

			std::vector<LocalityData> primaryLocalitiesDead, primaryLocalitiesLeft;
			std::vector<LocalityData> primarySatelliteLocalitiesDead, primarySatelliteLocalitiesLeft;
			std::vector<LocalityData> remoteLocalitiesDead, remoteLocalitiesLeft;
			std::vector<LocalityData> remoteSatelliteLocalitiesDead, remoteSatelliteLocalitiesLeft;

			std::vector<LocalityData> badCombo;
			std::set<Optional<Standalone<StringRef>>> uniqueMachines;

			if (!state.primaryDcId.present() || state.usableRegions == 1) {
				for (auto processInfo : availableProcesses) {
					primaryProcessesLeft.add(processInfo->locality);
					primaryLocalitiesLeft.push_back(processInfo->locality);
					uniqueMachines.insert(processInfo->locality.zoneId());
				}
				for (auto processInfo : deadProcesses) {
					primaryProcessesDead.add(processInfo->locality);
					primaryLocalitiesDead.push_back(processInfo->locality);
				}
			} else {
				for (auto processInfo : availableProcesses) {
					uniqueMachines.insert(processInfo->locality.zoneId());
					if (processInfo->locality.dcId() == state.primaryDcId) {
						primaryProcessesLeft.add(processInfo->locality);
						primaryLocalitiesLeft.push_back(processInfo->locality);
					}
					if (processInfo->locality.dcId() == state.remoteDcId) {
						remoteProcessesLeft.add(processInfo->locality);
						remoteLocalitiesLeft.push_back(processInfo->locality);
					}
					if (std::find(state.primarySatelliteDcIds.begin(),
					              state.primarySatelliteDcIds.end(),
					              processInfo->locality.dcId()) != state.primarySatelliteDcIds.end()) {
						primarySatelliteProcessesLeft.add(processInfo->locality);
						primarySatelliteLocalitiesLeft.push_back(processInfo->locality);
					}
					if (std::find(state.remoteSatelliteDcIds.begin(),
					              state.remoteSatelliteDcIds.end(),
					              processInfo->locality.dcId()) != state.remoteSatelliteDcIds.end()) {
						remoteSatelliteProcessesLeft.add(processInfo->locality);
						remoteSatelliteLocalitiesLeft.push_back(processInfo->locality);
					}
				}
				for (auto processInfo : deadProcesses) {
					if (processInfo->locality.dcId() == state.primaryDcId) {
						primaryProcessesDead.add(processInfo->locality);
						primaryLocalitiesDead.push_back(processInfo->locality);
					}
					if (processInfo->locality.dcId() == state.remoteDcId) {
						remoteProcessesDead.add(processInfo->locality);
						remoteLocalitiesDead.push_back(processInfo->locality);
					}
					if (std::find(state.primarySatelliteDcIds.begin(),
					              state.primarySatelliteDcIds.end(),
					              processInfo->locality.dcId()) != state.primarySatelliteDcIds.end()) {
						primarySatelliteProcessesDead.add(processInfo->locality);
						primarySatelliteLocalitiesDead.push_back(processInfo->locality);
					}
					if (std::find(state.remoteSatelliteDcIds.begin(),
					              state.remoteSatelliteDcIds.end(),
					              processInfo->locality.dcId()) != state.remoteSatelliteDcIds.end()) {
						remoteSatelliteProcessesDead.add(processInfo->locality);
						remoteSatelliteLocalitiesDead.push_back(processInfo->locality);
					}
				}
			}

			bool tooManyDead = false;
			bool notEnoughLeft = false;
			bool primaryTLogsDead = state.tLogWriteAntiQuorum ? !validateAllCombinations(badCombo,
			                                                                             primaryProcessesDead,
			                                                                             state.tLogPolicy,
			                                                                             primaryLocalitiesLeft,
			                                                                             state.tLogWriteAntiQuorum,
			                                                                             false)
			                                                  : primaryProcessesDead.validate(state.tLogPolicy);
			if (state.usableRegions > 1 && state.remoteTLogPolicy && !primaryTLogsDead) {
				primaryTLogsDead = primaryProcessesDead.validate(state.remoteTLogPolicy);
			}

			if (!state.primaryDcId.present()) {
				tooManyDead = primaryTLogsDead || primaryProcessesDead.validate(state.storagePolicy);
				notEnoughLeft = !primaryProcessesLeft.validate(state.tLogPolicy) ||
				                !primaryProcessesLeft.validate(state.storagePolicy);
			} else {
				bool remoteTLogsDead = state.tLogWriteAntiQuorum ? !validateAllCombinations(badCombo,
				                                                                            remoteProcessesDead,
				                                                                            state.tLogPolicy,
				                                                                            remoteLocalitiesLeft,
				                                                                            state.tLogWriteAntiQuorum,
				                                                                            false)
				                                                 : remoteProcessesDead.validate(state.tLogPolicy);
				if (state.usableRegions > 1 && state.remoteTLogPolicy && !remoteTLogsDead) {
					remoteTLogsDead = remoteProcessesDead.validate(state.remoteTLogPolicy);
				}

				if (!state.hasSatelliteReplication) {
					if (state.usableRegions > 1) {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              (primaryProcessesDead.validate(state.storagePolicy) &&
						               remoteProcessesDead.validate(state.storagePolicy));
						notEnoughLeft = !primaryProcessesLeft.validate(state.tLogPolicy) ||
						                !primaryProcessesLeft.validate(state.remoteTLogPolicy) ||
						                !primaryProcessesLeft.validate(state.storagePolicy) ||
						                !remoteProcessesLeft.validate(state.tLogPolicy) ||
						                !remoteProcessesLeft.validate(state.remoteTLogPolicy) ||
						                !remoteProcessesLeft.validate(state.storagePolicy);
					} else {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              primaryProcessesDead.validate(state.storagePolicy) ||
						              remoteProcessesDead.validate(state.storagePolicy);
						notEnoughLeft = !primaryProcessesLeft.validate(state.tLogPolicy) ||
						                !primaryProcessesLeft.validate(state.storagePolicy) ||
						                !remoteProcessesLeft.validate(state.tLogPolicy) ||
						                !remoteProcessesLeft.validate(state.storagePolicy);
					}
				} else {
					bool primarySatelliteTLogsDead =
					    state.satelliteTLogWriteAntiQuorumFallback
					        ? !validateAllCombinations(badCombo,
					                                   primarySatelliteProcessesDead,
					                                   state.satelliteTLogPolicyFallback,
					                                   primarySatelliteLocalitiesLeft,
					                                   state.satelliteTLogWriteAntiQuorumFallback,
					                                   false)
					        : primarySatelliteProcessesDead.validate(state.satelliteTLogPolicyFallback);

					if (state.usableRegions > 1) {
						notEnoughLeft = !primaryProcessesLeft.validate(state.tLogPolicy) ||
						                !primaryProcessesLeft.validate(state.remoteTLogPolicy) ||
						                !primaryProcessesLeft.validate(state.storagePolicy) ||
						                !primarySatelliteProcessesLeft.validate(state.satelliteTLogPolicy) ||
						                !remoteProcessesLeft.validate(state.tLogPolicy) ||
						                !remoteProcessesLeft.validate(state.remoteTLogPolicy) ||
						                !remoteProcessesLeft.validate(state.storagePolicy) ||
						                !remoteSatelliteProcessesLeft.validate(state.satelliteTLogPolicy);
					} else {
						notEnoughLeft = !primaryProcessesLeft.validate(state.tLogPolicy) ||
						                !primaryProcessesLeft.validate(state.storagePolicy) ||
						                !primarySatelliteProcessesLeft.validate(state.satelliteTLogPolicy) ||
						                !remoteProcessesLeft.validate(state.tLogPolicy) ||
						                !remoteProcessesLeft.validate(state.storagePolicy) ||
						                !remoteSatelliteProcessesLeft.validate(state.satelliteTLogPolicy);
					}

					if (state.usableRegions > 1 && state.allowLogSetKills) {
						tooManyDead = (primaryTLogsDead && primarySatelliteTLogsDead) || remoteTLogsDead ||
						              (primaryTLogsDead && remoteTLogsDead) ||
						              (primaryProcessesDead.validate(state.storagePolicy) &&
						               remoteProcessesDead.validate(state.storagePolicy));
					} else {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              primaryProcessesDead.validate(state.storagePolicy) ||
						              remoteProcessesDead.validate(state.storagePolicy);
					}
				}
			}

			if (tooManyDead || (state.usableRegions > 1 && notEnoughLeft)) {
				newKt = KillType::Reboot;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("TLogPolicy", state.tLogPolicy->info())
				    .detail("Reason", "Too many dead processes that cannot satisfy tLogPolicy.");
			} else if ((kt < KillType::RebootAndDelete) && notEnoughLeft) {
				newKt = KillType::RebootAndDelete;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("TLogPolicy", state.tLogPolicy->info())
				    .detail("Reason", "Not enough tLog left to satisfy tLogPolicy.");
			} else if ((kt < KillType::RebootAndDelete) && (nQuorum > uniqueMachines.size())) {
				newKt = KillType::RebootAndDelete;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("StoragePolicy", state.storagePolicy->info())
				    .detail("Quorum", nQuorum)
				    .detail("Machines", uniqueMachines.size())
				    .detail("Reason", "Not enough unique machines to perform auto configuration of coordinators.");
			} else {
				TraceEvent("CanSurviveKills")
				    .detail("KillType", kt)
				    .detail("TLogPolicy", state.tLogPolicy->info())
				    .detail("StoragePolicy", state.storagePolicy->info())
				    .detail("Quorum", nQuorum)
				    .detail("Machines", uniqueMachines.size());
			}
		}
		if (newKillType) {
			*newKillType = newKt;
		}
		return canSurvive;
	}
};

} // namespace

void installFDBSimulationPolicy() {
	ASSERT(g_simulator);
	g_simulator->setSimulationPolicy(makeReference<FDBSimulationPolicy>());
}

FDBSimulationPolicyState& fdbSimulationPolicyState() {
	return policyState;
}

void updateFDBSimulationPolicy(DatabaseConfiguration const& configuration, bool restartingTest) {
	auto& state = fdbSimulationPolicyState();
	state.storagePolicy = configuration.storagePolicy;
	state.tLogPolicy = configuration.tLogPolicy;
	state.tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
	state.remoteTLogPolicy = configuration.remoteTLogPolicy;
	state.usableRegions = configuration.usableRegions;
	state.primaryDcId = Optional<Standalone<StringRef>>();
	state.remoteDcId = Optional<Standalone<StringRef>>();
	state.hasSatelliteReplication = false;
	state.satelliteTLogPolicy = Reference<IReplicationPolicy>();
	state.satelliteTLogPolicyFallback = Reference<IReplicationPolicy>();
	state.satelliteTLogWriteAntiQuorum = 0;
	state.satelliteTLogWriteAntiQuorumFallback = 0;
	state.primarySatelliteDcIds.clear();
	state.remoteSatelliteDcIds.clear();
	state.allowLogSetKills = true;

	if (!configuration.regions.empty()) {
		state.primaryDcId = configuration.regions[0].dcId;
		state.hasSatelliteReplication = configuration.regions[0].satelliteTLogReplicationFactor > 0;
		if (configuration.regions[0].satelliteTLogUsableDcsFallback > 0) {
			state.satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicyFallback;
			state.satelliteTLogWriteAntiQuorumFallback = configuration.regions[0].satelliteTLogWriteAntiQuorumFallback;
		} else {
			state.satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicy;
			state.satelliteTLogWriteAntiQuorumFallback = configuration.regions[0].satelliteTLogWriteAntiQuorum;
		}
		state.satelliteTLogPolicy = configuration.regions[0].satelliteTLogPolicy;
		state.satelliteTLogWriteAntiQuorum = configuration.regions[0].satelliteTLogWriteAntiQuorum;

		for (const auto& s : configuration.regions[0].satellites) {
			state.primarySatelliteDcIds.push_back(s.dcId);
		}
	}

	if (configuration.regions.size() == 2) {
		state.remoteDcId = configuration.regions[1].dcId;
		for (const auto& s : configuration.regions[1].satellites) {
			state.remoteSatelliteDcIds.push_back(s.dcId);
		}
	}

	if (restartingTest || state.usableRegions < 2 || !state.hasSatelliteReplication) {
		state.allowLogSetKills = false;
	}
}

void setFDBSimulationPolicyRemoteTLogPolicy(Reference<IReplicationPolicy> remoteTLogPolicy) {
	fdbSimulationPolicyState().remoteTLogPolicy = remoteTLogPolicy;
}
