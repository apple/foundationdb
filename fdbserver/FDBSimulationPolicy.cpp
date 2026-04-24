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

#include "fdbserver/FDBSimulationPolicy.h"

#include <algorithm>
#include <set>
#include <vector>

#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"

namespace {

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
		bool primaryTLogsDead = g_simulator->tLogWriteAntiQuorum
		                            ? !validateAllCombinations(badCombo,
		                                                       primaryProcessesDead,
		                                                       g_simulator->tLogPolicy,
		                                                       primaryLocalitiesLeft,
		                                                       g_simulator->tLogWriteAntiQuorum,
		                                                       false)
		                            : primaryProcessesDead.validate(g_simulator->tLogPolicy);
		if (g_simulator->usableRegions > 1 && g_simulator->remoteTLogPolicy && !primaryTLogsDead) {
			primaryTLogsDead = primaryProcessesDead.validate(g_simulator->remoteTLogPolicy);
		}
		return primaryTLogsDead || primaryProcessesDead.validate(g_simulator->storagePolicy);
	}

	bool canKillProcesses(std::vector<ProcessInfo*> const& availableProcesses,
	                      std::vector<ProcessInfo*> const& deadProcesses,
	                      KillType kt,
	                      KillType* newKillType) const override {
		bool canSurvive = true;
		int nQuorum = ((g_simulator->desiredCoordinators + 1) / 2) * 2 - 1;

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

			if (!g_simulator->primaryDcId.present() || g_simulator->usableRegions == 1) {
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
					if (processInfo->locality.dcId() == g_simulator->primaryDcId) {
						primaryProcessesLeft.add(processInfo->locality);
						primaryLocalitiesLeft.push_back(processInfo->locality);
					}
					if (processInfo->locality.dcId() == g_simulator->remoteDcId) {
						remoteProcessesLeft.add(processInfo->locality);
						remoteLocalitiesLeft.push_back(processInfo->locality);
					}
					if (std::find(g_simulator->primarySatelliteDcIds.begin(),
					              g_simulator->primarySatelliteDcIds.end(),
					              processInfo->locality.dcId()) != g_simulator->primarySatelliteDcIds.end()) {
						primarySatelliteProcessesLeft.add(processInfo->locality);
						primarySatelliteLocalitiesLeft.push_back(processInfo->locality);
					}
					if (std::find(g_simulator->remoteSatelliteDcIds.begin(),
					              g_simulator->remoteSatelliteDcIds.end(),
					              processInfo->locality.dcId()) != g_simulator->remoteSatelliteDcIds.end()) {
						remoteSatelliteProcessesLeft.add(processInfo->locality);
						remoteSatelliteLocalitiesLeft.push_back(processInfo->locality);
					}
				}
				for (auto processInfo : deadProcesses) {
					if (processInfo->locality.dcId() == g_simulator->primaryDcId) {
						primaryProcessesDead.add(processInfo->locality);
						primaryLocalitiesDead.push_back(processInfo->locality);
					}
					if (processInfo->locality.dcId() == g_simulator->remoteDcId) {
						remoteProcessesDead.add(processInfo->locality);
						remoteLocalitiesDead.push_back(processInfo->locality);
					}
					if (std::find(g_simulator->primarySatelliteDcIds.begin(),
					              g_simulator->primarySatelliteDcIds.end(),
					              processInfo->locality.dcId()) != g_simulator->primarySatelliteDcIds.end()) {
						primarySatelliteProcessesDead.add(processInfo->locality);
						primarySatelliteLocalitiesDead.push_back(processInfo->locality);
					}
					if (std::find(g_simulator->remoteSatelliteDcIds.begin(),
					              g_simulator->remoteSatelliteDcIds.end(),
					              processInfo->locality.dcId()) != g_simulator->remoteSatelliteDcIds.end()) {
						remoteSatelliteProcessesDead.add(processInfo->locality);
						remoteSatelliteLocalitiesDead.push_back(processInfo->locality);
					}
				}
			}

			bool tooManyDead = false;
			bool notEnoughLeft = false;
			bool primaryTLogsDead = g_simulator->tLogWriteAntiQuorum
			                            ? !validateAllCombinations(badCombo,
			                                                       primaryProcessesDead,
			                                                       g_simulator->tLogPolicy,
			                                                       primaryLocalitiesLeft,
			                                                       g_simulator->tLogWriteAntiQuorum,
			                                                       false)
			                            : primaryProcessesDead.validate(g_simulator->tLogPolicy);
			if (g_simulator->usableRegions > 1 && g_simulator->remoteTLogPolicy && !primaryTLogsDead) {
				primaryTLogsDead = primaryProcessesDead.validate(g_simulator->remoteTLogPolicy);
			}

			if (!g_simulator->primaryDcId.present()) {
				tooManyDead = primaryTLogsDead || primaryProcessesDead.validate(g_simulator->storagePolicy);
				notEnoughLeft = !primaryProcessesLeft.validate(g_simulator->tLogPolicy) ||
				                !primaryProcessesLeft.validate(g_simulator->storagePolicy);
			} else {
				bool remoteTLogsDead = g_simulator->tLogWriteAntiQuorum
				                           ? !validateAllCombinations(badCombo,
				                                                      remoteProcessesDead,
				                                                      g_simulator->tLogPolicy,
				                                                      remoteLocalitiesLeft,
				                                                      g_simulator->tLogWriteAntiQuorum,
				                                                      false)
				                           : remoteProcessesDead.validate(g_simulator->tLogPolicy);
				if (g_simulator->usableRegions > 1 && g_simulator->remoteTLogPolicy && !remoteTLogsDead) {
					remoteTLogsDead = remoteProcessesDead.validate(g_simulator->remoteTLogPolicy);
				}

				if (!g_simulator->hasSatelliteReplication) {
					if (g_simulator->usableRegions > 1) {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              (primaryProcessesDead.validate(g_simulator->storagePolicy) &&
						               remoteProcessesDead.validate(g_simulator->storagePolicy));
						notEnoughLeft = !primaryProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->remoteTLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->remoteTLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->storagePolicy);
					} else {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              primaryProcessesDead.validate(g_simulator->storagePolicy) ||
						              remoteProcessesDead.validate(g_simulator->storagePolicy);
						notEnoughLeft = !primaryProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->storagePolicy);
					}
				} else {
					bool primarySatelliteTLogsDead =
					    g_simulator->satelliteTLogWriteAntiQuorumFallback
					        ? !validateAllCombinations(badCombo,
					                                   primarySatelliteProcessesDead,
					                                   g_simulator->satelliteTLogPolicyFallback,
					                                   primarySatelliteLocalitiesLeft,
					                                   g_simulator->satelliteTLogWriteAntiQuorumFallback,
					                                   false)
					        : primarySatelliteProcessesDead.validate(g_simulator->satelliteTLogPolicyFallback);

					if (g_simulator->usableRegions > 1) {
						notEnoughLeft = !primaryProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->remoteTLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !primarySatelliteProcessesLeft.validate(g_simulator->satelliteTLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->remoteTLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !remoteSatelliteProcessesLeft.validate(g_simulator->satelliteTLogPolicy);
					} else {
						notEnoughLeft = !primaryProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !primaryProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !primarySatelliteProcessesLeft.validate(g_simulator->satelliteTLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->tLogPolicy) ||
						                !remoteProcessesLeft.validate(g_simulator->storagePolicy) ||
						                !remoteSatelliteProcessesLeft.validate(g_simulator->satelliteTLogPolicy);
					}

					if (g_simulator->usableRegions > 1 && g_simulator->allowLogSetKills) {
						tooManyDead = (primaryTLogsDead && primarySatelliteTLogsDead) || remoteTLogsDead ||
						              (primaryTLogsDead && remoteTLogsDead) ||
						              (primaryProcessesDead.validate(g_simulator->storagePolicy) &&
						               remoteProcessesDead.validate(g_simulator->storagePolicy));
					} else {
						tooManyDead = primaryTLogsDead || remoteTLogsDead ||
						              primaryProcessesDead.validate(g_simulator->storagePolicy) ||
						              remoteProcessesDead.validate(g_simulator->storagePolicy);
					}
				}
			}

			if (tooManyDead || (g_simulator->usableRegions > 1 && notEnoughLeft)) {
				newKt = KillType::Reboot;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("TLogPolicy", g_simulator->tLogPolicy->info())
				    .detail("Reason", "Too many dead processes that cannot satisfy tLogPolicy.");
			} else if ((kt < KillType::RebootAndDelete) && notEnoughLeft) {
				newKt = KillType::RebootAndDelete;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("TLogPolicy", g_simulator->tLogPolicy->info())
				    .detail("Reason", "Not enough tLog left to satisfy tLogPolicy.");
			} else if ((kt < KillType::RebootAndDelete) && (nQuorum > uniqueMachines.size())) {
				newKt = KillType::RebootAndDelete;
				canSurvive = false;
				TraceEvent("KillChanged")
				    .detail("KillType", kt)
				    .detail("NewKillType", newKt)
				    .detail("StoragePolicy", g_simulator->storagePolicy->info())
				    .detail("Quorum", nQuorum)
				    .detail("Machines", uniqueMachines.size())
				    .detail("Reason", "Not enough unique machines to perform auto configuration of coordinators.");
			} else {
				TraceEvent("CanSurviveKills")
				    .detail("KillType", kt)
				    .detail("TLogPolicy", g_simulator->tLogPolicy->info())
				    .detail("StoragePolicy", g_simulator->storagePolicy->info())
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
