/*
 * DDTeamCollection.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/DDTeamCollection.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DEFINE_BOOLEAN_PARAM(IsPrimary);

namespace {

// Helper function for STL containers, with flow-friendly error handling
template <class MapContainer, class K>
auto get(MapContainer& m, K const& k) -> decltype(m.at(k)) {
	auto it = m.find(k);
	ASSERT(it != m.end());
	return it->second;
}

} // namespace

class DDTeamCollectionImpl {
	ACTOR static Future<Void> checkAndRemoveInvalidLocalityAddr(DDTeamCollection* self) {
		state double start = now();
		state bool hasCorrectedLocality = false;

		loop {
			try {
				wait(delay(SERVER_KNOBS->DD_CHECK_INVALID_LOCALITY_DELAY, TaskPriority::DataDistribution));

				// Because worker's processId can be changed when its locality is changed, we cannot watch on the old
				// processId; This actor is inactive most time, so iterating all workers incurs little performance
				// overhead.
				state std::vector<ProcessData> workers = wait(getWorkers(self->cx));
				state std::set<AddressExclusion> existingAddrs;
				for (int i = 0; i < workers.size(); i++) {
					const ProcessData& workerData = workers[i];
					AddressExclusion addr(workerData.address.ip, workerData.address.port);
					existingAddrs.insert(addr);
					if (self->invalidLocalityAddr.count(addr) &&
					    self->isValidLocality(self->configuration.storagePolicy, workerData.locality)) {
						// The locality info on the addr has been corrected
						self->invalidLocalityAddr.erase(addr);
						hasCorrectedLocality = true;
						TraceEvent("InvalidLocalityCorrected").detail("Addr", addr.toString());
					}
				}

				wait(yield(TaskPriority::DataDistribution));

				// In case system operator permanently excludes workers on the address with invalid locality
				for (auto addr = self->invalidLocalityAddr.begin(); addr != self->invalidLocalityAddr.end();) {
					if (!existingAddrs.count(*addr)) {
						// The address no longer has a worker
						addr = self->invalidLocalityAddr.erase(addr);
						hasCorrectedLocality = true;
						TraceEvent("InvalidLocalityNoLongerExists").detail("Addr", addr->toString());
					} else {
						++addr;
					}
				}

				if (hasCorrectedLocality) {
					// Recruit on address who locality has been corrected
					self->restartRecruiting.trigger();
					hasCorrectedLocality = false;
				}

				if (self->invalidLocalityAddr.empty()) {
					break;
				}

				if (now() - start > 300) { // Report warning if invalid locality is not corrected within 300 seconds
					// The incorrect locality info has not been properly corrected in a reasonable time
					TraceEvent(SevWarn, "PersistentInvalidLocality")
					    .detail("Addresses", self->invalidLocalityAddr.size());
					start = now();
				}
			} catch (Error& e) {
				TraceEvent("CheckAndRemoveInvalidLocalityAddrRetry", self->distributorId).detail("Error", e.what());
			}
		}

		return Void();
	}

public:
	ACTOR static Future<Void> logOnCompletion(DDTeamCollection* self, Future<Void> signal) {
		wait(signal);
		wait(delay(SERVER_KNOBS->LOG_ON_COMPLETION_DELAY, TaskPriority::DataDistribution));

		if (!self->primary || self->configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", self->distributorId)
			    .detail("State", "Active")
			    .trackLatest(self->ddTrackerStartingEventHolder->trackingKey);
		}

		return Void();
	}

	ACTOR static Future<Void> interruptableBuildTeams(DDTeamCollection* self) {
		if (!self->addSubsetComplete.isSet()) {
			wait(addSubsetOfEmergencyTeams(self));
			self->addSubsetComplete.send(Void());
		}

		loop {
			choose {
				when(wait(self->buildTeams())) { return Void(); }
				when(wait(self->restartTeamBuilder.onTrigger())) {}
			}
		}
	}

	ACTOR static Future<Void> checkBuildTeams(DDTeamCollection* self) {
		wait(self->checkTeamDelay);
		while (!self->teamBuilder.isReady())
			wait(self->teamBuilder);

		if (self->doBuildTeams && self->readyToStart.isReady()) {
			self->doBuildTeams = false;
			self->teamBuilder = self->interruptableBuildTeams();
			wait(self->teamBuilder);
		}

		return Void();
	}

	// SOMEDAY: Make bestTeam better about deciding to leave a shard where it is (e.g. in PRIORITY_TEAM_HEALTHY case)
	//		    use keys, src, dest, metrics, priority, system load, etc.. to decide...
	ACTOR static Future<Void> getTeam(DDTeamCollection* self, GetTeamRequest req) {
		try {
			wait(self->checkBuildTeams());
			if (now() - self->lastMedianAvailableSpaceUpdate > SERVER_KNOBS->AVAILABLE_SPACE_UPDATE_DELAY) {
				self->lastMedianAvailableSpaceUpdate = now();
				std::vector<double> teamAvailableSpace;
				teamAvailableSpace.reserve(self->teams.size());
				for (const auto& team : self->teams) {
					if (team->isHealthy()) {
						teamAvailableSpace.push_back(team->getMinAvailableSpaceRatio());
					}
				}

				size_t pivot = teamAvailableSpace.size() / 2;
				if (teamAvailableSpace.size() > 1) {
					std::nth_element(
					    teamAvailableSpace.begin(), teamAvailableSpace.begin() + pivot, teamAvailableSpace.end());
					self->medianAvailableSpace =
					    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO,
					             std::min(SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO, teamAvailableSpace[pivot]));
				} else {
					self->medianAvailableSpace = SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO;
				}
				if (self->medianAvailableSpace < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
					TraceEvent(SevWarn, "DDTeamMedianAvailableSpaceTooSmall", self->distributorId)
					    .detail("MedianAvailableSpaceRatio", self->medianAvailableSpace)
					    .detail("TargetAvailableSpaceRatio", SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO)
					    .detail("Primary", self->primary);
					self->printDetailedTeamsInfo.trigger();
				}
			}

			bool foundSrc = false;
			for (int i = 0; i < req.src.size(); i++) {
				if (self->server_info.count(req.src[i])) {
					foundSrc = true;
					break;
				}
			}

			// Select the best team
			// Currently the metric is minimum used disk space (adjusted for data in flight)
			// Only healthy teams may be selected. The team has to be healthy at the moment we update
			//   shardsAffectedByTeamFailure or we could be dropping a shard on the floor (since team
			//   tracking is "edge triggered")
			// SOMEDAY: Account for capacity, load (when shardMetrics load is high)

			// self->teams.size() can be 0 under the ConfigureTest.txt test when we change configurations
			// The situation happens rarely. We may want to eliminate this situation someday
			if (!self->teams.size()) {
				req.reply.send(std::make_pair(Optional<Reference<IDataDistributionTeam>>(), foundSrc));
				return Void();
			}

			int64_t bestLoadBytes = 0;
			Optional<Reference<IDataDistributionTeam>> bestOption;
			std::vector<Reference<IDataDistributionTeam>> randomTeams;
			const std::set<UID> completeSources(req.completeSources.begin(), req.completeSources.end());

			// Note: this block does not apply any filters from the request
			if (!req.wantsNewServers) {
				for (int i = 0; i < req.completeSources.size(); i++) {
					if (!self->server_info.count(req.completeSources[i])) {
						continue;
					}
					auto& teamList = self->server_info[req.completeSources[i]]->teams;
					for (int j = 0; j < teamList.size(); j++) {
						bool found = true;
						auto serverIDs = teamList[j]->getServerIDs();
						for (int k = 0; k < teamList[j]->size(); k++) {
							if (!completeSources.count(serverIDs[k])) {
								found = false;
								break;
							}
						}
						if (found && teamList[j]->isHealthy()) {
							bestOption = teamList[j];
							req.reply.send(std::make_pair(bestOption, foundSrc));
							return Void();
						}
					}
				}
			}

			if (req.wantsTrueBest) {
				ASSERT(!bestOption.present());
				auto& startIndex =
				    req.preferLowerUtilization ? self->lowestUtilizationTeam : self->highestUtilizationTeam;
				if (startIndex >= self->teams.size()) {
					startIndex = 0;
				}

				int bestIndex = startIndex;
				for (int i = 0; i < self->teams.size(); i++) {
					int currentIndex = (startIndex + i) % self->teams.size();
					if (self->teams[currentIndex]->isHealthy() &&
					    (!req.preferLowerUtilization ||
					     self->teams[currentIndex]->hasHealthyAvailableSpace(self->medianAvailableSpace))) {
						int64_t loadBytes = self->teams[currentIndex]->getLoadBytes(true, req.inflightPenalty);
						if ((!bestOption.present() || (req.preferLowerUtilization && loadBytes < bestLoadBytes) ||
						     (!req.preferLowerUtilization && loadBytes > bestLoadBytes)) &&
						    (!req.teamMustHaveShards ||
						     self->shardsAffectedByTeamFailure->hasShards(ShardsAffectedByTeamFailure::Team(
						         self->teams[currentIndex]->getServerIDs(), self->primary)))) {
							bestLoadBytes = loadBytes;
							bestOption = self->teams[currentIndex];
							bestIndex = currentIndex;
						}
					}
				}

				startIndex = bestIndex;
			} else {
				int nTries = 0;
				while (randomTeams.size() < SERVER_KNOBS->BEST_TEAM_OPTION_COUNT &&
				       nTries < SERVER_KNOBS->BEST_TEAM_MAX_TEAM_TRIES) {
					// If unhealthy team is majority, we may not find an ok dest in this while loop
					Reference<IDataDistributionTeam> dest = deterministicRandom()->randomChoice(self->teams);

					bool ok = dest->isHealthy() && (!req.preferLowerUtilization ||
					                                dest->hasHealthyAvailableSpace(self->medianAvailableSpace));

					for (int i = 0; ok && i < randomTeams.size(); i++) {
						if (randomTeams[i]->getServerIDs() == dest->getServerIDs()) {
							ok = false;
							break;
						}
					}

					ok = ok && (!req.teamMustHaveShards ||
					            self->shardsAffectedByTeamFailure->hasShards(
					                ShardsAffectedByTeamFailure::Team(dest->getServerIDs(), self->primary)));

					if (ok)
						randomTeams.push_back(dest);
					else
						nTries++;
				}

				// Log BestTeamStuck reason when we have healthy teams but they do not have healthy free space
				if (randomTeams.empty() && !self->zeroHealthyTeams->get()) {
					self->bestTeamKeepStuckCount++;
					if (g_network->isSimulated()) {
						TraceEvent(SevWarn, "GetTeamReturnEmpty").detail("HealthyTeams", self->healthyTeamCount);
					}
				} else {
					self->bestTeamKeepStuckCount = 0;
				}

				for (int i = 0; i < randomTeams.size(); i++) {
					int64_t loadBytes = randomTeams[i]->getLoadBytes(true, req.inflightPenalty);
					if (!bestOption.present() || (req.preferLowerUtilization && loadBytes < bestLoadBytes) ||
					    (!req.preferLowerUtilization && loadBytes > bestLoadBytes)) {
						bestLoadBytes = loadBytes;
						bestOption = randomTeams[i];
					}
				}
			}

			// Note: req.completeSources can be empty and all servers (and server teams) can be unhealthy.
			// We will get stuck at this! This only happens when a DC fails. No need to consider it right now.
			// Note: this block does not apply any filters from the request
			if (!bestOption.present() && self->zeroHealthyTeams->get()) {
				// Attempt to find the unhealthy source server team and return it
				for (int i = 0; i < req.completeSources.size(); i++) {
					if (!self->server_info.count(req.completeSources[i])) {
						continue;
					}
					auto& teamList = self->server_info[req.completeSources[i]]->teams;
					for (int j = 0; j < teamList.size(); j++) {
						bool found = true;
						auto serverIDs = teamList[j]->getServerIDs();
						for (int k = 0; k < teamList[j]->size(); k++) {
							if (!completeSources.count(serverIDs[k])) {
								found = false;
								break;
							}
						}
						if (found) {
							bestOption = teamList[j];
							req.reply.send(std::make_pair(bestOption, foundSrc));
							return Void();
						}
					}
				}
			}
			// if (!bestOption.present()) {
			// 	TraceEvent("GetTeamRequest").detail("Request", req.getDesc());
			// 	self->traceAllInfo(true);
			// }

			req.reply.send(std::make_pair(bestOption, foundSrc));

			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				req.reply.sendError(e);
			throw;
		}
	}

	ACTOR static Future<Void> addSubsetOfEmergencyTeams(DDTeamCollection* self) {
		state int idx = 0;
		state std::vector<Reference<TCServerInfo>> servers;
		state std::vector<UID> serverIds;
		state Reference<LocalitySet> tempSet = Reference<LocalitySet>(new LocalityMap<UID>());
		state LocalityMap<UID>* tempMap = (LocalityMap<UID>*)tempSet.getPtr();

		for (; idx < self->badTeams.size(); idx++) {
			servers.clear();
			for (const auto& server : self->badTeams[idx]->getServers()) {
				if (server->inDesiredDC && !self->server_status.get(server->id).isUnhealthy()) {
					servers.push_back(server);
				}
			}

			// For the bad team that is too big (too many servers), we will try to find a subset of servers in the team
			// to construct a new healthy team, so that moving data to the new healthy team will not
			// cause too much data movement overhead
			// FIXME: This code logic can be simplified.
			if (servers.size() >= self->configuration.storageTeamSize) {
				bool foundTeam = false;
				for (int j = 0; j < servers.size() - self->configuration.storageTeamSize + 1 && !foundTeam; j++) {
					auto& serverTeams = servers[j]->teams;
					for (int k = 0; k < serverTeams.size(); k++) {
						auto& testTeam = serverTeams[k]->getServerIDs();
						bool allInTeam = true; // All servers in testTeam belong to the healthy servers
						for (int l = 0; l < testTeam.size(); l++) {
							bool foundServer = false;
							for (auto it : servers) {
								if (it->id == testTeam[l]) {
									foundServer = true;
									break;
								}
							}
							if (!foundServer) {
								allInTeam = false;
								break;
							}
						}
						if (allInTeam) {
							foundTeam = true;
							break;
						}
					}
				}
				if (!foundTeam) {
					if (self->satisfiesPolicy(servers)) {
						if (servers.size() == self->configuration.storageTeamSize ||
						    self->satisfiesPolicy(servers, self->configuration.storageTeamSize)) {
							servers.resize(self->configuration.storageTeamSize);
							self->addTeam(servers, true);
							// self->traceTeamCollectionInfo(); // Trace at the end of the function
						} else {
							tempSet->clear();
							for (auto it : servers) {
								tempMap->add(it->lastKnownInterface.locality, &it->id);
							}

							std::vector<LocalityEntry> resultEntries, forcedEntries;
							bool result = tempSet->selectReplicas(
							    self->configuration.storagePolicy, forcedEntries, resultEntries);
							ASSERT(result && resultEntries.size() == self->configuration.storageTeamSize);

							serverIds.clear();
							for (auto& it : resultEntries) {
								serverIds.push_back(*tempMap->getObject(it));
							}
							std::sort(serverIds.begin(), serverIds.end());
							self->addTeam(serverIds.begin(), serverIds.end(), true);
						}
					} else {
						serverIds.clear();
						for (auto it : servers) {
							serverIds.push_back(it->id);
						}
						TraceEvent(SevWarnAlways, "CannotAddSubset", self->distributorId)
						    .detail("Servers", describe(serverIds));
					}
				}
			}
			wait(yield());
		}

		// Trace and record the current number of teams for correctness test
		self->traceTeamCollectionInfo();

		return Void();
	}

	ACTOR static Future<Void> init(DDTeamCollection* self,
	                               Reference<InitialDataDistribution> initTeams,
	                               const DDEnabledState* ddEnabledState) {
		self->healthyZone.set(initTeams->initHealthyZoneValue);
		// SOMEDAY: If some servers have teams and not others (or some servers have more data than others) and there is
		// an address/locality collision, should we preferentially mark the least used server as undesirable?

		for (auto i = initTeams->allServers.begin(); i != initTeams->allServers.end(); ++i) {
			if (self->shouldHandleServer(i->first)) {
				if (!self->isValidLocality(self->configuration.storagePolicy, i->first.locality)) {
					TraceEvent(SevWarnAlways, "MissingLocality")
					    .detail("Server", i->first.uniqueID)
					    .detail("Locality", i->first.locality.toString());
					auto addr = i->first.stableAddress();
					self->invalidLocalityAddr.insert(AddressExclusion(addr.ip, addr.port));
					if (self->checkInvalidLocalities.isReady()) {
						self->checkInvalidLocalities = checkAndRemoveInvalidLocalityAddr(self);
						self->addActor.send(self->checkInvalidLocalities);
					}
				}
				self->addServer(i->first, i->second, self->serverTrackerErrorOut, 0, ddEnabledState);
			}
		}

		state std::set<std::vector<UID>>::iterator teamIter =
		    self->primary ? initTeams->primaryTeams.begin() : initTeams->remoteTeams.begin();
		state std::set<std::vector<UID>>::iterator teamIterEnd =
		    self->primary ? initTeams->primaryTeams.end() : initTeams->remoteTeams.end();
		for (; teamIter != teamIterEnd; ++teamIter) {
			self->addTeam(teamIter->begin(), teamIter->end(), true);
			wait(yield());
		}

		return Void();
	}

	ACTOR static Future<Void> buildTeams(DDTeamCollection* self) {
		state int desiredTeams;
		state int serverCount = 0;
		state int uniqueMachines = 0;
		state std::set<Optional<Standalone<StringRef>>> machines;

		// wait to see whether restartTeamBuilder is triggered
		wait(delay(0, g_network->getCurrentTask()));
		// make team builder don't build team during the interval between excluding the wiggled process and recruited a
		// new SS to avoid redundant teams
		while (self->pauseWiggle && !self->pauseWiggle->get() && self->waitUntilRecruited.get()) {
			choose {
				when(wait(self->waitUntilRecruited.onChange() || self->pauseWiggle->onChange())) {}
				when(wait(delay(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY, g_network->getCurrentTask()))) { break; }
			}
		}

		for (auto i = self->server_info.begin(); i != self->server_info.end(); ++i) {
			if (!self->server_status.get(i->first).isUnhealthy()) {
				++serverCount;
				LocalityData& serverLocation = i->second->lastKnownInterface.locality;
				machines.insert(serverLocation.zoneId());
			}
		}
		uniqueMachines = machines.size();
		TraceEvent("BuildTeams", self->distributorId)
		    .detail("ServerCount", self->server_info.size())
		    .detail("UniqueMachines", uniqueMachines)
		    .detail("Primary", self->primary)
		    .detail("StorageTeamSize", self->configuration.storageTeamSize);

		// If there are too few machines to even build teams or there are too few represented datacenters, build no new
		// teams
		if (uniqueMachines >= self->configuration.storageTeamSize) {
			desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * serverCount;
			int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * serverCount;

			// Exclude teams who have members in the wrong configuration, since we don't want these teams
			int teamCount = 0;
			int totalTeamCount = 0;
			int wigglingTeams = 0;
			for (int i = 0; i < self->teams.size(); ++i) {
				if (!self->teams[i]->isWrongConfiguration()) {
					if (self->teams[i]->isHealthy()) {
						teamCount++;
					}
					totalTeamCount++;
				}
				if (self->teams[i]->getPriority() == SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE) {
					wigglingTeams++;
				}
			}

			// teamsToBuild is calculated such that we will not build too many teams in the situation
			// when all (or most of) teams become unhealthy temporarily and then healthy again
			state int teamsToBuild;
			teamsToBuild = std::max(0, std::min(desiredTeams - teamCount, maxTeams - totalTeamCount));

			TraceEvent("BuildTeamsBegin", self->distributorId)
			    .detail("TeamsToBuild", teamsToBuild)
			    .detail("DesiredTeams", desiredTeams)
			    .detail("MaxTeams", maxTeams)
			    .detail("BadServerTeams", self->badTeams.size())
			    .detail("PerpetualWigglingTeams", wigglingTeams)
			    .detail("UniqueMachines", uniqueMachines)
			    .detail("TeamSize", self->configuration.storageTeamSize)
			    .detail("Servers", serverCount)
			    .detail("CurrentTrackedServerTeams", self->teams.size())
			    .detail("HealthyTeamCount", teamCount)
			    .detail("TotalTeamCount", totalTeamCount)
			    .detail("MachineTeamCount", self->machineTeams.size())
			    .detail("MachineCount", self->machine_info.size())
			    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER);

			self->lastBuildTeamsFailed = false;
			if (teamsToBuild > 0 || self->notEnoughTeamsForAServer()) {
				state std::vector<std::vector<UID>> builtTeams;

				// addTeamsBestOf() will not add more teams than needed.
				// If the team number is more than the desired, the extra teams are added in the code path when
				// a team is added as an initial team
				int addedTeams = self->addTeamsBestOf(teamsToBuild, desiredTeams, maxTeams);

				if (addedTeams <= 0 && self->teams.size() == 0) {
					TraceEvent(SevWarn, "NoTeamAfterBuildTeam", self->distributorId)
					    .detail("ServerTeamNum", self->teams.size())
					    .detail("Debug", "Check information below");
					// Debug: set true for traceAllInfo() to print out more information
					self->traceAllInfo();
				}
			} else {
				int totalHealthyMachineCount = self->calculateHealthyMachineCount();

				int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
				int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
				int healthyMachineTeamCount = self->getHealthyMachineTeamCount();

				std::pair<uint64_t, uint64_t> minMaxTeamsOnServer = self->calculateMinMaxServerTeamsOnServer();
				std::pair<uint64_t, uint64_t> minMaxMachineTeamsOnMachine =
				    self->calculateMinMaxMachineTeamsOnMachine();

				TraceEvent("TeamCollectionInfo", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("AddedTeams", 0)
				    .detail("TeamsToBuild", teamsToBuild)
				    .detail("CurrentServerTeams", self->teams.size())
				    .detail("DesiredTeams", desiredTeams)
				    .detail("MaxTeams", maxTeams)
				    .detail("StorageTeamSize", self->configuration.storageTeamSize)
				    .detail("CurrentMachineTeams", self->machineTeams.size())
				    .detail("CurrentHealthyMachineTeams", healthyMachineTeamCount)
				    .detail("DesiredMachineTeams", desiredMachineTeams)
				    .detail("MaxMachineTeams", maxMachineTeams)
				    .detail("TotalHealthyMachines", totalHealthyMachineCount)
				    .detail("MinTeamsOnServer", minMaxTeamsOnServer.first)
				    .detail("MaxTeamsOnServer", minMaxTeamsOnServer.second)
				    .detail("MinMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.first)
				    .detail("MaxMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.second)
				    .detail("DoBuildTeams", self->doBuildTeams)
				    .trackLatest(self->teamCollectionInfoEventHolder->trackingKey);
			}
		} else {
			self->lastBuildTeamsFailed = true;
		}

		self->evaluateTeamQuality();

		// Building teams can cause servers to become undesired, which can make teams unhealthy.
		// Let all of these changes get worked out before responding to the get team request
		wait(delay(0, TaskPriority::DataDistributionLaunch));

		return Void();
	}

	// Track a team and issue RelocateShards when the level of degradation changes
	// A badTeam can be unhealthy or just a redundantTeam removed by machineTeamRemover() or serverTeamRemover()
	ACTOR static Future<Void> teamTracker(DDTeamCollection* self,
	                                      Reference<TCTeamInfo> team,
	                                      bool badTeam,
	                                      bool redundantTeam) {
		state int lastServersLeft = team->size();
		state bool lastAnyUndesired = false;
		state bool lastAnyWigglingServer = false;
		state bool logTeamEvents =
		    g_network->isSimulated() || !badTeam || team->size() <= self->configuration.storageTeamSize;
		state bool lastReady = false;
		state bool lastHealthy;
		state bool lastOptimal;
		state bool lastWrongConfiguration = team->isWrongConfiguration();

		state bool lastZeroHealthy = self->zeroHealthyTeams->get();
		state bool firstCheck = true;

		state Future<Void> zeroServerLeftLogger;

		if (logTeamEvents) {
			TraceEvent("ServerTeamTrackerStarting", self->distributorId)
			    .detail("Reason", "Initial wait complete (sc)")
			    .detail("ServerTeam", team->getDesc());
		}
		self->priority_teams[team->getPriority()]++;

		try {
			loop {
				if (logTeamEvents) {
					TraceEvent("ServerTeamHealthChangeDetected", self->distributorId)
					    .detail("ServerTeam", team->getDesc())
					    .detail("Primary", self->primary)
					    .detail("IsReady", self->initialFailureReactionDelay.isReady());
					self->traceTeamCollectionInfo();
				}

				// Check if the number of degraded machines has changed
				state std::vector<Future<Void>> change;
				bool anyUndesired = false;
				bool anyWrongConfiguration = false;
				bool anyWigglingServer = false;
				int serversLeft = 0, serverUndesired = 0, serverWrongConf = 0, serverWiggling = 0;

				for (const UID& uid : team->getServerIDs()) {
					change.push_back(self->server_status.onChange(uid));
					auto& status = self->server_status.get(uid);
					if (!status.isFailed) {
						serversLeft++;
					}
					if (status.isUndesired) {
						anyUndesired = true;
						serverUndesired++;
					}
					if (status.isWrongConfiguration) {
						anyWrongConfiguration = true;
						serverWrongConf++;
					}
					if (status.isWiggling) {
						anyWigglingServer = true;
						serverWiggling++;
					}
				}

				if (serversLeft == 0) {
					logTeamEvents = true;
				}

				// Failed server should not trigger DD if SS failures are set to be ignored
				if (!badTeam && self->healthyZone.get().present() &&
				    (self->healthyZone.get().get() == ignoreSSFailuresZoneString)) {
					ASSERT_WE_THINK(serversLeft == self->configuration.storageTeamSize);
				}

				if (!self->initialFailureReactionDelay.isReady()) {
					change.push_back(self->initialFailureReactionDelay);
				}
				change.push_back(self->zeroHealthyTeams->onChange());

				bool healthy = !badTeam && !anyUndesired && serversLeft == self->configuration.storageTeamSize;
				team->setHealthy(healthy); // Unhealthy teams won't be chosen by bestTeam
				bool optimal = team->isOptimal() && healthy;
				bool containsFailed = self->teamContainsFailedServer(team);
				bool recheck = !healthy && (lastReady != self->initialFailureReactionDelay.isReady() ||
				                            (lastZeroHealthy && !self->zeroHealthyTeams->get()) || containsFailed);

				// TraceEvent("TeamHealthChangeDetected", self->distributorId)
				//     .detail("Team", team->getDesc())
				//     .detail("ServersLeft", serversLeft)
				//     .detail("LastServersLeft", lastServersLeft)
				//     .detail("AnyUndesired", anyUndesired)
				//     .detail("LastAnyUndesired", lastAnyUndesired)
				//     .detail("AnyWrongConfiguration", anyWrongConfiguration)
				//     .detail("LastWrongConfiguration", lastWrongConfiguration)
				//     .detail("Recheck", recheck)
				//     .detail("BadTeam", badTeam)
				//     .detail("LastZeroHealthy", lastZeroHealthy)
				//     .detail("ZeroHealthyTeam", self->zeroHealthyTeams->get());

				lastReady = self->initialFailureReactionDelay.isReady();
				lastZeroHealthy = self->zeroHealthyTeams->get();

				if (firstCheck) {
					firstCheck = false;
					if (healthy) {
						self->healthyTeamCount++;
						self->zeroHealthyTeams->set(false);
					}
					lastHealthy = healthy;

					if (optimal) {
						self->optimalTeamCount++;
						self->zeroOptimalTeams.set(false);
					}
					lastOptimal = optimal;
				}

				if (serversLeft != lastServersLeft || anyUndesired != lastAnyUndesired ||
				    anyWrongConfiguration != lastWrongConfiguration || anyWigglingServer != lastAnyWigglingServer ||
				    recheck) { // NOTE: do not check wrongSize
					if (logTeamEvents) {
						TraceEvent("ServerTeamHealthChanged", self->distributorId)
						    .detail("ServerTeam", team->getDesc())
						    .detail("ServersLeft", serversLeft)
						    .detail("LastServersLeft", lastServersLeft)
						    .detail("ContainsUndesiredServer", anyUndesired)
						    .detail("ContainsWigglingServer", anyWigglingServer)
						    .detail("HealthyTeamsCount", self->healthyTeamCount)
						    .detail("IsWrongConfiguration", anyWrongConfiguration);
					}

					team->setWrongConfiguration(anyWrongConfiguration);

					if (optimal != lastOptimal) {
						lastOptimal = optimal;
						self->optimalTeamCount += optimal ? 1 : -1;

						ASSERT(self->optimalTeamCount >= 0);
						self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
					}

					if (lastHealthy != healthy) {
						lastHealthy = healthy;
						// Update healthy team count when the team healthy changes
						self->healthyTeamCount += healthy ? 1 : -1;

						ASSERT(self->healthyTeamCount >= 0);
						self->zeroHealthyTeams->set(self->healthyTeamCount == 0);

						if (self->healthyTeamCount == 0) {
							TraceEvent(SevWarn, "ZeroServerTeamsHealthySignalling", self->distributorId)
							    .detail("SignallingTeam", team->getDesc())
							    .detail("Primary", self->primary);
						}

						if (logTeamEvents) {
							TraceEvent("ServerTeamHealthDifference", self->distributorId)
							    .detail("ServerTeam", team->getDesc())
							    .detail("LastOptimal", lastOptimal)
							    .detail("LastHealthy", lastHealthy)
							    .detail("Optimal", optimal)
							    .detail("OptimalTeamCount", self->optimalTeamCount);
						}
					}

					lastServersLeft = serversLeft;
					lastAnyUndesired = anyUndesired;
					lastWrongConfiguration = anyWrongConfiguration;
					lastAnyWigglingServer = anyWigglingServer;

					state int lastPriority = team->getPriority();
					if (team->size() == 0) {
						team->setPriority(SERVER_KNOBS->PRIORITY_POPULATE_REGION);
					} else if (serversLeft < self->configuration.storageTeamSize) {
						if (serversLeft == 0)
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_0_LEFT);
						else if (serversLeft == 1)
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_1_LEFT);
						else if (serversLeft == 2)
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_2_LEFT);
						else
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY);
					} else if (!badTeam && anyWigglingServer && serverWiggling == serverWrongConf &&
					           serverWiggling == serverUndesired) {
						// the wrong configured and undesired server is the wiggling server
						team->setPriority(SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE);

					} else if (badTeam || anyWrongConfiguration) {
						if (redundantTeam) {
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT);
						} else {
							team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY);
						}
					} else if (anyUndesired) {
						team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER);
					} else {
						team->setPriority(SERVER_KNOBS->PRIORITY_TEAM_HEALTHY);
					}

					if (lastPriority != team->getPriority()) {
						self->priority_teams[lastPriority]--;
						self->priority_teams[team->getPriority()]++;
						if (lastPriority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT &&
						    team->getPriority() < SERVER_KNOBS->PRIORITY_TEAM_0_LEFT) {
							zeroServerLeftLogger = Void();
						}
						if (logTeamEvents) {
							int dataLoss = team->getPriority() == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT;
							Severity severity = dataLoss ? SevWarnAlways : SevInfo;
							TraceEvent(severity, "ServerTeamPriorityChange", self->distributorId)
							    .detail("Priority", team->getPriority())
							    .detail("Info", team->getDesc())
							    .detail("ZeroHealthyServerTeams", self->zeroHealthyTeams->get())
							    .detail("Hint",
							            severity == SevWarnAlways ? "No replicas remain of some data"
							                                      : "The priority of this team changed");
							if (team->getPriority() == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT) {
								// 0 servers left in this team, data might be lost.
								zeroServerLeftLogger = zeroServerLeftLoggerActor(self, team);
							}
						}
					}

					lastZeroHealthy = self->zeroHealthyTeams
					                      ->get(); // set this again in case it changed from this teams health changing
					if ((self->initialFailureReactionDelay.isReady() && !self->zeroHealthyTeams->get()) ||
					    containsFailed) {

						std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor(
						    ShardsAffectedByTeamFailure::Team(team->getServerIDs(), self->primary));

						for (int i = 0; i < shards.size(); i++) {
							// Make it high priority to move keys off failed server or else RelocateShards may never be
							// addressed
							int maxPriority = containsFailed ? SERVER_KNOBS->PRIORITY_TEAM_FAILED : team->getPriority();
							// The shard split/merge and DD rebooting may make a shard mapped to multiple teams,
							// so we need to recalculate the shard's priority
							if (maxPriority < SERVER_KNOBS->PRIORITY_TEAM_FAILED) {
								std::pair<std::vector<ShardsAffectedByTeamFailure::Team>,
								          std::vector<ShardsAffectedByTeamFailure::Team>>
								    teams = self->shardsAffectedByTeamFailure->getTeamsFor(shards[i]);
								for (int j = 0; j < teams.first.size() + teams.second.size(); j++) {
									// t is the team in primary DC or the remote DC
									auto& t =
									    j < teams.first.size() ? teams.first[j] : teams.second[j - teams.first.size()];
									if (!t.servers.size()) {
										maxPriority = std::max(maxPriority, SERVER_KNOBS->PRIORITY_POPULATE_REGION);
										break;
									}

									auto tc = self->teamCollections[t.primary ? 0 : 1];
									if (tc == nullptr) {
										// teamTracker only works when all teamCollections are valid.
										// Always check if all teamCollections are valid, and throw error if any
										// teamCollection has been destructed, because the teamTracker can be triggered
										// after a DDTeamCollection was destroyed and before the other DDTeamCollection
										// is destroyed. Do not throw actor_cancelled() because flow treat it
										// differently.
										throw dd_cancelled();
									}
									ASSERT(tc->primary == t.primary);
									// tc->traceAllInfo();
									if (tc->server_info.count(t.servers[0])) {
										auto& info = tc->server_info[t.servers[0]];

										bool found = false;
										for (int k = 0; k < info->teams.size(); k++) {
											if (info->teams[k]->getServerIDs() == t.servers) {
												maxPriority = std::max(maxPriority, info->teams[k]->getPriority());
												found = true;

												break;
											}
										}

										// If we cannot find the team, it could be a bad team so assume unhealthy
										// priority
										if (!found) {
											// If the input team (in function parameters) is a redundant team, found
											// will be false We want to differentiate the redundant_team from
											// unhealthy_team in terms of relocate priority
											maxPriority =
											    std::max<int>(maxPriority,
											                  redundantTeam ? SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT
											                                : SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY);
										}
									} else {
										TEST(true); // A removed server is still associated with a team in
										            // ShardsAffectedByTeamFailure
									}
								}
							}

							RelocateShard rs;
							rs.keys = shards[i];
							rs.priority = maxPriority;

							self->output.send(rs);
							TraceEvent("SendRelocateToDDQueue", self->distributorId)
							    .suppressFor(1.0)
							    .detail("ServerPrimary", self->primary)
							    .detail("ServerTeam", team->getDesc())
							    .detail("KeyBegin", rs.keys.begin)
							    .detail("KeyEnd", rs.keys.end)
							    .detail("Priority", rs.priority)
							    .detail("ServerTeamFailedMachines", team->size() - serversLeft)
							    .detail("ServerTeamOKMachines", serversLeft);
						}
					} else {
						if (logTeamEvents) {
							TraceEvent("ServerTeamHealthNotReady", self->distributorId)
							    .detail("HealthyServerTeamCount", self->healthyTeamCount)
							    .detail("ServerTeamID", team->getTeamID());
						}
					}
				}

				// Wait for any of the machines to change status
				wait(quorum(change, 1));
				wait(yield());
			}
		} catch (Error& e) {
			if (logTeamEvents) {
				TraceEvent("TeamTrackerStopping", self->distributorId)
				    .detail("ServerPrimary", self->primary)
				    .detail("Team", team->getDesc())
				    .detail("Priority", team->getPriority());
			}
			self->priority_teams[team->getPriority()]--;
			if (team->isHealthy()) {
				self->healthyTeamCount--;
				ASSERT(self->healthyTeamCount >= 0);

				if (self->healthyTeamCount == 0) {
					TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->distributorId)
					    .detail("ServerPrimary", self->primary)
					    .detail("SignallingServerTeam", team->getDesc());
					self->zeroHealthyTeams->set(true);
				}
			}
			if (lastOptimal) {
				self->optimalTeamCount--;
				ASSERT(self->optimalTeamCount >= 0);
				self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
			}
			throw;
		}
	}

	// Check the status of a storage server.
	// Apply all requirements to the server and mark it as excluded if it fails to satisfies these requirements
	ACTOR static Future<Void> storageServerTracker(
	    DDTeamCollection* self,
	    Database cx,
	    TCServerInfo* server, // This actor is owned by this TCServerInfo, point to server_info[id]
	    Promise<Void> errorOut,
	    Version addedVersion,
	    const DDEnabledState* ddEnabledState,
	    bool isTss) {

		state Future<Void> failureTracker;
		state ServerStatus status(false, false, false, server->lastKnownInterface.locality);
		state bool lastIsUnhealthy = false;
		state Future<Void> metricsTracker = server->serverMetricsPolling();

		state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged = server->onInterfaceChanged;

		state Future<Void> storeTypeTracker = (isTss) ? Never() : self->keyValueStoreTypeTracker(server);
		state bool hasWrongDC = !self->isCorrectDC(server);
		state bool hasInvalidLocality =
		    !self->isValidLocality(self->configuration.storagePolicy, server->lastKnownInterface.locality);
		state int targetTeamNumPerServer =
		    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (self->configuration.storageTeamSize + 1)) / 2;

		try {
			loop {
				status.isUndesired = !self->disableFailingLaggingServers.get() && server->ssVersionTooFarBehind.get();
				status.isWrongConfiguration = false;
				status.isWiggling = false;
				hasWrongDC = !self->isCorrectDC(server);
				hasInvalidLocality =
				    !self->isValidLocality(self->configuration.storagePolicy, server->lastKnownInterface.locality);

				// If there is any other server on this exact NetworkAddress, this server is undesired and will
				// eventually be eliminated. This samAddress checking must be redo whenever the server's state (e.g.,
				// storeType, dcLocation, interface) is changed.
				state std::vector<Future<Void>> otherChanges;
				std::vector<Promise<Void>> wakeUpTrackers;
				for (const auto& i : self->server_and_tss_info) {
					if (i.second.getPtr() != server &&
					    i.second->lastKnownInterface.address() == server->lastKnownInterface.address()) {
						auto& statusInfo = self->server_status.get(i.first);
						TraceEvent("SameAddress", self->distributorId)
						    .detail("Failed", statusInfo.isFailed)
						    .detail("Undesired", statusInfo.isUndesired)
						    .detail("Server", server->id)
						    .detail("OtherServer", i.second->id)
						    .detail("Address", server->lastKnownInterface.address())
						    .detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
						    .detail("OtherNumShards",
						            self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id))
						    .detail("OtherHealthy", !self->server_status.get(i.second->id).isUnhealthy());
						// wait for the server's ip to be changed
						otherChanges.push_back(self->server_status.onChange(i.second->id));
						if (!self->server_status.get(i.second->id).isUnhealthy()) {
							if (self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id) >=
							    self->shardsAffectedByTeamFailure->getNumberOfShards(server->id)) {
								TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
								    .detail("Server", server->id)
								    .detail("Address", server->lastKnownInterface.address())
								    .detail("OtherServer", i.second->id)
								    .detail("NumShards",
								            self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
								    .detail("OtherNumShards",
								            self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id));

								status.isUndesired = true;
							} else
								wakeUpTrackers.push_back(i.second->wakeUpTracker);
						}
					}
				}

				for (auto& p : wakeUpTrackers) {
					if (!p.isSet())
						p.send(Void());
				}

				if (server->lastKnownClass.machineClassFitness(ProcessClass::Storage) > ProcessClass::UnsetFit) {
					// NOTE: Should not use self->healthyTeamCount > 0 in if statement, which will cause status bouncing
					// between healthy and unhealthy and result in OOM (See PR#2228).

					if (self->optimalTeamCount > 0) {
						TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
						    .detail("Server", server->id)
						    .detail("OptimalTeamCount", self->optimalTeamCount)
						    .detail("Fitness", server->lastKnownClass.machineClassFitness(ProcessClass::Storage));
						status.isUndesired = true;
					}
					otherChanges.push_back(self->zeroOptimalTeams.onChange());
				}

				// If this storage server has the wrong key-value store type, then mark it undesired so it will be
				// replaced with a server having the correct type
				if (hasWrongDC || hasInvalidLocality) {
					TraceEvent(SevWarn, "UndesiredDCOrLocality", self->distributorId)
					    .detail("Server", server->id)
					    .detail("WrongDC", hasWrongDC)
					    .detail("InvalidLocality", hasInvalidLocality);
					status.isUndesired = true;
					status.isWrongConfiguration = true;
				}
				if (server->wrongStoreTypeToRemove.get()) {
					TraceEvent(SevWarn, "WrongStoreTypeToRemove", self->distributorId)
					    .detail("Server", server->id)
					    .detail("StoreType", "?");
					status.isUndesired = true;
					status.isWrongConfiguration = true;
				}

				// An invalid wiggle server should set itself the right status. Otherwise, it cannot be re-included by
				// wiggler.
				auto invalidWiggleServer =
				    [](const AddressExclusion& addr, const DDTeamCollection* tc, const TCServerInfo* server) {
					    return server->lastKnownInterface.locality.processId() != tc->wigglingPid;
				    };
				// If the storage server is in the excluded servers list, it is undesired
				NetworkAddress a = server->lastKnownInterface.address();
				AddressExclusion worstAddr(a.ip, a.port);
				DDTeamCollection::Status worstStatus = self->excludedServers.get(worstAddr);

				if (worstStatus == DDTeamCollection::Status::WIGGLING && invalidWiggleServer(worstAddr, self, server)) {
					TraceEvent(SevInfo, "InvalidWiggleServer", self->distributorId)
					    .detail("Address", worstAddr.toString())
					    .detail("ProcessId", server->lastKnownInterface.locality.processId())
					    .detail("ValidWigglingId", self->wigglingPid.present());
					self->excludedServers.set(worstAddr, DDTeamCollection::Status::NONE);
					worstStatus = DDTeamCollection::Status::NONE;
				}
				otherChanges.push_back(self->excludedServers.onChange(worstAddr));

				for (int i = 0; i < 3; i++) {
					if (i > 0 && !server->lastKnownInterface.secondaryAddress().present()) {
						break;
					}
					AddressExclusion testAddr;
					if (i == 0)
						testAddr = AddressExclusion(a.ip);
					else if (i == 1)
						testAddr = AddressExclusion(server->lastKnownInterface.secondaryAddress().get().ip,
						                            server->lastKnownInterface.secondaryAddress().get().port);
					else if (i == 2)
						testAddr = AddressExclusion(server->lastKnownInterface.secondaryAddress().get().ip);
					DDTeamCollection::Status testStatus = self->excludedServers.get(testAddr);

					if (testStatus == DDTeamCollection::Status::WIGGLING &&
					    invalidWiggleServer(testAddr, self, server)) {
						TraceEvent(SevInfo, "InvalidWiggleServer", self->distributorId)
						    .detail("Address", testAddr.toString())
						    .detail("ProcessId", server->lastKnownInterface.locality.processId())
						    .detail("ValidWigglingId", self->wigglingPid.present());
						self->excludedServers.set(testAddr, DDTeamCollection::Status::NONE);
						testStatus = DDTeamCollection::Status::NONE;
					}

					if (testStatus > worstStatus) {
						worstStatus = testStatus;
						worstAddr = testAddr;
					}
					otherChanges.push_back(self->excludedServers.onChange(testAddr));
				}

				if (worstStatus != DDTeamCollection::Status::NONE) {
					TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
					    .detail("Server", server->id)
					    .detail("Excluded", worstAddr.toString());
					status.isUndesired = true;
					status.isWrongConfiguration = true;

					if (worstStatus == DDTeamCollection::Status::WIGGLING && !isTss) {
						status.isWiggling = true;
						TraceEvent("PerpetualWigglingStorageServer", self->distributorId)
						    .detail("Primary", self->primary)
						    .detail("Server", server->id)
						    .detail("ProcessId", server->lastKnownInterface.locality.processId())
						    .detail("Address", worstAddr.toString());
					} else if (worstStatus == DDTeamCollection::Status::FAILED && !isTss) {
						TraceEvent(SevWarn, "FailedServerRemoveKeys", self->distributorId)
						    .detail("Server", server->id)
						    .detail("Excluded", worstAddr.toString());
						wait(delay(0.0)); // Do not throw an error while still inside trackExcludedServers
						while (!ddEnabledState->isDDEnabled()) {
							wait(delay(1.0));
						}
						if (self->removeFailedServer.canBeSet()) {
							self->removeFailedServer.send(server->id);
						}
						throw movekeys_conflict();
					}
				}

				failureTracker = self->storageServerFailureTracker(server, cx, &status, addedVersion);
				// We need to recruit new storage servers if the key value store type has changed
				if (hasWrongDC || hasInvalidLocality || server->wrongStoreTypeToRemove.get()) {
					self->restartRecruiting.trigger();
				}

				if (lastIsUnhealthy && !status.isUnhealthy() && !isTss &&
				    (server->teams.size() < targetTeamNumPerServer || self->lastBuildTeamsFailed)) {
					self->doBuildTeams = true;
					self->restartTeamBuilder
					    .trigger(); // This does not trigger building teams if there exist healthy teams
				}
				lastIsUnhealthy = status.isUnhealthy();

				state bool recordTeamCollectionInfo = false;
				choose {
					when(wait(failureTracker || server->onTSSPairRemoved || server->killTss.getFuture())) {
						// The server is failed AND all data has been removed from it, so permanently remove it.
						TraceEvent("StatusMapChange", self->distributorId)
						    .detail("ServerID", server->id)
						    .detail("Status", "Removing");

						if (server->updated.canBeSet()) {
							server->updated.send(Void());
						}

						// Remove server from FF/serverList
						wait(removeStorageServer(
						    cx, server->id, server->lastKnownInterface.tssPairID, self->lock, ddEnabledState));

						TraceEvent("StatusMapChange", self->distributorId)
						    .detail("ServerID", server->id)
						    .detail("Status", "Removed");
						// Sets removeSignal (alerting dataDistributionTeamCollection to remove the storage server from
						// its own data structures)
						server->removed.send(Void());
						if (isTss) {
							self->removedTSS.send(server->id);
						} else {
							self->removedServers.send(server->id);
						}
						return Void();
					}
					when(std::pair<StorageServerInterface, ProcessClass> newInterface = wait(interfaceChanged)) {
						bool restartRecruiting =
						    newInterface.first.waitFailure.getEndpoint().getPrimaryAddress() !=
						    server->lastKnownInterface.waitFailure.getEndpoint().getPrimaryAddress();
						bool localityChanged = server->lastKnownInterface.locality != newInterface.first.locality;
						bool machineLocalityChanged = server->lastKnownInterface.locality.zoneId().get() !=
						                              newInterface.first.locality.zoneId().get();
						bool processIdChanged = server->lastKnownInterface.locality.processId().get() !=
						                        newInterface.first.locality.processId().get();
						TraceEvent("StorageServerInterfaceChanged", self->distributorId)
						    .detail("ServerID", server->id)
						    .detail("NewWaitFailureToken", newInterface.first.waitFailure.getEndpoint().token)
						    .detail("OldWaitFailureToken", server->lastKnownInterface.waitFailure.getEndpoint().token)
						    .detail("LocalityChanged", localityChanged)
						    .detail("ProcessIdChanged", processIdChanged)
						    .detail("MachineLocalityChanged", machineLocalityChanged);

						server->lastKnownInterface = newInterface.first;
						server->lastKnownClass = newInterface.second;
						if (localityChanged && !isTss) {
							TEST(true); // Server locality changed

							// The locality change of a server will affect machine teams related to the server if
							// the server's machine locality is changed
							if (machineLocalityChanged) {
								// First handle the impact on the machine of the server on the old locality
								Reference<TCMachineInfo> machine = server->machine;
								ASSERT(machine->serversOnMachine.size() >= 1);
								if (machine->serversOnMachine.size() == 1) {
									// When server is the last server on the machine,
									// remove the machine and the related machine team
									self->removeMachine(machine);
									server->machine = Reference<TCMachineInfo>();
								} else {
									// we remove the server from the machine, and
									// update locality entry for the machine and the global machineLocalityMap
									int serverIndex = -1;
									for (int i = 0; i < machine->serversOnMachine.size(); ++i) {
										if (machine->serversOnMachine[i].getPtr() == server) {
											// NOTE: now the machine's locality is wrong. Need update it whenever uses
											// it.
											serverIndex = i;
											machine->serversOnMachine[i] = machine->serversOnMachine.back();
											machine->serversOnMachine.pop_back();
											break; // Invariant: server only appear on the machine once
										}
									}
									ASSERT(serverIndex != -1);
									// NOTE: we do not update the machine's locality map even when
									// its representative server is changed.
								}

								// Second handle the impact on the destination machine where the server's new locality
								// is; If the destination machine is new, create one; otherwise, add server to an
								// existing one Update server's machine reference to the destination machine
								Reference<TCMachineInfo> destMachine =
								    self->checkAndCreateMachine(self->server_info[server->id]);
								ASSERT(destMachine.isValid());
							}

							// update pid2server_info if the process id has changed
							if (processIdChanged) {
								self->pid2server_info[newInterface.first.locality.processId().get()].push_back(
								    self->server_info[server->id]);
								// delete the old one
								auto& old_infos =
								    self->pid2server_info[server->lastKnownInterface.locality.processId().get()];
								for (int i = 0; i < old_infos.size(); ++i) {
									if (old_infos[i].getPtr() == server) {
										std::swap(old_infos[i--], old_infos.back());
										old_infos.pop_back();
									}
								}
							}
							// Ensure the server's server team belong to a machine team, and
							// Get the newBadTeams due to the locality change
							std::vector<Reference<TCTeamInfo>> newBadTeams;
							for (auto& serverTeam : server->teams) {
								if (!self->satisfiesPolicy(serverTeam->getServers())) {
									newBadTeams.push_back(serverTeam);
									continue;
								}
								if (machineLocalityChanged) {
									Reference<TCMachineTeamInfo> machineTeam =
									    self->checkAndCreateMachineTeam(serverTeam);
									ASSERT(machineTeam.isValid());
									serverTeam->machineTeam = machineTeam;
								}
							}

							server->inDesiredDC =
							    (self->includedDCs.empty() ||
							     std::find(self->includedDCs.begin(),
							               self->includedDCs.end(),
							               server->lastKnownInterface.locality.dcId()) != self->includedDCs.end());
							self->resetLocalitySet();

							bool addedNewBadTeam = false;
							for (auto it : newBadTeams) {
								if (self->removeTeam(it)) {
									self->addTeam(it->getServers(), true);
									addedNewBadTeam = true;
								}
							}
							if (addedNewBadTeam && self->badTeamRemover.isReady()) {
								TEST(true); // Server locality change created bad teams
								self->doBuildTeams = true;
								self->badTeamRemover = self->removeBadTeams();
								self->addActor.send(self->badTeamRemover);
								// The team number changes, so we need to update the team number info
								// self->traceTeamCollectionInfo();
								recordTeamCollectionInfo = true;
							}
							// The locality change of the server will invalid the server's old teams,
							// so we need to rebuild teams for the server
							self->doBuildTeams = true;
						}

						interfaceChanged = server->onInterfaceChanged;
						// Old failureTracker for the old interface will be actorCancelled since the handler of the old
						// actor now points to the new failure monitor actor.
						status = ServerStatus(status.isFailed,
						                      status.isUndesired,
						                      status.isWiggling,
						                      server->lastKnownInterface.locality);

						// self->traceTeamCollectionInfo();
						recordTeamCollectionInfo = true;
						// Restart the storeTracker for the new interface. This will cancel the previous
						// keyValueStoreTypeTracker
						storeTypeTracker = (isTss) ? Never() : self->keyValueStoreTypeTracker(server);
						hasWrongDC = !self->isCorrectDC(server);
						hasInvalidLocality = !self->isValidLocality(self->configuration.storagePolicy,
						                                            server->lastKnownInterface.locality);
						self->restartTeamBuilder.trigger();

						if (restartRecruiting)
							self->restartRecruiting.trigger();
					}
					when(wait(otherChanges.empty() ? Never() : quorum(otherChanges, 1))) {
						TraceEvent("SameAddressChangedStatus", self->distributorId).detail("ServerID", server->id);
					}
					when(wait(server->wrongStoreTypeToRemove.onChange())) {
						TraceEvent("UndesiredStorageServerTriggered", self->distributorId)
						    .detail("Server", server->id)
						    .detail("StoreType", server->storeType)
						    .detail("ConfigStoreType", self->configuration.storageServerStoreType)
						    .detail("WrongStoreTypeRemoved", server->wrongStoreTypeToRemove.get());
					}
					when(wait(server->wakeUpTracker.getFuture())) { server->wakeUpTracker = Promise<Void>(); }
					when(wait(storeTypeTracker)) {}
					when(wait(server->ssVersionTooFarBehind.onChange())) {}
					when(wait(self->disableFailingLaggingServers.onChange())) {}
				}

				if (recordTeamCollectionInfo) {
					self->traceTeamCollectionInfo();
				}
			}
		} catch (Error& e) {
			state Error err = e;
			TraceEvent("StorageServerTrackerCancelled", self->distributorId)
			    .suppressFor(1.0)
			    .detail("Primary", self->primary)
			    .detail("Server", server->id)
			    .error(e, /*includeCancelled*/ true);
			if (e.code() != error_code_actor_cancelled && errorOut.canBeSet()) {
				errorOut.sendError(e);
				wait(delay(0)); // Check for cancellation, since errorOut.sendError(e) could delete self
			}
			throw err;
		}
	}

	ACTOR static Future<Void> removeWrongStoreType(DDTeamCollection* self) {
		// Wait for storage servers to initialize its storeType
		wait(delay(SERVER_KNOBS->DD_REMOVE_STORE_ENGINE_DELAY));

		state Future<Void> fisServerRemoved = Never();

		TraceEvent("WrongStoreTypeRemoverStart", self->distributorId).detail("Servers", self->server_info.size());
		loop {
			// Removing a server here when DD is not healthy may lead to rare failure scenarios, for example,
			// the server with wrong storeType is shutting down while this actor marks it as to-be-removed.
			// In addition, removing servers cause extra data movement, which should be done while a cluster is healthy
			wait(waitUntilHealthy(self));

			bool foundSSToRemove = false;

			for (auto& server : self->server_info) {
				if (!server.second->isCorrectStoreType(self->configuration.storageServerStoreType)) {
					// Server may be removed due to failure while the wrongStoreTypeToRemove is sent to the
					// storageServerTracker. This race may cause the server to be removed before react to
					// wrongStoreTypeToRemove
					if (self->configuration.storageMigrationType == StorageMigrationType::AGGRESSIVE) {
						// if the Storage Migration type is aggressive, let DD remove SS with wrong storage type
						server.second->wrongStoreTypeToRemove.set(true);
					}
					// Otherwise, wait Perpetual Wiggler to wiggle the SS with wrong storage type
					foundSSToRemove = true;
					TraceEvent("WrongStoreTypeRemover", self->distributorId)
					    .detail("Server", server.first)
					    .detail("StoreType", server.second->storeType)
					    .detail("ConfiguredStoreType", self->configuration.storageServerStoreType);
					break;
				}
			}

			if (!foundSSToRemove) {
				break;
			}
		}

		return Void();
	}

	// NOTE: this actor returns when the cluster is healthy and stable (no server is expected to be removed in a period)
	// processingWiggle and processingUnhealthy indicate that some servers are going to be removed.
	ACTOR static Future<Void> waitUntilHealthy(DDTeamCollection* self, double extraDelay = 0, bool waitWiggle = false) {
		state int waitCount = 0;
		loop {
			while (self->zeroHealthyTeams->get() || self->processingUnhealthy->get() ||
			       (waitWiggle && self->processingWiggle->get())) {
				// processingUnhealthy: true when there exists data movement
				// processingWiggle: true when there exists data movement because we want to wiggle a SS
				TraceEvent("WaitUntilHealthyStalled", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("ZeroHealthy", self->zeroHealthyTeams->get())
				    .detail("ProcessingUnhealthy", self->processingUnhealthy->get())
				    .detail("ProcessingPerpetualWiggle", self->processingWiggle->get());
				wait(self->zeroHealthyTeams->onChange() || self->processingUnhealthy->onChange() ||
				     self->processingWiggle->onChange());
				waitCount = 0;
			}
			wait(delay(SERVER_KNOBS->DD_STALL_CHECK_DELAY,
			           TaskPriority::Low)); // After the team trackers wait on the initial failure reaction delay, they
			                                // yield. We want to make sure every tracker has had the opportunity to send
			                                // their relocations to the queue.
			if (!self->zeroHealthyTeams->get() && !self->processingUnhealthy->get() &&
			    (!waitWiggle || !self->processingWiggle->get())) {
				if (extraDelay <= 0.01 || waitCount >= 1) {
					// Return healthy if we do not need extraDelay or when DD are healthy in at least two consecutive
					// check
					return Void();
				} else {
					wait(delay(extraDelay, TaskPriority::Low));
					waitCount++;
				}
			}
		}
	}

	ACTOR static Future<Void> removeBadTeams(DDTeamCollection* self) {
		wait(self->initialFailureReactionDelay);
		wait(waitUntilHealthy(self));
		wait(self->addSubsetComplete.getFuture());
		TraceEvent("DDRemovingBadServerTeams", self->distributorId).detail("Primary", self->primary);
		for (auto it : self->badTeams) {
			it->tracker.cancel();
		}
		self->badTeams.clear();
		return Void();
	}

	ACTOR static Future<Void> zeroServerLeftLoggerActor(DDTeamCollection* self, Reference<TCTeamInfo> team) {
		wait(delay(SERVER_KNOBS->DD_TEAM_ZERO_SERVER_LEFT_LOG_DELAY));
		state std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor(
		    ShardsAffectedByTeamFailure::Team(team->getServerIDs(), self->primary));
		state std::vector<Future<StorageMetrics>> sizes;
		sizes.reserve(shards.size());

		for (auto const& shard : shards) {
			sizes.emplace_back(brokenPromiseToNever(self->getShardMetrics.getReply(GetMetricsRequest(shard))));
			TraceEvent(SevWarnAlways, "DDShardLost", self->distributorId)
			    .detail("ServerTeamID", team->getTeamID())
			    .detail("ShardBegin", shard.begin)
			    .detail("ShardEnd", shard.end);
		}

		wait(waitForAll(sizes));

		int64_t bytesLost = 0;
		for (auto const& size : sizes) {
			bytesLost += size.get().bytes;
		}

		TraceEvent(SevWarnAlways, "DDZeroServerLeftInTeam", self->distributorId)
		    .detail("Team", team->getDesc())
		    .detail("TotalBytesLost", bytesLost);

		return Void();
	}

	ACTOR static Future<Void> keyValueStoreTypeTracker(DDTeamCollection* self, TCServerInfo* server) {
		// Update server's storeType, especially when it was created
		state KeyValueStoreType type = wait(
		    brokenPromiseToNever(server->lastKnownInterface.getKeyValueStoreType.getReplyWithTaskID<KeyValueStoreType>(
		        TaskPriority::DataDistribution)));
		server->storeType = type;

		if (type != self->configuration.storageServerStoreType) {
			if (self->wrongStoreTypeRemover.isReady()) {
				self->wrongStoreTypeRemover = removeWrongStoreType(self);
				self->addActor.send(self->wrongStoreTypeRemover);
			}
		}

		return Never();
	}

	ACTOR static Future<Void> storageServerFailureTracker(DDTeamCollection* self,
	                                                      TCServerInfo* server,
	                                                      Database cx,
	                                                      ServerStatus* status,
	                                                      Version addedVersion) {
		state StorageServerInterface interf = server->lastKnownInterface;
		state int targetTeamNumPerServer =
		    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (self->configuration.storageTeamSize + 1)) / 2;
		loop {
			state bool inHealthyZone = false; // healthChanged actor will be Never() if this flag is true
			if (self->healthyZone.get().present()) {
				if (interf.locality.zoneId() == self->healthyZone.get()) {
					status->isFailed = false;
					inHealthyZone = true;
				} else if (self->healthyZone.get().get() == ignoreSSFailuresZoneString) {
					// Ignore all SS failures
					status->isFailed = false;
					inHealthyZone = true;
					TraceEvent("SSFailureTracker", self->distributorId)
					    .suppressFor(1.0)
					    .detail("IgnoredFailure", "BeforeChooseWhen")
					    .detail("ServerID", interf.id())
					    .detail("Status", status->toString());
				}
			}

			if (!interf.isTss()) {
				if (self->server_status.get(interf.id()).initialized) {
					bool unhealthy = self->server_status.get(interf.id()).isUnhealthy();
					if (unhealthy && !status->isUnhealthy()) {
						self->unhealthyServers--;
					}
					if (!unhealthy && status->isUnhealthy()) {
						self->unhealthyServers++;
					}
				} else if (status->isUnhealthy()) {
					self->unhealthyServers++;
				}
			}

			self->server_status.set(interf.id(), *status);
			if (status->isFailed) {
				self->restartRecruiting.trigger();
			}

			Future<Void> healthChanged = Never();
			if (status->isFailed) {
				ASSERT(!inHealthyZone);
				healthChanged = IFailureMonitor::failureMonitor().onStateEqual(interf.waitFailure.getEndpoint(),
				                                                               FailureStatus(false));
			} else if (!inHealthyZone) {
				healthChanged = waitFailureClientStrict(interf.waitFailure,
				                                        SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME,
				                                        TaskPriority::DataDistribution);
			}
			choose {
				when(wait(healthChanged)) {
					status->isFailed = !status->isFailed;
					if (status->isFailed && self->healthyZone.get().present()) {
						if (self->healthyZone.get().get() == ignoreSSFailuresZoneString) {
							// Ignore the failed storage server
							TraceEvent("SSFailureTracker", self->distributorId)
							    .detail("IgnoredFailure", "InsideChooseWhen")
							    .detail("ServerID", interf.id())
							    .detail("Status", status->toString());
							status->isFailed = false;
						} else if (self->clearHealthyZoneFuture.isReady()) {
							self->clearHealthyZoneFuture = clearHealthyZone(self->cx);
							TraceEvent("MaintenanceZoneCleared", self->distributorId).log();
							self->healthyZone.set(Optional<Key>());
						}
					}
					if (!status->isUnhealthy()) {
						// On server transistion from unhealthy -> healthy, trigger buildTeam check,
						// handles scenario when team building failed due to insufficient healthy servers.
						// Operaton cost is minimal if currentTeamCount == desiredTeamCount/maxTeamCount.
						self->doBuildTeams = true;
					}

					TraceEvent(SevDebug, "StatusMapChange", self->distributorId)
					    .detail("ServerID", interf.id())
					    .detail("Status", status->toString())
					    .detail(
					        "Available",
					        IFailureMonitor::failureMonitor().getState(interf.waitFailure.getEndpoint()).isAvailable());
				}
				when(wait(status->isUnhealthy() ? self->waitForAllDataRemoved(cx, interf.id(), addedVersion)
				                                : Never())) {
					break;
				}
				when(wait(self->healthyZone.onChange())) {}
			}
		}

		return Void(); // Don't ignore failures
	}

	ACTOR static Future<Void> waitForAllDataRemoved(DDTeamCollection* teams,
	                                                Database cx,
	                                                UID serverID,
	                                                Version addedVersion) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Version ver = wait(tr->getReadVersion());

				// we cannot remove a server immediately after adding it, because a perfectly timed cluster recovery
				// could cause us to not store the mutations sent to the short lived storage server.
				if (ver > addedVersion + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
					bool canRemove = wait(canRemoveStorageServer(tr, serverID));
					// TraceEvent("WaitForAllDataRemoved")
					//     .detail("Server", serverID)
					//     .detail("CanRemove", canRemove)
					//     .detail("Shards", teams->shardsAffectedByTeamFailure->getNumberOfShards(serverID));
					ASSERT(teams->shardsAffectedByTeamFailure->getNumberOfShards(serverID) >= 0);
					if (canRemove && teams->shardsAffectedByTeamFailure->getNumberOfShards(serverID) == 0) {
						return Void();
					}
				}

				// Wait for any change to the serverKeys for this server
				wait(delay(SERVER_KNOBS->ALL_DATA_REMOVED_DELAY, TaskPriority::DataDistribution));
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> machineTeamRemover(DDTeamCollection* self) {
		state int numMachineTeamRemoved = 0;
		loop {
			// In case the machineTeamRemover cause problems in production, we can disable it
			if (SERVER_KNOBS->TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER) {
				return Void(); // Directly return Void()
			}

			// To avoid removing machine teams too fast, which is unlikely happen though
			wait(delay(SERVER_KNOBS->TR_REMOVE_MACHINE_TEAM_DELAY, TaskPriority::DataDistribution));

			wait(waitUntilHealthy(self, SERVER_KNOBS->TR_REMOVE_SERVER_TEAM_EXTRA_DELAY));

			// Wait for the badTeamRemover() to avoid the potential race between adding the bad team (add the team
			// tracker) and remove bad team (cancel the team tracker).
			wait(self->badTeamRemover);

			state int healthyMachineCount = self->calculateHealthyMachineCount();
			// Check if all machines are healthy, if not, we wait for 1 second and loop back.
			// Eventually, all machines will become healthy.
			if (healthyMachineCount != self->machine_info.size()) {
				continue;
			}

			// From this point, all machine teams and server teams should be healthy, because we wait above
			// until processingUnhealthy is done, and all machines are healthy

			// Sanity check all machine teams are healthy
			//		int currentHealthyMTCount = self->getHealthyMachineTeamCount();
			//		if (currentHealthyMTCount != self->machineTeams.size()) {
			//			TraceEvent(SevError, "InvalidAssumption")
			//			    .detail("HealthyMachineCount", healthyMachineCount)
			//			    .detail("Machines", self->machine_info.size())
			//			    .detail("CurrentHealthyMTCount", currentHealthyMTCount)
			//			    .detail("MachineTeams", self->machineTeams.size());
			//			self->traceAllInfo(true);
			//		}

			// In most cases, all machine teams should be healthy teams at this point.
			int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * healthyMachineCount;
			int totalMTCount = self->machineTeams.size();
			// Pick the machine team to remove. After release-6.2 version,
			// we remove the machine team with most machine teams, the same logic as serverTeamRemover
			std::pair<Reference<TCMachineTeamInfo>, int> foundMTInfo =
			    SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS ? self->getMachineTeamWithMostMachineTeams()
			                                                    : self->getMachineTeamWithLeastProcessTeams();

			if (totalMTCount > desiredMachineTeams && foundMTInfo.first.isValid()) {
				Reference<TCMachineTeamInfo> mt = foundMTInfo.first;
				int minNumProcessTeams = foundMTInfo.second;
				ASSERT(mt.isValid());

				// Pick one process team, and mark it as a bad team
				// Remove the machine by removing its process team one by one
				Reference<TCTeamInfo> team;
				int teamIndex = 0;
				for (teamIndex = 0; teamIndex < mt->serverTeams.size(); ++teamIndex) {
					team = mt->serverTeams[teamIndex];
					ASSERT(team->machineTeam->machineIDs == mt->machineIDs); // Sanity check

					// Check if a server will have 0 team after the team is removed
					for (auto& s : team->getServers()) {
						if (s->teams.size() == 0) {
							TraceEvent(SevError, "MachineTeamRemoverTooAggressive", self->distributorId)
							    .detail("Server", s->id)
							    .detail("ServerTeam", team->getDesc());
							self->traceAllInfo(true);
						}
					}

					// The team will be marked as a bad team
					bool foundTeam = self->removeTeam(team);
					ASSERT(foundTeam == true);
					// removeTeam() has side effect of swapping the last element to the current pos
					// in the serverTeams vector in the machine team.
					--teamIndex;
					self->addTeam(team->getServers(), true, true);
					TEST(true); // Removed machine team
				}

				self->doBuildTeams = true;

				if (self->badTeamRemover.isReady()) {
					self->badTeamRemover = removeBadTeams(self);
					self->addActor.send(self->badTeamRemover);
				}

				TraceEvent("MachineTeamRemover", self->distributorId)
				    .detail("MachineTeamIDToRemove", mt->id.shortString())
				    .detail("MachineTeamToRemove", mt->getMachineIDsStr())
				    .detail("NumProcessTeamsOnTheMachineTeam", minNumProcessTeams)
				    .detail("CurrentMachineTeams", self->machineTeams.size())
				    .detail("DesiredMachineTeams", desiredMachineTeams);

				// Remove the machine team
				bool foundRemovedMachineTeam = self->removeMachineTeam(mt);
				// When we remove the last server team on a machine team in removeTeam(), we also remove the machine
				// team This is needed for removeTeam() functoin. So here the removeMachineTeam() should not find the
				// machine team
				ASSERT(foundRemovedMachineTeam);
				numMachineTeamRemoved++;
			} else {
				if (numMachineTeamRemoved > 0) {
					// Only trace the information when we remove a machine team
					TraceEvent("MachineTeamRemoverDone", self->distributorId)
					    .detail("HealthyMachines", healthyMachineCount)
					    // .detail("CurrentHealthyMachineTeams", currentHealthyMTCount)
					    .detail("CurrentMachineTeams", self->machineTeams.size())
					    .detail("DesiredMachineTeams", desiredMachineTeams)
					    .detail("NumMachineTeamsRemoved", numMachineTeamRemoved);
					self->traceTeamCollectionInfo();
					numMachineTeamRemoved = 0; // Reset the counter to avoid keep printing the message
				}
			}
		}
	}

	ACTOR static Future<Void> serverTeamRemover(DDTeamCollection* self) {
		state int numServerTeamRemoved = 0;
		loop {
			// In case the serverTeamRemover cause problems in production, we can disable it
			if (SERVER_KNOBS->TR_FLAG_DISABLE_SERVER_TEAM_REMOVER) {
				return Void(); // Directly return Void()
			}

			double removeServerTeamDelay = SERVER_KNOBS->TR_REMOVE_SERVER_TEAM_DELAY;
			if (g_network->isSimulated()) {
				// Speed up the team remover in simulation; otherwise,
				// it may time out because we need to remove hundreds of teams
				removeServerTeamDelay = removeServerTeamDelay / 100;
			}
			// To avoid removing server teams too fast, which is unlikely happen though
			wait(delay(removeServerTeamDelay, TaskPriority::DataDistribution));

			if (SERVER_KNOBS->PERPETUAL_WIGGLE_DISABLE_REMOVER && self->pauseWiggle) {
				while (!self->pauseWiggle->get()) {
					wait(self->pauseWiggle->onChange());
				}
			} else {
				wait(waitUntilHealthy(self, SERVER_KNOBS->TR_REMOVE_SERVER_TEAM_EXTRA_DELAY));
			}
			// Wait for the badTeamRemover() to avoid the potential race between
			// adding the bad team (add the team tracker) and remove bad team (cancel the team tracker).
			wait(self->badTeamRemover);

			// From this point, all server teams should be healthy, because we wait above
			// until processingUnhealthy is done, and all machines are healthy
			int desiredServerTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * self->server_info.size();
			int totalSTCount = self->teams.size();
			// Pick the server team whose members are on the most number of server teams, and mark it undesired
			std::pair<Reference<TCTeamInfo>, int> foundSTInfo = self->getServerTeamWithMostProcessTeams();

			if (totalSTCount > desiredServerTeams && foundSTInfo.first.isValid()) {
				ASSERT(foundSTInfo.first.isValid());
				Reference<TCTeamInfo> st = foundSTInfo.first;
				int maxNumProcessTeams = foundSTInfo.second;
				ASSERT(st.isValid());
				// The team will be marked as a bad team
				bool foundTeam = self->removeTeam(st);
				ASSERT(foundTeam == true);
				self->addTeam(st->getServers(), true, true);
				TEST(true); // Marked team as a bad team

				self->doBuildTeams = true;

				if (self->badTeamRemover.isReady()) {
					self->badTeamRemover = removeBadTeams(self);
					self->addActor.send(self->badTeamRemover);
				}

				TraceEvent("ServerTeamRemover", self->distributorId)
				    .detail("ServerTeamToRemove", st->getServerIDsStr())
				    .detail("ServerTeamID", st->getTeamID())
				    .detail("NumProcessTeamsOnTheServerTeam", maxNumProcessTeams)
				    .detail("CurrentServerTeams", self->teams.size())
				    .detail("DesiredServerTeams", desiredServerTeams);

				numServerTeamRemoved++;
			} else {
				if (numServerTeamRemoved > 0) {
					// Only trace the information when we remove a machine team
					TraceEvent("ServerTeamRemoverDone", self->distributorId)
					    .detail("CurrentServerTeams", self->teams.size())
					    .detail("DesiredServerTeams", desiredServerTeams)
					    .detail("NumServerTeamRemoved", numServerTeamRemoved);
					self->traceTeamCollectionInfo();
					numServerTeamRemoved = 0; // Reset the counter to avoid keep printing the message
				}
			}
		}
	}

	ACTOR static Future<Void> trackExcludedServers(DDTeamCollection* self) {
		// Fetch the list of excluded servers
		state ReadYourWritesTransaction tr(self->cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<RangeResult> fresultsExclude = tr.getRange(excludedServersKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fresultsFailed = tr.getRange(failedServersKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> flocalitiesExclude =
				    tr.getRange(excludedLocalityKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> flocalitiesFailed = tr.getRange(failedLocalityKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<std::vector<ProcessData>> fworkers = getWorkers(self->cx);
				wait(success(fresultsExclude) && success(fresultsFailed) && success(flocalitiesExclude) &&
				     success(flocalitiesFailed));

				state RangeResult excludedResults = fresultsExclude.get();
				ASSERT(!excludedResults.more && excludedResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult failedResults = fresultsFailed.get();
				ASSERT(!failedResults.more && failedResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult excludedLocalityResults = flocalitiesExclude.get();
				ASSERT(!excludedLocalityResults.more && excludedLocalityResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult failedLocalityResults = flocalitiesFailed.get();
				ASSERT(!failedLocalityResults.more && failedLocalityResults.size() < CLIENT_KNOBS->TOO_MANY);

				state std::set<AddressExclusion> excluded;
				state std::set<AddressExclusion> failed;
				for (const auto& r : excludedResults) {
					AddressExclusion addr = decodeExcludedServersKey(r.key);
					if (addr.isValid()) {
						excluded.insert(addr);
					}
				}
				for (const auto& r : failedResults) {
					AddressExclusion addr = decodeFailedServersKey(r.key);
					if (addr.isValid()) {
						failed.insert(addr);
					}
				}

				wait(success(fworkers));
				std::vector<ProcessData> workers = fworkers.get();
				for (const auto& r : excludedLocalityResults) {
					std::string locality = decodeExcludedLocalityKey(r.key);
					std::set<AddressExclusion> localityExcludedAddresses = getAddressesByLocality(workers, locality);
					excluded.insert(localityExcludedAddresses.begin(), localityExcludedAddresses.end());
				}
				for (const auto& r : failedLocalityResults) {
					std::string locality = decodeFailedLocalityKey(r.key);
					std::set<AddressExclusion> localityFailedAddresses = getAddressesByLocality(workers, locality);
					failed.insert(localityFailedAddresses.begin(), localityFailedAddresses.end());
				}

				// Reset and reassign self->excludedServers based on excluded, but we only
				// want to trigger entries that are different
				// Do not retrigger and double-overwrite failed or wiggling servers
				auto old = self->excludedServers.getKeys();
				for (const auto& o : old) {
					if (!excluded.count(o) && !failed.count(o) &&
					    !(self->excludedServers.count(o) &&
					      self->excludedServers.get(o) == DDTeamCollection::Status::WIGGLING)) {
						self->excludedServers.set(o, DDTeamCollection::Status::NONE);
					}
				}
				for (const auto& n : excluded) {
					if (!failed.count(n)) {
						self->excludedServers.set(n, DDTeamCollection::Status::EXCLUDED);
					}
				}

				for (const auto& f : failed) {
					self->excludedServers.set(f, DDTeamCollection::Status::FAILED);
				}

				TraceEvent("DDExcludedServersChanged", self->distributorId)
				    .detail("AddressesExcluded", excludedResults.size())
				    .detail("AddressesFailed", failedResults.size())
				    .detail("LocalitiesExcluded", excludedLocalityResults.size())
				    .detail("LocalitiesFailed", failedLocalityResults.size());

				self->restartRecruiting.trigger();
				state Future<Void> watchFuture =
				    tr.watch(excludedServersVersionKey) || tr.watch(failedServersVersionKey) ||
				    tr.watch(excludedLocalityVersionKey) || tr.watch(failedLocalityVersionKey);
				wait(tr.commit());
				wait(watchFuture);
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> updateNextWigglingStoragePID(DDTeamCollection* teamCollection) {
		state ReadYourWritesTransaction tr(teamCollection->cx);
		state Value writeValue = ""_sr;
		state const Key writeKey =
		    wigglingStorageServerKey.withSuffix(teamCollection->primary ? "/primary"_sr : "/remote"_sr);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> locality = wait(tr.get(perpetualStorageWiggleLocalityKey));

				if (teamCollection->pid2server_info.empty()) {
					writeValue = ""_sr;
				} else if (locality.present() && locality.get().toString().compare("0")) {
					// if perpetual_storage_wiggle_locality has value and not 0(disabled).
					state std::string localityKeyValue = locality.get().toString();
					ASSERT(isValidPerpetualStorageWiggleLocality(localityKeyValue));

					// get key and value from perpetual_storage_wiggle_locality.
					int split = localityKeyValue.find(':');
					state std::string localityKey = localityKeyValue.substr(0, split);
					state std::string localityValue = localityKeyValue.substr(split + 1);
					state Value prevValue;
					state int serverInfoSize = teamCollection->pid2server_info.size();

					Optional<Value> value = wait(tr.get(writeKey));
					if (value.present()) {
						prevValue = value.get();
					} else {
						// if value not present, check for locality match of the first entry in pid2server_info.
						auto& info_vec = teamCollection->pid2server_info.begin()->second;
						if (info_vec.size() &&
						    info_vec[0]->lastKnownInterface.locality.get(localityKey) == localityValue) {
							writeValue =
							    teamCollection->pid2server_info.begin()->first; // first entry locality matched.
						} else {
							prevValue = teamCollection->pid2server_info.begin()->first;
							serverInfoSize--;
						}
					}

					// If first entry of pid2server_info, did not match the locality.
					if (!(writeValue.compare(LiteralStringRef("")))) {
						auto nextIt = teamCollection->pid2server_info.upper_bound(prevValue);
						while (true) {
							if (nextIt == teamCollection->pid2server_info.end()) {
								nextIt = teamCollection->pid2server_info.begin();
							}

							if (nextIt->second.size() &&
							    nextIt->second[0]->lastKnownInterface.locality.get(localityKey) == localityValue) {
								writeValue = nextIt->first; // locality matched
								break;
							}
							serverInfoSize--;
							if (!serverInfoSize) {
								// None of the entries in pid2server_info matched the given locality.
								writeValue = LiteralStringRef("");
								TraceEvent("PerpetualNextWigglingStoragePIDNotFound", teamCollection->distributorId)
								    .detail("WriteValue", "No process matched the given perpetualStorageWiggleLocality")
								    .detail("PerpetualStorageWiggleLocality", localityKeyValue);
								break;
							}
							nextIt++;
						}
					}
				} else {
					Optional<Value> value = wait(tr.get(writeKey));
					Value pid = teamCollection->pid2server_info.begin()->first;
					if (value.present()) {
						auto nextIt = teamCollection->pid2server_info.upper_bound(value.get());
						if (nextIt == teamCollection->pid2server_info.end()) {
							writeValue = pid;
						} else {
							writeValue = nextIt->first;
						}
					} else {
						writeValue = pid;
					}
				}

				tr.set(writeKey, writeValue);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent(SevDebug, "PerpetualNextWigglingStoragePID", teamCollection->distributorId)
		    .detail("Primary", teamCollection->primary)
		    .detail("WriteValue", writeValue);

		return Void();
	}

	ACTOR static Future<Void> perpetualStorageWiggleIterator(DDTeamCollection* teamCollection,
	                                                         AsyncVar<bool>* stopSignal,
	                                                         FutureStream<Void> finishStorageWiggleSignal) {
		loop {
			choose {
				when(wait(stopSignal->onChange())) {}
				when(waitNext(finishStorageWiggleSignal)) {
					state bool takeRest = true; // delay to avoid delete and update ServerList too frequently
					while (takeRest) {
						wait(delayJittered(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY));
						// there must not have other teams to place wiggled data
						takeRest =
						    teamCollection->server_info.size() <= teamCollection->configuration.storageTeamSize ||
						    teamCollection->machine_info.size() < teamCollection->configuration.storageTeamSize;
						if (takeRest &&
						    teamCollection->configuration.storageMigrationType == StorageMigrationType::GRADUAL) {
							TraceEvent(SevWarn, "PerpetualWiggleSleep", teamCollection->distributorId)
							    .suppressFor(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY * 4)
							    .detail("ServerSize", teamCollection->server_info.size())
							    .detail("MachineSize", teamCollection->machine_info.size())
							    .detail("StorageTeamSize", teamCollection->configuration.storageTeamSize);
						}
					}
					wait(updateNextWigglingStoragePID(teamCollection));
				}
			}
			if (stopSignal->get()) {
				break;
			}
		}

		return Void();
	}

	ACTOR static Future<std::pair<Future<Void>, Value>> watchPerpetualStoragePIDChange(DDTeamCollection* self) {
		state ReadYourWritesTransaction tr(self->cx);
		state Future<Void> watchFuture;
		state Value ret;
		state const Key readKey = wigglingStorageServerKey.withSuffix(self->primary ? "/primary"_sr : "/remote"_sr);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> value = wait(tr.get(readKey));
				if (value.present()) {
					ret = value.get();
				}
				watchFuture = tr.watch(readKey);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return std::make_pair(watchFuture, ret);
	}

	ACTOR static Future<Void> clusterHealthCheckForPerpetualWiggle(DDTeamCollection* self, int* extraTeamCount) {
		state int pausePenalty = 1;
		loop {
			Promise<int> countp;
			self->getUnhealthyRelocationCount.send(countp);
			int count = wait(countp.getFuture());
			// pause wiggle when
			// a. DDQueue is busy with unhealthy relocation request
			// b. healthy teams are not enough
			// c. the overall disk space is not enough
			if (count >= SERVER_KNOBS->DD_STORAGE_WIGGLE_PAUSE_THRESHOLD || self->healthyTeamCount <= *extraTeamCount ||
			    self->bestTeamKeepStuckCount > SERVER_KNOBS->DD_STORAGE_WIGGLE_STUCK_THRESHOLD) {
				// if we pause wiggle not because the reason a, increase extraTeamCount. This helps avoid oscillation
				// between pause and non-pause status.
				if ((self->healthyTeamCount <= *extraTeamCount ||
				     self->bestTeamKeepStuckCount > SERVER_KNOBS->DD_STORAGE_WIGGLE_PAUSE_THRESHOLD) &&
				    !self->pauseWiggle->get()) {
					*extraTeamCount = std::min(*extraTeamCount + pausePenalty, (int)self->teams.size());
					pausePenalty = std::min(pausePenalty * 2, (int)self->teams.size());
				}
				self->pauseWiggle->set(true);
			} else {
				self->pauseWiggle->set(false);
			}
			wait(delay(SERVER_KNOBS->CHECK_TEAM_DELAY, TaskPriority::DataDistributionLow));
		}
	}

	ACTOR static Future<Void> perpetualStorageWiggler(DDTeamCollection* self,
	                                                  AsyncVar<bool>* stopSignal,
	                                                  PromiseStream<Void> finishStorageWiggleSignal) {
		state Future<Void> watchFuture = Never();
		state Future<Void> moveFinishFuture = Never();
		state int extraTeamCount = 0;
		state Future<Void> ddQueueCheck = clusterHealthCheckForPerpetualWiggle(self, &extraTeamCount);
		state int movingCount = 0;
		state std::pair<Future<Void>, Value> res = wait(watchPerpetualStoragePIDChange(self));
		ASSERT(!self->wigglingPid.present()); // only single process wiggle is allowed
		self->wigglingPid = Optional<Key>(res.second);

		loop {
			if (self->wigglingPid.present()) {
				state StringRef pid = self->wigglingPid.get();
				if (self->pauseWiggle->get()) {
					TEST(true); // paused because cluster is unhealthy
					moveFinishFuture = Never();
					self->includeStorageServersForWiggle();
					TraceEvent(self->configuration.storageMigrationType == StorageMigrationType::AGGRESSIVE ? SevInfo
					                                                                                        : SevWarn,
					           "PerpetualStorageWigglePause",
					           self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("ProcessId", pid)
					    .detail("BestTeamKeepStuckCount", self->bestTeamKeepStuckCount)
					    .detail("ExtraHealthyTeamCount", extraTeamCount)
					    .detail("HealthyTeamCount", self->healthyTeamCount)
					    .detail("StorageCount", movingCount);
				} else {
					TEST(true); // start wiggling
					choose {
						when(wait(waitUntilHealthy(self))) {
							auto fv = self->excludeStorageServersForWiggle(pid);
							movingCount = fv.size();
							moveFinishFuture = waitForAll(fv);
							TraceEvent("PerpetualStorageWiggleStart", self->distributorId)
							    .detail("Primary", self->primary)
							    .detail("ProcessId", pid)
							    .detail("ExtraHealthyTeamCount", extraTeamCount)
							    .detail("HealthyTeamCount", self->healthyTeamCount)
							    .detail("StorageCount", movingCount);
						}
						when(wait(self->pauseWiggle->onChange())) { continue; }
					}
				}
			}

			choose {
				when(wait(watchFuture)) {
					ASSERT(!self->wigglingPid.present()); // the previous wiggle must be finished
					watchFuture = Never();
					// read new pid and set the next watch Future
					wait(store(res, watchPerpetualStoragePIDChange(self)));
					self->wigglingPid = Optional<Key>(res.second);

					// random delay
					wait(delayJittered(5.0, TaskPriority::DataDistributionLow));
				}
				when(wait(moveFinishFuture)) {
					ASSERT(self->wigglingPid.present());
					StringRef pid = self->wigglingPid.get();
					TEST(pid != LiteralStringRef("")); // finish wiggling this process

					self->waitUntilRecruited.set(true);
					self->restartTeamBuilder.trigger();

					moveFinishFuture = Never();
					self->includeStorageServersForWiggle();
					TraceEvent("PerpetualStorageWiggleFinish", self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("ProcessId", pid.toString())
					    .detail("StorageCount", movingCount);

					self->wigglingPid.reset();
					watchFuture = res.first;
					finishStorageWiggleSignal.send(Void());
					extraTeamCount = std::max(0, extraTeamCount - 1);
				}
				when(wait(ddQueueCheck || self->pauseWiggle->onChange() || stopSignal->onChange())) {}
			}

			if (stopSignal->get()) {
				break;
			}
		}

		if (self->wigglingPid.present()) {
			self->includeStorageServersForWiggle();
			TraceEvent("PerpetualStorageWiggleExitingPause", self->distributorId)
			    .detail("Primary", self->primary)
			    .detail("ProcessId", self->wigglingPid.get());
			self->wigglingPid.reset();
		}

		return Void();
	}

	// This coroutine sets a watch to monitor the value change of `perpetualStorageWiggleKey` which is controlled by
	// command `configure perpetual_storage_wiggle=$value` if the value is 1, this actor start 2 actors,
	// `perpetualStorageWiggleIterator` and `perpetualStorageWiggler`. Otherwise, it sends stop signal to them.
	ACTOR static Future<Void> monitorPerpetualStorageWiggle(DDTeamCollection* teamCollection) {
		state int speed = 0;
		state AsyncVar<bool> stopWiggleSignal(true);
		state PromiseStream<Void> finishStorageWiggleSignal;
		state SignalableActorCollection collection;
		teamCollection->pauseWiggle = makeReference<AsyncVar<bool>>(true);

		loop {
			state ReadYourWritesTransaction tr(teamCollection->cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					Optional<Standalone<StringRef>> value = wait(tr.get(perpetualStorageWiggleKey));

					if (value.present()) {
						speed = std::stoi(value.get().toString());
					}
					state Future<Void> watchFuture = tr.watch(perpetualStorageWiggleKey);
					wait(tr.commit());

					ASSERT(speed == 1 || speed == 0);
					if (speed == 1 && stopWiggleSignal.get()) { // avoid duplicated start
						stopWiggleSignal.set(false);
						collection.add(teamCollection->perpetualStorageWiggleIterator(
						    &stopWiggleSignal, finishStorageWiggleSignal.getFuture()));
						collection.add(
						    teamCollection->perpetualStorageWiggler(&stopWiggleSignal, finishStorageWiggleSignal));
						TraceEvent("PerpetualStorageWiggleOpen", teamCollection->distributorId)
						    .detail("Primary", teamCollection->primary);
					} else if (speed == 0) {
						if (!stopWiggleSignal.get()) {
							stopWiggleSignal.set(true);
							wait(collection.signalAndReset());
							teamCollection->pauseWiggle->set(true);
						}
						TraceEvent("PerpetualStorageWiggleClose", teamCollection->distributorId)
						    .detail("Primary", teamCollection->primary);
					}
					wait(watchFuture);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> waitHealthyZoneChange(DDTeamCollection* self) {
		state ReadYourWritesTransaction tr(self->cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> val = wait(tr.get(healthyZoneKey));
				state Future<Void> healthyZoneTimeout = Never();
				if (val.present()) {
					auto p = decodeHealthyZoneValue(val.get());
					if (p.first == ignoreSSFailuresZoneString) {
						// healthyZone is now overloaded for DD diabling purpose, which does not timeout
						TraceEvent("DataDistributionDisabledForStorageServerFailuresStart", self->distributorId).log();
						healthyZoneTimeout = Never();
					} else if (p.second > tr.getReadVersion().get()) {
						double timeoutSeconds =
						    (p.second - tr.getReadVersion().get()) / (double)SERVER_KNOBS->VERSIONS_PER_SECOND;
						healthyZoneTimeout = delay(timeoutSeconds, TaskPriority::DataDistribution);
						if (self->healthyZone.get() != p.first) {
							TraceEvent("MaintenanceZoneStart", self->distributorId)
							    .detail("ZoneID", printable(p.first))
							    .detail("EndVersion", p.second)
							    .detail("Duration", timeoutSeconds);
							self->healthyZone.set(p.first);
						}
					} else if (self->healthyZone.get().present()) {
						// maintenance hits timeout
						TraceEvent("MaintenanceZoneEndTimeout", self->distributorId).log();
						self->healthyZone.set(Optional<Key>());
					}
				} else if (self->healthyZone.get().present()) {
					// `healthyZone` has been cleared
					if (self->healthyZone.get().get() == ignoreSSFailuresZoneString) {
						TraceEvent("DataDistributionDisabledForStorageServerFailuresEnd", self->distributorId).log();
					} else {
						TraceEvent("MaintenanceZoneEndManualClear", self->distributorId).log();
					}
					self->healthyZone.set(Optional<Key>());
				}

				state Future<Void> watchFuture = tr.watch(healthyZoneKey);
				wait(tr.commit());
				wait(watchFuture || healthyZoneTimeout);
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> monitorStorageServerRecruitment(DDTeamCollection* self) {
		state bool recruiting = false;
		state bool lastIsTss = false;
		TraceEvent("StorageServerRecruitment", self->distributorId)
		    .detail("State", "Idle")
		    .trackLatest(self->storageServerRecruitmentEventHolder->trackingKey);
		loop {
			if (!recruiting) {
				while (self->recruitingStream.get() == 0) {
					wait(self->recruitingStream.onChange());
				}
				TraceEvent("StorageServerRecruitment", self->distributorId)
				    .detail("State", "Recruiting")
				    .detail("IsTSS", self->isTssRecruiting ? "True" : "False")
				    .trackLatest(self->storageServerRecruitmentEventHolder->trackingKey);
				recruiting = true;
				lastIsTss = self->isTssRecruiting;
			} else {
				loop {
					choose {
						when(wait(self->recruitingStream.onChange())) {
							if (lastIsTss != self->isTssRecruiting) {
								TraceEvent("StorageServerRecruitment", self->distributorId)
								    .detail("State", "Recruiting")
								    .detail("IsTSS", self->isTssRecruiting ? "True" : "False")
								    .trackLatest(self->storageServerRecruitmentEventHolder->trackingKey);
								lastIsTss = self->isTssRecruiting;
							}
						}
						when(wait(self->recruitingStream.get() == 0
						              ? delay(SERVER_KNOBS->RECRUITMENT_IDLE_DELAY, TaskPriority::DataDistribution)
						              : Future<Void>(Never()))) {
							break;
						}
					}
				}
				TraceEvent("StorageServerRecruitment", self->distributorId)
				    .detail("State", "Idle")
				    .trackLatest(self->storageServerRecruitmentEventHolder->trackingKey);
				recruiting = false;
			}
		}
	}

	ACTOR static Future<Void> initializeStorage(DDTeamCollection* self,
	                                            RecruitStorageReply candidateWorker,
	                                            const DDEnabledState* ddEnabledState,
	                                            bool recruitTss,
	                                            Reference<TSSPairState> tssState) {
		// SOMEDAY: Cluster controller waits for availability, retry quickly if a server's Locality changes
		self->recruitingStream.set(self->recruitingStream.get() + 1);

		const NetworkAddress& netAddr = candidateWorker.worker.stableAddress();
		AddressExclusion workerAddr(netAddr.ip, netAddr.port);
		if (self->numExistingSSOnAddr(workerAddr) <= 2 &&
		    self->recruitingLocalities.find(candidateWorker.worker.stableAddress()) ==
		        self->recruitingLocalities.end()) {
			// Only allow at most 2 storage servers on an address, because
			// too many storage server on the same address (i.e., process) can cause OOM.
			// Ask the candidateWorker to initialize a SS only if the worker does not have a pending request
			state UID interfaceId = deterministicRandom()->randomUniqueID();

			UID clusterId = wait(self->getClusterId());

			state InitializeStorageRequest isr;
			isr.storeType = recruitTss ? self->configuration.testingStorageServerStoreType
			                           : self->configuration.storageServerStoreType;
			isr.seedTag = invalidTag;
			isr.reqId = deterministicRandom()->randomUniqueID();
			isr.interfaceId = interfaceId;
			isr.clusterId = clusterId;

			self->recruitingIds.insert(interfaceId);
			self->recruitingLocalities.insert(candidateWorker.worker.stableAddress());

			// if tss, wait for pair ss to finish and add its id to isr. If pair fails, don't recruit tss
			state bool doRecruit = true;
			if (recruitTss) {
				TraceEvent("TSS_Recruit", self->distributorId)
				    .detail("TSSID", interfaceId)
				    .detail("Stage", "TSSWaitingPair")
				    .detail("Addr", candidateWorker.worker.address())
				    .detail("Locality", candidateWorker.worker.locality.toString());

				Optional<std::pair<UID, Version>> ssPairInfoResult = wait(tssState->waitOnSS());
				if (ssPairInfoResult.present()) {
					isr.tssPairIDAndVersion = ssPairInfoResult.get();

					TraceEvent("TSS_Recruit", self->distributorId)
					    .detail("SSID", ssPairInfoResult.get().first)
					    .detail("TSSID", interfaceId)
					    .detail("Stage", "TSSWaitingPair")
					    .detail("Addr", candidateWorker.worker.address())
					    .detail("Version", ssPairInfoResult.get().second)
					    .detail("Locality", candidateWorker.worker.locality.toString());
				} else {
					doRecruit = false;

					TraceEvent(SevWarnAlways, "TSS_RecruitError", self->distributorId)
					    .detail("TSSID", interfaceId)
					    .detail("Reason", "SS recruitment failed for some reason")
					    .detail("Addr", candidateWorker.worker.address())
					    .detail("Locality", candidateWorker.worker.locality.toString());
				}
			}

			TraceEvent("DDRecruiting")
			    .detail("Primary", self->primary)
			    .detail("State", "Sending request to worker")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address())
			    .detail("TSS", recruitTss ? "true" : "false")
			    .detail("RecruitingStream", self->recruitingStream.get());

			Future<ErrorOr<InitializeStorageReply>> fRecruit =
			    doRecruit
			        ? candidateWorker.worker.storage.tryGetReply(isr, TaskPriority::DataDistribution)
			        : Future<ErrorOr<InitializeStorageReply>>(ErrorOr<InitializeStorageReply>(recruitment_failed()));

			state ErrorOr<InitializeStorageReply> newServer = wait(fRecruit);

			if (doRecruit && newServer.isError()) {
				TraceEvent(SevWarn, "DDRecruitmentError").error(newServer.getError());
				if (!newServer.isError(error_code_recruitment_failed) &&
				    !newServer.isError(error_code_request_maybe_delivered)) {
					tssState->markComplete();
					throw newServer.getError();
				}
				wait(delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskPriority::DataDistribution));
			}

			if (!recruitTss && newServer.present() &&
			    tssState->ssRecruitSuccess(std::pair(interfaceId, newServer.get().addedVersion))) {
				// SS has a tss pair. send it this id, but try to wait for add server until tss is recruited

				TraceEvent("TSS_Recruit", self->distributorId)
				    .detail("SSID", interfaceId)
				    .detail("Stage", "SSSignaling")
				    .detail("Addr", candidateWorker.worker.address())
				    .detail("Locality", candidateWorker.worker.locality.toString());

				// wait for timeout, but eventually move on if no TSS pair recruited
				Optional<bool> tssSuccessful =
				    wait(timeout(tssState->waitOnTSS(), SERVER_KNOBS->TSS_RECRUITMENT_TIMEOUT));

				if (tssSuccessful.present() && tssSuccessful.get()) {
					TraceEvent("TSS_Recruit", self->distributorId)
					    .detail("SSID", interfaceId)
					    .detail("Stage", "SSGotPair")
					    .detail("Addr", candidateWorker.worker.address())
					    .detail("Locality", candidateWorker.worker.locality.toString());
				} else {
					TraceEvent(SevWarn, "TSS_RecruitError", self->distributorId)
					    .detail("SSID", interfaceId)
					    .detail("Reason",
					            tssSuccessful.present() ? "TSS recruitment failed for some reason"
					                                    : "TSS recruitment timed out")
					    .detail("Addr", candidateWorker.worker.address())
					    .detail("Locality", candidateWorker.worker.locality.toString());
				}
			}

			self->recruitingIds.erase(interfaceId);
			self->recruitingLocalities.erase(candidateWorker.worker.stableAddress());

			TraceEvent("DDRecruiting")
			    .detail("Primary", self->primary)
			    .detail("State", "Finished request")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address())
			    .detail("RecruitingStream", self->recruitingStream.get());

			if (newServer.present()) {
				UID id = newServer.get().interf.id();
				if (!self->server_and_tss_info.count(id)) {
					if (!recruitTss || tssState->tssRecruitSuccess()) {
						self->addServer(newServer.get().interf,
						                candidateWorker.processClass,
						                self->serverTrackerErrorOut,
						                newServer.get().addedVersion,
						                ddEnabledState);
						self->waitUntilRecruited.set(false);
						// signal all done after adding tss to tracking info
						tssState->markComplete();
					}
				} else {
					TraceEvent(SevWarn, "DDRecruitmentError")
					    .detail("Reason", "Server ID already recruited")
					    .detail("ServerID", id);
				}
			}
		}

		// SS and/or TSS recruitment failed at this point, update tssState
		if (recruitTss && tssState->tssRecruitFailed()) {
			tssState->markComplete();
			TEST(true); // TSS recruitment failed for some reason
		}
		if (!recruitTss && tssState->ssRecruitFailed()) {
			TEST(true); // SS with pair TSS recruitment failed for some reason
		}

		self->recruitingStream.set(self->recruitingStream.get() - 1);
		self->restartRecruiting.trigger();

		return Void();
	}

	ACTOR static Future<Void> storageRecruiter(
	    DDTeamCollection* self,
	    Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
	    DDEnabledState const* ddEnabledState) {
		state Future<RecruitStorageReply> fCandidateWorker;
		state RecruitStorageRequest lastRequest;
		state bool hasHealthyTeam;
		state std::map<AddressExclusion, int> numSSPerAddr;

		// tss-specific recruitment state
		state int32_t targetTSSInDC = 0;
		state int32_t tssToRecruit = 0;
		state int inProgressTSSCount = 0;
		state PromiseStream<Future<Void>> addTSSInProgress;
		state Future<Void> inProgressTSS =
		    actorCollection(addTSSInProgress.getFuture(), &inProgressTSSCount, nullptr, nullptr, nullptr);
		state Reference<TSSPairState> tssState = makeReference<TSSPairState>();
		state Future<Void> checkTss = self->initialFailureReactionDelay;
		state bool pendingTSSCheck = false;

		TraceEvent(SevDebug, "TSS_RecruitUpdated", self->distributorId).detail("Count", tssToRecruit);

		loop {
			try {
				// Divide TSS evenly in each DC if there are multiple
				// TODO would it be better to put all of them in primary DC?
				targetTSSInDC = self->configuration.desiredTSSCount;
				if (self->configuration.usableRegions > 1) {
					targetTSSInDC /= self->configuration.usableRegions;
					if (self->primary) {
						// put extras in primary DC if it's uneven
						targetTSSInDC += (self->configuration.desiredTSSCount % self->configuration.usableRegions);
					}
				}
				int newTssToRecruit = targetTSSInDC - self->tss_info_by_pair.size() - inProgressTSSCount;
				// FIXME: Should log this if the recruit count stays the same but the other numbers update?
				if (newTssToRecruit != tssToRecruit) {
					TraceEvent("TSS_RecruitUpdated", self->distributorId)
					    .detail("Desired", targetTSSInDC)
					    .detail("Existing", self->tss_info_by_pair.size())
					    .detail("InProgress", inProgressTSSCount)
					    .detail("NotStarted", newTssToRecruit);
					tssToRecruit = newTssToRecruit;

					// if we need to get rid of some TSS processes, signal to either cancel recruitment or kill existing
					// TSS processes
					if (!pendingTSSCheck && (tssToRecruit < 0 || self->zeroHealthyTeams->get()) &&
					    (self->isTssRecruiting ||
					     (self->zeroHealthyTeams->get() && self->tss_info_by_pair.size() > 0))) {
						checkTss = self->initialFailureReactionDelay;
					}
				}
				numSSPerAddr.clear();
				hasHealthyTeam = (self->healthyTeamCount != 0);
				RecruitStorageRequest rsr;
				std::set<AddressExclusion> exclusions;
				for (auto s = self->server_and_tss_info.begin(); s != self->server_and_tss_info.end(); ++s) {
					auto serverStatus = self->server_status.get(s->second->lastKnownInterface.id());
					if (serverStatus.excludeOnRecruit()) {
						TraceEvent(SevDebug, "DDRecruitExcl1")
						    .detail("Primary", self->primary)
						    .detail("Excluding", s->second->lastKnownInterface.address());
						auto addr = s->second->lastKnownInterface.stableAddress();
						AddressExclusion addrExcl(addr.ip, addr.port);
						exclusions.insert(addrExcl);
						numSSPerAddr[addrExcl]++; // increase from 0
					}
				}
				for (auto addr : self->recruitingLocalities) {
					exclusions.insert(AddressExclusion(addr.ip, addr.port));
				}

				auto excl = self->excludedServers.getKeys();
				for (const auto& s : excl) {
					if (self->excludedServers.get(s) != DDTeamCollection::Status::NONE) {
						TraceEvent(SevDebug, "DDRecruitExcl2")
						    .detail("Primary", self->primary)
						    .detail("Excluding", s.toString());
						exclusions.insert(s);
					}
				}

				// Exclude workers that have invalid locality
				for (auto& addr : self->invalidLocalityAddr) {
					TraceEvent(SevDebug, "DDRecruitExclInvalidAddr").detail("Excluding", addr.toString());
					exclusions.insert(addr);
				}

				rsr.criticalRecruitment = !hasHealthyTeam;
				for (auto it : exclusions) {
					rsr.excludeAddresses.push_back(it);
				}

				rsr.includeDCs = self->includedDCs;

				TraceEvent(rsr.criticalRecruitment ? SevWarn : SevInfo, "DDRecruiting")
				    .detail("Primary", self->primary)
				    .detail("State", "Sending request to CC")
				    .detail("Exclusions", rsr.excludeAddresses.size())
				    .detail("Critical", rsr.criticalRecruitment)
				    .detail("IncludedDCsSize", rsr.includeDCs.size());

				if (rsr.criticalRecruitment) {
					TraceEvent(SevWarn, "DDRecruitingEmergency", self->distributorId).detail("Primary", self->primary);
				}

				if (!fCandidateWorker.isValid() || fCandidateWorker.isReady() ||
				    rsr.excludeAddresses != lastRequest.excludeAddresses ||
				    rsr.criticalRecruitment != lastRequest.criticalRecruitment) {
					lastRequest = rsr;
					fCandidateWorker =
					    brokenPromiseToNever(recruitStorage->get().getReply(rsr, TaskPriority::DataDistribution));
				}

				choose {
					when(RecruitStorageReply candidateWorker = wait(fCandidateWorker)) {
						AddressExclusion candidateSSAddr(candidateWorker.worker.stableAddress().ip,
						                                 candidateWorker.worker.stableAddress().port);
						int numExistingSS = numSSPerAddr[candidateSSAddr];
						if (numExistingSS >= 2) {
							TraceEvent(SevWarnAlways, "StorageRecruiterTooManySSOnSameAddr", self->distributorId)
							    .detail("Primary", self->primary)
							    .detail("Addr", candidateSSAddr.toString())
							    .detail("NumExistingSS", numExistingSS);
						}

						if (hasHealthyTeam && !tssState->active && tssToRecruit > 0) {
							TraceEvent("TSS_Recruit", self->distributorId)
							    .detail("Stage", "HoldTSS")
							    .detail("Addr", candidateSSAddr.toString())
							    .detail("Locality", candidateWorker.worker.locality.toString());

							TEST(true); // Starting TSS recruitment
							self->isTssRecruiting = true;
							tssState = makeReference<TSSPairState>(candidateWorker.worker.locality);

							addTSSInProgress.send(tssState->waitComplete());
							self->addActor.send(
							    initializeStorage(self, candidateWorker, ddEnabledState, true, tssState));
							checkTss = self->initialFailureReactionDelay;
						} else {
							if (tssState->active && tssState->inDataZone(candidateWorker.worker.locality)) {
								TEST(true); // TSS recruits pair in same dc/datahall
								self->isTssRecruiting = false;
								TraceEvent("TSS_Recruit", self->distributorId)
								    .detail("Stage", "PairSS")
								    .detail("Addr", candidateSSAddr.toString())
								    .detail("Locality", candidateWorker.worker.locality.toString());
								self->addActor.send(
								    initializeStorage(self, candidateWorker, ddEnabledState, false, tssState));
								// successfully started recruitment of pair, reset tss recruitment state
								tssState = makeReference<TSSPairState>();
							} else {
								TEST(tssState->active); // TSS recruitment skipped potential pair because it's in a
								                        // different dc/datahall
								self->addActor.send(initializeStorage(
								    self, candidateWorker, ddEnabledState, false, makeReference<TSSPairState>()));
							}
						}
					}
					when(wait(recruitStorage->onChange())) { fCandidateWorker = Future<RecruitStorageReply>(); }
					when(wait(self->zeroHealthyTeams->onChange())) {
						if (!pendingTSSCheck && self->zeroHealthyTeams->get() &&
						    (self->isTssRecruiting || self->tss_info_by_pair.size() > 0)) {
							checkTss = self->initialFailureReactionDelay;
						}
					}
					when(wait(checkTss)) {
						bool cancelTss = self->isTssRecruiting && (tssToRecruit < 0 || self->zeroHealthyTeams->get());
						// Can't kill more tss' than we have. Kill 1 if zero healthy teams, otherwise kill enough to get
						// back to the desired amount
						int tssToKill = std::min((int)self->tss_info_by_pair.size(),
						                         std::max(-tssToRecruit, self->zeroHealthyTeams->get() ? 1 : 0));
						if (cancelTss) {
							TEST(tssToRecruit < 0); // tss recruitment cancelled due to too many TSS
							TEST(self->zeroHealthyTeams->get()); // tss recruitment cancelled due zero healthy teams

							TraceEvent(SevWarn, "TSS_RecruitCancelled", self->distributorId)
							    .detail("Reason", tssToRecruit <= 0 ? "TooMany" : "ZeroHealthyTeams");
							tssState->cancel();
							tssState = makeReference<TSSPairState>();
							self->isTssRecruiting = false;

							pendingTSSCheck = true;
							checkTss = delay(SERVER_KNOBS->TSS_DD_CHECK_INTERVAL);
						} else if (tssToKill > 0) {
							auto itr = self->tss_info_by_pair.begin();
							for (int i = 0; i < tssToKill; i++, itr++) {
								UID tssId = itr->second->id;
								StorageServerInterface tssi = itr->second->lastKnownInterface;

								if (self->shouldHandleServer(tssi) && self->server_and_tss_info.count(tssId)) {
									Promise<Void> killPromise = itr->second->killTss;
									if (killPromise.canBeSet()) {
										TEST(tssToRecruit < 0); // Killing TSS due to too many TSS
										TEST(self->zeroHealthyTeams->get()); // Killing TSS due zero healthy teams
										TraceEvent(SevWarn, "TSS_DDKill", self->distributorId)
										    .detail("TSSID", tssId)
										    .detail("Reason",
										            self->zeroHealthyTeams->get() ? "ZeroHealthyTeams" : "TooMany");
										killPromise.send(Void());
									}
								}
							}
							// If we're killing a TSS because of zero healthy teams, wait a bit to give the replacing SS
							// a change to join teams and stuff before killing another TSS
							pendingTSSCheck = true;
							checkTss = delay(SERVER_KNOBS->TSS_DD_CHECK_INTERVAL);
						} else if (self->isTssRecruiting) {
							// check again later in case we need to cancel recruitment
							pendingTSSCheck = true;
							checkTss = delay(SERVER_KNOBS->TSS_DD_CHECK_INTERVAL);
							// FIXME: better way to do this than timer?
						} else {
							pendingTSSCheck = false;
							checkTss = Never();
						}
					}
					when(wait(self->restartRecruiting.onTrigger())) {}
				}
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskPriority::DataDistribution));
			} catch (Error& e) {
				if (e.code() != error_code_timed_out) {
					throw;
				}
				TEST(true); // Storage recruitment timed out
			}
		}
	}

	ACTOR static Future<Void> updateReplicasKey(DDTeamCollection* self, Optional<Key> dcId) {
		std::vector<Future<Void>> serverUpdates;

		for (auto& it : self->server_info) {
			serverUpdates.push_back(it.second->updated.getFuture());
		}

		wait(self->initialFailureReactionDelay && waitForAll(serverUpdates));
		wait(waitUntilHealthy(self));
		TraceEvent("DDUpdatingReplicas", self->distributorId)
		    .detail("Primary", self->primary)
		    .detail("DcId", dcId)
		    .detail("Replicas", self->configuration.storageTeamSize);
		state Transaction tr(self->cx);
		loop {
			try {
				Optional<Value> val = wait(tr.get(datacenterReplicasKeyFor(dcId)));
				state int oldReplicas = val.present() ? decodeDatacenterReplicasValue(val.get()) : 0;
				if (oldReplicas == self->configuration.storageTeamSize) {
					TraceEvent("DDUpdatedAlready", self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("DcId", dcId)
					    .detail("Replicas", self->configuration.storageTeamSize);
					return Void();
				}
				if (oldReplicas < self->configuration.storageTeamSize) {
					tr.set(rebootWhenDurableKey, StringRef());
				}
				tr.set(datacenterReplicasKeyFor(dcId), datacenterReplicasValue(self->configuration.storageTeamSize));
				wait(tr.commit());
				TraceEvent("DDUpdatedReplicas", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("DcId", dcId)
				    .detail("Replicas", self->configuration.storageTeamSize)
				    .detail("OldReplicas", oldReplicas);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> serverGetTeamRequests(DDTeamCollection* self, TeamCollectionInterface tci) {
		loop {
			GetTeamRequest req = waitNext(tci.getTeam.getFuture());
			self->addActor.send(self->getTeam(req));
		}
	}

	ACTOR static Future<Void> monitorHealthyTeams(DDTeamCollection* self) {
		TraceEvent("DDMonitorHealthyTeamsStart").detail("ZeroHealthyTeams", self->zeroHealthyTeams->get());
		loop choose {
			when(wait(self->zeroHealthyTeams->get()
			              ? delay(SERVER_KNOBS->DD_ZERO_HEALTHY_TEAM_DELAY, TaskPriority::DataDistribution)
			              : Never())) {
				self->doBuildTeams = true;
				wait(self->checkBuildTeams());
			}
			when(wait(self->zeroHealthyTeams->onChange())) {}
		}
	}

	ACTOR static Future<UID> getClusterId(DDTeamCollection* self) {
		state ReadYourWritesTransaction tr(self->cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> clusterId = wait(tr.get(clusterIdKey));
				ASSERT(clusterId.present());
				return BinaryReader::fromStringRef<UID>(clusterId.get(), Unversioned());
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> waitServerListChange(DDTeamCollection* self,
	                                               FutureStream<Void> serverRemoved,
	                                               const DDEnabledState* ddEnabledState) {
		state Future<Void> checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY, TaskPriority::DataDistributionLaunch);
		state Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> serverListAndProcessClasses =
		    Never();
		state bool isFetchingResults = false;
		state Transaction tr(self->cx);
		loop {
			try {
				choose {
					when(wait(checkSignal)) {
						checkSignal = Never();
						isFetchingResults = true;
						serverListAndProcessClasses = getServerListAndProcessClasses(&tr);
					}
					when(std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
					         wait(serverListAndProcessClasses)) {
						serverListAndProcessClasses = Never();
						isFetchingResults = false;

						for (int i = 0; i < results.size(); i++) {
							UID serverId = results[i].first.id();
							StorageServerInterface const& ssi = results[i].first;
							ProcessClass const& processClass = results[i].second;
							if (!self->shouldHandleServer(ssi)) {
								continue;
							} else if (self->server_and_tss_info.count(serverId)) {
								auto& serverInfo = self->server_and_tss_info[serverId];
								if (ssi.getValue.getEndpoint() !=
								        serverInfo->lastKnownInterface.getValue.getEndpoint() ||
								    processClass != serverInfo->lastKnownClass.classType()) {
									Promise<std::pair<StorageServerInterface, ProcessClass>> currentInterfaceChanged =
									    serverInfo->interfaceChanged;
									serverInfo->interfaceChanged =
									    Promise<std::pair<StorageServerInterface, ProcessClass>>();
									serverInfo->onInterfaceChanged =
									    Future<std::pair<StorageServerInterface, ProcessClass>>(
									        serverInfo->interfaceChanged.getFuture());
									currentInterfaceChanged.send(std::make_pair(ssi, processClass));
								}
							} else if (!self->recruitingIds.count(ssi.id())) {
								self->addServer(ssi,
								                processClass,
								                self->serverTrackerErrorOut,
								                tr.getReadVersion().get(),
								                ddEnabledState);
							}
						}

						tr = Transaction(self->cx);
						checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY, TaskPriority::DataDistributionLaunch);
					}
					when(waitNext(serverRemoved)) {
						if (isFetchingResults) {
							tr = Transaction(self->cx);
							serverListAndProcessClasses = getServerListAndProcessClasses(&tr);
						}
					}
				}
			} catch (Error& e) {
				wait(tr.onError(e));
				serverListAndProcessClasses = Never();
				isFetchingResults = false;
				checkSignal = Void();
			}
		}
	}
};

Reference<TCMachineTeamInfo> DDTeamCollection::findMachineTeam(
    std::vector<Standalone<StringRef>> const& machineIDs) const {
	if (machineIDs.empty()) {
		return Reference<TCMachineTeamInfo>();
	}

	Standalone<StringRef> machineID = machineIDs[0];
	for (auto& machineTeam : get(machine_info, machineID)->machineTeams) {
		if (machineTeam->machineIDs == machineIDs) {
			return machineTeam;
		}
	}

	return Reference<TCMachineTeamInfo>();
}

void DDTeamCollection::traceServerInfo() const {
	int i = 0;

	TraceEvent("ServerInfo", distributorId).detail("Size", server_info.size());
	for (auto& server : server_info) {
		TraceEvent("ServerInfo", distributorId)
		    .detail("ServerInfoIndex", i++)
		    .detail("ServerID", server.first.toString())
		    .detail("ServerTeamOwned", server.second->teams.size())
		    .detail("MachineID", server.second->machine->machineID.contents().toString())
		    .detail("StoreType", server.second->storeType.toString())
		    .detail("InDesiredDC", server.second->inDesiredDC);
	}
	for (auto& server : server_info) {
		const UID& uid = server.first;
		TraceEvent("ServerStatus", distributorId)
		    .detail("ServerID", uid)
		    .detail("Healthy", !server_status.get(uid).isUnhealthy())
		    .detail("MachineIsValid", get(server_info, uid)->machine.isValid())
		    .detail("MachineTeamSize",
		            get(server_info, uid)->machine.isValid() ? get(server_info, uid)->machine->machineTeams.size()
		                                                     : -1);
	}
}

bool DDTeamCollection::isMachineTeamHealthy(std::vector<Standalone<StringRef>> const& machineIDs) const {
	int healthyNum = 0;

	// A healthy machine team should have the desired number of machines
	if (machineIDs.size() != configuration.storageTeamSize)
		return false;

	for (auto& id : machineIDs) {
		auto& machine = get(machine_info, id);
		if (isMachineHealthy(machine)) {
			healthyNum++;
		}
	}
	return (healthyNum == machineIDs.size());
}

bool DDTeamCollection::teamContainsFailedServer(Reference<TCTeamInfo> team) {
	auto ssis = team->getLastKnownServerInterfaces();
	for (const auto& ssi : ssis) {
		AddressExclusion addr(ssi.address().ip, ssi.address().port);
		AddressExclusion ipaddr(ssi.address().ip);
		if (excludedServers.get(addr) == DDTeamCollection::Status::FAILED ||
		    excludedServers.get(ipaddr) == DDTeamCollection::Status::FAILED) {
			return true;
		}
		if (ssi.secondaryAddress().present()) {
			AddressExclusion saddr(ssi.secondaryAddress().get().ip, ssi.secondaryAddress().get().port);
			AddressExclusion sipaddr(ssi.secondaryAddress().get().ip);
			if (excludedServers.get(saddr) == DDTeamCollection::Status::FAILED ||
			    excludedServers.get(sipaddr) == DDTeamCollection::Status::FAILED) {
				return true;
			}
		}
	}
	return false;
}

Future<Void> DDTeamCollection::logOnCompletion(Future<Void> signal) {
	return DDTeamCollectionImpl::logOnCompletion(this, signal);
}

Future<Void> DDTeamCollection::interruptableBuildTeams() {
	return DDTeamCollectionImpl::interruptableBuildTeams(this);
}

Future<Void> DDTeamCollection::checkBuildTeams() {
	return DDTeamCollectionImpl::checkBuildTeams(this);
}

Future<Void> DDTeamCollection::getTeam(GetTeamRequest req) {
	return DDTeamCollectionImpl::getTeam(this, req);
}

Future<Void> DDTeamCollection::addSubsetOfEmergencyTeams() {
	return DDTeamCollectionImpl::addSubsetOfEmergencyTeams(this);
}

Future<Void> DDTeamCollection::init(Reference<InitialDataDistribution> initTeams,
                                    DDEnabledState const* ddEnabledState) {
	return DDTeamCollectionImpl::init(this, initTeams, ddEnabledState);
}

Future<Void> DDTeamCollection::buildTeams() {
	return DDTeamCollectionImpl::buildTeams(this);
}

Future<Void> DDTeamCollection::teamTracker(Reference<TCTeamInfo> team, bool badTeam, bool redundantTeam) {
	return DDTeamCollectionImpl::teamTracker(this, team, badTeam, redundantTeam);
}

Future<Void> DDTeamCollection::storageServerTracker(
    Database cx,
    TCServerInfo* server, // This actor is owned by this TCServerInfo, point to server_info[id]
    Promise<Void> errorOut,
    Version addedVersion,
    const DDEnabledState* ddEnabledState,
    bool isTss) {
	return DDTeamCollectionImpl::storageServerTracker(this, cx, server, errorOut, addedVersion, ddEnabledState, isTss);
}

Future<Void> DDTeamCollection::removeWrongStoreType() {
	return DDTeamCollectionImpl::removeWrongStoreType(this);
}

Future<Void> DDTeamCollection::waitUntilHealthy(double extraDelay, bool waitWiggle) {
	return DDTeamCollectionImpl::waitUntilHealthy(this, extraDelay, waitWiggle);
}

Future<Void> DDTeamCollection::removeBadTeams() {
	return DDTeamCollectionImpl::removeBadTeams(this);
}

Future<Void> DDTeamCollection::zeroServerLeftLoggerActor(Reference<TCTeamInfo> team) {
	return DDTeamCollectionImpl::zeroServerLeftLoggerActor(this, team);
}

Future<Void> DDTeamCollection::keyValueStoreTypeTracker(TCServerInfo* server) {
	return DDTeamCollectionImpl::keyValueStoreTypeTracker(this, server);
}

Future<Void> DDTeamCollection::storageServerFailureTracker(TCServerInfo* server,
                                                           Database cx,
                                                           ServerStatus* status,
                                                           Version addedVersion) {
	return DDTeamCollectionImpl::storageServerFailureTracker(this, server, cx, status, addedVersion);
}

Future<Void> DDTeamCollection::waitForAllDataRemoved(Database cx, UID serverID, Version addedVersion) {
	return DDTeamCollectionImpl::waitForAllDataRemoved(this, cx, serverID, addedVersion);
}

Future<Void> DDTeamCollection::machineTeamRemover() {
	return DDTeamCollectionImpl::machineTeamRemover(this);
}

Future<Void> DDTeamCollection::serverTeamRemover() {
	return DDTeamCollectionImpl::serverTeamRemover(this);
}

Future<Void> DDTeamCollection::trackExcludedServers() {
	return DDTeamCollectionImpl::trackExcludedServers(this);
}

Future<Void> DDTeamCollection::updateNextWigglingStoragePID() {
	return DDTeamCollectionImpl::updateNextWigglingStoragePID(this);
}

Future<Void> DDTeamCollection::perpetualStorageWiggleIterator(AsyncVar<bool>* stopSignal,
                                                              FutureStream<Void> finishStorageWiggleSignal) {
	return DDTeamCollectionImpl::perpetualStorageWiggleIterator(this, stopSignal, finishStorageWiggleSignal);
}

Future<std::pair<Future<Void>, Value>> DDTeamCollection::watchPerpetualStoragePIDChange() {
	return DDTeamCollectionImpl::watchPerpetualStoragePIDChange(this);
}

Future<Void> DDTeamCollection::clusterHealthCheckForPerpetualWiggle(int* extraTeamCount) {
	return DDTeamCollectionImpl::clusterHealthCheckForPerpetualWiggle(this, extraTeamCount);
}

Future<Void> DDTeamCollection::perpetualStorageWiggler(AsyncVar<bool>* stopSignal,
                                                       PromiseStream<Void> finishStorageWiggleSignal) {
	return DDTeamCollectionImpl::perpetualStorageWiggler(this, stopSignal, finishStorageWiggleSignal);
}

Future<Void> DDTeamCollection::monitorPerpetualStorageWiggle() {
	return DDTeamCollectionImpl::monitorPerpetualStorageWiggle(this);
}

Future<Void> DDTeamCollection::waitServerListChange(FutureStream<Void> serverRemoved,
                                                    DDEnabledState const* ddEnabledState) {
	return DDTeamCollectionImpl::waitServerListChange(this, serverRemoved, ddEnabledState);
}

Future<Void> DDTeamCollection::waitHealthyZoneChange() {
	return DDTeamCollectionImpl::waitHealthyZoneChange(this);
}

Future<Void> DDTeamCollection::monitorStorageServerRecruitment() {
	return DDTeamCollectionImpl::monitorStorageServerRecruitment(this);
}

Future<Void> DDTeamCollection::initializeStorage(RecruitStorageReply candidateWorker,
                                                 DDEnabledState const* ddEnabledState,
                                                 bool recruitTss,
                                                 Reference<TSSPairState> tssState) {
	return DDTeamCollectionImpl::initializeStorage(this, candidateWorker, ddEnabledState, recruitTss, tssState);
}

Future<Void> DDTeamCollection::storageRecruiter(
    Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
    DDEnabledState const* ddEnabledState) {
	return DDTeamCollectionImpl::storageRecruiter(this, recruitStorage, ddEnabledState);
}

Future<Void> DDTeamCollection::updateReplicasKey(Optional<Key> dcId) {
	return DDTeamCollectionImpl::updateReplicasKey(this, dcId);
}

Future<Void> DDTeamCollection::serverGetTeamRequests(TeamCollectionInterface tci) {
	return DDTeamCollectionImpl::serverGetTeamRequests(this, tci);
}

Future<Void> DDTeamCollection::monitorHealthyTeams() {
	return DDTeamCollectionImpl::monitorHealthyTeams(this);
}

Future<UID> DDTeamCollection::getClusterId() {
	return DDTeamCollectionImpl::getClusterId(this);
}
