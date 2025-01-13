/*
 * DDTeamCollection.actor.cpp
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

#include <climits>

#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/ExclusionTracker.actor.h"
#include "fdbserver/DataDistributionTeam.h"
#include "fdbserver/Knobs.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/network.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// Helper function for STL containers, with flow-friendly error handling
template <class MapContainer, class K>
auto get(MapContainer& m, K const& k) -> decltype(m.at(k)) {
	auto it = m.find(k);
	ASSERT(it != m.end());
	return it->second;
}

} // namespace

namespace data_distribution {
int EligibilityCounter::fromGetTeamRequest(GetTeamRequest const& req) {
	// equivalent to bit set operation
	return req.preferLowerDiskUtil * EligibilityCounter::LOW_DISK_UTIL +
	       // When preferLowerReadUtil, CPU stat is for admittance to eligible pool, and ReadLoad is for sorting inside
	       // the pool.
	       req.preferLowerReadUtil * EligibilityCounter::LOW_CPU;
}

void EligibilityCounter::increase(Type type) {
	type_count[type]++;
}

void EligibilityCounter::reset(Type type) {
	type_count[type] = 0;
}

int EligibilityCounter::getCount(int combinedType) const {
	unsigned minCount = std::numeric_limits<unsigned>::max();
	for (auto& [t, c] : type_count) {
		if ((combinedType & t) > 0 && minCount > c) {
			minCount = c;
		}
	}
	return minCount;
}

} // namespace data_distribution

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
				state std::vector<ProcessData> workers = wait(self->db->getWorkers());
				state std::set<AddressExclusion> existingAddrs;
				for (int i = 0; i < workers.size(); i++) {
					const ProcessData& workerData = workers[i];
					AddressExclusion addr(workerData.address.ip, workerData.address.port);
					existingAddrs.insert(addr);
					if (self->invalidLocalityAddr.contains(addr) &&
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
					if (!existingAddrs.contains(*addr)) {
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
				when(wait(self->buildTeams())) {
					return Void();
				}
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

	// Find the team with the exact storage servers as req.src.
	static void getTeamByServers(DDTeamCollection* self, GetTeamRequest req) {
		const std::string servers = TCTeamInfo::serversToString(req.src);
		Optional<Reference<IDataDistributionTeam>> res;
		for (const auto& team : self->teams) {
			if (team->getServerIDsStr() == servers) {
				res = team;
				break;
			}
		}
		req.reply.send(std::make_pair(res, false));
	}

	// Random selection for load balance
	ACTOR static Future<Void> getTeamForBulkLoad(DDTeamCollection* self, GetTeamRequest req) {
		try {
			TraceEvent(SevInfo, "DDBulkLoadEngineTaskGetTeamReqReceived", self->distributorId)
			    .detail("TCReady", self->readyToStart.isReady())
			    .detail("TeamBuilderValid", self->teamBuilder.isValid())
			    .detail("TeamBuilderReady", self->teamBuilder.isValid() ? self->teamBuilder.isReady() : false)
			    .detail("SrcIds", describe(req.src))
			    .detail("Primary", self->isPrimary())
			    .detail("TeamSize", self->teams.size());
			wait(self->checkBuildTeams());

			TraceEvent(SevInfo, "DDBulkLoadEngineTaskGetTeamCheckBuildTeamDone", self->distributorId)
			    .detail("TCReady", self->readyToStart.isReady())
			    .detail("TeamBuilderValid", self->teamBuilder.isValid())
			    .detail("TeamBuilderReady", self->teamBuilder.isValid() ? self->teamBuilder.isReady() : false)
			    .detail("SrcIds", describe(req.src))
			    .detail("Primary", self->isPrimary())
			    .detail("TeamSize", self->teams.size());

			if (!self->primary && !self->readyToStart.isReady()) {
				// When remote DC is not ready, DD shouldn't reply with a new team because
				// a data movement to that team can't be completed and such a move
				// may block the primary DC from reaching "storage_recovered".
				auto team = self->findTeamFromServers(req.completeSources, /*wantHealthy=*/false);
				TraceEvent(SevWarn, "DDBulkLoadEngineTaskGetTeamRemoteDCNotReady", self->distributorId)
				    .suppressFor(1.0)
				    .detail("Primary", self->primary)
				    .detail("Team", team.present() ? describe(team.get()->getServerIDs()) : "");
				req.reply.send(std::make_pair(team, true));
				return Void();
			}

			self->updateTeamPivotValues();

			std::vector<Reference<TCTeamInfo>> candidateTeams;
			int unhealthyTeamCount = 0;
			int notEligibileTeamCount = 0;
			int duplicatedCount = 0;
			for (const auto& dest : self->teams) {
				if (!dest->isHealthy()) {
					unhealthyTeamCount++;
					continue;
				}
				bool allDestServerHaveLowDiskUtil =
				    dest->getEligibilityCount(data_distribution::EligibilityCounter::LOW_DISK_UTIL) > 0;
				if (!allDestServerHaveLowDiskUtil) {
					notEligibileTeamCount++;
					continue;
				}
				bool ok = true;
				for (const auto& srcId : req.src) {
					std::vector<UID> serverIds = dest->getServerIDs();
					for (const auto& serverId : serverIds) {
						if (serverId == srcId) {
							ok = false; // Do not select a team that has a server owning the bulk loading range.
							// TODO(BulkLoad): remove this later. Require the support in SS.
							break;
						}
					}
					if (!ok) {
						break;
					}
				}
				if (!ok) {
					duplicatedCount++;
					continue;
				}
				candidateTeams.push_back(dest);
			}
			Optional<Reference<IDataDistributionTeam>> res;
			if (candidateTeams.size() >= 1) {
				res = deterministicRandom()->randomChoice(candidateTeams);
				TraceEvent(SevInfo, "DDBulkLoadEngineTaskGetTeamReply", self->distributorId)
				    .detail("TCReady", self->readyToStart.isReady())
				    .detail("SrcIds", describe(req.src))
				    .detail("Primary", self->isPrimary())
				    .detail("TeamSize", self->teams.size())
				    .detail("CandidateSize", candidateTeams.size())
				    .detail("UnhealthyTeamCount", unhealthyTeamCount)
				    .detail("DuplicatedCount", duplicatedCount)
				    .detail("NotEligibileTeamCount", notEligibileTeamCount)
				    .detail("DestIds", describe(res.get()->getServerIDs()))
				    .detail("DestTeam", res.get()->getTeamID());
			} else {
				TraceEvent(SevWarnAlways, "DDBulkLoadEngineTaskGetTeamFailedToFindValidTeam", self->distributorId)
				    .detail("TCReady", self->readyToStart.isReady())
				    .detail("SrcIds", describe(req.src))
				    .detail("Primary", self->isPrimary())
				    .detail("TeamSize", self->teams.size())
				    .detail("CandidateSize", candidateTeams.size())
				    .detail("UnhealthyTeamCount", unhealthyTeamCount)
				    .detail("DuplicatedCount", duplicatedCount)
				    .detail("NotEligibileTeamCount", notEligibileTeamCount);
			}
			req.reply.send(std::make_pair(res, false));
			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled && req.reply.canBeSet())
				req.reply.sendError(e);
			throw;
		}
	}

	// Return a threshold of team queue size which guarantees at least DD_LONG_STORAGE_QUEUE_TEAM_MAJORITY_PERCENTILE
	// portion of teams that have longer storage queues
	// A team storage queue size is defined as the longest storage queue size among all SSes of the team
	static int64_t calculateTeamStorageQueueThreshold(const std::vector<Reference<TCTeamInfo>>& teams) {
		if (teams.size() == 0) {
			return std::numeric_limits<int64_t>::max(); // disable this funcationality
		}
		std::vector<int64_t> queueLengthList;
		for (const auto& team : teams) {
			Optional<int64_t> storageQueueSize = team->getLongestStorageQueueSize();
			if (!storageQueueSize.present()) {
				// This team may have an unhealthy SS, so avoid selecting it
				queueLengthList.push_back(std::numeric_limits<int64_t>::max());
			} else {
				queueLengthList.push_back(storageQueueSize.get());
			}
		}
		double percentile = std::max(0.0, std::min(SERVER_KNOBS->DD_LONG_STORAGE_QUEUE_TEAM_MAJORITY_PERCENTILE, 1.0));
		int position = (queueLengthList.size() - 1) * (1 - percentile);
		std::nth_element(queueLengthList.begin(), queueLengthList.begin() + position, queueLengthList.end());
		int64_t threshold = queueLengthList[position];
		TraceEvent(SevInfo, "StorageQueueAwareGotThreshold").suppressFor(5.0).detail("Threshold", threshold);
		return threshold;
	}

	// Returns the overall best team that matches the requirement from `req`. When preferWithinShardLimit is true, it
	// also tries to select a team whose existing shard is less than SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM.
	static Optional<Reference<IDataDistributionTeam>> getBestTeam(DDTeamCollection* self,
	                                                              const GetTeamRequest& req,
	                                                              bool preferWithinShardLimit,
	                                                              int& numSkippedSSFailedGetQueueLength,
	                                                              int& numSkippedSSQueueTooLong,
	                                                              Optional<int64_t> storageQueueThreshold) {
		ASSERT(!req.storageQueueAware || storageQueueThreshold.present());
		auto& startIndex = req.preferLowerDiskUtil ? self->lowestUtilizationTeam : self->highestUtilizationTeam;
		if (startIndex >= self->teams.size()) {
			startIndex = 0;
		}
		Optional<Reference<IDataDistributionTeam>> bestOption;
		int64_t bestLoadBytes = 0;
		bool wigglingBestOption = false; // best option contains server in paused wiggle state
		int bestIndex = startIndex;
		for (int i = 0; i < self->teams.size(); i++) {
			int currentIndex = (startIndex + i) % self->teams.size();
			if (self->teams[currentIndex]->isHealthy()) {
				int eligibilityType = data_distribution::EligibilityCounter::fromGetTeamRequest(req);
				if (eligibilityType != data_distribution::EligibilityCounter::NONE &&
				    self->teams[currentIndex]->getEligibilityCount(eligibilityType) <= 0) {
					continue;
				}

				int64_t loadBytes = self->teams[currentIndex]->getLoadBytes(true, req.inflightPenalty);
				if (req.storageQueueAware) {
					Optional<int64_t> storageQueueSize = self->teams[currentIndex]->getLongestStorageQueueSize();
					if (!storageQueueSize.present()) {
						numSkippedSSFailedGetQueueLength++;
						continue; // this team may have an unhealthy SS, skip
					} else if (storageQueueSize.get() > storageQueueThreshold.get()) {
						numSkippedSSQueueTooLong++;
						continue; // this team has a SS with a too long storage queue, skip
					}
				}

				auto team = ShardsAffectedByTeamFailure::Team(self->teams[currentIndex]->getServerIDs(), self->primary);
				if ((!req.teamMustHaveShards || self->shardsAffectedByTeamFailure->hasShards(team)) &&
				    // sort conditions
				    (!bestOption.present() ||
				     req.lessCompare(bestOption.get(), self->teams[currentIndex], bestLoadBytes, loadBytes))) {

					// bestOption doesn't contain wiggling SS while current team does. Don't replace bestOption
					// in this case
					if (bestOption.present() && !wigglingBestOption &&
					    self->teams[currentIndex]->hasWigglePausedServer()) {
						continue;
					}

					if (SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM && preferWithinShardLimit &&
					    self->shardsAffectedByTeamFailure->getNumberOfShards(team) >
					        SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM) {
						continue;
					}

					bestLoadBytes = loadBytes;
					bestOption = self->teams[currentIndex];
					bestIndex = currentIndex;
					wigglingBestOption = self->teams[bestIndex]->hasWigglePausedServer();
				}
			}
		}
		startIndex = bestIndex;
		return bestOption;
	}

	// Returns the best team from `candidates` that matches the requirement from `req`. When preferWithinShardLimit is
	// true, it also tries to select a team whose existing team is less than SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM.
	// Do not check storage queue size since getTeam has checked the size when selecting the input candidates
	static Optional<Reference<IDataDistributionTeam>> getBestTeamFromCandidates(
	    DDTeamCollection* self,
	    const GetTeamRequest& req,
	    const std::vector<Reference<TCTeamInfo>>& candidates,
	    bool preferWithinShardLimit,
	    int& numSkippedSSFailedGetQueueLength,
	    int& numSkippedSSQueueTooLong) {
		Optional<Reference<IDataDistributionTeam>> bestOption;
		int64_t bestLoadBytes = 0;
		bool wigglingBestOption = false; // best option contains server in paused wiggle state
		for (int i = 0; i < candidates.size(); i++) {
			int64_t loadBytes = candidates[i]->getLoadBytes(true, req.inflightPenalty);
			if (!bestOption.present() || req.lessCompare(bestOption.get(), candidates[i], bestLoadBytes, loadBytes)) {

				// bestOption doesn't contain wiggling SS while current team does. Don't replace bestOption
				// in this case
				if (bestOption.present() && !wigglingBestOption && candidates[i]->hasWigglePausedServer()) {
					continue;
				}

				if (SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM && preferWithinShardLimit &&
				    self->shardsAffectedByTeamFailure->getNumberOfShards(ShardsAffectedByTeamFailure::Team(
				        candidates[i]->getServerIDs(), self->primary)) > SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM) {
					continue;
				}

				bestLoadBytes = loadBytes;
				bestOption = candidates[i];
				wigglingBestOption = candidates[i]->hasWigglePausedServer();
			}
		}
		return bestOption;
	}

	// SOMEDAY: Make bestTeam better about deciding to leave a shard where it is (e.g. in PRIORITY_TEAM_HEALTHY case)
	//		    use keys, src, dest, metrics, priority, system load, etc.. to decide...
	ACTOR static Future<Void> getTeam(DDTeamCollection* self, GetTeamRequest req) {
		try {
			wait(self->checkBuildTeams());

			if (!self->primary && !self->readyToStart.isReady()) {
				// When remote DC is not ready, DD shouldn't reply with a new team because
				// a data movement to that team can't be completed and such a move
				// may block the primary DC from reaching "storage_recovered".
				auto team = self->findTeamFromServers(req.completeSources, /*wantHealthy=*/false);
				TraceEvent("GetTeamNotReady", self->distributorId)
				    .suppressFor(1.0)
				    .detail("Primary", self->primary)
				    .detail("Team", team.present() ? describe(team.get()->getServerIDs()) : "");
				req.reply.send(std::make_pair(team, true));
				return Void();
			}

			// report the pivot values
			self->updateTeamPivotValues();

			bool foundSrc = false;
			for (const auto& id : req.src) {
				if (self->server_info.contains(id)) {
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

			Optional<Reference<IDataDistributionTeam>> bestOption;
			state int numSkippedSSFailedGetQueueLength = 0;
			state int numSkippedSSQueueTooLong = 0;

			if (ddLargeTeamEnabled() && req.keys.present()) {
				int customReplicas = self->configuration.storageTeamSize;
				for (auto it : self->userRangeConfig->intersectingRanges(req.keys->begin, req.keys->end)) {
					customReplicas = std::max(customReplicas, it->value().replicationFactor.orDefault(0));
				}
				if (customReplicas > self->configuration.storageTeamSize) {
					auto newTeam = self->buildLargeTeam(customReplicas);
					auto& firstFailureTime = self->firstLargeTeamFailure[customReplicas];
					if (newTeam) {
						if (newTeam->size() < customReplicas) {
							if (!firstFailureTime.present()) {
								firstFailureTime = now();
							}
							if (now() - firstFailureTime.get() < SERVER_KNOBS->DD_LARGE_TEAM_DELAY) {
								req.reply.send(std::make_pair(Optional<Reference<IDataDistributionTeam>>(), foundSrc));
								return Void();
							}
							self->underReplication.insert(req.keys.get(), true);
						} else {
							firstFailureTime = Optional<double>();
						}
						TraceEvent("ReplicatingToLargeTeam", self->distributorId)
						    .detail("Team", newTeam->getDesc())
						    .detail("Healthy", newTeam->isHealthy())
						    .detail("DesiredReplicas", customReplicas)
						    .detail("UnderReplicated", newTeam->size() < customReplicas);
						req.reply.send(std::make_pair(newTeam, foundSrc));
						return Void();
					} else {
						if (!firstFailureTime.present()) {
							firstFailureTime = now();
						}
						if (now() - firstFailureTime.get() < SERVER_KNOBS->DD_LARGE_TEAM_DELAY) {
							req.reply.send(std::make_pair(Optional<Reference<IDataDistributionTeam>>(), foundSrc));
							return Void();
						}
						TraceEvent(SevWarnAlways, "LargeTeamNotFound", self->distributorId)
						    .suppressFor(1.0)
						    .detail("Replicas", customReplicas)
						    .detail("StorageTeamSize", self->configuration.storageTeamSize)
						    .detail("LargeTeamDiff", now() - firstFailureTime.get());
						self->underReplication.insert(req.keys.get(), true);
					}
				}
			}

			// Note: this block does not apply any filters from the request
			if (req.teamSelect == TeamSelect::WANT_COMPLETE_SRCS) {
				auto healthyTeam = self->findTeamFromServers(req.completeSources, /* wantHealthy=*/true);
				if (healthyTeam.present()) {
					req.reply.send(std::make_pair(healthyTeam, foundSrc));
					return Void();
				}
			}

			Optional<int64_t> storageQueueThreshold;
			if (req.storageQueueAware) {
				storageQueueThreshold = calculateTeamStorageQueueThreshold(self->teams);
			}
			if (req.teamSelect == TeamSelect::WANT_TRUE_BEST || req.wantTrueBestIfMoveout) {
				ASSERT(!bestOption.present());
				if (SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM && req.preferWithinShardLimit) {
					bestOption = getBestTeam(self,
					                         req,
					                         /*preferWithinShardLimit=*/true,
					                         numSkippedSSFailedGetQueueLength,
					                         numSkippedSSQueueTooLong,
					                         storageQueueThreshold);
					if (!bestOption.present()) {
						// In case, we may return a team whose shard count is more than DESIRED_MAX_SHARDS_PER_TEAM.
						TraceEvent("GetBestTeamPreferWithinShardLimitFailed").log();
					}
				}
				if (!bestOption.present()) {
					bestOption = getBestTeam(self,
					                         req,
					                         /*preferWithinShardLimit=*/false,
					                         numSkippedSSFailedGetQueueLength,
					                         numSkippedSSQueueTooLong,
					                         storageQueueThreshold);
				}
			} else {
				ASSERT(!bestOption.present());
				std::vector<Reference<TCTeamInfo>> randomTeams;
				int nTries = 0;
				while (randomTeams.size() < SERVER_KNOBS->BEST_TEAM_OPTION_COUNT &&
				       nTries < SERVER_KNOBS->BEST_TEAM_MAX_TEAM_TRIES) {
					// If unhealthy team is majority, we may not find an ok dest in this while loop
					Reference<TCTeamInfo> dest = deterministicRandom()->randomChoice(self->teams);

					bool ok = dest->isHealthy();
					if (ok) {
						int eligibilityType = data_distribution::EligibilityCounter::fromGetTeamRequest(req);
						ok = eligibilityType == data_distribution::EligibilityCounter::NONE ||
						     dest->getEligibilityCount(eligibilityType) > 0;
					}

					for (int i = 0; ok && i < randomTeams.size(); i++) {
						if (randomTeams[i]->getServerIDs() == dest->getServerIDs()) {
							// Found a duplicate team. Skip `dest`.
							ok = false;
							break;
						}
					}

					ok = ok && (!req.teamMustHaveShards ||
					            self->shardsAffectedByTeamFailure->hasShards(
					                ShardsAffectedByTeamFailure::Team(dest->getServerIDs(), self->primary)));

					if (req.storageQueueAware) {
						Optional<int64_t> storageQueueSize = dest->getLongestStorageQueueSize();
						if (!storageQueueSize.present()) {
							numSkippedSSFailedGetQueueLength++;
							ok = false; // this team may have an unhealthy SS, skip
						} else if (storageQueueSize.get() > storageQueueThreshold.get()) {
							numSkippedSSQueueTooLong++;
							ok = false; // this team has a SS with a too long storage queue, skip
						}
					}

					if (ok)
						randomTeams.push_back(dest);
					else
						nTries++;
				}

				// Log BestTeamStuck reason when we have healthy teams but they do not have healthy free space
				if (randomTeams.empty() && !self->zeroHealthyTeams->get()) {
					self->bestTeamKeepStuckCount++;
				} else {
					self->bestTeamKeepStuckCount = 0;
				}

				if (!randomTeams.empty()) {
					if (SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM && req.preferWithinShardLimit) {
						bestOption = getBestTeamFromCandidates(self,
						                                       req,
						                                       randomTeams,
						                                       /*preferWithinShardLimit=*/true,
						                                       numSkippedSSFailedGetQueueLength,
						                                       numSkippedSSQueueTooLong);
						if (!bestOption.present()) {
							// In case, we may return a team whose shard count is more than DESIRED_MAX_SHARDS_PER_TEAM.
							TraceEvent("GetBestTeamFromCandidatesPreferWithinShardLimitFailed").log();
						}
					}

					if (!bestOption.present()) {
						bestOption = getBestTeamFromCandidates(self,
						                                       req,
						                                       randomTeams,
						                                       /*preferWithinShardLimit=*/false,
						                                       numSkippedSSFailedGetQueueLength,
						                                       numSkippedSSQueueTooLong);
					}
				}
			}

			// Note: req.completeSources can be empty and all servers (and server teams) can be unhealthy.
			// We will get stuck at this! This only happens when a DC fails. No need to consider it right now.
			// Note: this block does not apply any filters from the request
			if (!bestOption.present() && self->zeroHealthyTeams->get()) {
				// Attempt to find the unhealthy source server team and return it
				auto healthyTeam = self->findTeamFromServers(req.completeSources, /* wantHealthy=*/false);
				if (healthyTeam.present()) {
					req.reply.send(std::make_pair(healthyTeam, foundSrc));
					return Void();
				}
			}
			if (g_network->isSimulated() && !bestOption.present()) {
				TraceEvent(SevDebug, "GetTeamReturnEmpty")
				    .detail("Request", req.getDesc())
				    .detail("HealthyTeams", self->healthyTeamCount)
				    .detail("PivotCPU", self->teamPivots.pivotCPU)
				    .detail("PivotDiskSpace", self->teamPivots.pivotAvailableSpaceRatio)
				    .detail("StorageQueueAware", req.storageQueueAware)
				    .detail("NumSkippedSSFailedGetQueueLength", numSkippedSSFailedGetQueueLength)
				    .detail("NumSkippedSSQueueTooLong", numSkippedSSQueueTooLong);
				// self->traceAllInfo(true);
			}

			if (!bestOption.present() && (req.storageQueueAware || req.wantTrueBestIfMoveout)) {
				// re-run getTeam without storageQueueAware and wantTrueBestIfMoveout
				req.storageQueueAware = false;
				req.wantTrueBestIfMoveout = false;
				TraceEvent(SevWarn, "GetTeamRetry", self->distributorId)
				    .detail("OldStorageQueueAware", req.storageQueueAware)
				    .detail("OldWantTrueBestIfMoveout", req.wantTrueBestIfMoveout);
				wait(getTeam(self, req));
			} else {
				req.reply.send(std::make_pair(bestOption, foundSrc));
			}

			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled && req.reply.canBeSet())
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
		state std::vector<Reference<TCTeamInfo>> largeOrBadTeams = self->badTeams;
		largeOrBadTeams.insert(largeOrBadTeams.end(), self->largeTeams.begin(), self->largeTeams.end());

		for (; idx < largeOrBadTeams.size(); idx++) {
			servers.clear();
			for (const auto& server : largeOrBadTeams[idx]->getServers()) {
				if (server->isInDesiredDC() && !self->server_status.get(server->getId()).isUnhealthy()) {
					servers.push_back(server);
				}
			}

			// For the bad team that is too big (too many servers), we will try to find a subset of servers in the
			// team to construct a new healthy team, so that moving data to the new healthy team will not cause too
			// much data movement overhead
			// FIXME: This code logic can be simplified.
			if (servers.size() >= self->configuration.storageTeamSize) {
				bool foundTeam = false;
				for (int j = 0; j < servers.size() - self->configuration.storageTeamSize + 1 && !foundTeam; j++) {
					auto const& serverTeams = servers[j]->getTeams();
					for (int k = 0; k < serverTeams.size(); k++) {
						auto& testTeam = serverTeams[k]->getServerIDs();
						bool allInTeam = true; // All servers in testTeam belong to the healthy servers
						for (int l = 0; l < testTeam.size(); l++) {
							bool foundServer = false;
							for (auto it : servers) {
								if (it->getId() == testTeam[l]) {
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
							self->addTeam(servers, IsInitialTeam::True);
							// self->traceTeamCollectionInfo(); // Trace at the end of the function
						} else {
							tempSet->clear();
							for (auto it : servers) {
								tempMap->add(it->getLastKnownInterface().locality, &it->getId());
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
							self->addTeam(serverIds.begin(), serverIds.end(), IsInitialTeam::True);
						}
					} else {
						serverIds.clear();
						for (auto it : servers) {
							serverIds.push_back(it->getId());
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

		self->userRangeConfig = initTeams->userRangeConfig;
		self->healthyZone.set(initTeams->initHealthyZoneValue);
		// SOMEDAY: If some servers have teams and not others (or some servers have more data than others) and there is
		// an address/locality collision, should we preferentially mark the least used server as undesirable?
		for (auto& [server, procClass] : initTeams->allServers) {
			if (self->shouldHandleServer(server)) {
				if (!self->isValidLocality(self->configuration.storagePolicy, server.locality)) {
					TraceEvent(SevWarnAlways, "MissingLocality")
					    .detail("Server", server.uniqueID)
					    .detail("Locality", server.locality.toString());
					auto addr = server.stableAddress();
					self->invalidLocalityAddr.insert(AddressExclusion(addr.ip, addr.port));
					if (self->checkInvalidLocalities.isReady()) {
						self->checkInvalidLocalities = checkAndRemoveInvalidLocalityAddr(self);
						self->addActor.send(self->checkInvalidLocalities);
					}
				}
				self->addServer(server, procClass, self->serverTrackerErrorOut, 0, *ddEnabledState);
			}
		}

		state std::set<std::vector<UID>>::iterator teamIter =
		    self->primary ? initTeams->primaryTeams.begin() : initTeams->remoteTeams.begin();
		state std::set<std::vector<UID>>::iterator teamIterEnd =
		    self->primary ? initTeams->primaryTeams.end() : initTeams->remoteTeams.end();
		for (; teamIter != teamIterEnd; ++teamIter) {
			self->addTeam(teamIter->begin(), teamIter->end(), IsInitialTeam::True);
			wait(yield());
		}

		return Void();
	}

	ACTOR static Future<Void> buildTeams(DDTeamCollection* self) {
		state int desiredTeams;
		state int serverCount = 0;
		state std::set<Optional<Standalone<StringRef>>> machines;

		// wait to see whether restartTeamBuilder is triggered
		wait(delay(0, g_network->getCurrentTask()));
		// make team builder don't build team during the interval between excluding the wiggled process and recruited a
		// new SS to avoid redundant teams
		while (self->pauseWiggle && !self->pauseWiggle->get() && self->waitUntilRecruited.get()) {
			choose {
				when(wait(self->waitUntilRecruited.onChange() || self->pauseWiggle->onChange())) {}
				when(wait(delay(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY, g_network->getCurrentTask()))) {
					break;
				}
			}
		}

		for (const auto& [serverID, server] : self->server_info) {
			if (!self->server_status.get(serverID).isUnhealthy()) {
				++serverCount;
				LocalityData const& serverLocation = server->getLastKnownInterface().locality;
				machines.insert(serverLocation.zoneId());
			}
		}

		int uniqueMachines = machines.size();
		TraceEvent("BuildTeams", self->distributorId)
		    .detail("ServerCount", self->server_info.size())
		    .detail("UniqueMachines", uniqueMachines)
		    .detail("Primary", self->primary)
		    .detail("StorageTeamSize", self->configuration.storageTeamSize);

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

			if (teamCount == 0 && teamsToBuild == 0 && SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE > 0) {
				// Use DD_BUILD_EXTRA_TEAMS_OVERRIDE > 0 as the feature flag: Set to 0 to disable it
				TraceEvent(SevWarnAlways, "BuildServerTeamsHaveTooManyUnhealthyTeams")
				    .detail("Hint", "Build teams may stuck and prevent DD from relocating data")
				    .detail("BuildExtraServerTeamsOverride", SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE);
				teamsToBuild = SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE;
			}

			TraceEvent("BuildTeamsBegin", self->distributorId)
			    .detail("Primary", self->isPrimary())
			    .detail("TeamsToBuild", teamsToBuild)
			    .detail("DesiredTeams", desiredTeams)
			    .detail("MaxTeams", maxTeams)
			    .detail("BadServerTeams", self->badTeams.size())
			    .detail("PerpetualWigglingTeams", wigglingTeams)
			    .detail("UniqueMachines", uniqueMachines)
			    .detail("TeamSize", self->configuration.storageTeamSize)
			    .detail("Servers", self->server_info.size())
			    .detail("HealthyServers", serverCount)
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

				auto [minTeamsOnServer, maxTeamsOnServer] = self->calculateMinMaxServerTeamsOnServer();
				auto [minMachineTeamsOnMachine, maxMachineTeamsOnMachine] =
				    self->calculateMinMaxMachineTeamsOnMachine();

				TraceEvent("TeamCollectionInfo", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("AddedTeams", 0)
				    .detail("TeamsToBuild", teamsToBuild)
				    .detail("CurrentServerTeams", self->teams.size())
				    .detail("Servers", self->server_info.size())
				    .detail("HealthyServers", serverCount)
				    .detail("DesiredTeams", desiredTeams)
				    .detail("MaxTeams", maxTeams)
				    .detail("StorageTeamSize", self->configuration.storageTeamSize)
				    .detail("CurrentMachineTeams", self->machineTeams.size())
				    .detail("CurrentHealthyMachineTeams", healthyMachineTeamCount)
				    .detail("DesiredMachineTeams", desiredMachineTeams)
				    .detail("MaxMachineTeams", maxMachineTeams)
				    .detail("TotalHealthyMachines", totalHealthyMachineCount)
				    .detail("MinTeamsOnServer", minTeamsOnServer)
				    .detail("MaxTeamsOnServer", maxTeamsOnServer)
				    .detail("MinMachineTeamsOnMachine", maxMachineTeamsOnMachine)
				    .detail("MaxMachineTeamsOnMachine", minMachineTeamsOnMachine)
				    .detail("DoBuildTeams", self->doBuildTeams)
				    .trackLatest(self->teamCollectionInfoEventHolder->trackingKey);
			}
		} else {
			// If there are too few machines to even build teams or there are too few represented datacenters, can't
			// build any team.
			self->lastBuildTeamsFailed = true;
			TraceEvent(SevWarnAlways, "BuildTeamsLastBuildTeamsFailed", self->distributorId)
			    .detail("Reason", "Do not have enough unique machines")
			    .detail("Primary", self->primary)
			    .detail("UniqueMachines", uniqueMachines)
			    .detail("Replication", self->configuration.storageTeamSize);
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
	                                      IsBadTeam badTeam,
	                                      IsRedundantTeam redundantTeam) {
		state int lastServersLeft = team->size();
		state bool lastAnyUndesired = false;
		state bool lastAnyWigglingServer = false;
		state bool logTeamEvents =
		    g_network->isSimulated() || !badTeam || team->size() <= self->configuration.storageTeamSize;
		state bool lastReady = false;
		state bool lastHealthy;
		state bool lastOptimal;
		state bool lastWrongConfiguration = team->isWrongConfiguration();
		state bool trackHealthyTeam = team->size() == self->configuration.storageTeamSize;
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
				const bool ignoreSSFailures = !badTeam && self->healthyZone.get().present() &&
				                              (self->healthyZone.get().get() == ignoreSSFailuresZoneString);
				int serversLeft = 0, serverUndesired = 0, serverWrongConf = 0, serverWiggling = 0;

				for (const UID& uid : team->getServerIDs()) {
					change.push_back(self->server_status.onChange(uid));
					auto& status = self->server_status.get(uid);
					if (!status.isFailed || ignoreSSFailures) {
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
				if (ignoreSSFailures) {
					ASSERT_WE_THINK(serversLeft == team->size());
				}

				if (!self->initialFailureReactionDelay.isReady()) {
					change.push_back(self->initialFailureReactionDelay);
				}
				change.push_back(self->zeroHealthyTeams->onChange());

				bool healthy = !badTeam && !anyUndesired && serversLeft == team->size();
				team->setHealthy(healthy); // Unhealthy teams won't be chosen by bestTeam
				bool optimal = team->isOptimal() && healthy;
				bool containsFailed = self->teamContainsFailedServer(team);
				bool recheck = !healthy && (lastReady != self->initialFailureReactionDelay.isReady() ||
				                            (lastZeroHealthy && !self->zeroHealthyTeams->get()) || containsFailed);

				TraceEvent(SevVerbose, "TeamHealthChangeDetected", self->distributorId)
				    .detail("Team", team->getDesc())
				    .detail("ServersLeft", serversLeft)
				    .detail("LastServersLeft", lastServersLeft)
				    .detail("AnyUndesired", anyUndesired)
				    .detail("LastAnyUndesired", lastAnyUndesired)
				    .detail("AnyWrongConfiguration", anyWrongConfiguration)
				    .detail("LastWrongConfiguration", lastWrongConfiguration)
				    .detail("ContainsWigglingServer", anyWigglingServer)
				    .detail("Recheck", recheck)
				    .detail("BadTeam", badTeam)
				    .detail("LastZeroHealthy", lastZeroHealthy)
				    .detail("ZeroHealthyTeam", self->zeroHealthyTeams->get());

				lastReady = self->initialFailureReactionDelay.isReady();
				lastZeroHealthy = self->zeroHealthyTeams->get();

				if (firstCheck && trackHealthyTeam) {
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

					if (trackHealthyTeam) {
						if (optimal != lastOptimal) {
							lastOptimal = optimal;
							self->optimalTeamCount += optimal ? 1 : -1;

							ASSERT_GE(self->optimalTeamCount, 0);
							self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
						}

						if (lastHealthy != healthy) {
							lastHealthy = healthy;
							// Update healthy team count when the team healthy changes
							self->healthyTeamCount += healthy ? 1 : -1;

							ASSERT_GE(self->healthyTeamCount, 0);
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
					}

					lastServersLeft = serversLeft;
					lastAnyUndesired = anyUndesired;
					lastWrongConfiguration = anyWrongConfiguration;
					lastAnyWigglingServer = anyWigglingServer;

					state int lastPriority = team->getPriority();
					if (team->size() == 0) {
						team->setPriority(SERVER_KNOBS->PRIORITY_POPULATE_REGION);
					} else if (serversLeft < team->size()) {
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

						TraceEvent(SevVerbose, "ServerTeamRelocatingShards", self->distributorId)
						    .detail("Info", team->getDesc())
						    .detail("TeamID", team->getTeamID())
						    .detail("Shards", shards.size());

						for (int i = 0; i < shards.size(); i++) {
							// Make it high priority to move keys off failed server or else RelocateShards may never be
							// addressed
							int maxPriority = containsFailed ? SERVER_KNOBS->PRIORITY_TEAM_FAILED : team->getPriority();
							// The shard split/merge and DD rebooting may make a shard mapped to multiple teams,
							// so we need to recalculate the shard's priority
							if (maxPriority < SERVER_KNOBS->PRIORITY_TEAM_FAILED) {
								std::pair<std::vector<ShardsAffectedByTeamFailure::Team>,
								          std::vector<ShardsAffectedByTeamFailure::Team>>
								    teams = self->shardsAffectedByTeamFailure->getTeamsForFirstShard(shards[i]);
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
									ASSERT_EQ(tc->primary, t.primary);
									// tc->traceAllInfo();
									if (tc->server_info.contains(t.servers[0])) {
										auto& info = tc->server_info[t.servers[0]];

										bool found = false;
										for (int k = 0; k < info->getTeams().size(); k++) {
											if (info->getTeams()[k]->getServerIDs() == t.servers) {
												maxPriority = std::max(maxPriority, info->getTeams()[k]->getPriority());
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
										CODE_PROBE(true,
										           "A removed server is still associated with a team in "
										           "ShardsAffectedByTeamFailure");
									}
								}
							}

							RelocateShard rs(
							    shards[i], maxPriority, RelocateReason::OTHER, deterministicRandom()->randomUniqueID());

							self->output.send(rs);
							TraceEvent("SendRelocateToDDQueue", self->distributorId)
							    .suppressFor(1.0)
							    .detail("TraceId", rs.traceId)
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
				    .errorUnsuppressed(e)
				    .detail("ServerPrimary", self->primary)
				    .detail("Team", team->getDesc())
				    .detail("Priority", team->getPriority());
			}
			self->priority_teams[team->getPriority()]--;
			if (trackHealthyTeam) {
				if (team->isHealthy()) {
					self->healthyTeamCount--;
					ASSERT_GE(self->healthyTeamCount, 0);

					if (self->healthyTeamCount == 0) {
						TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->distributorId)
						    .detail("ServerPrimary", self->primary)
						    .detail("SignallingServerTeam", team->getDesc());
						self->zeroHealthyTeams->set(true);
					}
				}
				if (lastOptimal) {
					self->optimalTeamCount--;
					ASSERT_GE(self->optimalTeamCount, 0);
					self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
				}
			}
			throw;
		}
	}

	ACTOR static Future<Void> storageServerTracker(
	    DDTeamCollection* self,
	    TCServerInfo* server, // This actor is owned by this TCServerInfo, point to server_info[id]
	    Promise<Void> errorOut,
	    Version addedVersion,
	    const DDEnabledState* ddEnabledState,
	    bool isTss) {
		state Future<Void> failureTracker;
		state ServerStatus status(server->getLastKnownInterface().locality);
		state bool lastIsUnhealthy = false;
		state Future<Void> metricsTracker = server->serverMetricsPolling(self->db);

		state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged = server->onInterfaceChanged;
		state bool hasWrongDC = !self->isCorrectDC(*server);
		state bool hasInvalidLocality =
		    !self->isValidLocality(self->configuration.storagePolicy, server->getLastKnownInterface().locality);
		state int targetTeamNumPerServer =
		    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (self->configuration.storageTeamSize + 1)) / 2;
		state Future<Void> storageMetadataTracker = self->updateStorageMetadata(server);
		try {
			loop {
				state bool isBm = BlobMigratorInterface::isBlobMigrator(server->getLastKnownInterface().id());
				status.isUndesired =
				    (!self->disableFailingLaggingServers.get() && server->ssVersionTooFarBehind.get()) || isBm;

				status.isWrongConfiguration = isBm;
				status.isWiggling = false;
				hasWrongDC = !self->isCorrectDC(*server);
				hasInvalidLocality =
				    !self->isValidLocality(self->configuration.storagePolicy, server->getLastKnownInterface().locality);

				// If there is any other server on this exact NetworkAddress, this server is undesired and will
				// eventually be eliminated. This samAddress checking must be redo whenever the server's state (e.g.,
				// storeType, dcLocation, interface) is changed.
				state std::vector<Future<Void>> otherChanges;
				std::vector<Promise<Void>> wakeUpTrackers;
				for (const auto& i : self->server_and_tss_info) {
					if (self->db->isMocked())
						continue;
					if (i.second.getPtr() != server &&
					    i.second->getLastKnownInterface().address() == server->getLastKnownInterface().address()) {
						auto& statusInfo = self->server_status.get(i.first);
						TraceEvent("SameAddress", self->distributorId)
						    .detail("Failed", statusInfo.isFailed)
						    .detail("Undesired", statusInfo.isUndesired)
						    .detail("Server", server->getId())
						    .detail("OtherServer", i.second->getId())
						    .detail("Address", server->getLastKnownInterface().address())
						    .detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->getId()))
						    .detail("OtherNumShards",
						            self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->getId()))
						    .detail("OtherHealthy", !self->server_status.get(i.second->getId()).isUnhealthy());
						// wait for the server's ip to be changed
						otherChanges.push_back(self->server_status.onChange(i.second->getId()));
						if (!self->server_status.get(i.second->getId()).isUnhealthy()) {
							if (self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->getId()) >=
							    self->shardsAffectedByTeamFailure->getNumberOfShards(server->getId())) {
								TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
								    .detail("Server", server->getId())
								    .detail("Address", server->getLastKnownInterface().address())
								    .detail("OtherServer", i.second->getId())
								    .detail("NumShards",
								            self->shardsAffectedByTeamFailure->getNumberOfShards(server->getId()))
								    .detail("OtherNumShards",
								            self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->getId()));

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

				if (server->getLastKnownClass().machineClassFitness(ProcessClass::Storage) > ProcessClass::UnsetFit) {
					// NOTE: Should not use self->healthyTeamCount > 0 in if statement, which will cause status bouncing
					// between healthy and unhealthy and result in OOM (See PR#2228).

					if (self->optimalTeamCount > 0) {
						TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
						    .detail("Server", server->getId())
						    .detail("OptimalTeamCount", self->optimalTeamCount)
						    .detail("Fitness", server->getLastKnownClass().machineClassFitness(ProcessClass::Storage));
						status.isUndesired = true;
					}
					otherChanges.push_back(self->zeroOptimalTeams.onChange());
				}

				// If this storage server has the wrong key-value store type, then mark it undesired so it will be
				// replaced with a server having the correct type
				if (hasWrongDC || hasInvalidLocality) {
					TraceEvent(SevWarn, "UndesiredDCOrLocality", self->distributorId)
					    .detail("Server", server->getId())
					    .detail("WrongDC", hasWrongDC)
					    .detail("InvalidLocality", hasInvalidLocality);
					status.isUndesired = true;
					status.isWrongConfiguration = true;
				}
				if (server->wrongStoreTypeToRemove.get()) {
					TraceEvent(SevWarn, "WrongStoreTypeToRemove", self->distributorId)
					    .detail("Server", server->getId())
					    .detail("StoreType", "?");
					status.isUndesired = true;
					status.isWrongConfiguration = true;
				}

				// An invalid wiggle server should set itself the right status. Otherwise, it cannot be re-included by
				// wiggler.
				auto invalidWiggleServer =
				    [](const AddressExclusion& addr, const DDTeamCollection* tc, const TCServerInfo* server) {
					    return !tc->wigglingId.present() || server->getId() != tc->wigglingId.get();
				    };
				// If the storage server is in the excluded servers list, it is undesired
				NetworkAddress a = server->getLastKnownInterface().address();
				AddressExclusion worstAddr(a.ip, a.port);
				DDTeamCollection::Status worstStatus = self->excludedServers.get(worstAddr);

				if (worstStatus == DDTeamCollection::Status::WIGGLING && invalidWiggleServer(worstAddr, self, server)) {
					TraceEvent(SevInfo, "InvalidWiggleServer", self->distributorId)
					    .detail("Address", worstAddr.toString())
					    .detail("ServerId", server->getId())
					    .detail("WigglingId", self->wigglingId.present() ? self->wigglingId.get().toString() : "");
					worstStatus = DDTeamCollection::Status::NONE;
				}
				otherChanges.push_back(self->excludedServers.onChange(worstAddr));

				for (int i = 0; i < 3; i++) {
					if (i > 0 && !server->getLastKnownInterface().secondaryAddress().present()) {
						break;
					}
					AddressExclusion testAddr;
					if (i == 0)
						testAddr = AddressExclusion(a.ip);
					else if (i == 1)
						testAddr = AddressExclusion(server->getLastKnownInterface().secondaryAddress().get().ip,
						                            server->getLastKnownInterface().secondaryAddress().get().port);
					else if (i == 2)
						testAddr = AddressExclusion(server->getLastKnownInterface().secondaryAddress().get().ip);
					DDTeamCollection::Status testStatus = self->excludedServers.get(testAddr);

					if (testStatus == DDTeamCollection::Status::WIGGLING &&
					    invalidWiggleServer(testAddr, self, server)) {
						TraceEvent(SevInfo, "InvalidWiggleServer", self->distributorId)
						    .detail("Address", worstAddr.toString())
						    .detail("ServerId", server->getId())
						    .detail("WigglingId", self->wigglingId.present() ? self->wigglingId.get().toString() : "");
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
					    .detail("Server", server->getId())
					    .detail("Excluded", worstAddr.toString());
					status.isUndesired = true;
					status.isWrongConfiguration = true;

					if (worstStatus == DDTeamCollection::Status::WIGGLING && !isTss) {
						status.isWiggling = true;
						TraceEvent("PerpetualStorageWiggleSS", self->distributorId)
						    .detail("Primary", self->primary)
						    .detail("Server", server->getId())
						    .detail("ProcessId", server->getLastKnownInterface().locality.processId())
						    .detail("Address", worstAddr.toString());
					} else if (worstStatus == DDTeamCollection::Status::FAILED && !isTss) {
						TraceEvent(SevWarn, "FailedServerRemoveKeys", self->distributorId)
						    .detail("Server", server->getId())
						    .detail("Excluded", worstAddr.toString());
						wait(delay(0.0)); // Do not throw an error while still inside trackExcludedServers
						while (!ddEnabledState->isEnabled()) {
							wait(delay(1.0));
						}
						if (self->removeFailedServer.canBeSet()) {
							self->removeFailedServer.send(server->getId());
						}
						throw movekeys_conflict();
					}
				}

				failureTracker = storageServerFailureTracker(self, server, &status, addedVersion);
				// We need to recruit new storage servers if the key value store type has changed
				if (hasWrongDC || hasInvalidLocality || server->wrongStoreTypeToRemove.get()) {
					self->restartRecruiting.trigger();
				}

				if (lastIsUnhealthy && !status.isUnhealthy() && !isTss &&
				    (server->getTeams().size() < targetTeamNumPerServer || self->lastBuildTeamsFailed)) {
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
						    .detail("ServerID", server->getId())
						    .detail("Status", "Removing");

						if (server->updated.canBeSet()) {
							server->updated.send(Void());
						}

						// Remove server from FF/serverList
						storageMetadataTracker.cancel();
						wait(self->db->removeStorageServer(
						    server->getId(), server->getLastKnownInterface().tssPairID, self->lock, ddEnabledState));

						TraceEvent("StatusMapChange", self->distributorId)
						    .detail("ServerID", server->getId())
						    .detail("Status", "Removed");
						// Sets removeSignal (alerting dataDistributionTeamCollection to remove the storage server from
						// its own data structures)
						server->removed.send(Void());
						if (isTss) {
							self->removedTSS.send(server->getId());
						} else {
							self->removedServers.send(server->getId());
						}
						return Void();
					}
					when(std::pair<StorageServerInterface, ProcessClass> newInterface = wait(interfaceChanged)) {
						auto const& lastKnownInterface = server->getLastKnownInterface();
						bool restartRecruiting = newInterface.first.waitFailure.getEndpoint().getPrimaryAddress() !=
						                         lastKnownInterface.waitFailure.getEndpoint().getPrimaryAddress();
						bool localityChanged = lastKnownInterface.locality != newInterface.first.locality;
						bool machineLocalityChanged =
						    lastKnownInterface.locality.zoneId().get() != newInterface.first.locality.zoneId().get();
						TraceEvent("StorageServerInterfaceChanged", self->distributorId)
						    .detail("ServerID", server->getId())
						    .detail("NewWaitFailureToken", newInterface.first.waitFailure.getEndpoint().token)
						    .detail("OldWaitFailureToken", lastKnownInterface.waitFailure.getEndpoint().token)
						    .detail("LocalityChanged", localityChanged)
						    .detail("MachineLocalityChanged", machineLocalityChanged);

						server->updateLastKnown(newInterface.first, newInterface.second);
						if (localityChanged && !isTss) {
							CODE_PROBE(true, "Server locality changed");

							// The locality change of a server will affect machine teams related to the server if
							// the server's machine locality is changed
							if (machineLocalityChanged) {
								// First handle the impact on the machine of the server on the old locality
								Reference<TCMachineInfo> machine = server->machine;
								ASSERT_GE(machine->serversOnMachine.size(), 1);
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
								    self->checkAndCreateMachine(self->server_info[server->getId()]);
								ASSERT(destMachine.isValid());
							}

							// Ensure the server's server team belong to a machine team, and
							// Get the newBadTeams due to the locality change
							std::vector<Reference<TCTeamInfo>> newBadTeams;
							for (auto& serverTeam : server->getTeams()) {
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

							server->updateInDesiredDC(self->includedDCs);
							self->resetLocalitySet();

							bool addedNewBadTeam = false;
							for (auto it : newBadTeams) {
								if (self->removeTeam(it)) {
									self->addTeam(it->getServers(), IsInitialTeam::True);
									addedNewBadTeam = true;
								}
							}
							if (addedNewBadTeam && self->badTeamRemover.isReady()) {
								// TODO: Improve simulation testing to test locality changes. Until then, we
								// realistically don't expect this code probe to be hit.
								CODE_PROBE(true, "Server locality change created bad teams", probe::decoration::rare);
								self->doBuildTeams = true;
								self->badTeamRemover = removeBadTeams(self);
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
						                      server->getLastKnownInterface().locality);

						// self->traceTeamCollectionInfo();
						recordTeamCollectionInfo = true;
						// Restart the storeTracker for the new interface. This will cancel the previous
						// keyValueStoreTypeTracker
						storageMetadataTracker = self->updateStorageMetadata(server);
						hasWrongDC = !self->isCorrectDC(*server);
						hasInvalidLocality = !self->isValidLocality(self->configuration.storagePolicy,
						                                            server->getLastKnownInterface().locality);
						self->restartTeamBuilder.trigger();

						if (restartRecruiting)
							self->restartRecruiting.trigger();
					}
					when(wait(otherChanges.empty() ? Never() : quorum(otherChanges, 1))) {
						TraceEvent("SameAddressChangedStatus", self->distributorId).detail("ServerID", server->getId());
					}
					when(wait(server->wrongStoreTypeToRemove.onChange())) {
						TraceEvent("UndesiredStorageServerTriggered", self->distributorId)
						    .detail("Server", server->getId())
						    .detail("StoreType", server->getStoreType())
						    .detail("ConfigStoreType", self->configuration.storageServerStoreType)
						    .detail("WrongStoreTypeRemoved", server->wrongStoreTypeToRemove.get());
					}
					when(wait(server->wakeUpTracker.getFuture())) {
						server->wakeUpTracker = Promise<Void>();
					}
					when(wait(storageMetadataTracker)) {}
					when(wait(server->ssVersionTooFarBehind.onChange())) {}
					when(wait(self->disableFailingLaggingServers.onChange())) {}
					when(wait(server->longStorageQueue.onChange())) {
						int64_t threshold = calculateTeamStorageQueueThreshold(self->teams);
						// threshold represents the queue length of majority teams
						// team queue length is defined as the max queue size among all SSes of the team
						if (server->longStorageQueue.get() < threshold) {
							TraceEvent(SevInfo, "TriggerStorageQueueRebalanceIgnored", self->distributorId)
							    .detail("SSID", server->getId());
						} else {
							TraceEvent(SevInfo, "TriggerStorageQueueRebalance", self->distributorId)
							    .detail("SSID", server->getId());
							std::vector<ShardsAffectedByTeamFailure::Team> teams;
							for (const auto& team : server->getTeams()) {
								std::vector<UID> servers;
								for (const auto& server : team->getServers()) {
									servers.push_back(server->getId());
								}
								teams.push_back(ShardsAffectedByTeamFailure::Team(servers, self->primary));
							}
							self->triggerStorageQueueRebalance.send(
							    RebalanceStorageQueueRequest(server->getId(), teams, self->primary));
						}
					}
				}

				if (recordTeamCollectionInfo) {
					self->traceTeamCollectionInfo();
				}
			}
		} catch (Error& e) {
			state Error err = e;
			TraceEvent("StorageServerTrackerCancelled", self->distributorId)
			    .errorUnsuppressed(e)
			    .suppressFor(1.0)
			    .detail("Primary", self->primary)
			    .detail("Server", server->getId());
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
			wait(self->waitUntilHealthy());

			bool foundSSToRemove = false;

			for (auto& server : self->server_info) {
				// If this server isn't the right storage type and its wrong-type trigger has not yet been set
				// then set it if we're in aggressive mode and log its presence either way.
				if (!(server.second->isCorrectStoreType(self->configuration.storageServerStoreType) ||
				      server.second->isCorrectStoreType(self->configuration.perpetualStoreType)) &&
				    !server.second->wrongStoreTypeToRemove.get()) {
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
					    .detail("StoreType", server.second->getStoreType())
					    .detail("ConfiguredStoreType", self->configuration.storageServerStoreType)
					    .detail("RemovingNow",
					            self->configuration.storageMigrationType == StorageMigrationType::AGGRESSIVE);
				}
			}

			// Stop if no incorrect storage types were found, or if we're not in aggressive mode and can't act on any
			// found. Aggressive mode is checked at this location so that in non-aggressive mode the loop will execute
			// once and log any incorrect storage types found.
			if (!foundSSToRemove || self->configuration.storageMigrationType != StorageMigrationType::AGGRESSIVE) {
				break;
			}
		}

		return Void();
	}

	// NOTE: this actor returns when the cluster is healthy and stable (no server is expected to be removed in a period)
	// processingWiggle and processingUnhealthy indicate that some servers are going to be removed.
	ACTOR static Future<Void> waitUntilHealthy(DDTeamCollection const* self, double extraDelay, WaitWiggle waitWiggle) {
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
		wait(self->waitUntilHealthy());
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

	ACTOR static Future<Void> storageServerFailureTracker(DDTeamCollection* self,
	                                                      TCServerInfo* server,
	                                                      ServerStatus* status,
	                                                      Version addedVersion) {
		state StorageServerInterface interf = server->getLastKnownInterface();
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
						TraceEvent("StorageServerBecomeHealthy", self->distributorId)
						    .detail("ServerID", interf.id())
						    .detail("ServerIpAddress", interf.address());
						self->unhealthyServers--;
					}
					if (!unhealthy && status->isUnhealthy()) {
						TraceEvent(SevWarn, "StorageServerUnhealthy", self->distributorId)
						    .detail("ServerID", interf.id())
						    .detail("ServerIpAddress", interf.address());
						self->unhealthyServers++;
					}
				} else if (status->isUnhealthy()) {
					TraceEvent(SevWarn, "StorageServerUnhealthy", self->distributorId)
					    .detail("ServerID", interf.id())
					    .detail("ServerIpAddress", interf.address());
					self->unhealthyServers++;
				}
			}

			self->server_status.set(interf.id(), *status);
			if (status->isFailed) {
				TraceEvent("RestartRecruiting", self->distributorId)
				    .detail("FailedServerID", interf.id())
				    .detail("IsTSS", interf.isTss());
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
						} else if (SERVER_KNOBS->DD_REMOVE_MAINTENANCE_ON_FAILURE &&
						           self->clearHealthyZoneFuture.isReady()) {
							self->clearHealthyZoneFuture = clearHealthyZone(self->dbContext());
							TraceEvent("MaintenanceZoneCleared", self->distributorId).log();
							self->healthyZone.set(Optional<Key>());
						}
					}
					if (!status->isUnhealthy()) {
						// On server transition from unhealthy -> healthy, trigger buildTeam check,
						// handles scenario when team building failed due to insufficient healthy servers.
						// Operation cost is minimal if currentTeamCount == desiredTeamCount/maxTeamCount.
						self->doBuildTeams = true;
					}

					TraceEvent(SevDebug, "StatusMapChange", self->distributorId)
					    .detail("ServerID", interf.id())
					    .detail("Status", status->toString())
					    .detail(
					        "Available",
					        IFailureMonitor::failureMonitor().getState(interf.waitFailure.getEndpoint()).isAvailable());
				}
				when(wait(status->isUnhealthy() ? self->waitForAllDataRemoved(interf.id(), addedVersion) : Never())) {
					break;
				}
				when(wait(self->healthyZone.onChange())) {}
			}
		}

		return Void(); // Don't ignore failures
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

			wait(self->waitUntilHealthy(SERVER_KNOBS->TR_REMOVE_SERVER_TEAM_EXTRA_DELAY));

			// Wait for the badTeamRemover() to avoid the potential race between adding the bad team (add the team
			// tracker) and remove bad team (cancel the team tracker).
			wait(self->badTeamRemover);

			if (SERVER_KNOBS->TR_LOW_SPACE_PIVOT_DELAY_SEC > 0 &&
			    self->teamPivots.pivotAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
				TraceEvent(SevWarn, "MachineTeamRemoverDelayedForLowSpacePivot", self->distributorId)
				    .detail("IsPrimary", self->primary)
				    .detail("CurrentSpacePivot", self->teamPivots.pivotAvailableSpaceRatio)
				    .detail("TargetSpacePivot", SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO);
				wait(delay(SERVER_KNOBS->TR_LOW_SPACE_PIVOT_DELAY_SEC));
				continue;
			}

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
				for (teamIndex = 0; teamIndex < mt->getServerTeams().size(); ++teamIndex) {
					team = mt->getServerTeams()[teamIndex];
					ASSERT(team->machineTeam->getMachineIDs() == mt->getMachineIDs()); // Sanity check

					// Check if a server will have 0 team after the team is removed
					for (auto& s : team->getServers()) {
						if (s->getTeams().size() == 0) {
							TraceEvent(SevError, "MachineTeamRemoverTooAggressive", self->distributorId)
							    .detail("Server", s->getId())
							    .detail("ServerTeam", team->getDesc());
							self->traceAllInfo(true);
						}
					}

					// The team will be marked as a bad team
					bool foundTeam = self->removeTeam(team);
					ASSERT(foundTeam);
					// removeTeam() has side effect of swapping the last element to the current pos
					// in the serverTeams vector in the machine team.
					--teamIndex;
					self->addTeam(team->getServers(), IsInitialTeam::True, IsRedundantTeam::True);
					CODE_PROBE(true, "Removed machine team");
				}

				self->doBuildTeams = true;

				if (self->badTeamRemover.isReady()) {
					self->badTeamRemover = removeBadTeams(self);
					self->addActor.send(self->badTeamRemover);
				}

				TraceEvent("MachineTeamRemover", self->distributorId)
				    .detail("MachineTeamIDToRemove", mt->id().shortString())
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
				wait(self->waitUntilHealthy(SERVER_KNOBS->TR_REMOVE_SERVER_TEAM_EXTRA_DELAY));
			}
			// Wait for the badTeamRemover() to avoid the potential race between
			// adding the bad team (add the team tracker) and remove bad team (cancel the team tracker).
			wait(self->badTeamRemover);

			if (SERVER_KNOBS->TR_LOW_SPACE_PIVOT_DELAY_SEC > 0 &&
			    self->teamPivots.pivotAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
				TraceEvent(SevWarn, "ServerTeamRemoverDelayedForLowSpacePivot", self->distributorId)
				    .detail("IsPrimary", self->primary)
				    .detail("CurrentSpacePivot", self->teamPivots.pivotAvailableSpaceRatio)
				    .detail("TargetSpacePivot", SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO);
				wait(delay(SERVER_KNOBS->TR_LOW_SPACE_PIVOT_DELAY_SEC));
				continue;
			}

			// From this point, all server teams should be healthy, because we wait above
			// until processingUnhealthy is done, and all machines are healthy
			int desiredServerTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * self->server_info.size();
			int totalSTCount = self->teams.size();
			// Pick the server team whose members are on the most number of server teams, and mark it undesired
			std::pair<Reference<TCTeamInfo>, int> foundSTInfo = self->getServerTeamWithMostProcessTeams();

			if (totalSTCount > desiredServerTeams * (1 + SERVER_KNOBS->TR_REDUNDANT_TEAM_PERCENTAGE_THRESHOLD) &&
			    foundSTInfo.first.isValid()) {
				ASSERT(foundSTInfo.first.isValid());
				Reference<TCTeamInfo> st = foundSTInfo.first;
				int maxNumProcessTeams = foundSTInfo.second;
				ASSERT(st.isValid());
				// The team will be marked as a bad team
				bool foundTeam = self->removeTeam(st);
				ASSERT(foundTeam);
				self->addTeam(st->getServers(), IsInitialTeam::True, IsRedundantTeam::True);
				CODE_PROBE(true, "Marked team as a bad team");

				self->doBuildTeams = true;

				if (self->badTeamRemover.isReady()) {
					self->badTeamRemover = removeBadTeams(self);
					self->addActor.send(self->badTeamRemover);
				}

				TraceEvent("ServerTeamRemover", self->distributorId)
				    .detail("ServerTeamToRemove", st->getServerIDsStr())
				    .detail("ServerTeamID", st->getTeamID())
				    .detail("NumProcessTeamsOnTheServerTeam", maxNumProcessTeams)
				    .detail("CurrentServerTeams", totalSTCount)
				    .detail("DesiredServerTeams", desiredServerTeams)
				    .detail("Primary", self->primary);

				numServerTeamRemoved++;
			} else {
				if (numServerTeamRemoved > 0) {
					// Only trace the information when we remove a machine team
					TraceEvent("ServerTeamRemoverDone", self->distributorId)
					    .detail("CurrentServerTeams", self->teams.size())
					    .detail("DesiredServerTeams", desiredServerTeams)
					    .detail("NumServerTeamRemoved", numServerTeamRemoved)
					    .detail("Primary", self->primary);
					self->traceTeamCollectionInfo();
					numServerTeamRemoved = 0; // Reset the counter to avoid keep printing the message
				}
			}
		}
	}

	ACTOR static Future<Void> fixUnderReplicationLoop(DDTeamCollection* self) {
		loop {
			wait(delay(SERVER_KNOBS->DD_FIX_WRONG_REPLICAS_DELAY));
			self->cleanupLargeTeams();
			self->fixUnderReplication();
		}
	}

	ACTOR static Future<Void> trackExcludedServers(DDTeamCollection* self) {
		state ExclusionTracker exclusionTracker(self->dbContext());
		loop {
			// wait for new set of excluded servers
			wait(exclusionTracker.changed.onTrigger());

			// Reset and reassign self->excludedServers based on excluded, but we only
			// want to trigger entries that are different
			// Do not retrigger and double-overwrite failed or wiggling servers
			auto old = self->excludedServers.getKeys();
			for (const auto& o : old) {
				if (!exclusionTracker.excluded.contains(o) && !exclusionTracker.failed.contains(o) &&
				    !(self->excludedServers.count(o) &&
				      self->excludedServers.get(o) == DDTeamCollection::Status::WIGGLING)) {
					self->excludedServers.set(o, DDTeamCollection::Status::NONE);
				}
			}
			for (const auto& n : exclusionTracker.excluded) {
				if (!exclusionTracker.failed.contains(n)) {
					self->excludedServers.set(n, DDTeamCollection::Status::EXCLUDED);
				}
			}

			for (const auto& f : exclusionTracker.failed) {
				self->excludedServers.set(f, DDTeamCollection::Status::FAILED);
			}

			TraceEvent("DDExcludedServersChanged", self->distributorId)
			    .detail("AddressesExcluded", exclusionTracker.excluded.size())
			    .detail("AddressesFailed", exclusionTracker.failed.size())
			    .detail("Primary", self->isPrimary());

			self->restartRecruiting.trigger();
		}
	}

	ACTOR static Future<Void> updateNextWigglingStorageID(DDTeamCollection* self) {
		state StorageWiggleData wiggleState;
		state KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())> metadataMap =
		    wiggleState.wigglingStorageServer(PrimaryRegion(self->primary));

		state UID nextId = wait(self->getNextWigglingServerID());
		state StorageWiggleValue value(nextId);
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->dbContext()));
		loop {
			// write the next server id
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				metadataMap.set(tr, nextId, value);
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		self->nextWiggleInfo.send(value);
		TraceEvent(SevDebug, "PerpetualStorageWiggleNextID", self->distributorId)
		    .detail("Primary", self->primary)
		    .detail("WriteID", nextId);

		return Void();
	}

	ACTOR static Future<Void> waitPerpetualWiggleDelay(DDTeamCollection* self) {
		if (g_network->isSimulated() && g_simulator->isConsistencyChecked) {
			// Wiggle can cause consistency check to repeatedly restart. So we want to
			// slow it down to avoid consistency check timeout.
			wait(delay(300));
			return Void();
		} else if (SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY <= 60.0) {
			wait(delay(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY));
			return Void();
		}
		state double nextDelay = 60.0;
		while (true) {
			wait(delay(nextDelay));

			double totalDelay = wait(self->storageWiggler->wiggleData.addPerpetualWiggleDelay(
			    self->dbContext().getReference(), PrimaryRegion(self->primary), nextDelay));
			nextDelay = std::min(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY - totalDelay, 60.0);

			if (totalDelay >= SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY) {
				wait(self->storageWiggler->wiggleData.clearPerpetualWiggleDelay(self->dbContext().getReference(),
				                                                                PrimaryRegion(self->primary)));
				return Void();
			}
		}
	}

	ACTOR static Future<Void> perpetualStorageWiggleRest(DDTeamCollection* self) {
		state bool takeRest = true;
		state Promise<int64_t> avgShardBytes;
		while (takeRest) {
			// a minimal delay to avoid excluding and including SS too fast
			wait(waitPerpetualWiggleDelay(self));

			avgShardBytes.reset();
			self->getAverageShardBytes.send(avgShardBytes);
			int64_t avgBytes = wait(avgShardBytes.getFuture());
			double ratio;
			bool imbalance;
			int numSSToBeLoadBytesBalanced;

			if (SERVER_KNOBS->PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO) {
				// PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO: Maximum number of storage servers that can
				// have the load bytes less than PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO before perpetual
				// wiggle will start the next wiggle.
				// The wiggle waits until the numSSToBeLoadBytesBalanced to be less than
				// PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO before starting the next wiggle. With this we can have
				// multiple SS that are in balancing state. Used to speed up wiggling rather than waiting for every
				// SS to get balanced/filledup before starting the next wiggle.
				numSSToBeLoadBytesBalanced =
				    self->numSSToBeLoadBytesBalanced(avgBytes * SERVER_KNOBS->PERPETUAL_WIGGLE_SMALL_LOAD_RATIO);
				imbalance = numSSToBeLoadBytesBalanced > SERVER_KNOBS->PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO;
			} else {
				ratio = self->loadBytesBalanceRatio(avgBytes * SERVER_KNOBS->PERPETUAL_WIGGLE_SMALL_LOAD_RATIO);
				imbalance = ratio < SERVER_KNOBS->PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO;
			}
			CODE_PROBE(imbalance, "Perpetual Wiggle pause because cluster is imbalance.");

			// there must not have other teams to place wiggled data
			takeRest = self->server_info.size() <= self->configuration.storageTeamSize ||
			           self->machine_info.size() < self->configuration.storageTeamSize || imbalance;

			if (SERVER_KNOBS->PERPETUAL_WIGGLE_PAUSE_AFTER_TSS_TARGET_MET &&
			    self->configuration.storageMigrationType == StorageMigrationType::DEFAULT) {
				takeRest = takeRest || (self->getTargetTSSInDC() > 0 && self->reachTSSPairTarget());
			}

			// log the extra delay and change the wiggler state
			if (takeRest) {
				self->storageWiggler->setWiggleState(StorageWiggler::PAUSE);
				Severity sev =
				    self->configuration.storageMigrationType == StorageMigrationType::GRADUAL ? SevWarn : SevInfo;
				TraceEvent(sev, "PerpetualStorageWiggleSleep", self->distributorId)
				    .suppressFor(SERVER_KNOBS->PERPETUAL_WIGGLE_DELAY * 4)
				    .detail("Primary", self->primary)
				    .detail("ImbalanceFactor",
				            SERVER_KNOBS->PW_MAX_SS_LESSTHAN_MIN_BYTES_BALANCE_RATIO ? numSSToBeLoadBytesBalanced
				                                                                     : ratio)
				    .detail("ServerSize", self->server_info.size())
				    .detail("MachineSize", self->machine_info.size())
				    .detail("StorageTeamSize", self->configuration.storageTeamSize)
				    .detail("TargetTSSInDC", self->getTargetTSSInDC())
				    .detail("ReachTSSPairTarget", self->reachTSSPairTarget())
				    .detail("MigrationType", self->configuration.storageMigrationType.toString());
			}
		}
		return Void();
	}

	ACTOR static Future<Void> perpetualStorageWiggleIterator(DDTeamCollection* teamCollection,
	                                                         AsyncVar<bool>* stopSignal,
	                                                         FutureStream<Void> finishStorageWiggleSignal) {
		loop {
			choose {
				when(wait(stopSignal->onChange())) {}
				when(waitNext(finishStorageWiggleSignal)) {
					// delay to avoid delete and update ServerList too frequently, which could result busy loop or over
					// utilize the disk of other active SS
					wait(perpetualStorageWiggleRest(teamCollection));
					wait(updateNextWigglingStorageID(teamCollection));
				}
			}
			if (stopSignal->get()) {
				break;
			}
		}

		return Void();
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
				TraceEvent("PerpetualWigglePausedDueToClusterHealth")
				    .detail("UnhealthyRelocation", count)
				    .detail("HealthyTeamCount", self->healthyTeamCount)
				    .detail("BestTeamStuckCount", self->bestTeamKeepStuckCount)
				    .detail("PausePenalty", pausePenalty)
				    .detail("Primary", self->primary);
			} else {
				self->pauseWiggle->set(false);
			}
			wait(delay(SERVER_KNOBS->CHECK_TEAM_DELAY, TaskPriority::DataDistributionLow));
		}
	}

	ACTOR static Future<Void> perpetualStorageWiggler(DDTeamCollection* self,
	                                                  AsyncVar<bool>* stopSignal,
	                                                  PromiseStream<Void> finishStorageWiggleSignal) {
		state StorageWiggleData wiggleState;
		state KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())> metadataMap =
		    wiggleState.wigglingStorageServer(PrimaryRegion(self->primary));

		state Future<StorageWiggleValue> nextFuture = Never();
		state Future<Void> moveFinishFuture = Never();
		state int extraTeamCount = 0;
		state Future<Void> ddQueueCheck = clusterHealthCheckForPerpetualWiggle(self, &extraTeamCount);
		state FutureStream<StorageWiggleValue> nextStream = self->nextWiggleInfo.getFuture();

		wait(readStorageWiggleMap(self));

		if (!self->wigglingId.present()) {
			// skip to the next valid ID
			nextFuture = waitAndForward(nextStream);
			finishStorageWiggleSignal.send(Void());
		}

		loop {
			state Future<Void> pauseChanged = self->pauseWiggle->onChange();
			state Future<Void> stopChanged = stopSignal->onChange();
			if (self->wigglingId.present()) {
				state UID id = self->wigglingId.get();
				if (self->pauseWiggle->get()) {
					CODE_PROBE(true, "paused because cluster is unhealthy");
					moveFinishFuture = Never();
					self->includeStorageServersForWiggle();
					self->storageWiggler->setWiggleState(StorageWiggler::PAUSE);
					TraceEvent(self->configuration.storageMigrationType == StorageMigrationType::AGGRESSIVE ? SevInfo
					                                                                                        : SevWarn,
					           "PerpetualStorageWigglePause",
					           self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("ServerId", id)
					    .detail("BestTeamKeepStuckCount", self->bestTeamKeepStuckCount)
					    .detail("ExtraHealthyTeamCount", extraTeamCount)
					    .detail("HealthyTeamCount", self->healthyTeamCount);
				} else {
					choose {
						when(wait(self->waitUntilHealthy())) {
							CODE_PROBE(true, "start wiggling");
							wait(self->storageWiggler->startWiggle());
							auto fv = self->excludeStorageServersForWiggle(id);
							moveFinishFuture = fv;
							self->storageWiggler->setWiggleState(StorageWiggler::RUN);
							TraceEvent("PerpetualStorageWiggleStart", self->distributorId)
							    .detail("Primary", self->primary)
							    .detail("ServerId", id)
							    .detail("ExtraHealthyTeamCount", extraTeamCount)
							    .detail("HealthyTeamCount", self->healthyTeamCount);
						}
						when(wait(pauseChanged)) {
							continue;
						}
					}
				}
			}

			choose {
				when(StorageWiggleValue value = wait(nextFuture)) {
					ASSERT(!self->wigglingId.present()); // the previous wiggle must be finished
					nextFuture = Never();
					self->wigglingId = value.id;
					// random delay
					wait(delayJittered(5.0, TaskPriority::DataDistributionLow));
				}
				when(wait(moveFinishFuture)) {
					ASSERT(self->wigglingId.present());
					self->waitUntilRecruited.set(true);
					self->restartTeamBuilder.trigger();

					moveFinishFuture = Never();
					self->includeStorageServersForWiggle();
					TraceEvent("PerpetualStorageWiggleFinish", self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("ServerId", self->wigglingId.get());

					wait(self->eraseStorageWiggleMap(&metadataMap, self->wigglingId.get()) &&
					     self->storageWiggler->finishWiggle());
					self->wigglingId.reset();
					nextFuture = waitAndForward(nextStream);
					finishStorageWiggleSignal.send(Void());
					extraTeamCount = std::max(0, extraTeamCount - 1);
				}
				when(wait(ddQueueCheck || pauseChanged || stopChanged)) {}
			}

			if (stopSignal->get()) {
				break;
			}
		}

		if (self->wigglingId.present()) {
			self->includeStorageServersForWiggle();
			TraceEvent("PerpetualStorageWiggleExitingPause", self->distributorId)
			    .detail("Primary", self->primary)
			    .detail("ServerId", self->wigglingId.get());
			self->wigglingId.reset();
		}

		return Void();
	}

	// This coroutine sets a watch to monitor the value change of `perpetualStorageWiggleKey` which is controlled by
	// command `configure perpetual_storage_wiggle=$value` if the value is 1, this actor start 2 actors,
	// `perpetualStorageWiggleIterator` and `perpetualStorageWiggler`. Otherwise, it sends stop signal to them.
	ACTOR static Future<Void> monitorPerpetualStorageWiggle(DDTeamCollection* self) {
		state int speed = 0;
		state PromiseStream<Void> finishStorageWiggleSignal;
		state SignalableActorCollection collection;
		self->pauseWiggle = makeReference<AsyncVar<bool>>(true);
		ASSERT(self->storageWiggler->isStopped());

		loop {
			state ReadYourWritesTransaction tr(self->dbContext());
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
					if (speed == 1 && self->storageWiggler->isStopped()) { // avoid duplicated start
						self->storageWiggler->setStopSignal(false);
						wait(self->storageWiggler->restoreStats());
						collection.add(self->perpetualStorageWiggleIterator(self->storageWiggler->stopWiggleSignal,
						                                                    finishStorageWiggleSignal.getFuture()));
						collection.add(self->perpetualStorageWiggler(self->storageWiggler->stopWiggleSignal,
						                                             finishStorageWiggleSignal));
						TraceEvent("PerpetualStorageWiggleOpen", self->distributorId).detail("Primary", self->primary);
					} else if (speed == 0) {
						if (!self->storageWiggler->isStopped()) {
							self->storageWiggler->setStopSignal(true);
							wait(collection.signalAndReset());
							self->pauseWiggle->set(true);
						}
						wait(self->storageWiggler->resetStats());
						TraceEvent("PerpetualStorageWiggleClose", self->distributorId).detail("Primary", self->primary);
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
		state ReadYourWritesTransaction tr(self->dbContext());
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> val = wait(tr.get(healthyZoneKey));
				state Future<Void> healthyZoneTimeout = Never();
				if (val.present()) {
					auto p = decodeHealthyZoneValue(val.get());
					if (p.first == ignoreSSFailuresZoneString) {
						// healthyZone is now overloaded for DD disabling purpose, which does not timeout
						TraceEvent("DataDistributionDisabledForStorageServerFailuresStart", self->distributorId).log();
						healthyZoneTimeout = Never();
						self->healthyZone.set(p.first);
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
		    .detail("Primary", self->primary)
		    .trackLatest(self->storageServerRecruitmentEventHolder->trackingKey);
		loop {
			if (!recruiting) {
				while (self->recruitingStream.get() == 0) {
					wait(self->recruitingStream.onChange());
				}
				TraceEvent("StorageServerRecruitment", self->distributorId)
				    .detail("State", "Recruiting")
				    .detail("IsTSS", self->isTssRecruiting ? "True" : "False")
				    .detail("Primary", self->primary)
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
								    .detail("Primary", self->primary)
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
				    .detail("Primary", self->primary)
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

			// insert recruiting localities BEFORE actor waits, to ensure we don't send many recruitment requests to the
			// same storage
			self->recruitingIds.insert(interfaceId);
			self->recruitingLocalities.insert(candidateWorker.worker.stableAddress());

			state InitializeStorageRequest isr;
			isr.storeType = recruitTss ? self->configuration.testingStorageServerStoreType
			                           : self->configuration.storageServerStoreType;

			// Check if perpetual storage wiggle is enabled and perpetualStoreType is set. If so, we use
			// perpetualStoreType for all new SSes that match perpetualStorageWiggleLocality.
			// Note that this only applies to regular storage servers, not TSS.
			if (!recruitTss && self->configuration.storageMigrationType == StorageMigrationType::GRADUAL &&
			    self->configuration.perpetualStoreType.isValid()) {
				if (self->configuration.perpetualStorageWiggleLocality == "0") {
					isr.storeType = self->configuration.perpetualStoreType;
				} else {
					std::vector<std::pair<Optional<Value>, Optional<Value>>> localityKeyValues =
					    ParsePerpetualStorageWiggleLocality(self->configuration.perpetualStorageWiggleLocality);
					if (localityMatchInList(localityKeyValues, candidateWorker.worker.locality)) {
						isr.storeType = self->configuration.perpetualStoreType;
					}
				}
			}
			isr.seedTag = invalidTag;
			isr.reqId = deterministicRandom()->randomUniqueID();
			isr.interfaceId = interfaceId;
			isr.encryptMode = self->configuration.encryptionAtRestMode;

			// if tss, wait for pair ss to finish and add its id to isr. If pair fails, don't recruit tss
			state bool doRecruit = true;
			if (recruitTss) {
				TraceEvent("TSS_Recruit", self->distributorId)
				    .detail("ReqID", isr.reqId)
				    .detail("TSSID", interfaceId)
				    .detail("Stage", "TSSWaitingPair")
				    .detail("TSSAddr", candidateWorker.worker.address())
				    .detail("TSSLocality", candidateWorker.worker.locality.toString())
				    .detail("Primary", self->primary);

				Optional<std::pair<UID, Version>> ssPairInfoResult = wait(tssState->waitOnSS());
				if (ssPairInfoResult.present()) {
					isr.tssPairIDAndVersion = ssPairInfoResult.get();

					TraceEvent("TSS_Recruit", self->distributorId)
					    .detail("ReqID", isr.reqId)
					    .detail("SSID", ssPairInfoResult.get().first)
					    .detail("TSSID", interfaceId)
					    .detail("Stage", "TSSGotPair")
					    .detail("TSSAddr", candidateWorker.worker.address())
					    .detail("SSVersion", ssPairInfoResult.get().second)
					    .detail("TSSLocality", candidateWorker.worker.locality.toString())
					    .detail("Primary", self->primary);
				} else {
					doRecruit = false;

					TraceEvent(SevWarnAlways, "TSS_RecruitError", self->distributorId)
					    .detail("ReqID", isr.reqId)
					    .detail("TSSID", interfaceId)
					    .detail("Reason", "TSS failed to get SS pair for some reason")
					    .detail("TSSAddr", candidateWorker.worker.address())
					    .detail("TSSLocality", candidateWorker.worker.locality.toString())
					    .detail("Primary", self->primary);
				}
			}

			TraceEvent("DDRecruiting")
			    .detail("ReqID", isr.reqId)
			    .detail("Primary", self->primary)
			    .detail("State", "Sending request to worker")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address())
			    .detail("TSS", recruitTss ? "true" : "false")
			    .detail("RecruitingStream", self->recruitingStream.get())
			    .detail("StoreType", isr.storeType);

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
				    .detail("ReqID", isr.reqId)
				    .detail("SSID", interfaceId)
				    .detail("Stage", "SSSignaling")
				    .detail("SSAddr", candidateWorker.worker.address())
				    .detail("SSLocality", candidateWorker.worker.locality.toString())
				    .detail("Primary", self->primary);

				// wait for timeout, but eventually move on if no TSS pair recruited
				Optional<bool> tssSuccessful =
				    wait(timeout(tssState->waitOnTSS(), SERVER_KNOBS->TSS_RECRUITMENT_TIMEOUT));

				if (tssSuccessful.present() && tssSuccessful.get()) {
					TraceEvent("TSS_Recruit", self->distributorId)
					    .detail("ReqID", isr.reqId)
					    .detail("SSID", interfaceId)
					    .detail("Stage", "SSGotPair")
					    .detail("SSAddr", candidateWorker.worker.address())
					    .detail("SSLocality", candidateWorker.worker.locality.toString())
					    .detail("Primary", self->primary);
				} else {
					TraceEvent(SevWarn, "TSS_RecruitError", self->distributorId)
					    .detail("ReqID", isr.reqId)
					    .detail("SSID", interfaceId)
					    .detail("Reason",
					            tssSuccessful.present() ? "TSS recruitment failed for some reason"
					                                    : "TSS recruitment timed out")
					    .detail("SSAddr", candidateWorker.worker.address())
					    .detail("SSLocality", candidateWorker.worker.locality.toString())
					    .detail("Primary", self->primary);
				}
			}

			self->recruitingIds.erase(interfaceId);
			self->recruitingLocalities.erase(candidateWorker.worker.stableAddress());

			TraceEvent("DDRecruiting")
			    .detail("ReqID", isr.reqId)
			    .detail("Primary", self->primary)
			    .detail("State", "Finished request")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address())
			    .detail("RecruitingStream", self->recruitingStream.get())
			    .detail("TSS", recruitTss ? "true" : "false")
			    .detail("StoreType", isr.storeType);

			if (newServer.present()) {
				UID id = newServer.get().interf.id();
				if (!self->server_and_tss_info.contains(id)) {
					if (!recruitTss || tssState->tssRecruitSuccess()) {
						self->addServer(newServer.get().interf,
						                candidateWorker.processClass,
						                self->serverTrackerErrorOut,
						                newServer.get().addedVersion,
						                *ddEnabledState);
						TraceEvent("DDRecruiting")
						    .detail("ReqID", isr.reqId)
						    .detail("Primary", self->primary)
						    .detail("State", "Add new SS to DD")
						    .detail("WorkerID", candidateWorker.worker.id())
						    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
						    .detail("Interf", interfaceId)
						    .detail("Addr", candidateWorker.worker.address())
						    .detail("RecruitingStream", self->recruitingStream.get())
						    .detail("TSS", recruitTss ? "true" : "false")
						    .detail("StoreType", isr.storeType);
						self->waitUntilRecruited.set(false);
						// signal all done after adding tss to tracking info
						tssState->markComplete();
					}
				} else {
					TraceEvent(SevWarn, "DDRecruitmentError")
					    .detail("ReqID", isr.reqId)
					    .detail("Reason", "Server ID already recruited")
					    .detail("ServerID", id)
					    .detail("Primary", self->primary);
				}
			}
		}

		// SS and/or TSS recruitment failed at this point, update tssState
		if (recruitTss && tssState->tssRecruitFailed()) {
			tssState->markComplete();
			CODE_PROBE(true, "TSS recruitment failed for some reason");
		}
		if (!recruitTss && tssState->ssRecruitFailed()) {
			CODE_PROBE(true, "SS with pair TSS recruitment failed for some reason");
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
		state std::map<AddressExclusion, int> numSSIgnoredPerAddr;

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
				targetTSSInDC = self->getTargetTSSInDC();
				int newTssToRecruit = targetTSSInDC - self->tss_info_by_pair.size() - inProgressTSSCount;
				// FIXME: Should log this if the recruit count stays the same but the other numbers update?
				if (newTssToRecruit != tssToRecruit) {
					TraceEvent("TSS_RecruitUpdated", self->distributorId)
					    .detail("Desired", targetTSSInDC)
					    .detail("Existing", self->tss_info_by_pair.size())
					    .detail("InProgress", inProgressTSSCount)
					    .detail("NotStarted", newTssToRecruit)
					    .detail("Primary", self->primary);
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
				numSSIgnoredPerAddr.clear();
				hasHealthyTeam = (self->healthyTeamCount != 0);
				RecruitStorageRequest rsr;
				std::set<AddressExclusion> exclusions;
				// Exclude existing servers running SS from being recruited again.
				for (auto s = self->server_and_tss_info.begin(); s != self->server_and_tss_info.end(); ++s) {
					auto serverStatus = self->server_status.get(s->second->getLastKnownInterface().id());
					auto addr = s->second->getLastKnownInterface().stableAddress();
					AddressExclusion addrExcl(addr.ip, addr.port);
					if (serverStatus.excludeOnRecruit()) {
						TraceEvent(SevDebug, "DDRecruitExcl1")
						    .detail("Primary", self->primary)
						    .detail("Excluding", s->second->getLastKnownInterface().address());
						exclusions.insert(addrExcl);
						numSSPerAddr[addrExcl]++; // increase from 0
					} else {
						numSSIgnoredPerAddr[addrExcl]++;
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

				for (auto& it : numSSIgnoredPerAddr) {
					if (it.second > 2) {
						// In this case, we know initialize storage will skip recruiting this host due to too many
						// storages already on the process. Exclude it from the request to the CC to try to find a
						// better fit, especially in the critical recruitment case. This is temporary while storages are
						// in this state. Either these failed storages will eventually have data moved away, which will
						// trigger restartRecruiting again, or the host will become healthy again, in which case we
						// won't need to recruit on it and it would be counted with Excl1.
						exclusions.insert(it.first);
						TraceEvent(SevDebug, "DDRecruitExcl3")
						    .detail("Primary", self->primary)
						    .detail("Excluding", it.first.toString())
						    .detail("IgnoredCount", it.second);
					}
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
						// Note that this call may be blocked in CC when there are no more storage process matching
						// the criteria in RecruitStorageRequest.
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
							    .detail("Locality", candidateWorker.worker.locality.toString())
							    .detail("Primary", self->primary);

							CODE_PROBE(true, "Starting TSS recruitment");
							self->isTssRecruiting = true;
							tssState = makeReference<TSSPairState>(candidateWorker.worker.locality);

							addTSSInProgress.send(tssState->waitComplete());
							self->addActor.send(
							    initializeStorage(self, candidateWorker, ddEnabledState, true, tssState));
							checkTss = self->initialFailureReactionDelay;
						} else {
							if (tssState->active && tssState->inDataZone(candidateWorker.worker.locality)) {
								CODE_PROBE(true, "TSS recruits pair in same dc/datahall");
								self->isTssRecruiting = false;
								TraceEvent("TSS_Recruit", self->distributorId)
								    .detail("Stage", "PairSS")
								    .detail("Addr", candidateSSAddr.toString())
								    .detail("Locality", candidateWorker.worker.locality.toString())
								    .detail("Primary", self->primary);
								self->addActor.send(
								    initializeStorage(self, candidateWorker, ddEnabledState, false, tssState));
								// successfully started recruitment of pair, reset tss recruitment state
								tssState = makeReference<TSSPairState>();
							} else {
								CODE_PROBE(
								    tssState->active,
								    "TSS recruitment skipped potential pair because it's in a different dc/datahall");
								self->addActor.send(initializeStorage(
								    self, candidateWorker, ddEnabledState, false, makeReference<TSSPairState>()));
							}
						}
					}
					when(wait(recruitStorage->onChange())) {
						fCandidateWorker = Future<RecruitStorageReply>();
					}
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
							CODE_PROBE(tssToRecruit < 0, "tss recruitment cancelled due to too many TSS");
							CODE_PROBE(self->zeroHealthyTeams->get(),
							           "tss recruitment cancelled due zero healthy teams");

							TraceEvent(SevWarn, "TSS_RecruitCancelled", self->distributorId)
							    .detail("Primary", self->primary)
							    .detail("Reason", tssToRecruit <= 0 ? "TooMany" : "ZeroHealthyTeams");
							tssState->cancel();
							tssState = makeReference<TSSPairState>();
							self->isTssRecruiting = false;

							pendingTSSCheck = true;
							checkTss = delay(SERVER_KNOBS->TSS_DD_CHECK_INTERVAL);
						} else if (tssToKill > 0) {
							auto itr = self->tss_info_by_pair.begin();
							for (int i = 0; i < tssToKill; i++, itr++) {
								UID tssId = itr->second->getId();
								StorageServerInterface tssi = itr->second->getLastKnownInterface();

								if (self->shouldHandleServer(tssi) && self->server_and_tss_info.contains(tssId)) {
									Promise<Void> killPromise = itr->second->killTss;
									if (killPromise.canBeSet()) {
										CODE_PROBE(tssToRecruit < 0, "Killing TSS due to too many TSS");
										CODE_PROBE(self->zeroHealthyTeams->get(), "Killing TSS due zero healthy teams");
										TraceEvent(SevWarn, "TSS_DDKill", self->distributorId)
										    .detail("Primary", self->primary)
										    .detail("TSSID", tssId)
										    .detail("Reason",
										            self->zeroHealthyTeams->get() ? "ZeroHealthyTeams" : "TooMany");
										Promise<Void> shutdown = self->shutdown;
										killPromise.send(Void());
										if (!shutdown.canBeSet()) {
											return Void(); // "self" got destroyed, so return.
										}
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
							if (!self->zeroHealthyTeams->get()) {
								checkTss = Never();
							} else {
								checkTss = delay(SERVER_KNOBS->TSS_DD_CHECK_INTERVAL);
							}
						}
					}
					when(wait(self->restartRecruiting.onTrigger())) {}
				}
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskPriority::DataDistribution));
			} catch (Error& e) {
				if (e.code() != error_code_timed_out) {
					throw;
				}
				CODE_PROBE(true, "Storage recruitment timed out");
			}
		}
	}

	ACTOR static Future<Void> updateReplicasKey(DDTeamCollection* self, Optional<Key> dcId) {
		std::vector<Future<Void>> serverUpdates;

		for (auto& it : self->server_info) {
			serverUpdates.push_back(it.second->updated.getFuture());
		}

		wait(self->initialFailureReactionDelay && waitForAll(serverUpdates));
		wait(self->waitUntilHealthy());
		TraceEvent("DDUpdatingReplicas", self->distributorId)
		    .detail("Primary", self->primary)
		    .detail("DcId", dcId)
		    .detail("Replicas", self->configuration.storageTeamSize);

		int oldReplicas = wait(self->db->tryUpdateReplicasKeyForDc(dcId, self->configuration.storageTeamSize));
		if (oldReplicas == self->configuration.storageTeamSize) {
			TraceEvent("DDUpdatedAlready", self->distributorId)
			    .detail("Primary", self->primary)
			    .detail("DcId", dcId)
			    .detail("Replicas", self->configuration.storageTeamSize);
		} else {
			TraceEvent("DDUpdatedReplicas", self->distributorId)
			    .detail("Primary", self->primary)
			    .detail("DcId", dcId)
			    .detail("Replicas", self->configuration.storageTeamSize)
			    .detail("OldReplicas", oldReplicas);
		}
		return Void();
	}

	ACTOR static Future<Void> serverGetTeamRequests(DDTeamCollection* self, TeamCollectionInterface tci) {
		loop {
			GetTeamRequest req = waitNext(tci.getTeam.getFuture());
			if (req.findTeamByServers) {
				getTeamByServers(self, req);
			} else if (req.findTeamForBulkLoad) {
				self->addActor.send(getTeamForBulkLoad(self, req));
			} else {
				self->addActor.send(self->getTeam(req));
			}
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

	ACTOR static Future<Void> waitServerListChange(DDTeamCollection* self,
	                                               FutureStream<Void> serverRemoved,
	                                               const DDEnabledState* ddEnabledState) {
		state Future<Void> checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY, TaskPriority::DataDistributionLaunch);
		state Future<ServerWorkerInfos> serverListAndProcessClasses = Never();
		state bool isFetchingResults = false;
		loop {
			choose {
				when(wait(checkSignal)) {
					checkSignal = Never();
					isFetchingResults = true;
					serverListAndProcessClasses = self->db->getServerListAndProcessClasses();
				}
				when(ServerWorkerInfos infos = wait(serverListAndProcessClasses)) {
					auto& servers = infos.servers;
					serverListAndProcessClasses = Never();
					isFetchingResults = false;

					for (int i = 0; i < servers.size(); i++) {
						UID serverId = servers[i].first.id();
						StorageServerInterface const& ssi = servers[i].first;
						ProcessClass const& processClass = servers[i].second;
						if (!self->shouldHandleServer(ssi)) {
							continue;
						} else if (self->server_and_tss_info.contains(serverId)) {
							auto& serverInfo = self->server_and_tss_info[serverId];
							if (ssi.getValue.getEndpoint() !=
							        serverInfo->getLastKnownInterface().getValue.getEndpoint() ||
							    processClass != serverInfo->getLastKnownClass().classType()) {
								Promise<std::pair<StorageServerInterface, ProcessClass>> currentInterfaceChanged =
								    serverInfo->interfaceChanged;
								serverInfo->interfaceChanged =
								    Promise<std::pair<StorageServerInterface, ProcessClass>>();
								serverInfo->onInterfaceChanged =
								    Future<std::pair<StorageServerInterface, ProcessClass>>(
								        serverInfo->interfaceChanged.getFuture());
								currentInterfaceChanged.send(std::make_pair(ssi, processClass));
							}
						} else if (!self->recruitingIds.contains(ssi.id())) {
							self->addServer(ssi,
							                processClass,
							                self->serverTrackerErrorOut,
							                infos.readVersion.get(),
							                *ddEnabledState);
						}
					}

					checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY, TaskPriority::DataDistributionLaunch);
				}
				when(waitNext(serverRemoved)) {
					if (isFetchingResults) {
						serverListAndProcessClasses = self->db->getServerListAndProcessClasses();
					}
				}
			}
		}
	}

	ACTOR static Future<UID> getNextWigglingServerID(
	    Reference<StorageWiggler> wiggler,
	    std::vector<std::pair<Optional<Value>, Optional<Value>>> localityKeyValues =
	        std::vector<std::pair<Optional<Value>, Optional<Value>>>(),
	    DDTeamCollection* teamCollection = nullptr) {
		ASSERT(wiggler->teamCollection == teamCollection);
		loop {
			// when the DC need more
			state Optional<UID> id =
			    wiggler->getNextServerId(teamCollection == nullptr || teamCollection->reachTSSPairTarget());
			if (!id.present()) {
				wait(wiggler->onCheck());
				continue;
			}

			// if perpetual_storage_wiggle_locality has value and not 0(disabled).
			if (!localityKeyValues.empty()) {
				// Whether the selected server matches the locality
				auto server = teamCollection->server_info.at(id.get());
				// TraceEvent("PerpetualLocality").detail("Server", server->getLastKnownInterface().locality.get(localityKey)).detail("Desire", localityValue);
				if (localityMatchInList(localityKeyValues, server->getLastKnownInterface().locality)) {
					return id.get();
				}

				if (wiggler->empty()) {
					// None of the entries in wiggle queue matches the given locality.
					TraceEvent("PerpetualStorageWiggleEmptyQueue", teamCollection->distributorId)
					    .detail("Primary", teamCollection->primary)
					    .detail("WriteValue", "No process matched the given perpetualStorageWiggleLocality")
					    .detail("PerpetualStorageWiggleLocality",
					            teamCollection->configuration.perpetualStorageWiggleLocality);
				}
				continue;
			}
			return id.get();
		}
	}

	// read the current map of `perpetualStorageWiggleIDPrefix`, then restore wigglingId.
	ACTOR static Future<Void> readStorageWiggleMap(DDTeamCollection* self) {
		state StorageWiggleData wiggleState;
		state KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())> metadataMap =
		    wiggleState.wigglingStorageServer(PrimaryRegion(self->primary));
		state std::vector<std::pair<UID, StorageWiggleValue>> res =
		    wait(readStorageWiggleValues(self->dbContext(), self->primary, false));

		if (res.size() > 0) {
			// SOMEDAY: support wiggle multiple SS at once
			ASSERT(!self->wigglingId.present()); // only single process wiggle is allowed

			std::vector<std::pair<Optional<Value>, Optional<Value>>> localityKeyValues;
			if (self->configuration.perpetualStorageWiggleLocality != "0") {
				localityKeyValues =
				    ParsePerpetualStorageWiggleLocality(self->configuration.perpetualStorageWiggleLocality);
			}

			// if perpetual_storage_wiggle_locality has value and not 0(disabled).
			if (!localityKeyValues.empty()) {
				if (self->server_info.contains(res.begin()->first)) {
					auto server = self->server_info.at(res.begin()->first);
					for (const auto& [localityKey, localityValue] : localityKeyValues) {
						// Update the wigglingId only if it matches the locality.
						if (server->getLastKnownInterface().locality.get(localityKey.get()) == localityValue) {
							self->wigglingId = res.begin()->first;
							break;
						}
					}

					if (!self->wigglingId.present()) {
						wait(self->eraseStorageWiggleMap(&metadataMap, res.begin()->first));
					}
				}
			} else {
				self->wigglingId = res.begin()->first;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> updateStorageMetadata(DDTeamCollection* self, TCServerInfo* server) {
		state KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(
		    serverMetadataKeys.begin, IncludeVersion());
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dbContext());

		state bool isTss = server->getLastKnownInterface().isTss();
		// Update server's storeType, especially when it was created
		wait(server->updateStoreType());
		if (server->getStoreType() == KeyValueStoreType::SSD_SHARDED_ROCKSDB &&
		    !SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			TraceEvent(SevError, "PhysicalShardNotEnabledForShardedRocks", self->getDistributorId())
			    .detail("StorageServer", server->getId());
			throw internal_error();
		}
		state StorageMetadataType data(
		    StorageMetadataType::currentTime(),
		    server->getStoreType(),
		    !(server->isCorrectStoreType(isTss ? self->configuration.testingStorageServerStoreType
		                                       : self->configuration.storageServerStoreType) ||
		      server->isCorrectStoreType(isTss ? self->configuration.testingStorageServerStoreType
		                                       : self->configuration.perpetualStoreType)));

		// read storage metadata
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> serverInterfaceValue = wait(tr->get(serverListKeyFor(server->getId())));
				// The storage server is removed
				if (!serverInterfaceValue.present()) {
					TraceEvent("UpdateStorageMetadataNoOp", self->getDistributorId())
					    .detail("Server", server->getId())
					    .detail("IsTss", isTss)
					    .detail("Reason", "Absent server list item");
					return Never();
				}
				Optional<StorageMetadataType> metadata = wait(metadataMap.get(tr, server->getId()));
				// NOTE: in upgrade testing, there may not be any metadata
				// TODO: change to ASSERT(metadata.present()) in a release version only supports upgrade from 71.3
				if (metadata.present()) {
					data.createdTime = metadata.get().createdTime;
				}
				metadataMap.set(tr, server->getId(), data);
				tr->set(serverMetadataChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		// printf("------ updated metadata %s\n", server->getId().toString().c_str());
		TraceEvent("UpdateStorageMetadata", self->getDistributorId())
		    .detail("Server", server->getId())
		    .detail("IsTss", isTss);

		// wrong store type handler
		if (!isTss) {
			if (!(server->isCorrectStoreType(self->configuration.storageServerStoreType) ||
			      server->isCorrectStoreType(self->configuration.perpetualStoreType)) &&
			    self->wrongStoreTypeRemover.isReady()) {
				self->wrongStoreTypeRemover = removeWrongStoreType(self);
				self->addActor.send(self->wrongStoreTypeRemover);
			}
			// add server to wiggler
			if (self->storageWiggler->contains(server->getId())) {
				self->storageWiggler->updateMetadata(server->getId(), data);
			} else {
				self->storageWiggler->addServer(server->getId(), data);
			}
		}

		return Never();
	}

	ACTOR static Future<Void> run(Reference<DDTeamCollection> teamCollection,
	                              Reference<InitialDataDistribution> initData,
	                              TeamCollectionInterface tci,
	                              Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
	                              DDEnabledState const* ddEnabledState) {
		state DDTeamCollection* self = teamCollection.getPtr();
		state Future<Void> loggingTrigger = Void();
		state PromiseStream<Void> serverRemoved;
		state Future<Void> error = actorCollection(self->addActor.getFuture());

		try {
			wait(self->init(initData, *ddEnabledState));
			initData.clear(); // release reference count
			self->addActor.send(self->serverGetTeamRequests(tci));

			TraceEvent("DDTeamCollectionBegin", self->distributorId).detail("Primary", self->primary);
			wait(self->readyToStart || error);
			TraceEvent("DDTeamCollectionReadyToStart", self->distributorId).detail("Primary", self->primary);

			// removeBadTeams() does not always run. We may need to restart the actor when needed.
			// So we need the badTeamRemover variable to check if the actor is ready.
			if (self->badTeamRemover.isReady()) {
				self->badTeamRemover = self->removeBadTeams();
				self->addActor.send(self->badTeamRemover);
			}

			self->addActor.send(self->machineTeamRemover());
			self->addActor.send(self->serverTeamRemover());
			if (ddLargeTeamEnabled()) {
				self->addActor.send(self->fixUnderReplicationLoop());
			}

			if (self->wrongStoreTypeRemover.isReady()) {
				self->wrongStoreTypeRemover = self->removeWrongStoreType();
				self->addActor.send(self->wrongStoreTypeRemover);
			}

			self->traceTeamCollectionInfo();

			if (self->includedDCs.size()) {
				// start this actor before any potential recruitments can happen
				self->addActor.send(self->updateReplicasKey(self->includedDCs[0]));
			}

			// The following actors (e.g. storageRecruiter) do not need to be assigned to a variable because
			// they are always running.
			self->addActor.send(self->monitorHealthyTeams());

			if (!self->db->isMocked()) {
				self->addActor.send(self->storageRecruiter(recruitStorage, *ddEnabledState));
				self->addActor.send(self->monitorStorageServerRecruitment());
				self->addActor.send(self->waitServerListChange(serverRemoved.getFuture(), *ddEnabledState));
				self->addActor.send(self->trackExcludedServers());
				self->addActor.send(self->waitHealthyZoneChange());
				self->addActor.send(self->monitorPerpetualStorageWiggle());
			}
			// SOMEDAY: Monitor FF/serverList for (new) servers that aren't in allServers and add or remove them

			loop choose {
				when(UID removedServer = waitNext(self->removedServers.getFuture())) {
					CODE_PROBE(true, "Storage server removed from database");
					self->removeServer(removedServer);
					serverRemoved.send(Void());

					self->restartRecruiting.trigger();
				}
				when(UID removedTSS = waitNext(self->removedTSS.getFuture())) {
					CODE_PROBE(true, "TSS removed from database");
					self->removeTSS(removedTSS);
					serverRemoved.send(Void());

					self->restartRecruiting.trigger();
				}
				when(wait(self->zeroHealthyTeams->onChange())) {
					if (self->zeroHealthyTeams->get()) {
						self->restartRecruiting.trigger();
						self->noHealthyTeams();
					}
				}
				when(wait(loggingTrigger)) {
					int highestPriority = 0;
					for (auto it : self->priority_teams) {
						if (it.second > 0) {
							highestPriority = std::max(highestPriority, it.first);
						}
					}

					TraceEvent("TotalDataInFlight", self->distributorId)
					    .detail("Primary", self->primary)
					    .detail("TotalBytes", self->getDebugTotalDataInFlight())
					    .detail("UnhealthyServers", self->unhealthyServers)
					    .detail("ServerCount", self->server_info.size())
					    .detail("StorageTeamSize", self->configuration.storageTeamSize)
					    .detail("ZeroHealthy", self->zeroOptimalTeams.get())
					    .detail("HighestPriority", highestPriority)
					    .trackLatest(self->primary ? "TotalDataInFlight"
					                               : "TotalDataInFlightRemote"); // This trace event's trackLatest
					                                                             // lifetime is controlled by
					// DataDistributor::totalDataInFlightEventHolder or
					// DataDistributor::totalDataInFlightRemoteEventHolder.
					// The track latest key we use here must match the key used in
					// the holder.

					loggingTrigger = delay(SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL, TaskPriority::FlushTrace);
				}
				when(wait(self->serverTrackerErrorOut.getFuture())) {} // Propagate errors from storageServerTracker
				when(wait(error)) {}
			}
		} catch (Error& e) {
			if (e.code() != error_code_movekeys_conflict)
				TraceEvent(SevError, "DataDistributionTeamCollectionError", self->distributorId).error(e);
			throw e;
		}
	}

	// Take a snapshot of necessary data structures from `DDTeamCollection` and print them out with yields to avoid slow
	// task on the run loop.
	ACTOR static Future<Void> printSnapshotTeamsInfo(Reference<DDTeamCollection> self) {
		state DatabaseConfiguration configuration;
		state std::map<UID, Reference<TCServerInfo>> server_info;
		state std::map<UID, ServerStatus> server_status;
		state std::vector<Reference<TCTeamInfo>> teams;
		state std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
		state std::vector<Reference<TCMachineTeamInfo>> machineTeams;
		// state std::vector<std::string> internedLocalityRecordKeyNameStrings;
		// state int machineLocalityMapEntryArraySize;
		// state std::vector<Reference<LocalityRecord>> machineLocalityMapRecordArray;
		state int traceEventsPrinted = 0;
		state std::vector<const UID*> serverIDs;
		state double lastPrintTime = 0;
		loop {

			wait(self->printDetailedTeamsInfo.onTrigger() || self->db->waitDDTeamInfoPrintSignal());

			if (now() - lastPrintTime < SERVER_KNOBS->DD_TEAMS_INFO_PRINT_INTERVAL) {
				continue;
			}
			lastPrintTime = now();

			traceEventsPrinted = 0;

			double snapshotStart = now();

			configuration = self->configuration;
			server_info = self->server_info;
			teams = self->teams;
			// Perform deep copy so we have a consistent snapshot, even if yields are performed
			for (const auto& [machineId, info] : self->machine_info) {
				machine_info.emplace(machineId, info->clone());
			}
			machineTeams = self->machineTeams;
			// internedLocalityRecordKeyNameStrings = self->machineLocalityMap._keymap->_lookuparray;
			// machineLocalityMapEntryArraySize = self->machineLocalityMap.size();
			// machineLocalityMapRecordArray = self->machineLocalityMap.getRecordArray();
			std::vector<const UID*> _uids = self->machineLocalityMap.getObjects();
			serverIDs = _uids;

			auto const& keys = self->server_status.getKeys();
			for (auto const& key : keys) {
				// Add to or update the local server_status map
				server_status[key] = self->server_status.get(key);
			}

			TraceEvent("DDPrintSnapshotTeamsInfo", self->getDistributorId())
			    .detail("SnapshotSpeed", now() - snapshotStart)
			    .detail("Primary", self->isPrimary());

			// Print to TraceEvents
			TraceEvent("DDConfig", self->getDistributorId())
			    .detail("StorageTeamSize", configuration.storageTeamSize)
			    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER)
			    .detail("MaxTeamsPerServer", SERVER_KNOBS->MAX_TEAMS_PER_SERVER)
			    .detail("Primary", self->isPrimary());

			TraceEvent("ServerInfo", self->getDistributorId())
			    .detail("Size", server_info.size())
			    .detail("Primary", self->isPrimary());

			state int i;
			state std::map<UID, Reference<TCServerInfo>>::iterator server = server_info.begin();
			for (i = 0; i < server_info.size(); i++) {
				auto serverStats = server->second->getStorageStats();
				TraceEvent("ServerInfo", self->getDistributorId())
				    .detail("ServerInfoIndex", i)
				    .detail("ServerID", server->first.toString())
				    .detail("ServerTeamOwned", server->second->getTeams().size())
				    .detail("MachineID", server->second->machine->machineID.contents().toString())
				    .detail("Primary", self->isPrimary())
				    .detail("IsInDesiredDC", server->second->isInDesiredDC())
				    .detail("DataInFlightToServer", server->second->getDataInFlightToServer())
				    .detail("ReadInFlightToServer", server->second->getReadInFlightToServer())
				    .detail("IsWigglePausedServer", server->second->isWigglePausedServer())
				    .detail("SpaceBytesAvailable",
				            server->second->metricsPresent() ? server->second->spaceBytes().first : -1)
				    .detail("SpaceBytesCapacity",
				            server->second->metricsPresent() ? server->second->spaceBytes().second : -1)
				    .detail("LoadBytes", server->second->metricsPresent() ? server->second->loadBytes() : -1)
				    .detail("CpuUsage", serverStats.present() ? serverStats.get().cpuUsage : 100.0);
				// If storage server hasn't gotten the health metrics updated, we assume it's too busy to respond so
				// return 100.0;
				server++;
				if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
					wait(yield());
				}
			}

			server = server_info.begin();
			for (i = 0; i < server_info.size(); i++) {
				const UID& uid = server->first;

				TraceEvent e("ServerStatus", self->getDistributorId());
				e.detail("ServerUID", uid)
				    .detail("MachineIsValid", server_info[uid]->machine.isValid())
				    .detail("IsMachineHealthy", self->isMachineHealthy(server_info[uid]->machine))
				    .detail("MachineTeamSize",
				            server_info[uid]->machine.isValid() ? server_info[uid]->machine->machineTeams.size() : -1)
				    .detail("Primary", self->isPrimary());

				// ServerStatus might not be known if server was very recently added and
				// storageServerFailureTracker() has not yet updated self->server_status If the UID is not found, do
				// not assume the server is healthy or unhealthy
				auto it = server_status.find(uid);
				if (it != server_status.end()) {
					e.detail("Healthy", !it->second.isUnhealthy());
					e.detail("IsWiggling", it->second.isWiggling);
					e.detail("IsFailed", it->second.isFailed);
					e.detail("IsUndesired", it->second.isUndesired);
					e.detail("IsWrongConfiguration", it->second.isWrongConfiguration);
					e.detail("Initialized", it->second.initialized);
					e.detail("ExcludeOnRecruit", it->second.excludeOnRecruit());
					e.detail("IsUnhealthy", it->second.isUnhealthy());
					e.detail("Locality", it->second.locality.toString());
				}

				server++;
				if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
					wait(yield());
				}
			}

			TraceEvent("ServerTeamInfo", self->getDistributorId())
			    .detail("Size", teams.size())
			    .detail("Primary", self->isPrimary());

			for (i = 0; i < teams.size(); i++) {
				const auto& team = teams[i];

				TraceEvent("ServerTeamInfo", self->getDistributorId())
				    .detail("TeamIndex", i)
				    .detail("Healthy", team->isHealthy())
				    .detail("TeamSize", team->size())
				    .detail("MemberIDs", team->getServerIDsStr())
				    .detail("Primary", self->isPrimary())
				    .detail("TeamID", team->getTeamID())
				    .detail("InflightDataToTeam", team->getDataInFlightToTeam())
				    .detail("LoadBytes", team->getLoadBytes())
				    .detail("ReadLoad", team->getReadLoad())
				    .detail("AverageCPU", team->getAverageCPU())
				    .detail("ReadInFlightToTeam", team->getReadInFlightToTeam())
				    .detail("MinAvailableSpace", team->getMinAvailableSpace())
				    .detail("MinAvailableSpaceRatio", team->getMinAvailableSpaceRatio())
				    .detail("IsOptimal", team->isOptimal())
				    .detail("IsWrongConfiguration", team->isWrongConfiguration())
				    .detail("IsHealthy", team->isHealthy())
				    .detail("Priority", team->getPriority())
				    .detail("HasWigglePausedServer", team->hasWigglePausedServer())
				    .detail("Shards",
				            self->shardsAffectedByTeamFailure
				                ->getShardsFor(ShardsAffectedByTeamFailure::Team(team->getServerIDs(), self->primary))
				                .size());
				if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
					wait(yield());
				}
			}

			TraceEvent("MachineInfo", self->getDistributorId())
			    .detail("Size", machine_info.size())
			    .detail("Primary", self->isPrimary());
			state std::map<Standalone<StringRef>, Reference<TCMachineInfo>>::iterator machine = machine_info.begin();
			state bool isMachineHealthy = false;
			for (i = 0; i < machine_info.size(); i++) {
				Reference<TCMachineInfo> _machine = machine->second;
				bool machineIDFound = machine_info.find(_machine->machineID) != machine_info.end();
				bool zeroHealthyServersOnMachine = true;
				if (!_machine.isValid() || !machineIDFound || _machine->serversOnMachine.empty()) {
					isMachineHealthy = false;
				}

				// Healthy machine has at least one healthy server
				for (auto& server : _machine->serversOnMachine) {
					// ServerStatus might not be known if server was very recently added and
					// storageServerFailureTracker() has not yet updated self->server_status If the UID is not
					// found, do not assume the server is healthy
					auto it = server_status.find(server->getId());
					if (it != server_status.end() && !it->second.isUnhealthy()) {
						isMachineHealthy = true;
						zeroHealthyServersOnMachine = false;
						break;
					}
				}

				TraceEvent("MachineInfo", self->getDistributorId())
				    .detail("MachineInfoIndex", i)
				    .detail("Healthy", isMachineHealthy)
				    .detail("MachineIDFound", machineIDFound)
				    .detail("ZeroServersOnMachine", _machine->serversOnMachine.empty())
				    .detail("ZeroHealthyServersOnMachine", zeroHealthyServersOnMachine)
				    .detail("MachineID", machine->first.contents().toString())
				    .detail("MachineTeamOwned", machine->second->machineTeams.size())
				    .detail("ServerNumOnMachine", machine->second->serversOnMachine.size())
				    .detail("ServersID", machine->second->getServersIDStr())
				    .detail("Primary", self->isPrimary());
				machine++;
				if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
					wait(yield());
				}
			}

			TraceEvent("MachineTeamInfo", self->getDistributorId())
			    .detail("Size", machineTeams.size())
			    .detail("Primary", self->isPrimary());

			for (i = 0; i < machineTeams.size(); i++) {
				const auto& team = machineTeams[i];
				TraceEvent(g_network->isSimulated() ? SevVerbose : SevInfo, "MachineTeamInfo", self->getDistributorId())
				    .detail("TeamIndex", i)
				    .detail("MachineIDs", team->getMachineIDsStr())
				    .detail("ServerTeams", team->getServerTeams().size())
				    .detail("Primary", self->isPrimary());
				if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
					wait(yield());
				}
			}

			// TODO: re-enable the following logging or remove them.
			// TraceEvent("LocalityRecordKeyName", self->getDistributorId())
			//     .detail("Size", internedLocalityRecordKeyNameStrings.size())
			//     .detail("Primary", self->isPrimary());
			// for (i = 0; i < internedLocalityRecordKeyNameStrings.size(); i++) {
			// 	TraceEvent("LocalityRecordKeyIndexName", self->getDistributorId())
			// 	    .detail("KeyIndex", i)
			// 	    .detail("KeyName", internedLocalityRecordKeyNameStrings[i])
			// 	    .detail("Primary", self->isPrimary());
			// 	if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
			// 		wait(yield());
			// 	}
			// }

			// TraceEvent("MachineLocalityMap", self->getDistributorId())
			//     .detail("Size", machineLocalityMapEntryArraySize)
			//     .detail("Primary", self->isPrimary());
			// for (i = 0; i < serverIDs.size(); i++) {
			// 	const auto& serverID = serverIDs[i];
			// 	Reference<LocalityRecord> record = machineLocalityMapRecordArray[i];
			// 	if (record.isValid()) {
			// 		TraceEvent("MachineLocalityMap", self->getDistributorId())
			// 		    .detail("LocalityIndex", i)
			// 		    .detail("UID", serverID->toString())
			// 		    .detail("LocalityRecord", record->toString())
			// 		    .detail("Primary", self->isPrimary());
			// 	} else {
			// 		TraceEvent("MachineLocalityMap", self->getDistributorId())
			// 		    .detail("LocalityIndex", i)
			// 		    .detail("UID", serverID->toString())
			// 		    .detail("LocalityRecord", "[NotFound]")
			// 		    .detail("Primary", self->isPrimary());
			// 	}
			// 	if (++traceEventsPrinted % SERVER_KNOBS->DD_TEAMS_INFO_PRINT_YIELD_COUNT == 0) {
			// 		wait(yield());
			// 	}
			// }
		}
	}
}; // class DDTeamCollectionImpl

void DDTeamCollection::updateAvailableSpacePivots() {
	std::vector<double> teamAvailableSpace;
	for (int i = 0; i < teams.size(); ++i) {
		if (teams[i]->isHealthy()) {
			teamAvailableSpace.push_back(teams[i]->getMinAvailableSpaceRatio());
		}
	}

	if (!teamAvailableSpace.empty()) {
		ASSERT_LT(SERVER_KNOBS->AVAILABLE_SPACE_PIVOT_RATIO, 1.0);
		size_t pivot = teamAvailableSpace.size() * SERVER_KNOBS->AVAILABLE_SPACE_PIVOT_RATIO;
		std::nth_element(
		    teamAvailableSpace.begin(), teamAvailableSpace.begin() + pivot, teamAvailableSpace.end(), std::greater{});
		teamPivots.pivotAvailableSpaceRatio =
		    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO,
		             std::min(SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO, teamAvailableSpace[pivot]));
	} else {
		teamPivots.pivotAvailableSpaceRatio = SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO;
	}

	if (teamPivots.pivotAvailableSpaceRatio < SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO) {
		TraceEvent(SevWarn, "DDTeamPivotAvailableSpaceTooSmall", distributorId)
		    .detail("PivotAvailableSpaceRatio", teamPivots.pivotAvailableSpaceRatio)
		    .detail("TargetAvailableSpaceRatio", SERVER_KNOBS->TARGET_AVAILABLE_SPACE_RATIO)
		    .detail("Primary", primary);
		printDetailedTeamsInfo.trigger();
	}
}

void DDTeamCollection::updateCpuPivots() {
	std::vector<double> teamAverageCPU;
	for (int i = 0; i < teams.size(); ++i) {
		if (teams[i]->isHealthy()) {
			teamAverageCPU.emplace_back(teams[i]->getAverageCPU());
			teamPivots.minTeamAvgCPU = std::min(teamPivots.minTeamAvgCPU, teamAverageCPU.back());
		}
	}

	if (!teamAverageCPU.empty()) {
		ASSERT_LT(SERVER_KNOBS->CPU_PIVOT_RATIO, 1.0);
		size_t pivot = teamAverageCPU.size() * SERVER_KNOBS->CPU_PIVOT_RATIO;
		std::nth_element(teamAverageCPU.begin(), teamAverageCPU.begin() + pivot, teamAverageCPU.end());
		teamPivots.pivotCPU = teamAverageCPU[pivot];
	} else {
		teamPivots.pivotCPU = SERVER_KNOBS->MAX_DEST_CPU_PERCENT;
	}

	if (teamPivots.pivotCPU > SERVER_KNOBS->MAX_DEST_CPU_PERCENT) {
		TraceEvent(SevWarnAlways, "DDTeamPivotCPUTooHigh", distributorId)
		    .detail("PivotCPU", teamPivots.pivotCPU)
		    .detail("MinTeamAvgCPU", teamPivots.minTeamAvgCPU)
		    .detail("Primary", primary);
	}
}

void DDTeamCollection::updateTeamEligibility() {
	int healthyCount = 0, lowDiskUtilTotal = 0, lowCpuTotal = 0, allMetricsLow = 0;
	for (auto& team : teams) {
		if (team->isHealthy()) {
			bool lowDiskUtil = team->hasHealthyAvailableSpace(teamPivots.pivotAvailableSpaceRatio);
			bool lowCPU = team->hasLowerCpu(teamPivots.pivotCPU);
			healthyCount++;

			DisabledTraceEvent(SevDebug, "EligiblityTeamDebug")
			    .detail("TeamId", team->getTeamID())
			    .detail("CPU", team->getAverageCPU())
			    .detail("AvailableSpace", team->getMinAvailableSpace())
			    .detail("AvailableSpaceRatio", team->getMinAvailableSpaceRatio());

			if (lowDiskUtil) {
				lowDiskUtilTotal++;
				team->increaseEligibilityCount(data_distribution::EligibilityCounter::LOW_DISK_UTIL);
			} else {
				team->resetEligibilityCount(data_distribution::EligibilityCounter::LOW_DISK_UTIL);
			}

			if (lowCPU) {
				lowCpuTotal++;
				team->increaseEligibilityCount(data_distribution::EligibilityCounter::LOW_CPU);
			} else {
				team->resetEligibilityCount(data_distribution::EligibilityCounter::LOW_CPU);
			}

			if (lowDiskUtil && lowCPU) {
				allMetricsLow++;
			}
		} else {
			team->resetEligibilityCount(data_distribution::EligibilityCounter::LOW_DISK_UTIL);
			team->resetEligibilityCount(data_distribution::EligibilityCounter::LOW_CPU);
		}
	}
	TraceEvent("TeamEligibilityCount", distributorId)
	    .detail("TotalTeam", teams.size())
	    .detail("HealthyTeam", healthyCount)
	    .detail("AllMetricsLowTeam", allMetricsLow)
	    .detail("LowDiskUtilTeam", lowDiskUtilTotal)
	    .detail("LowCPUTeam", lowCpuTotal)
	    .detail("PivotAvailableSpaceRatio", teamPivots.pivotAvailableSpaceRatio)
	    .detail("PivotCpuRatio", teamPivots.pivotCPU);
}

void DDTeamCollection::updateTeamPivotValues() {
	if (now() - teamPivots.lastPivotValuesUpdate >= SERVER_KNOBS->DD_TEAM_PIVOT_UPDATE_DELAY) {
		updateAvailableSpacePivots();
		updateCpuPivots();
		updateTeamEligibility();
		teamPivots.lastPivotValuesUpdate = now();
	}
}

int32_t DDTeamCollection::getTargetTSSInDC() const {
	int32_t targetTSSInDC = configuration.desiredTSSCount;
	if (configuration.usableRegions > 1) {
		targetTSSInDC /= configuration.usableRegions;
		if (primary) {
			// put extras in primary DC if it's uneven
			targetTSSInDC += (configuration.desiredTSSCount % configuration.usableRegions);
		}
	}
	return targetTSSInDC;
}

bool DDTeamCollection::reachTSSPairTarget() const {
	return tss_info_by_pair.size() >= getTargetTSSInDC();
}

Reference<TCMachineTeamInfo> DDTeamCollection::findMachineTeam(
    std::vector<Standalone<StringRef>> const& machineIDs) const {
	if (machineIDs.empty()) {
		return Reference<TCMachineTeamInfo>();
	}

	Standalone<StringRef> machineID = machineIDs[0];
	for (auto& machineTeam : get(machine_info, machineID)->machineTeams) {
		if (machineTeam->getMachineIDs() == machineIDs) {
			return machineTeam;
		}
	}

	return Reference<TCMachineTeamInfo>();
}

void DDTeamCollection::traceServerInfo() const {
	int i = 0;

	TraceEvent("ServerInfo", distributorId).detail("Size", server_info.size());
	for (auto& [serverID, server] : server_info) {
		TraceEvent("ServerInfo", distributorId)
		    .detail("ServerInfoIndex", i++)
		    .detail("ServerID", serverID.toString())
		    .detail("ServerTeamOwned", server->getTeams().size())
		    .detail("MachineID", server->machine->machineID.contents().toString())
		    .detail("StoreType", server->getStoreType().toString())
		    .detail("InDesiredDC", server->isInDesiredDC());
	}
	i = 0;
	for (auto& server : server_info) {
		const UID& serverID = server.first;
		const ServerStatus& status = server_status.get(serverID);
		TraceEvent("ServerStatus", distributorId)
		    .detail("ServerInfoIndex", i++)
		    .detail("ServerID", serverID)
		    .detail("Healthy", !status.isUnhealthy())
		    .detail("StatusString", status.toString())
		    .detail("MachineIsValid", get(server_info, serverID)->machine.isValid())
		    .detail("MachineTeamSize",
		            get(server_info, serverID)->machine.isValid()
		                ? get(server_info, serverID)->machine->machineTeams.size()
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

bool DDTeamCollection::isMachineTeamHealthy(TCMachineTeamInfo const& machineTeam) const {
	// A healthy machine team should have the desired number of machines
	if (machineTeam.size() != configuration.storageTeamSize)
		return false;

	for (auto const& machine : machineTeam.getMachines()) {
		if (!isMachineHealthy(machine)) {
			return false;
		}
	}
	return true;
}

bool DDTeamCollection::isMachineHealthy(Reference<TCMachineInfo> const& machine) const {
	if (!machine.isValid() || machine_info.find(machine->machineID) == machine_info.end() ||
	    machine->serversOnMachine.empty()) {
		return false;
	}

	// Healthy machine has at least one healthy server
	for (auto& server : machine->serversOnMachine) {
		if (!server_status.get(server->getId()).isUnhealthy()) {
			return true;
		}
	}

	return false;
}

bool DDTeamCollection::teamContainsFailedServer(Reference<TCTeamInfo> team) const {
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

Optional<Reference<IDataDistributionTeam>> DDTeamCollection::findTeamFromServers(const std::vector<UID>& servers,
                                                                                 bool wantHealthy) {
	const std::set<UID> completeSources(servers.begin(), servers.end());

	for (const auto& server : servers) {
		if (!server_info.contains(server)) {
			continue;
		}
		auto const& teamList = server_info[server]->getTeams();
		for (const auto& team : teamList) {
			bool found = true;
			for (const UID& s : team->getServerIDs()) {
				if (!completeSources.contains(s)) {
					found = false;
					break;
				}
			}
			if (found && (!wantHealthy || team->isHealthy())) {
				return team;
			}
		}
	}
	return Optional<Reference<IDataDistributionTeam>>();
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
                                    DDEnabledState const& ddEnabledState) {
	return DDTeamCollectionImpl::init(this, initTeams, &ddEnabledState);
}

Future<Void> DDTeamCollection::buildTeams() {
	return DDTeamCollectionImpl::buildTeams(this);
}

Future<Void> DDTeamCollection::teamTracker(Reference<TCTeamInfo> team,
                                           IsBadTeam isBadTeam,
                                           IsRedundantTeam isRedundantTeam) {
	return DDTeamCollectionImpl::teamTracker(this, team, isBadTeam, isRedundantTeam);
}

Future<Void> DDTeamCollection::storageServerTracker(TCServerInfo* server,
                                                    Promise<Void> errorOut,
                                                    Version addedVersion,
                                                    DDEnabledState const& ddEnabledState,
                                                    bool isTss) {
	return DDTeamCollectionImpl::storageServerTracker(this, server, errorOut, addedVersion, &ddEnabledState, isTss);
}

Future<Void> DDTeamCollection::removeWrongStoreType() {
	return DDTeamCollectionImpl::removeWrongStoreType(this);
}

Future<Void> DDTeamCollection::waitUntilHealthy(double extraDelay, WaitWiggle waitWiggle) const {
	return DDTeamCollectionImpl::waitUntilHealthy(this, extraDelay, waitWiggle);
}

bool DDTeamCollection::isCorrectDC(TCServerInfo const& server) const {
	return (includedDCs.empty() ||
	        std::find(includedDCs.begin(), includedDCs.end(), server.getLastKnownInterface().locality.dcId()) !=
	            includedDCs.end());
}

Future<Void> DDTeamCollection::removeBadTeams() {
	return DDTeamCollectionImpl::removeBadTeams(this);
}

double DDTeamCollection::loadBytesBalanceRatio(int64_t smallLoadThreshold) const {
	double minLoadBytes = std::numeric_limits<double>::max();
	double totalLoadBytes = 0;
	int count = 0;
	for (auto& [id, s] : server_info) {
		// If a healthy SS don't have storage metrics, skip this round
		if (server_status.get(s->getId()).isUnhealthy() || !s->metricsPresent()) {
			TraceEvent(SevDebug, "LoadBytesBalanceRatioNoMetrics").detail("Server", id);
			return 0;
		}

		double load = s->loadBytes();
		totalLoadBytes += load;
		++count;
		minLoadBytes = std::min(minLoadBytes, load);
	}

	TraceEvent(SevDebug, "LoadBytesBalanceRatioMetrics")
	    .detail("TotalLoad", totalLoadBytes)
	    .detail("MinLoadBytes", minLoadBytes)
	    .detail("SmallLoadThreshold", smallLoadThreshold)
	    .detail("Count", count);

	// avoid division-by-zero
	double avgLoad = totalLoadBytes / count;
	if (totalLoadBytes == 0 || avgLoad < smallLoadThreshold) {
		CODE_PROBE(true, "The cluster load is small enough to ignore load bytes balance.");
		return 1;
	}

	return minLoadBytes / avgLoad;
}

int DDTeamCollection::numSSToBeLoadBytesBalanced(int64_t smallLoadThreshold) const {
	double totalLoadBytes = 0;
	int count = 0;
	for (auto& [id, s] : server_info) {
		// If a healthy SS don't have storage metrics, skip this round
		if (server_status.get(s->getId()).isUnhealthy() || !s->metricsPresent()) {
			TraceEvent(SevDebug, "NumSSToBeLoadBytesBalancedNoMetrics").detail("Server", id);
			return INT_MAX; // return all are imbalanced
		}

		totalLoadBytes += s->loadBytes();
		++count;
	}

	if (!count)
		return INT_MAX;

	double avgLoad = totalLoadBytes / count;
	if (totalLoadBytes == 0 || avgLoad < smallLoadThreshold) {
		return 0;
	}

	int numSSToBeLoadBytesBalanced = 0;
	double balanceRatio;
	for (auto& [id, s] : server_info) {
		// If a healthy SS don't have storage metrics, skip this round
		if (server_status.get(s->getId()).isUnhealthy() || !s->metricsPresent()) {
			TraceEvent(SevDebug, "NumSSToBeLoadBytesBalancedNoMetrics").detail("Server", id);
			return INT_MAX;
		}

		balanceRatio = s->loadBytes() / avgLoad;
		if (balanceRatio < SERVER_KNOBS->PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO) {
			numSSToBeLoadBytesBalanced++;
		}
	}

	TraceEvent(SevDebug, "NumSSToBeLoadBytesBalancedMetrics")
	    .detail("NumSSToBeLoadBytesBalanced", numSSToBeLoadBytesBalanced)
	    .detail("TotalLoad", totalLoadBytes)
	    .detail("AvgLoad", avgLoad)
	    .detail("SmallLoadThreshold", smallLoadThreshold)
	    .detail("Count", count);

	return numSSToBeLoadBytesBalanced;
}

Future<Void> DDTeamCollection::storageServerFailureTracker(TCServerInfo* server,
                                                           ServerStatus* status,
                                                           Version addedVersion) {
	return DDTeamCollectionImpl::storageServerFailureTracker(this, server, status, addedVersion);
}

Future<Void> DDTeamCollection::waitForAllDataRemoved(UID serverID, Version addedVersion) const {
	return db->waitForAllDataRemoved(serverID, addedVersion, shardsAffectedByTeamFailure);
}

Future<Void> DDTeamCollection::machineTeamRemover() {
	return DDTeamCollectionImpl::machineTeamRemover(this);
}

Future<Void> DDTeamCollection::serverTeamRemover() {
	return DDTeamCollectionImpl::serverTeamRemover(this);
}

Future<Void> DDTeamCollection::fixUnderReplicationLoop() {
	return DDTeamCollectionImpl::fixUnderReplicationLoop(this);
}

void DDTeamCollection::fixUnderReplication() {
	int maxTeamSize = maxLargeTeamSize();
	int checkCount = 0;
	for (auto& it : underReplication.ranges()) {
		if (!it.value()) {
			// The key range is not under-replicated
			continue;
		}
		for (auto& r : shardsAffectedByTeamFailure->intersectingRanges(it.range())) {
			if (++checkCount > SERVER_KNOBS->DD_MAXIMUM_LARGE_TEAM_CLEANUP) {
				return;
			}

			auto& teams = r.value();
			if (!teams.second.empty()) {
				// The key range is currently being moved
				continue;
			}

			int customReplicas = configuration.storageTeamSize;
			for (auto it : userRangeConfig->intersectingRanges(r.range().begin, r.range().end)) {
				customReplicas = std::max(customReplicas, it->value().replicationFactor.orDefault(0));
			}

			int currentSize = 0;
			for (auto& c : teams.first) {
				if (c.primary == primary) {
					currentSize = c.servers.size();
				}
			}

			if (currentSize < customReplicas) {
				// check if a larger team exists
				if (maxTeamSize > currentSize) {
					TraceEvent("FixUnderReplication", distributorId)
					    .suppressFor(1.0)
					    .detail("MaxTeamSize", maxTeamSize)
					    .detail("Current", currentSize)
					    .detail("Desired", customReplicas);
					RelocateShard rs(r.range(),
					                 SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY,
					                 RelocateReason::OTHER,
					                 deterministicRandom()->randomUniqueID());
					output.send(rs);
					underReplication.insert(r.range(), false);
					return;
				}
			} else {
				underReplication.insert(r.range(), false);
				return;
			}
		}
	}
}

Future<Void> DDTeamCollection::trackExcludedServers() {
	ASSERT(!db->isMocked());
	return DDTeamCollectionImpl::trackExcludedServers(this);
}

Future<Void> DDTeamCollection::updateNextWigglingStorageID() {
	return DDTeamCollectionImpl::updateNextWigglingStorageID(this);
}

Future<Void> DDTeamCollection::perpetualStorageWiggleIterator(AsyncVar<bool>& stopSignal,
                                                              FutureStream<Void> finishStorageWiggleSignal) {
	return DDTeamCollectionImpl::perpetualStorageWiggleIterator(this, &stopSignal, finishStorageWiggleSignal);
}

Future<Void> DDTeamCollection::clusterHealthCheckForPerpetualWiggle(int& extraTeamCount) {
	return DDTeamCollectionImpl::clusterHealthCheckForPerpetualWiggle(this, &extraTeamCount);
}

Future<Void> DDTeamCollection::perpetualStorageWiggler(AsyncVar<bool>& stopSignal,
                                                       PromiseStream<Void> finishStorageWiggleSignal) {
	return DDTeamCollectionImpl::perpetualStorageWiggler(this, &stopSignal, finishStorageWiggleSignal);
}

Future<Void> DDTeamCollection::monitorPerpetualStorageWiggle() {
	ASSERT(!db->isMocked());
	return DDTeamCollectionImpl::monitorPerpetualStorageWiggle(this);
}

Future<Void> DDTeamCollection::waitServerListChange(FutureStream<Void> serverRemoved,
                                                    DDEnabledState const& ddEnabledState) {
	return DDTeamCollectionImpl::waitServerListChange(this, serverRemoved, &ddEnabledState);
}

Future<Void> DDTeamCollection::waitHealthyZoneChange() {
	ASSERT(!db->isMocked());
	return DDTeamCollectionImpl::waitHealthyZoneChange(this);
}

Future<Void> DDTeamCollection::monitorStorageServerRecruitment() {
	return DDTeamCollectionImpl::monitorStorageServerRecruitment(this);
}

Future<Void> DDTeamCollection::initializeStorage(RecruitStorageReply candidateWorker,
                                                 DDEnabledState const& ddEnabledState,
                                                 bool recruitTss,
                                                 Reference<TSSPairState> tssState) {
	return DDTeamCollectionImpl::initializeStorage(this, candidateWorker, &ddEnabledState, recruitTss, tssState);
}

Future<Void> DDTeamCollection::storageRecruiter(
    Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
    DDEnabledState const& ddEnabledState) {
	return DDTeamCollectionImpl::storageRecruiter(this, recruitStorage, &ddEnabledState);
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

Future<UID> DDTeamCollection::getNextWigglingServerID() {
	std::vector<std::pair<Optional<Value>, Optional<Value>>> localityKeyValues;

	// NOTE: because normal \xff/conf change through `changeConfig` now will cause DD throw `movekeys_conflict()`
	// then recruit a new DD, we only need to read current configuration once
	if (configuration.perpetualStorageWiggleLocality != "0") {
		localityKeyValues = ParsePerpetualStorageWiggleLocality(configuration.perpetualStorageWiggleLocality);
	}
	return DDTeamCollectionImpl::getNextWigglingServerID(storageWiggler, localityKeyValues, this);
}

Future<Void> DDTeamCollection::readStorageWiggleMap() {
	return DDTeamCollectionImpl::readStorageWiggleMap(this);
}

Future<Void> DDTeamCollection::updateStorageMetadata(TCServerInfo* server) {
	if (db->isMocked())
		return Never();
	return DDTeamCollectionImpl::updateStorageMetadata(this, server);
}

void DDTeamCollection::resetLocalitySet() {
	storageServerSet = Reference<LocalitySet>(new LocalityMap<UID>());
	LocalityMap<UID>* storageServerMap = (LocalityMap<UID>*)storageServerSet.getPtr();

	for (auto& it : server_info) {
		it.second->localityEntry =
		    storageServerMap->add(it.second->getLastKnownInterface().locality, &it.second->getId());
	}
}

bool DDTeamCollection::satisfiesPolicy(const std::vector<Reference<TCServerInfo>>& team, int amount) const {
	std::vector<LocalityEntry> forcedEntries, resultEntries;
	if (amount == -1) {
		amount = team.size();
	}

	forcedEntries.reserve(amount);
	for (int i = 0; i < amount; i++) {
		forcedEntries.push_back(team[i]->localityEntry);
	}

	bool result = storageServerSet->selectReplicas(configuration.storagePolicy, forcedEntries, resultEntries);
	return result && resultEntries.size() == 0;
}

DDTeamCollection::DDTeamCollection(DDTeamCollectionInitParams const& params)
  : db(params.db), doBuildTeams(true), lastBuildTeamsFailed(false), teamBuilder(Void()), lock(params.lock),
    output(params.output), unhealthyServers(0), storageWiggler(makeReference<StorageWiggler>(this)),
    processingWiggle(params.processingWiggle), shardsAffectedByTeamFailure(params.shardsAffectedByTeamFailure),
    initialFailureReactionDelay(
        delayed(params.readyToStart, SERVER_KNOBS->INITIAL_FAILURE_REACTION_DELAY, TaskPriority::DataDistribution)),
    initializationDoneActor(logOnCompletion(params.readyToStart && initialFailureReactionDelay)), recruitingStream(0),
    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY), healthyTeamCount(0),
    zeroHealthyTeams(params.zeroHealthyTeams), optimalTeamCount(0), zeroOptimalTeams(true), isTssRecruiting(false),
    includedDCs(params.includedDCs), otherTrackedDCs(params.otherTrackedDCs),
    processingUnhealthy(params.processingUnhealthy), getAverageShardBytes(params.getAverageShardBytes),
    triggerStorageQueueRebalance(params.triggerStorageQueueRebalance), readyToStart(params.readyToStart),
    checkTeamDelay(delay(SERVER_KNOBS->CHECK_TEAM_DELAY, TaskPriority::DataDistribution)), badTeamRemover(Void()),
    checkInvalidLocalities(Void()), wrongStoreTypeRemover(Void()), clearHealthyZoneFuture(true),
    lowestUtilizationTeam(0), highestUtilizationTeam(0), getShardMetrics(params.getShardMetrics),
    getUnhealthyRelocationCount(params.getUnhealthyRelocationCount), removeFailedServer(params.removeFailedServer),
    ddTrackerStartingEventHolder(makeReference<EventCacheHolder>("DDTrackerStarting")),
    teamCollectionInfoEventHolder(makeReference<EventCacheHolder>("TeamCollectionInfo")),
    storageServerRecruitmentEventHolder(
        makeReference<EventCacheHolder>("StorageServerRecruitment_" + params.distributorId.toString())),
    primary(params.primary), distributorId(params.distributorId), underReplication(false),
    configuration(params.configuration), storageServerSet(new LocalityMap<UID>()) {

	if (!primary || configuration.usableRegions == 1) {
		TraceEvent("DDTrackerStarting", distributorId)
		    .detail("State", "Inactive")
		    .trackLatest(ddTrackerStartingEventHolder->trackingKey);
	}
	teamPivots = { .lastPivotValuesUpdate = 0,
		           .pivotAvailableSpaceRatio = SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO,
		           .pivotCPU = SERVER_KNOBS->MAX_DEST_CPU_PERCENT,
		           .minTeamAvgCPU = std::numeric_limits<double>::max() };
}

DDTeamCollection::~DDTeamCollection() {
	TraceEvent("DDTeamCollectionDestructed", distributorId).detail("Primary", primary);
	// Signal that the object is being destroyed.
	shutdown.send(Void());

	// Cancel the teamBuilder to avoid creating new teams after teams are cancelled.
	teamBuilder.cancel();
	// TraceEvent("DDTeamCollectionDestructed", distributorId)
	//    .detail("Primary", primary)
	//    .detail("TeamBuilderDestroyed", server_info.size());

	// Other teamCollections also hold pointer to this teamCollection;
	// TeamTracker may access the destructed DDTeamCollection if we do not reset the pointer
	for (int i = 0; i < teamCollections.size(); i++) {
		if (teamCollections[i] != nullptr && teamCollections[i] != this) {
			for (int j = 0; j < teamCollections[i]->teamCollections.size(); ++j) {
				if (teamCollections[i]->teamCollections[j] == this) {
					teamCollections[i]->teamCollections[j] = nullptr;
				}
			}
		}
	}
	// Team tracker has pointers to DDTeamCollections both in primary and remote.
	// The following kills a reference cycle between the teamTracker actor and the TCTeamInfo that both holds and is
	// held by the actor It also ensures that the trackers are done fiddling with healthyTeamCount before we free
	// this
	for (auto& team : teams) {
		team->tracker.cancel();
	}
	// The commented TraceEvent log is useful in detecting what is running during the destruction
	// TraceEvent("DDTeamCollectionDestructed", distributorId)
	//     .detail("Primary", primary)
	//     .detail("TeamTrackerDestroyed", teams.size());
	for (auto& badTeam : badTeams) {
		badTeam->tracker.cancel();
	}

	for (auto& largeTeam : largeTeams) {
		largeTeam->tracker.cancel();
	}

	// TraceEvent("DDTeamCollectionDestructed", distributorId)
	//     .detail("Primary", primary)
	//     .detail("BadTeamTrackerDestroyed", badTeams.size());
	// The following makes sure that, even if a reference to a team is held in the DD Queue, the tracker will be
	// stopped
	//  before the server_status map to which it has a pointer, is destroyed.
	for (auto& [_, info] : server_and_tss_info) {
		info->cancel();
	}

	storageWiggler->teamCollection = nullptr;
	// TraceEvent("DDTeamCollectionDestructed", distributorId)
	//    .detail("Primary", primary)
	//    .detail("ServerTrackerDestroyed", server_info.size());
}

void DDTeamCollection::addLaggingStorageServer(Key zoneId) {
	lagging_zones[zoneId]++;
	if (lagging_zones.size() > std::max(1, configuration.storageTeamSize - 1) && !disableFailingLaggingServers.get())
		disableFailingLaggingServers.set(true);
}

void DDTeamCollection::removeLaggingStorageServer(Key zoneId) {
	auto iter = lagging_zones.find(zoneId);
	ASSERT(iter != lagging_zones.end());
	iter->second--;
	ASSERT_GE(iter->second, 0);
	if (iter->second == 0)
		lagging_zones.erase(iter);
	if (lagging_zones.size() <= std::max(1, configuration.storageTeamSize - 1) && disableFailingLaggingServers.get())
		disableFailingLaggingServers.set(false);
}

bool DDTeamCollection::isWigglePausedServer(const UID& server) const {
	return pauseWiggle && pauseWiggle->get() && wigglingId == server;
}

std::vector<UID> DDTeamCollection::getRandomHealthyTeam(const UID& excludeServer) {
	std::vector<int> candidates, backup;
	for (int i = 0; i < teams.size(); ++i) {
		if (teams[i]->isHealthy() && !teams[i]->hasServer(excludeServer)) {
			candidates.push_back(i);
		} else if (teams[i]->size() - (teams[i]->hasServer(excludeServer) ? 1 : 0) > 0) {
			// If a team has at least one other server besides excludeServer, select it
			// as a backup candidate.
			backup.push_back(i);
		}
	}

	// Prefer a healthy team not containing excludeServer.
	if (candidates.size() > 0) {
		return teams[candidates[deterministicRandom()->randomInt(0, candidates.size())]]->getServerIDs();
	} else if (backup.size() > 0) {
		// The backup choice is a team with at least one server besides excludeServer, in this
		// case, the team will be possibly relocated to a healthy destination later by DD.
		std::vector<UID> servers = teams[backup[deterministicRandom()->randomInt(0, backup.size())]]->getServerIDs();
		std::vector<UID> res;
		for (const UID& id : servers) {
			if (id != excludeServer) {
				res.push_back(id);
			}
		}
		TraceEvent("FoundNonoptimalTeamForDroppedShard", excludeServer).detail("Team", describe(res));
		return res;
	}

	return std::vector<UID>();
}

int64_t DDTeamCollection::getDebugTotalDataInFlight() const {
	int64_t total = 0;
	for (const auto& [_, server] : server_info) {
		total += server->getDataInFlightToServer();
	}
	return total;
}

bool DDTeamCollection::isValidLocality(Reference<IReplicationPolicy> storagePolicy,
                                       const LocalityData& locality) const {
	// Future: Once we add simulation test that misconfigure a cluster, such as not setting some locality entries,
	// DD_VALIDATE_LOCALITY should always be true. Otherwise, simulation test may fail.
	if (!SERVER_KNOBS->DD_VALIDATE_LOCALITY) {
		// Disable the checking if locality is valid
		return true;
	}

	std::set<std::string> replicationPolicyKeys = storagePolicy->attributeKeys();
	for (auto& policy : replicationPolicyKeys) {
		if (!locality.isPresent(policy)) {
			return false;
		}
	}

	return true;
}

void DDTeamCollection::evaluateTeamQuality() const {
	int teamCount = teams.size(), serverCount = allServers.size();
	double teamsPerServer = (double)teamCount * configuration.storageTeamSize / serverCount;
	const int targetTeamNumPerServer =
	    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;
	ASSERT_EQ(serverCount, server_info.size());

	int minTeams = std::numeric_limits<int>::max();
	int maxTeams = std::numeric_limits<int>::min();
	double varTeams = 0;

	std::map<Optional<Standalone<StringRef>>, int> machineTeams;
	for (const auto& [id, info] : server_info) {
		if (!server_status.get(id).isUnhealthy()) {
			int stc = info->getTeams().size();
			minTeams = std::min(minTeams, stc);
			maxTeams = std::max(maxTeams, stc);
			varTeams += (stc - teamsPerServer) * (stc - teamsPerServer);
			// Use zoneId as server's machine id
			machineTeams[info->getLastKnownInterface().locality.zoneId()] += stc;
			// Check invariant: if latest buildTeam succeeds, then each server must have at least
			// targetTeamNumPerServer serverTeams
			// lastBuildTeamsFailed is set only when (1) machine count is less than configured team size;
			// (2) Not find any server team candidates when creating server team; (3) failed to add machine team
			if (SERVER_KNOBS->DD_VALIDATE_SERVER_TEAM_COUNT_AFTER_BUILD_TEAM && !lastBuildTeamsFailed &&
			    stc < targetTeamNumPerServer) {
				TraceEvent(SevError, "NewAddServerNotMatchTargetSTCount", distributorId)
				    .detail("CurrentServerTeams", stc)
				    .detail("TargetServerTeams", targetTeamNumPerServer);
			}
		}
	}
	varTeams /= teamsPerServer * teamsPerServer;

	int minMachineTeams = std::numeric_limits<int>::max();
	int maxMachineTeams = std::numeric_limits<int>::min();
	for (auto m = machineTeams.begin(); m != machineTeams.end(); ++m) {
		minMachineTeams = std::min(minMachineTeams, m->second);
		maxMachineTeams = std::max(maxMachineTeams, m->second);
	}

	TraceEvent(minTeams > 0 ? SevInfo : SevWarn, "DataDistributionTeamQuality", distributorId)
	    .detail("Servers", serverCount)
	    .detail("Teams", teamCount)
	    .detail("TeamsPerServer", teamsPerServer)
	    .detail("Variance", varTeams / serverCount)
	    .detail("ServerMinTeams", minTeams)
	    .detail("ServerMaxTeams", maxTeams)
	    .detail("MachineMinTeams", minMachineTeams)
	    .detail("MachineMaxTeams", maxMachineTeams);
}

int DDTeamCollection::overlappingMembers(const std::vector<UID>& team) const {
	if (team.empty()) {
		return 0;
	}

	int maxMatchingServers = 0;
	const UID& serverID = team[0];
	const auto it = server_info.find(serverID);
	ASSERT(it != server_info.end());
	const auto& usedTeams = it->second->getTeams();
	for (const auto& usedTeam : usedTeams) {
		auto used = usedTeam->getServerIDs();
		int teamIdx = 0;
		int usedIdx = 0;
		int matchingServers = 0;
		while (teamIdx < team.size() && usedIdx < used.size()) {
			if (team[teamIdx] == used[usedIdx]) {
				matchingServers++;
				teamIdx++;
				usedIdx++;
			} else if (team[teamIdx] < used[usedIdx]) {
				teamIdx++;
			} else {
				usedIdx++;
			}
		}
		ASSERT_GT(matchingServers, 0);
		maxMatchingServers = std::max(maxMatchingServers, matchingServers);
		if (maxMatchingServers == team.size()) {
			return maxMatchingServers;
		}
	}

	return maxMatchingServers;
}

int DDTeamCollection::overlappingMachineMembers(std::vector<Standalone<StringRef>> const& team) const {
	if (team.empty()) {
		return 0;
	}

	int maxMatchingServers = 0;
	auto it = machine_info.find(team[0]);
	ASSERT(it != machine_info.end());
	auto const& machineTeams = it->second->machineTeams;
	for (auto const& usedTeam : machineTeams) {
		auto used = usedTeam->getMachineIDs();
		int teamIdx = 0;
		int usedIdx = 0;
		int matchingServers = 0;
		while (teamIdx < team.size() && usedIdx < used.size()) {
			if (team[teamIdx] == used[usedIdx]) {
				matchingServers++;
				teamIdx++;
				usedIdx++;
			} else if (team[teamIdx] < used[usedIdx]) {
				teamIdx++;
			} else {
				usedIdx++;
			}
		}
		ASSERT_GT(matchingServers, 0);
		maxMatchingServers = std::max(maxMatchingServers, matchingServers);
		if (maxMatchingServers == team.size()) {
			return maxMatchingServers;
		}
	}

	return maxMatchingServers;
}

void DDTeamCollection::cleanupLargeTeams() {
	for (int t = 0; t < std::min<int>(largeTeams.size(), SERVER_KNOBS->DD_MAXIMUM_LARGE_TEAM_CLEANUP); t++) {
		if (!shardsAffectedByTeamFailure->hasShards(
		        ShardsAffectedByTeamFailure::Team(largeTeams[t]->getServerIDs(), primary))) {
			largeTeams[t]->tracker.cancel();
			largeTeams[t--] = largeTeams.back();
			largeTeams.pop_back();
		}
	}
}

int DDTeamCollection::maxLargeTeamSize() const {
	std::vector<Reference<TCServerInfo>> healthy;
	for (auto& [serverID, server] : server_info) {
		if (!server_status.get(serverID).isUnhealthy()) {
			healthy.push_back(server);
		}
	}

	if (!satisfiesPolicy(healthy)) {
		return -1;
	}
	return healthy.size();
}

struct ServerPriority {
	int healthyShards = 0;
	int unhealthyShards = 0;
	int64_t loadBytes = std::numeric_limits<int64_t>::max();
	UID id;
	Reference<TCServerInfo> info;

	ServerPriority() {}
	ServerPriority(int healthyShards, int unhealthyShards, int64_t loadBytes, UID id, Reference<TCServerInfo> info)
	  : healthyShards(healthyShards), unhealthyShards(unhealthyShards), loadBytes(loadBytes), id(id), info(info) {}

	bool operator<(ServerPriority const& r) const {
		if (healthyShards != r.healthyShards) {
			return healthyShards < r.healthyShards;
		} else if (unhealthyShards != r.unhealthyShards) {
			return unhealthyShards < r.unhealthyShards;
		} else if (loadBytes != r.loadBytes) {
			return loadBytes < r.loadBytes;
		} else {
			return id < r.id;
		}
	}
};

Reference<TCTeamInfo> DDTeamCollection::buildLargeTeam(int teamSize) {
	cleanupLargeTeams();

	std::map<UID, ServerPriority> server_priority;
	for (auto& [serverID, server] : server_info) {
		if (!server_status.get(serverID).isUnhealthy()) {
			server_priority[serverID] =
			    ServerPriority(0,
			                   0,
			                   server->metricsPresent() ? server->loadBytes() : std::numeric_limits<int64_t>::max(),
			                   serverID,
			                   server);
		}
	}

	int totalShardCount = 0;
	for (auto& team : largeTeams) {
		const auto servers = team->getServerIDs();
		const int shardCount =
		    shardsAffectedByTeamFailure->getNumberOfShards(ShardsAffectedByTeamFailure::Team(servers, primary));
		totalShardCount += shardCount;
		if (team->isHealthy()) {
			for (auto& it : servers) {
				auto f = server_priority.find(it);
				if (f != server_priority.end()) {
					f->second.healthyShards += shardCount;
				}
			}
		} else {
			for (auto& it : servers) {
				auto f = server_priority.find(it);
				if (f != server_priority.end()) {
					f->second.unhealthyShards += shardCount;
				}
			}
		}
	}

	if (totalShardCount >= SERVER_KNOBS->DD_MAX_SHARDS_ON_LARGE_TEAMS) {
		TraceEvent(SevWarnAlways, "TooManyShardsOnLargeTeams", distributorId)
		    .suppressFor(1.0)
		    .detail("TeamCount", largeTeams.size())
		    .detail("ShardCount", totalShardCount);
		return Reference<TCTeamInfo>();
	}

	// The set of all healthy servers sorted so the most desirable option is first in the list
	std::vector<ServerPriority> sortedServers;

	sortedServers.reserve(server_priority.size());
	for (auto& it : server_priority) {
		sortedServers.emplace_back(it.second);
	}
	std::sort(sortedServers.begin(), sortedServers.end());

	std::set<UID> serverIds;
	std::vector<Reference<TCServerInfo>> candidateTeam;
	for (int i = 0; i < sortedServers.size(); i++) {
		if (candidateTeam.size() >= teamSize && satisfiesPolicy(candidateTeam)) {
			break;
		}
		candidateTeam.push_back(sortedServers[i].info);
	}
	if (candidateTeam.size() <= configuration.storageTeamSize || !satisfiesPolicy(candidateTeam)) {
		TraceEvent(SevWarnAlways, "TooFewServersForLargeTeam", distributorId)
		    .suppressFor(1.0)
		    .detail("TeamSize", candidateTeam.size())
		    .detail("Desired", teamSize)
		    .detail("SatisfiesPolicy", satisfiesPolicy(candidateTeam));
		return Reference<TCTeamInfo>();
	} else if (candidateTeam.size() > teamSize) {
		Reference<LocalitySet> tempSet = Reference<LocalitySet>(new LocalityMap<UID>());
		LocalityMap<UID>* tempMap = (LocalityMap<UID>*)tempSet.getPtr();
		tempSet->clear();
		for (auto& it : candidateTeam) {
			tempMap->add(it->getLastKnownInterface().locality, &it->getId());
		}

		std::vector<LocalityEntry> resultEntries, forcedEntries;
		bool result = tempSet->selectReplicas(configuration.storagePolicy, forcedEntries, resultEntries);
		ASSERT(result && resultEntries.size() == configuration.storageTeamSize);

		for (auto& it : resultEntries) {
			serverIds.insert(*tempMap->getObject(it));
		}
		for (int i = 0; i < sortedServers.size(); i++) {
			if (serverIds.size() >= teamSize) {
				break;
			}
			serverIds.insert(sortedServers[i].id);
		}
	} else {
		for (auto& it : candidateTeam) {
			serverIds.insert(it->getId());
		}
	}

	const std::vector<UID> serverIDVector(serverIds.begin(), serverIds.end());
	for (int t = 0; t < largeTeams.size(); t++) {
		if (largeTeams[t]->getServerIDs() == serverIDVector) {
			return largeTeams[t];
		}
	}

	candidateTeam.clear();
	for (auto& it : serverIds) {
		candidateTeam.push_back(server_info[it]);
	}
	Optional<Reference<TCTenantInfo>> no_tenant = {};
	auto teamInfo = makeReference<TCTeamInfo>(candidateTeam, no_tenant);
	teamInfo->tracker = teamTracker(teamInfo, IsBadTeam::False, IsRedundantTeam::False);
	largeTeams.push_back(teamInfo);
	return teamInfo;
}

void DDTeamCollection::addTeam(const std::vector<Reference<TCServerInfo>>& newTeamServers,
                               IsInitialTeam isInitialTeam,
                               IsRedundantTeam redundantTeam) {
	Optional<Reference<TCTenantInfo>> no_tenant = {};
	auto teamInfo = makeReference<TCTeamInfo>(newTeamServers, no_tenant);

	// Move satisfiesPolicy to the end for performance benefit
	auto badTeam = IsBadTeam{ redundantTeam || !satisfiesPolicy(teamInfo->getServers()) ||
		                      (!ddLargeTeamEnabled() && teamInfo->size() != configuration.storageTeamSize) };

	teamInfo->tracker = teamTracker(teamInfo, badTeam, redundantTeam);
	// ASSERT( teamInfo->serverIDs.size() > 0 ); //team can be empty at DB initialization
	if (badTeam) {
		badTeams.push_back(teamInfo);
		return;
	}

	if (teamInfo->size() > configuration.storageTeamSize) {
		largeTeams.push_back(teamInfo);
		return;
	}

	// For a good team, we add it to teams and create machine team for it when necessary
	teams.push_back(teamInfo);
	for (auto& server : newTeamServers) {
		server->addTeam(teamInfo);
	}

	// Find or create machine team for the server team
	// Add the reference of machineTeam (with machineIDs) into process team
	std::vector<Standalone<StringRef>> machineIDs;
	for (auto& server : newTeamServers) {
		ASSERT_WE_THINK(server->machine.isValid());
		machineIDs.push_back(server->machine->machineID);
	}
	sort(machineIDs.begin(), machineIDs.end());
	Reference<TCMachineTeamInfo> machineTeamInfo = findMachineTeam(machineIDs);

	// A team is not initial team if it is added by addTeamsBestOf() which always create a team with correct size
	// A non-initial team must have its machine team created and its size must be correct
	ASSERT(isInitialTeam || machineTeamInfo.isValid());

	// Create a machine team if it does not exist
	// Note an initial team may be added at init() even though the team size is not storageTeamSize
	if (!machineTeamInfo.isValid() && !machineIDs.empty()) {
		machineTeamInfo = addMachineTeam(machineIDs.begin(), machineIDs.end());
	}

	if (!machineTeamInfo.isValid()) {
		TraceEvent(SevWarn, "AddTeamWarning")
		    .detail("NotFoundMachineTeam", "OKIfTeamIsEmpty")
		    .detail("TeamInfo", teamInfo->getDesc());
	}

	teamInfo->machineTeam = machineTeamInfo;
	machineTeamInfo->addServerTeam(teamInfo);
	if (g_network->isSimulated()) {
		// Update server team information for consistency check in simulation
		traceTeamCollectionInfo();
	}
}

Reference<TCMachineTeamInfo> DDTeamCollection::addMachineTeam(std::vector<Reference<TCMachineInfo>> machines) {
	auto machineTeamInfo = makeReference<TCMachineTeamInfo>(machines);
	machineTeams.push_back(machineTeamInfo);

	// Assign machine teams to machine
	for (auto machine : machines) {
		// A machine's machineTeams vector should not hold duplicate machineTeam members
		ASSERT_WE_THINK(std::count(machine->machineTeams.begin(), machine->machineTeams.end(), machineTeamInfo) == 0);
		machine->machineTeams.push_back(machineTeamInfo);
	}

	return machineTeamInfo;
}

Reference<TCMachineTeamInfo> DDTeamCollection::addMachineTeam(std::vector<Standalone<StringRef>>::iterator begin,
                                                              std::vector<Standalone<StringRef>>::iterator end) {
	std::vector<Reference<TCMachineInfo>> machines;

	for (auto i = begin; i != end; ++i) {
		if (machine_info.find(*i) != machine_info.end()) {
			machines.push_back(machine_info[*i]);
		} else {
			TraceEvent(SevWarn, "AddMachineTeamError").detail("MachineIDNotExist", i->contents().toString());
		}
	}

	return addMachineTeam(machines);
}

int DDTeamCollection::constructMachinesFromServers() {
	int totalServerIndex = 0;
	for (auto& [serverID, server] : server_info) {
		if (!server_status.get(serverID).isUnhealthy()) {
			checkAndCreateMachine(server);
			totalServerIndex++;
		}
	}

	return totalServerIndex;
}

void DDTeamCollection::traceConfigInfo() const {
	TraceEvent("DDConfig", distributorId)
	    .detail("StorageTeamSize", configuration.storageTeamSize)
	    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER)
	    .detail("MaxTeamsPerServer", SERVER_KNOBS->MAX_TEAMS_PER_SERVER)
	    .detail("StoreType", configuration.storageServerStoreType);
}

void DDTeamCollection::traceServerTeamInfo() const {
	int i = 0;

	TraceEvent("ServerTeamInfo", distributorId).detail("Size", teams.size());
	for (auto& team : teams) {
		TraceEvent("ServerTeamInfo", distributorId)
		    .detail("TeamIndex", i++)
		    .detail("Healthy", team->isHealthy())
		    .detail("TeamSize", team->size())
		    .detail("MemberIDs", team->getServerIDsStr())
		    .detail("TeamID", team->getTeamID());
	}
}

void DDTeamCollection::traceMachineInfo() const {
	int i = 0;

	TraceEvent("MachineInfo").detail("Size", machine_info.size());
	for (auto& [machineName, machineInfo] : machine_info) {
		TraceEvent("MachineInfo", distributorId)
		    .detail("MachineInfoIndex", i++)
		    .detail("Healthy", isMachineHealthy(machineInfo))
		    .detail("MachineID", machineName.contents().toString())
		    .detail("MachineTeamOwned", machineInfo->machineTeams.size())
		    .detail("ServerNumOnMachine", machineInfo->serversOnMachine.size())
		    .detail("ServersID", machineInfo->getServersIDStr());
	}
}

void DDTeamCollection::traceMachineTeamInfo() const {
	int i = 0;

	TraceEvent("MachineTeamInfo", distributorId).detail("Size", machineTeams.size());
	for (auto& team : machineTeams) {
		TraceEvent("MachineTeamInfo", distributorId)
		    .detail("TeamIndex", i++)
		    .detail("MachineIDs", team->getMachineIDsStr())
		    .detail("ServerTeams", team->getServerTeams().size());
	}
}

void DDTeamCollection::traceLocalityArrayIndexName() const {
	TraceEvent("LocalityRecordKeyName").detail("Size", machineLocalityMap._keymap->_lookuparray.size());
	for (int i = 0; i < machineLocalityMap._keymap->_lookuparray.size(); ++i) {
		TraceEvent("LocalityRecordKeyIndexName")
		    .detail("KeyIndex", i)
		    .detail("KeyName", machineLocalityMap._keymap->_lookuparray[i]);
	}
}

void DDTeamCollection::traceMachineLocalityMap() const {
	int i = 0;

	TraceEvent("MachineLocalityMap", distributorId).detail("Size", machineLocalityMap.size());
	for (auto& uid : machineLocalityMap.getObjects()) {
		Reference<LocalityRecord> record = machineLocalityMap.getRecord(i);
		if (record.isValid()) {
			TraceEvent("MachineLocalityMap", distributorId)
			    .detail("LocalityIndex", i++)
			    .detail("UID", uid->toString())
			    .detail("LocalityRecord", record->toString());
		} else {
			TraceEvent("MachineLocalityMap")
			    .detail("LocalityIndex", i++)
			    .detail("UID", uid->toString())
			    .detail("LocalityRecord", "[NotFound]");
		}
	}
}

void DDTeamCollection::traceAllInfo(bool shouldPrint) const {

	if (!shouldPrint)
		return;
	// Record all team collections IDs
	for (int i = 0; i < teamCollections.size(); ++i) {
		if (teamCollections[i] != nullptr) {
			TraceEvent("TraceAllInfo", distributorId)
			    .detail("TeamCollectionIndex", i)
			    .detail("Primary", teamCollections[i]->primary);
		}
	}

	// TODO: flush trace log to avoid trace buffer overflow when DD has too many servers and teams
	TraceEvent("TraceAllInfo", distributorId).detail("Primary", primary);
	traceConfigInfo();
	traceServerInfo();
	traceServerTeamInfo();
	traceMachineInfo();
	traceMachineTeamInfo();
	traceLocalityArrayIndexName();
	traceMachineLocalityMap();
}

void DDTeamCollection::rebuildMachineLocalityMap() {
	machineLocalityMap.clear();
	for (auto& [_, machine] : machine_info) {
		if (machine->serversOnMachine.empty()) {
			TraceEvent(SevWarn, "RebuildMachineLocalityMapError")
			    .detail("Machine", machine->machineID.toString())
			    .detail("NumServersOnMachine", 0);
			continue;
		}
		if (!isMachineHealthy(machine)) {
			continue;
		}
		Reference<TCServerInfo> representativeServer = machine->serversOnMachine[0];
		auto& locality = representativeServer->getLastKnownInterface().locality;
		if (!isValidLocality(configuration.storagePolicy, locality)) {
			TraceEvent(SevWarn, "RebuildMachineLocalityMapError")
			    .detail("Machine", machine->machineID.toString())
			    .detail("InvalidLocality", locality.toString());
			continue;
		}
		const LocalityEntry& localityEntry = machineLocalityMap.add(locality, &representativeServer->getId());
		machine->localityEntry = localityEntry;
	}
}

int DDTeamCollection::addBestMachineTeams(int machineTeamsToBuild) {
	int addedMachineTeams = 0;

	ASSERT_GE(machineTeamsToBuild, 0);
	// The number of machines is always no smaller than the storageTeamSize in a correct configuration
	ASSERT_GE(machine_info.size(), configuration.storageTeamSize);
	// Future: Consider if we should overbuild more machine teams to
	// allow machineTeamRemover() to get a more balanced machine teams per machine

	// Step 1: Create machineLocalityMap which will be used in building machine team
	rebuildMachineLocalityMap();

	// Add a team in each iteration
	while (addedMachineTeams < machineTeamsToBuild || notEnoughMachineTeamsForAMachine()) {
		// Step 2: Get least used machines from which we choose machines as a machine team
		std::vector<Reference<TCMachineInfo>> leastUsedMachines; // A less used machine has less number of teams
		int minTeamCount = std::numeric_limits<int>::max();
		for (auto& machine : machine_info) {
			// Skip invalid machine whose representative server is not in server_info
			ASSERT_WE_THINK(server_info.find(machine.second->serversOnMachine[0]->getId()) != server_info.end());
			// Skip unhealthy machines
			if (!isMachineHealthy(machine.second))
				continue;
			// Skip machine with incomplete locality
			if (!isValidLocality(configuration.storagePolicy,
			                     machine.second->serversOnMachine[0]->getLastKnownInterface().locality)) {
				continue;
			}

			// Invariant: We only create correct size machine teams.
			// When configuration (e.g., team size) is changed, the DDTeamCollection will be destroyed and rebuilt
			// so that the invariant will not be violated.
			int teamCount = machine.second->machineTeams.size();

			if (teamCount < minTeamCount) {
				leastUsedMachines.clear();
				minTeamCount = teamCount;
			}
			if (teamCount == minTeamCount) {
				leastUsedMachines.push_back(machine.second);
			}
		}

		std::vector<UID*> team;
		std::vector<LocalityEntry> forcedAttributes;
		std::vector<UID*> bestTeam;
		int bestScore = std::numeric_limits<int>::max();
		int maxAttempts = SERVER_KNOBS->BEST_OF_AMT; // BEST_OF_AMT = 4
		for (int i = 0; i < maxAttempts && i < 100; ++i) {
			// Step 3: Create a representative process for each machine.
			// Construct forcedAttribute from leastUsedMachines.
			// We will use forcedAttribute to call existing function to form a team
			if (leastUsedMachines.size()) {
				forcedAttributes.clear();
				// Randomly choose 1 least used machine
				Reference<TCMachineInfo> tcMachineInfo = deterministicRandom()->randomChoice(leastUsedMachines);
				ASSERT(!tcMachineInfo->serversOnMachine.empty());
				LocalityEntry process = tcMachineInfo->localityEntry;
				forcedAttributes.push_back(process);
				TraceEvent("ChosenMachine")
				    .suppressFor(30.0)
				    .detail("MachineInfo", tcMachineInfo->machineID)
				    .detail("LeaseUsedMachinesSize", leastUsedMachines.size())
				    .detail("ForcedAttributesSize", forcedAttributes.size());
			} else {
				// when leastUsedMachine is empty, we will never find a team later, so we can simply return.
				return addedMachineTeams;
			}

			// Choose a team that balances the # of teams per server among the teams
			// that have the least-utilized server
			team.clear();
			ASSERT_WE_THINK(forcedAttributes.size() == 1);
			// Step 4: Reuse Policy's selectReplicas() to create team for the representative process.
			auto success = machineLocalityMap.selectReplicas(configuration.storagePolicy, forcedAttributes, team);
			// NOTE: selectReplicas() should always return success when storageTeamSize = 1
			ASSERT_WE_THINK(configuration.storageTeamSize > 1 || (configuration.storageTeamSize == 1 && success));
			if (!success) {
				continue; // Try up to maxAttempts, since next time we may choose a different forcedAttributes
			}
			ASSERT_GT(forcedAttributes.size(), 0);
			team.push_back((UID*)machineLocalityMap.getObject(forcedAttributes[0]));

			// selectReplicas() may NEVER return server not in server_info.
			for (auto& pUID : team) {
				ASSERT_WE_THINK(server_info.find(*pUID) != server_info.end());
			}

			// selectReplicas() should always return a team with correct size. otherwise, it has a bug
			ASSERT_EQ(team.size(), configuration.storageTeamSize);

			int score = 0;
			std::vector<Standalone<StringRef>> machineIDs;
			for (auto process = team.begin(); process != team.end(); process++) {
				Reference<TCServerInfo> server = server_info[**process];
				score += server->machine->machineTeams.size();
				Standalone<StringRef> machine_id = server->getLastKnownInterface().locality.zoneId().get();
				machineIDs.push_back(machine_id);
			}

			// Only choose healthy machines into machine team
			ASSERT_WE_THINK(isMachineTeamHealthy(machineIDs));

			std::sort(machineIDs.begin(), machineIDs.end());
			int overlap = overlappingMachineMembers(machineIDs);
			if (overlap == machineIDs.size()) {
				maxAttempts += 1;
				continue;
			}
			score += SERVER_KNOBS->DD_OVERLAP_PENALTY * overlap;

			// SOMEDAY: randomly pick one from teams with the lowest score
			if (score < bestScore) {
				// bestTeam is the team which has the smallest number of teams its team members belong to.
				bestTeam = team;
				bestScore = score;
			}
		}

		// bestTeam should be a new valid team to be added into machine team now
		// Step 5: Restore machine from its representative process team and get the machine team
		if (bestTeam.size() == configuration.storageTeamSize) {
			// machineIDs is used to quickly check if the machineIDs belong to an existed team
			// machines keep machines reference for performance benefit by avoiding looking up machine by machineID
			std::vector<Reference<TCMachineInfo>> machines;
			for (auto process = bestTeam.begin(); process < bestTeam.end(); process++) {
				Reference<TCMachineInfo> machine = server_info[**process]->machine;
				machines.push_back(machine);
			}

			addMachineTeam(machines);
			addedMachineTeams++;
		} else {
			// When too many teams exist in simulation, traceAllInfo will buffer too many trace logs before
			// trace has a chance to flush its buffer, which causes assertion failure.
			traceAllInfo(!g_network->isSimulated());
			TraceEvent(SevWarn, "BuildTeamsLastBuildTeamsFailed", distributorId)
			    .detail("Primary", primary)
			    .detail("Reason", "Unable to make desired machineTeams")
			    .detail("Hint", "Check TraceAllInfo event");
			lastBuildTeamsFailed = true;
			break;
		}
	}

	return addedMachineTeams;
}

Reference<TCServerInfo> DDTeamCollection::findOneLeastUsedServer() const {
	std::vector<Reference<TCServerInfo>> leastUsedServers;
	int minTeams = std::numeric_limits<int>::max();
	for (auto& [serverID, server] : server_info) {
		// Only pick healthy server, which is not failed or excluded.
		if (server_status.get(serverID).isUnhealthy())
			continue;
		if (!isValidLocality(configuration.storagePolicy, server->getLastKnownInterface().locality))
			continue;

		int numTeams = server->getTeams().size();
		if (numTeams < minTeams) {
			minTeams = numTeams;
			leastUsedServers.clear();
		}
		if (minTeams == numTeams) {
			leastUsedServers.push_back(server);
		}
	}

	if (leastUsedServers.empty()) {
		// If we cannot find a healthy server with valid locality
		TraceEvent("NoHealthyAndValidLocalityServers")
		    .detail("Servers", server_info.size())
		    .detail("UnhealthyServers", unhealthyServers);
		return Reference<TCServerInfo>();
	} else {
		return deterministicRandom()->randomChoice(leastUsedServers);
	}
}

Reference<TCMachineTeamInfo> DDTeamCollection::findOneRandomMachineTeam(TCServerInfo const& chosenServer) const {
	if (!chosenServer.machine->machineTeams.empty()) {
		std::vector<Reference<TCMachineTeamInfo>> healthyMachineTeamsForChosenServer;
		for (auto& mt : chosenServer.machine->machineTeams) {
			if (isMachineTeamHealthy(*mt)) {
				healthyMachineTeamsForChosenServer.push_back(mt);
			}
		}
		if (!healthyMachineTeamsForChosenServer.empty()) {
			return deterministicRandom()->randomChoice(healthyMachineTeamsForChosenServer);
		}
	}

	// If we cannot find a healthy machine team
	TraceEvent("NoHealthyMachineTeamForServer")
	    .detail("ServerID", chosenServer.getId())
	    .detail("MachineTeams", chosenServer.machine->machineTeams.size());
	return Reference<TCMachineTeamInfo>();
}

bool DDTeamCollection::isOnSameMachineTeam(TCTeamInfo const& team) const {
	std::vector<Standalone<StringRef>> machineIDs;
	for (const auto& server : team.getServers()) {
		if (!server->machine.isValid())
			return false;
		machineIDs.push_back(server->machine->machineID);
	}
	std::sort(machineIDs.begin(), machineIDs.end());

	int numExistence = 0;
	for (const auto& server : team.getServers()) {
		for (const auto& candidateMachineTeam : server->machine->machineTeams) {
			if (candidateMachineTeam->matches(machineIDs)) {
				numExistence++;
				break;
			}
		}
	}
	return (numExistence == team.size());
}

bool DDTeamCollection::sanityCheckTeams() const {
	for (auto& team : teams) {
		if (isOnSameMachineTeam(*team) == false) {
			return false;
		}
	}

	return true;
}

int DDTeamCollection::calculateHealthyServerCount() const {
	int serverCount = 0;
	for (const auto& [id, _] : server_info) {
		if (!server_status.get(id).isUnhealthy()) {
			++serverCount;
		}
	}
	return serverCount;
}

int DDTeamCollection::calculateHealthyMachineCount() const {
	int totalHealthyMachineCount = 0;
	for (auto& m : machine_info) {
		if (isMachineHealthy(m.second)) {
			++totalHealthyMachineCount;
		}
	}

	return totalHealthyMachineCount;
}

std::pair<int64_t, int64_t> DDTeamCollection::calculateMinMaxServerTeamsOnServer() const {
	int64_t minTeams = std::numeric_limits<int64_t>::max();
	int64_t maxTeams = 0;
	for (auto& [serverID, server] : server_info) {
		if (server_status.get(serverID).isUnhealthy()) {
			continue;
		}
		minTeams = std::min((int64_t)server->getTeams().size(), minTeams);
		maxTeams = std::max((int64_t)server->getTeams().size(), maxTeams);
	}
	return std::make_pair(minTeams, maxTeams);
}

std::pair<int64_t, int64_t> DDTeamCollection::calculateMinMaxMachineTeamsOnMachine() const {
	int64_t minTeams = std::numeric_limits<int64_t>::max();
	int64_t maxTeams = 0;
	for (auto& [_, machine] : machine_info) {
		if (!isMachineHealthy(machine)) {
			continue;
		}
		minTeams = std::min<int64_t>((int64_t)machine->machineTeams.size(), minTeams);
		maxTeams = std::max<int64_t>((int64_t)machine->machineTeams.size(), maxTeams);
	}
	return std::make_pair(minTeams, maxTeams);
}

bool DDTeamCollection::isServerTeamCountCorrect(Reference<TCMachineTeamInfo> const& mt) const {
	int num = 0;
	bool ret = true;
	for (auto& team : teams) {
		if (team->machineTeam->getMachineIDs() == mt->getMachineIDs()) {
			++num;
		}
	}
	if (num != mt->getServerTeams().size()) {
		ret = false;
		TraceEvent(SevError, "ServerTeamCountOnMachineIncorrect")
		    .detail("MachineTeam", mt->getMachineIDsStr())
		    .detail("ServerTeamsSize", mt->getServerTeams().size())
		    .detail("CountedServerTeams", num);
	}
	return ret;
}

std::pair<Reference<TCMachineTeamInfo>, int> DDTeamCollection::getMachineTeamWithLeastProcessTeams() const {
	Reference<TCMachineTeamInfo> retMT;
	int minNumProcessTeams = std::numeric_limits<int>::max();

	for (auto& mt : machineTeams) {
		if (EXPENSIVE_VALIDATION) {
			ASSERT(isServerTeamCountCorrect(mt));
		}

		if (mt->getServerTeams().size() < minNumProcessTeams) {
			minNumProcessTeams = mt->getServerTeams().size();
			retMT = mt;
		}
	}

	return std::pair<Reference<TCMachineTeamInfo>, int>(retMT, minNumProcessTeams);
}

std::pair<Reference<TCMachineTeamInfo>, int> DDTeamCollection::getMachineTeamWithMostMachineTeams() const {
	Reference<TCMachineTeamInfo> retMT;
	int maxNumMachineTeams = 0;
	int targetMachineTeamNumPerMachine =
	    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;

	for (auto& mt : machineTeams) {
		// The representative team number for the machine team mt is
		// the minimum number of machine teams of a machine in the team mt
		int representNumMachineTeams = std::numeric_limits<int>::max();
		for (auto& m : mt->getMachines()) {
			representNumMachineTeams = std::min<int>(representNumMachineTeams, m->machineTeams.size());
		}
		if (representNumMachineTeams > targetMachineTeamNumPerMachine &&
		    representNumMachineTeams > maxNumMachineTeams) {
			maxNumMachineTeams = representNumMachineTeams;
			retMT = mt;
		}
	}

	return std::pair<Reference<TCMachineTeamInfo>, int>(retMT, maxNumMachineTeams);
}

std::pair<Reference<TCTeamInfo>, int> DDTeamCollection::getServerTeamWithMostProcessTeams() const {
	Reference<TCTeamInfo> retST;
	int maxNumProcessTeams = 0;
	int targetTeamNumPerServer = (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;

	for (auto& t : teams) {
		// The minimum number of teams of a server in a team is the representative team number for the team t
		int representNumProcessTeams = std::numeric_limits<int>::max();
		for (auto& server : t->getServers()) {
			representNumProcessTeams = std::min<int>(representNumProcessTeams, server->getTeams().size());
		}
		// We only remove the team whose representNumProcessTeams is larger than the targetTeamNumPerServer number
		// otherwise, teamBuilder will build the to-be-removed team again
		if (representNumProcessTeams > targetTeamNumPerServer && representNumProcessTeams > maxNumProcessTeams) {
			maxNumProcessTeams = representNumProcessTeams;
			retST = t;
		}
	}

	return std::pair<Reference<TCTeamInfo>, int>(retST, maxNumProcessTeams);
}

int DDTeamCollection::getHealthyMachineTeamCount() const {
	int healthyTeamCount = 0;
	for (const auto& mt : machineTeams) {
		ASSERT_EQ(mt->getMachines().size(), configuration.storageTeamSize);

		if (isMachineTeamHealthy(*mt)) {
			++healthyTeamCount;
		}
	}

	return healthyTeamCount;
}

bool DDTeamCollection::notEnoughMachineTeamsForAMachine() const {
	// If we want to remove the machine team with most machine teams, we use the same logic as
	// notEnoughTeamsForAServer
	int targetMachineTeamNumPerMachine =
	    SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS
	        ? (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2
	        : SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER;
	for (auto& [_, machine] : machine_info) {
		// If SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS is false,
		// The desired machine team number is not the same with the desired server team number
		// in notEnoughTeamsForAServer() below, because the machineTeamRemover() does not
		// remove a machine team with the most number of machine teams.
		if (machine->machineTeams.size() < targetMachineTeamNumPerMachine && isMachineHealthy(machine)) {
			return true;
		}
	}

	return false;
}

bool DDTeamCollection::notEnoughTeamsForAServer() const {
	// We build more teams than we finally want so that we can use serverTeamRemover() actor to remove the teams
	// whose member belong to too many teams. This allows us to get a more balanced number of teams per server.
	// We want to ensure every server has targetTeamNumPerServer teams.
	// The numTeamsPerServerFactor is calculated as
	// (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER + ideal_num_of_teams_per_server) / 2
	// ideal_num_of_teams_per_server is (#teams * storageTeamSize) / #servers, which is
	// (#servers * DESIRED_TEAMS_PER_SERVER * storageTeamSize) / #servers.
	int targetTeamNumPerServer = (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;
	ASSERT_GT(targetTeamNumPerServer, 0);
	for (auto& [serverID, server] : server_info) {
		if (server->getTeams().size() < targetTeamNumPerServer && !server_status.get(serverID).isUnhealthy()) {
			return true;
		}
	}

	return false;
}

int DDTeamCollection::addTeamsBestOf(int teamsToBuild, int desiredTeams, int maxTeams) {
	ASSERT_GE(teamsToBuild, 0);
	ASSERT_WE_THINK(machine_info.size() > 0 || server_info.size() == 0);
	ASSERT_WE_THINK(SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER >= 1 && configuration.storageTeamSize >= 1);

	int addedTeams = 0;

	// Exclude machine teams who have members in the wrong configuration.
	// When we change configuration, we may have machine teams with storageTeamSize in the old configuration.
	int healthyMachineTeamCount = getHealthyMachineTeamCount();
	int totalMachineTeamCount = machineTeams.size();
	int totalHealthyMachineCount = calculateHealthyMachineCount();

	int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
	int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
	// machineTeamsToBuild mimics how the teamsToBuild is calculated in buildTeams()
	int machineTeamsToBuild =
	    std::max(0, std::min(desiredMachineTeams - healthyMachineTeamCount, maxMachineTeams - totalMachineTeamCount));
	if (healthyMachineTeamCount == 0 && machineTeamsToBuild == 0 && SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE > 0) {
		// Use DD_BUILD_EXTRA_TEAMS_OVERRIDE > 0 as the feature flag: Set to 0 to disable it
		TraceEvent(SevWarnAlways, "BuildMachineTeamsHaveTooManyUnhealthyMachineTeams")
		    .detail("Hint", "Build teams may stuck and prevent DD from relocating data")
		    .detail("BuildExtraMachineTeamsOverride", SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE);
		machineTeamsToBuild = SERVER_KNOBS->DD_BUILD_EXTRA_TEAMS_OVERRIDE;
	}
	if (g_network->isSimulated() && deterministicRandom()->random01() < 0.1) {
		// Test when the system has lots of unhealthy machine teams, which may prevent TC from building new teams.
		// The scenario creates a deadlock situation that DD cannot relocate data.
		int totalMachineTeams = nChooseK(machine_info.size(), configuration.storageTeamSize);
		TraceEvent("BuildMachineTeams")
		    .detail("Primary", primary)
		    .detail("CalculatedMachineTeamsToBuild", machineTeamsToBuild)
		    .detail("OverwriteMachineTeamsToBuildForTesting", totalMachineTeams);

		machineTeamsToBuild = totalMachineTeams;
	}

	{
		TraceEvent te("BuildMachineTeams");
		te.detail("Primary", primary)
		    .detail("TotalMachines", machine_info.size())
		    .detail("TotalHealthyMachine", totalHealthyMachineCount)
		    .detail("HealthyMachineTeamCount", healthyMachineTeamCount)
		    .detail("DesiredMachineTeams", desiredMachineTeams)
		    .detail("MaxMachineTeams", maxMachineTeams)
		    .detail("TotalMachineTeams", totalMachineTeamCount)
		    .detail("MachineTeamsToBuild", machineTeamsToBuild);
		// Pre-build all machine teams until we have the desired number of machine teams
		if (machineTeamsToBuild > 0 || notEnoughMachineTeamsForAMachine()) {
			auto addedMachineTeams = addBestMachineTeams(machineTeamsToBuild);
			te.detail("MachineTeamsAdded", addedMachineTeams);
		}
	}

	while (addedTeams < teamsToBuild || notEnoughTeamsForAServer()) {
		std::vector<UID> bestServerTeam;
		int bestScore = std::numeric_limits<int>::max();
		int maxAttempts = SERVER_KNOBS->BEST_OF_AMT; // BEST_OF_AMT = 4
		bool earlyQuitBuild = false;
		for (int i = 0; i < maxAttempts && i < 100; ++i) {
			// Step 1: Choose 1 least used server and then choose 1 least used machine team from the server
			Reference<TCServerInfo> chosenServer = findOneLeastUsedServer();
			if (!chosenServer.isValid()) {
				TraceEvent(SevWarn, "NoValidServer").detail("Primary", primary);
				earlyQuitBuild = true;
				break;
			}
			// Note: To avoid creating correlation of picked machine teams, we simply choose a random machine team
			// instead of choosing the least used machine team.
			// The correlation happens, for example, when we add two new machines, we may always choose the machine
			// team with these two new machines because they are typically less used.
			Reference<TCMachineTeamInfo> chosenMachineTeam = findOneRandomMachineTeam(*chosenServer);

			if (!chosenMachineTeam.isValid()) {
				// We may face the situation that temporarily we have no healthy machine.
				TraceEvent(SevWarn, "MachineTeamNotFound")
				    .detail("Primary", primary)
				    .detail("MachineTeams", machineTeams.size());
				continue; // try randomly to find another least used server
			}

			// From here, chosenMachineTeam must have a healthy server team
			// Step 2: Randomly pick 1 server from each machine in the chosen machine team to form a server team
			std::vector<UID> serverTeam;
			int chosenServerCount = 0;
			for (auto& machine : chosenMachineTeam->getMachines()) {
				UID serverID;
				if (machine == chosenServer->machine) {
					// If the machine is from `chosenServer`, use `chosenServer` from this machine, since it is a least
					// utilized server on this machine.
					serverID = chosenServer->getId();
					++chosenServerCount;
				} else {
					std::vector<Reference<TCServerInfo>> healthyProcesses;
					for (auto it : machine->serversOnMachine) {
						if (!server_status.get(it->getId()).isUnhealthy()) {
							healthyProcesses.push_back(it);
						}
					}
					serverID = deterministicRandom()->randomChoice(healthyProcesses)->getId();
				}
				serverTeam.push_back(serverID);
			}

			ASSERT_EQ(chosenServerCount, 1); // chosenServer should be used exactly once
			ASSERT_EQ(serverTeam.size(), configuration.storageTeamSize);

			std::sort(serverTeam.begin(), serverTeam.end());
			int overlap = overlappingMembers(serverTeam);
			if (overlap == serverTeam.size()) {
				maxAttempts += 1;
				continue;
			}

			// Pick the server team with smallest score in all attempts
			// If we use different metric here, DD may oscillate infinitely in creating and removing teams.
			// SOMEDAY: Improve the code efficiency by using reservoir algorithm
			int score = SERVER_KNOBS->DD_OVERLAP_PENALTY * overlap;
			for (auto& server : serverTeam) {
				score += server_info[server]->getTeams().size();
			}
			TraceEvent(SevDebug, "BuildServerTeams")
			    .detail("Score", score)
			    .detail("BestScore", bestScore)
			    .detail("TeamSize", serverTeam.size())
			    .detail("StorageTeamSize", configuration.storageTeamSize);
			if (score < bestScore) {
				bestScore = score;
				bestServerTeam = serverTeam;
			}
		}

		if (earlyQuitBuild) {
			break;
		}
		if (bestServerTeam.size() != configuration.storageTeamSize) {
			// Not find any team and will unlikely find a team
			lastBuildTeamsFailed = true;
			TraceEvent(SevWarn, "BuildTeamsLastBuildTeamsFailed", distributorId)
			    .detail("Reason", "Unable to find any valid serverTeam")
			    .detail("Primary", primary)
			    .detail("BestServerTeam", describe(bestServerTeam))
			    .detail("ConfigStorageTeamSize", configuration.storageTeamSize);
			break;
		}

		// Step 4: Add the server team
		addTeam(bestServerTeam.begin(), bestServerTeam.end(), IsInitialTeam::False);
		addedTeams++;
	}

	healthyMachineTeamCount = getHealthyMachineTeamCount();

	auto [minTeamsOnServer, maxTeamsOnServer] = calculateMinMaxServerTeamsOnServer();
	auto [minMachineTeamsOnMachine, maxMachineTeamsOnMachine] = calculateMinMaxMachineTeamsOnMachine();

	TraceEvent("TeamCollectionInfo", distributorId)
	    .detail("Primary", primary)
	    .detail("AddedTeams", addedTeams)
	    .detail("TeamsToBuild", teamsToBuild)
	    .detail("CurrentServerTeams", teams.size())
	    .detail("DesiredTeams", desiredTeams)
	    .detail("MaxTeams", maxTeams)
	    .detail("StorageTeamSize", configuration.storageTeamSize)
	    .detail("CurrentMachineTeams", machineTeams.size())
	    .detail("CurrentHealthyMachineTeams", healthyMachineTeamCount)
	    .detail("DesiredMachineTeams", desiredMachineTeams)
	    .detail("MaxMachineTeams", maxMachineTeams)
	    .detail("TotalHealthyMachines", totalHealthyMachineCount)
	    .detail("MinTeamsOnServer", minTeamsOnServer)
	    .detail("MaxTeamsOnServer", maxTeamsOnServer)
	    .detail("MinMachineTeamsOnMachine", minMachineTeamsOnMachine)
	    .detail("MaxMachineTeamsOnMachine", maxMachineTeamsOnMachine)
	    .detail("DoBuildTeams", doBuildTeams)
	    .trackLatest(teamCollectionInfoEventHolder->trackingKey);

	return addedTeams;
}

void DDTeamCollection::traceTeamCollectionInfo() const {
	int totalHealthyServerCount = calculateHealthyServerCount();
	int desiredServerTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyServerCount;
	int maxServerTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyServerCount;

	int totalHealthyMachineCount = calculateHealthyMachineCount();
	int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
	int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
	int healthyMachineTeamCount = getHealthyMachineTeamCount();

	auto [minTeamsOnServer, maxTeamsOnServer] = calculateMinMaxServerTeamsOnServer();
	auto [minMachineTeamsOnMachine, maxMachineTeamsOnMachine] = calculateMinMaxMachineTeamsOnMachine();

	TraceEvent("TeamCollectionInfo", distributorId)
	    .detail("Primary", primary)
	    .detail("AddedTeams", 0)
	    .detail("TeamsToBuild", 0)
	    .detail("CurrentServerTeams", teams.size())
	    .detail("DesiredTeams", desiredServerTeams)
	    .detail("MaxTeams", maxServerTeams)
	    .detail("StorageTeamSize", configuration.storageTeamSize)
	    .detail("CurrentMachineTeams", machineTeams.size())
	    .detail("CurrentHealthyMachineTeams", healthyMachineTeamCount)
	    .detail("DesiredMachineTeams", desiredMachineTeams)
	    .detail("MaxMachineTeams", maxMachineTeams)
	    .detail("TotalHealthyMachines", totalHealthyMachineCount)
	    .detail("MinTeamsOnServer", minTeamsOnServer)
	    .detail("MaxTeamsOnServer", maxTeamsOnServer)
	    .detail("MinMachineTeamsOnMachine", minMachineTeamsOnMachine)
	    .detail("MaxMachineTeamsOnMachine", maxMachineTeamsOnMachine)
	    .detail("DoBuildTeams", doBuildTeams)
	    .trackLatest(teamCollectionInfoEventHolder->trackingKey);

	// Advance time so that we will not have multiple TeamCollectionInfo at the same time, otherwise
	// simulation test will randomly pick one TeamCollectionInfo trace, which could be the one before build teams
	// wait(delay(0.01));

	// Debug purpose
	// if (healthyMachineTeamCount > desiredMachineTeams || machineTeams.size() > maxMachineTeams) {
	// 	// When the number of machine teams is over the limit, print out the current team info.
	// 	traceAllInfo(true);
	// }
}

void DDTeamCollection::noHealthyTeams() const {
	std::set<UID> desiredServerSet;
	std::string desc;
	for (auto& [serverID, server] : server_info) {
		ASSERT(serverID == server->getId());
		if (!server_status.get(serverID).isFailed) {
			desiredServerSet.insert(serverID);
			desc += serverID.shortString() + " (" + server->getLastKnownInterface().toString() + "), ";
		}
	}

	TraceEvent(SevWarn, "NoHealthyTeams", distributorId)
	    .detail("CurrentServerTeamCount", teams.size())
	    .detail("ServerCount", server_info.size())
	    .detail("NonFailedServerCount", desiredServerSet.size());
}

bool DDTeamCollection::shouldHandleServer(const StorageServerInterface& newServer) const {
	return (includedDCs.empty() ||
	        std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end() ||
	        (otherTrackedDCs.present() &&
	         std::find(otherTrackedDCs.get().begin(), otherTrackedDCs.get().end(), newServer.locality.dcId()) ==
	             otherTrackedDCs.get().end()));
}

void DDTeamCollection::addServer(StorageServerInterface newServer,
                                 ProcessClass processClass,
                                 Promise<Void> errorOut,
                                 Version addedVersion,
                                 DDEnabledState const& ddEnabledState) {
	if (!shouldHandleServer(newServer)) {
		return;
	}

	if (!newServer.isTss()) {
		allServers.push_back(newServer.id());
	}

	TraceEvent(newServer.isTss() ? "AddedTSS" : "AddedStorageServer", distributorId)
	    .detail("ServerID", newServer.id())
	    .detail("ProcessID", newServer.locality.processId())
	    .detail("ProcessClass", processClass.toString())
	    .detail("WaitFailureToken", newServer.waitFailure.getEndpoint().token)
	    .detail("Address", newServer.waitFailure.getEndpoint().getPrimaryAddress());

	auto& r = server_and_tss_info[newServer.id()] = makeReference<TCServerInfo>(
	    newServer,
	    this,
	    processClass,
	    includedDCs.empty() ||
	        std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end(),
	    storageServerSet,
	    addedVersion);

	if (newServer.isTss()) {
		tss_info_by_pair[newServer.tssPairID.get()] = r;

		if (server_info.contains(newServer.tssPairID.get())) {
			r->onTSSPairRemoved = server_info[newServer.tssPairID.get()]->onRemoved;
		}
	} else {
		server_info[newServer.id()] = r;
		// Establish the relation between server and machine
		checkAndCreateMachine(r);
	}

	r->setTracker(storageServerTracker(r.getPtr(), errorOut, addedVersion, ddEnabledState, newServer.isTss()));

	if (!newServer.isTss()) {
		// link and wake up tss' tracker so it knows when this server gets removed
		if (tss_info_by_pair.contains(newServer.id())) {
			tss_info_by_pair[newServer.id()]->onTSSPairRemoved = r->onRemoved;
			if (tss_info_by_pair[newServer.id()]->wakeUpTracker.canBeSet()) {
				auto p = tss_info_by_pair[newServer.id()]->wakeUpTracker;
				// This callback could delete tss_info_by_pair[newServer.id()], so use a copy
				p.send(Void());
			}
		}

		doBuildTeams = true; // Adding a new server triggers to build new teams
		restartTeamBuilder.trigger();
	}
}

bool DDTeamCollection::removeTeam(Reference<TCTeamInfo> team) {
	TraceEvent("RemovedServerTeam", distributorId).detail("Team", team->getDesc());
	bool found = false;
	for (int t = 0; t < teams.size(); t++) {
		if (teams[t] == team) {
			teams[t--] = teams.back();
			teams.pop_back();
			found = true;
			break;
		}
	}

	for (auto& server : team->getServers()) {
		server->removeTeam(team);
	}

	// Remove the team from its machine team
	bool foundInMachineTeam = team->machineTeam->removeServerTeam(team);

	ASSERT_WE_THINK(foundInMachineTeam);
	team->tracker.cancel();
	team->setHealthy(false);
	if (g_network->isSimulated()) {
		// Update server team information for consistency check in simulation
		traceTeamCollectionInfo();
	}
	return found;
}

Reference<TCMachineInfo> DDTeamCollection::checkAndCreateMachine(Reference<TCServerInfo> server) {
	ASSERT(server.isValid() && server_info.find(server->getId()) != server_info.end());
	auto const& locality = server->getLastKnownInterface().locality;
	Standalone<StringRef> machine_id = locality.zoneId().get(); // locality to machine_id with std::string type

	Reference<TCMachineInfo> machineInfo;
	if (machine_info.find(machine_id) == machine_info.end()) {
		// uid is the first storage server process on the machine
		CODE_PROBE(true, "First storage server in process on the machine");
		// For each machine, store the first server's localityEntry into machineInfo for later use.
		LocalityEntry localityEntry = machineLocalityMap.add(locality, &server->getId());
		machineInfo = makeReference<TCMachineInfo>(server, localityEntry);
		machine_info.insert(std::make_pair(machine_id, machineInfo));
	} else {
		machineInfo = machine_info.find(machine_id)->second;
		machineInfo->serversOnMachine.push_back(server);
	}
	server->machine = machineInfo;
	ASSERT(machineInfo->machineID == machine_id); // invariant for TC to work

	return machineInfo;
}

Reference<TCMachineTeamInfo> DDTeamCollection::checkAndCreateMachineTeam(Reference<TCTeamInfo> serverTeam) {
	std::vector<Standalone<StringRef>> machineIDs;
	for (auto& server : serverTeam->getServers()) {
		Reference<TCMachineInfo> machine = server->machine;
		machineIDs.push_back(machine->machineID);
	}

	std::sort(machineIDs.begin(), machineIDs.end());
	Reference<TCMachineTeamInfo> machineTeam = findMachineTeam(machineIDs);
	if (!machineTeam.isValid()) { // Create the machine team if it does not exist
		machineTeam = addMachineTeam(machineIDs.begin(), machineIDs.end());
	}

	machineTeam->addServerTeam(serverTeam);

	return machineTeam;
}

void DDTeamCollection::removeMachine(Reference<TCMachineInfo> removedMachineInfo) {
	// Find machines that share teams with the removed machine
	std::set<Standalone<StringRef>> machinesWithAjoiningTeams;
	for (auto& machineTeam : removedMachineInfo->machineTeams) {
		machinesWithAjoiningTeams.insert(machineTeam->getMachineIDs().begin(), machineTeam->getMachineIDs().end());
	}
	machinesWithAjoiningTeams.erase(removedMachineInfo->machineID);
	// For each machine in a machine team with the removed machine,
	// erase shared machine teams from the list of teams.
	for (auto it = machinesWithAjoiningTeams.begin(); it != machinesWithAjoiningTeams.end(); ++it) {
		auto& machineTeams = machine_info[*it]->machineTeams;
		for (int t = 0; t < machineTeams.size(); t++) {
			auto& machineTeam = machineTeams[t];
			if (machineTeam->containsMachine(removedMachineInfo->machineID)) {
				machineTeams[t--] = machineTeams.back();
				machineTeams.pop_back();
			}
		}
	}
	removedMachineInfo->machineTeams.clear();

	// Remove global machine team that includes removedMachineInfo
	for (int t = 0; t < machineTeams.size(); t++) {
		auto& machineTeam = machineTeams[t];
		if (machineTeam->containsMachine(removedMachineInfo->machineID)) {
			removeMachineTeam(machineTeam);
			// removeMachineTeam will swap the last team in machineTeams vector into [t];
			// t-- to avoid skipping the element
			t--;
		}
	}

	// Remove removedMachineInfo from machine's global info
	machine_info.erase(removedMachineInfo->machineID);
	TraceEvent("MachineLocalityMapUpdate").detail("MachineUIDRemoved", removedMachineInfo->machineID.toString());

	// We do not update macineLocalityMap when a machine is removed because we will do so when we use it in
	// addBestMachineTeams()
	// rebuildMachineLocalityMap();
}

bool DDTeamCollection::removeMachineTeam(Reference<TCMachineTeamInfo> targetMT) {
	bool foundMachineTeam = false;
	for (int i = 0; i < machineTeams.size(); i++) {
		Reference<TCMachineTeamInfo> mt = machineTeams[i];
		if (mt->getMachineIDs() == targetMT->getMachineIDs()) {
			machineTeams[i--] = machineTeams.back();
			machineTeams.pop_back();
			foundMachineTeam = true;
			break;
		}
	}
	// Remove machine team on each machine
	for (auto& machine : targetMT->getMachines()) {
		for (int i = 0; i < machine->machineTeams.size(); ++i) {
			if (machine->machineTeams[i]->getMachineIDs() == targetMT->getMachineIDs()) {
				machine->machineTeams[i--] = machine->machineTeams.back();
				machine->machineTeams.pop_back();
				break; // The machineTeams on a machine should never duplicate
			}
		}
	}

	return foundMachineTeam;
}

void DDTeamCollection::removeTSS(UID removedServer) {
	// much simpler than remove server. tss isn't in any teams, so just remove it from data structures
	TraceEvent("RemovedTSS", distributorId).detail("ServerID", removedServer);
	Reference<TCServerInfo> removedServerInfo = server_and_tss_info[removedServer];

	tss_info_by_pair.erase(removedServerInfo->getLastKnownInterface().tssPairID.get());
	server_and_tss_info.erase(removedServer);

	server_status.clear(removedServer);
}

void DDTeamCollection::removeServer(UID removedServer) {
	TraceEvent("RemovedStorageServer", distributorId).detail("ServerID", removedServer);

	// ASSERT( !shardsAffectedByTeamFailure->getServersForTeam( t ) for all t in teams that contain removedServer )
	Reference<TCServerInfo> removedServerInfo = server_info[removedServer];
	// Step: Remove TCServerInfo from storageWiggler
	storageWiggler->removeServer(removedServer);

	// Step: Remove server team that relate to removedServer
	// Find all servers with which the removedServer shares teams
	std::set<UID> serversWithAjoiningTeams;
	auto const& sharedTeams = removedServerInfo->getTeams();
	for (int i = 0; i < sharedTeams.size(); ++i) {
		auto& teamIds = sharedTeams[i]->getServerIDs();
		serversWithAjoiningTeams.insert(teamIds.begin(), teamIds.end());
	}
	serversWithAjoiningTeams.erase(removedServer);

	// For each server in a team with the removedServer, erase shared teams from the list of teams in that other
	// server
	for (auto it = serversWithAjoiningTeams.begin(); it != serversWithAjoiningTeams.end(); ++it) {
		server_info[*it]->removeTeamsContainingServer(removedServer);
	}

	// Step: Remove all teams that contain removedServer
	// SOMEDAY: can we avoid walking through all teams, since we have an index of teams in which removedServer
	// participated
	int removedCount = 0;
	for (int t = 0; t < teams.size(); t++) {
		if (std::count(teams[t]->getServerIDs().begin(), teams[t]->getServerIDs().end(), removedServer)) {
			TraceEvent("ServerTeamRemoved")
			    .detail("Primary", primary)
			    .detail("TeamServerIDs", teams[t]->getServerIDsStr())
			    .detail("TeamID", teams[t]->getTeamID());
			// removeTeam also needs to remove the team from the machine team info.
			removeTeam(teams[t]);
			t--;
			removedCount++;
		}
	}

	if (removedCount == 0) {
		TraceEvent(SevInfo, "NoTeamsRemovedWhenServerRemoved")
		    .detail("Primary", primary)
		    .detail("Debug", "ThisShouldRarelyHappen_CheckInfoBelow");
	}

	for (int t = 0; t < badTeams.size(); t++) {
		if (std::count(badTeams[t]->getServerIDs().begin(), badTeams[t]->getServerIDs().end(), removedServer)) {
			badTeams[t]->tracker.cancel();
			badTeams[t--] = badTeams.back();
			badTeams.pop_back();
		}
	}

	for (int t = 0; t < largeTeams.size(); t++) {
		if (std::count(largeTeams[t]->getServerIDs().begin(), largeTeams[t]->getServerIDs().end(), removedServer)) {
			largeTeams[t]->tracker.cancel();
			largeTeams[t--] = largeTeams.back();
			largeTeams.pop_back();
		}
	}

	// Step: Remove machine info related to removedServer
	// Remove the server from its machine
	Reference<TCMachineInfo> removedMachineInfo = removedServerInfo->machine;
	for (int i = 0; i < removedMachineInfo->serversOnMachine.size(); ++i) {
		if (removedMachineInfo->serversOnMachine[i] == removedServerInfo) {
			// Safe even when removedServerInfo is the last one
			removedMachineInfo->serversOnMachine[i--] = removedMachineInfo->serversOnMachine.back();
			removedMachineInfo->serversOnMachine.pop_back();
			break;
		}
	}
	// Remove machine if no server on it
	// Note: Remove machine (and machine team) after server teams have been removed, because
	// we remove a machine team only when the server teams on it have been removed
	if (removedMachineInfo->serversOnMachine.size() == 0) {
		removeMachine(removedMachineInfo);
	}

	// If the machine uses removedServer's locality and the machine still has servers, the the machine's
	// representative server will be updated when it is used in addBestMachineTeams()
	// Note that since we do not rebuildMachineLocalityMap() here, the machineLocalityMap can be stale.
	// This is ok as long as we do not arbitrarily validate if machine team satisfies replication policy.

	if (server_info[removedServer]->wrongStoreTypeToRemove.get()) {
		if (wrongStoreTypeRemover.isReady()) {
			wrongStoreTypeRemover = removeWrongStoreType();
			addActor.send(wrongStoreTypeRemover);
		}
	}

	// Step: Remove removedServer from server's global data
	for (int s = 0; s < allServers.size(); s++) {
		if (allServers[s] == removedServer) {
			allServers[s--] = allServers.back();
			allServers.pop_back();
		}
	}
	server_info.erase(removedServer);
	server_and_tss_info.erase(removedServer);

	if (server_status.get(removedServer).initialized && server_status.get(removedServer).isUnhealthy()) {
		unhealthyServers--;
	}
	server_status.clear(removedServer);

	// FIXME: add remove support to localitySet so we do not have to recreate it
	resetLocalitySet();

	doBuildTeams = true;
	restartTeamBuilder.trigger();

	TraceEvent("DataDistributionTeamCollectionUpdate", distributorId)
	    .detail("ServerTeams", teams.size())
	    .detail("BadServerTeams", badTeams.size())
	    .detail("Servers", allServers.size())
	    .detail("Machines", machine_info.size())
	    .detail("MachineTeams", machineTeams.size())
	    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER);
}

Future<Void> DDTeamCollection::excludeStorageServersForWiggle(const UID& id) {
	Future<Void> moveFuture = Void();
	if (this->server_info.contains(id)) {
		auto& info = server_info.at(id);
		AddressExclusion addr(info->getLastKnownInterface().address().ip, info->getLastKnownInterface().address().port);

		// don't overwrite the value set by actor trackExcludedServer
		bool abnormal =
		    this->excludedServers.count(addr) && this->excludedServers.get(addr) != DDTeamCollection::Status::NONE;

		if (info->getLastKnownInterface().secondaryAddress().present()) {
			AddressExclusion addr2(info->getLastKnownInterface().secondaryAddress().get().ip,
			                       info->getLastKnownInterface().secondaryAddress().get().port);
			abnormal |= this->excludedServers.count(addr2) &&
			            this->excludedServers.get(addr2) != DDTeamCollection::Status::NONE;
		}

		if (!abnormal) {
			this->wiggleAddresses.push_back(addr);
			this->excludedServers.set(addr, DDTeamCollection::Status::WIGGLING);
			moveFuture = info->onRemoved;
			this->restartRecruiting.trigger();
		}
	}
	return moveFuture;
}

void DDTeamCollection::includeStorageServersForWiggle() {
	bool included = false;
	for (auto& address : this->wiggleAddresses) {
		if (!this->excludedServers.count(address) ||
		    this->excludedServers.get(address) != DDTeamCollection::Status::WIGGLING) {
			continue;
		}
		included = true;
		this->excludedServers.set(address, DDTeamCollection::Status::NONE);
	}
	this->wiggleAddresses.clear();
	if (included) {
		this->restartRecruiting.trigger();
	}
}

int DDTeamCollection::numExistingSSOnAddr(const AddressExclusion& addr) const {
	int numExistingSS = 0;
	for (auto& server : server_and_tss_info) {
		const NetworkAddress& netAddr = server.second->getLastKnownInterface().stableAddress();
		AddressExclusion usedAddr(netAddr.ip, netAddr.port);
		if (usedAddr == addr) {
			++numExistingSS;
		}
	}

	return numExistingSS;
}

bool DDTeamCollection::exclusionSafetyCheck(std::vector<UID>& excludeServerIDs) {
	std::sort(excludeServerIDs.begin(), excludeServerIDs.end());
	for (const auto& team : teams) {
		std::vector<UID> teamServerIDs = team->getServerIDs();
		std::sort(teamServerIDs.begin(), teamServerIDs.end());
		TraceEvent(SevDebug, "DDExclusionSafetyCheck", distributorId)
		    .detail("Excluding", describe(excludeServerIDs))
		    .detail("Existing", team->getDesc());
		// Find size of set intersection of both vectors and see if the leftover team is valid
		std::vector<UID> intersectSet(teamServerIDs.size());
		auto it = std::set_intersection(excludeServerIDs.begin(),
		                                excludeServerIDs.end(),
		                                teamServerIDs.begin(),
		                                teamServerIDs.end(),
		                                intersectSet.begin());
		intersectSet.resize(it - intersectSet.begin());
		if (teamServerIDs.size() - intersectSet.size() < SERVER_KNOBS->DD_EXCLUDE_MIN_REPLICAS) {
			return false;
		}
	}
	return true;
}

std::pair<StorageWiggler::State, double> DDTeamCollection::getStorageWigglerState() const {
	if (storageWiggler) {
		return { storageWiggler->getWiggleState(), storageWiggler->lastStateChangeTs };
	}
	return { StorageWiggler::INVALID, 0.0 };
}

Future<Void> DDTeamCollection::run(Reference<DDTeamCollection> teamCollection,
                                   Reference<InitialDataDistribution> initData,
                                   TeamCollectionInterface tci,
                                   Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
                                   DDEnabledState const& ddEnabledState) {
	return DDTeamCollectionImpl::run(teamCollection, initData, tci, recruitStorage, &ddEnabledState);
}

Future<Void> DDTeamCollection::printSnapshotTeamsInfo(Reference<DDTeamCollection> self) {
	return DDTeamCollectionImpl::printSnapshotTeamsInfo(self);
}

class DDTeamCollectionUnitTest {
public:
	static std::unique_ptr<DDTeamCollection> testTeamCollection(
	    int teamSize,
	    Reference<IReplicationPolicy> policy,
	    int processCount,
	    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) {
		Database database = DatabaseContext::create(
		    makeReference<AsyncVar<ClientDBInfo>>(), Never(), LocalityData(), EnableLocalityLoadBalance::False);
		auto txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(database));
		DatabaseConfiguration conf;
		conf.storageTeamSize = teamSize;
		conf.storagePolicy = policy;

		auto collection = std::unique_ptr<DDTeamCollection>(
		    new DDTeamCollection(DDTeamCollectionInitParams{ txnProcessor,
		                                                     UID(0, 0),
		                                                     MoveKeysLock(),
		                                                     PromiseStream<RelocateShard>(),
		                                                     shardsAffectedByTeamFailure,
		                                                     conf,
		                                                     {},
		                                                     {},
		                                                     Future<Void>(Void()),
		                                                     makeReference<AsyncVar<bool>>(true),
		                                                     IsPrimary::True,
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     PromiseStream<GetMetricsRequest>(),
		                                                     Promise<UID>(),
		                                                     PromiseStream<Promise<int>>(),
		                                                     PromiseStream<Promise<int64_t>>(),
		                                                     PromiseStream<RebalanceStorageQueueRequest>() }));

		for (int id = 1; id <= processCount; ++id) {
			UID uid(id, 0);
			StorageServerInterface interface;
			interface.uniqueID = uid;
			interface.locality.set("machineid"_sr, Standalone<StringRef>(std::to_string(id)));
			interface.locality.set("zoneid"_sr, Standalone<StringRef>(std::to_string(id % 5)));
			interface.locality.set("data_hall"_sr, Standalone<StringRef>(std::to_string(id % 3)));
			collection->server_info[uid] = makeReference<TCServerInfo>(
			    interface, collection.get(), ProcessClass(), true, collection->storageServerSet);
			collection->server_status.set(uid, ServerStatus(false, false, false, interface.locality));
			collection->checkAndCreateMachine(collection->server_info[uid]);
		}

		return collection;
	}

	static std::unique_ptr<DDTeamCollection> testTeamCollection(int teamSize,
	                                                            Reference<IReplicationPolicy> policy,
	                                                            int processCount) {
		Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure =
		    makeReference<ShardsAffectedByTeamFailure>();
		return testTeamCollection(teamSize, policy, processCount, shardsAffectedByTeamFailure);
	}

	static std::unique_ptr<DDTeamCollection> testMachineTeamCollection(int teamSize,
	                                                                   Reference<IReplicationPolicy> policy,
	                                                                   int processCount) {
		Database database = DatabaseContext::create(
		    makeReference<AsyncVar<ClientDBInfo>>(), Never(), LocalityData(), EnableLocalityLoadBalance::False);
		auto txnProcessor = Reference<IDDTxnProcessor>(new DDTxnProcessor(database));
		DatabaseConfiguration conf;
		conf.storageTeamSize = teamSize;
		conf.storagePolicy = policy;

		auto collection = std::unique_ptr<DDTeamCollection>(
		    new DDTeamCollection(DDTeamCollectionInitParams{ txnProcessor,
		                                                     UID(0, 0),
		                                                     MoveKeysLock(),
		                                                     PromiseStream<RelocateShard>(),
		                                                     makeReference<ShardsAffectedByTeamFailure>(),
		                                                     conf,
		                                                     {},
		                                                     {},
		                                                     Future<Void>(Void()),
		                                                     makeReference<AsyncVar<bool>>(true),
		                                                     IsPrimary::True,
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     makeReference<AsyncVar<bool>>(false),
		                                                     PromiseStream<GetMetricsRequest>(),
		                                                     Promise<UID>(),
		                                                     PromiseStream<Promise<int>>(),
		                                                     PromiseStream<Promise<int64_t>>(),
		                                                     PromiseStream<RebalanceStorageQueueRequest>() }));

		for (int id = 1; id <= processCount; id++) {
			UID uid(id, 0);
			StorageServerInterface interface;
			interface.uniqueID = uid;
			int process_id = id;
			int dc_id = process_id / 1000;
			int data_hall_id = process_id / 100;
			int zone_id = process_id / 10;
			int machine_id = process_id / 5;

			printf("testMachineTeamCollection: process_id:%d zone_id:%d machine_id:%d ip_addr:%s\n",
			       process_id,
			       zone_id,
			       machine_id,
			       interface.address().toString().c_str());
			interface.locality.set("processid"_sr, Standalone<StringRef>(std::to_string(process_id)));
			interface.locality.set("machineid"_sr, Standalone<StringRef>(std::to_string(machine_id)));
			interface.locality.set("zoneid"_sr, Standalone<StringRef>(std::to_string(zone_id)));
			interface.locality.set("data_hall"_sr, Standalone<StringRef>(std::to_string(data_hall_id)));
			interface.locality.set("dcid"_sr, Standalone<StringRef>(std::to_string(dc_id)));
			collection->server_info[uid] = makeReference<TCServerInfo>(
			    interface, collection.get(), ProcessClass(), true, collection->storageServerSet);

			collection->server_status.set(uid, ServerStatus(false, false, false, interface.locality));
		}

		int totalServerIndex = collection->constructMachinesFromServers();
		printf("testMachineTeamCollection: construct machines for %d servers\n", totalServerIndex);

		return collection;
	}

	ACTOR static Future<Void> AddTeamsBestOf_UseMachineID() {
		wait(Future<Void>(Void()));

		int teamSize = 3; // replication size
		int processSize = 60;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
		    new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		state std::unique_ptr<DDTeamCollection> collection = testMachineTeamCollection(teamSize, policy, processSize);

		collection->addTeamsBestOf(30, desiredTeams, maxTeams);

		ASSERT(collection->sanityCheckTeams() == true);

		return Void();
	}

	ACTOR static Future<Void> AddTeamsBestOf_NotUseMachineID() {
		wait(Future<Void>(Void()));

		int teamSize = 3; // replication size
		int processSize = 60;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
		    new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
		state std::unique_ptr<DDTeamCollection> collection = testMachineTeamCollection(teamSize, policy, processSize);

		if (collection == nullptr) {
			fprintf(stderr, "collection is null\n");
			return Void();
		}

		collection->addBestMachineTeams(30); // Create machine teams to help debug
		collection->addTeamsBestOf(30, desiredTeams, maxTeams);
		collection->sanityCheckTeams(); // Server team may happen to be on the same machine team, although unlikely

		return Void();
	}

	static void AddAllTeams_isExhaustive() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		int processSize = 10;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		std::unique_ptr<DDTeamCollection> collection = testTeamCollection(3, policy, processSize);

		int result = collection->addTeamsBestOf(200, desiredTeams, maxTeams);

		// The maximum number of available server teams without considering machine locality is 120
		// The maximum number of available server teams with machine locality constraint is 120 - 40, because
		// the 40 (5*4*2) server teams whose servers come from the same machine are invalid.
		ASSERT(result == 80);
	}

	static void AddAllTeams_withLimit() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		int processSize = 10;
		int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

		std::unique_ptr<DDTeamCollection> collection = testTeamCollection(3, policy, processSize);

		int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

		ASSERT(result >= 10);
	}

	ACTOR static Future<Void> AddTeamsBestOf_SkippingBusyServers() {
		wait(Future<Void>(Void()));
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 10;
		state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		state int teamSize = 3;
		// state int targetTeamsPerServer = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (teamSize + 1) / 2;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);

		state int result = collection->addTeamsBestOf(8, desiredTeams, maxTeams);

		ASSERT(result >= 8);

		for (const auto& [serverID, server] : collection->server_info) {
			auto teamCount = server->getTeams().size();
			ASSERT(teamCount >= 1);
			// ASSERT(teamCount <= targetTeamsPerServer);
		}

		return Void();
	}

	// Due to the randomness in choosing the machine team and the server team from the machine team, it is possible that
	// we may not find the remaining several (e.g., 1 or 2) available teams.
	// It is hard to conclude what is the minimum number of  teams the addTeamsBestOf() should create in this situation.
	ACTOR static Future<Void> AddTeamsBestOf_NotEnoughServers() {
		wait(Future<Void>(Void()));
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
		state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);

		collection->addBestMachineTeams(10);
		int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

		if (collection->machineTeams.size() != 10 || result != 8) {
			collection->traceAllInfo(true); // Debug message
		}

		// NOTE: Due to the pure randomness in selecting a machine for a machine team,
		// we cannot guarantee that all machine teams are created.
		// When we change the selectReplicas function to achieve such guarantee, we can enable the following ASSERT
		ASSERT(collection->machineTeams.size() == 10); // Should create all machine teams

		// We need to guarantee a server always have at least a team so that the server can participate in data
		// distribution
		for (const auto& [serverID, server] : collection->server_info) {
			auto teamCount = server->getTeams().size();
			ASSERT(teamCount >= 1);
		}

		// If we find all available teams, result will be 8 because we prebuild 2 teams
		ASSERT(result == 8);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_NewServersNotNeeded() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		/*
		 * Suppose  1, 2 and 3 are complete sources, i.e., they have all shards in
		 * the key range being considered for movement. If the caller says that they
		 * don't strictly need new servers and all of these servers are healthy,
		 * maintain status quo.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_COMPLETE_SRCS,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_HealthyCompleteSource() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * Suppose  1, 2, 3 and 4 are complete sources, i.e., they have all shards in
		 * the key range being considered for movement. If the caller says that they don't
		 * strictly need new servers but '1' is not healthy, see that the other team of
		 * complete sources is selected.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0), UID(4, 0) };

		state GetTeamRequest req(TeamSelect::WANT_COMPLETE_SRCS,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(2, 0), UID(3, 0), UID(4, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());

		ASSERT(expectedServers == selectedServers);
		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestLeastUtilized() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		/*
		 * Among server teams that have healthy space available, pick the team that is
		 * least utilized, if the caller says they preferLowerDiskUtil.
		 */

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(2, 0), UID(3, 0), UID(4, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestMostUtilized() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		/*
		 * Among server teams that have healthy space available, pick the team that is
		 * most utilized, if the caller says they don't preferLowerDiskUtil.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::False,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_ServerUtilizationBelowCutoff() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply low_avail;
		low_avail.capacity.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 20;
		low_avail.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE / 2;
		low_avail.load.bytes = 90 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 2000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(high_avail);
		collection->server_info[UID(2, 0)]->setMetrics(low_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(low_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * If the only available team is one where at least one server is low on
		 * space, decline to pick that team. Every server must have some minimum
		 * free space defined by the MIN_AVAILABLE_SPACE server knob.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		ASSERT(!resTeam.present());

		return Void();
	}

	ACTOR static Future<Void> GetTeam_ServerUtilizationNearCutoff() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		GetStorageMetricsReply low_avail;
		if (SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO > 0) {
			/* Pick a capacity where MIN_AVAILABLE_SPACE_RATIO of the capacity would be higher than MIN_AVAILABLE_SPACE
			 */
			low_avail.capacity.bytes =
			    SERVER_KNOBS->MIN_AVAILABLE_SPACE * (2 / SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO);
		} else {
			low_avail.capacity.bytes = 2000 * 1024 * 1024;
		}
		low_avail.available.bytes = (SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * 1.1) * low_avail.capacity.bytes;
		low_avail.load.bytes = 90 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 2000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(3, 0), UID(4, 0), UID(5, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(high_avail);
		collection->server_info[UID(2, 0)]->setMetrics(low_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(low_avail);
		collection->server_info[UID(5, 0)]->setMetrics(high_avail);
		collection->server_info[UID(1, 0)]->markTeamUnhealthy(0);

		/*
		 * If the only available team is one where all servers are low on space,
		 * test that each server has at least MIN_AVAILABLE_SPACE_RATIO (server knob)
		 * percentage points of capacity free before picking that team.
		 */
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto& [resTeam, srcTeamFound] = req.reply.getFuture().get();

		ASSERT(!resTeam.present());

		return Void();
	}

	ACTOR static Future<Void> GetTeam_TrueBestLeastReadBandwidth() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(1, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 1;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);

		int64_t capacity = 1000 * 1024 * 1024, available = 800 * 1024 * 1024;
		std::vector<int64_t> read_bandwidths{
			300 * 1024 * 1024, 100 * 1024 * 1024, 500 * 1024 * 1024, 100 * 1024 * 1024, 900 * 1024 * 1024
		};
		std::vector<int64_t> load_bytes{
			50 * 1024 * 1024, 600 * 1024 * 1024, 800 * 1024 * 1024, 200 * 1024 * 1024, 100 * 1024 * 1024
		};
		GetStorageMetricsReply metrics[5];
		for (int i = 0; i < 5; ++i) {
			metrics[i].capacity.bytes = capacity;
			metrics[i].available.bytes = available;
			metrics[i].load.bytesReadPerKSecond = read_bandwidths[i];
			metrics[i].load.bytes = load_bytes[i];
			collection->addTeam(std::set<UID>({ UID(i + 1, 0) }), IsInitialTeam::True);
			collection->server_info[UID(i + 1, 0)]->setMetrics(metrics[i]);
		}

		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		auto preferLowerDiskUtil = PreferLowerDiskUtil::True;
		auto teamMustHaveShards = TeamMustHaveShards::False;
		auto forReadBalance = ForReadBalance::True;
		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         preferLowerDiskUtil,
		                         teamMustHaveShards,
		                         PreferLowerReadUtil::True,
		                         PreferWithinShardLimit::False,
		                         forReadBalance);
		req.completeSources = completeSources;

		state GetTeamRequest reqHigh(TeamSelect::WANT_TRUE_BEST,
		                             PreferLowerDiskUtil::False,
		                             teamMustHaveShards,
		                             PreferLowerReadUtil::False,
		                             PreferWithinShardLimit::False,
		                             forReadBalance);

		wait(collection->getTeam(req) && collection->getTeam(reqHigh));
		auto [resTeam, resTeamSrcFound] = req.reply.getFuture().get();
		auto [resTeamHigh, resTeamHighSrcFound] = reqHigh.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(4, 0) };
		std::set<UID> expectedServersHigh{ UID(5, 0) };

		ASSERT(resTeam.present());
		ASSERT(resTeamHigh.present());
		auto servers = resTeam.get()->getServerIDs(), serversHigh = resTeamHigh.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end()),
		    selectedServersHigh(serversHigh.begin(), serversHigh.end());
		// for (auto id : selectedServers)
		// 	std::cout << id.toString() << std::endl;
		ASSERT(expectedServers == selectedServers);
		ASSERT(expectedServersHigh == selectedServersHigh);

		resTeam.get()->addReadInFlightToTeam(50);
		req.reply.reset();
		wait(collection->getTeam(req));
		auto [resTeam1, resTeam1Found] = req.reply.getFuture().get();
		std::set<UID> expectedServers1{ UID(2, 0) };
		auto servers1 = resTeam1.get()->getServerIDs();
		const std::set<UID> selectedServers1(servers1.begin(), servers1.end());
		ASSERT(expectedServers1 == selectedServers1);

		return Void();
	}

	ACTOR static Future<Void> GetTeam_DeprioritizeWigglePausedTeam() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);
		GetStorageMetricsReply mid_avail;
		mid_avail.capacity.bytes = 1000 * 1024 * 1024;
		mid_avail.available.bytes = 400 * 1024 * 1024;
		mid_avail.load.bytes = 100 * 1024 * 1024;

		GetStorageMetricsReply high_avail;
		high_avail.capacity.bytes = 1000 * 1024 * 1024;
		high_avail.available.bytes = 800 * 1024 * 1024;
		high_avail.load.bytes = 90 * 1024 * 1024;

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		collection->server_info[UID(1, 0)]->setMetrics(mid_avail);
		collection->server_info[UID(2, 0)]->setMetrics(high_avail);
		collection->server_info[UID(3, 0)]->setMetrics(high_avail);
		collection->server_info[UID(4, 0)]->setMetrics(high_avail);

		collection->wigglingId = UID(4, 0);
		collection->pauseWiggle = makeReference<AsyncVar<bool>>(true);

		std::vector<UID> completeSources{ UID(1, 0), UID(2, 0), UID(3, 0) };

		state GetTeamRequest req(TeamSelect::WANT_TRUE_BEST,
		                         PreferLowerDiskUtil::True,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::False);
		req.completeSources = completeSources;

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(1, 0), UID(2, 0), UID(3, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}

	// Cut off high Cpu teams
	ACTOR static Future<Void> GetTeam_CutOffByCpu() {
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(1, "zoneid", makeReference<PolicyOne>());
		state int processSize = 4;
		state int teamSize = 1;
		state std::unique_ptr<DDTeamCollection> collection = testTeamCollection(teamSize, policy, processSize);
		state GetTeamRequest bestReq(TeamSelect::WANT_TRUE_BEST,
		                             PreferLowerDiskUtil::True,
		                             TeamMustHaveShards::False,
		                             PreferLowerReadUtil::True,
		                             PreferWithinShardLimit::False,
		                             ForReadBalance::True);
		state GetTeamRequest randomReq(TeamSelect::ANY,
		                               PreferLowerDiskUtil::True,
		                               TeamMustHaveShards::False,
		                               PreferLowerReadUtil::True,
		                               PreferWithinShardLimit::False,
		                               ForReadBalance::True);
		collection->teamPivots.lastPivotValuesUpdate = -100;

		int64_t capacity = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 20, loadBytes = 90 * 1024 * 1024;
		GetStorageMetricsReply high_s_high_r;
		high_s_high_r.capacity.bytes = capacity;
		high_s_high_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_high_r.load.bytes = loadBytes;
		high_s_high_r.load.opsReadPerKSecond = 7000 * 1000;

		GetStorageMetricsReply high_s_low_r;
		high_s_low_r.capacity.bytes = capacity;
		high_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE * 5;
		high_s_low_r.load.bytes = loadBytes;
		high_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		GetStorageMetricsReply low_s_low_r;
		low_s_low_r.capacity.bytes = capacity;
		low_s_low_r.available.bytes = SERVER_KNOBS->MIN_AVAILABLE_SPACE / 2;
		low_s_low_r.load.bytes = loadBytes;
		low_s_low_r.load.opsReadPerKSecond = 100 * 1000;

		HealthMetrics::StorageStats low_cpu, mid_cpu, high_cpu;
		// use constant cutoff value
		bool maxCutoff = deterministicRandom()->coinflip();
		if (!maxCutoff) {
			// use pivot value as cutoff
			auto ratio = KnobValueRef::create(double{ 0.7 });
			IKnobCollection::getMutableGlobalKnobCollection().setKnob("cpu_pivot_ratio", ratio);
		}
		low_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 60;
		mid_cpu.cpuUsage = SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 40;
		high_cpu.cpuUsage = maxCutoff ? SERVER_KNOBS->MAX_DEST_CPU_PERCENT + 1 : SERVER_KNOBS->MAX_DEST_CPU_PERCENT - 1;

		// high space, low cpu, high read (in pool)
		collection->addTeam(std::set<UID>({ UID(1, 0) }), IsInitialTeam::True);
		collection->server_info[UID(1, 0)]->setMetrics(high_s_high_r);
		collection->server_info[UID(1, 0)]->setStorageStats(low_cpu);
		// high space, mid cpu, low read (in pool)
		collection->addTeam(std::set<UID>({ UID(2, 0) }), IsInitialTeam::True);
		collection->server_info[UID(2, 0)]->setMetrics(high_s_low_r);
		collection->server_info[UID(2, 0)]->setStorageStats(mid_cpu);
		// low space, low cpu, low read (not in pool)
		collection->addTeam(std::set<UID>({ UID(3, 0) }), IsInitialTeam::True);
		collection->server_info[UID(3, 0)]->setMetrics(low_s_low_r);
		collection->server_info[UID(3, 0)]->setStorageStats(low_cpu);
		// high space, high cpu, low read (not in pool)
		collection->addTeam(std::set<UID>({ UID(4, 0) }), IsInitialTeam::True);
		collection->server_info[UID(4, 0)]->setMetrics(high_s_low_r);
		collection->server_info[UID(4, 0)]->setStorageStats(high_cpu);

		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		wait(collection->getTeam(bestReq));
		const auto [bestTeam, found1] = bestReq.reply.getFuture().get();
		fmt::print("{} {} {}\n",
		           SERVER_KNOBS->CPU_PIVOT_RATIO,
		           collection->teamPivots.pivotCPU,
		           collection->teamPivots.pivotAvailableSpaceRatio);
		ASSERT(bestTeam.present());
		ASSERT_EQ(bestTeam.get()->getServerIDs(), std::vector<UID>{ UID(2, 0) });

		wait(collection->getTeam(randomReq));
		const auto [randomTeam, found2] = randomReq.reply.getFuture().get();
		if (randomTeam.present()) {
			CODE_PROBE(true, "Unit Test Random Team Return Candidate.");
			ASSERT_NE(randomTeam.get()->getServerIDs(), std::vector<UID>{ UID(3, 0) });
			ASSERT_NE(randomTeam.get()->getServerIDs(), std::vector<UID>{ UID(4, 0) });
		}
		return Void();
	}

	ACTOR static Future<Void> GetTeam_PreferShardsWithinLimit() {
		ASSERT(SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM);
		Reference<IReplicationPolicy> policy = makeReference<PolicyAcross>(3, "zoneid", makeReference<PolicyOne>());
		state int processSize = 5;
		state int teamSize = 3;
		Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure =
		    makeReference<ShardsAffectedByTeamFailure>();
		state std::unique_ptr<DDTeamCollection> collection =
		    testTeamCollection(teamSize, policy, processSize, shardsAffectedByTeamFailure);

		collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), IsInitialTeam::True);
		collection->addTeam(std::set<UID>({ UID(2, 0), UID(3, 0), UID(4, 0) }), IsInitialTeam::True);
		collection->disableBuildingTeams();
		collection->setCheckTeamDelay();

		// Assign more than SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM shards to the first team.
		std::vector<Key> randomKeys;
		for (int i = 0; i < SERVER_KNOBS->DESIRED_MAX_SHARDS_PER_TEAM + 10; ++i) {
			Key randomKey = Key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, 1500)));
			randomKeys.push_back(randomKey);
		}
		std::sort(randomKeys.begin(), randomKeys.end());
		ShardsAffectedByTeamFailure::Team highShardTeam({ UID(1, 0), UID(2, 0), UID(3, 0) }, true);
		ASSERT(!randomKeys.empty());
		for (int i = 0; i < randomKeys.size() - 1; ++i) {
			shardsAffectedByTeamFailure->assignRangeToTeams(KeyRangeRef(randomKeys[i], randomKeys[i + 1]),
			                                                { highShardTeam });
		}

		// Call getTeam and check that the second team should always be the selected team.
		state GetTeamRequest req(deterministicRandom()->coinflip() ? TeamSelect::WANT_TRUE_BEST : TeamSelect::ANY,
		                         PreferLowerDiskUtil::False,
		                         TeamMustHaveShards::False,
		                         PreferLowerReadUtil::False,
		                         PreferWithinShardLimit::True);

		wait(collection->getTeam(req));

		const auto [resTeam, srcFound] = req.reply.getFuture().get();

		std::set<UID> expectedServers{ UID(2, 0), UID(3, 0), UID(4, 0) };
		ASSERT(resTeam.present());
		auto servers = resTeam.get()->getServerIDs();
		const std::set<UID> selectedServers(servers.begin(), servers.end());
		ASSERT(expectedServers == selectedServers);

		return Void();
	}
};

TEST_CASE("DataDistribution/AddTeamsBestOf/UseMachineID") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_UseMachineID());
	return Void();
}

TEST_CASE("DataDistribution/AddTeamsBestOf/NotUseMachineID") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_NotUseMachineID());
	return Void();
}

TEST_CASE("DataDistribution/AddAllTeams/isExhaustive") {
	DDTeamCollectionUnitTest::AddAllTeams_isExhaustive();
	return Void();
}

TEST_CASE("/DataDistribution/AddAllTeams/withLimit") {
	DDTeamCollectionUnitTest::AddAllTeams_withLimit();
	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/SkippingBusyServers") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_SkippingBusyServers());
	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/NotEnoughServers") {
	wait(DDTeamCollectionUnitTest::AddTeamsBestOf_NotEnoughServers());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/NewServersNotNeeded") {
	wait(DDTeamCollectionUnitTest::GetTeam_NewServersNotNeeded());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/HealthyCompleteSource") {
	wait(DDTeamCollectionUnitTest::GetTeam_HealthyCompleteSource());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestLeastUtilized") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestLeastUtilized());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestMostUtilized") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestMostUtilized());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/ServerUtilizationBelowCutoff") {
	wait(DDTeamCollectionUnitTest::GetTeam_ServerUtilizationBelowCutoff());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/ServerUtilizationNearCutoff") {
	wait(DDTeamCollectionUnitTest::GetTeam_ServerUtilizationNearCutoff());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/TrueBestLeastReadBandwidth") {
	wait(DDTeamCollectionUnitTest::GetTeam_TrueBestLeastReadBandwidth());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/DeprioritizeWigglePausedTeam") {
	wait(DDTeamCollectionUnitTest::GetTeam_DeprioritizeWigglePausedTeam());
	return Void();
}

TEST_CASE("/DataDistribution/StorageWiggler/NextIdWithMinAge") {
	state Reference<StorageWiggler> wiggler = makeReference<StorageWiggler>(nullptr);
	state double startTime = now();
	wiggler->addServer(UID(1, 0),
	                   StorageMetadataType(startTime - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 5.0,
	                                       KeyValueStoreType::SSD_BTREE_V2));
	wiggler->addServer(UID(2, 0),
	                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC,
	                                       KeyValueStoreType::MEMORY,
	                                       true));
	wiggler->addServer(UID(3, 0), StorageMetadataType(startTime - 5.0, KeyValueStoreType::SSD_ROCKSDB_V1, true));
	wiggler->addServer(UID(4, 0),
	                   StorageMetadataType(startTime - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC - 1.0,
	                                       KeyValueStoreType::SSD_BTREE_V2));
	std::vector<Optional<UID>> correctResult{ UID(3, 0), UID(2, 0), UID(4, 0), Optional<UID>() };
	for (int i = 0; i < 4; ++i) {
		auto id = wiggler->getNextServerId();
		ASSERT(id == correctResult[i]);
	}

	{
		std::cout << "Finish Initial Check. Start test getNextWigglingServerID() loop...\n";
		// test the getNextWigglingServerID() loop
		UID id = wait(DDTeamCollectionImpl::getNextWigglingServerID(wiggler));
		ASSERT(id == UID(1, 0));
	}

	std::cout << "Test after addServer() ...\n";
	state Future<UID> nextFuture = DDTeamCollectionImpl::getNextWigglingServerID(wiggler);
	ASSERT(!nextFuture.isReady());
	startTime = now();
	StorageMetadataType metadata(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 100.0,
	                             KeyValueStoreType::SSD_BTREE_V2);
	wiggler->addServer(UID(5, 0), metadata);
	ASSERT(!nextFuture.isReady());

	std::cout << "Test after updateServer() ...\n";
	StorageWiggler* ptr = wiggler.getPtr();
	wait(trigger(
	    [ptr]() {
		    ptr->updateMetadata(UID(5, 0),
		                        StorageMetadataType(now() - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC,
		                                            KeyValueStoreType::SSD_BTREE_V2));
	    },
	    delay(5.0)));
	wait(success(nextFuture));
	ASSERT(now() - startTime < SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 100.0);
	ASSERT(nextFuture.get() == UID(5, 0));
	return Void();
}

TEST_CASE("/DataDistribution/StorageWiggler/NextIdWithTSS") {
	state std::unique_ptr<DDTeamCollection> collection =
	    DDTeamCollectionUnitTest::testMachineTeamCollection(1, Reference<IReplicationPolicy>(new PolicyOne()), 5);
	state Reference<StorageWiggler> wiggler = makeReference<StorageWiggler>(collection.get());

	std::cout << "Test when need TSS ... \n";
	collection->configuration.usableRegions = 1;
	collection->configuration.desiredTSSCount = 1;
	state double startTime = now();
	wiggler->addServer(UID(1, 0),
	                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0,
	                                       KeyValueStoreType::SSD_BTREE_V2));
	wiggler->addServer(UID(2, 0),
	                   StorageMetadataType(startTime + SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0,
	                                       KeyValueStoreType::SSD_BTREE_V2));
	ASSERT(!wiggler->getNextServerId(true).present());
	ASSERT(wiggler->getNextServerId(collection->reachTSSPairTarget()) == UID(1, 0));
	UID id = wait(DDTeamCollectionImpl::getNextWigglingServerID(wiggler, {}, collection.get()));
	ASSERT(now() - startTime < SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC + 150.0);
	ASSERT(id == UID(2, 0));
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/CutOffByCpu") {
	wait(DDTeamCollectionUnitTest::GetTeam_CutOffByCpu());
	return Void();
}

TEST_CASE("/DataDistribution/GetTeam/PreferWithinShardRange") {
	if (!SERVER_KNOBS->ENFORCE_SHARD_COUNT_PER_TEAM) {
		return Void();
	}
	wait(DDTeamCollectionUnitTest::GetTeam_PreferShardsWithinLimit());
	return Void();
}