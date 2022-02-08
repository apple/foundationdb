/*
 * DDTeamCollection.h
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

#pragma once

#include <set>
#include <sstream>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbrpc/Replication.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TCInfo.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/BooleanParam.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class TCTeamInfo;
class TCMachineInfo;
class TCMachineTeamInfo;

FDB_DECLARE_BOOLEAN_PARAM(IsPrimary);

// All state that represents an ongoing tss pair recruitment
struct TSSPairState : ReferenceCounted<TSSPairState>, NonCopyable {
	Promise<Optional<std::pair<UID, Version>>>
	    ssPairInfo; // if set, for ss to pass its id to tss pair once it is successfully recruited
	Promise<bool> tssPairDone; // if set, for tss to pass ss that it was successfully recruited
	Promise<Void> complete;

	Optional<Key> dcId; // dc
	Optional<Key> dataHallId; // data hall

	bool active;

	TSSPairState() : active(false) {}

	TSSPairState(const LocalityData& locality)
	  : dcId(locality.dcId()), dataHallId(locality.dataHallId()), active(true) {}

	bool inDataZone(const LocalityData& locality) {
		return locality.dcId() == dcId && locality.dataHallId() == dataHallId;
	}

	void cancel() {
		// only cancel if both haven't been set, otherwise one half of pair could think it was successful but the other
		// half would think it failed
		if (active && ssPairInfo.canBeSet() && tssPairDone.canBeSet()) {
			ssPairInfo.send(Optional<std::pair<UID, Version>>());
			// callback of ssPairInfo could have cancelled tssPairDone already, so double check before cancelling
			if (tssPairDone.canBeSet()) {
				tssPairDone.send(false);
			}
			if (complete.canBeSet()) {
				complete.send(Void());
			}
		}
	}

	bool tssRecruitSuccess() {
		if (active && tssPairDone.canBeSet()) {
			tssPairDone.send(true);
			return true;
		}
		return false;
	}

	bool tssRecruitFailed() {
		if (active && tssPairDone.canBeSet()) {
			tssPairDone.send(false);
			return true;
		}
		return false;
	}

	bool ssRecruitSuccess(std::pair<UID, Version> ssInfo) {
		if (active && ssPairInfo.canBeSet()) {
			ssPairInfo.send(Optional<std::pair<UID, Version>>(ssInfo));
			return true;
		}
		return false;
	}

	bool ssRecruitFailed() {
		if (active && ssPairInfo.canBeSet()) {
			ssPairInfo.send(Optional<std::pair<UID, Version>>());
			return true;
		}
		return false;
	}

	bool markComplete() {
		if (active && complete.canBeSet()) {
			complete.send(Void());
			return true;
		}
		return false;
	}

	Future<Optional<std::pair<UID, Version>>> waitOnSS() { return ssPairInfo.getFuture(); }

	Future<bool> waitOnTSS() { return tssPairDone.getFuture(); }

	Future<Void> waitComplete() { return complete.getFuture(); }
};

class ServerStatus {
public:
	bool isWiggling;
	bool isFailed;
	bool isUndesired;
	bool isWrongConfiguration;
	bool initialized; // AsyncMap erases default constructed objects
	LocalityData locality;
	ServerStatus()
	  : isWiggling(false), isFailed(true), isUndesired(false), isWrongConfiguration(false), initialized(false) {}
	ServerStatus(bool isFailed, bool isUndesired, bool isWiggling, LocalityData const& locality)
	  : isWiggling(isWiggling), isFailed(isFailed), isUndesired(isUndesired), isWrongConfiguration(false),
	    initialized(true), locality(locality) {}
	bool isUnhealthy() const { return isFailed || isUndesired; }
	const char* toString() const {
		return isFailed ? "Failed" : isUndesired ? "Undesired" : isWiggling ? "Wiggling" : "Healthy";
	}

	bool operator==(ServerStatus const& r) const {
		return isFailed == r.isFailed && isUndesired == r.isUndesired && isWiggling == r.isWiggling &&
		       isWrongConfiguration == r.isWrongConfiguration && locality == r.locality && initialized == r.initialized;
	}
	bool operator!=(ServerStatus const& r) const { return !(*this == r); }

	// If a process has reappeared without the storage server that was on it (isFailed == true), we don't need to
	// exclude it We also don't need to exclude processes who are in the wrong configuration (since those servers will
	// be removed)
	bool excludeOnRecruit() const { return !isFailed && !isWrongConfiguration; }
};
typedef AsyncMap<UID, ServerStatus> ServerStatusMap;

class DDTeamCollection : public ReferenceCounted<DDTeamCollection> {
	friend class DDTeamCollectionImpl;

public:
	// clang-format off
	enum class Status { NONE = 0, WIGGLING = 1, EXCLUDED = 2, FAILED = 3};

	// addActor: add to actorCollection so that when an actor has error, the ActorCollection can catch the error.
	// addActor is used to create the actorCollection when the dataDistributionTeamCollection is created
	PromiseStream<Future<Void>> addActor;
	Database cx;
	UID distributorId;
	DatabaseConfiguration configuration;

	bool doBuildTeams;
	bool lastBuildTeamsFailed;
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;
	AsyncVar<bool> waitUntilRecruited; // make teambuilder wait until one new SS is recruited

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	std::vector<UID> allServers;
	ServerStatusMap server_status;
	int64_t unhealthyServers;
	std::map<int,int> priority_teams;
	std::map<UID, Reference<TCServerInfo>> server_info;
	std::map<UID, Reference<TCServerInfo>> tss_info_by_pair;
	std::map<UID, Reference<TCServerInfo>> server_and_tss_info; // TODO could replace this with an efficient way to do a read-only concatenation of 2 data structures?
	std::map<Key, int> lagging_zones; // zone to number of storage servers lagging
	AsyncVar<bool> disableFailingLaggingServers;

	// storage wiggle info
	Reference<StorageWiggler> storageWiggler;
	std::vector<AddressExclusion> wiggleAddresses; // collection of wiggling servers' address
	Optional<UID> wigglingId; // Process id of current wiggling storage server;
	Reference<AsyncVar<bool>> pauseWiggle;
	Reference<AsyncVar<bool>> processingWiggle; // track whether wiggling relocation is being processed
	PromiseStream<StorageWiggleValue> nextWiggleInfo;

	// machine_info has all machines info; key must be unique across processes on the same machine
	std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // all machine teams
	LocalityMap<UID> machineLocalityMap; // locality info of machines

	std::vector<Reference<TCTeamInfo>> teams;
	std::vector<Reference<TCTeamInfo>> badTeams;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	PromiseStream<UID> removedServers;
	PromiseStream<UID> removedTSS;
	std::set<UID> recruitingIds; // The IDs of the SS/TSS which are being recruited
	std::set<NetworkAddress> recruitingLocalities;
	Future<Void> initialFailureReactionDelay;
	Future<Void> initializationDoneActor;
	Promise<Void> serverTrackerErrorOut;
	AsyncVar<int> recruitingStream;
	Debouncer restartRecruiting;

	int healthyTeamCount;
	Reference<AsyncVar<bool>> zeroHealthyTeams;

	int optimalTeamCount;
	AsyncVar<bool> zeroOptimalTeams;

	int bestTeamKeepStuckCount = 0;

	bool isTssRecruiting; // If tss recruiting is waiting on a pair, don't consider DD recruiting for the purposes of QuietDB

	// WIGGLING if an address is under storage wiggling.
	// EXCLUDED if an address is in the excluded list in the database.
	// FAILED if an address is permanently failed.
	// NONE by default.  Updated asynchronously (eventually)
	AsyncMap< AddressExclusion, Status > excludedServers;

	std::set<AddressExclusion> invalidLocalityAddr; // These address have invalidLocality for the configured storagePolicy

	std::vector<Optional<Key>> includedDCs;
	Optional<std::vector<Optional<Key>>> otherTrackedDCs;
	bool primary;
	Reference<AsyncVar<bool>> processingUnhealthy;
	Future<Void> readyToStart;
	Future<Void> checkTeamDelay;
	Promise<Void> addSubsetComplete;
	Future<Void> badTeamRemover;
	Future<Void> checkInvalidLocalities;

	Future<Void> wrongStoreTypeRemover;

	Reference<LocalitySet> storageServerSet;

	std::vector<DDTeamCollection*> teamCollections;
	AsyncVar<Optional<Key>> healthyZone;
	Future<bool> clearHealthyZoneFuture;
	double medianAvailableSpace;
	double lastMedianAvailableSpaceUpdate;
	// clang-format on

	int lowestUtilizationTeam;
	int highestUtilizationTeam;

	AsyncTrigger printDetailedTeamsInfo;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<Promise<int>> getUnhealthyRelocationCount;
	Promise<UID> removeFailedServer;

	Reference<EventCacheHolder> ddTrackerStartingEventHolder;
	Reference<EventCacheHolder> teamCollectionInfoEventHolder;
	Reference<EventCacheHolder> storageServerRecruitmentEventHolder;

	void resetLocalitySet() {
		storageServerSet = Reference<LocalitySet>(new LocalityMap<UID>());
		LocalityMap<UID>* storageServerMap = (LocalityMap<UID>*)storageServerSet.getPtr();

		for (auto& it : server_info) {
			it.second->localityEntry = storageServerMap->add(it.second->lastKnownInterface.locality, &it.second->id);
		}
	}

	bool satisfiesPolicy(const std::vector<Reference<TCServerInfo>>& team, int amount = -1) const {
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

	DDTeamCollection(Database const& cx,
	                 UID distributorId,
	                 MoveKeysLock const& lock,
	                 PromiseStream<RelocateShard> const& output,
	                 Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
	                 DatabaseConfiguration configuration,
	                 std::vector<Optional<Key>> includedDCs,
	                 Optional<std::vector<Optional<Key>>> otherTrackedDCs,
	                 Future<Void> readyToStart,
	                 Reference<AsyncVar<bool>> zeroHealthyTeams,
	                 IsPrimary primary,
	                 Reference<AsyncVar<bool>> processingUnhealthy,
	                 Reference<AsyncVar<bool>> processingWiggle,
	                 PromiseStream<GetMetricsRequest> getShardMetrics,
	                 Promise<UID> removeFailedServer,
	                 PromiseStream<Promise<int>> getUnhealthyRelocationCount)
	  : cx(cx), distributorId(distributorId), configuration(configuration), doBuildTeams(true),
	    lastBuildTeamsFailed(false), teamBuilder(Void()), lock(lock), output(output), unhealthyServers(0),
	    storageWiggler(makeReference<StorageWiggler>(this)), processingWiggle(processingWiggle),
	    shardsAffectedByTeamFailure(shardsAffectedByTeamFailure),
	    initialFailureReactionDelay(
	        delayed(readyToStart, SERVER_KNOBS->INITIAL_FAILURE_REACTION_DELAY, TaskPriority::DataDistribution)),
	    initializationDoneActor(logOnCompletion(readyToStart && initialFailureReactionDelay)), recruitingStream(0),
	    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY), healthyTeamCount(0),
	    zeroHealthyTeams(zeroHealthyTeams), optimalTeamCount(0), zeroOptimalTeams(true), isTssRecruiting(false),
	    includedDCs(includedDCs), otherTrackedDCs(otherTrackedDCs), primary(primary),
	    processingUnhealthy(processingUnhealthy), readyToStart(readyToStart),
	    checkTeamDelay(delay(SERVER_KNOBS->CHECK_TEAM_DELAY, TaskPriority::DataDistribution)), badTeamRemover(Void()),
	    checkInvalidLocalities(Void()), wrongStoreTypeRemover(Void()), storageServerSet(new LocalityMap<UID>()),
	    clearHealthyZoneFuture(true), medianAvailableSpace(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO),
	    lastMedianAvailableSpaceUpdate(0), lowestUtilizationTeam(0), highestUtilizationTeam(0),
	    getShardMetrics(getShardMetrics), getUnhealthyRelocationCount(getUnhealthyRelocationCount),
	    removeFailedServer(removeFailedServer),
	    ddTrackerStartingEventHolder(makeReference<EventCacheHolder>("DDTrackerStarting")),
	    teamCollectionInfoEventHolder(makeReference<EventCacheHolder>("TeamCollectionInfo")),
	    storageServerRecruitmentEventHolder(
	        makeReference<EventCacheHolder>("StorageServerRecruitment_" + distributorId.toString())) {
		if (!primary || configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", distributorId)
			    .detail("State", "Inactive")
			    .trackLatest(ddTrackerStartingEventHolder->trackingKey);
		}
	}

	~DDTeamCollection() {
		TraceEvent("DDTeamCollectionDestructed", distributorId).detail("Primary", primary);

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
		// TraceEvent("DDTeamCollectionDestructed", distributorId)
		//     .detail("Primary", primary)
		//     .detail("BadTeamTrackerDestroyed", badTeams.size());
		// The following makes sure that, even if a reference to a team is held in the DD Queue, the tracker will be
		// stopped
		//  before the server_status map to which it has a pointer, is destroyed.
		for (auto& [_, info] : server_and_tss_info) {
			info->tracker.cancel();
			info->collection = nullptr;
		}

		storageWiggler->teamCollection = nullptr;
		// TraceEvent("DDTeamCollectionDestructed", distributorId)
		//    .detail("Primary", primary)
		//    .detail("ServerTrackerDestroyed", server_info.size());
	}

	void addLaggingStorageServer(Key zoneId) {
		lagging_zones[zoneId]++;
		if (lagging_zones.size() > std::max(1, configuration.storageTeamSize - 1) &&
		    !disableFailingLaggingServers.get())
			disableFailingLaggingServers.set(true);
	}

	void removeLaggingStorageServer(Key zoneId) {
		auto iter = lagging_zones.find(zoneId);
		ASSERT(iter != lagging_zones.end());
		iter->second--;
		ASSERT(iter->second >= 0);
		if (iter->second == 0)
			lagging_zones.erase(iter);
		if (lagging_zones.size() <= std::max(1, configuration.storageTeamSize - 1) &&
		    disableFailingLaggingServers.get())
			disableFailingLaggingServers.set(false);
	}

	Future<Void> logOnCompletion(Future<Void> signal);
	Future<Void> interruptableBuildTeams();
	Future<Void> checkBuildTeams();

	// Returns a random healthy team, which does not contain excludeServer.
	std::vector<UID> getRandomHealthyTeam(const UID& excludeServer) {
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
			// case, the team  will be possibily relocated to a healthy destination later by DD.
			std::vector<UID> servers =
			    teams[backup[deterministicRandom()->randomInt(0, backup.size())]]->getServerIDs();
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

	Future<Void> getTeam(GetTeamRequest);

	int64_t getDebugTotalDataInFlight() const {
		int64_t total = 0;
		for (auto itr = server_info.begin(); itr != server_info.end(); ++itr)
			total += itr->second->dataInFlightToServer;
		return total;
	}

	Future<Void> addSubsetOfEmergencyTeams();
	Future<Void> init(Reference<InitialDataDistribution> initTeams, DDEnabledState const* ddEnabledState);

	// Check if server or machine has a valid locality based on configured replication policy
	bool isValidLocality(Reference<IReplicationPolicy> storagePolicy, const LocalityData& locality) const {
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

	void evaluateTeamQuality() const {
		int teamCount = teams.size(), serverCount = allServers.size();
		double teamsPerServer = (double)teamCount * configuration.storageTeamSize / serverCount;

		ASSERT(serverCount == server_info.size());

		int minTeams = std::numeric_limits<int>::max();
		int maxTeams = std::numeric_limits<int>::min();
		double varTeams = 0;

		std::map<Optional<Standalone<StringRef>>, int> machineTeams;
		for (const auto& [id, info] : server_info) {
			if (!server_status.get(id).isUnhealthy()) {
				int stc = info->teams.size();
				minTeams = std::min(minTeams, stc);
				maxTeams = std::max(maxTeams, stc);
				varTeams += (stc - teamsPerServer) * (stc - teamsPerServer);
				// Use zoneId as server's machine id
				machineTeams[info->lastKnownInterface.locality.zoneId()] += stc;
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

	int overlappingMembers(const std::vector<UID>& team) const {
		if (team.empty()) {
			return 0;
		}

		int maxMatchingServers = 0;
		const UID& serverID = team[0];
		const auto it = server_info.find(serverID);
		ASSERT(it != server_info.end());
		const auto& usedTeams = it->second->teams;
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
			ASSERT(matchingServers > 0);
			maxMatchingServers = std::max(maxMatchingServers, matchingServers);
			if (maxMatchingServers == team.size()) {
				return maxMatchingServers;
			}
		}

		return maxMatchingServers;
	}

	int overlappingMachineMembers(std::vector<Standalone<StringRef>> const& team) const {
		if (team.empty()) {
			return 0;
		}

		int maxMatchingServers = 0;
		auto it = machine_info.find(team[0]);
		ASSERT(it != machine_info.end());
		auto const& machineTeams = it->second->machineTeams;
		for (auto const& usedTeam : machineTeams) {
			auto used = usedTeam->machineIDs;
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
			ASSERT(matchingServers > 0);
			maxMatchingServers = std::max(maxMatchingServers, matchingServers);
			if (maxMatchingServers == team.size()) {
				return maxMatchingServers;
			}
		}

		return maxMatchingServers;
	}

	Reference<TCMachineTeamInfo> findMachineTeam(std::vector<Standalone<StringRef>> const& machineIDs) const;

	// Assume begin to end is sorted by std::sort
	// Assume InputIt is iterator to UID
	// Note: We must allow creating empty teams because empty team is created when a remote DB is initialized.
	// The empty team is used as the starting point to move data to the remote DB
	// begin : the start of the team member ID
	// end : end of the team member ID
	// isIntialTeam : False when the team is added by addTeamsBestOf(); True otherwise, e.g.,
	// when the team added at init() when we recreate teams by looking up DB
	template <class InputIt>
	void addTeam(InputIt begin, InputIt end, bool isInitialTeam) {
		std::vector<Reference<TCServerInfo>> newTeamServers;
		for (auto i = begin; i != end; ++i) {
			if (server_info.find(*i) != server_info.end()) {
				newTeamServers.push_back(server_info[*i]);
			}
		}

		addTeam(newTeamServers, isInitialTeam);
	}

	void addTeam(const std::vector<Reference<TCServerInfo>>& newTeamServers,
	             bool isInitialTeam,
	             bool redundantTeam = false) {
		auto teamInfo = makeReference<TCTeamInfo>(newTeamServers);

		// Move satisfiesPolicy to the end for performance benefit
		bool badTeam = redundantTeam || teamInfo->size() != configuration.storageTeamSize ||
		               !satisfiesPolicy(teamInfo->getServers());

		teamInfo->tracker = teamTracker(teamInfo, badTeam, redundantTeam);
		// ASSERT( teamInfo->serverIDs.size() > 0 ); //team can be empty at DB initialization
		if (badTeam) {
			badTeams.push_back(teamInfo);
			return;
		}

		// For a good team, we add it to teams and create machine team for it when necessary
		teams.push_back(teamInfo);
		for (int i = 0; i < newTeamServers.size(); ++i) {
			newTeamServers[i]->teams.push_back(teamInfo);
		}

		// Find or create machine team for the server team
		// Add the reference of machineTeam (with machineIDs) into process team
		std::vector<Standalone<StringRef>> machineIDs;
		for (auto server = newTeamServers.begin(); server != newTeamServers.end(); ++server) {
			ASSERT_WE_THINK((*server)->machine.isValid());
			machineIDs.push_back((*server)->machine->machineID);
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
		machineTeamInfo->serverTeams.push_back(teamInfo);
		if (g_network->isSimulated()) {
			// Update server team information for consistency check in simulation
			traceTeamCollectionInfo();
		}
	}

	void addTeam(std::set<UID> const& team, bool isInitialTeam) { addTeam(team.begin(), team.end(), isInitialTeam); }

	// Add a machine team specified by input machines
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Reference<TCMachineInfo>> machines) {
		auto machineTeamInfo = makeReference<TCMachineTeamInfo>(machines);
		machineTeams.push_back(machineTeamInfo);

		// Assign machine teams to machine
		for (auto machine : machines) {
			// A machine's machineTeams vector should not hold duplicate machineTeam members
			ASSERT_WE_THINK(std::count(machine->machineTeams.begin(), machine->machineTeams.end(), machineTeamInfo) ==
			                0);
			machine->machineTeams.push_back(machineTeamInfo);
		}

		return machineTeamInfo;
	}

	// Add a machine team by using the machineIDs from begin to end
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Standalone<StringRef>>::iterator begin,
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

	// Group storage servers (process) based on their machineId in LocalityData
	// All created machines are healthy
	// Return The number of healthy servers we grouped into machines
	int constructMachinesFromServers() {
		int totalServerIndex = 0;
		for (auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				checkAndCreateMachine(i->second);
				totalServerIndex++;
			}
		}

		return totalServerIndex;
	}

	void traceConfigInfo() const {
		TraceEvent("DDConfig", distributorId)
		    .detail("StorageTeamSize", configuration.storageTeamSize)
		    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER)
		    .detail("MaxTeamsPerServer", SERVER_KNOBS->MAX_TEAMS_PER_SERVER)
		    .detail("StoreType", configuration.storageServerStoreType);
	}

	void traceServerInfo() const;

	void traceServerTeamInfo() const {
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

	void traceMachineInfo() const {
		int i = 0;

		TraceEvent("MachineInfo").detail("Size", machine_info.size());
		for (auto& machine : machine_info) {
			TraceEvent("MachineInfo", distributorId)
			    .detail("MachineInfoIndex", i++)
			    .detail("Healthy", isMachineHealthy(machine.second))
			    .detail("MachineID", machine.first.contents().toString())
			    .detail("MachineTeamOwned", machine.second->machineTeams.size())
			    .detail("ServerNumOnMachine", machine.second->serversOnMachine.size())
			    .detail("ServersID", machine.second->getServersIDStr());
		}
	}

	void traceMachineTeamInfo() const {
		int i = 0;

		TraceEvent("MachineTeamInfo", distributorId).detail("Size", machineTeams.size());
		for (auto& team : machineTeams) {
			TraceEvent("MachineTeamInfo", distributorId)
			    .detail("TeamIndex", i++)
			    .detail("MachineIDs", team->getMachineIDsStr())
			    .detail("ServerTeams", team->serverTeams.size());
		}
	}

	// Locality string is hashed into integer, used as KeyIndex
	// For better understand which KeyIndex is used for locality, we print this info in trace.
	void traceLocalityArrayIndexName() const {
		TraceEvent("LocalityRecordKeyName").detail("Size", machineLocalityMap._keymap->_lookuparray.size());
		for (int i = 0; i < machineLocalityMap._keymap->_lookuparray.size(); ++i) {
			TraceEvent("LocalityRecordKeyIndexName")
			    .detail("KeyIndex", i)
			    .detail("KeyName", machineLocalityMap._keymap->_lookuparray[i]);
		}
	}

	void traceMachineLocalityMap() const {
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

	// To enable verbose debug info, set shouldPrint to true
	void traceAllInfo(bool shouldPrint = false) const {

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

		TraceEvent("TraceAllInfo", distributorId).detail("Primary", primary);
		traceConfigInfo();
		traceServerInfo();
		traceServerTeamInfo();
		traceMachineInfo();
		traceMachineTeamInfo();
		traceLocalityArrayIndexName();
		traceMachineLocalityMap();
	}

	// We must rebuild machine locality map whenever the entry in the map is inserted or removed
	void rebuildMachineLocalityMap() {
		machineLocalityMap.clear();
		int numHealthyMachine = 0;
		for (auto machine = machine_info.begin(); machine != machine_info.end(); ++machine) {
			if (machine->second->serversOnMachine.empty()) {
				TraceEvent(SevWarn, "RebuildMachineLocalityMapError")
				    .detail("Machine", machine->second->machineID.toString())
				    .detail("NumServersOnMachine", 0);
				continue;
			}
			if (!isMachineHealthy(machine->second)) {
				continue;
			}
			Reference<TCServerInfo> representativeServer = machine->second->serversOnMachine[0];
			auto& locality = representativeServer->lastKnownInterface.locality;
			if (!isValidLocality(configuration.storagePolicy, locality)) {
				TraceEvent(SevWarn, "RebuildMachineLocalityMapError")
				    .detail("Machine", machine->second->machineID.toString())
				    .detail("InvalidLocality", locality.toString());
				continue;
			}
			const LocalityEntry& localityEntry = machineLocalityMap.add(locality, &representativeServer->id);
			machine->second->localityEntry = localityEntry;
			++numHealthyMachine;
		}
	}

	// Create machineTeamsToBuild number of machine teams
	// No operation if machineTeamsToBuild is 0
	// Note: The creation of machine teams should not depend on server teams:
	// No matter how server teams will be created, we will create the same set of machine teams;
	// We should never use server team number in building machine teams.
	//
	// Five steps to create each machine team, which are document in the function
	// Reuse ReplicationPolicy selectReplicas func to select machine team
	// return number of added machine teams
	int addBestMachineTeams(int machineTeamsToBuild) {
		int addedMachineTeams = 0;

		ASSERT(machineTeamsToBuild >= 0);
		// The number of machines is always no smaller than the storageTeamSize in a correct configuration
		ASSERT(machine_info.size() >= configuration.storageTeamSize);
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
				ASSERT_WE_THINK(server_info.find(machine.second->serversOnMachine[0]->id) != server_info.end());
				// Skip unhealthy machines
				if (!isMachineHealthy(machine.second))
					continue;
				// Skip machine with incomplete locality
				if (!isValidLocality(configuration.storagePolicy,
				                     machine.second->serversOnMachine[0]->lastKnownInterface.locality)) {
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

			// Step 4: Reuse Policy's selectReplicas() to create team for the representative process.
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
				auto success = machineLocalityMap.selectReplicas(configuration.storagePolicy, forcedAttributes, team);
				// NOTE: selectReplicas() should always return success when storageTeamSize = 1
				ASSERT_WE_THINK(configuration.storageTeamSize > 1 || (configuration.storageTeamSize == 1 && success));
				if (!success) {
					continue; // Try up to maxAttempts, since next time we may choose a different forcedAttributes
				}
				ASSERT(forcedAttributes.size() > 0);
				team.push_back((UID*)machineLocalityMap.getObject(forcedAttributes[0]));

				// selectReplicas() may NEVER return server not in server_info.
				for (auto& pUID : team) {
					ASSERT_WE_THINK(server_info.find(*pUID) != server_info.end());
				}

				// selectReplicas() should always return a team with correct size. otherwise, it has a bug
				ASSERT(team.size() == configuration.storageTeamSize);

				int score = 0;
				std::vector<Standalone<StringRef>> machineIDs;
				for (auto process = team.begin(); process != team.end(); process++) {
					Reference<TCServerInfo> server = server_info[**process];
					score += server->machine->machineTeams.size();
					Standalone<StringRef> machine_id = server->lastKnownInterface.locality.zoneId().get();
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
				traceAllInfo(true);
				TraceEvent(SevWarn, "DataDistributionBuildTeams", distributorId)
				    .detail("Primary", primary)
				    .detail("Reason", "Unable to make desired machine Teams");
				lastBuildTeamsFailed = true;
				break;
			}
		}

		return addedMachineTeams;
	}

	bool isMachineTeamHealthy(std::vector<Standalone<StringRef>> const& machineIDs) const;

	bool isMachineTeamHealthy(TCMachineTeamInfo const& machineTeam) const {
		int healthyNum = 0;

		// A healthy machine team should have the desired number of machines
		if (machineTeam.size() != configuration.storageTeamSize)
			return false;

		for (auto& machine : machineTeam.machines) {
			if (isMachineHealthy(machine)) {
				healthyNum++;
			}
		}
		return (healthyNum == machineTeam.machines.size());
	}

	bool isMachineHealthy(Reference<TCMachineInfo> const& machine) const {
		if (!machine.isValid() || machine_info.find(machine->machineID) == machine_info.end() ||
		    machine->serversOnMachine.empty()) {
			return false;
		}

		// Healthy machine has at least one healthy server
		for (auto& server : machine->serversOnMachine) {
			if (!server_status.get(server->id).isUnhealthy()) {
				return true;
			}
		}

		return false;
	}

	// Return the healthy server with the least number of correct-size server teams
	Reference<TCServerInfo> findOneLeastUsedServer() const {
		std::vector<Reference<TCServerInfo>> leastUsedServers;
		int minTeams = std::numeric_limits<int>::max();
		for (auto& server : server_info) {
			// Only pick healthy server, which is not failed or excluded.
			if (server_status.get(server.first).isUnhealthy())
				continue;
			if (!isValidLocality(configuration.storagePolicy, server.second->lastKnownInterface.locality))
				continue;

			int numTeams = server.second->teams.size();
			if (numTeams < minTeams) {
				minTeams = numTeams;
				leastUsedServers.clear();
			}
			if (minTeams == numTeams) {
				leastUsedServers.push_back(server.second);
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

	// Randomly choose one machine team that has chosenServer and has the correct size
	// When configuration is changed, we may have machine teams with old storageTeamSize
	Reference<TCMachineTeamInfo> findOneRandomMachineTeam(TCServerInfo const& chosenServer) const {
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
		    .detail("ServerID", chosenServer.id)
		    .detail("MachineTeams", chosenServer.machine->machineTeams.size());
		return Reference<TCMachineTeamInfo>();
	}

	// A server team should always come from servers on a machine team
	// Check if it is true
	bool isOnSameMachineTeam(TCTeamInfo const& team) const {
		std::vector<Standalone<StringRef>> machineIDs;
		for (const auto& server : team.getServers()) {
			if (!server->machine.isValid())
				return false;
			machineIDs.push_back(server->machine->machineID);
		}
		std::sort(machineIDs.begin(), machineIDs.end());

		int numExistance = 0;
		for (const auto& server : team.getServers()) {
			for (const auto& candidateMachineTeam : server->machine->machineTeams) {
				std::sort(candidateMachineTeam->machineIDs.begin(), candidateMachineTeam->machineIDs.end());
				if (machineIDs == candidateMachineTeam->machineIDs) {
					numExistance++;
					break;
				}
			}
		}
		return (numExistance == team.size());
	}

	// Sanity check the property of teams in unit test
	// Return true if all server teams belong to machine teams
	bool sanityCheckTeams() const {
		for (auto& team : teams) {
			if (isOnSameMachineTeam(*team) == false) {
				return false;
			}
		}

		return true;
	}

	int calculateHealthyServerCount() const {
		int serverCount = 0;
		for (auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				++serverCount;
			}
		}
		return serverCount;
	}

	int calculateHealthyMachineCount() const {
		int totalHealthyMachineCount = 0;
		for (auto& m : machine_info) {
			if (isMachineHealthy(m.second)) {
				++totalHealthyMachineCount;
			}
		}

		return totalHealthyMachineCount;
	}

	std::pair<int64_t, int64_t> calculateMinMaxServerTeamsOnServer() const {
		int64_t minTeams = std::numeric_limits<int64_t>::max();
		int64_t maxTeams = 0;
		for (auto& server : server_info) {
			if (server_status.get(server.first).isUnhealthy()) {
				continue;
			}
			minTeams = std::min((int64_t)server.second->teams.size(), minTeams);
			maxTeams = std::max((int64_t)server.second->teams.size(), maxTeams);
		}
		return std::make_pair(minTeams, maxTeams);
	}

	std::pair<int64_t, int64_t> calculateMinMaxMachineTeamsOnMachine() const {
		int64_t minTeams = std::numeric_limits<int64_t>::max();
		int64_t maxTeams = 0;
		for (auto& machine : machine_info) {
			if (!isMachineHealthy(machine.second)) {
				continue;
			}
			minTeams = std::min<int64_t>((int64_t)machine.second->machineTeams.size(), minTeams);
			maxTeams = std::max<int64_t>((int64_t)machine.second->machineTeams.size(), maxTeams);
		}
		return std::make_pair(minTeams, maxTeams);
	}

	// Sanity check
	bool isServerTeamCountCorrect(Reference<TCMachineTeamInfo> const& mt) const {
		int num = 0;
		bool ret = true;
		for (auto& team : teams) {
			if (team->machineTeam->machineIDs == mt->machineIDs) {
				++num;
			}
		}
		if (num != mt->serverTeams.size()) {
			ret = false;
			TraceEvent(SevError, "ServerTeamCountOnMachineIncorrect")
			    .detail("MachineTeam", mt->getMachineIDsStr())
			    .detail("ServerTeamsSize", mt->serverTeams.size())
			    .detail("CountedServerTeams", num);
		}
		return ret;
	}

	// Find the machine team with the least number of server teams
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithLeastProcessTeams() const {
		Reference<TCMachineTeamInfo> retMT;
		int minNumProcessTeams = std::numeric_limits<int>::max();

		for (auto& mt : machineTeams) {
			if (EXPENSIVE_VALIDATION) {
				ASSERT(isServerTeamCountCorrect(mt));
			}

			if (mt->serverTeams.size() < minNumProcessTeams) {
				minNumProcessTeams = mt->serverTeams.size();
				retMT = mt;
			}
		}

		return std::pair<Reference<TCMachineTeamInfo>, int>(retMT, minNumProcessTeams);
	}

	// Find the machine team whose members are on the most number of machine teams, same logic as serverTeamRemover
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithMostMachineTeams() const {
		Reference<TCMachineTeamInfo> retMT;
		int maxNumMachineTeams = 0;
		int targetMachineTeamNumPerMachine =
		    (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;

		for (auto& mt : machineTeams) {
			// The representative team number for the machine team mt is
			// the minimum number of machine teams of a machine in the team mt
			int representNumMachineTeams = std::numeric_limits<int>::max();
			for (auto& m : mt->machines) {
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

	// Find the server team whose members are on the most number of server teams
	std::pair<Reference<TCTeamInfo>, int> getServerTeamWithMostProcessTeams() const {
		Reference<TCTeamInfo> retST;
		int maxNumProcessTeams = 0;
		int targetTeamNumPerServer = (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;

		for (auto& t : teams) {
			// The minimum number of teams of a server in a team is the representative team number for the team t
			int representNumProcessTeams = std::numeric_limits<int>::max();
			for (auto& server : t->getServers()) {
				representNumProcessTeams = std::min<int>(representNumProcessTeams, server->teams.size());
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

	int getHealthyMachineTeamCount() const {
		int healthyTeamCount = 0;
		for (const auto& mt : machineTeams) {
			ASSERT(mt->machines.size() == configuration.storageTeamSize);

			if (isMachineTeamHealthy(*mt)) {
				++healthyTeamCount;
			}
		}

		return healthyTeamCount;
	}

	// Each machine is expected to have targetMachineTeamNumPerMachine
	// Return true if there exists a machine that does not have enough teams.
	bool notEnoughMachineTeamsForAMachine() const {
		// If we want to remove the machine team with most machine teams, we use the same logic as
		// notEnoughTeamsForAServer
		int targetMachineTeamNumPerMachine =
		    SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS
		        ? (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2
		        : SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER;
		for (auto& m : machine_info) {
			// If SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS is false,
			// The desired machine team number is not the same with the desired server team number
			// in notEnoughTeamsForAServer() below, because the machineTeamRemover() does not
			// remove a machine team with the most number of machine teams.
			if (m.second->machineTeams.size() < targetMachineTeamNumPerMachine && isMachineHealthy(m.second)) {
				return true;
			}
		}

		return false;
	}

	// Each server is expected to have targetTeamNumPerServer teams.
	// Return true if there exists a server that does not have enough teams.
	bool notEnoughTeamsForAServer() const {
		// We build more teams than we finally want so that we can use serverTeamRemover() actor to remove the teams
		// whose member belong to too many teams. This allows us to get a more balanced number of teams per server.
		// We want to ensure every server has targetTeamNumPerServer teams.
		// The numTeamsPerServerFactor is calculated as
		// (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER + ideal_num_of_teams_per_server) / 2
		// ideal_num_of_teams_per_server is (#teams * storageTeamSize) / #servers, which is
		// (#servers * DESIRED_TEAMS_PER_SERVER * storageTeamSize) / #servers.
		int targetTeamNumPerServer = (SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * (configuration.storageTeamSize + 1)) / 2;
		ASSERT(targetTeamNumPerServer > 0);
		for (auto& s : server_info) {
			if (s.second->teams.size() < targetTeamNumPerServer && !server_status.get(s.first).isUnhealthy()) {
				return true;
			}
		}

		return false;
	}

	// Create server teams based on machine teams
	// Before the number of machine teams reaches the threshold, build a machine team for each server team
	// When it reaches the threshold, first try to build a server team with existing machine teams; if failed,
	// build an extra machine team and record the event in trace
	int addTeamsBestOf(int teamsToBuild, int desiredTeams, int maxTeams) {
		ASSERT(teamsToBuild >= 0);
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
		int machineTeamsToBuild = std::max(
		    0, std::min(desiredMachineTeams - healthyMachineTeamCount, maxMachineTeams - totalMachineTeamCount));

		{
			TraceEvent te("BuildMachineTeams");
			te.detail("TotalHealthyMachine", totalHealthyMachineCount)
			    .detail("HealthyMachineTeamCount", healthyMachineTeamCount)
			    .detail("DesiredMachineTeams", desiredMachineTeams)
			    .detail("MaxMachineTeams", maxMachineTeams)
			    .detail("MachineTeamsToBuild", machineTeamsToBuild);
			// Pre-build all machine teams until we have the desired number of machine teams
			if (machineTeamsToBuild > 0 || notEnoughMachineTeamsForAMachine()) {
				auto addedMachineTeams = addBestMachineTeams(machineTeamsToBuild);
				te.detail("MachineTeamsAdded", addedMachineTeams);
			}
		}

		while (addedTeams < teamsToBuild || notEnoughTeamsForAServer()) {
			// Step 1: Create 1 best machine team
			std::vector<UID> bestServerTeam;
			int bestScore = std::numeric_limits<int>::max();
			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT; // BEST_OF_AMT = 4
			bool earlyQuitBuild = false;
			for (int i = 0; i < maxAttempts && i < 100; ++i) {
				// Step 2: Choose 1 least used server and then choose 1 least used machine team from the server
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
				// Step 3: Randomly pick 1 server from each machine in the chosen machine team to form a server team
				std::vector<UID> serverTeam;
				int chosenServerCount = 0;
				for (auto& machine : chosenMachineTeam->machines) {
					UID serverID;
					if (machine == chosenServer->machine) {
						serverID = chosenServer->id;
						++chosenServerCount;
					} else {
						std::vector<Reference<TCServerInfo>> healthyProcesses;
						for (auto it : machine->serversOnMachine) {
							if (!server_status.get(it->id).isUnhealthy()) {
								healthyProcesses.push_back(it);
							}
						}
						serverID = deterministicRandom()->randomChoice(healthyProcesses)->id;
					}
					serverTeam.push_back(serverID);
				}

				ASSERT(chosenServerCount == 1); // chosenServer should be used exactly once
				ASSERT(serverTeam.size() == configuration.storageTeamSize);

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
					score += server_info[server]->teams.size();
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
				break;
			}

			// Step 4: Add the server team
			addTeam(bestServerTeam.begin(), bestServerTeam.end(), false);
			addedTeams++;
		}

		healthyMachineTeamCount = getHealthyMachineTeamCount();

		std::pair<uint64_t, uint64_t> minMaxTeamsOnServer = calculateMinMaxServerTeamsOnServer();
		std::pair<uint64_t, uint64_t> minMaxMachineTeamsOnMachine = calculateMinMaxMachineTeamsOnMachine();

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
		    .detail("MinTeamsOnServer", minMaxTeamsOnServer.first)
		    .detail("MaxTeamsOnServer", minMaxTeamsOnServer.second)
		    .detail("MinMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.first)
		    .detail("MaxMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.second)
		    .detail("DoBuildTeams", doBuildTeams)
		    .trackLatest(teamCollectionInfoEventHolder->trackingKey);

		return addedTeams;
	}

	// Check if the number of server (and machine teams) is larger than the maximum allowed number
	void traceTeamCollectionInfo() const {
		int totalHealthyServerCount = calculateHealthyServerCount();
		int desiredServerTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyServerCount;
		int maxServerTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyServerCount;

		int totalHealthyMachineCount = calculateHealthyMachineCount();
		int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
		int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
		int healthyMachineTeamCount = getHealthyMachineTeamCount();

		std::pair<uint64_t, uint64_t> minMaxTeamsOnServer = calculateMinMaxServerTeamsOnServer();
		std::pair<uint64_t, uint64_t> minMaxMachineTeamsOnMachine = calculateMinMaxMachineTeamsOnMachine();

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
		    .detail("MinTeamsOnServer", minMaxTeamsOnServer.first)
		    .detail("MaxTeamsOnServer", minMaxTeamsOnServer.second)
		    .detail("MinMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.first)
		    .detail("MaxMachineTeamsOnMachine", minMaxMachineTeamsOnMachine.second)
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

	// Use the current set of known processes (from server_info) to compute an optimized set of storage server teams.
	// The following are guarantees of the process:
	//   - Each newly-built team will meet the replication policy
	//   - All newly-built teams will have exactly teamSize machines
	//
	// buildTeams() only ever adds teams to the list of teams. Teams are only removed from the list when all data has
	// been removed.
	//
	// buildTeams will not count teams larger than teamSize against the desired teams.
	Future<Void> buildTeams();

	void noHealthyTeams() const {
		std::set<UID> desiredServerSet;
		std::string desc;
		for (auto i = server_info.begin(); i != server_info.end(); ++i) {
			ASSERT(i->first == i->second->id);
			if (!server_status.get(i->first).isFailed) {
				desiredServerSet.insert(i->first);
				desc += i->first.shortString() + " (" + i->second->lastKnownInterface.toString() + "), ";
			}
		}

		TraceEvent(SevWarn, "NoHealthyTeams", distributorId)
		    .detail("CurrentServerTeamCount", teams.size())
		    .detail("ServerCount", server_info.size())
		    .detail("NonFailedServerCount", desiredServerSet.size());
	}

	bool shouldHandleServer(const StorageServerInterface& newServer) const {
		return (includedDCs.empty() ||
		        std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end() ||
		        (otherTrackedDCs.present() &&
		         std::find(otherTrackedDCs.get().begin(), otherTrackedDCs.get().end(), newServer.locality.dcId()) ==
		             otherTrackedDCs.get().end()));
	}

	void addServer(StorageServerInterface newServer,
	               ProcessClass processClass,
	               Promise<Void> errorOut,
	               Version addedVersion,
	               const DDEnabledState* ddEnabledState) {
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

			if (server_info.count(newServer.tssPairID.get())) {
				r->onTSSPairRemoved = server_info[newServer.tssPairID.get()]->onRemoved;
			}
		} else {
			server_info[newServer.id()] = r;
			// Establish the relation between server and machine
			checkAndCreateMachine(r);
		}

		r->tracker = storageServerTracker(cx, r.getPtr(), errorOut, addedVersion, ddEnabledState, newServer.isTss());

		if (!newServer.isTss()) {
			// link and wake up tss' tracker so it knows when this server gets removed
			if (tss_info_by_pair.count(newServer.id())) {
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

	bool removeTeam(Reference<TCTeamInfo> team) {
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

		for (const auto& server : team->getServers()) {
			for (int t = 0; t < server->teams.size(); t++) {
				if (server->teams[t] == team) {
					ASSERT(found);
					server->teams[t--] = server->teams.back();
					server->teams.pop_back();
					break; // The teams on a server should never duplicate
				}
			}
		}

		// Remove the team from its machine team
		bool foundInMachineTeam = false;
		for (int t = 0; t < team->machineTeam->serverTeams.size(); ++t) {
			if (team->machineTeam->serverTeams[t] == team) {
				team->machineTeam->serverTeams[t--] = team->machineTeam->serverTeams.back();
				team->machineTeam->serverTeams.pop_back();
				foundInMachineTeam = true;
				break; // The same team is added to the serverTeams only once
			}
		}

		ASSERT_WE_THINK(foundInMachineTeam);
		team->tracker.cancel();
		if (g_network->isSimulated()) {
			// Update server team information for consistency check in simulation
			traceTeamCollectionInfo();
		}
		return found;
	}

	// Check if the server belongs to a machine; if not, create the machine.
	// Establish the two-direction link between server and machine
	Reference<TCMachineInfo> checkAndCreateMachine(Reference<TCServerInfo> server) {
		ASSERT(server.isValid() && server_info.find(server->id) != server_info.end());
		auto& locality = server->lastKnownInterface.locality;
		Standalone<StringRef> machine_id = locality.zoneId().get(); // locality to machine_id with std::string type

		Reference<TCMachineInfo> machineInfo;
		if (machine_info.find(machine_id) == machine_info.end()) {
			// uid is the first storage server process on the machine
			TEST(true); // First storage server in process on the machine
			// For each machine, store the first server's localityEntry into machineInfo for later use.
			LocalityEntry localityEntry = machineLocalityMap.add(locality, &server->id);
			machineInfo = makeReference<TCMachineInfo>(server, localityEntry);
			machine_info.insert(std::make_pair(machine_id, machineInfo));
		} else {
			machineInfo = machine_info.find(machine_id)->second;
			machineInfo->serversOnMachine.push_back(server);
		}
		server->machine = machineInfo;

		return machineInfo;
	}

	// Check if the serverTeam belongs to a machine team; If not, create the machine team
	// Note: This function may make the machine team number larger than the desired machine team number
	Reference<TCMachineTeamInfo> checkAndCreateMachineTeam(Reference<TCTeamInfo> serverTeam) {
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

		machineTeam->serverTeams.push_back(serverTeam);

		return machineTeam;
	}

	// Remove the removedMachineInfo machine and any related machine team
	void removeMachine(Reference<TCMachineInfo> removedMachineInfo) {
		// Find machines that share teams with the removed machine
		std::set<Standalone<StringRef>> machinesWithAjoiningTeams;
		for (auto& machineTeam : removedMachineInfo->machineTeams) {
			machinesWithAjoiningTeams.insert(machineTeam->machineIDs.begin(), machineTeam->machineIDs.end());
		}
		machinesWithAjoiningTeams.erase(removedMachineInfo->machineID);
		// For each machine in a machine team with the removed machine,
		// erase shared machine teams from the list of teams.
		for (auto it = machinesWithAjoiningTeams.begin(); it != machinesWithAjoiningTeams.end(); ++it) {
			auto& machineTeams = machine_info[*it]->machineTeams;
			for (int t = 0; t < machineTeams.size(); t++) {
				auto& machineTeam = machineTeams[t];
				if (std::count(machineTeam->machineIDs.begin(),
				               machineTeam->machineIDs.end(),
				               removedMachineInfo->machineID)) {
					machineTeams[t--] = machineTeams.back();
					machineTeams.pop_back();
				}
			}
		}
		removedMachineInfo->machineTeams.clear();

		// Remove global machine team that includes removedMachineInfo
		for (int t = 0; t < machineTeams.size(); t++) {
			auto& machineTeam = machineTeams[t];
			if (std::count(
			        machineTeam->machineIDs.begin(), machineTeam->machineIDs.end(), removedMachineInfo->machineID)) {
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

	// Invariant: Remove a machine team only when the server teams on it has been removed
	// We never actively remove a machine team.
	// A machine team is removed when a machine is removed,
	// which is caused by the event when all servers on the machine is removed.
	// NOTE: When this function is called in the loop of iterating machineTeams, make sure NOT increase the index
	// in the next iteration of the loop. Otherwise, you may miss checking some elements in machineTeams
	bool removeMachineTeam(Reference<TCMachineTeamInfo> targetMT) {
		bool foundMachineTeam = false;
		for (int i = 0; i < machineTeams.size(); i++) {
			Reference<TCMachineTeamInfo> mt = machineTeams[i];
			if (mt->machineIDs == targetMT->machineIDs) {
				machineTeams[i--] = machineTeams.back();
				machineTeams.pop_back();
				foundMachineTeam = true;
				break;
			}
		}
		// Remove machine team on each machine
		for (auto& machine : targetMT->machines) {
			for (int i = 0; i < machine->machineTeams.size(); ++i) {
				if (machine->machineTeams[i]->machineIDs == targetMT->machineIDs) {
					machine->machineTeams[i--] = machine->machineTeams.back();
					machine->machineTeams.pop_back();
					break; // The machineTeams on a machine should never duplicate
				}
			}
		}

		return foundMachineTeam;
	}

	void removeTSS(UID removedServer) {
		// much simpler than remove server. tss isn't in any teams, so just remove it from data structures
		TraceEvent("RemovedTSS", distributorId).detail("ServerID", removedServer);
		Reference<TCServerInfo> removedServerInfo = server_and_tss_info[removedServer];

		tss_info_by_pair.erase(removedServerInfo->lastKnownInterface.tssPairID.get());
		server_and_tss_info.erase(removedServer);

		server_status.clear(removedServer);
	}

	void removeServer(UID removedServer) {
		TraceEvent("RemovedStorageServer", distributorId).detail("ServerID", removedServer);

		// ASSERT( !shardsAffectedByTeamFailure->getServersForTeam( t ) for all t in teams that contain removedServer )
		Reference<TCServerInfo> removedServerInfo = server_info[removedServer];
		// Step: Remove TCServerInfo from storageWiggler
		storageWiggler->removeServer(removedServer);

		// Step: Remove server team that relate to removedServer
		// Find all servers with which the removedServer shares teams
		std::set<UID> serversWithAjoiningTeams;
		auto& sharedTeams = removedServerInfo->teams;
		for (int i = 0; i < sharedTeams.size(); ++i) {
			auto& teamIds = sharedTeams[i]->getServerIDs();
			serversWithAjoiningTeams.insert(teamIds.begin(), teamIds.end());
		}
		serversWithAjoiningTeams.erase(removedServer);

		// For each server in a team with the removedServer, erase shared teams from the list of teams in that other
		// server
		for (auto it = serversWithAjoiningTeams.begin(); it != serversWithAjoiningTeams.end(); ++it) {
			auto& serverTeams = server_info[*it]->teams;
			for (int t = 0; t < serverTeams.size(); t++) {
				auto& serverIds = serverTeams[t]->getServerIDs();
				if (std::count(serverIds.begin(), serverIds.end(), removedServer)) {
					serverTeams[t--] = serverTeams.back();
					serverTeams.pop_back();
				}
			}
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

	// Adds storage servers held on process of which the Process Id is “id” into excludeServers which prevent
	// recruiting the wiggling storage servers and let teamTracker start to move data off the affected teams;
	// Return a vector of futures wait for all data is moved to other teams.
	Future<Void> excludeStorageServersForWiggle(const UID& id) {
		Future<Void> moveFuture = Void();
		if (this->server_info.count(id) != 0) {
			auto& info = server_info.at(id);
			AddressExclusion addr(info->lastKnownInterface.address().ip, info->lastKnownInterface.address().port);

			// don't overwrite the value set by actor trackExcludedServer
			bool abnormal =
			    this->excludedServers.count(addr) && this->excludedServers.get(addr) != DDTeamCollection::Status::NONE;

			if (info->lastKnownInterface.secondaryAddress().present()) {
				AddressExclusion addr2(info->lastKnownInterface.secondaryAddress().get().ip,
				                       info->lastKnownInterface.secondaryAddress().get().port);
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

	// Include wiggled storage servers by setting their status from `WIGGLING`
	// to `NONE`. The storage recruiter will recruit them as new storage servers
	void includeStorageServersForWiggle() {
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

	// Track a team and issue RelocateShards when the level of degradation changes
	// A badTeam can be unhealthy or just a redundantTeam removed by machineTeamRemover() or serverTeamRemover()
	Future<Void> teamTracker(Reference<TCTeamInfo> team, bool badTeam, bool redundantTeam);

	// Check the status of a storage server.
	// Apply all requirements to the server and mark it as excluded if it fails to satisfies these requirements
	Future<Void> storageServerTracker(Database cx,
	                                  TCServerInfo* server,
	                                  Promise<Void> errorOut,
	                                  Version addedVersion,
	                                  const DDEnabledState* ddEnabledState,
	                                  bool isTss);

	Future<Void> removeWrongStoreType();

	bool teamContainsFailedServer(Reference<TCTeamInfo> team);

	// NOTE: this actor returns when the cluster is healthy and stable (no server is expected to be removed in a period)
	// processingWiggle and processingUnhealthy indicate that some servers are going to be removed.
	Future<Void> waitUntilHealthy(double extraDelay = 0, bool waitWiggle = false);

	bool isCorrectDC(TCServerInfo* server) {
		return (includedDCs.empty() ||
		        std::find(includedDCs.begin(), includedDCs.end(), server->lastKnownInterface.locality.dcId()) !=
		            includedDCs.end());
	}

	Future<Void> removeBadTeams();

	Future<Void> zeroServerLeftLoggerActor(Reference<TCTeamInfo> team);

	// Set the server's storeType; Error is catched by the caller
	Future<Void> keyValueStoreTypeTracker(TCServerInfo* server);

	Future<Void> storageServerFailureTracker(TCServerInfo* server,
	                                         Database cx,
	                                         ServerStatus* status,
	                                         Version addedVersion);

	Future<Void> waitForAllDataRemoved(Database cx, UID serverID, Version addedVersion);

	Future<Void> machineTeamRemover();

	// Remove the server team whose members have the most number of process teams
	// until the total number of server teams is no larger than the desired number
	Future<Void> serverTeamRemover();

	Future<Void> trackExcludedServers();

	// Create a transaction updating `perpetualStorageWiggleIDPrefix` to the next serverID according to a sorted
	// wiggle_pq maintained by the wiggler.
	Future<Void> updateNextWigglingStorageID();

	// Iterate over each storage process to do storage wiggle. After initializing the first Process ID, it waits a
	// signal from `perpetualStorageWiggler` indicating the wiggling of current process is finished. Then it writes the
	// next Process ID to a system key: `perpetualStorageWiggleIDPrefix` to show the next process to wiggle.
	Future<Void> perpetualStorageWiggleIterator(AsyncVar<bool>* stopSignal,
	                                            FutureStream<Void> finishStorageWiggleSignal);

	// periodically check whether the cluster is healthy if we continue perpetual wiggle
	Future<Void> clusterHealthCheckForPerpetualWiggle(int* extraTeamCount);

	// Watches the value change of `perpetualStorageWiggleIDPrefix`, and adds the storage server into excludeServers
	// which prevent recruiting the wiggling storage servers and let teamTracker start to move data off the affected
	// teams. The wiggling process of current storage servers will be paused if the cluster is unhealthy and restarted
	// once the cluster is healthy again.
	Future<Void> perpetualStorageWiggler(AsyncVar<bool>* stopSignal, PromiseStream<Void> finishStorageWiggleSignal);

	// This coroutine sets a watch to monitor the value change of `perpetualStorageWiggleKey` which is controlled by
	// command `configure perpetual_storage_wiggle=$value` if the value is 1, this actor start 2 actors,
	// `perpetualStorageWiggleIterator` and `perpetualStorageWiggler`. Otherwise, it sends stop signal to them.
	Future<Void> monitorPerpetualStorageWiggle();

	// The serverList system keyspace keeps the StorageServerInterface for each serverID. Storage server's storeType
	// and serverID are decided by the server's filename. By parsing storage server file's filename on each disk,
	// process on each machine creates the TCServer with the correct serverID and StorageServerInterface.
	Future<Void> waitServerListChange(FutureStream<Void> serverRemoved, const DDEnabledState* ddEnabledState);

	Future<Void> waitHealthyZoneChange();

	// Monitor whether or not storage servers are being recruited.  If so, then a database cannot be considered quiet
	Future<Void> monitorStorageServerRecruitment();

	int numExistingSSOnAddr(const AddressExclusion& addr) {
		int numExistingSS = 0;
		for (auto& server : server_and_tss_info) {
			const NetworkAddress& netAddr = server.second->lastKnownInterface.stableAddress();
			AddressExclusion usedAddr(netAddr.ip, netAddr.port);
			if (usedAddr == addr) {
				++numExistingSS;
			}
		}

		return numExistingSS;
	}

	Future<Void> initializeStorage(RecruitStorageReply candidateWorker,
	                               DDEnabledState const* ddEnabledState,
	                               bool recruitTss,
	                               Reference<TSSPairState> tssState);

	Future<Void> storageRecruiter(Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
	                              DDEnabledState const* ddEnabledState);

	Future<Void> updateReplicasKey(Optional<Key> dcId);

	Future<Void> serverGetTeamRequests(TeamCollectionInterface tci);

	Future<Void> monitorHealthyTeams();

	// Find size of set intersection of excludeServerIDs and serverIDs on each team and see if the leftover team is
	// valid
	bool exclusionSafetyCheck(std::vector<UID>& excludeServerIDs) {
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

	Future<UID> getClusterId();

	// return the next ServerID in storageWiggler
	Future<UID> getNextWigglingServerID();

	// read the current map of `perpetualStorageWiggleIDPrefix`, then restore wigglingId.
	Future<Void> readStorageWiggleMap();

	auto eraseStorageWiggleMap(KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())>* metadataMap,
	                           UID id) {
		return runRYWTransaction(cx, [metadataMap, id](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			metadataMap->erase(tr, id);
			return Void();
		});
	}

	// Read storage metadata from database, and do necessary updates
	Future<Void> readOrCreateStorageMetadata(TCServerInfo* server);
};
