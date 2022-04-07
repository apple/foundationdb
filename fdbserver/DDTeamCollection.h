/*
 * DDTeamCollection.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

	bool inDataZone(const LocalityData& locality) const {
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

	Future<Optional<std::pair<UID, Version>>> waitOnSS() const { return ssPairInfo.getFuture(); }

	Future<bool> waitOnTSS() const { return tssPairDone.getFuture(); }

	Future<Void> waitComplete() const { return complete.getFuture(); }
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

FDB_DECLARE_BOOLEAN_PARAM(IsPrimary);
FDB_DECLARE_BOOLEAN_PARAM(IsInitialTeam);
FDB_DECLARE_BOOLEAN_PARAM(IsRedundantTeam);
FDB_DECLARE_BOOLEAN_PARAM(IsBadTeam);
FDB_DECLARE_BOOLEAN_PARAM(WaitWiggle);

class DDTeamCollection : public ReferenceCounted<DDTeamCollection> {
	friend class DDTeamCollectionImpl;
	friend class DDTeamCollectionUnitTest;

	enum class Status { NONE = 0, WIGGLING = 1, EXCLUDED = 2, FAILED = 3 };

	// addActor: add to actorCollection so that when an actor has error, the ActorCollection can catch the error.
	// addActor is used to create the actorCollection when the dataDistributionTeamCollection is created
	PromiseStream<Future<Void>> addActor;

	bool doBuildTeams;
	bool lastBuildTeamsFailed;
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;
	AsyncVar<bool> waitUntilRecruited; // make teambuilder wait until one new SS is recruited

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	std::vector<UID> allServers;
	int64_t unhealthyServers;
	std::map<int, int> priority_teams;
	std::map<UID, Reference<TCServerInfo>> tss_info_by_pair;
	std::map<UID, Reference<TCServerInfo>> server_and_tss_info; // TODO could replace this with an efficient way to do a
	                                                            // read-only concatenation of 2 data structures?
	std::map<Key, int> lagging_zones; // zone to number of storage servers lagging
	AsyncVar<bool> disableFailingLaggingServers;

	// storage wiggle info
	Reference<StorageWiggler> storageWiggler;
	std::vector<AddressExclusion> wiggleAddresses; // collection of wiggling servers' address
	Optional<UID> wigglingId; // Process id of current wiggling storage server;
	Reference<AsyncVar<bool>> pauseWiggle;
	Reference<AsyncVar<bool>> processingWiggle; // track whether wiggling relocation is being processed
	PromiseStream<StorageWiggleValue> nextWiggleInfo;

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

	bool isTssRecruiting; // If tss recruiting is waiting on a pair, don't consider DD recruiting for the purposes of
	                      // QuietDB

	std::set<AddressExclusion>
	    invalidLocalityAddr; // These address have invalidLocality for the configured storagePolicy

	std::vector<Optional<Key>> includedDCs;
	Optional<std::vector<Optional<Key>>> otherTrackedDCs;
	Reference<AsyncVar<bool>> processingUnhealthy;
	Future<Void> readyToStart;
	Future<Void> checkTeamDelay;
	Promise<Void> addSubsetComplete;
	Future<Void> badTeamRemover;
	Future<Void> checkInvalidLocalities;

	Future<Void> wrongStoreTypeRemover;

	AsyncVar<Optional<Key>> healthyZone;
	Future<bool> clearHealthyZoneFuture;
	double medianAvailableSpace;
	double lastMedianAvailableSpaceUpdate;

	int lowestUtilizationTeam;
	int highestUtilizationTeam;

	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<Promise<int>> getUnhealthyRelocationCount;
	Promise<UID> removeFailedServer;

	// WIGGLING if an address is under storage wiggling.
	// EXCLUDED if an address is in the excluded list in the database.
	// FAILED if an address is permanently failed.
	// NONE by default.  Updated asynchronously (eventually)
	AsyncMap<AddressExclusion, Status> excludedServers;

	Reference<EventCacheHolder> ddTrackerStartingEventHolder;
	Reference<EventCacheHolder> teamCollectionInfoEventHolder;
	Reference<EventCacheHolder> storageServerRecruitmentEventHolder;

	bool primary;
	UID distributorId;

	LocalityMap<UID> machineLocalityMap; // locality info of machines

	// Randomly choose one machine team that has chosenServer and has the correct size
	// When configuration is changed, we may have machine teams with old storageTeamSize
	Reference<TCMachineTeamInfo> findOneRandomMachineTeam(TCServerInfo const& chosenServer) const;

	Future<Void> logOnCompletion(Future<Void> signal);

	void resetLocalitySet();

	bool satisfiesPolicy(const std::vector<Reference<TCServerInfo>>& team, int amount = -1) const;

	Future<Void> interruptableBuildTeams();

	Future<Void> checkBuildTeams();

	Future<Void> addSubsetOfEmergencyTeams();

	// Check if server or machine has a valid locality based on configured replication policy
	bool isValidLocality(Reference<IReplicationPolicy> storagePolicy, const LocalityData& locality) const;

	void evaluateTeamQuality() const;

	int overlappingMembers(const std::vector<UID>& team) const;

	int overlappingMachineMembers(std::vector<Standalone<StringRef>> const& team) const;

	Reference<TCMachineTeamInfo> findMachineTeam(std::vector<Standalone<StringRef>> const& machineIDs) const;

	// Add a machine team specified by input machines
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Reference<TCMachineInfo>> machines);

	// Add a machine team by using the machineIDs from begin to end
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Standalone<StringRef>>::iterator begin,
	                                            std::vector<Standalone<StringRef>>::iterator end);

	void traceConfigInfo() const;

	void traceServerInfo() const;

	void traceServerTeamInfo() const;

	void traceMachineInfo() const;

	void traceMachineTeamInfo() const;

	// Locality string is hashed into integer, used as KeyIndex
	// For better understand which KeyIndex is used for locality, we print this info in trace.
	void traceLocalityArrayIndexName() const;

	void traceMachineLocalityMap() const;

	// We must rebuild machine locality map whenever the entry in the map is inserted or removed
	void rebuildMachineLocalityMap();

	bool isMachineTeamHealthy(std::vector<Standalone<StringRef>> const& machineIDs) const;

	bool isMachineTeamHealthy(TCMachineTeamInfo const& machineTeam) const;

	bool isMachineHealthy(Reference<TCMachineInfo> const& machine) const;

	// Return the healthy server with the least number of correct-size server teams
	Reference<TCServerInfo> findOneLeastUsedServer() const;

	// A server team should always come from servers on a machine team
	// Check if it is true
	bool isOnSameMachineTeam(TCTeamInfo const& team) const;

	int calculateHealthyServerCount() const;

	int calculateHealthyMachineCount() const;

	std::pair<int64_t, int64_t> calculateMinMaxServerTeamsOnServer() const;

	std::pair<int64_t, int64_t> calculateMinMaxMachineTeamsOnMachine() const;

	// Sanity check
	bool isServerTeamCountCorrect(Reference<TCMachineTeamInfo> const& mt) const;

	// Find the machine team with the least number of server teams
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithLeastProcessTeams() const;

	// Find the machine team whose members are on the most number of machine teams, same logic as serverTeamRemover
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithMostMachineTeams() const;

	// Find the server team whose members are on the most number of server teams
	std::pair<Reference<TCTeamInfo>, int> getServerTeamWithMostProcessTeams() const;

	int getHealthyMachineTeamCount() const;

	// Each machine is expected to have targetMachineTeamNumPerMachine
	// Return true if there exists a machine that does not have enough teams.
	bool notEnoughMachineTeamsForAMachine() const;

	// Each server is expected to have targetTeamNumPerServer teams.
	// Return true if there exists a server that does not have enough teams.
	bool notEnoughTeamsForAServer() const;

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

	bool shouldHandleServer(const StorageServerInterface& newServer) const;

	// Check if the serverTeam belongs to a machine team; If not, create the machine team
	// Note: This function may make the machine team number larger than the desired machine team number
	Reference<TCMachineTeamInfo> checkAndCreateMachineTeam(Reference<TCTeamInfo> serverTeam);

	// Remove the removedMachineInfo machine and any related machine team
	void removeMachine(Reference<TCMachineInfo> removedMachineInfo);

	// Invariant: Remove a machine team only when the server teams on it has been removed
	// We never actively remove a machine team.
	// A machine team is removed when a machine is removed,
	// which is caused by the event when all servers on the machine is removed.
	// NOTE: When this function is called in the loop of iterating machineTeams, make sure NOT increase the index
	// in the next iteration of the loop. Otherwise, you may miss checking some elements in machineTeams
	bool removeMachineTeam(Reference<TCMachineTeamInfo> targetMT);

	// Adds storage servers held on process of which the Process Id is “id” into excludeServers which prevent
	// recruiting the wiggling storage servers and let teamTracker start to move data off the affected teams;
	// Return a vector of futures wait for all data is moved to other teams.
	Future<Void> excludeStorageServersForWiggle(const UID& id);

	// Include wiggled storage servers by setting their status from `WIGGLING`
	// to `NONE`. The storage recruiter will recruit them as new storage servers
	void includeStorageServersForWiggle();

	// Track a team and issue RelocateShards when the level of degradation changes
	// A bad team can be unhealthy or just a redundant team removed by machineTeamRemover() or serverTeamRemover()
	Future<Void> teamTracker(Reference<TCTeamInfo> team, IsBadTeam, IsRedundantTeam);

	// Check the status of a storage server.
	// Apply all requirements to the server and mark it as excluded if it fails to satisfies these requirements
	Future<Void> storageServerTracker(Database cx,
	                                  TCServerInfo* server,
	                                  Promise<Void> errorOut,
	                                  Version addedVersion,
	                                  DDEnabledState const& ddEnabledState,
	                                  bool isTss);

	bool teamContainsFailedServer(Reference<TCTeamInfo> team) const;

	// NOTE: this actor returns when the cluster is healthy and stable (no server is expected to be removed in a period)
	// processingWiggle and processingUnhealthy indicate that some servers are going to be removed.
	Future<Void> waitUntilHealthy(double extraDelay = 0, WaitWiggle = WaitWiggle::False) const;

	bool isCorrectDC(TCServerInfo const& server) const;

	// Set the server's storeType; Error is caught by the caller
	Future<Void> keyValueStoreTypeTracker(TCServerInfo* server);

	Future<Void> storageServerFailureTracker(TCServerInfo* server,
	                                         Database cx,
	                                         ServerStatus* status,
	                                         Version addedVersion);

	Future<Void> waitForAllDataRemoved(Database cx, UID serverID, Version addedVersion) const;

	// Create a transaction updating `perpetualStorageWiggleIDPrefix` to the next serverID according to a sorted
	// wiggle_pq maintained by the wiggler.
	Future<Void> updateNextWigglingStorageID();

	// Iterate over each storage process to do storage wiggle. After initializing the first Process ID, it waits a
	// signal from `perpetualStorageWiggler` indicating the wiggling of current process is finished. Then it writes the
	// next Process ID to a system key: `perpetualStorageWiggleIDPrefix` to show the next process to wiggle.
	Future<Void> perpetualStorageWiggleIterator(AsyncVar<bool>& stopSignal,
	                                            FutureStream<Void> finishStorageWiggleSignal);

	// periodically check whether the cluster is healthy if we continue perpetual wiggle
	Future<Void> clusterHealthCheckForPerpetualWiggle(int& extraTeamCount);

	// Watches the value change of `perpetualStorageWiggleIDPrefix`, and adds the storage server into excludeServers
	// which prevent recruiting the wiggling storage servers and let teamTracker start to move data off the affected
	// teams. The wiggling process of current storage servers will be paused if the cluster is unhealthy and restarted
	// once the cluster is healthy again.
	Future<Void> perpetualStorageWiggler(AsyncVar<bool>& stopSignal, PromiseStream<Void> finishStorageWiggleSignal);

	int numExistingSSOnAddr(const AddressExclusion& addr) const;

	Future<Void> initializeStorage(RecruitStorageReply candidateWorker,
	                               DDEnabledState const& ddEnabledState,
	                               bool recruitTss,
	                               Reference<TSSPairState> tssState);

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

	Future<Void> serverGetTeamRequests(TeamCollectionInterface tci);

	Future<Void> removeBadTeams();

	Future<Void> machineTeamRemover();

	// Remove the server team whose members have the most number of process teams
	// until the total number of server teams is no larger than the desired number
	Future<Void> serverTeamRemover();

	Future<Void> removeWrongStoreType();

	// Check if the number of server (and machine teams) is larger than the maximum allowed number
	void traceTeamCollectionInfo() const;

	Future<Void> updateReplicasKey(Optional<Key> dcId);

	Future<Void> storageRecruiter(Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
	                              DDEnabledState const& ddEnabledState);

	// Monitor whether or not storage servers are being recruited.  If so, then a database cannot be considered quiet
	Future<Void> monitorStorageServerRecruitment();

	// The serverList system keyspace keeps the StorageServerInterface for each serverID. Storage server's storeType
	// and serverID are decided by the server's filename. By parsing storage server file's filename on each disk,
	// process on each machine creates the TCServer with the correct serverID and StorageServerInterface.
	Future<Void> waitServerListChange(FutureStream<Void> serverRemoved, DDEnabledState const& ddEnabledState);

	Future<Void> trackExcludedServers();

	Future<Void> monitorHealthyTeams();

	// This coroutine sets a watch to monitor the value change of `perpetualStorageWiggleKey` which is controlled by
	// command `configure perpetual_storage_wiggle=$value` if the value is 1, this actor start 2 actors,
	// `perpetualStorageWiggleIterator` and `perpetualStorageWiggler`. Otherwise, it sends stop signal to them.
	Future<Void> monitorPerpetualStorageWiggle();

	Future<Void> waitHealthyZoneChange();

	int64_t getDebugTotalDataInFlight() const;

	void noHealthyTeams() const;

	// To enable verbose debug info, set shouldPrint to true
	void traceAllInfo(bool shouldPrint = false) const;

	// Check if the server belongs to a machine; if not, create the machine.
	// Establish the two-direction link between server and machine
	Reference<TCMachineInfo> checkAndCreateMachine(Reference<TCServerInfo> server);

	// Group storage servers (process) based on their machineId in LocalityData
	// All created machines are healthy
	// Return The number of healthy servers we grouped into machines
	int constructMachinesFromServers();

	// Create machineTeamsToBuild number of machine teams
	// No operation if machineTeamsToBuild is 0
	// Note: The creation of machine teams should not depend on server teams:
	// No matter how server teams will be created, we will create the same set of machine teams;
	// We should never use server team number in building machine teams.
	//
	// Five steps to create each machine team, which are document in the function
	// Reuse ReplicationPolicy selectReplicas func to select machine team
	// return number of added machine teams
	int addBestMachineTeams(int machineTeamsToBuild);

	// Sanity check the property of teams in unit test
	// Return true if all server teams belong to machine teams
	bool sanityCheckTeams() const;

	void disableBuildingTeams() { doBuildTeams = false; }

	void setCheckTeamDelay() { this->checkTeamDelay = Void(); }

	// Assume begin to end is sorted by std::sort
	// Assume InputIt is iterator to UID
	// Note: We must allow creating empty teams because empty team is created when a remote DB is initialized.
	// The empty team is used as the starting point to move data to the remote DB
	// begin : the start of the team member ID
	// end : end of the team member ID
	// isIntialTeam : False when the team is added by addTeamsBestOf(); True otherwise, e.g.,
	// when the team added at init() when we recreate teams by looking up DB
	template <class InputIt>
	void addTeam(InputIt begin, InputIt end, IsInitialTeam isInitialTeam) {
		std::vector<Reference<TCServerInfo>> newTeamServers;
		for (auto i = begin; i != end; ++i) {
			if (server_info.find(*i) != server_info.end()) {
				newTeamServers.push_back(server_info[*i]);
			}
		}

		addTeam(newTeamServers, isInitialTeam);
	}

	void addTeam(const std::vector<Reference<TCServerInfo>>& newTeamServers,
	             IsInitialTeam,
	             IsRedundantTeam = IsRedundantTeam::False);

	void addTeam(std::set<UID> const& team, IsInitialTeam isInitialTeam) {
		addTeam(team.begin(), team.end(), isInitialTeam);
	}

	// Create server teams based on machine teams
	// Before the number of machine teams reaches the threshold, build a machine team for each server team
	// When it reaches the threshold, first try to build a server team with existing machine teams; if failed,
	// build an extra machine team and record the event in trace
	int addTeamsBestOf(int teamsToBuild, int desiredTeams, int maxTeams);

public:
	Database cx;

	DatabaseConfiguration configuration;

	ServerStatusMap server_status;
	std::map<UID, Reference<TCServerInfo>> server_info;

	// machine_info has all machines info; key must be unique across processes on the same machine
	std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // all machine teams

	std::vector<Reference<TCTeamInfo>> teams;

	std::vector<DDTeamCollection*> teamCollections;
	AsyncTrigger printDetailedTeamsInfo;
	Reference<LocalitySet> storageServerSet;

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
	                 PromiseStream<Promise<int>> getUnhealthyRelocationCount);

	~DDTeamCollection();

	void addLaggingStorageServer(Key zoneId);

	void removeLaggingStorageServer(Key zoneId);

	// whether server is under wiggling proces, but wiggle is paused for some healthy compliance.
	bool isWigglePausedServer(const UID& server) const;

	// Returns a random healthy team, which does not contain excludeServer.
	std::vector<UID> getRandomHealthyTeam(const UID& excludeServer);

	Future<Void> getTeam(GetTeamRequest);

	Future<Void> init(Reference<InitialDataDistribution> initTeams, DDEnabledState const& ddEnabledState);

	void addServer(StorageServerInterface newServer,
	               ProcessClass processClass,
	               Promise<Void> errorOut,
	               Version addedVersion,
	               DDEnabledState const& ddEnabledState);

	bool removeTeam(Reference<TCTeamInfo> team);

	void removeTSS(UID removedServer);

	void removeServer(UID removedServer);

	// Find size of set intersection of excludeServerIDs and serverIDs on each team and see if the leftover team is
	// valid
	bool exclusionSafetyCheck(std::vector<UID>& excludeServerIDs);

	bool isPrimary() const { return primary; }

	UID getDistributorId() const { return distributorId; }

	// Keep track of servers and teams -- serves requests for getRandomTeam
	static Future<Void> run(Reference<DDTeamCollection> teamCollection,
	                        Reference<InitialDataDistribution> initData,
	                        TeamCollectionInterface tci,
	                        Reference<IAsyncListener<RequestStream<RecruitStorageRequest>>> recruitStorage,
	                        DDEnabledState const& ddEnabledState);

	// Take a snapshot of necessary data structures from `DDTeamCollection` and print them out with yields to avoid slow
	// task on the run loop.
	static Future<Void> printSnapshotTeamsInfo(Reference<DDTeamCollection> self);
};
