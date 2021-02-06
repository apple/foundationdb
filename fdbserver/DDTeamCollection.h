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

#include "fdbserver/TCMachineInfo.h"
#include "fdbserver/TCMachineTeamInfo.h"
#include "fdbserver/TCServerInfo.h"
#include "fdbserver/TCTeamInfo.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include <map>
#include <set>
#include <vector>

struct TCServerInfo;
struct TCMachineInfo;
struct TCTeamInfo;
struct TCMachineTeamInfo;

// TODO: Move to impl
struct ServerStatus {
	bool isFailed;
	bool isUndesired;
	bool isWrongConfiguration;
	bool initialized; // AsyncMap erases default constructed objects
	LocalityData locality;
	ServerStatus() : isFailed(true), isUndesired(false), isWrongConfiguration(false), initialized(false) {}
	ServerStatus(bool isFailed, bool isUndesired, LocalityData const& locality)
	  : isFailed(isFailed), isUndesired(isUndesired), locality(locality), isWrongConfiguration(false),
	    initialized(true) {}
	bool isUnhealthy() const { return isFailed || isUndesired; }
	const char* toString() const { return isFailed ? "Failed" : isUndesired ? "Undesired" : "Healthy"; }

	bool operator==(ServerStatus const& r) const {
		return isFailed == r.isFailed && isUndesired == r.isUndesired &&
		       isWrongConfiguration == r.isWrongConfiguration && locality == r.locality && initialized == r.initialized;
	}
	bool operator!=(ServerStatus const& r) const { return !(*this == r); }

	// If a process has reappeared without the storage server that was on it (isFailed == true), we don't need to
	// exclude it We also don't need to exclude processes who are in the wrong configuration (since those servers will be
	// removed)
	bool excludeOnRecruit() const { return !isFailed && !isWrongConfiguration; }
};
using ServerStatusMap = AsyncMap<UID, ServerStatus>;

class DDTeamCollection : public ReferenceCounted<DDTeamCollection> {
	void traceConfigInfo() const;
	void traceServerInfo() const;
	void traceServerTeamInfo() const;
	void traceMachineInfo() const;
	void traceMachineTeamInfo() const;
	void traceLocalityArrayIndexName() const;
	void traceMachineLocalityMap() const;
	int numExistingSSOnAddr(const AddressExclusion& addr);
	Future<Void> trackExcludedServers();
	bool teamContainsFailedServer(Reference<TCTeamInfo> team);
	Future<Void> waitForAllDataRemoved(Database cx, UID serverID, Version addedVersion);
	bool isCorrectDC(TCServerInfo const* server) const;
	Future<Void> updateReplicasKey(Optional<Key> dcId);
	Future<Void> storageRecruiter(Reference<AsyncVar<struct ServerDBInfo>> db, const DDEnabledState* ddEnabledState);
	Future<Void> initializeStorage(RecruitStorageReply candidateWorker, const DDEnabledState* ddEnabledState);
	Future<Void> teamTracker(Reference<TCTeamInfo> team, bool badTeam, bool redundantTeam);
	Future<Void> waitHealthyZoneChange();
	Future<Void> storageServerTracker(Database cx, TCServerInfo* server, Promise<Void> errorOut, Version addedVersion,
	                                  const DDEnabledState* ddEnabledState);
	Future<Void> monitorStorageServerRecruitment();
	Future<Void> monitorHealthyTeams();
	Future<Void> serverGetTeamRequests(TeamCollectionInterface tci);
	Future<Void> waitServerListChange(FutureStream<Void> serverRemoved, const DDEnabledState* ddEnabledState);
	Future<Void> removeBadTeams();
	Future<Void> machineTeamRemover();
	Future<Void> serverTeamRemover();
	Future<Void> zeroServerLeftLogger_impl(Reference<TCTeamInfo> team);
	Future<Void> checkAndRemoveInvalidLocalityAddr();
	Future<Void> removeWrongStoreType();
	Future<Void> waitUntilHealthy(double extraDelay = 0);
	void removeMachine(Reference<TCMachineInfo> removedMachineInfo);
	bool removeMachineTeam(Reference<TCMachineTeamInfo> targetMT);
	void removeServer(UID removedServer);
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithLeastProcessTeams() const;
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithMostMachineTeams() const;
	std::pair<Reference<TCTeamInfo>, int> getServerTeamWithMostProcessTeams() const;
	std::pair<int64_t, int64_t> calculateMinMaxServerTeamsOnServer() const;
	std::pair<int64_t, int64_t> calculateMinMaxMachineTeamsOnMachine() const;
	bool isServerTeamCountCorrect(TCMachineTeamInfo const& mt) const;
	bool isOnSameMachineTeam(const TCTeamInfo& team) const;
	int calculateHealthyServerCount() const;
	int calculateHealthyMachineCount() const;
	int getHealthyMachineTeamCount() const;
	bool notEnoughMachineTeamsForAMachine() const;
	bool notEnoughTeamsForAServer() const;
	void traceTeamCollectionInfo() const;
	Future<Void> buildTeams();
	void noHealthyTeams() const;
	bool shouldHandleServer(const StorageServerInterface& newServer) const;
	void addServer(StorageServerInterface newServer, ProcessClass processClass, Promise<Void> errorOut,
	               Version addedVersion, const DDEnabledState* ddEnabledState);
	bool isMachineTeamHealthy(std::vector<Standalone<StringRef>> const& machineIDs) const;
	bool isMachineTeamHealthy(Reference<TCMachineTeamInfo> const& machineTeam) const;
	bool isMachineHealthy(TCMachineInfo const* machine) const;
	Reference<TCServerInfo> findOneLeastUsedServer() const;
	Reference<TCMachineTeamInfo> findOneRandomMachineTeam(Reference<TCServerInfo> chosenServer) const;
	bool removeTeam(Reference<TCTeamInfo> team);
	Reference<TCMachineTeamInfo> checkAndCreateMachineTeam(Reference<TCTeamInfo> serverTeam);
	void addTeam(const std::vector<Reference<TCServerInfo>>& newTeamServers, bool isInitialTeam,
	             bool redundantTeam = false);
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Reference<TCMachineInfo>> machines);
	Reference<TCMachineTeamInfo> addMachineTeam(std::vector<Standalone<StringRef>>::iterator begin,
	                                            std::vector<Standalone<StringRef>>::iterator end);
	void rebuildMachineLocalityMap();
	Future<Void> logOnCompletion(Future<Void> signal);
	Future<Void> interruptableBuildTeams();
	Future<Void> checkBuildTeams();
	Future<Void> getTeam(GetTeamRequest req);
	int64_t getDebugTotalDataInFlight() const;
	Future<Void> addSubsetOfEmergencyTeams();
	Future<Void> init(Reference<InitialDataDistribution> initTeams, const DDEnabledState* ddEnabledState);
	bool isValidLocality(const IReplicationPolicy& storagePolicy, const LocalityData& locality) const;
	void evaluateTeamQuality() const;
	int overlappingMembers(const std::vector<UID>& team) const;
	int overlappingMachineMembers(std::vector<Standalone<StringRef>>& team) const;
	Reference<TCMachineTeamInfo> findMachineTeam(std::vector<Standalone<StringRef>>& machineIDs);
	template <class InputIt>
	void addTeam(InputIt begin, InputIt end, bool isInitialTeam) {
		vector<Reference<TCServerInfo>> newTeamServers;
		for (auto i = begin; i != end; ++i) {
			if (server_info.find(*i) != server_info.end()) {
				newTeamServers.push_back(server_info[*i]);
			}
		}
		addTeam(newTeamServers, isInitialTeam);
	}
	void resetLocalitySet();
	bool satisfiesPolicy(const std::vector<Reference<TCServerInfo>>& team, int amount = -1);

	friend class DDTeamCollectionImpl;

	Database cx;
	DatabaseConfiguration configuration;

	bool doBuildTeams;
	bool lastBuildTeamsFailed;
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	std::vector<UID> allServers;
	int64_t unhealthyServers;
	std::map<int, int> priority_teams;
	std::map<Key, int> lagging_zones; // zone to number of storage servers lagging
	AsyncVar<bool> disableFailingLaggingServers;

	// machine_info has all machines info; key must be unique across processes on the same machine
	std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
	LocalityMap<UID> machineLocalityMap; // locality info of machines

	std::vector<Reference<TCTeamInfo>> badTeams;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	PromiseStream<UID> removedServers;
	std::set<UID> recruitingIds; // The IDs of the SS which are being recruited
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

	// EXCLUDED if an address is in the excluded list in the database.
	// FAILED if an address is permanently failed.
	// NONE by default.  Updated asynchronously (eventually)
	enum class Status { NONE, EXCLUDED, FAILED };
	AsyncMap<AddressExclusion, Status> excludedServers;
	std::set<AddressExclusion>
	    invalidLocalityAddr; // These address have invalidLocality for the configured storagePolicy
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
	std::vector<LocalityEntry> forcedEntries, resultEntries;
	AsyncVar<Optional<Key>> healthyZone;
	Future<bool> clearHealthyZoneFuture;
	double medianAvailableSpace;
	double lastMedianAvailableSpaceUpdate;
	int lowestUtilizationTeam;
	int highestUtilizationTeam;
	AsyncTrigger printDetailedTeamsInfo;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	UID distributorId;
	std::vector<DDTeamCollection*> teamCollections;

	// addActor: add to actorCollection so that when an actor has error, the ActorCollection can catch the error.
	// addActor is used to create the actorCollection when the dataDistributionTeamCollection is created
	PromiseStream<Future<Void>> addActor;

public: // testing only
	std::map<UID, Reference<TCServerInfo>> server_info;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // all machine teams
	ServerStatusMap server_status;
	std::vector<Reference<TCTeamInfo>> teams;

	int addTeamsBestOf(int teamsToBuild, int desiredTeams, int maxTeams);
	bool sanityCheckTeams() const;
	Reference<TCMachineInfo> checkAndCreateMachine(Reference<TCServerInfo> server);
	int addBestMachineTeams(int machineTeamsToBuild);
	void addTeam(std::set<UID> const& team, bool isInitialTeam);
	Reference<LocalitySet> storageServerSet;

public:

	DDTeamCollection(Database const& cx, UID distributorId, MoveKeysLock const& lock,
	                 PromiseStream<RelocateShard> const& output,
	                 Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
	                 DatabaseConfiguration configuration, std::vector<Optional<Key>> includedDCs,
	                 Optional<std::vector<Optional<Key>>> otherTrackedDCs, Future<Void> readyToStart,
	                 Reference<AsyncVar<bool>> zeroHealthyTeams, bool primary,
	                 Reference<AsyncVar<bool>> processingUnhealthy, PromiseStream<GetMetricsRequest> getShardMetrics);
	~DDTeamCollection();

	void setTeamCollections(const std::vector<DDTeamCollection*>& teamCollections);
	UID getDistributorId() const;
	void traceAllInfo(bool shouldPrint = false) const;
	int constructMachinesFromServers();
	void addLaggingStorageServer(Key zoneId);
	void removeLaggingStorageServer(Key zoneId);
	Future<Void> printSnapshotTeamsInfo();
	Future<Void> run(Reference<InitialDataDistribution> initData, TeamCollectionInterface tci,
	                 Reference<AsyncVar<struct ServerDBInfo>> db, const DDEnabledState* ddEnabledState);
};
