/*
 * DataDistribution.actor.cpp
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

#include "flow/ActorCollection.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/Knobs.h"
#include <set>
#include <sstream>
#include "fdbserver/WaitFailure.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbrpc/Replication.h"
#include "flow/UnitTest.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

class TCTeamInfo;
struct TCMachineInfo;
class TCMachineTeamInfo;

struct TCServerInfo : public ReferenceCounted<TCServerInfo> {
	UID id;
	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;
	vector<Reference<TCTeamInfo>> teams;
	Reference<TCMachineInfo> machine;
	Future<Void> tracker;
	int64_t dataInFlightToServer;
	ErrorOr<GetPhysicalMetricsReply> serverMetrics;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged;
	Promise<Void> removed;
	Future<Void> onRemoved;
	Promise<Void> wakeUpTracker;
	bool inDesiredDC;
	LocalityEntry localityEntry;
	Promise<Void> updated;

	TCServerInfo(StorageServerInterface ssi, ProcessClass processClass, bool inDesiredDC, Reference<LocalitySet> storageServerSet) : id(ssi.id()), lastKnownInterface(ssi), lastKnownClass(processClass), dataInFlightToServer(0), onInterfaceChanged(interfaceChanged.getFuture()), onRemoved(removed.getFuture()), inDesiredDC(inDesiredDC) {
		localityEntry = ((LocalityMap<UID>*) storageServerSet.getPtr())->add(ssi.locality, &id);
	}
};

struct TCMachineInfo : public ReferenceCounted<TCMachineInfo> {
	std::vector<Reference<TCServerInfo>> serversOnMachine; // SOMEDAY: change from vector to set
	Standalone<StringRef> machineID;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // SOMEDAY: split good and bad machine teams.
	LocalityEntry localityEntry;

	explicit TCMachineInfo(Reference<TCServerInfo> server, const LocalityEntry& entry) : localityEntry(entry) {
		ASSERT(serversOnMachine.empty());
		serversOnMachine.push_back(server);
		machineID = server->lastKnownInterface.locality.zoneId().get();
	}

	std::string getServersIDStr() {
		std::stringstream ss;
		if (serversOnMachine.empty()) return "[unset]";

		for (auto& server : serversOnMachine) {
			ss << server->id.toString() << " ";
		}

		return ss.str();
	}
};

ACTOR Future<Void> updateServerMetrics( TCServerInfo *server ) {
	state StorageServerInterface ssi = server->lastKnownInterface;
	state Future<ErrorOr<GetPhysicalMetricsReply>> metricsRequest = ssi.getPhysicalMetrics.tryGetReply( GetPhysicalMetricsRequest(), TaskDataDistributionLaunch );
	state Future<Void> resetRequest = Never();
	state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged( server->onInterfaceChanged );
	state Future<Void> serverRemoved( server->onRemoved );

	loop {
		choose {
			when( ErrorOr<GetPhysicalMetricsReply> rep = wait( metricsRequest ) ) {
				if( rep.present() ) {
					server->serverMetrics = rep;
					if(server->updated.canBeSet()) {
						server->updated.send(Void());
					}
					return Void();
				}
				metricsRequest = Never();
				resetRequest = delay( SERVER_KNOBS->METRIC_DELAY, TaskDataDistributionLaunch );
			}
			when( std::pair<StorageServerInterface,ProcessClass> _ssi = wait( interfaceChanged ) ) {
				ssi = _ssi.first;
				interfaceChanged = server->onInterfaceChanged;
				resetRequest = Void();
			}
			when( wait( serverRemoved ) ) {
				return Void();
			}
			when( wait( resetRequest ) ) { //To prevent a tight spin loop
				if(IFailureMonitor::failureMonitor().getState(ssi.getPhysicalMetrics.getEndpoint()).isFailed()) {
					resetRequest = IFailureMonitor::failureMonitor().onStateEqual(ssi.getPhysicalMetrics.getEndpoint(), FailureStatus(false));
				}
				else {
					resetRequest = Never();
					metricsRequest = ssi.getPhysicalMetrics.tryGetReply( GetPhysicalMetricsRequest(), TaskDataDistributionLaunch );
				}
			}
		}
	}
}

ACTOR Future<Void> updateServerMetrics( Reference<TCServerInfo> server ) {
	wait( updateServerMetrics( server.getPtr() ) );
	return Void();
}

// Machine team information
class TCMachineTeamInfo : public ReferenceCounted<TCMachineTeamInfo> {
public:
	vector<Reference<TCMachineInfo>> machines;
	vector<Standalone<StringRef>> machineIDs;
	vector<Reference<TCTeamInfo>> serverTeams;

	explicit TCMachineTeamInfo(vector<Reference<TCMachineInfo>> const& machines) : machines(machines) {
		machineIDs.reserve(machines.size());
		for (int i = 0; i < machines.size(); i++) {
			machineIDs.push_back(machines[i]->machineID);
		}
		sort(machineIDs.begin(), machineIDs.end());
	}

	int size() {
		ASSERT(machines.size() == machineIDs.size());
		return machineIDs.size();
	}

	std::string getMachineIDsStr() {
		std::stringstream ss;

		if (machineIDs.empty()) return "[unset]";

		for (auto& id : machineIDs) {
			ss << id.contents().toString() << " ";
		}

		return ss.str();
	}

	int getTotalMachineTeamNumber() {
		int count = 0;

		for (auto& machine : machines) {
			ASSERT(machine->machineTeams.size() >= 0);
			count += machine->machineTeams.size();
		}

		return count;
	}

	bool operator==(TCMachineTeamInfo& rhs) const { return this->machineIDs == rhs.machineIDs; }
};

class TCTeamInfo : public ReferenceCounted<TCTeamInfo>, public IDataDistributionTeam {
private:
	vector< Reference<TCServerInfo> > servers;
	vector<UID> serverIDs;

public:
	Reference<TCMachineTeamInfo> machineTeam;
	Future<Void> tracker;
	bool healthy;
	bool wrongConfiguration; //True if any of the servers in the team have the wrong configuration
	int priority;

	explicit TCTeamInfo(vector<Reference<TCServerInfo>> const& servers)
	  : servers(servers), healthy(true), priority(PRIORITY_TEAM_HEALTHY), wrongConfiguration(false) {
		if (servers.empty()) {
			TraceEvent(SevInfo, "ConstructTCTeamFromEmptyServers");
		}
		serverIDs.reserve(servers.size());
		for (int i = 0; i < servers.size(); i++) {
			serverIDs.push_back(servers[i]->id);
		}
	}

	virtual vector<StorageServerInterface> getLastKnownServerInterfaces() {
		vector<StorageServerInterface> v;
		v.reserve(servers.size());
		for(int i=0; i<servers.size(); i++)
			v.push_back(servers[i]->lastKnownInterface);
		return v;
	}
	virtual int size() {
		ASSERT(servers.size() == serverIDs.size());
		return servers.size();
	}
	virtual vector<UID> const& getServerIDs() { return serverIDs; }
	const vector<Reference<TCServerInfo>>& getServers() { return servers; }

	virtual std::string getServerIDsStr() {
		std::stringstream ss;

		if (serverIDs.empty()) return "[unset]";

		for (auto& id : serverIDs) {
			ss << id.toString() << " ";
		}

		return ss.str();
	}

	virtual void addDataInFlightToTeam( int64_t delta ) {
		for(int i=0; i<servers.size(); i++)
			servers[i]->dataInFlightToServer += delta;
	}
	virtual int64_t getDataInFlightToTeam() {
		int64_t dataInFlight = 0.0;
		for(int i=0; i<servers.size(); i++)
			dataInFlight += servers[i]->dataInFlightToServer;
		return dataInFlight;
	}

	virtual int64_t getLoadBytes( bool includeInFlight = true, double inflightPenalty = 1.0 ) {
		int64_t physicalBytes = getLoadAverage();
		double minFreeSpaceRatio = getMinFreeSpaceRatio(includeInFlight);
		int64_t inFlightBytes = includeInFlight ? getDataInFlightToTeam() / servers.size() : 0;
		double freeSpaceMultiplier = SERVER_KNOBS->FREE_SPACE_RATIO_CUTOFF / ( std::max( std::min( SERVER_KNOBS->FREE_SPACE_RATIO_CUTOFF, minFreeSpaceRatio ), 0.000001 ) );

		if(freeSpaceMultiplier > 1 && g_random->random01() < 0.001)
			TraceEvent(SevWarn, "DiskNearCapacity").detail("FreeSpaceRatio", minFreeSpaceRatio);

		return (physicalBytes + (inflightPenalty*inFlightBytes)) * freeSpaceMultiplier;
	}

	virtual int64_t getMinFreeSpace( bool includeInFlight = true ) {
		int64_t minFreeSpace = std::numeric_limits<int64_t>::max();
		for(int i=0; i<servers.size(); i++) {
			if( servers[i]->serverMetrics.present() ) {
				auto& replyValue = servers[i]->serverMetrics.get();

				ASSERT(replyValue.free.bytes >= 0);
				ASSERT(replyValue.capacity.bytes >= 0);

				int64_t bytesFree = replyValue.free.bytes;
				if(includeInFlight) {
					bytesFree -= servers[i]->dataInFlightToServer;
				}

				minFreeSpace = std::min(bytesFree, minFreeSpace);
			}
		}

		return minFreeSpace; // Could be negative
	}

	virtual double getMinFreeSpaceRatio( bool includeInFlight = true ) {
		double minRatio = 1.0;
		for(int i=0; i<servers.size(); i++) {
			if( servers[i]->serverMetrics.present() ) {
				auto& replyValue = servers[i]->serverMetrics.get();

				ASSERT(replyValue.free.bytes >= 0);
				ASSERT(replyValue.capacity.bytes >= 0);

				int64_t bytesFree = replyValue.free.bytes;
				if(includeInFlight) {
					bytesFree = std::max((int64_t)0, bytesFree - servers[i]->dataInFlightToServer);
				}

				if(replyValue.capacity.bytes == 0)
					minRatio = 0;
				else
					minRatio = std::min( minRatio, ((double)bytesFree) / replyValue.capacity.bytes );
			}
		}

		return minRatio;
	}

	virtual bool hasHealthyFreeSpace() {
		return getMinFreeSpaceRatio() > SERVER_KNOBS->MIN_FREE_SPACE_RATIO && getMinFreeSpace() > SERVER_KNOBS->MIN_FREE_SPACE;
	}

	virtual Future<Void> updatePhysicalMetrics() {
		return doUpdatePhysicalMetrics( this );
	}

	virtual bool isOptimal() {
		for(int i=0; i<servers.size(); i++) {
			if( servers[i]->lastKnownClass.machineClassFitness( ProcessClass::Storage ) > ProcessClass::UnsetFit ) {
				return false;
			}
		}
		return true;
	}

	virtual bool isWrongConfiguration() { return wrongConfiguration; }
	virtual void setWrongConfiguration(bool wrongConfiguration) { this->wrongConfiguration = wrongConfiguration; }
	virtual bool isHealthy() { return healthy; }
	virtual void setHealthy(bool h) { healthy = h; }
	virtual int getPriority() { return priority; }
	virtual void setPriority(int p) { priority = p; }
	virtual void addref() { ReferenceCounted<TCTeamInfo>::addref(); }
	virtual void delref() { ReferenceCounted<TCTeamInfo>::delref(); }

	virtual void addServers(const vector<UID> & servers) {
		serverIDs.reserve(servers.size());
		for (int i = 0; i < servers.size(); i++) {
			serverIDs.push_back(servers[i]);
		}
	}

private:
	// Calculate an "average" of the metrics replies that we received.  Penalize teams from which we did not receive all replies.
	int64_t getLoadAverage() {
		int64_t bytesSum = 0;
		int added = 0;
		for(int i=0; i<servers.size(); i++)
			if( servers[i]->serverMetrics.present() ) {
				added++;
				bytesSum += servers[i]->serverMetrics.get().load.bytes;
			}

		if( added < servers.size() )
			bytesSum *= 2;

		return added == 0 ? 0 : bytesSum / added;
	}

	// Calculate the max of the metrics replies that we received.


	ACTOR Future<Void> doUpdatePhysicalMetrics( TCTeamInfo* self ) {
		std::vector<Future<Void>> updates;
		for( int i = 0; i< self->servers.size(); i++ )
			updates.push_back( updateServerMetrics( self->servers[i] ) );
		wait( waitForAll( updates ) );
		return Void();
	}
};

struct ServerStatus {
	bool isFailed;
	bool isUndesired;
	bool isWrongConfiguration;
	bool initialized; //AsyncMap erases default constructed objects
	LocalityData locality;
	ServerStatus() : isFailed(true), isUndesired(false), isWrongConfiguration(false), initialized(false) {}
	ServerStatus( bool isFailed, bool isUndesired, LocalityData const& locality ) : isFailed(isFailed), isUndesired(isUndesired), locality(locality), isWrongConfiguration(false), initialized(true) {}
	bool isUnhealthy() const { return isFailed || isUndesired; }
	const char* toString() const { return isFailed ? "Failed" : isUndesired ? "Undesired" : "Healthy"; }

	bool operator == (ServerStatus const& r) const { return isFailed == r.isFailed && isUndesired == r.isUndesired && isWrongConfiguration == r.isWrongConfiguration && locality == r.locality && initialized == r.initialized; }

	//If a process has reappeared without the storage server that was on it (isFailed == true), we don't need to exclude it
	//We also don't need to exclude processes who are in the wrong configuration (since those servers will be removed)
	bool excludeOnRecruit() { return !isFailed && !isWrongConfiguration; }
};
typedef AsyncMap<UID, ServerStatus> ServerStatusMap;

// Read keyservers, return unique set of teams
ACTOR Future<Reference<InitialDataDistribution>> getInitialDataDistribution( Database cx, UID distributorId, MoveKeysLock moveKeysLock, std::vector<Optional<Key>> remoteDcIds ) {
	state Reference<InitialDataDistribution> result = Reference<InitialDataDistribution>(new InitialDataDistribution);
	state Key beginKey = allKeys.begin;

	state bool succeeded;

	state Transaction tr( cx );

	state std::map<UID, Optional<Key>> server_dc;
	state std::map<vector<UID>, std::pair<vector<UID>, vector<UID>>> team_cache;

	//Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure causing entries to be duplicated
	loop {
		server_dc.clear();
		succeeded = false;
		try {
			result->mode = 1;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> mode = wait( tr.get( dataDistributionModeKey ) );
			if (mode.present()) {
				BinaryReader rd( mode.get(), Unversioned() );
				rd >> result->mode;
			}
			if (!result->mode) // result->mode can be changed to 0 when we disable data distribution
				return result;


			state Future<vector<ProcessData>> workers = getWorkers(&tr);
			state Future<Standalone<RangeResultRef>> serverList = tr.getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY );
			wait( success(workers) && success(serverList) );
			ASSERT( !serverList.get().more && serverList.get().size() < CLIENT_KNOBS->TOO_MANY );

			std::map<Optional<Standalone<StringRef>>, ProcessData> id_data;
			for( int i = 0; i < workers.get().size(); i++ )
				id_data[workers.get()[i].locality.processId()] = workers.get()[i];

			succeeded = true;

			for( int i = 0; i < serverList.get().size(); i++ ) {
				auto ssi = decodeServerListValue( serverList.get()[i].value );
				result->allServers.push_back( std::make_pair(ssi, id_data[ssi.locality.processId()].processClass) );
				server_dc[ssi.id()] = ssi.locality.dcId();
			}

			break;
		}
		catch(Error &e) {
			wait( tr.onError(e) );

			ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
			TraceEvent("GetInitialTeamsRetry", distributorId);
		}
	}

	//If keyServers is too large to read in a single transaction, then we will have to break this process up into multiple transactions.
	//In that case, each iteration should begin where the previous left off
	while(beginKey < allKeys.end) {
		TEST(beginKey > allKeys.begin); //Multi-transactional getInitialDataDistribution
		loop {
			succeeded = false;
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				wait(checkMoveKeysLockReadOnly(&tr, moveKeysLock));
				Standalone<RangeResultRef> keyServers = wait(krmGetRanges(&tr, keyServersPrefix, KeyRangeRef(beginKey, allKeys.end), SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT, SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
				succeeded = true;

				vector<UID> src, dest, last;

				// for each range
				for(int i = 0; i < keyServers.size() - 1; i++) {
					DDShardInfo info( keyServers[i].key );
					decodeKeyServersValue( keyServers[i].value, src, dest );
					if(remoteDcIds.size()) {
						auto srcIter = team_cache.find(src);
						if(srcIter == team_cache.end()) {
							for(auto& id : src) {
								auto& dc = server_dc[id];
								if(std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
									info.remoteSrc.push_back(id);
								} else {
									info.primarySrc.push_back(id);
								}
							}
							result->primaryTeams.insert( info.primarySrc );
							result->remoteTeams.insert( info.remoteSrc );
							team_cache[src] = std::make_pair(info.primarySrc, info.remoteSrc);
						} else {
							info.primarySrc = srcIter->second.first;
							info.remoteSrc = srcIter->second.second;
						}
						if(dest.size()) {
							info.hasDest = true;
							auto destIter = team_cache.find(dest);
							if(destIter == team_cache.end()) {
								for(auto& id : dest) {
									auto& dc = server_dc[id];
									if(std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
										info.remoteDest.push_back(id);
									} else {
										info.primaryDest.push_back(id);
									}
								}
								result->primaryTeams.insert( info.primaryDest );
								result->remoteTeams.insert( info.remoteDest );
								team_cache[dest] = std::make_pair(info.primaryDest, info.remoteDest);
							} else {
								info.primaryDest = destIter->second.first;
								info.remoteDest = destIter->second.second;
							}
						}
					} else {
						info.primarySrc = src;
						auto srcIter = team_cache.find(src);
						if(srcIter == team_cache.end()) {
							result->primaryTeams.insert( src );
							team_cache[src] = std::pair<vector<UID>, vector<UID>>();
						}
						if (dest.size()) {
							info.hasDest = true;
							info.primaryDest = dest;
							auto destIter = team_cache.find(dest);
							if(destIter == team_cache.end()) {
								result->primaryTeams.insert( dest );
								team_cache[dest] = std::pair<vector<UID>, vector<UID>>();
							}
						}
					}
					result->shards.push_back( info );
				}

				ASSERT(keyServers.size() > 0);
				beginKey = keyServers.end()[-1].key;
				break;
			} catch (Error& e) {
				wait( tr.onError(e) );

				ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
				TraceEvent("GetInitialTeamsKeyServersRetry", distributorId);
			}
		}

		tr.reset();
	}

	// a dummy shard at the end with no keys or servers makes life easier for trackInitialShards()
	result->shards.push_back( DDShardInfo(allKeys.end) );

	return result;
}

Future<Void> storageServerTracker(
	struct DDTeamCollection* const& self,
	Database const& cx,
	TCServerInfo* const& server,
	Promise<Void> const& errorOut,
	Version const& addedVersion);

Future<Void> teamTracker(struct DDTeamCollection* const& self, Reference<TCTeamInfo> const& team, bool const& badTeam, bool const& redundantTeam);

struct DDTeamCollection : ReferenceCounted<DDTeamCollection> {
	enum { REQUESTING_WORKER = 0, GETTING_WORKER = 1, GETTING_STORAGE = 2 };

	// addActor: add to actorCollection so that when an actor has error, the ActorCollection can catch the error.
	// addActor is used to create the actorCollection when the dataDistributionTeamCollection is created
	PromiseStream<Future<Void>> addActor;
	Database cx;
	UID distributorId;
	DatabaseConfiguration configuration;

	bool doBuildTeams;
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	vector<UID> allServers;
	ServerStatusMap server_status;
	int64_t unhealthyServers;
	std::map<int,int> priority_teams;
	std::map<UID, Reference<TCServerInfo>> server_info;

	// machine_info has all machines info; key must be unique across processes on the same machine
	std::map<Standalone<StringRef>, Reference<TCMachineInfo>> machine_info;
	std::vector<Reference<TCMachineTeamInfo>> machineTeams; // all machine teams
	LocalityMap<UID> machineLocalityMap; // locality info of machines

	vector<Reference<TCTeamInfo>> teams;
	vector<Reference<TCTeamInfo>> badTeams;
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

	AsyncMap< AddressExclusion, bool > excludedServers;  // true if an address is in the excluded list in the database.  Updated asynchronously (eventually)

	std::vector<Optional<Key>> includedDCs;
	Optional<std::vector<Optional<Key>>> otherTrackedDCs;
	bool primary;
	Reference<AsyncVar<bool>> processingUnhealthy;
	Future<Void> readyToStart;
	Future<Void> checkTeamDelay;
	Promise<Void> addSubsetComplete;
	Future<Void> badTeamRemover;
	Future<Void> redundantTeamRemover;

	Reference<LocalitySet> storageServerSet;
	std::vector<LocalityEntry> forcedEntries, resultEntries;

	std::vector<DDTeamCollection*> teamCollections;
	AsyncVar<Optional<Key>> healthyZone;
	Future<Void> clearHealthyZoneFuture;

	void resetLocalitySet() {
		storageServerSet = Reference<LocalitySet>(new LocalityMap<UID>());
		LocalityMap<UID>* storageServerMap = (LocalityMap<UID>*) storageServerSet.getPtr();

		for( auto& it : server_info ) {
			it.second->localityEntry = storageServerMap->add(it.second->lastKnownInterface.locality, &it.second->id);
		}
	}

	bool satisfiesPolicy(const std::vector<Reference<TCServerInfo>>& team, int amount = -1) {
		forcedEntries.clear();
		resultEntries.clear();
		if(amount == -1) {
			amount = team.size();
		}

		for(int i = 0; i < amount; i++) {
			forcedEntries.push_back(team[i]->localityEntry);
		}

		bool result = storageServerSet->selectReplicas(configuration.storagePolicy, forcedEntries, resultEntries);
		return result && resultEntries.size() == 0;
	}

	DDTeamCollection(Database const& cx, UID distributorId, MoveKeysLock const& lock,
	                 PromiseStream<RelocateShard> const& output,
	                 Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
	                 DatabaseConfiguration configuration, std::vector<Optional<Key>> includedDCs,
	                 Optional<std::vector<Optional<Key>>> otherTrackedDCs,
	                 Future<Void> readyToStart, Reference<AsyncVar<bool>> zeroHealthyTeams, bool primary,
	                 Reference<AsyncVar<bool>> processingUnhealthy)
	  : cx(cx), distributorId(distributorId), lock(lock), output(output),
	    shardsAffectedByTeamFailure(shardsAffectedByTeamFailure), doBuildTeams(true), teamBuilder(Void()),
	    badTeamRemover(Void()), redundantTeamRemover(Void()), configuration(configuration),
	    readyToStart(readyToStart), clearHealthyZoneFuture(Void()),
	    checkTeamDelay(delay(SERVER_KNOBS->CHECK_TEAM_DELAY, TaskDataDistribution)),
	    initialFailureReactionDelay(
	        delayed(readyToStart, SERVER_KNOBS->INITIAL_FAILURE_REACTION_DELAY, TaskDataDistribution)),
	    healthyTeamCount(0), storageServerSet(new LocalityMap<UID>()),
	    initializationDoneActor(logOnCompletion(readyToStart && initialFailureReactionDelay, this)),
	    optimalTeamCount(0), recruitingStream(0), restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY),
	    unhealthyServers(0), includedDCs(includedDCs), otherTrackedDCs(otherTrackedDCs),
	    zeroHealthyTeams(zeroHealthyTeams), zeroOptimalTeams(true), primary(primary),
	    processingUnhealthy(processingUnhealthy) {
		if(!primary || configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", distributorId)
				.detail( "State", "Inactive" )
				.trackLatest( "DDTrackerStarting" );
		}
	}

	~DDTeamCollection() {
		// The following kills a reference cycle between the teamTracker actor and the TCTeamInfo that both holds and is held by the actor
		// It also ensures that the trackers are done fiddling with healthyTeamCount before we free this
		for(int i=0; i < teams.size(); i++) {
			teams[i]->tracker.cancel();
		}
		for(int i=0; i < badTeams.size(); i++) {
			badTeams[i]->tracker.cancel();
		}
		// The following makes sure that, even if a reference to a team is held in the DD Queue, the tracker will be stopped
		//  before the server_status map to which it has a pointer, is destroyed.
		for(auto it = server_info.begin(); it != server_info.end(); ++it) {
			it->second->tracker.cancel();
		}

		teamBuilder.cancel();
	}

	ACTOR static Future<Void> logOnCompletion( Future<Void> signal, DDTeamCollection* self ) {
		wait(signal);
		wait(delay(SERVER_KNOBS->LOG_ON_COMPLETION_DELAY, TaskDataDistribution));

		if(!self->primary || self->configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", self->distributorId)
				.detail( "State", "Active" )
				.trackLatest( "DDTrackerStarting" );
		}

		return Void();
	}

	ACTOR static Future<Void> interruptableBuildTeams( DDTeamCollection* self ) {
		if(!self->addSubsetComplete.isSet()) {
			wait( addSubsetOfEmergencyTeams(self) );
			self->addSubsetComplete.send(Void());
		}

		loop {
			choose {
				when( wait( self->buildTeams( self ) ) ) {
					return Void();
				}
				when( wait( self->restartTeamBuilder.onTrigger() ) ) {}
			}
		}
	}

	ACTOR static Future<Void> checkBuildTeams( DDTeamCollection* self ) {
		wait( self->checkTeamDelay );
		while( !self->teamBuilder.isReady() )
			wait( self->teamBuilder );

		if( self->doBuildTeams && self->readyToStart.isReady() ) {
			self->doBuildTeams = false;
			self->teamBuilder = self->interruptableBuildTeams( self );
			wait( self->teamBuilder );
		}

		return Void();
	}

	// SOMEDAY: Make bestTeam better about deciding to leave a shard where it is (e.g. in PRIORITY_TEAM_HEALTHY case)
	//		    use keys, src, dest, metrics, priority, system load, etc.. to decide...
	ACTOR static Future<Void> getTeam( DDTeamCollection* self, GetTeamRequest req ) {
		try {
			wait( self->checkBuildTeams( self ) );

			// Select the best team
			// Currently the metric is minimum used disk space (adjusted for data in flight)
			// Only healthy teams may be selected. The team has to be healthy at the moment we update
			//   shardsAffectedByTeamFailure or we could be dropping a shard on the floor (since team
			//   tracking is "edge triggered")
			// SOMEDAY: Account for capacity, load (when shardMetrics load is high)

			// self->teams.size() can be 0 under the ConfigureTest.txt test when we change configurations
			// The situation happens rarely. We may want to eliminate this situation someday
			if( !self->teams.size() ) {
				req.reply.send( Optional<Reference<IDataDistributionTeam>>() );
				return Void();
			}

			int64_t bestLoadBytes = 0;
			Optional<Reference<IDataDistributionTeam>> bestOption;
			std::vector<std::pair<int, Reference<IDataDistributionTeam>>> randomTeams;
			std::set< UID > sources;

			if( !req.wantsNewServers ) {
				std::vector<Reference<IDataDistributionTeam>> similarTeams;
				bool foundExact = false;

				for( int i = 0; i < req.sources.size(); i++ )
					sources.insert( req.sources[i] );

				for( int i = 0; i < req.sources.size(); i++ ) {
					if( self->server_info.count( req.sources[i] ) ) {
						auto& teamList = self->server_info[ req.sources[i] ]->teams;
						for( int j = 0; j < teamList.size(); j++ ) {
							if( teamList[j]->isHealthy() && (!req.preferLowerUtilization || teamList[j]->hasHealthyFreeSpace())) {
								int sharedMembers = 0;
								for( const UID& id : teamList[j]->getServerIDs() )
									if( sources.count( id ) )
										sharedMembers++;

								if( !foundExact && sharedMembers == teamList[j]->size() ) {
									foundExact = true;
									bestOption = Optional<Reference<IDataDistributionTeam>>();
									similarTeams.clear();
								}

								if( (sharedMembers == teamList[j]->size()) || (!foundExact && req.wantsTrueBest) ) {
									int64_t loadBytes = SOME_SHARED * teamList[j]->getLoadBytes(true, req.inflightPenalty);
									if( !bestOption.present() || ( req.preferLowerUtilization && loadBytes < bestLoadBytes ) || ( !req.preferLowerUtilization && loadBytes > bestLoadBytes ) ) {
										bestLoadBytes = loadBytes;
										bestOption = teamList[j];
									}
								}
								else if( !req.wantsTrueBest && !foundExact )
									similarTeams.push_back( teamList[j] );
							}
						}
					}
				}

				if( foundExact || (req.wantsTrueBest && bestOption.present() ) ) {
					ASSERT( bestOption.present() );
					// Check the team size: be sure team size is correct
					ASSERT(bestOption.get()->size() == self->configuration.storageTeamSize);
					req.reply.send( bestOption );
					return Void();
				}

				if( !req.wantsTrueBest ) {
					while( similarTeams.size() && randomTeams.size() < SERVER_KNOBS->BEST_TEAM_OPTION_COUNT ) {
						int randomTeam = g_random->randomInt( 0, similarTeams.size() );
						randomTeams.push_back( std::make_pair( SOME_SHARED, similarTeams[randomTeam] ) );
						swapAndPop( &similarTeams, randomTeam );
					}
				}
			}

			if( req.wantsTrueBest ) {
				ASSERT( !bestOption.present() );
				for( int i = 0; i < self->teams.size(); i++ ) {
					if( self->teams[i]->isHealthy() && (!req.preferLowerUtilization || self->teams[i]->hasHealthyFreeSpace()) ) {
						int64_t loadBytes = NONE_SHARED * self->teams[i]->getLoadBytes(true, req.inflightPenalty);
						if( !bestOption.present() || ( req.preferLowerUtilization && loadBytes < bestLoadBytes ) || ( !req.preferLowerUtilization && loadBytes > bestLoadBytes ) ) {
							bestLoadBytes = loadBytes;
							bestOption = self->teams[i];
						}
					}
				}
			}
			else {
				int nTries = 0;
				while( randomTeams.size() < SERVER_KNOBS->BEST_TEAM_OPTION_COUNT && nTries < SERVER_KNOBS->BEST_TEAM_MAX_TEAM_TRIES ) {
					Reference<IDataDistributionTeam> dest = g_random->randomChoice(self->teams);

					bool ok = dest->isHealthy() && (!req.preferLowerUtilization || dest->hasHealthyFreeSpace());
					for(int i=0; ok && i<randomTeams.size(); i++)
						if (randomTeams[i].second->getServerIDs() == dest->getServerIDs())
							ok = false;

					if (ok)
						randomTeams.push_back( std::make_pair( NONE_SHARED, dest ) );
					else
						nTries++;
				}

				for( int i = 0; i < randomTeams.size(); i++ ) {
					int64_t loadBytes = randomTeams[i].first * randomTeams[i].second->getLoadBytes(true, req.inflightPenalty);
					if( !bestOption.present() || ( req.preferLowerUtilization && loadBytes < bestLoadBytes ) || ( !req.preferLowerUtilization && loadBytes > bestLoadBytes ) ) {
						bestLoadBytes = loadBytes;
						bestOption = randomTeams[i].second;
					}
				}
			}

			// Note: req.completeSources can be empty and all servers (and server teams) can be unhealthy.
			// We will get stuck at this! This only happens when a DC fails. No need to consider it right now.
			if(!bestOption.present() && self->zeroHealthyTeams->get()) {
				//Attempt to find the unhealthy source server team and return it
				std::set<UID> completeSources;
				for( int i = 0; i < req.completeSources.size(); i++ ) {
					completeSources.insert( req.completeSources[i] );
				}

				int bestSize = 0;
				for( int i = 0; i < req.completeSources.size(); i++ ) {
					if( self->server_info.count( req.completeSources[i] ) ) {
						auto& teamList = self->server_info[ req.completeSources[i] ]->teams;
						for( int j = 0; j < teamList.size(); j++ ) {
							bool found = true;
							auto serverIDs = teamList[j]->getServerIDs();
							for( int k = 0; k < teamList[j]->size(); k++ ) {
								if( !completeSources.count( serverIDs[k] ) ) {
									found = false;
									break;
								}
							}
							if(found && teamList[j]->size() > bestSize) {
								bestOption = teamList[j];
								bestSize = teamList[j]->size();
							}
						}
						break;
					}
				}
			}

			req.reply.send( bestOption );

			return Void();
		} catch( Error &e ) {
			if( e.code() != error_code_actor_cancelled)
				req.reply.sendError( e );
			throw;
		}
	}

	int64_t getDebugTotalDataInFlight() {
		int64_t total = 0;
		for(auto itr = server_info.begin(); itr != server_info.end(); ++itr)
			total += itr->second->dataInFlightToServer;
		return total;
	}

	ACTOR static Future<Void> addSubsetOfEmergencyTeams( DDTeamCollection* self ) {
		state int idx = 0;
		state std::vector<Reference<TCServerInfo>> servers;
		state std::vector<UID> serverIds;
		state Reference<LocalitySet> tempSet = Reference<LocalitySet>(new LocalityMap<UID>());
		state LocalityMap<UID>* tempMap = (LocalityMap<UID>*) tempSet.getPtr();

		for(; idx < self->badTeams.size(); idx++ ) {
			servers.clear();
			for(const auto& server : self->badTeams[idx]->getServers()) {
				if(server->inDesiredDC && !self->server_status.get(server->id).isUnhealthy()) {
					servers.push_back(server);
				}
			}

			// For the bad team that is too big (too many servers), we will try to find a subset of servers in the team
			// to construct a new healthy team, so that moving data to the new healthy team will not
			// cause too much data movement overhead
			// FIXME: This code logic can be simplified.
			if(servers.size() >= self->configuration.storageTeamSize) {
				bool foundTeam = false;
				for( int j = 0; j < servers.size() - self->configuration.storageTeamSize + 1 && !foundTeam; j++ ) {
					auto& serverTeams = servers[j]->teams;
					for( int k = 0; k < serverTeams.size(); k++ ) {
						auto &testTeam = serverTeams[k]->getServerIDs();
						bool allInTeam = true; // All servers in testTeam belong to the healthy servers
						for( int l = 0; l < testTeam.size(); l++ ) {
							bool foundServer = false;
							for( auto it : servers ) {
								if( it->id == testTeam[l] ) {
									foundServer = true;
									break;
								}
							}
							if(!foundServer) {
								allInTeam = false;
								break;
							}
						}
						if( allInTeam ) {
							foundTeam = true;
							break;
						}
					}
				}
				if( !foundTeam ) {
					if( self->satisfiesPolicy(servers) ) {
						if(servers.size() == self->configuration.storageTeamSize || self->satisfiesPolicy(servers, self->configuration.storageTeamSize)) {
							servers.resize(self->configuration.storageTeamSize);
							self->addTeam(servers, true);
							//self->traceTeamCollectionInfo(); // Trace at the end of the function
						} else {
							tempSet->clear();
							for( auto it : servers ) {
								tempMap->add(it->lastKnownInterface.locality, &it->id);
							}

							self->resultEntries.clear();
							self->forcedEntries.clear();
							bool result = tempSet->selectReplicas(self->configuration.storagePolicy, self->forcedEntries, self->resultEntries);
							ASSERT(result && self->resultEntries.size() == self->configuration.storageTeamSize);

							serverIds.clear();
							for(auto& it : self->resultEntries) {
								serverIds.push_back(*tempMap->getObject(it));
							}
							self->addTeam(serverIds.begin(), serverIds.end(), true);
						}
					} else {
						serverIds.clear();
						for(auto it : servers) {
							serverIds.push_back(it->id);
						}
						TraceEvent(SevWarnAlways, "CannotAddSubset", self->distributorId).detail("Servers", describe(serverIds));
					}
				}
			}
			wait( yield() );
		}

		// Trace and record the current number of teams for correctness test
		self->traceTeamCollectionInfo();

		return Void();
	}

	ACTOR static Future<Void> init( DDTeamCollection* self, Reference<InitialDataDistribution> initTeams ) {
		// SOMEDAY: If some servers have teams and not others (or some servers have more data than others) and there is an address/locality collision, should
		// we preferentially mark the least used server as undesirable?
		for (auto i = initTeams->allServers.begin(); i != initTeams->allServers.end(); ++i) {
			if (self->shouldHandleServer(i->first)) {
				self->addServer(i->first, i->second, self->serverTrackerErrorOut, 0);
			}
		}

		state std::set<std::vector<UID>>::iterator teamIter = self->primary ? initTeams->primaryTeams.begin() : initTeams->remoteTeams.begin();
		state std::set<std::vector<UID>>::iterator teamIterEnd = self->primary ? initTeams->primaryTeams.end() : initTeams->remoteTeams.end();
		for(; teamIter != teamIterEnd; ++teamIter) {
			self->addTeam(teamIter->begin(), teamIter->end(), true);
			wait( yield() );
		}

		return Void();
	}

	void evaluateTeamQuality() {
		int teamCount = teams.size(), serverCount = allServers.size();
		double teamsPerServer = (double)teamCount * configuration.storageTeamSize / serverCount;

		ASSERT( serverCount == server_info.size() );

		int minTeams = std::numeric_limits<int>::max();
		int maxTeams = std::numeric_limits<int>::min();
		double varTeams = 0;

		std::map<Optional<Standalone<StringRef>>, int> machineTeams;
		for(auto s = server_info.begin(); s != server_info.end(); ++s) {
			if(!server_status.get(s->first).isUnhealthy()) {
				int stc = s->second->teams.size();
				minTeams = std::min(minTeams, stc);
				maxTeams = std::max(maxTeams, stc);
				varTeams += (stc - teamsPerServer)*(stc - teamsPerServer);
				// Use zoneId as server's machine id
				machineTeams[s->second->lastKnownInterface.locality.zoneId()] += stc;
			}
		}
		varTeams /= teamsPerServer*teamsPerServer;

		int minMachineTeams = std::numeric_limits<int>::max();
		int maxMachineTeams = std::numeric_limits<int>::min();
		for( auto m = machineTeams.begin(); m != machineTeams.end(); ++m ) {
			minMachineTeams = std::min( minMachineTeams, m->second );
			maxMachineTeams = std::max( maxMachineTeams, m->second );
		}

		TraceEvent(
			minTeams>0 ? SevInfo : SevWarn,
			"DataDistributionTeamQuality", distributorId)
			.detail("Servers", serverCount)
			.detail("Teams", teamCount)
			.detail("TeamsPerServer", teamsPerServer)
			.detail("Variance", varTeams/serverCount)
			.detail("ServerMinTeams", minTeams)
			.detail("ServerMaxTeams", maxTeams)
			.detail("MachineMinTeams", minMachineTeams)
			.detail("MachineMaxTeams", maxMachineTeams);
	}

	bool teamExists( vector<UID> &team ) {
		if (team.empty()) {
			return false;
		}

		UID& serverID = team[0];
		for (auto& usedTeam : server_info[serverID]->teams) {
			if (team == usedTeam->getServerIDs()) {
				return true;
			}
		}

		return false;
	}

	// SOMEDAY: when machineTeams is changed from vector to set, we may check the existance faster
	bool machineTeamExists(vector<Standalone<StringRef>>& machineIDs) { return findMachineTeam(machineIDs).isValid(); }

	Reference<TCMachineTeamInfo> findMachineTeam(vector<Standalone<StringRef>>& machineIDs) {
		if (machineIDs.empty()) {
			return Reference<TCMachineTeamInfo>();
		}

		Standalone<StringRef> machineID = machineIDs[0];
		for (auto& machineTeam : machine_info[machineID]->machineTeams) {
			if (machineTeam->machineIDs == machineIDs) {
				return machineTeam;
			}
		}

		return Reference<TCMachineTeamInfo>();
	}

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
		vector<Reference<TCServerInfo>> newTeamServers;
		for (auto i = begin; i != end; ++i) {
			if (server_info.find(*i) != server_info.end()) {
				newTeamServers.push_back(server_info[*i]);
			}
		}

		addTeam(newTeamServers, isInitialTeam);
	}

	void addTeam(const vector<Reference<TCServerInfo>>& newTeamServers, bool isInitialTeam,
	             bool redundantTeam = false) {
		Reference<TCTeamInfo> teamInfo(new TCTeamInfo(newTeamServers));

		// Move satisfiesPolicy to the end for performance benefit
		bool badTeam = redundantTeam || teamInfo->size() != configuration.storageTeamSize
				|| !satisfiesPolicy(teamInfo->getServers());

		teamInfo->tracker = teamTracker(this, teamInfo, badTeam, redundantTeam);
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
		vector<Standalone<StringRef>> machineIDs;
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
	}

	void addTeam(std::set<UID> const& team, bool isInitialTeam) { addTeam(team.begin(), team.end(), isInitialTeam); }

	// Add a machine team specified by input machines
	Reference<TCMachineTeamInfo> addMachineTeam(vector<Reference<TCMachineInfo>> machines) {
		Reference<TCMachineTeamInfo> machineTeamInfo(new TCMachineTeamInfo(machines));
		machineTeams.push_back(machineTeamInfo);

		// Assign machine teams to machine
		for (auto machine : machines) {
			// A machine's machineTeams vector should not hold duplicate machineTeam members
			ASSERT_WE_THINK(std::count(machine->machineTeams.begin(), machine->machineTeams.end(), machineTeamInfo)==0);
			machine->machineTeams.push_back(machineTeamInfo);
		}

		return machineTeamInfo;
	}

	// Add a machine team by using the machineIDs from begin to end
	Reference<TCMachineTeamInfo> addMachineTeam(vector<Standalone<StringRef>>::iterator begin,
	                                            vector<Standalone<StringRef>>::iterator end) {
		vector<Reference<TCMachineInfo>> machines;

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
		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				checkAndCreateMachine(i->second);
				totalServerIndex++;
			}
		}

		return totalServerIndex;
	}

	void traceConfigInfo() {
		TraceEvent("DDConfig")
		    .detail("StorageTeamSize", configuration.storageTeamSize)
		    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER)
		    .detail("MaxTeamsPerServer", SERVER_KNOBS->MAX_TEAMS_PER_SERVER);
	}

	void traceServerInfo() {
		int i = 0;

		TraceEvent("ServerInfo").detail("Size", server_info.size());
		for (auto& server : server_info) {
			const UID& uid = server.first;
			TraceEvent("ServerInfo")
			    .detail("ServerInfoIndex", i++)
			    .detail("ServerID", server.first.toString())
			    .detail("ServerTeamOwned", server.second->teams.size())
			    .detail("MachineID", server.second->machine->machineID.contents().toString());
		}
		for (auto& server : server_info) {
			const UID& uid = server.first;
			TraceEvent("ServerStatus", uid)
			    .detail("Healthy", !server_status.get(uid).isUnhealthy())
			    .detail("MachineIsValid", server_info[uid]->machine.isValid())
			    .detail("MachineTeamSize",
			            server_info[uid]->machine.isValid() ? server_info[uid]->machine->machineTeams.size() : -1);
		}
	}

	void traceServerTeamInfo() {
		int i = 0;

		TraceEvent("ServerTeamInfo").detail("Size", teams.size());
		for (auto& team : teams) {
			TraceEvent("ServerTeamInfo")
			    .detail("TeamIndex", i++)
			    .detail("Healthy", team->isHealthy())
			    .detail("ServerNumber", team->size())
			    .detail("MemberIDs", team->getServerIDsStr());
		}
	}

	void traceMachineInfo() {
		int i = 0;

		TraceEvent("MachineInfo").detail("Size", machine_info.size());
		for (auto& machine : machine_info) {
			TraceEvent("MachineInfo")
			    .detail("MachineInfoIndex", i++)
			    .detail("Healthy", isMachineHealthy(machine.second))
			    .detail("MachineID", machine.first.contents().toString())
			    .detail("MachineTeamOwned", machine.second->machineTeams.size())
			    .detail("ServerNumOnMachine", machine.second->serversOnMachine.size())
			    .detail("ServersID", machine.second->getServersIDStr());
		}
	}

	void traceMachineTeamInfo() {
		int i = 0;

		TraceEvent("MachineTeamInfo").detail("Size", machineTeams.size());
		for (auto& team : machineTeams) {
			TraceEvent("MachineTeamInfo")
			    .detail("TeamIndex", i++)
			    .detail("MachineIDs", team->getMachineIDsStr())
			    .detail("ServerTeamNumber", team->serverTeams.size());
		}
	}

	void traceMachineLocalityMap() {
		int i = 0;

		TraceEvent("MachineLocalityMap").detail("Size", machineLocalityMap.size());
		for (auto& uid : machineLocalityMap.getObjects()) {
			Reference<LocalityRecord> record = machineLocalityMap.getRecord(i);
			if (record.isValid()) {
				TraceEvent("MachineLocalityMap")
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
	void traceAllInfo(bool shouldPrint = false) {
		if (!shouldPrint) return;

		TraceEvent("TraceAllInfo").detail("Primary", primary);
		traceConfigInfo();
		traceServerInfo();
		traceServerTeamInfo();
		traceMachineInfo();
		traceMachineTeamInfo();
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
	int addBestMachineTeams(int targetMachineTeamsToBuild) {
		int addedMachineTeams = 0;
		int totalServerIndex = 0;
		int machineTeamsToBuild = 0;

		ASSERT(targetMachineTeamsToBuild >= 0);
		// Not build any machine team if asked to build none
		if (targetMachineTeamsToBuild == 0) return 0;

		machineTeamsToBuild = targetMachineTeamsToBuild;

		// The number of machines is always no smaller than the storageTeamSize in a correct configuration
		ASSERT(machine_info.size() >= configuration.storageTeamSize);

		// Step 1: Create machineLocalityMap which will be used in building machine team
		rebuildMachineLocalityMap();

		int loopCount = 0;
		// Add a team in each iteration
		while (addedMachineTeams < machineTeamsToBuild) {
			// Step 2: Get least used machines from which we choose machines as a machine team
			std::vector<Reference<TCMachineInfo>> leastUsedMachines; // A less used machine has less number of teams
			int minTeamCount = std::numeric_limits<int>::max();
			for (auto& machine : machine_info) {
				// Skip invalid machine whose representative server is not in server_info
				ASSERT_WE_THINK(server_info.find(machine.second->serversOnMachine[0]->id) != server_info.end());
				// Skip unhealthy machines
				if (!isMachineHealthy(machine.second)) continue;

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

			// Step 3: Create a representative process for each machine.
			// Construct forcedAttribute from leastUsedMachines.
			// We will use forcedAttribute to call existing function to form a team
			if (leastUsedMachines.size()) {
				// Randomly choose 1 least used machine
				Reference<TCMachineInfo> tcMachineInfo = g_random->randomChoice(leastUsedMachines);
				ASSERT(!tcMachineInfo->serversOnMachine.empty());
				LocalityEntry process = tcMachineInfo->localityEntry;
				forcedAttributes.push_back(process);
			} else {
				// when leastUsedMachine is empty, we will never find a team later, so we can simply return.
				return addedMachineTeams;
			}

			// Step 4: Reuse Policy's selectReplicas() to create team for the representative process.
			std::vector<UID*> bestTeam;
			int bestScore = std::numeric_limits<int>::max();
			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT; // BEST_OF_AMT = 4
			for (int i = 0; i < maxAttempts && i < 100; ++i) {
				// Choose a team that balances the # of teams per server among the teams
				// that have the least-utilized server
				team.clear();
				auto success = machineLocalityMap.selectReplicas(configuration.storagePolicy, forcedAttributes, team);
				if (!success) {
					break;
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
				vector<Standalone<StringRef>> machineIDs;
				for (auto process = team.begin(); process != team.end(); process++) {
					Reference<TCServerInfo> server = server_info[**process];
					score += server->machine->machineTeams.size();
					Standalone<StringRef> machine_id = server->lastKnownInterface.locality.zoneId().get();
					machineIDs.push_back(machine_id);
				}

				// Only choose healthy machines into machine team
				ASSERT_WE_THINK(isMachineTeamHealthy(machineIDs));

				std::sort(machineIDs.begin(), machineIDs.end());
				if (machineTeamExists(machineIDs)) {
					maxAttempts += 1;
					continue;
				}

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
				vector<Reference<TCMachineInfo>> machines;
				for (auto process = bestTeam.begin(); process < bestTeam.end(); process++) {
					Reference<TCMachineInfo> machine = server_info[**process]->machine;
					machines.push_back(machine);
				}

				addMachineTeam(machines);
				addedMachineTeams++;
			} else {
				TraceEvent(SevWarn, "DataDistributionBuildTeams", distributorId)
				    .detail("Primary", primary)
				    .detail("Reason", "Unable to make desired machine Teams");
				break;
			}

			if (++loopCount > 2 * machineTeamsToBuild * (configuration.storageTeamSize + 1)) {
				break;
			}
		}

		return addedMachineTeams;
	}

	bool isMachineTeamHealthy(vector<Standalone<StringRef>> const& machineIDs) {
		int healthyNum = 0;

		// A healthy machine team should have the desired number of machines
		if (machineIDs.size() != configuration.storageTeamSize) return false;

		for (auto& id : machineIDs) {
			auto& machine = machine_info[id];
			if (isMachineHealthy(machine)) {
				healthyNum++;
			}
		}
		return (healthyNum == machineIDs.size());
	}

	bool isMachineTeamHealthy(Reference<TCMachineTeamInfo> const& machineTeam) {
		int healthyNum = 0;

		// A healthy machine team should have the desired number of machines
		if (machineTeam->size() != configuration.storageTeamSize) return false;

		for (auto& machine : machineTeam->machines) {
			if (isMachineHealthy(machine)) {
				healthyNum++;
			}
		}
		return (healthyNum == machineTeam->machines.size());
	}

	bool isMachineHealthy(Reference<TCMachineInfo> const& machine) {
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
	Reference<TCServerInfo> findOneLeastUsedServer() {
		vector<Reference<TCServerInfo>> leastUsedServers;
		int minTeamNumber = std::numeric_limits<int>::max();
		for (auto& server : server_info) {
			// Only pick healthy server, which is not failed or excluded.
			if (server_status.get(server.first).isUnhealthy()) continue;

			int numTeams = server.second->teams.size();
			if (numTeams < minTeamNumber) {
				minTeamNumber = numTeams;
				leastUsedServers.clear();
			}
			if (minTeamNumber == numTeams) {
				leastUsedServers.push_back(server.second);
			}
		}

		return g_random->randomChoice(leastUsedServers);
	}

	// Randomly choose one machine team that has chosenServer and has the correct size
	// When configuration is changed, we may have machine teams with old storageTeamSize
	Reference<TCMachineTeamInfo> findOneRandomMachineTeam(Reference<TCServerInfo> chosenServer) {
		if (!chosenServer->machine->machineTeams.empty()) {
			std::vector<Reference<TCMachineTeamInfo>> healthyMachineTeamsForChosenServer;
			for (auto& mt : chosenServer->machine->machineTeams) {
				if (isMachineTeamHealthy(mt)) {
					healthyMachineTeamsForChosenServer.push_back(mt);
				}
			}
			if (!healthyMachineTeamsForChosenServer.empty()) {
				return g_random->randomChoice(healthyMachineTeamsForChosenServer);
			}
		}

		// If we cannot find a healthy machine team
		TraceEvent("NoHealthyMachineTeamForServer")
		    .detail("ServerID", chosenServer->id)
		    .detail("MachineTeamsNumber", chosenServer->machine->machineTeams.size());
		return Reference<TCMachineTeamInfo>();
	}

	// A server team should always come from servers on a machine team
	// Check if it is true
	bool isOnSameMachineTeam(Reference<TCTeamInfo>& team) {
		std::vector<Standalone<StringRef>> machineIDs;
		for (const auto& server : team->getServers()) {
			if (!server->machine.isValid()) return false;
			machineIDs.push_back(server->machine->machineID);
		}
		std::sort(machineIDs.begin(), machineIDs.end());

		int numExistance = 0;
		for (const auto& server : team->getServers()) {
			for (const auto& candidateMachineTeam : server->machine->machineTeams) {
				std::sort(candidateMachineTeam->machineIDs.begin(), candidateMachineTeam->machineIDs.end());
				if (machineIDs == candidateMachineTeam->machineIDs) {
					numExistance++;
					break;
				}
			}
		}
		return (numExistance == team->size());
	}

	// Sanity check the property of teams in unit test
	// Return true if all server teams belong to machine teams
	bool sanityCheckTeams() {
		for (auto& team : teams) {
			if (isOnSameMachineTeam(team) == false) {
				return false;
			}
		}

		return true;
	}

	int calculateHealthyServerCount() {
		int serverCount = 0;
		for (auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				++serverCount;
			}
		}
		return serverCount;
	}

	int calculateHealthyMachineCount() {
		int totalHealthyMachineCount = 0;
		for (auto& m : machine_info) {
			if (isMachineHealthy(m.second)) {
				++totalHealthyMachineCount;
			}
		}

		return totalHealthyMachineCount;
	}

	// Sanity check
	bool isServerTeamNumberCorrect(Reference<TCMachineTeamInfo>& mt) {
		int num = 0;
		bool ret = true;
		for (auto& team : teams) {
			if (team->machineTeam->machineIDs == mt->machineIDs) {
				++num;
			}
		}
		if (num != mt->serverTeams.size()) {
			ret = false;
			TraceEvent(SevError, "ServerTeamNumberOnMachineIncorrect")
			    .detail("MachineTeam", mt->getMachineIDsStr())
			    .detail("ServerTeamsSize", mt->serverTeams.size())
			    .detail("CountedServerTeamNumber", num);
		}
		return ret;
	}

	// Find the machine team with the least number of server teams
	std::pair<Reference<TCMachineTeamInfo>, int> getMachineTeamWithLeastProcessTeams() {
		Reference<TCMachineTeamInfo> retMT;
		int minNumProcessTeams = std::numeric_limits<int>::max();

		for (auto& mt : machineTeams) {
			if (EXPENSIVE_VALIDATION) {
				ASSERT(isServerTeamNumberCorrect(mt));
			}
			if (mt->serverTeams.size() < minNumProcessTeams) {
				minNumProcessTeams = mt->serverTeams.size();
				retMT = mt;
			}
		}

		return std::pair<Reference<TCMachineTeamInfo>, int>(retMT, minNumProcessTeams);
	}

	int getHealthyMachineTeamCount() {
		int healthyTeamCount = 0;
		for (auto mt = machineTeams.begin(); mt != machineTeams.end(); ++mt) {
			ASSERT((*mt)->machines.size() == configuration.storageTeamSize);

			if (isMachineTeamHealthy(*mt)) {
				++healthyTeamCount;
			}
		}

		return healthyTeamCount;
	}

	// Create server teams based on machine teams
	// Before the number of machine teams reaches the threshold, build a machine team for each server team
	// When it reaches the threshold, first try to build a server team with existing machine teams; if failed,
	// build an extra machine team and record the event in trace
	int addTeamsBestOf(int teamsToBuild, int desiredTeamNumber, int maxTeamNumber) {
		ASSERT(teamsToBuild > 0);
		ASSERT_WE_THINK(machine_info.size() > 0 || server_info.size() == 0);

		int addedMachineTeams = 0;
		int addedTeams = 0;
		int loopCount = 0;

		// Exclude machine teams who have members in the wrong configuration.
		// When we change configuration, we may have machine teams with storageTeamSize in the old configuration.
		int healthyMachineTeamCount = getHealthyMachineTeamCount();
		int totalMachineTeamCount = machineTeams.size();

		int totalHealthyMachineCount = calculateHealthyMachineCount();

		int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
		int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
		// machineTeamsToBuild mimics how the teamsToBuild is calculated in buildTeams()
		int machineTeamsToBuild =
		    std::min(desiredMachineTeams - healthyMachineTeamCount, maxMachineTeams - totalMachineTeamCount);

		TraceEvent("BuildMachineTeams")
		    .detail("TotalHealthyMachine", totalHealthyMachineCount)
		    .detail("HealthyMachineTeamCount", healthyMachineTeamCount)
		    .detail("DesiredMachineTeams", desiredMachineTeams)
		    .detail("MaxMachineTeams", maxMachineTeams)
		    .detail("MachineTeamsToBuild", machineTeamsToBuild);
		// Pre-build all machine teams until we have the desired number of machine teams
		if (machineTeamsToBuild > 0) {
			addedMachineTeams = addBestMachineTeams(machineTeamsToBuild);
		}

		while (addedTeams < teamsToBuild) {
			// Step 1: Create 1 best machine team
			std::vector<UID> bestServerTeam;
			int bestScore = std::numeric_limits<int>::max();
			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT; // BEST_OF_AMT = 4
			for (int i = 0; i < maxAttempts && i < 100; ++i) {
				// Step 2: Choose 1 least used server and then choose 1 least used machine team from the server
				Reference<TCServerInfo> chosenServer = findOneLeastUsedServer();
				// Note: To avoid creating correlation of picked machine teams, we simply choose a random machine team
				// instead of choosing the least used machine team.
				// The correlation happens, for example, when we add two new machines, we may always choose the machine
				// team with these two new machines because they are typically less used.
				Reference<TCMachineTeamInfo> chosenMachineTeam = findOneRandomMachineTeam(chosenServer);

				if (!chosenMachineTeam.isValid()) {
					// We may face the situation that temporarily we have no healthy machine.
					TraceEvent(SevWarn, "MachineTeamNotFound")
					    .detail("Primary", primary)
					    .detail("MachineTeamNumber", machineTeams.size());
					continue; // try randomly to find another least used server
				}

				// From here, chosenMachineTeam must have a healthy server team
				// Step 3: Randomly pick 1 server from each machine in the chosen machine team to form a server team
				vector<UID> serverTeam;
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
						serverID = g_random->randomChoice(healthyProcesses)->id;
					}
					serverTeam.push_back(serverID);
				}

				ASSERT(chosenServerCount == 1); // chosenServer should be used exactly once
				ASSERT(serverTeam.size() == configuration.storageTeamSize);

				std::sort(serverTeam.begin(), serverTeam.end());
				if (teamExists(serverTeam)) {
					maxAttempts += 1;
					continue;
				}

				// Pick the server team with smallest score in all attempts
				// SOMEDAY: Improve the code efficiency by using reservoir algorithm
				int score = 0;
				for (auto& server : serverTeam) {
					score += server_info[server]->teams.size();
				}
				TraceEvent("BuildServerTeams")
				    .detail("Score", score)
				    .detail("BestScore", bestScore)
				    .detail("TeamSize", serverTeam.size())
				    .detail("StorageTeamSize", configuration.storageTeamSize);
				if (score < bestScore) {
					bestScore = score;
					bestServerTeam = serverTeam;
				}
			}

			if (bestServerTeam.size() != configuration.storageTeamSize) {
				// Not find any team and will unlikely find a team
				break;
			}

			// Step 4: Add the server team
			addTeam(bestServerTeam.begin(), bestServerTeam.end(), false);
			addedTeams++;

			if (++loopCount > 2 * teamsToBuild * (configuration.storageTeamSize + 1)) {
				break;
			}
		}

		healthyMachineTeamCount = getHealthyMachineTeamCount();

		TraceEvent("TeamCollectionInfo", distributorId)
		    .detail("Primary", primary)
		    .detail("AddedTeamNumber", addedTeams)
		    .detail("AimToBuildTeamNumber", teamsToBuild)
		    .detail("CurrentTeamNumber", teams.size())
		    .detail("DesiredTeamNumber", desiredTeamNumber)
		    .detail("MaxTeamNumber", maxTeamNumber)
		    .detail("StorageTeamSize", configuration.storageTeamSize)
		    .detail("CurrentMachineTeamNumber", machineTeams.size())
		    .detail("CurrentHealthyMachineTeamNumber", healthyMachineTeamCount)
		    .detail("DesiredMachineTeams", desiredMachineTeams)
		    .detail("MaxMachineTeams", maxMachineTeams)
		    .detail("TotalHealthyMachine", totalHealthyMachineCount)
		    .trackLatest("TeamCollectionInfo");

		return addedTeams;
	}

	// Check if the number of server (and machine teams) is larger than the maximum allowed number
	void traceTeamCollectionInfo() {
		int totalHealthyServerCount = calculateHealthyServerCount();
		int desiredServerTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyServerCount;
		int maxServerTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyServerCount;

		int totalHealthyMachineCount = calculateHealthyMachineCount();
		int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
		int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
		int healthyMachineTeamCount = getHealthyMachineTeamCount();

		TraceEvent("TeamCollectionInfo", distributorId)
		    .detail("Primary", primary)
		    .detail("AddedTeamNumber", 0)
		    .detail("AimToBuildTeamNumber", 0)
		    .detail("CurrentTeamNumber", teams.size())
		    .detail("DesiredTeamNumber", desiredServerTeams)
		    .detail("MaxTeamNumber", maxServerTeams)
		    .detail("StorageTeamSize", configuration.storageTeamSize)
		    .detail("CurrentMachineTeamNumber", machineTeams.size())
		    .detail("CurrentHealthyMachineTeamNumber", healthyMachineTeamCount)
		    .detail("DesiredMachineTeams", desiredMachineTeams)
		    .detail("MaxMachineTeams", maxMachineTeams)
		    .detail("TotalHealthyMachine", totalHealthyMachineCount)
		    .trackLatest("TeamCollectionInfo");

		// Debug purpose
//		if (healthyMachineTeamCount > desiredMachineTeams || machineTeams.size() > maxMachineTeams) {
//			// When the number of machine teams is over the limit, print out the current team info.
//			traceAllInfo(true);
//		}

	}

	// Use the current set of known processes (from server_info) to compute an optimized set of storage server teams.
	// The following are guarantees of the process:
	//   - Each newly-built team will meet the replication policy
	//   - All newly-built teams will have exactly teamSize machines
	//
	// buildTeams() only ever adds teams to the list of teams. Teams are only removed from the list when all data has been removed.
	//
	// buildTeams will not count teams larger than teamSize against the desired teams.
	ACTOR static Future<Void> buildTeams( DDTeamCollection* self ) {
		state int desiredTeams;
		int serverCount = 0;
		int uniqueDataCenters = 0;
		int uniqueMachines = 0;
		std::set<Optional<Standalone<StringRef>>> machines;

		for (auto i = self->server_info.begin(); i != self->server_info.end(); ++i) {
			if (!self->server_status.get(i->first).isUnhealthy()) {
				++serverCount;
				LocalityData& serverLocation = i->second->lastKnownInterface.locality;
				machines.insert( serverLocation.zoneId() );
			}
		}
		uniqueMachines = machines.size();
		TraceEvent("BuildTeams")
			.detail("ServerNumber", self->server_info.size())
			.detail("UniqueMachines", uniqueMachines)
			.detail("Primary", self->primary)
			.detail("StorageTeamSize", self->configuration.storageTeamSize);

		// If there are too few machines to even build teams or there are too few represented datacenters, build no new teams
		if( uniqueMachines >= self->configuration.storageTeamSize ) {
			desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER*serverCount;
			int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER*serverCount;

			// Exclude teams who have members in the wrong configuration, since we don't want these teams
			int teamCount = 0;
			int totalTeamCount = 0;
			for (int i = 0; i < self->teams.size(); ++i) {
				if (!self->teams[i]->isWrongConfiguration()) {
					if( self->teams[i]->isHealthy() ) {
						teamCount++;
					}
					totalTeamCount++;
				}
			}

			// teamsToBuild is calculated such that we will not build too many teams in the situation
			// when all (or most of) teams become unhealthy temporarily and then healthy again
			state int teamsToBuild = std::min(desiredTeams - teamCount, maxTeams - totalTeamCount);

			TraceEvent("BuildTeamsBegin", self->distributorId)
			    .detail("TeamsToBuild", teamsToBuild)
			    .detail("DesiredTeams", desiredTeams)
			    .detail("MaxTeams", maxTeams)
			    .detail("BadTeams", self->badTeams.size())
			    .detail("UniqueMachines", uniqueMachines)
			    .detail("TeamSize", self->configuration.storageTeamSize)
			    .detail("Servers", serverCount)
			    .detail("CurrentTrackedTeams", self->teams.size())
			    .detail("HealthyTeamCount", teamCount)
			    .detail("TotalTeamCount", totalTeamCount)
			    .detail("MachineTeamCount", self->machineTeams.size())
			    .detail("MachineCount", self->machine_info.size())
			    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER);

			if (teamsToBuild > 0) {
				state vector<std::vector<UID>> builtTeams;

				// addTeamsBestOf() will not add more teams than needed.
				// If the team number is more than the desired, the extra teams are added in the code path when
				// a team is added as an initial team
				int addedTeams = self->addTeamsBestOf(teamsToBuild, desiredTeams, maxTeams);

				if (addedTeams <= 0 && self->teams.size() == 0) {
					TraceEvent(SevWarn, "NoTeamAfterBuildTeam")
						.detail("TeamNum", self->teams.size())
						.detail("Debug", "Check information below");
					// Debug: set true for traceAllInfo() to print out more information
					self->traceAllInfo();
				}
			} else {
				int totalHealthyMachineCount = self->calculateHealthyMachineCount();

				int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * totalHealthyMachineCount;
				int maxMachineTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * totalHealthyMachineCount;
				int healthyMachineTeamCount = self->getHealthyMachineTeamCount();

				TraceEvent("TeamCollectionInfo", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("AddedTeamNumber", 0)
				    .detail("AimToBuildTeamNumber", teamsToBuild)
				    .detail("CurrentTeamNumber", self->teams.size())
				    .detail("DesiredTeamNumber", desiredTeams)
				    .detail("MaxTeamNumber", maxTeams)
				    .detail("StorageTeamSize", self->configuration.storageTeamSize)
				    .detail("CurrentMachineTeamNumber", self->machineTeams.size())
				    .detail("CurrentHealthyMachineTeamNumber", healthyMachineTeamCount)
				    .detail("DesiredMachineTeams", desiredMachineTeams)
				    .detail("MaxMachineTeams", maxMachineTeams)
				    .detail("TotalHealthyMachine", totalHealthyMachineCount)
				    .trackLatest("TeamCollectionInfo");
			}
		}

		self->evaluateTeamQuality();

		//Building teams can cause servers to become undesired, which can make teams unhealthy.
		//Let all of these changes get worked out before responding to the get team request
		wait( delay(0, TaskDataDistributionLaunch) );

		return Void();
	}

	void noHealthyTeams() {
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
			.detail("CurrentTeamCount", teams.size())
			.detail("ServerCount", server_info.size())
			.detail("NonFailedServerCount", desiredServerSet.size());
	}

	bool shouldHandleServer(const StorageServerInterface &newServer) {
		return (includedDCs.empty() ||
		        std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end() ||
		        (otherTrackedDCs.present() && std::find(otherTrackedDCs.get().begin(), otherTrackedDCs.get().end(),
		                                                newServer.locality.dcId()) == otherTrackedDCs.get().end()));
	}

	void addServer( StorageServerInterface newServer, ProcessClass processClass, Promise<Void> errorOut, Version addedVersion ) {
		if (!shouldHandleServer(newServer)) {
			return;
		}
		allServers.push_back( newServer.id() );

		TraceEvent("AddedStorageServer", distributorId).detail("ServerID", newServer.id()).detail("ProcessClass", processClass.toString()).detail("WaitFailureToken", newServer.waitFailure.getEndpoint().token).detail("Address", newServer.waitFailure.getEndpoint().getPrimaryAddress());
		auto &r = server_info[newServer.id()] = Reference<TCServerInfo>( new TCServerInfo( newServer, processClass, includedDCs.empty() || std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end(), storageServerSet ) );

		// Establish the relation between server and machine
		checkAndCreateMachine(r);

		r->tracker = storageServerTracker( this, cx, r.getPtr(), errorOut, addedVersion );
		doBuildTeams = true; // Adding a new server triggers to build new teams
		restartTeamBuilder.trigger();
	}

	bool removeTeam( Reference<TCTeamInfo> team ) {
		TraceEvent("RemovedTeam", distributorId).detail("Team", team->getDesc());
		bool found = false;
		for(int t=0; t<teams.size(); t++) {
			if( teams[t] == team ) {
				teams[t--] = teams.back();
				teams.pop_back();
				found = true;
				break;
			}
		}

		for(const auto& server : team->getServers()) {
			for(int t = 0; t<server->teams.size(); t++) {
				if( server->teams[t] == team ) {
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
		return found;
	}

	// Check if the server belongs to a machine; if not, create the machine.
	// Establish the two-direction link between server and machine
	Reference<TCMachineInfo> checkAndCreateMachine(Reference<TCServerInfo> server) {
		ASSERT(server.isValid() && server_info.find(server->id) != server_info.end());
		auto& locality = server->lastKnownInterface.locality;
		Standalone<StringRef> machine_id = locality.zoneId().get(); // locality to machine_id with std::string type

		Reference<TCMachineInfo> machineInfo;
		if (machine_info.find(machine_id) ==
		    machine_info.end()) { // uid is the first storage server process on the machine
			TEST(true);
			// For each machine, store the first server's localityEntry into machineInfo for later use.
			LocalityEntry localityEntry = machineLocalityMap.add(locality, &server->id);
			machineInfo = Reference<TCMachineInfo>(new TCMachineInfo(server, localityEntry));
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
	void removeMachine(DDTeamCollection* self, Reference<TCMachineInfo> removedMachineInfo) {
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
				if (std::count(machineTeam->machineIDs.begin(), machineTeam->machineIDs.end(),
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
			if (std::count(machineTeam->machineIDs.begin(), machineTeam->machineIDs.end(),
			               removedMachineInfo->machineID)) {
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

	void removeServer(DDTeamCollection* self, UID removedServer) {
		TraceEvent("RemovedStorageServer", distributorId).detail("ServerID", removedServer);

		// ASSERT( !shardsAffectedByTeamFailure->getServersForTeam( t ) for all t in teams that contain removedServer )
		Reference<TCServerInfo> removedServerInfo = server_info[removedServer];

		// Step: Remove server team that relate to removedServer
		// Find all servers with which the removedServer shares teams
		std::set<UID> serversWithAjoiningTeams;
		auto& sharedTeams = removedServerInfo->teams;
		for (int i = 0; i < sharedTeams.size(); ++i) {
			auto& teamIds = sharedTeams[i]->getServerIDs();
			serversWithAjoiningTeams.insert( teamIds.begin(), teamIds.end() );
		}
		serversWithAjoiningTeams.erase( removedServer );

		// For each server in a team with the removedServer, erase shared teams from the list of teams in that other server
		for( auto it = serversWithAjoiningTeams.begin(); it != serversWithAjoiningTeams.end(); ++it ) {
			auto& serverTeams = server_info[*it]->teams;
			for (int t = 0; t < serverTeams.size(); t++) {
				auto& serverIds = serverTeams[t]->getServerIDs();
				if ( std::count( serverIds.begin(), serverIds.end(), removedServer ) ) {
					serverTeams[t--] = serverTeams.back();
					serverTeams.pop_back();
				}
			}
		}

		// Step: Remove all teams that contain removedServer
		// SOMEDAY: can we avoid walking through all teams, since we have an index of teams in which removedServer participated
		int removedCount = 0;
		for (int t = 0; t < teams.size(); t++) {
			if ( std::count( teams[t]->getServerIDs().begin(), teams[t]->getServerIDs().end(), removedServer ) ) {
				TraceEvent("TeamRemoved")
				    .detail("Primary", primary)
				    .detail("TeamServerIDs", teams[t]->getServerIDsStr());
				// removeTeam also needs to remove the team from the machine team info.
				removeTeam(teams[t]);
				t--;
				removedCount++;
			}
		}

		if (removedCount == 0) {
			TraceEvent(SevInfo, "NoneTeamRemovedWhenServerRemoved")
			    .detail("Primary", primary)
			    .detail("Debug", "ThisShouldRarelyHappen_CheckInfoBelow");
			traceAllInfo();
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
			removeMachine(self, removedMachineInfo);
		}

		// If the machine uses removedServer's locality and the machine still has servers, the the machine's
		// representative server will be updated when it is used in addBestMachineTeams()
		// Note that since we do not rebuildMachineLocalityMap() here, the machineLocalityMap can be stale.
		// This is ok as long as we do not arbitrarily validate if machine team satisfies replication policy.

		// Step: Remove removedServer from server's global data
		for (int s = 0; s < allServers.size(); s++) {
			if (allServers[s] == removedServer) {
				allServers[s--] = allServers.back();
				allServers.pop_back();
			}
		}
		server_info.erase( removedServer );

		if(server_status.get(removedServer).initialized && server_status.get(removedServer).isUnhealthy()) {
			unhealthyServers--;
		}
		server_status.clear( removedServer );

		//FIXME: add remove support to localitySet so we do not have to recreate it
		resetLocalitySet();

		doBuildTeams = true;
		restartTeamBuilder.trigger();

		TraceEvent("DataDistributionTeamCollectionUpdate", distributorId)
		    .detail("Teams", teams.size())
		    .detail("BadTeams", badTeams.size())
		    .detail("Servers", allServers.size())
		    .detail("Machines", machine_info.size())
		    .detail("MachineTeams", machineTeams.size())
		    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER);
	}
};

ACTOR Future<Void> waitUntilHealthy(DDTeamCollection* self) {
	loop {
		while(self->zeroHealthyTeams->get() || self->processingUnhealthy->get()) {
			TraceEvent("WaitUntilHealthyStalled", self->distributorId).detail("Primary", self->primary).detail("ZeroHealthy", self->zeroHealthyTeams->get()).detail("ProcessingUnhealthy", self->processingUnhealthy->get());
			wait(self->zeroHealthyTeams->onChange() || self->processingUnhealthy->onChange());
		}
		wait(delay(SERVER_KNOBS->DD_STALL_CHECK_DELAY, TaskLowPriority)); //After the team trackers wait on the initial failure reaction delay, they yield. We want to make sure every tracker has had the opportunity to send their relocations to the queue.
		if(!self->zeroHealthyTeams->get() && !self->processingUnhealthy->get()) {
			return Void();
		}
	}
}

ACTOR Future<Void> removeBadTeams(DDTeamCollection* self) {
	wait(self->initialFailureReactionDelay);
	wait(waitUntilHealthy(self));
	wait(self->addSubsetComplete.getFuture());
	TraceEvent("DDRemovingBadTeams", self->distributorId).detail("Primary", self->primary);
	for(auto it : self->badTeams) {
		it->tracker.cancel();
	}
	self->badTeams.clear();
	return Void();
}

ACTOR Future<Void> teamRemover(DDTeamCollection* self) {
	state int numMachineTeamRemoved = 0;
	loop {
		// In case the teamRemover cause problems in production, we can disable it
		if (SERVER_KNOBS->TR_FLAG_DISABLE_TEAM_REMOVER) {
			return Void(); // Directly return Void()
		}

		wait(waitUntilHealthy(self));

		// To avoid removing machine teams too fast, which is unlikely happen though
		wait( delay(SERVER_KNOBS->TR_REMOVE_MACHINE_TEAM_DELAY) );

		// Wait for the badTeamRemover() to avoid the potential race between adding the bad team (add the team tracker)
		// and remove bad team (cancel the team tracker).
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
		//			    .detail("healthyMachineCount", healthyMachineCount)
		//			    .detail("MachineNumber", self->machine_info.size())
		//			    .detail("CurrentHealthyMTCount", currentHealthyMTCount)
		//			    .detail("MachineTeamNumber", self->machineTeams.size());
		//			self->traceAllInfo(true);
		//		}

		// In most cases, all machine teams should be healthy teams at this point.
		int desiredMachineTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * healthyMachineCount;
		int totalMTCount = self->machineTeams.size();

		if (totalMTCount > desiredMachineTeams) {
			// Pick the machine team with the least number of server teams and mark it undesired
			state std::pair<Reference<TCMachineTeamInfo>, int> foundMTInfo = self->getMachineTeamWithLeastProcessTeams();
			state Reference<TCMachineTeamInfo> mt = foundMTInfo.first;
			state int minNumProcessTeams = foundMTInfo.second;
			ASSERT(mt.isValid());

			// Pick one process team, and mark it as a bad team
			// Remove the machine by removing its process team one by one
			state Reference<TCTeamInfo> team;
			state int teamIndex = 0;
			for (teamIndex = 0; teamIndex < mt->serverTeams.size(); ++teamIndex) {
				team = mt->serverTeams[teamIndex];
				ASSERT(team->machineTeam->machineIDs == mt->machineIDs); // Sanity check

				// The team will be marked as a bad team
				bool foundTeam = self->removeTeam(team);
				ASSERT(foundTeam == true);
				// removeTeam() has side effect of swapping the last element to the current pos
				// in the serverTeams vector in the machine team.
				--teamIndex;
				self->addTeam(team->getServers(), true, true);
				TEST(true);
			}

			self->doBuildTeams = true;

			if (self->badTeamRemover.isReady()) {
				self->badTeamRemover = removeBadTeams(self);
				self->addActor.send(self->badTeamRemover);
			}

			TraceEvent("TeamRemover")
			    .detail("MachineTeamToRemove", mt->getMachineIDsStr())
			    .detail("NumProcessTeamsOnTheMachineTeam", minNumProcessTeams)
			    .detail("CurrentMachineTeamNumber", self->machineTeams.size())
			    .detail("DesiredMachineTeam", desiredMachineTeams);

			// Remove the machine team
			bool foundRemovedMachineTeam = self->removeMachineTeam(mt);
			// When we remove the last server team on a machine team in removeTeam(), we also remove the machine team
			// This is needed for removeTeam() functoin.
			// So here the removeMachineTeam() should not find the machine team
			ASSERT(foundRemovedMachineTeam);
			numMachineTeamRemoved++;
		} else {
			if (numMachineTeamRemoved > 0) {
				// Only trace the information when we remove a machine team
				TraceEvent("TeamRemoverDone")
				    .detail("HealthyMachineNumber", healthyMachineCount)
				    // .detail("CurrentHealthyMachineTeamNumber", currentHealthyMTCount)
				    .detail("CurrentMachineTeamNumber", self->machineTeams.size())
				    .detail("DesiredMachineTeam", desiredMachineTeams)
				    .detail("NumMachineTeamRemoved", numMachineTeamRemoved);
				self->traceTeamCollectionInfo();
			}
		}
	}
}

// Track a team and issue RelocateShards when the level of degradation changes
// A badTeam can be unhealthy or just a redundantTeam removed by teamRemover()
ACTOR Future<Void> teamTracker(DDTeamCollection* self, Reference<TCTeamInfo> team, bool badTeam, bool redundantTeam) {
	state int lastServersLeft = team->size();
	state bool lastAnyUndesired = false;
	state bool logTeamEvents = g_network->isSimulated() || !badTeam;
	state bool lastReady = false;
	state bool lastHealthy;
	state bool lastOptimal;
	state bool lastWrongConfiguration = team->isWrongConfiguration();

	state bool lastZeroHealthy = self->zeroHealthyTeams->get();
	state bool firstCheck = true;

	if(logTeamEvents) {
		TraceEvent("TeamTrackerStarting", self->distributorId).detail("Reason", "Initial wait complete (sc)").detail("Team", team->getDesc());
	}
	self->priority_teams[team->getPriority()]++;

	try {
		loop {
			if(logTeamEvents) {
				TraceEvent("TeamHealthChangeDetected", self->distributorId)
					.detail("Team", team->getDesc())
					.detail("Primary", self->primary)
					.detail("IsReady", self->initialFailureReactionDelay.isReady());
			}
			// Check if the number of degraded machines has changed
			state vector<Future<Void>> change;
			bool anyUndesired = false;
			bool anyWrongConfiguration = false;
			int serversLeft = 0;

			for (const UID& uid : team->getServerIDs()) {
				change.push_back( self->server_status.onChange( uid ) );
				auto& status = self->server_status.get(uid);
				if (!status.isFailed) {
					serversLeft++;
				}
				if (status.isUndesired) {
					anyUndesired = true;
				}
				if (status.isWrongConfiguration) {
					anyWrongConfiguration = true;
				}
			}

			if( !self->initialFailureReactionDelay.isReady() ) {
				change.push_back( self->initialFailureReactionDelay );
			}
			change.push_back( self->zeroHealthyTeams->onChange() );

			bool healthy = !badTeam && !anyUndesired && serversLeft == self->configuration.storageTeamSize;
			team->setHealthy( healthy );	// Unhealthy teams won't be chosen by bestTeam
			bool optimal = team->isOptimal() && healthy;
			bool recheck = !healthy && (lastReady != self->initialFailureReactionDelay.isReady() || (lastZeroHealthy && !self->zeroHealthyTeams->get()));

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
			    anyWrongConfiguration != lastWrongConfiguration || recheck) { // NOTE: do not check wrongSize
				if(logTeamEvents) {
					TraceEvent("TeamHealthChanged", self->distributorId)
						.detail("Team", team->getDesc()).detail("ServersLeft", serversLeft)
						.detail("LastServersLeft", lastServersLeft).detail("ContainsUndesiredServer", anyUndesired)
						.detail("HealthyTeamsCount", self->healthyTeamCount).detail("IsWrongConfiguration", anyWrongConfiguration);
				}

				team->setWrongConfiguration( anyWrongConfiguration );

				if( optimal != lastOptimal ) {
					lastOptimal = optimal;
					self->optimalTeamCount += optimal ? 1 : -1;

					ASSERT( self->optimalTeamCount >= 0 );
					self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
				}

				if( lastHealthy != healthy ) {
					lastHealthy = healthy;
					// Update healthy team count when the team healthy changes
					self->healthyTeamCount += healthy ? 1 : -1;

					ASSERT( self->healthyTeamCount >= 0 );
					self->zeroHealthyTeams->set(self->healthyTeamCount == 0);

					if( self->healthyTeamCount == 0 ) {
						TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->distributorId)
							.detail("SignallingTeam", team->getDesc())
							.detail("Primary", self->primary);
					}

					if(logTeamEvents) {
						TraceEvent("TeamHealthDifference", self->distributorId)
							.detail("Team", team->getDesc())
							.detail("LastOptimal", lastOptimal)
							.detail("LastHealthy", lastHealthy)
							.detail("Optimal", optimal)
							.detail("OptimalTeamCount", self->optimalTeamCount);
					}
				}

				lastServersLeft = serversLeft;
				lastAnyUndesired = anyUndesired;
				lastWrongConfiguration = anyWrongConfiguration;

				state int lastPriority = team->getPriority();
				if( serversLeft < self->configuration.storageTeamSize ) {
					if( serversLeft == 0 )
						team->setPriority( PRIORITY_TEAM_0_LEFT );
					else if( serversLeft == 1 )
						team->setPriority( PRIORITY_TEAM_1_LEFT );
					else if( serversLeft == 2 )
						team->setPriority( PRIORITY_TEAM_2_LEFT );
					else
						team->setPriority( PRIORITY_TEAM_UNHEALTHY );
				}
				else if ( badTeam || anyWrongConfiguration ) {
					if ( redundantTeam ) {
						team->setPriority( PRIORITY_TEAM_REDUNDANT );
					} else {
						team->setPriority( PRIORITY_TEAM_UNHEALTHY );
					}
				}
				else if( anyUndesired )
					team->setPriority( PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER );
				else
					team->setPriority( PRIORITY_TEAM_HEALTHY );

				if(lastPriority != team->getPriority()) {
					self->priority_teams[lastPriority]--;
					self->priority_teams[team->getPriority()]++;
				}

				if(logTeamEvents) {
					TraceEvent("TeamPriorityChange", self->distributorId).detail("Priority", team->getPriority())
					.detail("Info", team->getDesc()).detail("ZeroHealthyTeams", self->zeroHealthyTeams->get());
				}

				lastZeroHealthy = self->zeroHealthyTeams->get(); //set this again in case it changed from this teams health changing
				if( self->initialFailureReactionDelay.isReady() && !self->zeroHealthyTeams->get() ) {
					vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor( ShardsAffectedByTeamFailure::Team(team->getServerIDs(), self->primary) );

					for(int i=0; i<shards.size(); i++) {
						int maxPriority = team->getPriority();
						if(maxPriority < PRIORITY_TEAM_0_LEFT) {
							auto teams = self->shardsAffectedByTeamFailure->getTeamsFor( shards[i] );
							for( int j=0; j < teams.first.size()+teams.second.size(); j++) {
								// t is the team in primary DC or the remote DC
								auto& t = j < teams.first.size() ? teams.first[j] : teams.second[j-teams.first.size()];
								if( !t.servers.size() ) {
									maxPriority = PRIORITY_TEAM_0_LEFT;
									break;
								}

								auto tc = self->teamCollections[t.primary ? 0 : 1];
								ASSERT(tc->primary == t.primary);
								if( tc->server_info.count( t.servers[0] ) ) {
									auto& info = tc->server_info[t.servers[0]];

									bool found = false;
									for( int k = 0; k < info->teams.size(); k++ ) {
										if( info->teams[k]->getServerIDs() == t.servers ) {
											maxPriority = std::max( maxPriority, info->teams[k]->getPriority() );
											found = true;
											break;
										}
									}

									//If we cannot find the team, it could be a bad team so assume unhealthy priority
									if(!found) {
										maxPriority = std::max<int>( maxPriority, PRIORITY_TEAM_UNHEALTHY );
									}
								} else {
									TEST(true); // A removed server is still associated with a team in SABTF
								}
							}
						}

						RelocateShard rs;
						rs.keys = shards[i];
						rs.priority = maxPriority;

						self->output.send(rs);
						if(g_random->random01() < 0.01) {
							TraceEvent("SendRelocateToDDQx100", self->distributorId)
								.detail("Team", team->getDesc())
								.detail("KeyBegin", printable(rs.keys.begin))
								.detail("KeyEnd", printable(rs.keys.end))
								.detail("Priority", rs.priority)
								.detail("TeamFailedMachines", team->size() - serversLeft)
								.detail("TeamOKMachines", serversLeft);
						}
					}
				} else {
					if(logTeamEvents) {
						TraceEvent("TeamHealthNotReady", self->distributorId).detail("HealthyTeamCount", self->healthyTeamCount);
					}
				}
			}

			// Wait for any of the machines to change status
			wait( quorum( change, 1 ) );
			wait( yield() );
		}
	} catch(Error& e) {
		if(logTeamEvents) {
			TraceEvent("TeamTrackerStopping", self->distributorId).detail("Team", team->getDesc());
		}
		self->priority_teams[team->getPriority()]--;
		if( team->isHealthy() ) {
			self->healthyTeamCount--;
			ASSERT( self->healthyTeamCount >= 0 );

			if( self->healthyTeamCount == 0 ) {
				TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->distributorId).detail("SignallingTeam", team->getDesc());
				self->zeroHealthyTeams->set(true);
			}
		}
		throw;
	}
}

ACTOR Future<Void> trackExcludedServers( DDTeamCollection* self ) {
	loop {
		// Fetch the list of excluded servers
		state Transaction tr(self->cx);
		state Optional<Value> lastChangeID;
		loop {
			try {
				state Future<Standalone<RangeResultRef>> fresults = tr.getRange( excludedServersKeys, CLIENT_KNOBS->TOO_MANY );
				state Future<Optional<Value>> fchid = tr.get( excludedServersVersionKey );
				wait( success(fresults) && success(fchid) );

				Standalone<RangeResultRef> results = fresults.get();
				lastChangeID = fchid.get();
				ASSERT( !results.more && results.size() < CLIENT_KNOBS->TOO_MANY );

				std::set<AddressExclusion> excluded;
				for(auto r = results.begin(); r != results.end(); ++r) {
					AddressExclusion addr = decodeExcludedServersKey(r->key);
					if (addr.isValid())
						excluded.insert( addr );
				}

				TraceEvent("DDExcludedServersChanged", self->distributorId).detail("Rows", results.size()).detail("Exclusions", excluded.size());

				// Reset and reassign self->excludedServers based on excluded, but we only
				// want to trigger entries that are different
				auto old = self->excludedServers.getKeys();
				for(auto& o : old)
					if (!excluded.count(o))
						self->excludedServers.set(o, false);
				for(auto& n : excluded)
					self->excludedServers.set(n, true);
				self->restartRecruiting.trigger();
				break;
			} catch (Error& e) {
				wait( tr.onError(e) );
			}
		}

		// Wait for a change in the list of excluded servers
		loop {
			try {
				Optional<Value> nchid = wait( tr.get( excludedServersVersionKey ) );
				if (nchid != lastChangeID)
					break;

				wait( delay( SERVER_KNOBS->SERVER_LIST_DELAY, TaskDataDistribution ) );  // FIXME: make this tr.watch( excludedServersVersionKey ) instead
				tr = Transaction(self->cx);
			} catch (Error& e) {
				wait( tr.onError(e) );
			}
		}
	}
}

ACTOR Future<vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses( Transaction *tr ) {
	state Future<vector<ProcessData>> workers = getWorkers(tr);
	state Future<Standalone<RangeResultRef>> serverList = tr->getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY );
	wait( success(workers) && success(serverList) );
	ASSERT( !serverList.get().more && serverList.get().size() < CLIENT_KNOBS->TOO_MANY );

	std::map<Optional<Standalone<StringRef>>, ProcessData> id_data;
	for( int i = 0; i < workers.get().size(); i++ )
		id_data[workers.get()[i].locality.processId()] = workers.get()[i];

	vector<std::pair<StorageServerInterface, ProcessClass>> results;
	for( int i = 0; i < serverList.get().size(); i++ ) {
		auto ssi = decodeServerListValue( serverList.get()[i].value );
		results.push_back( std::make_pair(ssi, id_data[ssi.locality.processId()].processClass) );
	}

	return results;
}

ACTOR Future<Void> waitServerListChange( DDTeamCollection* self, FutureStream<Void> serverRemoved ) {
	state Future<Void> checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY);
	state Future<vector<std::pair<StorageServerInterface, ProcessClass>>> serverListAndProcessClasses = Never();
	state bool isFetchingResults = false;
	state Transaction tr(self->cx);
	loop {
		try {
			choose {
				when( wait( checkSignal ) ) {
					checkSignal = Never();
					isFetchingResults = true;
					serverListAndProcessClasses = getServerListAndProcessClasses(&tr);
				}
				when( vector<std::pair<StorageServerInterface, ProcessClass>> results = wait( serverListAndProcessClasses ) ) {
					serverListAndProcessClasses = Never();
					isFetchingResults = false;

					for( int i = 0; i < results.size(); i++ ) {
						UID serverId = results[i].first.id();
						StorageServerInterface const& ssi = results[i].first;
						ProcessClass const& processClass = results[i].second;
						if (!self->shouldHandleServer(ssi)) {
							continue;
						}
						else if( self->server_info.count( serverId ) ) {
							auto& serverInfo = self->server_info[ serverId ];
							if (ssi.getValue.getEndpoint() != serverInfo->lastKnownInterface.getValue.getEndpoint() || processClass != serverInfo->lastKnownClass.classType()) {
								Promise<std::pair<StorageServerInterface, ProcessClass>> currentInterfaceChanged = serverInfo->interfaceChanged;
								serverInfo->interfaceChanged = Promise<std::pair<StorageServerInterface, ProcessClass>>();
								serverInfo->onInterfaceChanged = Future<std::pair<StorageServerInterface, ProcessClass>>( serverInfo->interfaceChanged.getFuture() );
								currentInterfaceChanged.send( std::make_pair(ssi,processClass) );
							}
						} else if( !self->recruitingIds.count(ssi.id()) ) {
							self->addServer( ssi, processClass, self->serverTrackerErrorOut, tr.getReadVersion().get() );
							self->doBuildTeams = true;
						}
					}

					tr = Transaction(self->cx);
					checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY);
				}
				when( waitNext( serverRemoved ) ) {
					if( isFetchingResults ) {
						tr = Transaction(self->cx);
						serverListAndProcessClasses = getServerListAndProcessClasses(&tr);
					}
				}
			}
		} catch(Error& e) {
			wait( tr.onError(e) );
			serverListAndProcessClasses = Never();
			isFetchingResults = false;
			checkSignal = Void();
		}
	}
}

ACTOR Future<Void> waitHealthyZoneChange( DDTeamCollection* self ) {
	state ReadYourWritesTransaction tr(self->cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> val = wait(tr.get(healthyZoneKey));
			state Future<Void> healthyZoneTimeout = Never();
			if(val.present()) {
				auto p = decodeHealthyZoneValue(val.get());
				if(p.second > tr.getReadVersion().get()) {
					healthyZoneTimeout = delay((p.second - tr.getReadVersion().get())/(double)SERVER_KNOBS->VERSIONS_PER_SECOND);
					self->healthyZone.set(p.first);
				} else {
					self->healthyZone.set(Optional<Key>());
				}
			} else {
				self->healthyZone.set(Optional<Key>());
			}
			
			state Future<Void> watchFuture = tr.watch(healthyZoneKey);
			wait(tr.commit());
			wait(watchFuture || healthyZoneTimeout);
			tr.reset();
		} catch(Error& e) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> serverMetricsPolling( TCServerInfo *server) {
	state double lastUpdate = now();
	loop {
		wait( updateServerMetrics( server ) );
		wait( delayUntil( lastUpdate + SERVER_KNOBS->STORAGE_METRICS_POLLING_DELAY + SERVER_KNOBS->STORAGE_METRICS_RANDOM_DELAY * g_random->random01(), TaskDataDistributionLaunch ) );
		lastUpdate = now();
	}
}

//Returns the KeyValueStoreType of server if it is different from self->storeType
ACTOR Future<KeyValueStoreType> keyValueStoreTypeTracker(DDTeamCollection* self, TCServerInfo *server) {
	state KeyValueStoreType type = wait(brokenPromiseToNever(server->lastKnownInterface.getKeyValueStoreType.getReplyWithTaskID<KeyValueStoreType>(TaskDataDistribution)));
	if(type == self->configuration.storageServerStoreType && (self->includedDCs.empty() || std::find(self->includedDCs.begin(), self->includedDCs.end(), server->lastKnownInterface.locality.dcId()) != self->includedDCs.end()) )
		wait(Future<Void>(Never()));

	return type;
}

ACTOR Future<Void> waitForAllDataRemoved( Database cx, UID serverID, Version addedVersion, DDTeamCollection* teams ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Version ver = wait( tr.getReadVersion() );

			//we cannot remove a server immediately after adding it, because a perfectly timed master recovery could cause us to not store the mutations sent to the short lived storage server.
			if(ver > addedVersion + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
				bool canRemove = wait( canRemoveStorageServer( &tr, serverID ) );
				if (canRemove && teams->shardsAffectedByTeamFailure->getNumberOfShards(serverID) == 0) {
					return Void();
				}
			}

			// Wait for any change to the serverKeys for this server
			wait( delay(SERVER_KNOBS->ALL_DATA_REMOVED_DELAY, TaskDataDistribution) );
			tr.reset();
		} catch (Error& e) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> storageServerFailureTracker(
	DDTeamCollection* self,
	TCServerInfo *server,
	Database cx,
	ServerStatus *status,
	Version addedVersion )
{
	state StorageServerInterface interf = server->lastKnownInterface;
	loop {
		state bool inHealthyZone = self->healthyZone.get().present() && interf.locality.zoneId() == self->healthyZone.get();
		if(inHealthyZone) {
			status->isFailed = false;
		}

		if( self->server_status.get(interf.id()).initialized ) {
			bool unhealthy = self->server_status.get(interf.id()).isUnhealthy();
			if(unhealthy && !status->isUnhealthy()) {
				self->unhealthyServers--;
			}
			if(!unhealthy && status->isUnhealthy()) {
				self->unhealthyServers++;
			}
		} else if(status->isUnhealthy()) {
			self->unhealthyServers++;
		}

		self->server_status.set( interf.id(), *status );
		if( status->isFailed )
			self->restartRecruiting.trigger();

		Future<Void> healthChanged = Never();
		if(status->isFailed) {
			ASSERT(!inHealthyZone);
			healthChanged = IFailureMonitor::failureMonitor().onStateEqual( interf.waitFailure.getEndpoint(), FailureStatus(false));
		} else if(!inHealthyZone) {
			healthChanged = waitFailureClientStrict(interf.waitFailure, SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME, TaskDataDistribution);
		}
		choose {
			when ( wait(healthChanged) ) {
				status->isFailed = !status->isFailed;
				if(!status->isFailed && !server->teams.size()) {
					self->doBuildTeams = true;
				}
				if(status->isFailed && self->healthyZone.get().present() && self->clearHealthyZoneFuture.isReady()) {
					self->clearHealthyZoneFuture = clearHealthyZone(self->cx);
					self->healthyZone.set(Optional<Key>());
				}

				TraceEvent("StatusMapChange", self->distributorId).detail("ServerID", interf.id()).detail("Status", status->toString())
					.detail("Available", IFailureMonitor::failureMonitor().getState(interf.waitFailure.getEndpoint()).isAvailable());
			}
			when ( wait( status->isUnhealthy() ? waitForAllDataRemoved(cx, interf.id(), addedVersion, self) : Never() ) ) { break; }
			when ( wait( self->healthyZone.onChange() ) ) {}
		}
	}

	return Void();
}

// Check the status of a storage server.
// Apply all requirements to the server and mark it as excluded if it fails to satisfies these requirements
ACTOR Future<Void> storageServerTracker(
	DDTeamCollection* self,
	Database cx,
	TCServerInfo *server, //This actor is owned by this TCServerInfo
	Promise<Void> errorOut,
	Version addedVersion)
{
	state Future<Void> failureTracker;
	state ServerStatus status( false, false, server->lastKnownInterface.locality );
	state bool lastIsUnhealthy = false;
	state Future<Void> metricsTracker = serverMetricsPolling( server );
	state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged = server->onInterfaceChanged;

	state Future<KeyValueStoreType> storeTracker = keyValueStoreTypeTracker( self, server );
	state bool hasWrongStoreTypeOrDC = false;

	try {
		loop {
			status.isUndesired = false;
			status.isWrongConfiguration = false;

			// If there is any other server on this exact NetworkAddress, this server is undesired and will eventually be eliminated
			state std::vector<Future<Void>> otherChanges;
			std::vector<Promise<Void>> wakeUpTrackers;
			for(const auto& i : self->server_info) {
				if (i.second.getPtr() != server && i.second->lastKnownInterface.address() == server->lastKnownInterface.address()) {
					auto& statusInfo = self->server_status.get( i.first );
					TraceEvent("SameAddress", self->distributorId)
						.detail("Failed", statusInfo.isFailed)
						.detail("Undesired", statusInfo.isUndesired)
						.detail("Server", server->id).detail("OtherServer", i.second->id)
						.detail("Address", server->lastKnownInterface.address())
						.detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
						.detail("OtherNumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id))
						.detail("OtherHealthy", !self->server_status.get( i.second->id ).isUnhealthy());
					// wait for the server's ip to be changed
					otherChanges.push_back(self->server_status.onChange(i.second->id));
					if(!self->server_status.get( i.second->id ).isUnhealthy()) {
						if(self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id) >= self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
						{
							TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
								.detail("Server", server->id)
								.detail("Address", server->lastKnownInterface.address())
								.detail("OtherServer", i.second->id)
								.detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
								.detail("OtherNumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(i.second->id));

							status.isUndesired = true;
						}
						else
							wakeUpTrackers.push_back(i.second->wakeUpTracker);
					}
				}
			}

			for(auto& p : wakeUpTrackers) {
				if( !p.isSet() )
					p.send(Void());
			}

			if( server->lastKnownClass.machineClassFitness( ProcessClass::Storage ) > ProcessClass::UnsetFit ) {
				if( self->optimalTeamCount > 0 ) {
					TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId)
					    .detail("Server", server->id)
					    .detail("OptimalTeamCount", self->optimalTeamCount)
					    .detail("Fitness", server->lastKnownClass.machineClassFitness(ProcessClass::Storage));
					status.isUndesired = true;
				}
				otherChanges.push_back( self->zeroOptimalTeams.onChange() );
			}

			//If this storage server has the wrong key-value store type, then mark it undesired so it will be replaced with a server having the correct type
			if(hasWrongStoreTypeOrDC) {
				TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId).detail("Server", server->id).detail("StoreType", "?");
				status.isUndesired = true;
				status.isWrongConfiguration = true;
			}

			// If the storage server is in the excluded servers list, it is undesired
			NetworkAddress a = server->lastKnownInterface.address();
			AddressExclusion addr( a.ip, a.port );
			AddressExclusion ipaddr( a.ip );
			if (self->excludedServers.get( addr ) || self->excludedServers.get( ipaddr )) {
				TraceEvent(SevWarn, "UndesiredStorageServer", self->distributorId).detail("Server", server->id)
					.detail("Excluded", self->excludedServers.get( addr ) ? addr.toString() : ipaddr.toString());
				status.isUndesired = true;
				status.isWrongConfiguration = true;
			}
			otherChanges.push_back( self->excludedServers.onChange( addr ) );
			otherChanges.push_back( self->excludedServers.onChange( ipaddr ) );

			failureTracker = storageServerFailureTracker( self, server, cx, &status, addedVersion );

			//We need to recruit new storage servers if the key value store type has changed
			if(hasWrongStoreTypeOrDC)
				self->restartRecruiting.trigger();

			if ( lastIsUnhealthy && !status.isUnhealthy() && !server->teams.size() ) {
				self->doBuildTeams = true;
			}
			lastIsUnhealthy = status.isUnhealthy();

			choose {
				when( wait( failureTracker ) ) {
					// The server is failed AND all data has been removed from it, so permanently remove it.
					TraceEvent("StatusMapChange", self->distributorId).detail("ServerID", server->id).detail("Status", "Removing");

					if(server->updated.canBeSet()) {
						server->updated.send(Void());
					}

					// Remove server from FF/serverList
					wait( removeStorageServer( cx, server->id, self->lock ) );

					TraceEvent("StatusMapChange", self->distributorId).detail("ServerID", server->id).detail("Status", "Removed");
					// Sets removeSignal (alerting dataDistributionTeamCollection to remove the storage server from its own data structures)
					server->removed.send( Void() );
					self->removedServers.send( server->id );
					return Void();
				}
				when( std::pair<StorageServerInterface, ProcessClass> newInterface = wait( interfaceChanged ) ) {
					bool restartRecruiting =  newInterface.first.waitFailure.getEndpoint().getPrimaryAddress() != server->lastKnownInterface.waitFailure.getEndpoint().getPrimaryAddress();
					bool localityChanged = server->lastKnownInterface.locality != newInterface.first.locality;
					bool machineLocalityChanged = server->lastKnownInterface.locality.zoneId().get() !=
					                              newInterface.first.locality.zoneId().get();
					TraceEvent("StorageServerInterfaceChanged", self->distributorId).detail("ServerID", server->id)
						.detail("NewWaitFailureToken", newInterface.first.waitFailure.getEndpoint().token)
						.detail("OldWaitFailureToken", server->lastKnownInterface.waitFailure.getEndpoint().token)
						.detail("LocalityChanged", localityChanged);

					server->lastKnownInterface = newInterface.first;
					server->lastKnownClass = newInterface.second;
					if (localityChanged) {
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
								self->removeMachine(self, machine);
								server->machine = Reference<TCMachineInfo>();
							} else {
								// we remove the server from the machine, and
								// update locality entry for the machine and the global machineLocalityMap
								int serverIndex = -1;
								for (int i = 0; i < machine->serversOnMachine.size(); ++i) {
									if (machine->serversOnMachine[i].getPtr() == server) {
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

							// Second handle the impact on the destination machine where the server's new locality is;
							// If the destination machine is new, create one; otherwise, add server to an existing one
							// Update server's machine reference to the destination machine
							Reference<TCMachineInfo> destMachine =
							    self->checkAndCreateMachine(self->server_info[server->id]);
							ASSERT(destMachine.isValid());
						}

						// Ensure the server's server team belong to a machine team, and
						// Get the newBadTeams due to the locality change
						vector<Reference<TCTeamInfo>> newBadTeams;
						for (auto& serverTeam : server->teams) {
							if (!self->satisfiesPolicy(serverTeam->getServers())) {
								newBadTeams.push_back(serverTeam);
								continue;
							}
							if (machineLocalityChanged) {
								Reference<TCMachineTeamInfo> machineTeam = self->checkAndCreateMachineTeam(serverTeam);
								ASSERT(machineTeam.isValid());
								serverTeam->machineTeam = machineTeam;
							}
						}

						server->inDesiredDC =
						    (self->includedDCs.empty() ||
						     std::find(self->includedDCs.begin(), self->includedDCs.end(),
						               server->lastKnownInterface.locality.dcId()) != self->includedDCs.end());
						self->resetLocalitySet();

						bool addedNewBadTeam = false;
						for(auto it : newBadTeams) {
							if( self->removeTeam(it) ) {
								self->addTeam(it->getServers(), true);
								addedNewBadTeam = true;
							}
						}
						if(addedNewBadTeam && self->badTeamRemover.isReady()) {
							TEST(true); // Server locality change created bad teams
							self->doBuildTeams = true;
							self->badTeamRemover = removeBadTeams(self);
							self->addActor.send(self->badTeamRemover);
							// The team number changes, so we need to update the team number info
							self->traceTeamCollectionInfo();
						}
					}

					interfaceChanged = server->onInterfaceChanged;
					// We rely on the old failureTracker being actorCancelled since the old actor now has a pointer to an invalid location
					status = ServerStatus( status.isFailed, status.isUndesired, server->lastKnownInterface.locality );

					//Restart the storeTracker for the new interface
					storeTracker = keyValueStoreTypeTracker(self, server);
					hasWrongStoreTypeOrDC = false;
					self->restartTeamBuilder.trigger();
					if(restartRecruiting)
						self->restartRecruiting.trigger();
				}
				when( wait( otherChanges.empty() ? Never() : quorum( otherChanges, 1 ) ) ) {
					TraceEvent("SameAddressChangedStatus", self->distributorId).detail("ServerID", server->id);
				}
				when( KeyValueStoreType type = wait( storeTracker ) ) {
					TraceEvent("KeyValueStoreTypeChanged", self->distributorId)
						.detail("ServerID", server->id)
						.detail("StoreType", type.toString())
						.detail("DesiredType", self->configuration.storageServerStoreType.toString());
					TEST(true); //KeyValueStore type changed

					storeTracker = Never();
					hasWrongStoreTypeOrDC = true;
				}
				when( wait( server->wakeUpTracker.getFuture() ) ) {
					server->wakeUpTracker = Promise<Void>();
				}
			}
		}
	} catch( Error &e ) {
		if (e.code() != error_code_actor_cancelled && errorOut.canBeSet())
			errorOut.sendError(e);
		throw;
	}
}

//Monitor whether or not storage servers are being recruited.  If so, then a database cannot be considered quiet
ACTOR Future<Void> monitorStorageServerRecruitment(DDTeamCollection* self) {
	state bool recruiting = false;
	TraceEvent("StorageServerRecruitment", self->distributorId)
	    .detail("State", "Idle")
	    .trackLatest(("StorageServerRecruitment_" + self->distributorId.toString()).c_str());
	loop {
		if( !recruiting ) {
			while(self->recruitingStream.get() == 0) {
				wait( self->recruitingStream.onChange() );
			}
			TraceEvent("StorageServerRecruitment", self->distributorId)
				.detail("State", "Recruiting")
				.trackLatest(("StorageServerRecruitment_" + self->distributorId.toString()).c_str());
			recruiting = true;
		} else {
			loop {
				choose {
					when( wait( self->recruitingStream.onChange() ) ) {}
					when( wait( self->recruitingStream.get() == 0 ? delay(SERVER_KNOBS->RECRUITMENT_IDLE_DELAY, TaskDataDistribution) : Future<Void>(Never()) ) ) { break; }
				}
			}
			TraceEvent("StorageServerRecruitment", self->distributorId)
				.detail("State", "Idle")
				.trackLatest(("StorageServerRecruitment_" + self->distributorId.toString()).c_str());
			recruiting = false;
		}
	}
}

ACTOR Future<Void> initializeStorage( DDTeamCollection* self, RecruitStorageReply candidateWorker ) {
	// SOMEDAY: Cluster controller waits for availability, retry quickly if a server's Locality changes
	self->recruitingStream.set(self->recruitingStream.get()+1);

	state UID interfaceId = g_random->randomUniqueID();
	InitializeStorageRequest isr;
	isr.storeType = self->configuration.storageServerStoreType;
	isr.seedTag = invalidTag;
	isr.reqId = g_random->randomUniqueID();
	isr.interfaceId = interfaceId;

	TraceEvent("DDRecruiting").detail("State", "Sending request to worker").detail("WorkerID", candidateWorker.worker.id())
		.detail("WorkerLocality", candidateWorker.worker.locality.toString()).detail("Interf", interfaceId).detail("Addr", candidateWorker.worker.address());

	self->recruitingIds.insert(interfaceId);
	self->recruitingLocalities.insert(candidateWorker.worker.address());
	state ErrorOr<InitializeStorageReply> newServer = wait( candidateWorker.worker.storage.tryGetReply( isr, TaskDataDistribution ) );
	if(newServer.isError()) {
		TraceEvent(SevWarn, "DDRecruitmentError").error(newServer.getError());
		if( !newServer.isError( error_code_recruitment_failed ) && !newServer.isError( error_code_request_maybe_delivered ) )
			throw newServer.getError();
		wait( delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskDataDistribution) );
	}
	self->recruitingIds.erase(interfaceId);
	self->recruitingLocalities.erase(candidateWorker.worker.address());

	self->recruitingStream.set(self->recruitingStream.get()-1);

	TraceEvent("DDRecruiting").detail("State", "Finished request").detail("WorkerID", candidateWorker.worker.id())
		.detail("WorkerLocality", candidateWorker.worker.locality.toString()).detail("Interf", interfaceId).detail("Addr", candidateWorker.worker.address());

	if( newServer.present() ) {
		if( !self->server_info.count( newServer.get().interf.id() ) )
			self->addServer( newServer.get().interf, candidateWorker.processClass, self->serverTrackerErrorOut, newServer.get().addedVersion );
		else
			TraceEvent(SevWarn, "DDRecruitmentError").detail("Reason", "Server ID already recruited");

		self->doBuildTeams = true;
	}

	self->restartRecruiting.trigger();

	return Void();
}

ACTOR Future<Void> storageRecruiter( DDTeamCollection* self, Reference<AsyncVar<struct ServerDBInfo>> db ) {
	state Future<RecruitStorageReply> fCandidateWorker;
	state RecruitStorageRequest lastRequest;
	loop {
		try {
			RecruitStorageRequest rsr;
			std::set<AddressExclusion> exclusions;
			for(auto s = self->server_info.begin(); s != self->server_info.end(); ++s) {
				auto serverStatus = self->server_status.get( s->second->lastKnownInterface.id() );
				if( serverStatus.excludeOnRecruit() ) {
					TraceEvent(SevDebug, "DDRecruitExcl1").detail("Excluding", s->second->lastKnownInterface.address());
					auto addr = s->second->lastKnownInterface.address();
					exclusions.insert( AddressExclusion( addr.ip, addr.port ) );
				}
			}
			for(auto addr : self->recruitingLocalities) {
				exclusions.insert( AddressExclusion(addr.ip, addr.port));
			}

			auto excl = self->excludedServers.getKeys();
			for(auto& s : excl)
				if (self->excludedServers.get(s)) {
					TraceEvent(SevDebug, "DDRecruitExcl2").detail("Excluding", s.toString());
					exclusions.insert( s );
				}
			rsr.criticalRecruitment = self->healthyTeamCount == 0;
			for(auto it : exclusions) {
				rsr.excludeAddresses.push_back(it);
			}

			rsr.includeDCs = self->includedDCs;

			TraceEvent(rsr.criticalRecruitment ? SevWarn : SevInfo, "DDRecruiting").detail("State", "Sending request to CC")
			.detail("Exclusions", rsr.excludeAddresses.size()).detail("Critical", rsr.criticalRecruitment);

			if( rsr.criticalRecruitment ) {
				TraceEvent(SevWarn, "DDRecruitingEmergency", self->distributorId);
			}

			if(!fCandidateWorker.isValid() || fCandidateWorker.isReady() || rsr.excludeAddresses != lastRequest.excludeAddresses || rsr.criticalRecruitment != lastRequest.criticalRecruitment) {
				lastRequest = rsr;
				fCandidateWorker = brokenPromiseToNever( db->get().clusterInterface.recruitStorage.getReply( rsr, TaskDataDistribution ) );
			}

			choose {
				when( RecruitStorageReply candidateWorker = wait( fCandidateWorker ) ) {
					self->addActor.send(initializeStorage(self, candidateWorker));
				}
				when( wait( db->onChange() ) ) { // SOMEDAY: only if clusterInterface changes?
					fCandidateWorker = Future<RecruitStorageReply>();
				}
				when( wait( self->restartRecruiting.onTrigger() ) ) {}
			}
			wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		} catch( Error &e ) {
			if(e.code() != error_code_timed_out) {
				throw;
			}
			TEST(true); //Storage recruitment timed out
		}
	}
}

ACTOR Future<Void> updateReplicasKey(DDTeamCollection* self, Optional<Key> dcId) {
	std::vector<Future<Void>> serverUpdates;

	for(auto& it : self->server_info) {
		serverUpdates.push_back(it.second->updated.getFuture());
	}

	wait(self->initialFailureReactionDelay && waitForAll(serverUpdates));
	wait(waitUntilHealthy(self));
	TraceEvent("DDUpdatingReplicas", self->distributorId).detail("DcId", printable(dcId)).detail("Replicas", self->configuration.storageTeamSize);
	state Transaction tr(self->cx);
	loop {
		try {
			Optional<Value> val = wait( tr.get(datacenterReplicasKeyFor(dcId)) );
			state int oldReplicas = val.present() ? decodeDatacenterReplicasValue(val.get()) : 0;
			if(oldReplicas == self->configuration.storageTeamSize) {
				TraceEvent("DDUpdatedAlready", self->distributorId).detail("DcId", printable(dcId)).detail("Replicas", self->configuration.storageTeamSize);
				return Void();
			}
			if(oldReplicas < self->configuration.storageTeamSize) {
				tr.set(rebootWhenDurableKey, StringRef());
			}
			tr.set(datacenterReplicasKeyFor(dcId), datacenterReplicasValue(self->configuration.storageTeamSize));
			wait( tr.commit() );
			TraceEvent("DDUpdatedReplicas", self->distributorId).detail("DcId", printable(dcId)).detail("Replicas", self->configuration.storageTeamSize).detail("OldReplicas", oldReplicas);
			return Void();
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> serverGetTeamRequests(TeamCollectionInterface tci, DDTeamCollection* self) {
	loop {
		GetTeamRequest req = waitNext(tci.getTeam.getFuture());
		self->addActor.send( self->getTeam( self, req ) );
	}
}

ACTOR Future<Void> remoteRecovered( Reference<AsyncVar<struct ServerDBInfo>> db ) {
	TraceEvent("DDTrackerStarting");
	while ( db->get().recoveryState < RecoveryState::ALL_LOGS_RECRUITED ) {
		TraceEvent("DDTrackerStarting").detail("RecoveryState", (int)db->get().recoveryState);
		wait( db->onChange() );
	}
	return Void();
}

ACTOR Future<Void> monitorHealthyTeams( DDTeamCollection* self ) {
	loop choose {
		when ( wait(self->zeroHealthyTeams->get() ? delay(SERVER_KNOBS->DD_ZERO_HEALTHY_TEAM_DELAY) : Never()) ) {
			self->doBuildTeams = true;
			wait( DDTeamCollection::checkBuildTeams(self) );
		}
		when ( wait(self->zeroHealthyTeams->onChange()) ) {}
	}
}

// Keep track of servers and teams -- serves requests for getRandomTeam
ACTOR Future<Void> dataDistributionTeamCollection(
	Reference<DDTeamCollection> teamCollection,
	Reference<InitialDataDistribution> initData,
	TeamCollectionInterface tci,
	Reference<AsyncVar<struct ServerDBInfo>> db)
{
	state DDTeamCollection* self = teamCollection.getPtr();
	state Future<Void> loggingTrigger = Void();
	state PromiseStream<Void> serverRemoved;
	state Future<Void> error = actorCollection( self->addActor.getFuture() );

	try {
		wait( DDTeamCollection::init( self, initData ) );
		initData = Reference<InitialDataDistribution>();
		self->addActor.send(serverGetTeamRequests(tci, self));

		TraceEvent("DDTeamCollectionBegin", self->distributorId).detail("Primary", self->primary);
		wait( self->readyToStart || error );
		TraceEvent("DDTeamCollectionReadyToStart", self->distributorId).detail("Primary", self->primary);

		if(self->badTeamRemover.isReady()) {
			self->badTeamRemover = removeBadTeams(self);
			self->addActor.send(self->badTeamRemover);
		}

		if (self->redundantTeamRemover.isReady()) {
			self->redundantTeamRemover = teamRemover(self);
			self->addActor.send(self->redundantTeamRemover);
		}
		self->traceTeamCollectionInfo();

		if(self->includedDCs.size()) {
			//start this actor before any potential recruitments can happen
			self->addActor.send(updateReplicasKey(self, self->includedDCs[0]));
		}

		self->addActor.send(storageRecruiter( self, db ));
		self->addActor.send(monitorStorageServerRecruitment( self ));
		self->addActor.send(waitServerListChange( self, serverRemoved.getFuture() ));
		self->addActor.send(trackExcludedServers( self ));
		self->addActor.send(monitorHealthyTeams( self ));
		self->addActor.send(waitHealthyZoneChange( self ));

		// SOMEDAY: Monitor FF/serverList for (new) servers that aren't in allServers and add or remove them

		loop choose {
			when( UID removedServer = waitNext( self->removedServers.getFuture() ) ) {
				TEST(true);  // Storage server removed from database
				self->removeServer(self, removedServer);
				serverRemoved.send( Void() );

				self->restartRecruiting.trigger();
			}
			when( wait( self->zeroHealthyTeams->onChange() ) ) {
				if(self->zeroHealthyTeams->get()) {
					self->restartRecruiting.trigger();
					self->noHealthyTeams();
				}
			}
			when( wait( loggingTrigger ) ) {
				int highestPriority = 0;
				for(auto it : self->priority_teams) {
					if(it.second > 0) {
						highestPriority = std::max(highestPriority, it.first);
					}
				}

				TraceEvent("TotalDataInFlight", self->distributorId)
				    .detail("Primary", self->primary)
				    .detail("TotalBytes", self->getDebugTotalDataInFlight())
				    .detail("UnhealthyServers", self->unhealthyServers)
				    .detail("ServerNumber", self->server_info.size())
				    .detail("StorageTeamSize", self->configuration.storageTeamSize)
				    .detail("HighestPriority", highestPriority)
				    .trackLatest(self->primary ? "TotalDataInFlight" : "TotalDataInFlightRemote");
				loggingTrigger = delay( SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL );
			}
			when( wait( self->serverTrackerErrorOut.getFuture() ) ) {} // Propagate errors from storageServerTracker
			when( wait( error ) ) {}
		}
	} catch (Error& e) {
		if (e.code() != error_code_movekeys_conflict)
			TraceEvent(SevError, "DataDistributionTeamCollectionError", self->distributorId).error(e);
		throw e;
	}
}

ACTOR Future<Void> waitForDataDistributionEnabled( Database cx ) {
	state Transaction tr(cx);
	loop {
		wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskDataDistribution));

		try {
			Optional<Value> mode = wait( tr.get( dataDistributionModeKey ) );
			if (!mode.present()) return Void();
			if (mode.present()) {
				BinaryReader rd( mode.get(), Unversioned() );
				int m;
				rd >> m;
				if (m) return Void();
			}

			tr.reset();
		} catch (Error& e) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<bool> isDataDistributionEnabled( Database cx ) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<Value> mode = wait( tr.get( dataDistributionModeKey ) );
			if (!mode.present()) return true;
			if (mode.present()) {
				BinaryReader rd( mode.get(), Unversioned() );
				int m;
				rd >> m;
				if (m) return true;
			}
			// SOMEDAY: Write a wrapper in MoveKeys.actor.h
			Optional<Value> readVal = wait( tr.get( moveKeysLockOwnerKey ) );
			UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			if( currentOwner != dataDistributionModeLock )
				return true;
			return false;
		} catch (Error& e) {
			wait( tr.onError(e) );
		}
	}
}

//Ensures that the serverKeys key space is properly coalesced
//This method is only used for testing and is not implemented in a manner that is safe for large databases
ACTOR Future<Void> debugCheckCoalescing(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			state Standalone<RangeResultRef> serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT( !serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			state int i;
			for(i = 0; i < serverList.size(); i++) {
				state UID id = decodeServerListValue(serverList[i].value).id();
				Standalone<RangeResultRef> ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(id), allKeys));
				ASSERT(ranges.end()[-1].key == allKeys.end);

				for(int j = 0; j < ranges.size() - 2; j++)
					if(ranges[j].value == ranges[j + 1].value)
						TraceEvent(SevError, "UncoalescedValues", id).detail("Key1", printable(ranges[j].key)).detail("Key2", printable(ranges[j + 1].key)).detail("Value", printable(ranges[j].value));
			}

			TraceEvent("DoneCheckingCoalescing");
			return Void();
		}
		catch(Error &e){
			wait( tr.onError(e) );
		}
	}
}

static std::set<int> const& normalDDQueueErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert( error_code_movekeys_conflict );
		s.insert( error_code_broken_promise );
	}
	return s;
}

ACTOR Future<Void> pollMoveKeysLock( Database cx, MoveKeysLock lock ) {
	loop {
		wait(delay(SERVER_KNOBS->MOVEKEYS_LOCK_POLLING_DELAY));
		state Transaction tr(cx);
		loop {
			try {
				wait( checkMoveKeysLockReadOnly(&tr, lock) );
				break;
			} catch( Error &e ) {
				wait( tr.onError(e) );
			}
		}
	}
}

struct DataDistributorData : NonCopyable, ReferenceCounted<DataDistributorData> {
	Reference<AsyncVar<struct ServerDBInfo>> dbInfo;
	UID ddId;
	PromiseStream<Future<Void>> addActor;

	DataDistributorData(Reference<AsyncVar<ServerDBInfo>> const& db, UID id) : dbInfo(db), ddId(id) {}
};

ACTOR Future<Void> monitorBatchLimitedTime(Reference<AsyncVar<ServerDBInfo>> db, double* lastLimited) {
	loop {
		wait( delay(SERVER_KNOBS->METRIC_UPDATE_RATE) );

		state Reference<ProxyInfo> proxies(new ProxyInfo(db->get().client.proxies, db->get().myLocality));

		choose {
			when (wait(db->onChange())) {}
			when (GetHealthMetricsReply reply = wait(proxies->size() ?
					loadBalance(proxies, &MasterProxyInterface::getHealthMetrics, GetHealthMetricsRequest(false))
					: Never())) {
				if (reply.healthMetrics.batchLimited) {
					*lastLimited = now();
				}
			}
		}
	}
}

ACTOR Future<Void> dataDistribution(Reference<DataDistributorData> self)
{
	state double lastLimited = 0;
	self->addActor.send( monitorBatchLimitedTime(self->dbInfo, &lastLimited) );

	state Database cx = openDBOnServer(self->dbInfo, TaskDataDistributionLaunch, true, true);
	cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;

	//cx->setOption( FDBDatabaseOptions::LOCATION_CACHE_SIZE, StringRef((uint8_t*) &SERVER_KNOBS->DD_LOCATION_CACHE_SIZE, 8) );
	//ASSERT( cx->locationCacheSize == SERVER_KNOBS->DD_LOCATION_CACHE_SIZE );

	//wait(debugCheckCoalescing(cx));
	state std::vector<Optional<Key>> primaryDcId;
	state std::vector<Optional<Key>> remoteDcIds;
	state DatabaseConfiguration configuration;
	state Reference<InitialDataDistribution> initData;
	state MoveKeysLock lock;
	loop {
		try {
			loop {
				TraceEvent("DDInitTakingMoveKeysLock", self->ddId);
				MoveKeysLock lock_ = wait( takeMoveKeysLock( cx, self->ddId ) );
				lock = lock_;
				TraceEvent("DDInitTookMoveKeysLock", self->ddId);

				DatabaseConfiguration configuration_ = wait( getDatabaseConfiguration(cx) );
				configuration = configuration_;
				primaryDcId.clear();
				remoteDcIds.clear();
				const std::vector<RegionInfo>& regions = configuration.regions;
				if ( configuration.regions.size() > 0 ) {
					primaryDcId.push_back( regions[0].dcId );
				}
				if ( configuration.regions.size() > 1 ) {
					remoteDcIds.push_back( regions[1].dcId );
				}

				TraceEvent("DDInitGotConfiguration", self->ddId).detail("Conf", configuration.toString());

				state Transaction tr(cx);
				loop {
					try {
						tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
						tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );

						Standalone<RangeResultRef> replicaKeys = wait(tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY));

						for(auto& kv : replicaKeys) {
							auto dcId = decodeDatacenterReplicasKey(kv.key);
							auto replicas = decodeDatacenterReplicasValue(kv.value);
							if((primaryDcId.size() && primaryDcId[0] == dcId) || (remoteDcIds.size() && remoteDcIds[0] == dcId && configuration.usableRegions > 1)) {
								if(replicas > configuration.storageTeamSize) {
									tr.set(kv.key, datacenterReplicasValue(configuration.storageTeamSize));
								}
							} else {
								tr.clear(kv.key);
							}
						}

						wait(tr.commit());
						break;
					}
					catch(Error &e) {
						wait(tr.onError(e));
					}
				}

				TraceEvent("DDInitUpdatedReplicaKeys", self->ddId);
				Reference<InitialDataDistribution> initData_ = wait( getInitialDataDistribution(cx, self->ddId, lock, configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>() ) );
				initData = initData_;
				if(initData->shards.size() > 1) {
					TraceEvent("DDInitGotInitialDD", self->ddId)
					    .detail("B", printable(initData->shards.end()[-2].key))
					    .detail("E", printable(initData->shards.end()[-1].key))
					    .detail("Src", describe(initData->shards.end()[-2].primarySrc))
					    .detail("Dest", describe(initData->shards.end()[-2].primaryDest))
					    .trackLatest("InitialDD");
				} else {
					TraceEvent("DDInitGotInitialDD", self->ddId).detail("B","").detail("E", "").detail("Src", "[no items]").detail("Dest", "[no items]").trackLatest("InitialDD");
				}

				if (initData->mode) break; // mode may be set true by system operator using fdbcli
				TraceEvent("DataDistributionDisabled", self->ddId);

				TraceEvent("MovingData", self->ddId)
					.detail( "InFlight", 0 )
					.detail( "InQueue", 0 )
					.detail( "AverageShardSize", -1 )
					.detail( "LowPriorityRelocations", 0 )
					.detail( "HighPriorityRelocations", 0 )
					.detail( "HighestPriority", 0 )
					.trackLatest( "MovingData" );

				TraceEvent("TotalDataInFlight", self->ddId).detail("Primary", true).detail("TotalBytes", 0).detail("UnhealthyServers", 0).detail("HighestPriority", 0).trackLatest("TotalDataInFlight");
				TraceEvent("TotalDataInFlight", self->ddId).detail("Primary", false).detail("TotalBytes", 0).detail("UnhealthyServers", 0).detail("HighestPriority", configuration.usableRegions > 1 ? 0 : -1).trackLatest("TotalDataInFlightRemote");

				wait( waitForDataDistributionEnabled(cx) );
				TraceEvent("DataDistributionEnabled");
			}

			// When/If this assertion fails, Evan owes Ben a pat on the back for his foresight
			ASSERT(configuration.storageTeamSize > 0);

			state PromiseStream<RelocateShard> output;
			state PromiseStream<RelocateShard> input;
			state PromiseStream<Promise<int64_t>> getAverageShardBytes;
			state PromiseStream<GetMetricsRequest> getShardMetrics;
			state Reference<AsyncVar<bool>> processingUnhealthy( new AsyncVar<bool>(false) );
			state Promise<Void> readyToStart;
			state Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure( new ShardsAffectedByTeamFailure );

			state int shard = 0;
			for (; shard < initData->shards.size() - 1; shard++) {
				KeyRangeRef keys = KeyRangeRef(initData->shards[shard].key, initData->shards[shard+1].key);
				shardsAffectedByTeamFailure->defineShard(keys);
				std::vector<ShardsAffectedByTeamFailure::Team> teams;
				teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].primarySrc, true));
				if (configuration.usableRegions > 1) {
					teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].remoteSrc, false));
				}
				if(g_network->isSimulated()) {
					TraceEvent("DDInitShard").detail("Keys", printable(keys)).detail("PrimarySrc", describe(initData->shards[shard].primarySrc)).detail("RemoteSrc", describe(initData->shards[shard].remoteSrc))
					.detail("PrimaryDest", describe(initData->shards[shard].primaryDest)).detail("RemoteDest", describe(initData->shards[shard].remoteDest));
				}

				shardsAffectedByTeamFailure->moveShard(keys, teams);
				if (initData->shards[shard].hasDest) {
					// This shard is already in flight.  Ideally we should use dest in sABTF and generate a dataDistributionRelocator directly in
					// DataDistributionQueue to track it, but it's easier to just (with low priority) schedule it for movement.
					bool unhealthy = initData->shards[shard].primarySrc.size() != configuration.storageTeamSize;
					if (!unhealthy && configuration.usableRegions > 1) {
						unhealthy = initData->shards[shard].remoteSrc.size() != configuration.storageTeamSize;
					}
					output.send( RelocateShard( keys, unhealthy ? PRIORITY_TEAM_UNHEALTHY : PRIORITY_RECOVER_MOVE ) );
				}
				wait( yield(TaskDataDistribution) );
			}

			vector<TeamCollectionInterface> tcis;

			Reference<AsyncVar<bool>> anyZeroHealthyTeams;
			vector<Reference<AsyncVar<bool>>> zeroHealthyTeams;
			tcis.push_back(TeamCollectionInterface());
			zeroHealthyTeams.push_back(Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ));
			int storageTeamSize = configuration.storageTeamSize;

			vector<Future<Void>> actors;
			if (configuration.usableRegions > 1) {
				tcis.push_back(TeamCollectionInterface());
				storageTeamSize = 2*configuration.storageTeamSize;

				zeroHealthyTeams.push_back( Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ) );
				anyZeroHealthyTeams = Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) );
				actors.push_back( anyTrue(zeroHealthyTeams, anyZeroHealthyTeams) );
			} else {
				anyZeroHealthyTeams = zeroHealthyTeams[0];
			}

			actors.push_back( pollMoveKeysLock(cx, lock) );
			actors.push_back( reportErrorsExcept( dataDistributionTracker( initData, cx, output, shardsAffectedByTeamFailure, getShardMetrics, getAverageShardBytes.getFuture(), readyToStart, anyZeroHealthyTeams, self->ddId ), "DDTracker", self->ddId, &normalDDQueueErrors() ) );
			actors.push_back( reportErrorsExcept( dataDistributionQueue( cx, output, input.getFuture(), getShardMetrics, processingUnhealthy, tcis, shardsAffectedByTeamFailure, lock, getAverageShardBytes, self->ddId, storageTeamSize, &lastLimited ), "DDQueue", self->ddId, &normalDDQueueErrors() ) );

			vector<DDTeamCollection*> teamCollectionsPtrs;
			Reference<DDTeamCollection> primaryTeamCollection( new DDTeamCollection(cx, self->ddId, lock, output, shardsAffectedByTeamFailure, configuration, primaryDcId, configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>(), readyToStart.getFuture(), zeroHealthyTeams[0], true, processingUnhealthy) );
			teamCollectionsPtrs.push_back(primaryTeamCollection.getPtr());
			if (configuration.usableRegions > 1) {
				Reference<DDTeamCollection> remoteTeamCollection( new DDTeamCollection(cx, self->ddId, lock, output, shardsAffectedByTeamFailure, configuration, remoteDcIds, Optional<std::vector<Optional<Key>>>(), readyToStart.getFuture() && remoteRecovered(self->dbInfo), zeroHealthyTeams[1], false, processingUnhealthy) );
				teamCollectionsPtrs.push_back(remoteTeamCollection.getPtr());
				remoteTeamCollection->teamCollections = teamCollectionsPtrs;
				actors.push_back( reportErrorsExcept( dataDistributionTeamCollection( remoteTeamCollection, initData, tcis[1], self->dbInfo ), "DDTeamCollectionSecondary", self->ddId, &normalDDQueueErrors() ) );
			}
			primaryTeamCollection->teamCollections = teamCollectionsPtrs;
			actors.push_back( reportErrorsExcept( dataDistributionTeamCollection( primaryTeamCollection, initData, tcis[0], self->dbInfo ), "DDTeamCollectionPrimary", self->ddId, &normalDDQueueErrors() ) );
			actors.push_back(yieldPromiseStream(output.getFuture(), input));

			wait( waitForAll( actors ) );
			return Void();
		}
		catch( Error &e ) {
			state Error err = e;
			if( e.code() != error_code_movekeys_conflict )
				throw err;
			bool ddEnabled = wait( isDataDistributionEnabled(cx) );
			TraceEvent("DataDistributionMoveKeysConflict").detail("DataDistributionEnabled", ddEnabled);
			if( ddEnabled )
				throw err;
		}
	}
}

static std::set<int> const& normalDataDistributorErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert( error_code_worker_removed );
		s.insert( error_code_broken_promise );
		s.insert( error_code_actor_cancelled );
		s.insert( error_code_please_reboot );
		s.insert( error_code_movekeys_conflict );
	}
	return s;
}

ACTOR Future<Void> dataDistributor(DataDistributorInterface di, Reference<AsyncVar<struct ServerDBInfo>> db ) {
	state Reference<DataDistributorData> self( new DataDistributorData(db, di.id()) );
	state Future<Void> collection = actorCollection( self->addActor.getFuture() );

	try {
		TraceEvent("DataDistributor_Running", di.id());
		self->addActor.send( waitFailureServer(di.waitFailure.getFuture()) );
		state Future<Void> distributor = reportErrorsExcept( dataDistribution(self), "DataDistribution", di.id(), &normalDataDistributorErrors() );

		loop choose {
			when ( wait(distributor || collection) ) {
				ASSERT(false);
				throw internal_error();
			}
			when ( HaltDataDistributorRequest req = waitNext(di.haltDataDistributor.getFuture()) ) {
				req.reply.send(Void());
				TraceEvent("DataDistributorHalted", di.id()).detail("ReqID", req.requesterID);
				break;
			}
		}
	}
	catch ( Error &err ) {
		if ( normalDataDistributorErrors().count(err.code()) == 0 ) {
			TraceEvent("DataDistributor_Error", di.id()).error(err, true);
			throw err;
		}
		TraceEvent("DataDistributor_Died", di.id()).error(err, true);
	}

	return Void();
}

DDTeamCollection* testTeamCollection(int teamSize, Reference<IReplicationPolicy> policy, int processCount) {
	Database database = DatabaseContext::create(
		Reference<AsyncVar<ClientDBInfo>>(new AsyncVar<ClientDBInfo>()),
		Never(),
		LocalityData(),
		false
	);

	DatabaseConfiguration conf;
	conf.storageTeamSize = teamSize;
	conf.storagePolicy = policy;

	DDTeamCollection* collection = new DDTeamCollection(
		database,
		UID(0, 0),
		MoveKeysLock(),
		PromiseStream<RelocateShard>(),
		Reference<ShardsAffectedByTeamFailure>(new ShardsAffectedByTeamFailure()),
		conf,
		{},
		{},
		Future<Void>(Void()),
		Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ),
		true,
		Reference<AsyncVar<bool>>( new AsyncVar<bool>(false) )
	);

	for (int id = 1; id <= processCount; ++id) {
		UID uid(id, 0);
		StorageServerInterface interface;
		interface.uniqueID = uid;
	 	interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(id % 5)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(id % 3)));
		collection->server_info[uid] = Reference<TCServerInfo>(new TCServerInfo(interface, ProcessClass(), true, collection->storageServerSet));
		collection->server_status.set(uid, ServerStatus(false, false, interface.locality));
		collection->checkAndCreateMachine(collection->server_info[uid]);
	}

	return collection;
}

DDTeamCollection* testMachineTeamCollection(int teamSize, Reference<IReplicationPolicy> policy, int processCount) {
	Database database = DatabaseContext::create(Reference<AsyncVar<ClientDBInfo>>(new AsyncVar<ClientDBInfo>()),
	                                            Never(), LocalityData(), false);

	DatabaseConfiguration conf;
	conf.storageTeamSize = teamSize;
	conf.storagePolicy = policy;

	DDTeamCollection* collection =
	    new DDTeamCollection(database, UID(0, 0), MoveKeysLock(), PromiseStream<RelocateShard>(),
	                         Reference<ShardsAffectedByTeamFailure>(new ShardsAffectedByTeamFailure()), conf, {}, {},
	                         Future<Void>(Void()),
	                         Reference<AsyncVar<bool>>(new AsyncVar<bool>(true)), true,
	                         Reference<AsyncVar<bool>>(new AsyncVar<bool>(false)));

	for (int id = 1; id <= processCount; id++) {
		UID uid(id, 0);
		StorageServerInterface interface;
		interface.uniqueID = uid;
		int process_id = id;
		int dc_id = process_id / 1000;
		int data_hall_id = process_id / 100;
		int zone_id = process_id / 10;
		int machine_id = process_id / 5;

		printf("testMachineTeamCollection: process_id:%d zone_id:%d machine_id:%d ip_addr:%s\n", process_id, zone_id,
		       machine_id, interface.address().toString().c_str());
		interface.locality.set(LiteralStringRef("processid"), Standalone<StringRef>(std::to_string(process_id)));
		interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(machine_id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(zone_id)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(data_hall_id)));
		interface.locality.set(LiteralStringRef("dcid"), Standalone<StringRef>(std::to_string(dc_id)));
		collection->server_info[uid] =
		    Reference<TCServerInfo>(new TCServerInfo(interface, ProcessClass(), true, collection->storageServerSet));

		collection->server_status.set(uid, ServerStatus(false, false, interface.locality));
	}

	int totalServerIndex = collection->constructMachinesFromServers();
	printf("testMachineTeamCollection: construct machines for %d servers\n", totalServerIndex);

	return collection;
}

TEST_CASE("DataDistribution/AddTeamsBestOf/UseMachineID") {
	wait(Future<Void>(Void()));

	int teamSize = 3; // replication size
	int processSize = 60;
	int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state DDTeamCollection* collection = testMachineTeamCollection(teamSize, policy, processSize);

	int result = collection->addTeamsBestOf(30, desiredTeams, maxTeams);

	ASSERT(collection->sanityCheckTeams() == true);

	delete (collection);

	return Void();
}

TEST_CASE("DataDistribution/AddTeamsBestOf/NotUseMachineID") {
	wait(Future<Void>(Void()));

	int teamSize = 3; // replication size
	int processSize = 60;
	int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(teamSize, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state DDTeamCollection* collection = testMachineTeamCollection(teamSize, policy, processSize);

	if (collection == NULL) {
		fprintf(stderr, "collection is null\n");
		return Void();
	}

	collection->addBestMachineTeams(30); // Create machine teams to help debug
	int result = collection->addTeamsBestOf(30, desiredTeams, maxTeams);
	collection->sanityCheckTeams(); // Server team may happen to be on the same machine team, although unlikely

	if (collection) delete (collection);

	return Void();
}

TEST_CASE("DataDistribution/AddAllTeams/isExhaustive") {
	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state int processSize = 10;
	state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
	state DDTeamCollection* collection = testTeamCollection(3, policy, processSize);

	int result = collection->addTeamsBestOf(200, desiredTeams, maxTeams);

	delete(collection);

	// The maximum number of available server teams without considering machine locality is 120
	// The maximum number of available server teams with machine locality constraint is 120 - 40, because
	// the 40 (5*4*2) server teams whose servers come from the same machine are invalid.
	ASSERT(result == 80);

	return Void();
}

TEST_CASE("/DataDistribution/AddAllTeams/withLimit") {
	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state int processSize = 10;
	state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;

	state DDTeamCollection* collection = testTeamCollection(3, policy, processSize);

	int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

	delete(collection);

	ASSERT(result == 10);

	return Void();
}

TEST_CASE("/DataDistribution/AddTeamsBestOf/SkippingBusyServers") {
	wait(Future<Void>(Void()));
	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state int processSize = 10;
	state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
	state DDTeamCollection* collection = testTeamCollection(3, policy, processSize);

	collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), true);
	collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), true);

	int result = collection->addTeamsBestOf(8, desiredTeams, maxTeams);

	ASSERT(result == 8);

	for(auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		auto teamCount = process->second->teams.size();
		ASSERT(teamCount >= 1);
		ASSERT(teamCount <= 5);
	}

	delete(collection);

	return Void();
}

// Due to the randomness in choosing the machine team and the server team from the machine team, it is possible that
// we may not find the remaining several (e.g., 1 or 2) available teams.
// It is hard to conclude what is the minimum number of  teams the addTeamsBestOf() should create in this situation.
TEST_CASE("/DataDistribution/AddTeamsBestOf/NotEnoughServers") {
	wait(Future<Void>(Void()));

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(new PolicyAcross(3, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
	state int processSize = 5;
	state int desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER * processSize;
	state int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER * processSize;
	state DDTeamCollection* collection = testTeamCollection(3, policy, processSize);

	collection->addTeam(std::set<UID>({ UID(1, 0), UID(2, 0), UID(3, 0) }), true);
	collection->addTeam(std::set<UID>({ UID(1, 0), UID(3, 0), UID(4, 0) }), true);

	int resultMachineTeams = collection->addBestMachineTeams(10);
	int result = collection->addTeamsBestOf(10, desiredTeams, maxTeams);

	if (collection->machineTeams.size() != 10 || result != 8) {
		collection->traceAllInfo(true); // Debug message
	}

	// NOTE: Due to the pure randomness in selecting a machine for a machine team,
	// we cannot guarantee that all machine teams are created.
	// When we chnage the selectReplicas function to achieve such guarantee, we can enable the following ASSERT
	ASSERT(collection->machineTeams.size() == 10); // Should create all machine teams

	// We need to guarantee a server always have at least a team so that the server can participate in data distribution
	for (auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		auto teamCount = process->second->teams.size();
		ASSERT(teamCount >= 1);
	}

	delete(collection);

	// If we find all available teams, result will be 8 because we prebuild 2 teams
	ASSERT(result == 8);

	return Void();
}
