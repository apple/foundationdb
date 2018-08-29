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

#include "flow/actorcompiler.h"
#include "flow/ActorCollection.h"
#include "DataDistribution.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DatabaseContext.h"
#include "MoveKeys.h"
#include "Knobs.h"
#include <set>
#include "WaitFailure.h"
#include "ServerDBInfo.h"
#include "IKeyValueStore.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbrpc/Replication.h"
#include "flow/UnitTest.h"

class TCTeamInfo;
class TCMachineTeamInfo;
struct TCMachineInfo;

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

	TCServerInfo(StorageServerInterface ssi, ProcessClass processClass) : id(ssi.id()), lastKnownInterface(ssi),
	 			lastKnownClass(processClass), dataInFlightToServer(0),
	 			onInterfaceChanged(interfaceChanged.getFuture()), onRemoved(removed.getFuture()) {}
	//copy the non-flow field to create the new class
	TCServerInfo(TCServerInfo &server): id(server.id), lastKnownInterface(server.lastKnownInterface),
	 									lastKnownClass(server.lastKnownClass), teams(server.teams) {}

	//sort TCServerInfo with its UID, used for std::set in TCMachineInfo
	bool operator < (const TCServerInfo &rhs) const {
		return this->id < rhs.id;
	}
};

//TODO: used to change serversOnMachine from vector to set
bool lessCompareTCServerInfo(Reference<TCServerInfo>&lhs, Reference<TCServerInfo> &rhs) {
	return lhs->id < rhs->id;
}


struct TCMachineInfo: public ReferenceCounted<TCMachineInfo> {
	//std::set< Reference<TCServerInfo>, bool(*)(Reference<TCServerInfo>&, Reference<TCServerInfo>&) > serversOnMachine; //TODO: Better use set
	std::vector< Reference<TCServerInfo> > serversOnMachine;
	Standalone<StringRef> machineID;
	std::vector< Reference<TCMachineTeamInfo> > machineTeams;
	LocalityEntry localityEntry;

	TCMachineInfo(Reference<TCServerInfo> server, LocalityEntry entry): localityEntry(entry) {
			serversOnMachine.push_back(server);
			machineID = server->lastKnownInterface.locality.machineId().get();
	}

	/*
	 * Return the total number of machine teams the machine belongs
	 */
	int getTotalMachineTeamCount () { //TODO: To  count the number of machine teams a machine belongs to
		return machineTeams.size();
	}

	/*
	 * Return the total number of server teams that all servers on this machine belong to
	 */
	int getTotalServerTeamCount () {
		int count = 0;

		for ( auto &server: serversOnMachine ) {
			assert(server->teams.size() >= 0);
			count += server->teams.size();
		}
		return count;
	}

	void findLeastUsedServers(std::vector<UID> &leastUsedServers) {
		int minTeamNumber = std::numeric_limits<int>::max();
		for ( auto &server: serversOnMachine ) {
			if ( server->teams.size() < minTeamNumber ) {
				minTeamNumber = server->teams.size();
				leastUsedServers.clear();
			}
			if ( minTeamNumber <= server->teams.size() ) {
				leastUsedServers.push_back(server->id);
			}
		}
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
			when( Void _ = wait( serverRemoved ) ) {
				return Void();
			}
			when( Void _ = wait( resetRequest ) ) { //To prevent a tight spin loop
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
	Void _ = wait( updateServerMetrics( server.getPtr() ) );
	return Void();
}

/**
 * Machine team information
 */
class TCMachineTeamInfo : public ReferenceCounted<TCMachineTeamInfo> {
public:
	vector< Reference<TCMachineInfo> > machines;
	vector< Standalone<StringRef> > machineIDs;

	vector< Standalone<StringRef> > getMachineIDs() {
		return machineIDs;
	}

	void getMachineIDsStr(vector<std::string> &machineIDsStr) {
		for ( auto &machineID : machineIDs ) {
			machineIDsStr.push_back(machineID.contents().toString());
		}
	}

	TCMachineTeamInfo( vector< Reference<TCMachineInfo> > const& machines ) : machines(machines) {
		machineIDs.reserve(machines.size());
		for(int i=0; i<machines.size(); i++)
			machineIDs.push_back(machines[i]->machineID);
		sort(machineIDs.begin(), machineIDs.end());
	}

	std::string getMachineIDsStr() {
		std::string str;
		for ( auto &id: machineIDs ) {
			str = str + id.contents().toString() + ",";
		}
		/*
		if ( str.length() )
			str = str.substr(0, str.length() - 1);
		 */
		return str;
	}

	int getTotalMachineTeamNumber() {
		int count = 0;
		for ( auto &machine: machines ) {
			assert(machine->machineTeams.size() >= 0);
			count += machine->machineTeams.size();
		}
		return count;
	}

};


class TCTeamInfo : public ReferenceCounted<TCTeamInfo>, public IDataDistributionTeam {
public:
	vector< Reference<TCServerInfo> > servers;
	vector<UID> serverIDs;
	Future<Void> tracker;
	bool healthy;
	bool wrongConfiguration; //True if any of the servers in the team have the wrong configuration
	int priority;

	TCTeamInfo( vector< Reference<TCServerInfo> > const& servers )
		: servers(servers), healthy(true), priority(PRIORITY_TEAM_HEALTHY), wrongConfiguration(false)
	{
		serverIDs.reserve(servers.size());
		for(int i=0; i<servers.size(); i++)
			serverIDs.push_back(servers[i]->id);
	}
	virtual vector<StorageServerInterface> getLastKnownServerInterfaces() {
		vector<StorageServerInterface> v;
		v.reserve(servers.size());
		for(int i=0; i<servers.size(); i++)
			v.push_back(servers[i]->lastKnownInterface);
		return v;
	}
	virtual int size() { return servers.size(); }
	virtual vector<UID> const& getServerIDs() { return serverIDs; }
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
		Void _ = wait( waitForAll( updates ) );
		return Void();
	}
};

struct ServerStatus {
	bool isFailed;
	bool isUndesired;
	bool isWrongConfiguration;
	LocalityData locality;
	ServerStatus() : isFailed(true), isUndesired(false), isWrongConfiguration(false) {}
	ServerStatus( bool isFailed, bool isUndesired, LocalityData const& locality ) : isFailed(isFailed), isUndesired(isUndesired), locality(locality), isWrongConfiguration(false) {}
	bool isUnhealthy() const { return isFailed || isUndesired; }
	const char* toString() const { return isFailed ? "Failed" : isUndesired ? "Undesired" : "Healthy"; }

	bool operator == (ServerStatus const& r) const { return isFailed == r.isFailed && isUndesired == r.isUndesired && isWrongConfiguration == r.isWrongConfiguration && locality.zoneId() == r.locality.zoneId(); }

	//If a process has reappeared without the storage server that was on it (isFailed == true), we don't need to exclude it
	//We also don't need to exclude processes who are in the wrong configuration (since those servers will be removed)
	bool excludeOnRecruit() { return !isFailed && !isWrongConfiguration; }
};
typedef AsyncMap<UID, ServerStatus> ServerStatusMap;

ACTOR Future<Void> waitForAllDataRemoved( Database cx, UID serverID, Version addedVersion ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Version ver = wait( tr.getReadVersion() );

			//we cannot remove a server immediately after adding it, because a perfectly timed master recovery could cause us to not store the mutations sent to the short lived storage server.
			if(ver > addedVersion + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
				bool canRemove = wait( canRemoveStorageServer( &tr, serverID ) );
				if (canRemove) {
					return Void();
				}
			}

			// Wait for any change to the serverKeys for this server
			Void _ = wait( delay(SERVER_KNOBS->ALL_DATA_REMOVED_DELAY, TaskDataDistribution) );
			//Void _ = tr.waitForChanges( KeyRangeRef( serverKeysPrefixFor(serverID),
			//										 serverKeysPrefixFor(serverID).toString() + allKeys.end.toString() ) );
			tr.reset();
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> storageServerFailureTracker(
	Database cx,
	StorageServerInterface server,
	ServerStatusMap *statusMap,
	ServerStatus *status,
	Debouncer* restartRecruiting,
	int64_t *unhealthyServers,
	UID masterId,
	Version addedVersion )
{
	loop {
		bool unhealthy = statusMap->count(server.id()) && statusMap->get(server.id()).isUnhealthy();
		if(unhealthy && !status->isUnhealthy()) {
			(*unhealthyServers)--;
		}
		if(!unhealthy && status->isUnhealthy()) {
			(*unhealthyServers)++;
		}

		statusMap->set( server.id(), *status );
		if( status->isFailed )
			restartRecruiting->trigger(); //MX: recruite a storageSever when a storageSever fails

		state double startTime = now();
		choose {
			when ( Void _ = wait( status->isFailed
				? IFailureMonitor::failureMonitor().onStateEqual( server.waitFailure.getEndpoint(), FailureStatus(false) )
				: waitFailureClient(server.waitFailure, SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME, 0, TaskDataDistribution) ) )
			{
				double elapsed = now() - startTime;
				if(!status->isFailed && elapsed < SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME) {
					Void _ = wait(delay(SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME - elapsed));
				}
				status->isFailed = !status->isFailed;
				TraceEvent("StatusMapChange", masterId).detail("ServerID", server.id()).detail("Status", status->toString())
					.detail("Available", IFailureMonitor::failureMonitor().getState(server.waitFailure.getEndpoint()).isAvailable());
			}
			when ( Void _ = wait( status->isUnhealthy() ? waitForAllDataRemoved(cx, server.id(), addedVersion) : Never() ) ) { break; }
		}
	}

	return Void();
}

// Read keyservers, return unique set of teams
ACTOR Future<Reference<InitialDataDistribution>> getInitialDataDistribution( Database cx, UID masterId, MoveKeysLock moveKeysLock, std::vector<Optional<Key>> remoteDcIds ) {
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
			if(!result->mode) //MX:Q: when result->mode can be changed to 0?
				return result;


			state Future<vector<ProcessData>> workers = getWorkers(&tr);
			state Future<Standalone<RangeResultRef>> serverList = tr.getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY );
			Void _ = wait( success(workers) && success(serverList) );
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
			Void _ = wait( tr.onError(e) );

			ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
			TraceEvent("GetInitialTeamsRetry", masterId);
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
				Void _ = wait(checkMoveKeysLockReadOnly(&tr, moveKeysLock));
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
				Void _ = wait( tr.onError(e) );

				ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
				TraceEvent("GetInitialTeamsKeyServersRetry", masterId);
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
	ServerStatusMap* const& statusMap,
	MoveKeysLock const& lock,
	UID const& masterId,
	std::map<UID, Reference<TCServerInfo>>* const& other_servers,
	Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >> const& changes,
	Promise<Void> const& errorOut,
	Version const& addedVersion);

Future<Void> teamTracker( struct DDTeamCollection* const& self, Reference<IDataDistributionTeam> const& team );

struct DDTeamCollection {
	enum { REQUESTING_WORKER = 0, GETTING_WORKER = 1, GETTING_STORAGE = 2 };

	PromiseStream<Future<Void>> addActor;
	Database cx;
	UID masterId;
	DatabaseConfiguration configuration;

	bool doBuildTeams; //MX:
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	vector<UID> allServers;
	ServerStatusMap server_status;
	int64_t unhealthyServers;
	std::map<int,int> priority_teams;
	std::map<UID, Reference<TCServerInfo>> server_info;

	std::map< Standalone<StringRef>, Reference<TCMachineInfo> > machine_info; //all machines' info. The first has to be unique across processes on the same machine! The reference pointer should not work
	std::vector< Reference<TCMachineTeamInfo> > machineTeams; //machine teams to help create teams //TODO: replace vector with set?
	Reference<LocalityMap<UID>> server_localityMap; //find locality info

	vector<Reference<TCTeamInfo>> teams;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	PromiseStream<UID> removedServers;
	std::set<UID> recruitingIds; // The IDs of the SS which are being recruited
	std::set<NetworkAddress> recruitingLocalities;
	Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >> serverChanges;
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

	DDTeamCollection(
		Database const& cx,
		UID masterId,
		MoveKeysLock const& lock,
		PromiseStream<RelocateShard> const& output,
		Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
		DatabaseConfiguration configuration,
		std::vector<Optional<Key>> includedDCs,
		Optional<std::vector<Optional<Key>>> otherTrackedDCs,
		Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >> const& serverChanges,
		Future<Void> readyToStart, Reference<AsyncVar<bool>> zeroHealthyTeams, bool primary,
		Reference<AsyncVar<bool>> processingUnhealthy)
		:cx(cx), masterId(masterId), lock(lock), output(output), shardsAffectedByTeamFailure(shardsAffectedByTeamFailure), doBuildTeams( true ), teamBuilder( Void() ),
		 configuration(configuration), serverChanges(serverChanges), readyToStart(readyToStart), checkTeamDelay( delay( SERVER_KNOBS->CHECK_TEAM_DELAY, TaskDataDistribution) ),
		 initialFailureReactionDelay( delayed( readyToStart, SERVER_KNOBS->INITIAL_FAILURE_REACTION_DELAY, TaskDataDistribution ) ), healthyTeamCount( 0 ),
		 initializationDoneActor(logOnCompletion(readyToStart && initialFailureReactionDelay, this)), optimalTeamCount( 0 ), recruitingStream(0), restartRecruiting( SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY ),
		 unhealthyServers(0), includedDCs(includedDCs), otherTrackedDCs(otherTrackedDCs), zeroHealthyTeams(zeroHealthyTeams), zeroOptimalTeams(true), primary(primary), processingUnhealthy(processingUnhealthy)
	{
		if(!primary || configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", masterId)
				.detail( "State", "Inactive" )
				.trackLatest( format("%s/DDTrackerStarting", printable(cx->dbName).c_str() ).c_str() );
		}
	}

	~DDTeamCollection() {
		// The following kills a reference cycle between the teamTracker actor and the TCTeamInfo that both holds and is held by the actor
		// It also ensures that the trackers are done fiddling with healthyTeamCount before we free this
		for(int i=0; i < teams.size(); i++) {
			teams[i]->tracker.cancel();
		}
		// The following makes sure that, even if a reference to a team is held in the DD Queue, the tracker will be stopped
		//  before the server_status map to which it has a pointer, is destroyed.
		for(auto it = server_info.begin(); it != server_info.end(); ++it) {
			it->second->tracker.cancel();
		}

		teamBuilder.cancel();
	}

	ACTOR Future<Void> logOnCompletion( Future<Void> signal, DDTeamCollection *self ) {
		Void _ = wait(signal);
		Void _ = wait(delay(SERVER_KNOBS->LOG_ON_COMPLETION_DELAY, TaskDataDistribution));

		if(!self->primary || self->configuration.usableRegions == 1) {
			TraceEvent("DDTrackerStarting", self->masterId)
				.detail( "State", "Active" )
				.trackLatest( format("%s/DDTrackerStarting", printable(self->cx->dbName).c_str() ).c_str() );
		}

		return Void();
	}

	ACTOR Future<Void> checkBuildTeams( DDTeamCollection* self ) {
		state Promise<Void> restart;

		Void _ = wait( self->checkTeamDelay );
		while( !self->teamBuilder.isReady() )
			Void _ = wait( self->teamBuilder );

		if( self->doBuildTeams ) {
			self->doBuildTeams = false;
			try {
				loop {
					Promise<Void> oldRestart = restart;
					restart = Promise<Void>();
					self->teamBuilder = self->buildTeams( self ) || restart.getFuture();
					oldRestart.send( Void() );
					choose {
						when( Void _ = wait( self->teamBuilder ) ) { break; }
						when( Void _ = wait( self->restartTeamBuilder.onTrigger() ) ) {}
					}
				}
			}
			catch(Error &e) {
				if(!restart.isSet()) {
					restart.send(Void());
				}
				throw;
			}
		}

		return Void();
	}

	// SOMEDAY: Make bestTeam better about deciding to leave a shard where it is (e.g. in PRIORITY_TEAM_HEALTHY case)
	//		    use keys, src, dest, metrics, priority, system load, etc.. to decide...
	ACTOR Future<Void> getTeam( DDTeamCollection* self, GetTeamRequest req ) { //MX:
		try {
			Void _ = wait( self->checkBuildTeams( self ) );

			// Select the best team
			// Currently the metric is minimum used disk space (adjusted for data in flight)
			// Only healthy teams may be selected. The team has to be healthy at the moment we update
			//   shardsAffectedByTeamFailure or we could be dropping a shard on the floor (since team
			//   tracking is "edge triggered")
			// SOMEDAY: Account for capacity, load (when shardMetrics load is high)

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
					if( !self->server_info.count( req.sources[i] ) ) {
						TEST( true ); // GetSimilarTeams source server now unknown
					}
					else {
						auto& teamList = self->server_info[ req.sources[i] ]->teams;
						for( int j = 0; j < teamList.size(); j++ ) {
							if( teamList[j]->isHealthy() && (!req.preferLowerUtilization || teamList[j]->hasHealthyFreeSpace())) {
								int sharedMembers = 0;
								for( int k = 0; k < teamList[j]->serverIDs.size(); k++ )
									if( sources.count( teamList[j]->serverIDs[k] ) )
										sharedMembers++;

								if( !foundExact && sharedMembers == teamList[j]->serverIDs.size() ) {
									foundExact = true;
									bestOption = Optional<Reference<IDataDistributionTeam>>();
									similarTeams.clear();
								}

								if( (sharedMembers == teamList[j]->serverIDs.size()) || (!foundExact && req.wantsTrueBest) ) {
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
					req.reply.send( bestOption );
					return Void();
				}

				if( !req.wantsTrueBest ) {
					while( similarTeams.size() && randomTeams.size() < SERVER_KNOBS->BEST_TEAM_OPTION_COUNT ) {
						int randomTeam = g_random->randomInt( 0, similarTeams.size() );
						randomTeams.push_back( std::make_pair( SOME_SHARED, similarTeams[randomTeam] ) );
						std::swap( similarTeams[randomTeam], similarTeams.back() );
						similarTeams.pop_back();
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
							for( int k = 0; k < teamList[j]->serverIDs.size(); k++ ) {
								if( !completeSources.count( teamList[j]->serverIDs[k] ) ) {
									found = false;
									break;
								}
							}
							if(found && teamList[j]->serverIDs.size() > bestSize) {
								bestOption = teamList[j];
								bestSize = teamList[j]->serverIDs.size();
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

	void addSubsetOfEmergencyTeams() {
		for( int i = 0; i < teams.size(); i++ ) {
			if( teams[i]->servers.size() > configuration.storageTeamSize ) {
				auto& serverIds = teams[i]->getServerIDs();
				bool foundTeam = false;
				for( int j = 0; j < std::max( 1, (int)(serverIds.size() - configuration.storageTeamSize + 1) ) && !foundTeam; j++ ) {
					auto& serverTeams = server_info[serverIds[j]]->teams;
					for( int k = 0; k < serverTeams.size(); k++ ) {
						auto &testTeam = serverTeams[k]->getServerIDs();
						bool allInTeam = true;
						for( int l = 0; l < testTeam.size(); l++ ) {
							if( std::find( serverIds.begin(), serverIds.end(), testTeam[l] ) == serverIds.end() ) {
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
					addTeam(serverIds.begin(), serverIds.begin() + configuration.storageTeamSize );
				}
			}
		}
	}

	//MX: START POINT for data distribution
	void init( InitialDataDistribution const& initTeams ) {
		// SOMEDAY: If some servers have teams and not others (or some servers have more data than others) and there is an address/locality collision, should
		// we preferentially mark the least used server as undesirable?
		for (auto i = initTeams.allServers.begin(); i != initTeams.allServers.end(); ++i) {
			if (shouldHandleServer(i->first)) {
				addServer(i->first, i->second, serverTrackerErrorOut, 0);
			}
		}

		if(primary) {
			for(auto t = initTeams.primaryTeams.begin(); t != initTeams.primaryTeams.end(); ++t) {
				addTeam(t->begin(), t->end() );
			}
		} else {
			for(auto t = initTeams.remoteTeams.begin(); t != initTeams.remoteTeams.end(); ++t) {
				addTeam(t->begin(), t->end() );
			}
		}

		addSubsetOfEmergencyTeams();
	}

	void evaluateTeamQuality() {
		int teamCount = teams.size(), serverCount = allServers.size();
		double teamsPerServer = (double)teamCount * configuration.storageTeamSize / serverCount;

		ASSERT( serverCount == server_info.size() );

		int minTeams = 100000, maxTeams = 0;
		double varTeams = 0;

		std::map<Optional<Standalone<StringRef>>, int> machineTeams;
		for(auto s = server_info.begin(); s != server_info.end(); ++s) {
			if(!server_status.get(s->first).isUnhealthy()) {
				int stc = s->second->teams.size();
				minTeams = std::min(minTeams, stc);
				maxTeams = std::max(maxTeams, stc);
				varTeams += (stc - teamsPerServer)*(stc - teamsPerServer);
				machineTeams[s->second->lastKnownInterface.locality.zoneId()] += stc;
			}
		}
		varTeams /= teamsPerServer*teamsPerServer;

		int minMachineTeams = 100000, maxMachineTeams = 0;
		for( auto m = machineTeams.begin(); m != machineTeams.end(); ++m ) {
			minMachineTeams = std::min( minMachineTeams, m->second );
			maxMachineTeams = std::max( maxMachineTeams, m->second );
		}

		TraceEvent(
			minTeams>0 ? SevInfo : SevWarn,
			"DataDistributionTeamQuality", masterId)
			.detail("Servers", serverCount)
			.detail("Teams", teamCount)
			.detail("TeamsPerServer", teamsPerServer)
			.detail("Variance", varTeams/serverCount)
			.detail("ServerMinTeams", minTeams)
			.detail("ServerMaxTeams", maxTeams)
			.detail("MachineMinTeams", minMachineTeams)
			.detail("MachineMaxTeams", maxMachineTeams);

		traceTeamInfo();
	}

	//MX: Print out all teams' information: each server's info in each team
	void traceTeamInfo(){
		printf("MX: Trace Team Info.\n");
		TraceEvent("TraceTeamInfoStart", masterId).detail("TeamsNum", teams.size());
		traceTeamInfo1(teams);
		traceTeamInfo2(teams);
		traceTeamInfo3(teams);
	}


	void traceTeamInfo1(vector<Reference<TCTeamInfo>> &myteams) {
		for (int i=0;i<myteams.size();i++) {
			const vector<UID> &serverIDs = myteams[i]->getServerIDs();
			for (auto sid = serverIDs.begin(); sid != serverIDs.end(); ++sid) {
				ServerStatus ss = server_status.get(*sid);
				TraceEvent("TeamInfo1", masterId).detail("MX", 1)
						.detail("TeamID", i).detail("ServerStatus", ss.toString())
						.detail("Locality", ss.locality.toString());
			}
		}
	}


	//Similar to traceTeamInfo(), but we print out the info with different function
	void traceTeamInfo2(vector<Reference<TCTeamInfo>> &myteams) {
		for (int i=0;i<myteams.size();i++) {
			const vector<UID> &serverIDs = myteams[i]->getServerIDs();
			for (auto sid = serverIDs.begin(); sid != serverIDs.end(); ++sid) {
				ServerStatus ss = server_status.get(*sid);
				TraceEvent("TeamInfo2", masterId).detail("MX", 1)
						.detail("TeamID", i)
						.detail("ServerStatus", ss.toString())
						.detail("ZoneID", ss.locality.describeZone())
						.detail("DcID", ss.locality.describeDcId())
						.detail("DataHallID", ss.locality.describeDataHall())
						.detail("MachineID", ss.locality.describeMachineId())
						.detail("UIDmachineId", ss.locality.describeValue(LiteralStringRef("machineid")))
						.detail("UIDzoneId", ss.locality.describeValue(LiteralStringRef("zoneid")))
						.detail("UIDdataHallId", ss.locality.describeValue(LiteralStringRef("data_hall")))
						.detail("ProcessID", ss.locality.describeProcessId());
			}
		}
	}

	void traceTeamInfo(Reference<TCTeamInfo> &team, std::string prefix="") {
		const vector<UID> &serverIDs = team->getServerIDs();
		int member_id = 0;
		for (auto sid = serverIDs.begin(); sid != serverIDs.end(); ++sid, ++member_id) {
			LocalityData localityData = server_info[*sid]->lastKnownInterface.locality;
			std::string process_id =  localityData.get(LiteralStringRef("processid")).get().toString();
			std::string machine_id = localityData.get(LiteralStringRef("machineid")).get().toString();
			std::string zone_id = localityData.get(LiteralStringRef("zoneid")).get().toString();
			std::string data_hall_id = localityData.get(LiteralStringRef("data_hall")).get().toString();
			std::string dc_id = localityData.get(LiteralStringRef("dcid")).get().toString();


			ServerStatus ss = server_status.get(*sid);
			std::string ip = server_info[*sid]->lastKnownInterface.address().toString();

			printf("%s MemberID:%d, TeamMemberUID:%s, ServerStatus:%s, ProcessId:%s, MachineId:%s, ZoneId:%s, HallId:%s, DcId:%s, IP:%s\n",
					prefix.c_str(), member_id, sid->toString().c_str(), ss.toString(),
					process_id.c_str(), machine_id.c_str(), zone_id.c_str(), data_hall_id.c_str(), dc_id.c_str(), ip.c_str());

			TraceEvent("TeamInfo", masterId).detail("MX", 1)
					.detail("TeamMemberID", member_id)
					.detail("TeamMemberUID", *sid)
					.detail("ServerStatus", ss.toString())
					.detail("ProcessId", process_id)
					.detail("MachineId", machine_id)
					.detail("ZoneId", zone_id)
					.detail("DataDallId", data_hall_id)
					.detail("DcId", dc_id)
					.detail("IP", ip);

			member_id++;
		}
	}

	void traceTeamInfo3(vector<Reference<TCTeamInfo>> &myteams) {
		Reference<LocalityMap<UID>> fromServers(new LocalityMap<UID>());
		this->getProcesses(fromServers);
		for (int i=0;i<myteams.size();i++) {
			TraceEvent("TeamInfo3", masterId).detail("MX", 1)
					.detail("TeamID", i);
			const vector<UID> &serverIDs = myteams[i]->getServerIDs();
			int member_id = 0;
			for (auto sid = serverIDs.begin(); sid != serverIDs.end(); ++sid, ++member_id) {
				LocalityData localityData = server_info[*sid]->lastKnownInterface.locality;
				std::string process_id =  localityData.get(LiteralStringRef("processid")).get().toString();
				std::string machine_id = localityData.get(LiteralStringRef("machineid")).get().toString();
				std::string zone_id = localityData.get(LiteralStringRef("zoneid")).get().toString();
				std::string data_hall_id = localityData.get(LiteralStringRef("data_hall")).get().toString();
				std::string dc_id = localityData.get(LiteralStringRef("dcid")).get().toString();


				ServerStatus ss = server_status.get(*sid);

				TraceEvent("TeamInfo3", masterId).detail("MX", 1)
						.detail("TeamID\t", i)
						.detail("TeamMemberID", member_id++)
						.detail("TeamMemberUID", *sid)
						.detail("ServerStatus", ss.toString())
						.detail("ProcessId", process_id)
						.detail("MachineId", machine_id)
						.detail("ZoneId", zone_id)
						.detail("DataDallId", data_hall_id)
						.detail("DcId", dc_id)
						.detail("IP", server_info[*sid]->lastKnownInterface.address().toString());
			}
		}
	}

	//MX: Can use this as a reference to print out the current teams' info.
	bool teamExists( vector<UID> &team ) {
		bool exists = false;
		for (int i=0;i<teams.size();i++){
			if (teams[i]->getServerIDs() == team) {
				exists = true;
				break;
			}
		}
		return exists;
	}

	//input team is the process team used to create the machine team
	bool machineTeamExists( vector< Standalone<StringRef> > &machineIDs ) {
		bool exists = false;

		for (int i = 0; i < machineTeams.size(); i++ ) {
			//vector< Standalone<StringRef> > curMachineIDs = machineTeams[i]->machineIDs;
			std::sort(machineTeams[i]->machineIDs.begin(), machineTeams[i]->machineIDs.end());
			if ( machineTeams[i]->machineIDs == machineIDs ) {
				exists = true;
				break;
			}
		}

		return exists;
	}

	void addTeam( std::set<UID> const& team ) {
		addTeam(team.begin(), team.end());
	}

	template<class InputIt>
	void addTeam( InputIt begin, InputIt end) {
		vector< Reference<TCServerInfo> > newTeamServers;

		for (auto i = begin; i != end; ++i) {
			if (server_info.find(*i) != server_info.end()) {
				newTeamServers.push_back(server_info[*i]);
			}
		}

		Reference<TCTeamInfo> teamInfo( new TCTeamInfo( newTeamServers ) );
		TraceEvent("TeamCreation", masterId).detail("Team", teamInfo->getDesc());
		printf("MX: addTeam(): validate the to-be-added team in teamTracker. Team info:%s\n", teamInfo->getDesc().c_str());
		teamInfo->tracker = teamTracker( this, teamInfo );
		teams.push_back( teamInfo );
		for (int i=0;i<newTeamServers.size();i++) {
			server_info[ newTeamServers[i]->id ]->teams.push_back( teamInfo );
		}
	}

	/**
	 * @tparam InputIt: iterator type
	 * @param begin is typically the start of the iterator
	 * @param end is typically the end of the iterator
	 */
	template<class InputIt>
	void addMachineTeam( InputIt begin, InputIt end) {
		vector< Reference<TCMachineInfo> > machines;

		for (auto i = begin; i != end; ++i) {
			if (machine_info.find(*i) != machine_info.end()) {
				machines.push_back(machine_info[*i]);
			} else {
				fprintf(stderr, "WARNING: machine_id:%s does not exit\n", i->contents().toString().c_str());
			}
		}

		Reference<TCMachineTeamInfo> machineTeamInfo( new TCMachineTeamInfo( machines ) );
		TraceEvent("AddMachineTeam", masterId).detail("MachineIDs", machineTeamInfo->getMachineIDsStr());
		printf("[DEBUG] AddMachineTeam, MachineIDs:%s\n", machineTeamInfo->getMachineIDsStr().c_str());
		//teamInfo->tracker = teamTracker( this, teamInfo ); //No team tracker for machine teams
		machineTeams.push_back(machineTeamInfo);
		// Assign machine teams to machine
		for ( auto machine: machines ) {
			machine->machineTeams.push_back(machineTeamInfo);
		}
	}


	//MX: Enumerate all possible teams by backtracing. Add a team, if it's valid, into the teamCollection
	//MX: From Meng's understanding, this function could be very slow! It is exponential complexity
	ACTOR Future<Void> addAllTeams( DDTeamCollection *self, int location, vector<LocalityEntry>* history, Reference<LocalityMap<UID>> processes, vector<std::vector<UID>>* output, int teamLimit, int* addedTeams ) {
		Void _ = wait( yield( TaskDataDistributionLaunch ) );

		// Add team, if valid
		if(history->size() == self->configuration.storageTeamSize) {
			//self->configuration.storagePolicy->traceLocalityRecords(processes);//MX: print out the locality records, which are used in validate the new team
			auto valid = self->configuration.storagePolicy->validate(*history, processes); //MX: TODO: Maybe very slow!
			if(!valid) {
				return Void();
			}
			std::vector<UID> team;
			for(auto it = history->begin(); it != history->end(); it++) {
				team.push_back(*processes->getObject(*it));
			}

			if( !self->teamExists(team) && *addedTeams < teamLimit ) {
				output->push_back(team);
				(*addedTeams)++;
			}
			return Void();
		}

		//loop through remaining potential team members, add one and recursively call function
		for(; location < processes->size(); location++) {
			history->push_back(processes->getEntry(location));
			state int depth = history->size();
			Void _ = wait( self->addAllTeams( self, location + 1, history, processes, output, teamLimit, addedTeams ) );
			ASSERT( history->size() == depth); // the "stack" should be unchanged by this call
			history->pop_back();
			if(*addedTeams > teamLimit)
				break;
		}

		return Void();
	}

	ACTOR Future<int> addAllTeams( DDTeamCollection *self, vector<UID> input, vector<std::vector<UID>>* output, int teamLimit ) {
		state int addedTeams = 0;
		state vector<LocalityEntry> history;
		state Reference<LocalityMap<UID>> processes(new LocalityMap<UID>());
		for(auto it = input.begin(); it != input.end(); it++) {
			if(self->server_info[*it]) {
				processes->add(self->server_info[*it]->lastKnownInterface.locality, &*it);
			}
		}
		Void _ = wait( self->addAllTeams( self, 0, &history, processes, output, teamLimit, &addedTeams ) );
		self->configuration.storagePolicy->traceLocalityRecords(processes);//MX: print out the locality records, which are used in validate the new team
		return addedTeams;
	}

	void getProcesses(Reference<LocalityMap<UID>> & processes) {

		for ( auto it = this->server_info.begin(); it != this->server_info.end(); it++ ) {
			processes->add(it->second->lastKnownInterface.locality, &it->first);
		}
		return;
	}

	/**
	 * Group storage servers (process) based on their machineId in LocalityData
	 * @param totalServers is the set of servers to be grouped
	 * @return The number of healthy servers we grouped into machines
	 */
	int constructMachinesFromServers(LocalityMap<UID> &totalServers) {
		//Collect machines for healthy servers
		int totalServerIndex = 0;
		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				auto &id = i->first; //UID
				auto &locality = i->second->lastKnownInterface.locality;
				Standalone<StringRef> machine_id = locality.machineId().get(); //locality to machine_id with std::string type
				LocalityEntry localityEntry = totalServers.add(locality, &id); //for each machine, store the first localityEntry into machineInfo for later use.

				if (machine_info.find(machine_id) == machine_info.end()) { //first storage server process on the machine
					Reference<TCMachineInfo> machineInfo = Reference<TCMachineInfo>(new TCMachineInfo(i->second, localityEntry)); //insert i->second in the constructor
					i->second->machine = machineInfo;
					machine_info.insert(std::make_pair(machine_id, machineInfo));
				} else {
					Reference<TCMachineInfo> machineInfo = machine_info.find(machine_id)->second;
					machineInfo->serversOnMachine.push_back(i->second);
					i->second->machine = machineInfo;
				}

				printf("[DEBUG] [%d] server:%s isMachineValid:%d\n", totalServerIndex,
					   i->first.toString().c_str(), i->second->machine.isValid());
				if ( i->second->machine.isValid() ) {
					printf("\t[DEBUG] machineID:%s  current number of servers on machine:%d\n",
						   i->second->machine->machineID.contents().toString().c_str(), i->second->machine->serversOnMachine.size());
				}

				//add into totalServers
				totalServerIndex++;
			}
		}
		printf("addBestMachineTeams(): total healthy servers:%d\n", totalServerIndex);

		return totalServerIndex;
	}


	/**
	 * Create machineTeamsToBuild number of machine teams
	 * Step 1: Create machineInfo by grouping servers (i.e., processes) using server's machineId tag in LocalityData
	 * Step 2: Pick the set of least machine teams, one of which will be used to form server teams
	 * Step 3: Pick one server in each machineInfo and create the LocalityEntry vector used by selectReplicas()
	 * Step 4: Reuse the selectReplicas() in Policy to pick the server team
	 * Step 5: Use the server team to construct the machine team, and update the machineInfo
	 * @return number of added machine teams
	 */
	int addBestMachineTeams() {
		//TODO: modify the following content for the function
		int addedMachineTeams = 0;
		LocalityMap<UID> totalServers; //totalServers;
		int totalServerIndex = 0;
		int machineTeamsToBuild = 0;

		// Step 1: Create machine by grouping servers
		totalServerIndex = constructMachinesFromServers(totalServers);
		machineTeamsToBuild = machine_info.size() * SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER; // must after constructMachinesFromServers();
		machineTeamsToBuild = 2; //TODO: Test! Set machine team number = 2

		TraceEvent("AddAllMachineTeams")
				.detail("MachineTeamsToBuild", machineTeamsToBuild)
				.detail("CurrentTotalMachines",machine_info.size());

		if(machine_info.size() < configuration.storageTeamSize ) {
			TraceEvent(SevWarn, "DataDistributionBuildMachineTeams", masterId)
				.detail("Reason","Not enough machines for a team. Machine number should > Team size")
				.detail("MachineNumber",machine_info.size()).detail("TeamSize", configuration.storageTeamSize);
			return addedMachineTeams;
		}

		int loopCount = 0;
		// Add team in each iteration
		printf("addBestMachineTeams: start adding machine teams...\n");
		while( addedMachineTeams < machineTeamsToBuild ) {
			printf("addBestMachineTeams: add machine team:%d\n", addedMachineTeams);
			//Step 2: Get least used machines to be used
			std::vector<Reference<TCMachineInfo>> leastUsedMachines; //A less used machine has less number of teams
			int minTeamCount = CLIENT_KNOBS->TOO_MANY;
			for ( auto &machine : machine_info ) {
				int teamCount = machine.second->getTotalMachineTeamCount();
				if(teamCount < minTeamCount) {
					leastUsedMachines.clear();
					minTeamCount = teamCount;
				}
				if(teamCount <= minTeamCount) {
					leastUsedMachines.push_back(machine.second);
				}
			}

			TraceEvent("AddAllMachineTeams").detail("LeastUsedMachineNumber", leastUsedMachines.size());

			std::vector<UID*> team;
			std::vector<LocalityEntry> forcedAttributes;

			// Step 3: Create a representative process for each machine.
			//Construct forcedAttribute from leastUsedMachines. We will use forcedAttribute to call existing function to form a team
			if (leastUsedMachines.size()) {
				// Randomly choose 1 least used machine
				Reference<TCMachineInfo> tcMachineInfo = g_random->randomChoice(leastUsedMachines);
				if ( tcMachineInfo->serversOnMachine.size() == 0 ) {
					fprintf(stderr, "leastUsedMachinesNumber:%d, serverNumber on chosenMachine:%d, retry...\n",
							leastUsedMachines.size(), tcMachineInfo->serversOnMachine.size() );
					continue;
				}
				Reference<TCServerInfo> tcServerInfo = *tcMachineInfo->serversOnMachine.begin(); // Use the first server as the representative of the machine
				LocalityEntry process = tcMachineInfo->localityEntry;
				forcedAttributes.push_back(process);
			}

			// Step 4: Reuse Policy's selectReplicas() to create team for the representative process.
			printf("AddAllMachineTeams: create a process team by using selectReplicas()\n");
			std::vector<UID*> bestTeam;
			int bestScore = std::numeric_limits<int>::max();
			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT;// BEST_OF_AMT = 4
			for( int i = 0; i < maxAttempts && i < 100; i++) {
				// Choose a team that balances the # of teams per server, among the teams that have the least-utilized server
				team.clear();
				//MX: We first choose a server with least utilization; we then choose a team that must include the least used server
				//MX: This is why the load balancing is not working as mentioned by Evan: The team members added by selectReplicas() may be overloaded.
				//MX: Choose the rest of the team members based on the randomly picked team member (forcedAtrributes[0])
				auto success = totalServers.selectReplicas(configuration.storagePolicy, forcedAttributes, team);
				if(!success) {
					break;//MX: re-select the forcedAtrributes server
				}

				if(forcedAttributes.size() > 0) {
					team.push_back((UID*)totalServers.getObject(forcedAttributes[0]));
				}
				if( team.size() != configuration.storageTeamSize) { //MX:Q: why will this happen? If this happens, it means selectReplicas() did not choose a correct team in the first place!
					maxAttempts += 1;
				}

				int score = 0;
				for(auto process = team.begin(); process != team.end(); process++) {
					score += server_info[**process]->teams.size();
					TraceEvent("AddMachineTeamsBestOf").detail("AddedTeams", addedMachineTeams)
						.detail("Attempt", i).detail("ProcessIP", server_info[**process]->lastKnownInterface.address());

				}

				printf("AddBestMachineTeams: representative process team score: %d at attempt id: %d\n", score, i);

				if(score < bestScore) {
					bestTeam = team;//MX:bestTeam is the team, which has the smallest number of teams its team members belong to.
					bestScore = score;
				}
			}
			printf("AddBestMachineTeams: created representative process team score:%d, team size:%d, addedMachineTeams:%d\n",
					bestScore, bestTeam.size(), addedMachineTeams);

			//Step 5: Restore machine from its representative process team and get the machine team
			if( bestTeam.size() == configuration.storageTeamSize) {
				//vector<UID> processIDs;
				vector<Standalone<StringRef>> machineIDs;

				for (auto process = bestTeam.begin(); process < bestTeam.end(); process++) {
					Standalone<StringRef> machine_id  = server_info[**process]->lastKnownInterface.locality.machineId().get();
					machineIDs.push_back(machine_id);
				}

				std::sort(machineIDs.begin(), machineIDs.end());
				
				if( !machineTeamExists( machineIDs ) ) {
					addMachineTeam(machineIDs.begin(), machineIDs.end());
					addedMachineTeams++;
				}
			}
			else {
				TraceEvent(SevWarn, "DataDistributionBuildTeams", masterId).detail("Reason","Unable to make desiredTeams");
				break;
			}
			if(++loopCount > 2*machineTeamsToBuild*(configuration.storageTeamSize+1) ) { //TODO: Q: why is this number?
				break;
			}
		}
		printf("addBestMachineTeams: finish adding %d machine teams...\n", addedMachineTeams);
		return addedMachineTeams;
	}

	void sanityCheckServersMachine() {
		int i = 0;
		for (auto server: server_info) {
			if (server_status.get(server.first).isUnhealthy()) {
				printf("[DEBUG] server:%s is unhealthy\n", server.first.toString().c_str());
			}
			if ( !server.second->machine.isValid() ) {
				printf("[DEBUG] server:%s belongs to invalid machine\n", server.second->id);
			}
			++i;
		}
		printf("[DEBUG] checked %d servers' machine status\n", i);
	}

	/**
	 * @return a set of least used servers from all servers on the machines that belong to a machine team
	 */
	std::vector<UID> findLeastUsedServersOnMachineTeams() {
		std::vector<UID> leastUsedServers;
		int minTeamCount = CLIENT_KNOBS->TOO_MANY;
		for (auto server: server_info) {
			if ( server_status.get(server.first).isUnhealthy() )
				continue;
			if ( !server.second->machine.isValid() )
				continue;
			if ( server.second->machine->machineTeams.size() <= 0 )
				continue;
			int teamCount = server.second->teams.size();
			if (teamCount < minTeamCount) {
				leastUsedServers.clear();
				minTeamCount = teamCount;
			}
			if (teamCount <= minTeamCount) {
				leastUsedServers.push_back(server.second->id);
			}
		}
		return leastUsedServers;
	}

	Reference<TCMachineTeamInfo> findLeastUsedMachineTeams(std::vector< Reference<TCMachineTeamInfo> > &machineTeams) {
		int minMachineTeamCount = CLIENT_KNOBS->TOO_MANY;
		int curMachineTeamCount = 0;
		Reference<TCMachineTeamInfo> leastUsedMachineTeam;
		for ( auto machineTeam: machineTeams ) {
			curMachineTeamCount = machineTeam->getTotalMachineTeamNumber();
			if ( curMachineTeamCount < minMachineTeamCount ) {
				minMachineTeamCount = curMachineTeamCount;
				leastUsedMachineTeam = machineTeam;
			}
		}

		return leastUsedMachineTeam;
	}

	/*
	 * Use machine of each server in team to create a machine team
	 * Check if the machine team exists.
	 * If the machine team exists, it must be one of the machine teams its machine member belongs to
	 */
	bool isOnSameMachineTeam(Reference<TCTeamInfo> &team) {
		std::vector< Standalone<StringRef> > machineIDs;
		for ( auto &server: team->servers ) {
			if ( !server->machine.isValid() )
				return false;
			machineIDs.push_back(server->machine->machineID);
		}
		std::sort(machineIDs.begin(), machineIDs.end());

		int numExistance = 0;
		for ( auto server: team->servers ) {
			for ( auto &candidateMachineTeam: server->machine->machineTeams ) {
				std::sort(candidateMachineTeam->machineIDs.begin(), candidateMachineTeam->machineIDs.end());
				if ( machineIDs == candidateMachineTeam->machineIDs ) { //the server is chosen from the machineTeam
					numExistance++;
					break;
				}
			}
		}
		if ( numExistance == team->servers.size() )
			return true;
		else
			return false;
	}

	/**
	 * Sanity check the property of teams and print out teams' info
	 * @return number of teams
	 */
	bool sanityCheckTeams() {
		int teamIndex = 0;
		int alwaysOnSameMachineTeam = true;
		for ( auto &team: teams ) {
			//Reference<TCTeamInfo>;
			bool onSameMachineTeam = isOnSameMachineTeam(team);
			printf("[INFO] Team:%d Num of members:%d onSameMachineTeam:%d\n",
					teamIndex, team->servers.size(), onSameMachineTeam);
			if ( onSameMachineTeam == false )
				alwaysOnSameMachineTeam = false;

			int memberIndex = 0;
			for ( auto &server: team->servers ) {
				printf("\t[INFO] Member:%d Server UID:%s zoneID:%s machine_id:%s\n",
					   memberIndex, server->id.toString().c_str(), server->lastKnownInterface.locality.describeZone().c_str(),
					   server->machine.isValid() ? server->machine->machineID.contents().toString().c_str(): "[unset]");
				memberIndex++;
			}
			traceTeamInfo(team, "\t[INFO_VERBOSE]");
			teamIndex++;
		}

		return alwaysOnSameMachineTeam;
	}

	/**
	 * Create server teams based on machine teams
	 * Step 1: Create best machine teams from least used machines
	 * Step 2: Find the least used servers from the best machine teams and randomly pick one
	 * Step 3: Find the least used machine team the picked server belong to
	 * Step 4: Randomly pick 1 server from each machine in the machine team into the server team
	 * Step 5: Step 4: Add the server team after sanity check
	 */
	int addTeamsBestOf( int teamsToBuild) {
		int addedMachineTeams = 0;
		assert(teamsToBuild > 0);

		//Step 1: Create beast machine teams
		addedMachineTeams = addBestMachineTeams(); //Compute the number of machine teams based on the server teams to build
		TraceEvent("AddTeamsBestOf").detail("AddMachineTeamsNumber", addedMachineTeams);
		printf("addTeamsBestOf: finishing add %d machine teams, start build process teams: teamsToBuild:%d\n", addedMachineTeams, teamsToBuild);

		LocalityMap<UID> totalServers;
		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				auto& id = i->first;
				auto& locality = i->second->lastKnownInterface.locality;
				totalServers.add(locality, &id);
			}
		}

		this->server_localityMap = Reference<LocalityMap<UID>> (new LocalityMap<UID>(totalServers)); //hack to locate locality info in sanity check function

		printf("Sanity check server's machine status after machine team is built\n");
		sanityCheckServersMachine();

		int addedTeams = 0;
		TraceEvent("AddTeamsBestOf")
				.detail("TeamsToBuild", teamsToBuild)
				.detail("TotalRawServerNumber", server_info.size())
				.detail("TotalHealthyServerNumber", totalServers.size())
				.detail("TotalMachineNumber", machine_info.size())
				.detail("MachineTeamNumber", machineTeams.size());

		int loopCount = 0;
		while( addedTeams < teamsToBuild ) {
			//Step 2: Find the least used servers and randomly pick one.
			std::vector<UID> leastUsedServers = findLeastUsedServersOnMachineTeams(); //It's possible that not all machines are chosen into machine teams
			if ( leastUsedServers.empty() ) {
				fprintf(stderr, "[ERROR] no server is found on machines that belong to a machien team.\n"
					"\tMaybe we should build more machine teams?");
				break;
			}
			UID chosenServerID = g_random->randomChoice(leastUsedServers); //randomChoice input must be > 0

			printf("Sanity check server's machine status when %d process teams was built\n", addedTeams);
			sanityCheckServersMachine();

			if ( server_info.find(chosenServerID) == server_info.end() ) {
				fprintf(stderr, "AddTeamsBestOf() leastUsedServersSize:%d, chosenServerID:%s, addedTeams:%d, teamsToBuild\n",
						leastUsedServers.size(), chosenServerID.toString().c_str(), addedTeams, teamsToBuild);
				break;
			}
			if ( server_status.get(chosenServerID).isUnhealthy() ) {
				fprintf(stderr, "AddTeamsBestOf() chosenServerID:%s becomes unhealthy. retry...\n",
						chosenServerID.toString().c_str());
				continue;
			}

			//Step 3: Find the least used machine team the picked server belong to
			Reference<TCMachineInfo> chosenMachine = server_info[chosenServerID]->machine;
			//Sanity check chosen machine
			if ( !chosenMachine.isValid() ) {
				fprintf(stderr, "AddTeamsBestOf() server's machine is not correctly set! Found server but not its machine: "
					"leastUsedServersSize:%d, chosenServerID:%s, addedTeams:%d, teamsToBuild:%d\n",
					leastUsedServers.size(), chosenServerID.toString().c_str(), addedTeams, teamsToBuild);

				// Check the machine info
				LocalityData locality = server_info[chosenServerID]->lastKnownInterface.locality;
				Standalone<StringRef> machine_id = locality.machineId().get();
				if ( machine_info.find(machine_id) == machine_info.end() ) {
					fprintf(stderr, "machine_id:%s not exist in machine_info\n", machine_id.contents().toString().c_str());
				} else {
					Reference<TCMachineInfo> machine = machine_info[machine_id];
					for ( auto &server: machine->serversOnMachine ) {
						printf("server: %s on machine %s: is machine field valid: %d\n",
								server->id.toString().c_str(), machine->machineID.contents().toString().c_str(),
								server->machine.isValid());
					}
				}

				break;
			}

			Reference<TCMachineTeamInfo> chosenMachineTeam = findLeastUsedMachineTeams(chosenMachine->machineTeams);
			if ( !chosenMachineTeam.isValid() ) {
				fprintf(stderr, "[WARNING] chosen machine does not belong to any machine team! Retry...\n");
				continue;
			}

			TraceEvent("AddTeamsBestOf").detail("ChosenMachineTeamSize", chosenMachineTeam->machines.size())
				.detail("MachineIDs", chosenMachineTeam->getMachineIDsStr());

			//Step 4: Randomly pick 1 server from each machine in the machine team into the server team
			vector<UID> serverTeam;
			for ( auto machine: chosenMachineTeam->machines ) {
				std::vector<UID> leastUsedServers;
				machine->findLeastUsedServers(leastUsedServers);
				UID chosenServer = g_random->randomChoice(leastUsedServers);
				serverTeam.push_back(chosenServer);
			}

			//Step 5: Add the server team after sanity check
			if( serverTeam.size() != configuration.storageTeamSize) {
				TraceEvent(SevWarn, "DataDistributionBuildTeams", masterId).detail("Reason","Unable to make desiredTeams");
				break;
			}
			std::sort(serverTeam.begin(), serverTeam.end());

			if( !teamExists( serverTeam ) ) {
				addTeam(serverTeam.begin(), serverTeam.end());
				addedTeams++;
			}

			if(++loopCount > 2*teamsToBuild*(configuration.storageTeamSize+1) ) {
				fprintf(stderr, "AddTeamsBestOf() loopCount:%d > 2*teamsToBuild*(configuration.storageTeamSize+1):%d\n",
						loopCount, 2*teamsToBuild*(configuration.storageTeamSize+1));
				break;
			}

		}
		return addedTeams;
	}

	//MX: Add new teams! The algorithm to create team may go here!
	//MX: The function can be very inefficient. It may fail to find a team although the team may exist?
	int addTeamsBestOf_old( int teamsToBuild ) {
		int addedTeams = 0;

		LocalityMap<UID> totalServers;

		TraceEvent("AddTeamsBestOf")
			.detail("TeamsToBuild", teamsToBuild)
		    .detail("TotalServers", totalServers.size());

		//server is stoarge server process! Multiple server  processes may belong to the same machine
		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUnhealthy()) {
				auto& id = i->first;
				auto& locality = i->second->lastKnownInterface.locality;
				totalServers.add(locality, &id);
			}
		}

		if(totalServers.size() < configuration.storageTeamSize ) {
			TraceEvent(SevWarn, "DataDistributionBuildTeams", masterId).detail("Reason","Not enough servers for a team").detail("Servers",totalServers.size()).detail("TeamSize", configuration.storageTeamSize);
			return addedTeams;
		}

		int loopCount = 0;
		// add teams
		while( addedTeams < teamsToBuild ) {//MX: Add one team per loop; In each iteration, it enumerates all servers and teams (maybe more than 1 time)
			std::vector<LocalityEntry> leastUsedServers;
			int minTeamCount = CLIENT_KNOBS->TOO_MANY;
			//MX: If the totalServers is sorted in decreasing order of teamCount, we will add all servers as leastUsedServers.
			//MX: This may be a possible performance issue, affecting the speed of algorithm later?
			for(int i = 0; i < totalServers.size(); i++) {
				LocalityEntry process = totalServers.getEntry(i);
				UID id = *totalServers.getObject(process); //localityEntry.id is where UID is inserted in the objectArray
				int teamCount = server_info[id]->teams.size();
				if(teamCount < minTeamCount) {
					leastUsedServers.clear();
					minTeamCount = teamCount;
				}
				if(teamCount <= minTeamCount) {
					leastUsedServers.push_back(process);
				}
			}

			std::vector<UID*> team;
			std::vector<LocalityEntry> forcedAttributes;

			if (leastUsedServers.size()) {
				//MX: randomly choose 1 least used process (server) as the member of the to-be-built team
				forcedAttributes.push_back(g_random->randomChoice(leastUsedServers));//MX: push_back one randomly chosen server from leastUsedServers
			}

			std::vector<UID*> bestTeam;
			int bestScore = CLIENT_KNOBS->TOO_MANY;

			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT;//MX: BEST_OF_AMT = 4
			for( int i = 0; i < maxAttempts && i < 100; i++) {//MX: Choose a team that balances the # of teams per machine, among the teams that have the least-utilized server
				team.clear();
				//MX: We first choose a server with least utilization; we then choose a team that must include the least used server
				//MX: This is why the load balancing is not working as mentioned by Evan: The team members added by selectReplicas() may be overloaded.
				//MX: Choose the rest of the team members based on the randomly picked team member (forcedAtrributes[0])
				auto success = totalServers.selectReplicas(configuration.storagePolicy, forcedAttributes, team);
				if(!success) {
					break;//MX: re-select the forcedAtrributes server
				}

				if(forcedAttributes.size() > 0) {
					team.push_back((UID*)totalServers.getObject(forcedAttributes[0]));
				}
				if( team.size() != configuration.storageTeamSize) { //MX:Q: why will this happen? If this happens, it means selectReplicas() did not choose a correct team in the first place!
					maxAttempts += 1;
				}

				int score = 0;
				for(auto process = team.begin(); process != team.end(); process++) {
					score += server_info[**process]->teams.size();
					TraceEvent("AddTeamsBestOf").detail("AddedTeams", addedTeams).detail("Attempt", i).detail("ProcessIP", server_info[**process]->lastKnownInterface.address());

				}

				printf("MX: team score: %d at attempt id: %d\n", score, i);

				if(score < bestScore) {
					bestTeam = team;//MX:bestTeam is the team, which has the smallest number of teams its team members belong to.
					bestScore = score;
				}
			}

			printf("MX: best team score:%d\n", bestScore);
			if( bestTeam.size() == configuration.storageTeamSize) {
				vector<UID> processIDs;

				for (auto process = bestTeam.begin(); process < bestTeam.end(); process++) {
					processIDs.push_back(**process);
				}

				std::sort(processIDs.begin(), processIDs.end());

				if( !teamExists( processIDs ) ) {
					addTeam(processIDs.begin(), processIDs.end());
					addedTeams++;
				}
			}
			else {
				TraceEvent(SevWarn, "DataDistributionBuildTeams", masterId).detail("Reason","Unable to make desiredTeams");
				break;
			}
			if(++loopCount > 2*teamsToBuild*(configuration.storageTeamSize+1) ) {
				break;
			}
		}
		return addedTeams;
	}

	// Use the current set of known processes (from server_info) to compute an optimized set of storage server teams.
	// The following are guarantees of the process:
	//   - Each newly-built team will meet the replication policy
	//   - All newly-built teams will have exactly teamSize machines
	//
	// buildTeams() only ever adds teams to the list of teams. Teams are only removed from the list when all data has been removed.
	//
	// buildTeams will not count teams larger than teamSize against the desired teams.
	ACTOR Future<Void> buildTeams( DDTeamCollection* self ) {//MX: Key function to change
		state int desiredTeams;
		int serverCount = 0;
		int uniqueDataCenters = 0;
		int uniqueMachines = 0;
		std::set<Optional<Standalone<StringRef>>> machines;

		for(auto i = self->server_info.begin(); i != self->server_info.end(); ++i) {
			if (!self->server_status.get(i->first).isUnhealthy()) {
				++serverCount;
				LocalityData& serverLocation = i->second->lastKnownInterface.locality;
				machines.insert( serverLocation.zoneId() );
			}
		}
		uniqueMachines = machines.size();

		// If there are too few machines to even build teams or there are too few represented datacenters, build no new teams
		if( uniqueMachines >= self->configuration.storageTeamSize ) {
			desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER*serverCount;//MX: each server is assigned to 5 teams by default empirically.
			int maxTeams = SERVER_KNOBS->MAX_TEAMS_PER_SERVER*serverCount;

			// Count only properly sized teams against the desired number of teams. This is to prevent "emergency" merged teams (see MoveKeys)
			//  from overwhelming the team count (since we really did not want that team in the first place). These larger teams will not be
			//  returned from getRandomTeam() (as used by bestTeam to find a new home for a shard).
			// Also exclude teams who have members in the wrong configuration, since we don't want these teams either
			int teamCount = 0;
			int totalTeamCount = 0;
			for(int i = 0; i < self->teams.size(); i++) {
				if( self->teams[i]->getServerIDs().size() == self->configuration.storageTeamSize && !self->teams[i]->isWrongConfiguration() ) {
					if( self->teams[i]->isHealthy() ) {
						teamCount++;
					}
					totalTeamCount++;
				}
			}

			TraceEvent("BuildTeamsBegin", self->masterId).detail("DesiredTeams", desiredTeams).detail("MaxTeams", maxTeams)
				.detail("UniqueMachines", uniqueMachines).detail("TeamSize", self->configuration.storageTeamSize).detail("Servers", serverCount)
				.detail("CurrentTrackedTeams", self->teams.size()).detail("HealthyTeamCount", teamCount).detail("TotalTeamCount", totalTeamCount);

			TraceEvent("BuildTeamsBegin", self->masterId).detail("MX", 1).detail("StorageTeamSize", self->configuration.storageTeamSize);

			teamCount = std::max(teamCount, desiredTeams + totalTeamCount - maxTeams );//MX:Q:why use max value here?

			if( desiredTeams > teamCount ) {
				std::set<UID> desiredServerSet;
				for(auto i = self->server_info.begin(); i != self->server_info.end(); ++i)
					if (!self->server_status.get(i->first).isUnhealthy())
						desiredServerSet.insert(i->second->id);

				vector<UID> desiredServerVector( desiredServerSet.begin(), desiredServerSet.end() );

				state int teamsToBuild = desiredTeams - teamCount;

				state vector<std::vector<UID>> builtTeams;

				if( self->configuration.storageTeamSize > 3) {
					int addedTeams = self->addTeamsBestOf( teamsToBuild );
					TraceEvent("AddTeamsBestOf", self->masterId).detail("CurrentTeams", self->teams.size()).detail("AddedTeams", addedTeams);
				} else {
					int addedTeams = wait( self->addAllTeams( self, desiredServerVector, &builtTeams, teamsToBuild ) );

					if( addedTeams < teamsToBuild ) {
						for( int i = 0; i < builtTeams.size(); i++ ) {
							std::sort(builtTeams[i].begin(), builtTeams[i].end());
							self->addTeam( builtTeams[i].begin(), builtTeams[i].end() );
						}
						TraceEvent("AddAllTeams", self->masterId).detail("CurrentTeams", self->teams.size()).detail("AddedTeams", builtTeams.size());
					}
					else {
						int addedTeams = self->addTeamsBestOf( teamsToBuild );
						TraceEvent("AddTeamsBestOf", self->masterId).detail("CurrentTeams", self->teams.size()).detail("AddedTeams", addedTeams);
					}
				}
			}
		}

		self->evaluateTeamQuality();//MX:Print out the built teams. Use the team info to understand the existing code

		//Building teams can cause servers to become undesired, which can make teams unhealthy.
		//Let all of these changes get worked out before responding to the get team request
		Void _ = wait( delay(0, TaskDataDistributionLaunch) );

		return Void();
	}

	void noHealthyTeams() {
		std::set<UID> desiredServerSet;
		std::string desc;
		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			ASSERT(i->first == i->second->id);
			if (!server_status.get(i->first).isFailed) {
				desiredServerSet.insert(i->first);
				desc += i->first.shortString() + " (" + i->second->lastKnownInterface.toString() + "), ";
			}
		}
		vector<UID> desiredServerVector( desiredServerSet.begin(), desiredServerSet.end() );

		TraceEvent(SevWarn, "NoHealthyTeams", masterId)
			.detail("CurrentTeamCount", teams.size())
			.detail("ServerCount", server_info.size())
			.detail("NonFailedServerCount", desiredServerVector.size());
	}

	void countHealthyTeams() {
		int healthy = 0;
		for(auto it = teams.begin(); it != teams.end(); it++) {
			if( (*it)->isHealthy() ) {
				healthy++;
			}
		}
		TraceEvent(healthy == healthyTeamCount ? SevInfo : SevWarnAlways, "HealthyTeamCheck", masterId)
			.detail("ValidatedCount", healthy)
			.detail("ProvidedCount", healthyTeamCount);
	}

	bool shouldHandleServer(const StorageServerInterface &newServer) {
		return (includedDCs.empty() || std::find(includedDCs.begin(), includedDCs.end(), newServer.locality.dcId()) != includedDCs.end() || (otherTrackedDCs.present() && std::find(otherTrackedDCs.get().begin(), otherTrackedDCs.get().end(), newServer.locality.dcId()) == otherTrackedDCs.get().end()));
	}

	void addServer( StorageServerInterface newServer, ProcessClass processClass, Promise<Void> errorOut, Version addedVersion ) {
		if (!shouldHandleServer(newServer)) {
			return;
		}
		allServers.push_back( newServer.id() );

		TraceEvent("AddedStorageServer", masterId).detail("ServerID", newServer.id()).detail("ProcessClass", processClass.toString()).detail("WaitFailureToken", newServer.waitFailure.getEndpoint().token).detail("Address", newServer.waitFailure.getEndpoint().address);
		auto &r = server_info[newServer.id()] = Reference<TCServerInfo>( new TCServerInfo( newServer, processClass ) );
		r->tracker = storageServerTracker( this, cx, r.getPtr(), &server_status, lock, masterId, &server_info, serverChanges, errorOut, addedVersion );
		restartTeamBuilder.trigger();
	}

	void removeServer( UID removedServer ) {
		TraceEvent("RemovedStorageServer", masterId).detail("ServerID", removedServer);
		// ASSERT( !shardsAffectedByTeamFailure->getServersForTeam( t ) for all t in teams that contain removedServer )

		// Find all servers with which the removedServer shares teams
		std::set<UID> serversWithAjoiningTeams;
		auto& sharedTeams = server_info[ removedServer ]->teams;
		for( int i = 0; i < sharedTeams.size(); i++ ) {
			auto& teamIds = sharedTeams[i]->getServerIDs();
			serversWithAjoiningTeams.insert( teamIds.begin(), teamIds.end() );
		}
		serversWithAjoiningTeams.erase( removedServer );

		// For each server in a team with the removedServer, erase shared teams from the list of teams in that other server
		for( auto it = serversWithAjoiningTeams.begin(); it != serversWithAjoiningTeams.end(); ++it ) {
			auto& teams = server_info[ *it ]->teams;
			for( int t = 0; t < teams.size(); t++ ) {
				auto& serverIds = teams[t]->getServerIDs();
				if ( std::count( serverIds.begin(), serverIds.end(), removedServer ) ) {
					teams[t--] = teams.back();
					teams.pop_back();
				}
			}
		}

		// remove removedServer from allServers, server_info
		for(int s=0; s<allServers.size(); s++) {
			if (allServers[s] == removedServer) {
				allServers[s--] = allServers.back();
				allServers.pop_back();
			}
		}
		server_info.erase( removedServer );

		// remove all teams that contain removedServer
		// SOMEDAY: can we avoid walking through all teams, since we have an index of teams in which removedServer participated
		for(int t=0; t<teams.size(); t++) {
			if ( std::count( teams[t]->getServerIDs().begin(), teams[t]->getServerIDs().end(), removedServer ) ) {
				teams[t]->tracker.cancel();
				teams[t--] = teams.back();
				teams.pop_back();
			}
		}
		doBuildTeams = true;
		restartTeamBuilder.trigger();

		TraceEvent("DataDistributionTeamCollectionUpdate", masterId)
			.detail("Teams", teams.size())
			.detail("Servers", allServers.size());
	}
};

// Track a team and issue RelocateShards when the level of degradation changes
ACTOR Future<Void> teamTracker( DDTeamCollection *self, Reference<IDataDistributionTeam> team) {
	state int lastServersLeft = team->getServerIDs().size();
	state bool lastAnyUndesired = false;
	state bool wrongSize = team->getServerIDs().size() != self->configuration.storageTeamSize;
	state bool lastReady = self->initialFailureReactionDelay.isReady();
	state bool lastHealthy = team->isHealthy();
	state bool lastOptimal = team->isOptimal() && lastHealthy;
	state bool lastWrongConfiguration = team->isWrongConfiguration();

	if(lastHealthy) {
		self->healthyTeamCount++;
		self->zeroHealthyTeams->set(false);
	}

	if(lastOptimal) {
		self->optimalTeamCount++;
		self->zeroOptimalTeams.set(false);
	}

	state bool lastZeroHealthy = self->zeroHealthyTeams->get();

	Void _ = wait( yield() );
	TraceEvent("TeamTrackerStarting", self->masterId).detail("Reason", "Initial wait complete (sc)").detail("Team", team->getDesc());
	self->priority_teams[team->getPriority()]++;

	try {
		loop {
			TraceEvent("TeamHealthChangeDetected", self->masterId).detail("IsReady", self->initialFailureReactionDelay.isReady() );
			// Check if the number of degraded machines has changed
			state vector<Future<Void>> change;
			auto servers = team->getServerIDs();
			bool anyUndesired = false;
			bool anyWrongConfiguration = false;
			Reference<LocalityGroup> teamLocality(new LocalityGroup());

			for(auto s = servers.begin(); s != servers.end(); ++s) {
				change.push_back( self->server_status.onChange( *s ) );
				auto& status = self->server_status.get(*s);
				if (!status.isFailed)
					teamLocality->add( status.locality );
				if (status.isUndesired)
					anyUndesired = true;
				if (status.isWrongConfiguration)
					anyWrongConfiguration = true;
			}

			int serversLeft = teamLocality->size();
			bool matchesPolicy = self->configuration.storagePolicy->validate(teamLocality->getEntries(), teamLocality);

			if( !self->initialFailureReactionDelay.isReady() ) {
				change.push_back( self->initialFailureReactionDelay );
			}
			change.push_back( self->zeroHealthyTeams->onChange() );

			bool recheck = (lastReady != self->initialFailureReactionDelay.isReady() || (lastZeroHealthy && !self->zeroHealthyTeams->get())) && (!matchesPolicy || anyUndesired || team->getServerIDs().size() != self->configuration.storageTeamSize);
			lastReady = self->initialFailureReactionDelay.isReady();
			lastZeroHealthy = self->zeroHealthyTeams->get();

			if( serversLeft != lastServersLeft || anyUndesired != lastAnyUndesired || anyWrongConfiguration != lastWrongConfiguration || wrongSize || recheck ) {
				TraceEvent("TeamHealthChanged", self->masterId)
					.detail("Team", team->getDesc()).detail("ServersLeft", serversLeft)
					.detail("LastServersLeft", lastServersLeft).detail("ContainsUndesiredServer", anyUndesired)
					.detail("HealthyTeamsCount", self->healthyTeamCount).detail("IsWrongConfiguration", anyWrongConfiguration);

				bool healthy = matchesPolicy && !anyUndesired && team->getServerIDs().size() == self->configuration.storageTeamSize && team->getServerIDs().size() == serversLeft;
				team->setHealthy( healthy );	// Unhealthy teams won't be chosen by bestTeam

				team->setWrongConfiguration( anyWrongConfiguration );

				bool optimal = team->isOptimal() && healthy;
				if( optimal != lastOptimal ) {
					lastOptimal = optimal;
					self->optimalTeamCount += optimal ? 1 : -1;

					ASSERT( self->optimalTeamCount >= 0 );
					self->zeroOptimalTeams.set(self->optimalTeamCount == 0);
				}

				if( lastHealthy != healthy ) {
					lastHealthy = healthy;
					self->healthyTeamCount += healthy ? 1 : -1;

					ASSERT( self->healthyTeamCount >= 0 );
					self->zeroHealthyTeams->set(self->healthyTeamCount == 0);

					if( self->healthyTeamCount == 0 ) {
						TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->masterId).detail("SignallingTeam", team->getDesc());
					}

					TraceEvent("TeamHealthDifference", self->masterId)
						.detail("LastOptimal", lastOptimal)
						.detail("LastHealthy", lastHealthy)
						.detail("Optimal", optimal)
						.detail("OptimalTeamCount", self->optimalTeamCount);
				}

				lastServersLeft = serversLeft;
				lastAnyUndesired = anyUndesired;
				lastWrongConfiguration = anyWrongConfiguration;
				wrongSize = false;

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
				else if ( team->getServerIDs().size() != self->configuration.storageTeamSize || anyWrongConfiguration )
					team->setPriority( PRIORITY_TEAM_UNHEALTHY );
				else if( anyUndesired )
					team->setPriority( PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER );
				else
					team->setPriority( PRIORITY_TEAM_HEALTHY );

				if(lastPriority != team->getPriority()) {
					self->priority_teams[lastPriority]--;
					self->priority_teams[team->getPriority()]++;
				}

				TraceEvent("TeamPriorityChange", self->masterId).detail("Priority", team->getPriority());

				lastZeroHealthy = self->zeroHealthyTeams->get(); //set this again in case it changed from this teams health changing
				if( self->initialFailureReactionDelay.isReady() && !self->zeroHealthyTeams->get() ) {
					vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor( ShardsAffectedByTeamFailure::Team(team->getServerIDs(), self->primary) );

					for(int i=0; i<shards.size(); i++) {
						int maxPriority = team->getPriority();
						if(maxPriority < PRIORITY_TEAM_0_LEFT) {
							auto teams = self->shardsAffectedByTeamFailure->getTeamsFor( shards[i] );
							for( int t=0; t<teams.size(); t++) {
								if( teams[t].servers.size() && self->server_info.count( teams[t].servers[0] ) ) {
									auto& info = self->server_info[teams[t].servers[0]];

									bool found = false;
									for( int i = 0; i < info->teams.size(); i++ ) {
										if( info->teams[i]->serverIDs == teams[t].servers ) {
											maxPriority = std::max( maxPriority, info->teams[i]->getPriority() );
											found = true;
											break;
										}
									}

									TEST(!found); // A removed team is still associated with a shard in SABTF
								} else {
									TEST(teams[t].servers.size()); // A removed server is still associated with a team in SABTF
								}
							}
						}

						if( maxPriority == team->getPriority() || lastPriority > maxPriority ) {
							RelocateShard rs;
							rs.keys = shards[i];
							rs.priority = maxPriority;

							self->output.send(rs);
							if(g_random->random01() < 0.01) {
								TraceEvent("SendRelocateToDDQx100", self->masterId)
									.detail("Team", team->getDesc())
									.detail("KeyBegin", printable(rs.keys.begin))
									.detail("KeyEnd", printable(rs.keys.end))
									.detail("Priority", rs.priority)
									.detail("TeamFailedMachines", team->getServerIDs().size()-serversLeft)
									.detail("TeamOKMachines", serversLeft);
							}
						} else {
							TraceEvent("RelocationNotSentToDDQ", self->masterId)
								.detail("Team", team->getDesc());
						}
					}
				} else {
					TraceEvent("TeamHealthNotReady", self->masterId).detail("HealthyTeamCount", self->healthyTeamCount);
				}
			}

			// Wait for any of the machines to change status
			Void _ = wait( quorum( change, 1 ) );
			Void _ = wait( yield() );
		}
	} catch(Error& e) {
		self->priority_teams[team->getPriority()]--;
		if( team->isHealthy() ) {
			self->healthyTeamCount--;
			ASSERT( self->healthyTeamCount >= 0 );

			if( self->healthyTeamCount == 0 ) {
				TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->masterId).detail("SignallingTeam", team->getDesc());
				self->zeroHealthyTeams->set(true);
			}
		}
		throw;
	}
}

ACTOR Future<Void> trackExcludedServers( DDTeamCollection *self, Database cx ) {
	loop {
		// Fetch the list of excluded servers
		state Transaction tr(cx);
		state Optional<Value> lastChangeID;
		loop {
			try {
				state Future<Standalone<RangeResultRef>> fresults = tr.getRange( excludedServersKeys, CLIENT_KNOBS->TOO_MANY );
				state Future<Optional<Value>> fchid = tr.get( excludedServersVersionKey );
				Void _ = wait( success(fresults) && success(fchid) );

				Standalone<RangeResultRef> results = fresults.get();
				lastChangeID = fchid.get();
				ASSERT( !results.more && results.size() < CLIENT_KNOBS->TOO_MANY );

				std::set<AddressExclusion> excluded;
				for(auto r = results.begin(); r != results.end(); ++r) {
					AddressExclusion addr = decodeExcludedServersKey(r->key);
					if (addr.isValid())
						excluded.insert( addr );
				}

				TraceEvent("DDExcludedServersChanged", self->masterId).detail("Rows", results.size()).detail("Exclusions", excluded.size());

				// Reset and reassign self->excludedServers based on excluded, but weonly
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
				Void _ = wait( tr.onError(e) );
			}
		}

		// Wait for a change in the list of excluded servers
		loop {
			try {
				Optional<Value> nchid = wait( tr.get( excludedServersVersionKey ) );
				if (nchid != lastChangeID)
					break;

				Void _ = wait( delay( SERVER_KNOBS->SERVER_LIST_DELAY, TaskDataDistribution ) );  // FIXME: make this tr.watch( excludedServersVersionKey ) instead
				tr = Transaction(cx);
			} catch (Error& e) {
				Void _ = wait( tr.onError(e) );
			}
		}
	}
}

ACTOR Future<vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses( Transaction *tr ) {
	state Future<vector<ProcessData>> workers = getWorkers(tr);
	state Future<Standalone<RangeResultRef>> serverList = tr->getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY );
	Void _ = wait( success(workers) && success(serverList) );
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

ACTOR Future<Void> waitServerListChange( DDTeamCollection *self, Database cx, FutureStream<Void> serverRemoved ) {
	state Future<Void> checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY);
	state Future<vector<std::pair<StorageServerInterface, ProcessClass>>> serverListAndProcessClasses = Never();
	state bool isFetchingResults = false;
	state Transaction tr(cx);
	loop {
		try {
			choose {
				when( Void _ = wait( checkSignal ) ) {
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

					tr = Transaction(cx);
					checkSignal = delay(SERVER_KNOBS->SERVER_LIST_DELAY);
				}
				when( Void _ = waitNext( serverRemoved ) ) {
					if( isFetchingResults ) {
						tr = Transaction(cx);
						serverListAndProcessClasses = getServerListAndProcessClasses(&tr);
					}
				}
			}
		} catch(Error& e) {
			Void _ = wait( tr.onError(e) );
			serverListAndProcessClasses = Never();
			isFetchingResults = false;
			checkSignal = Void();
		}
	}
}

ACTOR Future<Void> serverMetricsPolling( TCServerInfo *server) {
	state double lastUpdate = now();
	loop {
		Void _ = wait( updateServerMetrics( server ) );
		Void _ = wait( delayUntil( lastUpdate + SERVER_KNOBS->STORAGE_METRICS_POLLING_DELAY + SERVER_KNOBS->STORAGE_METRICS_RANDOM_DELAY * g_random->random01(), TaskDataDistributionLaunch ) );
		lastUpdate = now();
	}
}

//Returns the KeyValueStoreType of server if it is different from self->storeType
ACTOR Future<KeyValueStoreType> keyValueStoreTypeTracker(DDTeamCollection *self, TCServerInfo *server) {
	state KeyValueStoreType type = wait(brokenPromiseToNever(server->lastKnownInterface.getKeyValueStoreType.getReplyWithTaskID<KeyValueStoreType>(TaskDataDistribution)));
	if(type == self->configuration.storageServerStoreType && (self->includedDCs.empty() || std::find(self->includedDCs.begin(), self->includedDCs.end(), server->lastKnownInterface.locality.dcId()) != self->includedDCs.end()) )
		Void _ = wait(Future<Void>(Never()));

	return type;
}

//MX: Check the status of a storage server. Apply all requirements to the server and mark it as excluded if it fails to satisfies these requirements
ACTOR Future<Void> storageServerTracker(
	DDTeamCollection *self,
	Database cx,
	TCServerInfo *server, //This actor is owned by this TCServerInfo
	ServerStatusMap *statusMap,
	MoveKeysLock lock,
	UID masterId,
	std::map<UID, Reference<TCServerInfo>>* other_servers,
	Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >> changes,
	Promise<Void> errorOut,
	Version addedVersion)
{
	Void _ = wait( self->readyToStart );
	
	state Future<Void> failureTracker;
	state ServerStatus status( false, false, server->lastKnownInterface.locality );
	state bool lastIsUnhealthy = false;
	state Future<Void> metricsTracker = serverMetricsPolling( server );
	state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged = server->onInterfaceChanged;

	state Future<KeyValueStoreType> storeTracker = keyValueStoreTypeTracker( self, server );
	state bool hasWrongStoreTypeOrDC = false;

	if(changes.present()) {
		changes.get().send( std::make_pair(server->id, server->lastKnownInterface) );
	}

	try {
		loop {
			status.isUndesired = false;
			status.isWrongConfiguration = false;

			// If there is any other server on this exact NetworkAddress, this server is undesired and will eventually be eliminated
			state std::vector<Future<Void>> otherChanges;
			std::vector<Promise<Void>> wakeUpTrackers;
			for(auto i = other_servers->begin(); i != other_servers->end(); ++i) {
				if (i->second.getPtr() != server && i->second->lastKnownInterface.address() == server->lastKnownInterface.address()) {
					auto& statusInfo = statusMap->get( i->first );
					TraceEvent("SameAddress", masterId)
						.detail("Failed", statusInfo.isFailed)
						.detail("Undesired", statusInfo.isUndesired)
						.detail("Server", server->id).detail("OtherServer", i->second->id)
						.detail("Address", server->lastKnownInterface.address())
						.detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
						.detail("OtherNumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(i->second->id))
						.detail("OtherHealthy", !statusMap->get( i->second->id ).isUnhealthy());
					otherChanges.push_back( statusMap->onChange( i->second->id ) ); //MX: wait for the server's ip to be changed
					if(!statusMap->get( i->second->id ).isUnhealthy()) {
						if(self->shardsAffectedByTeamFailure->getNumberOfShards(i->second->id) >= self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
						{
							TraceEvent(SevWarn, "UndesiredStorageServer", masterId)
								.detail("Server", server->id)
								.detail("Address", server->lastKnownInterface.address())
								.detail("OtherServer", i->second->id)
								.detail("NumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(server->id))
								.detail("OtherNumShards", self->shardsAffectedByTeamFailure->getNumberOfShards(i->second->id));

							status.isUndesired = true;
						}
						else
							wakeUpTrackers.push_back(i->second->wakeUpTracker);
					}
				}
			}

			for(auto& p : wakeUpTrackers) {
				if( !p.isSet() )
					p.send(Void());
			}

			if( server->lastKnownClass.machineClassFitness( ProcessClass::Storage ) > ProcessClass::UnsetFit ) {
				if( self->optimalTeamCount > 0 ) {
					TraceEvent(SevWarn, "UndesiredStorageServer", masterId).detail("Server", server->id).detail("OptimalTeamCount", self->optimalTeamCount);
					status.isUndesired = true;
				}
				otherChanges.push_back( self->zeroOptimalTeams.onChange() );
			}

			//If this storage server has the wrong key-value store type, then mark it undesired so it will be replaced with a server having the correct type
			if(hasWrongStoreTypeOrDC) {
				TraceEvent(SevWarn, "UndesiredStorageServer", masterId).detail("Server", server->id).detail("StoreType", "?");
				status.isUndesired = true;
				status.isWrongConfiguration = true;
			}

			// If the storage server is in the excluded servers list, it is undesired
			NetworkAddress a = server->lastKnownInterface.address();
			AddressExclusion addr( a.ip, a.port );
			AddressExclusion ipaddr( a.ip );
			if (self->excludedServers.get( addr ) || self->excludedServers.get( ipaddr )) {
				TraceEvent(SevWarn, "UndesiredStorageServer", masterId).detail("Server", server->id)
					.detail("Excluded", self->excludedServers.get( addr ) ? addr.toString() : ipaddr.toString());
				status.isUndesired = true;
				status.isWrongConfiguration = true;
			}
			otherChanges.push_back( self->excludedServers.onChange( addr ) );
			otherChanges.push_back( self->excludedServers.onChange( ipaddr ) );

			failureTracker = storageServerFailureTracker( cx, server->lastKnownInterface, statusMap, &status, &self->restartRecruiting, &self->unhealthyServers, masterId, addedVersion );

			//We need to recruit new storage servers if the key value store type has changed
			if(hasWrongStoreTypeOrDC)
				self->restartRecruiting.trigger();

			if( lastIsUnhealthy && !status.isUnhealthy() && !server->teams.size() )
				self->doBuildTeams = true;
			lastIsUnhealthy = status.isUnhealthy();

			choose {
				when( Void _ = wait( failureTracker ) ) {
					// The server is failed AND all data has been removed from it, so permanently remove it.
					TraceEvent("StatusMapChange", masterId).detail("ServerID", server->id).detail("Status", "Removing");
					if(changes.present()) {
						changes.get().send( std::make_pair(server->id, Optional<StorageServerInterface>()) );
					}

					// Remove server from FF/serverList
					Void _ = wait( removeStorageServer( cx, server->id, lock ) );

					TraceEvent("StatusMapChange", masterId).detail("ServerID", server->id).detail("Status", "Removed");
					// Sets removeSignal (alerting dataDistributionTeamCollection to remove the storage server from its own data structures)
					server->removed.send( Void() );
					self->removedServers.send( server->id );
					return Void();
				}
				when( std::pair<StorageServerInterface, ProcessClass> newInterface = wait( interfaceChanged ) ) {
					bool restartRecruiting =  newInterface.first.waitFailure.getEndpoint().address != server->lastKnownInterface.waitFailure.getEndpoint().address;
					TraceEvent("StorageServerInterfaceChanged", masterId).detail("ServerID", server->id)
						.detail("NewWaitFailureToken", newInterface.first.waitFailure.getEndpoint().token)
						.detail("OldWaitFailureToken", server->lastKnownInterface.waitFailure.getEndpoint().token);
					server->lastKnownInterface = newInterface.first;
					server->lastKnownClass = newInterface.second;
					interfaceChanged = server->onInterfaceChanged;
					if(changes.present()) {
						changes.get().send( std::make_pair(server->id, server->lastKnownInterface) );
					}
					// We rely on the old failureTracker being actorCancelled since the old actor now has a pointer to an invalid location
					status = ServerStatus( status.isFailed, status.isUndesired, server->lastKnownInterface.locality );

					//Restart the storeTracker for the new interface
					storeTracker = keyValueStoreTypeTracker(self, server);
					hasWrongStoreTypeOrDC = false;
					self->restartTeamBuilder.trigger();
					if(restartRecruiting)
						self->restartRecruiting.trigger();
				}
				when( Void _ = wait( otherChanges.empty() ? Never() : quorum( otherChanges, 1 ) ) ) {
					TraceEvent("SameAddressChangedStatus", masterId).detail("ServerID", server->id);
				}
				when( KeyValueStoreType type = wait( storeTracker ) ) {
					TraceEvent("KeyValueStoreTypeChanged", masterId)
						.detail("ServerID", server->id)
						.detail("StoreType", type.toString())
						.detail("DesiredType", self->configuration.storageServerStoreType.toString());
					TEST(true); //KeyValueStore type changed

					storeTracker = Never();
					hasWrongStoreTypeOrDC = true;
				}
				when( Void _ = wait( server->wakeUpTracker.getFuture() ) ) {
					server->wakeUpTracker = Promise<Void>();
				}
			}
		}
	} catch( Error &e ) {
		if (e.code() != error_code_actor_cancelled)
			errorOut.sendError(e);
		throw;
	}
}

//Monitor whether or not storage servers are being recruited.  If so, then a database cannot be considered quiet
ACTOR Future<Void> monitorStorageServerRecruitment(DDTeamCollection *self) {
	state bool recruiting = false;
	TraceEvent("StorageServerRecruitment", self->masterId)
		.detail("State", "Idle")
		.trackLatest((self->cx->dbName.toString() + "/StorageServerRecruitment_" + self->masterId.toString()).c_str());
	loop {
		if( !recruiting ) {
			while(self->recruitingStream.get() == 0) {
				Void _ = wait( self->recruitingStream.onChange() );
			}
			TraceEvent("StorageServerRecruitment", self->masterId)
				.detail("State", "Recruiting")
				.trackLatest((self->cx->dbName.toString() + "/StorageServerRecruitment_" + self->masterId.toString()).c_str());
			recruiting = true;
		} else {
			loop {
				choose {
					when( Void _ = wait( self->recruitingStream.onChange() ) ) {}
					when( Void _ = wait( self->recruitingStream.get() == 0 ? delay(SERVER_KNOBS->RECRUITMENT_IDLE_DELAY, TaskDataDistribution) : Future<Void>(Never()) ) ) { break; }
				}
			}
			TraceEvent("StorageServerRecruitment", self->masterId)
				.detail("State", "Idle")
				.trackLatest((self->cx->dbName.toString() + "/StorageServerRecruitment_" + self->masterId.toString()).c_str());
			recruiting = false;
		}
	}
}

ACTOR Future<Void> initializeStorage( DDTeamCollection *self, RecruitStorageReply candidateWorker ) {
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
	ErrorOr<InitializeStorageReply> newServer = wait( candidateWorker.worker.storage.tryGetReply( isr, TaskDataDistribution ) );
	self->recruitingIds.erase(interfaceId);
	self->recruitingLocalities.erase(candidateWorker.worker.address());

	self->recruitingStream.set(self->recruitingStream.get()-1);

	TraceEvent("DDRecruiting").detail("State", "Finished request").detail("WorkerID", candidateWorker.worker.id())
		.detail("WorkerLocality", candidateWorker.worker.locality.toString()).detail("Interf", interfaceId).detail("Addr", candidateWorker.worker.address());

	if( newServer.isError() ) {
		TraceEvent(SevWarn, "DDRecruitmentError").error(newServer.getError());
		if( !newServer.isError( error_code_recruitment_failed ) && !newServer.isError( error_code_request_maybe_delivered ) )
			throw newServer.getError();
		Void _ = wait( delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskDataDistribution) );
	}
	else if( newServer.present() ) {
		if( !self->server_info.count( newServer.get().interf.id() ) )
			self->addServer( newServer.get().interf, candidateWorker.processClass, self->serverTrackerErrorOut, newServer.get().addedVersion );
		else
			TraceEvent(SevWarn, "DDRecruitmentError").detail("Reason", "Server ID already recruited");

		self->doBuildTeams = true;
		if( self->healthyTeamCount == 0 ) {
			Void _ = wait( self->checkBuildTeams( self ) );
		}
	}

	self->restartRecruiting.trigger();

	return Void();
}

ACTOR Future<Void> storageRecruiter( DDTeamCollection *self, Reference<AsyncVar<struct ServerDBInfo>> db ) {
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
				TraceEvent(SevWarn, "DDRecruitingEmergency", self->masterId);
			}

			if(!fCandidateWorker.isValid() || fCandidateWorker.isReady() || rsr.excludeAddresses != lastRequest.excludeAddresses || rsr.criticalRecruitment != lastRequest.criticalRecruitment) {
				lastRequest = rsr;
				fCandidateWorker = brokenPromiseToNever( db->get().clusterInterface.recruitStorage.getReply( rsr, TaskDataDistribution ) );
			}

			choose {
				when( RecruitStorageReply candidateWorker = wait( fCandidateWorker ) ) {
					self->addActor.send(initializeStorage(self, candidateWorker));
				}
				when( Void _ = wait( db->onChange() ) ) { // SOMEDAY: only if clusterInterface changes?
					fCandidateWorker = Future<RecruitStorageReply>();
				}
				when( Void _ = wait( self->restartRecruiting.onTrigger() ) ) {}
			}
			Void _ = wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		} catch( Error &e ) {
			if(e.code() != error_code_timed_out) {
				throw;
			}
			TEST(true); //Storage recruitment timed out
		}
	}
}

ACTOR Future<Void> updateReplicasKey(DDTeamCollection* self, Optional<Key> dcId) {
	Void _ = wait(self->initialFailureReactionDelay);
	loop {
		while(self->zeroHealthyTeams->get() || self->processingUnhealthy->get()) {
			TraceEvent("DDUpdatingStalled", self->masterId).detail("DcId", printable(dcId)).detail("ZeroHealthy", self->zeroHealthyTeams->get()).detail("ProcessingUnhealthy", self->processingUnhealthy->get());
			Void _ = wait(self->zeroHealthyTeams->onChange() || self->processingUnhealthy->onChange());
		}
		Void _ = wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskLowPriority)); //After the team trackers wait on the initial failure reaction delay, they yield. We want to make sure every tracker has had the opportunity to send their relocations to the queue.
		if(!self->zeroHealthyTeams->get() && !self->processingUnhealthy->get()) {
			break;
		}
	}
	TraceEvent("DDUpdatingReplicas", self->masterId).detail("DcId", printable(dcId)).detail("Replicas", self->configuration.storageTeamSize);
	state Transaction tr(self->cx);
	loop {
		try {
			tr.addReadConflictRange(singleKeyRange(datacenterReplicasKeyFor(dcId)));
			tr.set(datacenterReplicasKeyFor(dcId), datacenterReplicasValue(self->configuration.storageTeamSize));
			Void _ = wait( tr.commit() );
			TraceEvent("DDUpdatedReplicas", self->masterId).detail("DcId", printable(dcId)).detail("Replicas", self->configuration.storageTeamSize);
			return Void();
		} catch( Error &e ) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> serverGetTeamRequests(TeamCollectionInterface tci, DDTeamCollection* self) {
	loop {
		GetTeamRequest req = waitNext(tci.getTeam.getFuture());
		self->addActor.send( self->getTeam( self, req ) );
	}
}

// Keep track of servers and teams -- serves requests for getRandomTeam
ACTOR Future<Void> dataDistributionTeamCollection(//MX: May be related!
	Reference<InitialDataDistribution> initData,
	TeamCollectionInterface tci,
	Database cx,
	Reference<AsyncVar<struct ServerDBInfo>> db,
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
	MoveKeysLock lock,
	PromiseStream<RelocateShard> output,
	UID masterId,
	DatabaseConfiguration configuration,
	std::vector<Optional<Key>> includedDCs,
	Optional<std::vector<Optional<Key>>> otherTrackedDCs,
	Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >> serverChanges,
	Future<Void> readyToStart,
	Reference<AsyncVar<bool>> zeroHealthyTeams,
	bool primary,
	Reference<AsyncVar<bool>> processingUnhealthy)
{
	state DDTeamCollection self( cx, masterId, lock, output, shardsAffectedByTeamFailure, configuration, includedDCs, otherTrackedDCs, serverChanges, readyToStart, zeroHealthyTeams, primary, processingUnhealthy );
	state Future<Void> loggingTrigger = Void();
	state PromiseStream<Void> serverRemoved;
	state Future<Void> error = actorCollection( self.addActor.getFuture() );

	try {
		self.init( *initData );
		initData = Reference<InitialDataDistribution>();
		self.addActor.send(serverGetTeamRequests(tci, &self));

		TraceEvent("DDTeamCollectionBegin", masterId).detail("Primary", primary);
		Void _ = wait( readyToStart || error );
		TraceEvent("DDTeamCollectionReadyToStart", masterId).detail("Primary", primary);
		
		self.addActor.send(storageRecruiter( &self, db ));
		self.addActor.send(monitorStorageServerRecruitment( &self ));
		self.addActor.send(waitServerListChange( &self, cx, serverRemoved.getFuture() ));
		self.addActor.send(trackExcludedServers( &self, cx ));
		
		if(includedDCs.size()) {
			self.addActor.send(updateReplicasKey(&self, includedDCs[0]));
		}
		// SOMEDAY: Monitor FF/serverList for (new) servers that aren't in allServers and add or remove them

		loop choose {
			when( UID removedServer = waitNext( self.removedServers.getFuture() ) ) {
				TEST(true);  // Storage server removed from database
				self.removeServer( removedServer );
				serverRemoved.send( Void() );

				self.restartRecruiting.trigger();
			}
			when( Void _ = wait( self.zeroHealthyTeams->onChange() ) ) {
				if(self.zeroHealthyTeams->get()) {
					self.restartRecruiting.trigger();
					self.noHealthyTeams();
				}
			}
			when( Void _ = wait( loggingTrigger ) ) {
				int highestPriority = 0;
				for(auto it : self.priority_teams) {
					if(it.second > 0) {
						highestPriority = std::max(highestPriority, it.first);
					}
				}
				TraceEvent("TotalDataInFlight", masterId).detail("Primary", self.primary).detail("TotalBytes", self.getDebugTotalDataInFlight()).detail("UnhealthyServers", self.unhealthyServers)
					.detail("HighestPriority", highestPriority).trackLatest( self.primary ? "TotalDataInFlight" : "TotalDataInFlightRemote" );
				loggingTrigger = delay( SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL );
				self.countHealthyTeams();
			}
			when( Void _ = wait( self.serverTrackerErrorOut.getFuture() ) ) {} // Propagate errors from storageServerTracker
			when( Void _ = wait( error ) ) {}
		}
	} catch (Error& e) {
		if (e.code() != error_code_movekeys_conflict)
			TraceEvent(SevError, "DataDistributionTeamCollectionError", masterId).error(e);
		throw e;
	}
}

ACTOR Future<Void> waitForDataDistributionEnabled( Database cx ) {
	state Transaction tr(cx);
	loop {
		Void _ = wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskDataDistribution));

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
			Void _ = wait( tr.onError(e) );
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
			// SOMEDAY: Write a wrapper in MoveKeys.h
			Optional<Value> readVal = wait( tr.get( moveKeysLockOwnerKey ) );
			UID currentOwner = readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
			if( currentOwner != dataDistributionModeLock )
				return true;
			return false;
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
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
			Void _ = wait( tr.onError(e) );
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
		Void _ = wait(delay(SERVER_KNOBS->MOVEKEYS_LOCK_POLLING_DELAY));
		state Transaction tr(cx);
		loop {
			try {
				Void _ = wait( checkMoveKeysLockReadOnly(&tr, lock) );
				break;
			} catch( Error &e ) {
				Void _ = wait( tr.onError(e) );
			}
		}
	}
}

ACTOR Future<Void> dataDistribution(
		Reference<AsyncVar<struct ServerDBInfo>> db,
		MasterInterface mi, DatabaseConfiguration configuration,
		PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges,
		Reference<ILogSystem> logSystem,
		Version recoveryCommitVersion,
		std::vector<Optional<Key>> primaryDcId,
		std::vector<Optional<Key>> remoteDcIds,
		double* lastLimited,
		Future<Void> remoteRecovered)
{
	state Database cx = openDBOnServer(db, TaskDataDistributionLaunch, true, true);
	cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;

	state Transaction tr(cx);
	loop {
		try {
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );

			Standalone<RangeResultRef> replicaKeys = wait(tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY));

			for(auto& kv : replicaKeys) {
				auto dcId = decodeDatacenterReplicasKey(kv.key);
				auto replicas = decodeDatacenterReplicasValue(kv.value);
				if((primaryDcId.size() && primaryDcId[0] == dcId) || (remoteDcIds.size() && remoteDcIds[0] == dcId)) {
					if(replicas > configuration.storageTeamSize) {
						tr.set(kv.key, datacenterReplicasValue(configuration.storageTeamSize));
					}
				} else {
					tr.clear(kv.key);
				}
			}

			Void _ = wait(tr.commit());
			break;
		}
		catch(Error &e) {
			Void _ = wait(tr.onError(e));
		}
	}


	//cx->setOption( FDBDatabaseOptions::LOCATION_CACHE_SIZE, StringRef((uint8_t*) &SERVER_KNOBS->DD_LOCATION_CACHE_SIZE, 8) );
	//ASSERT( cx->locationCacheSize == SERVER_KNOBS->DD_LOCATION_CACHE_SIZE );

	//Void _ = wait(debugCheckCoalescing(cx));

	loop {
		try {
			loop {
				TraceEvent("DDInitTakingMoveKeysLock", mi.id());
				state MoveKeysLock lock = wait( takeMoveKeysLock( cx, mi.id() ) );
				TraceEvent("DDInitTookMoveKeysLock", mi.id());
				state Reference<InitialDataDistribution> initData = wait( getInitialDataDistribution(cx, mi.id(), lock, configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>() ) );
				if(initData->shards.size() > 1) {
					TraceEvent("DDInitGotInitialDD", mi.id()).detail("B", printable(initData->shards.end()[-2].key)).detail("E", printable(initData->shards.end()[-1].key))
					.detail("Src", describe(initData->shards.end()[-2].primarySrc)).detail("Dest", describe(initData->shards.end()[-2].primaryDest)).trackLatest("InitialDD");
				} else {
					TraceEvent("DDInitGotInitialDD", mi.id()).detail("B","").detail("E", "").detail("Src", "[no items]").detail("Dest", "[no items]").trackLatest("InitialDD");
				}

				if (initData->mode) break;//MX:Q: when will initData->mode become true?
				TraceEvent("DataDistributionDisabled", mi.id());

				TraceEvent("MovingData", mi.id())
					.detail( "InFlight", 0 )
					.detail( "InQueue", 0 )
					.detail( "AverageShardSize", -1 )
					.detail( "LowPriorityRelocations", 0 )
					.detail( "HighPriorityRelocations", 0 )
					.detail( "HighestPriority", 0 )
					.trackLatest( format("%s/MovingData", printable(cx->dbName).c_str() ).c_str() );

				TraceEvent("TotalDataInFlight", mi.id()).detail("Primary", true).detail("TotalBytes", 0).detail("UnhealthyServers", 0).detail("HighestPriority", 0).trackLatest("TotalDataInFlight");
				TraceEvent("TotalDataInFlight", mi.id()).detail("Primary", false).detail("TotalBytes", 0).detail("UnhealthyServers", 0).detail("HighestPriority", configuration.usableRegions > 1 ? 0 : -1).trackLatest("TotalDataInFlightRemote");

				Void _ = wait( waitForDataDistributionEnabled(cx) );
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
			for(; shard<initData->shards.size() - 1; shard++) {
				KeyRangeRef keys = KeyRangeRef(initData->shards[shard].key, initData->shards[shard+1].key);
				shardsAffectedByTeamFailure->defineShard(keys);
				std::vector<ShardsAffectedByTeamFailure::Team> teams;
				teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].primarySrc, true));
				if(configuration.usableRegions > 1) {
					teams.push_back(ShardsAffectedByTeamFailure::Team(initData->shards[shard].remoteSrc, false));
				}
				shardsAffectedByTeamFailure->moveShard(keys, teams);
				if(initData->shards[shard].hasDest) {
					// This shard is already in flight.  Ideally we should use dest in sABTF and generate a dataDistributionRelocator directly in
					// DataDistributionQueue to track it, but it's easier to just (with low priority) schedule it for movement.
					output.send( RelocateShard( keys, PRIORITY_RECOVER_MOVE ) );
				}
				Void _ = wait( yield(TaskDataDistribution) );
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
			actors.push_back( reportErrorsExcept( dataDistributionTracker( initData, cx, output, shardsAffectedByTeamFailure, getShardMetrics, getAverageShardBytes.getFuture(), readyToStart, anyZeroHealthyTeams, mi.id() ), "DDTracker", mi.id(), &normalDDQueueErrors() ) );
			actors.push_back( reportErrorsExcept( dataDistributionQueue( cx, output, input.getFuture(), getShardMetrics, processingUnhealthy, tcis, shardsAffectedByTeamFailure, lock, getAverageShardBytes, mi, storageTeamSize, lastLimited, recoveryCommitVersion ), "DDQueue", mi.id(), &normalDDQueueErrors() ) );
			actors.push_back( reportErrorsExcept( dataDistributionTeamCollection( initData, tcis[0], cx, db, shardsAffectedByTeamFailure, lock, output, mi.id(), configuration, primaryDcId, configuration.usableRegions > 1 ? remoteDcIds : std::vector<Optional<Key>>(), serverChanges, readyToStart.getFuture(), zeroHealthyTeams[0], true, processingUnhealthy ), "DDTeamCollectionPrimary", mi.id(), &normalDDQueueErrors() ) );
			if (configuration.usableRegions > 1) {
				actors.push_back( reportErrorsExcept( dataDistributionTeamCollection( initData, tcis[1], cx, db, shardsAffectedByTeamFailure, lock, output, mi.id(), configuration, remoteDcIds, Optional<std::vector<Optional<Key>>>(), Optional<PromiseStream< std::pair<UID, Optional<StorageServerInterface>> >>(), readyToStart.getFuture() && remoteRecovered, zeroHealthyTeams[1], false, processingUnhealthy ), "DDTeamCollectionSecondary", mi.id(), &normalDDQueueErrors() ) );
			}
			actors.push_back(yieldPromiseStream(output.getFuture(), input));

			Void _ = wait( waitForAll( actors ) );
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


void traceServerInfo(DDTeamCollection* collection, int processCount) {
	for (int id = 1; id <= processCount; id++ ) {
		UID uid(id, 0);
		LocalityData locality = collection->server_status.get(uid).locality;
		TraceEvent("ServerInfo", uid)
				.detail("MachineId", locality.describeMachineId())
				.detail("ZoneId", locality.describeZone())
				.detail("DatahallId", locality.describeDataHall())
				.detail("DcId", locality.describeDcId());
	}

	return;
}


DDTeamCollection* testTeamCollection(int teamSize, IRepPolicyRef policy, int processCount) {
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
		PromiseStream<std::pair<UID, Optional<StorageServerInterface>>>(),
		Future<Void>(Void()),
		Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ),
		true,
		Reference<AsyncVar<bool>>( new AsyncVar<bool>(false) )
	);

	for(int id = 1; id <= processCount; id++) {
		UID uid(id, 0);
		StorageServerInterface interface;
		interface.uniqueID = uid;
	 	interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(id % 5)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(id % 3)));
		collection->server_info[uid] = Reference<TCServerInfo>(new TCServerInfo(
			interface,
			ProcessClass()
		));
		collection->server_status.set(uid, ServerStatus(false, false, interface.locality));
	}

	traceServerInfo(collection, processCount);

	return collection;
}

//=====================MX Added function to understand the current team collection function
//TODO: To delete this function
DDTeamCollection* testTeamCollectionMX1(int teamSize, IRepPolicyRef policy, int processCount) {
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
			PromiseStream<std::pair<UID, Optional<StorageServerInterface>>>(),
			Future<Void>(Void()),
			Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ),
			true,
			Reference<AsyncVar<bool>>( new AsyncVar<bool>(false) )
	);

	for(int id = 1; id <= processCount; id++) {
		UID uid(id, 0);
		StorageServerInterface interface;
		interface.uniqueID = uid;
		int process_id = id;
		int machine_id = process_id % 10; //each machine has 10 processes
		int zone_id = machine_id % 5; //each zone (rack) has 5 machines
		int data_hall_id = zone_id % 3; //each data hall has 3 zones(racks)
		int dc_id = data_hall_id % 1; //only 1 dc for now

		interface.locality.set(LiteralStringRef("processid"), Standalone<StringRef>(std::to_string(process_id)));
		interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(machine_id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(zone_id)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(data_hall_id)));
		interface.locality.set(LiteralStringRef("dcid"), Standalone<StringRef>(std::to_string(dc_id)));
		collection->server_info[uid] = Reference<TCServerInfo>(new TCServerInfo(
				interface,
				ProcessClass()
		));
		collection->server_status.set(uid, ServerStatus(false, false, interface.locality));
	}

	traceServerInfo(collection, processCount);

	return collection;
}

DDTeamCollection* testMachineTeamCollection(int teamSize, IRepPolicyRef policy, int processCount) {
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
			PromiseStream<std::pair<UID, Optional<StorageServerInterface>>>(),
			Future<Void>(Void()),
			Reference<AsyncVar<bool>>( new AsyncVar<bool>(true) ),
			true,
			Reference<AsyncVar<bool>>( new AsyncVar<bool>(false) )
	);

	for(int id = 1; id <= processCount; id++) {
		UID uid(id, 0);
		StorageServerInterface interface;
		interface.uniqueID = uid;
		int process_id = id;
		int dc_id = process_id / 1000;
		int data_hall_id = process_id / 100;
		int zone_id = process_id / 20;
		int machine_id = process_id / 10;

		printf("testMachineTeamCollection: process_id:%d zone_id:%d machine_id:%d ip_addr:%s\n",
				process_id, zone_id, machine_id, interface.address().toString().c_str());
		interface.locality.set(LiteralStringRef("processid"), Standalone<StringRef>(std::to_string(process_id)));
		interface.locality.set(LiteralStringRef("machineid"), Standalone<StringRef>(std::to_string(machine_id)));
		interface.locality.set(LiteralStringRef("zoneid"), Standalone<StringRef>(std::to_string(zone_id)));
		interface.locality.set(LiteralStringRef("data_hall"), Standalone<StringRef>(std::to_string(data_hall_id)));
		interface.locality.set(LiteralStringRef("dcid"), Standalone<StringRef>(std::to_string(dc_id)));
		collection->server_info[uid] = Reference<TCServerInfo>( new TCServerInfo(interface, ProcessClass()) );

		collection->server_status.set(uid, ServerStatus(false, false, interface.locality));
	}

	traceServerInfo(collection, processCount);

	return collection;
}



//=====================MX Unit Test to understand the Data Distribution function

TEST_CASE("MX/DataDistribution/AddAllTeams/isExhaustive") {
	int teamSize = 3; //replication size
	int processSize = 50;

	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(teamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollectionMX1(teamSize, policy, processSize);

	vector<UID> processes;
	for(auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		processes.push_back(process->first);
	}

	state vector<vector<UID>> teams;
	int result = wait(collection->addAllTeams(collection, processes, &teams, 200));

	//MX: Print out test
	collection->traceTeamInfo();

	delete(collection);
	//TraceEvent("DDTeamCollectionDeleted", 1).detail("AssertStart",1);

	for(int i = 0; i < teams.size(); i++) {
		auto team = teams[i];
	}
	ASSERT(result == 80);
	//ASSERT(result == 81); //MX
	ASSERT(teams[0] == std::vector<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	ASSERT(teams[1] == std::vector<UID>({ UID(1,0), UID(2,0), UID(4,0) }));
	ASSERT(teams[2] == std::vector<UID>({ UID(1,0), UID(2,0), UID(5,0) }));
	//ASSERT(teams[3] == std::vector<UID>({ UID(1,0), UID(2,0), UID(6,0) }));//Why team 3 is not this?
	ASSERT(teams[3] == std::vector<UID>({ UID(1,0), UID(2,0), UID(8,0) }));
	ASSERT(teams[4] == std::vector<UID>({ UID(1,0), UID(2,0), UID(9,0) }));
	ASSERT(teams[5] == std::vector<UID>({ UID(1,0), UID(2,0), UID(10,0) }));
	ASSERT(teams[6] == std::vector<UID>({ UID(1,0), UID(3,0), UID(4,0) }));
	ASSERT(teams[7] == std::vector<UID>({ UID(1,0), UID(3,0), UID(5,0) }));
	ASSERT(teams[8] == std::vector<UID>({ UID(1,0), UID(3,0), UID(7,0) }));
	ASSERT(teams[9] == std::vector<UID>({ UID(1,0), UID(3,0), UID(9,0) }));
	ASSERT(teams[10] == std::vector<UID>({ UID(1,0), UID(3,0), UID(10,0) }));
	ASSERT(teams[79] == std::vector<UID>({ UID(8,0), UID(9,0), UID(10,0) }));

	//Trace Team Info
	TraceEvent("TeamInfoAferAssert", UID(0,0)).detail("NumTeams", teams.size());
	for(int i = 0; i < teams.size(); i++) {
		auto team = teams[i];
		//print out machineID
		TraceEvent("TeamInfoUID", UID(i,0))
			.detail("TeamId", i)
			.detail("Member1a", team[0].first())
			.detail("Member1b", team[0].second())
			.detail("Member2a", team[1].first())
			.detail("Member2b", team[1].second())
			.detail("Member3a", team[2].first())
			.detail("Member3b", team[2].second())
			;
		}

	return Void();
}


TEST_CASE("MX/DataDistribution/AddTeamsBestOf/NotEnoughServers") {
	Void _ = wait(Future<Void>(Void()));

	int teamSize = 3; //replication size
	int processSize = 60;

	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(teamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollectionMX1(teamSize, policy, processSize);

	//collection->addTeam(std::set<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	//collection->addTeam(std::set<UID>({ UID(1,0), UID(3,0), UID(4,0) }));

	int result = collection->addTeamsBestOf(1);
	delete(collection);

	//ASSERT(result == 8);

	return Void();
}

//TODO: Test case
TEST_CASE("MX/DataDistribution/AddTeamsBestOf/UseMachineID") {
	Void _ = wait(Future<Void>(Void()));

	int teamSize = 3; //replication size
	int processSize = 60;

	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(teamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testMachineTeamCollection(teamSize, policy, processSize);

	//collection->addTeam(std::set<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	//collection->addTeam(std::set<UID>({ UID(1,0), UID(3,0), UID(4,0) }));

	int result = collection->addTeamsBestOf(10);
	collection->sanityCheckTeams();

	delete(collection);

	//ASSERT(result == 8);

	return Void();
}

TEST_CASE("MX/DataDistribution/AddTeamsBestOf/UseMachineIDBaseline") {
	Void _ = wait(Future<Void>(Void()));

	int teamSize = 3; //replication size
	int processSize = 60;

	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(teamSize, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testMachineTeamCollection(teamSize, policy, processSize);

	if ( collection == NULL ) {
		fprintf(stderr, "collection is null\n");
		return Void();
	}

	//collection->addTeam(std::set<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	//collection->addTeam(std::set<UID>({ UID(1,0), UID(3,0), UID(4,0) }));
	
	collection->addBestMachineTeams(); //not used by addTeamsBestOf_old, but used as a reference.
	int result = collection->addTeamsBestOf_old(10);
	collection->sanityCheckTeams();

	if ( collection )
		delete(collection);

	//ASSERT(result == 8);

	return Void();
}


//=====================

//MX: Test the buildTeam function!
TEST_CASE("DataDistribution/AddAllTeams/isExhaustive") {
	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollection(3, policy, 10);

	vector<UID> processes;
	for(auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		processes.push_back(process->first);
	}

	state vector<vector<UID>> teams;
	int result = wait(collection->addAllTeams(collection, processes, &teams, 200));

	delete(collection);

	for(int i = 0; i < teams.size(); i++) {
		auto team = teams[i];
	}
	ASSERT(result == 80);
	ASSERT(teams[0] == std::vector<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	ASSERT(teams[1] == std::vector<UID>({ UID(1,0), UID(2,0), UID(4,0) }));
	ASSERT(teams[2] == std::vector<UID>({ UID(1,0), UID(2,0), UID(5,0) }));
	ASSERT(teams[3] == std::vector<UID>({ UID(1,0), UID(2,0), UID(8,0) }));
	ASSERT(teams[4] == std::vector<UID>({ UID(1,0), UID(2,0), UID(9,0) }));
	ASSERT(teams[5] == std::vector<UID>({ UID(1,0), UID(2,0), UID(10,0) }));
	ASSERT(teams[6] == std::vector<UID>({ UID(1,0), UID(3,0), UID(4,0) }));
	ASSERT(teams[7] == std::vector<UID>({ UID(1,0), UID(3,0), UID(5,0) }));
	ASSERT(teams[8] == std::vector<UID>({ UID(1,0), UID(3,0), UID(7,0) }));
	ASSERT(teams[9] == std::vector<UID>({ UID(1,0), UID(3,0), UID(9,0) }));
	ASSERT(teams[10] == std::vector<UID>({ UID(1,0), UID(3,0), UID(10,0) }));
	ASSERT(teams[79] == std::vector<UID>({ UID(8,0), UID(9,0), UID(10,0) }));

	return Void();
}

TEST_CASE("DataDistribution/AddAllTeams/withLimit") {
	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollection(3, policy, 10);

	vector<UID> processes;
	for(auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		processes.push_back(process->first);
	}

	state vector<vector<UID>> teams;
	int result = wait(collection->addAllTeams(collection, processes, &teams, 10));
	delete(collection);

	for(int i = 0; i < teams.size(); i++) {
		auto team = teams[i];
	}
	ASSERT(result == 10);
	ASSERT(teams[0] == std::vector<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	ASSERT(teams[1] == std::vector<UID>({ UID(1,0), UID(2,0), UID(4,0) }));
	ASSERT(teams[2] == std::vector<UID>({ UID(1,0), UID(2,0), UID(5,0) }));
	ASSERT(teams[3] == std::vector<UID>({ UID(1,0), UID(2,0), UID(8,0) }));
	ASSERT(teams[4] == std::vector<UID>({ UID(1,0), UID(2,0), UID(9,0) }));
	ASSERT(teams[5] == std::vector<UID>({ UID(1,0), UID(2,0), UID(10,0) }));
	ASSERT(teams[6] == std::vector<UID>({ UID(1,0), UID(3,0), UID(4,0) }));
	ASSERT(teams[7] == std::vector<UID>({ UID(1,0), UID(3,0), UID(5,0) }));
	ASSERT(teams[8] == std::vector<UID>({ UID(1,0), UID(3,0), UID(7,0) }));
	ASSERT(teams[9] == std::vector<UID>({ UID(1,0), UID(3,0), UID(9,0) }));

	return Void();
}

TEST_CASE("DataDistribution/AddTeamsBestOf/SkippingBusyServers") {
	Void _ = wait(Future<Void>(Void()));
	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollection(3, policy, 10);

	collection->addTeam(std::set<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	collection->addTeam(std::set<UID>({ UID(1,0), UID(3,0), UID(4,0) }));

	int result = collection->addTeamsBestOf(8);

	ASSERT(result == 8);

	for(auto process = collection->server_info.begin(); process != collection->server_info.end(); process++) {
		auto teamCount = process->second->teams.size();
		ASSERT(teamCount >= 1);
		ASSERT(teamCount <= 5);
	}

	delete(collection);

	return Void();
}

TEST_CASE("DataDistribution/AddTeamsBestOf/NotEnoughServers") {
	Void _ = wait(Future<Void>(Void()));

	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
	state DDTeamCollection* collection = testTeamCollection(3, policy, 5);

	collection->addTeam(std::set<UID>({ UID(1,0), UID(2,0), UID(3,0) }));
	collection->addTeam(std::set<UID>({ UID(1,0), UID(3,0), UID(4,0) }));

	int result = collection->addTeamsBestOf(10);
	delete(collection);

	ASSERT(result == 8);

	return Void();
}
