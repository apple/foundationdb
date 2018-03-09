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

struct TCServerInfo : public ReferenceCounted<TCServerInfo> {
	UID id;
	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;
	vector<Reference<TCTeamInfo>> teams;
	Future<Void> tracker;
	int64_t dataInFlightToServer;
	ErrorOr<GetPhysicalMetricsReply> serverMetrics;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged;
	Promise<Void> removed;
	Future<Void> onRemoved;
	Promise<Void> wakeUpTracker;

	TCServerInfo(StorageServerInterface ssi, ProcessClass processClass) : id(ssi.id()), lastKnownInterface(ssi), lastKnownClass(processClass), dataInFlightToServer(0), onInterfaceChanged(interfaceChanged.getFuture()), onRemoved(removed.getFuture()) {}
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

private:
	// Calculate an "average" of the metrics replies that we received.  Penalize teams from which we did not receieve all replies.
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

ACTOR Future<Void> waitForAllDataRemoved( Database cx, UID serverID ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			bool canRemove = wait( canRemoveStorageServer( &tr, serverID ) );
			if (canRemove)
				return Void();

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
	PromiseStream<Void> serverFailures,
	int64_t *unhealthyServers,
	UID masterId )
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
			serverFailures.send( Void() );

		choose {
			when ( Void _ = wait( status->isFailed
				? IFailureMonitor::failureMonitor().onStateEqual( server.waitFailure.getEndpoint(), FailureStatus(false) )
				: waitFailureClient(server.waitFailure, SERVER_KNOBS->DATA_DISTRIBUTION_FAILURE_REACTION_TIME, 0, TaskDataDistribution) ) )
			{
				status->isFailed = !status->isFailed;
				TraceEvent("StatusMapChange", masterId).detail("ServerID", server.id()).detail("Status", status->toString()).
					detail("Available", IFailureMonitor::failureMonitor().getState(server.waitFailure.getEndpoint()).isAvailable());
			}
			when ( Void _ = wait( status->isUnhealthy() ? waitForAllDataRemoved(cx, server.id()) : Never() ) ) { break; }
		}
	}

	return Void();
}

// Read keyservers, return unique set of teams
ACTOR Future<Reference<InitialDataDistribution>> getInitialDataDistribution( Database cx, UID masterId, MoveKeysLock moveKeysLock ) {
	state Reference<InitialDataDistribution> result = Reference<InitialDataDistribution>(new InitialDataDistribution);
	state Key beginKey = allKeys.begin;

	state bool succeeded;

	state Transaction tr( cx );

	//Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure causing entries to be duplicated
	loop {
		succeeded = false;
		try {
			result->mode = 1;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> mode = wait( tr.get( dataDistributionModeKey ) );
			if (mode.present()) {
				BinaryReader rd( mode.get(), Unversioned() );
				rd >> result->mode;
			}
			if(!result->mode)
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
			}

			break;
		}
		catch(Error &e) {
			Void _ = wait( tr.onError(e) );

			ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
			TraceEvent("getInitialTeamsRetry", masterId);
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
					KeyRangeRef keys( keyServers[i].key, keyServers[i+1].key );
					decodeKeyServersValue( keyServers[i].value, src, dest );
					std::pair<vector<UID>,vector<UID>> teams;
					for(int j=0; j<src.size(); j++)
						teams.first.push_back(src[j]);
					for(int j=0; j<dest.size(); j++)
						teams.second.push_back(dest[j]);
					result->shards.push_back( keyRangeWith(keys, teams) );
					result->teams.insert( teams.first );
					if (dest.size())
						result->teams.insert( teams.second );
				}

				ASSERT(keyServers.size() > 0);
				beginKey = keyServers.end()[-1].key;
				break;
			} catch (Error& e) {
				Void _ = wait( tr.onError(e) );

				ASSERT(!succeeded); //We shouldn't be retrying if we have already started modifying result in this loop
				TraceEvent("getInitialTeamsKeyServersRetry", masterId);
			}
		}

		tr.reset();
	}

	// a dummy shard at the end with no keys or servers makes life easier for trackInitialShards()
	result->shards.push_back( keyRangeWith(KeyRangeRef(allKeys.end,allKeys.end), std::pair<vector<UID>, vector<UID>>()) );

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
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > const& changes,
	PromiseStream<Void> const& serverFailures,
	Promise<Void> const& errorOut);

Future<Void> teamTracker( struct DDTeamCollection* const& self, Reference<IDataDistributionTeam> const& team );

struct DDTeamCollection {
	enum { REQUESTING_WORKER = 0, GETTING_WORKER = 1, GETTING_STORAGE = 2 };

	PromiseStream<Future<Void>> addActor;
	Database cx;
	UID masterId;
	int teamSize;
	IRepPolicyRef replicationPolicy;
	KeyValueStoreType storeType;

	bool doBuildTeams;
	Future<Void> teamBuilder;
	AsyncTrigger restartTeamBuilder;

	MoveKeysLock lock;
	PromiseStream<RelocateShard> output;
	vector<UID> allServers;
	ServerStatusMap server_status;
	int64_t unhealthyServers;
	std::map<UID, Reference<TCServerInfo>> server_info;
	vector<Reference<TCTeamInfo>> teams;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	PromiseStream<UID> removedServers;
	std::set<UID> recruitingIds; // The IDs of the SS which are being recruited
	std::set<NetworkAddress> recruitingLocalities;
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges;
	PromiseStream<Void> serverFailures;
	Future<Void> initialFailureReactionDelay;
	Future<Void> initializationDoneActor;
	Promise<Void> serverTrackerErrorOut;
	AsyncVar<int> recruitingStream;
	Debouncer restartRecruiting;

	int healthyTeamCount;
	PromiseStream<Void> zeroHealthyTeams;

	int optimalTeamCount;
	PromiseStream<Void> optimalTeamChange;
	Promise<Void> onOptimalTeamChange;

	AsyncMap< AddressExclusion, bool > excludedServers;  // true if an address is in the excluded list in the database.  Updated asynchronously (eventually)

	DDTeamCollection(
		Database const& cx,
		UID masterId,
		MoveKeysLock const& lock,
		PromiseStream<RelocateShard> const& output,
		Reference<ShardsAffectedByTeamFailure> const& shardsAffectedByTeamFailure,
		int teamSize,
		IRepPolicyRef replicationPolicy,
		KeyValueStoreType storeType,
		PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > const& serverChanges,
		Future<Void> readyToStart )
		:cx(cx), masterId(masterId), lock(lock), output(output), shardsAffectedByTeamFailure(shardsAffectedByTeamFailure), doBuildTeams( true ), teamBuilder( Void() ),
		 teamSize( teamSize ), replicationPolicy(replicationPolicy), storeType( storeType ), serverChanges(serverChanges),
		 initialFailureReactionDelay( delay( BUGGIFY ? 0 : SERVER_KNOBS->INITIAL_FAILURE_REACTION_DELAY, TaskDataDistribution  ) ), healthyTeamCount( 0 ),
		 initializationDoneActor(logOnCompletion(readyToStart && initialFailureReactionDelay, this)), optimalTeamCount( 0 ), recruitingStream(0), restartRecruiting( SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY ),
		 unhealthyServers(0)
	{
		TraceEvent("DDTrackerStarting", masterId)
			.detail( "State", "Inactive" )
			.trackLatest( format("%s/DDTrackerStarting", printable(cx->dbName).c_str() ).c_str() );
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

		TraceEvent("DDTrackerStarting", self->masterId)
			.detail( "State", "Active" )
			.trackLatest( format("%s/DDTrackerStarting", printable(self->cx->dbName).c_str() ).c_str() );

		return Void();
	}

	ACTOR Future<Void> checkBuildTeams( DDTeamCollection* self ) {
		state Promise<Void> restart;

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
	ACTOR Future<Void> getTeam( DDTeamCollection* self, GetTeamRequest req ) {
		try {
			Void _ = wait( self->checkBuildTeams( self ) );

			// Select the best team
			// Currently the metric is minimum used disk space (adjusted for data in flight)
			// Only healthy teams may be selected. The team has to be healthy at the moment we update
			//   shardsAffectedByTeamFailure or we could be dropping a shard on the floor (since team
			//   tracking is "edge triggered")
			// SOMEDAY: Account for capacity, load (when shardMetrics load is high)

			int64_t bestLoadBytes = 0;
			Optional<Reference<IDataDistributionTeam>> bestOption;
			std::vector<std::pair<int, Reference<IDataDistributionTeam>>> randomTeams;

			if( !req.wantsNewServers ) {
				std::set< UID > sources;
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

			ASSERT( self->teams.size() );

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
			if( teams[i]->servers.size() > teamSize ) {
				auto& serverIds = teams[i]->getServerIDs();
				bool foundTeam = false;
				for( int j = 0; j < std::max( 1, (int)(serverIds.size() - teamSize + 1) ) && !foundTeam; j++ ) {
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
					addTeam(serverIds.begin(), serverIds.begin() + teamSize );
				}
			}
		}
	}

	void init( InitialDataDistribution const& initTeams ) {
		// SOMEDAY: If some servers have teams and not others (or some servers have more data than others) and there is an address/locality collision, should
		// we preferentially mark the least used server as undesirable?
		for(auto i = initTeams.allServers.begin(); i != initTeams.allServers.end(); ++i)
			addServer( i->first, i->second, serverTrackerErrorOut );

		for(auto t = initTeams.teams.begin(); t != initTeams.teams.end(); ++t) {
			addTeam(t->begin(), t->end() );
		}

		addSubsetOfEmergencyTeams();
	}

	void evaluateTeamQuality() {
		int teamCount = teams.size(), serverCount = allServers.size();
		double teamsPerServer = (double)teamCount * teamSize / serverCount;

		ASSERT( serverCount == server_info.size() );

		int minTeams = 100000, maxTeams = 0;
		double varTeams = 0;

		std::map<Optional<Standalone<StringRef>>, int> machineTeams;
		for(auto s = server_info.begin(); s != server_info.end(); ++s) {
			if(!server_status.get(s->first).isUndesired) {
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
	}

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

	void addTeam( std::set<UID> const& team ) {
		addTeam(team.begin(), team.end());
	}

	template<class InputIt>
	void addTeam( InputIt begin, InputIt end) {
		vector< Reference<TCServerInfo> > newTeamServers;
		for(auto i = begin; i != end; ++i) {
			if(server_info[*i]) {
				newTeamServers.push_back( server_info[ *i ] );
			}
			else {
				TraceEvent(SevError, "DDTeamCollection_addedTeamNotInCollection");
			}
		}
		Reference<TCTeamInfo> teamInfo( new TCTeamInfo( newTeamServers ) );
		TraceEvent("TeamCreation", masterId).detail("Team", teamInfo->getDesc());
		teamInfo->tracker = teamTracker( this, teamInfo );
		teams.push_back( teamInfo );
		for (int i=0;i<newTeamServers.size();i++) {
			server_info[ newTeamServers[i]->id ]->teams.push_back( teamInfo );
		}
	}

	ACTOR Future<Void> addAllTeams( DDTeamCollection *self, int location, vector<LocalityEntry>* history, Reference<LocalityMap<UID>> processes, vector<std::vector<UID>>* output, int teamLimit, int* addedTeams ) {
		Void _ = wait( yield( TaskDataDistributionLaunch ) );

		// Add team, if valid
		if(history->size() == self->teamSize) {
			auto valid = self->replicationPolicy->validate(*history, processes);
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
		return addedTeams;
	}

	int addTeamsBestOf( int teamsToBuild ) {
		int addedTeams = 0;

		LocalityMap<UID> totalServers;

		for(auto i = server_info.begin(); i != server_info.end(); ++i) {
			if (!server_status.get(i->first).isUndesired) {
				auto& id = i->first;
				auto& locality = i->second->lastKnownInterface.locality;
				totalServers.add(locality, &id);
			}
		}

		if(totalServers.size() < teamSize ) {
			TraceEvent(SevWarn, "DataDistributionBuildTeams", masterId).detail("Reason","Not enough servers for a team").detail("Servers",totalServers.size()).detail("teamSize", teamSize);
			return addedTeams;
		}

		int loopCount = 0;
		// add teams
		while( addedTeams < teamsToBuild ) {
			std::vector<LocalityEntry> leastUsedServers;
			int minTeamCount = CLIENT_KNOBS->TOO_MANY;
			for(int i = 0; i < totalServers.size(); i++) {
				LocalityEntry process = totalServers.getEntry(i);
				UID id = *totalServers.getObject(process);
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
				forcedAttributes.push_back(g_random->randomChoice(leastUsedServers));
			}

			std::vector<UID*> bestTeam;
			int bestScore = CLIENT_KNOBS->TOO_MANY;

			int maxAttempts = SERVER_KNOBS->BEST_OF_AMT;
			for( int i = 0; i < maxAttempts && i < 100; i++) {
				team.clear();
				auto success = totalServers.selectReplicas(replicationPolicy, forcedAttributes, team);
				if(!success) {
					break;
				}

				if(forcedAttributes.size() > 0) {
					team.push_back((UID*)totalServers.getObject(forcedAttributes[0]));
				}
				if( team.size() != teamSize) {
					maxAttempts += 1;
				}

				int score = 0;
				for(auto process = team.begin(); process != team.end(); process++) {
					score += server_info[**process]->teams.size();
				}

				if(score < bestScore) {
					bestTeam = team;
					bestScore = score;
				}
			}

			if( bestTeam.size() == teamSize) {
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
			if(++loopCount > 2*teamsToBuild*(teamSize+1) ) {
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
	ACTOR Future<Void> buildTeams( DDTeamCollection* self ) {
		state int desiredTeams;
		int serverCount = 0;
		int uniqueDataCenters = 0;
		int uniqueMachines = 0;
		std::set<Optional<Standalone<StringRef>>> machines;

		for(auto i = self->server_info.begin(); i != self->server_info.end(); ++i) {
			if (!self->server_status.get(i->first).isUndesired) {
				++serverCount;
				LocalityData& serverLocation = i->second->lastKnownInterface.locality;
				machines.insert( serverLocation.zoneId() );
			}
		}
		uniqueMachines = machines.size();

		// If there are too few machines to even build teams or there are too few represented datacenters, build no new teams
		if( uniqueMachines >= self->teamSize ) {
			desiredTeams = SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER*serverCount;

			// Count only properly sized teams against the desired number of teams. This is to prevent "emergency" merged teams (see MoveKeys)
			//  from overwhelming the team count (since we really did not want that team in the first place). These larger teams will not be
			//  returned from getRandomTeam() (as used by bestTeam to find a new home for a shard).
			// Also exclude teams who have members in the wrong configuration, since we don't want these teams either
			int teamCount = 0;
			for(int i = 0; i < self->teams.size(); i++) {
				if( self->teams[i]->getServerIDs().size() == self->teamSize && !self->teams[i]->isWrongConfiguration() ) {
					teamCount++;
				}
			}

			TraceEvent("BuildTeamsBegin", self->masterId).detail("DesiredTeams", desiredTeams).detail("UniqueMachines", uniqueMachines)
				.detail("TeamSize", self->teamSize).detail("Servers", serverCount)
				.detail("CurrentTrackedTeams", self->teams.size()).detail("TeamCount", teamCount);

			if( desiredTeams > teamCount ) {
				std::set<UID> desiredServerSet;
				for(auto i = self->server_info.begin(); i != self->server_info.end(); ++i)
					if (!self->server_status.get(i->first).isUndesired)
						desiredServerSet.insert(i->second->id);

				vector<UID> desiredServerVector( desiredServerSet.begin(), desiredServerSet.end() );

				state int teamsToBuild = desiredTeams - teamCount;

				state vector<std::vector<UID>> builtTeams;

				if (self->teamSize <= 3) {
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
				} else {
					int addedTeams = self->addTeamsBestOf( teamsToBuild );
					TraceEvent("AddTeamsBestOf", self->masterId).detail("CurrentTeams", self->teams.size()).detail("AddedTeams", addedTeams);
				}
			}
		}

		self->evaluateTeamQuality();

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

	void addServer( StorageServerInterface newServer, ProcessClass processClass, Promise<Void> errorOut ) {
		allServers.push_back( newServer.id() );

		TraceEvent("AddedStorageServer", masterId).detail("ServerID", newServer.id()).detail("ProcessClass", processClass.toString()).detail("WaitFailureToken", newServer.waitFailure.getEndpoint().token).detail("address", newServer.waitFailure.getEndpoint().address);
		auto &r = server_info[newServer.id()] = Reference<TCServerInfo>( new TCServerInfo( newServer, processClass ) );
		r->tracker = storageServerTracker( this, cx, r.getPtr(), &server_status, lock, masterId, &server_info, serverChanges, serverFailures, errorOut );
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
	state bool wrongSize = team->getServerIDs().size() != self->teamSize;
	state bool lastReady = self->initialFailureReactionDelay.isReady();
	state bool lastHealthy = team->isHealthy();
	state bool lastOptimal = team->isOptimal();
	state bool lastWrongConfiguration = team->isWrongConfiguration();

	if(lastHealthy) {
		self->healthyTeamCount++;

		if(lastOptimal) {
			self->optimalTeamCount++;
			if( self->optimalTeamCount == 1 )
				self->optimalTeamChange.send(Void());
		}
	}

	TraceEvent("TeamTrackerStarting", self->masterId).detail("Reason", "Initial wait complete (sc)").detail("Team", team->getDesc());

	try {
		loop {
			TraceEvent("TeamHealthChangeDetected", self->masterId).detail("isReady", self->initialFailureReactionDelay.isReady() );
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
			bool matchesPolicy = self->replicationPolicy->validate(teamLocality->getEntries(), teamLocality);

			if( !self->initialFailureReactionDelay.isReady() )
				change.push_back( self->initialFailureReactionDelay );

			bool recheck = lastReady != self->initialFailureReactionDelay.isReady() && ( !matchesPolicy || anyUndesired || team->getServerIDs().size() != self->teamSize );
			lastReady = self->initialFailureReactionDelay.isReady();

			if( serversLeft != lastServersLeft || anyUndesired != lastAnyUndesired || anyWrongConfiguration != lastWrongConfiguration || wrongSize || recheck ) {
				TraceEvent("TeamHealthChanged", self->masterId)
					.detail("Team", team->getDesc()).detail("serversLeft", serversLeft)
					.detail("lastServersLeft", lastServersLeft).detail("ContainsUndesiredServer", anyUndesired)
					.detail("HealthyTeamsCount", self->healthyTeamCount).detail("IsWrongConfiguration", anyWrongConfiguration);

				bool healthy = matchesPolicy && !anyUndesired && team->getServerIDs().size() == self->teamSize && team->getServerIDs().size() == serversLeft;
				team->setHealthy( healthy );	// Unhealthy teams won't be chosen by bestTeam

				team->setWrongConfiguration( anyWrongConfiguration );

				bool optimal = team->isOptimal();
				int lastOptimalCount = self->optimalTeamCount;
				if( optimal != lastOptimal ) {
					lastOptimal = optimal;
					if( lastHealthy )
						self->optimalTeamCount += lastOptimal ? 1 : -1;
				}

				if( lastHealthy != healthy ) {
					lastHealthy = healthy;
					self->healthyTeamCount += healthy ? 1 : -1;

					ASSERT( self->healthyTeamCount >= 0 );

					if( self->healthyTeamCount == 0 ) {
						TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->masterId)
							.detail("SignallingTeam", team->getDesc());
						self->zeroHealthyTeams.send( Void() );
					}

					if( lastOptimal )
						self->optimalTeamCount += lastHealthy ? 1 : -1;

					TraceEvent("TeamHealthDifference", self->masterId)
						.detail("LastOptimal", lastOptimal)
						.detail("LastHealthy", lastHealthy)
						.detail("Optimal", optimal)
						.detail("OptimalTeamCount", self->optimalTeamCount);
				}

				if( lastOptimalCount != self->optimalTeamCount && ( self->optimalTeamCount == 0 || self->optimalTeamCount == 1 ) ) {
					TraceEvent("OptimalTeamsChanging", self->masterId);
					self->optimalTeamChange.send( Void() );
				}

				lastServersLeft = serversLeft;
				lastAnyUndesired = anyUndesired;
				lastWrongConfiguration = anyWrongConfiguration;
				wrongSize = false;

				state int lastPriority = team->getPriority();
				if( serversLeft < self->teamSize ) {
					if( serversLeft == 0 )
						team->setPriority( PRIORITY_TEAM_0_LEFT );
					else if( serversLeft == 1 )
						team->setPriority( PRIORITY_TEAM_1_LEFT );
					else if( serversLeft == 2 )
						team->setPriority( PRIORITY_TEAM_2_LEFT );
					else
						team->setPriority( PRIORITY_TEAM_UNHEALTHY );
				}
				else if ( team->getServerIDs().size() != self->teamSize )
					team->setPriority( PRIORITY_TEAM_UNHEALTHY );
				else if( anyUndesired )
					team->setPriority( PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER );
				else
					team->setPriority( PRIORITY_TEAM_HEALTHY );
				TraceEvent("TeamPriorityChange", self->masterId).detail("Priority", team->getPriority());

				if( self->initialFailureReactionDelay.isReady() ) {
					vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor( team->getServerIDs() );

					for(int i=0; i<shards.size(); i++) {
						int maxPriority = team->getPriority();
						auto teams = self->shardsAffectedByTeamFailure->getTeamsFor( shards[i] );
						for( int t=0; t<teams.size(); t++) {
							ASSERT( teams[t].size() );

							if( self->server_info.count( teams[t][0] ) ) {
								auto& info = self->server_info[teams[t][0]];

								bool found = false;
								for( int i = 0; i < info->teams.size(); i++ ) {
									if( info->teams[i]->serverIDs == teams[t] ) {
										maxPriority = std::max( maxPriority, info->teams[i]->getPriority() );
										found = true;
										break;
									}
								}

								TEST(!found); // A removed team is still associated with a shard in SABTF
							} else {
								TEST(true); // A removed server is still associated with a team in SABTF
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
					TraceEvent("TeamHealthNotReady", self->masterId);
				}
			}

			// Wait for any of the machines to change status
			Void _ = wait( quorum( change, 1 ) );
			Void _ = wait( yield() );
		}
	} catch(Error& e) {
		if( team->isHealthy() ) {
			self->healthyTeamCount--;
			ASSERT( self->healthyTeamCount >= 0 );

			if( self->healthyTeamCount == 0 ) {
				TraceEvent(SevWarn, "ZeroTeamsHealthySignalling", self->masterId).detail("SignallingTeam", team->getDesc());
				self->zeroHealthyTeams.send( Void() );
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
						if( self->server_info.count( serverId ) ) {
							auto& serverInfo = self->server_info[ serverId ];
							if (ssi.getValue.getEndpoint() != serverInfo->lastKnownInterface.getValue.getEndpoint() || processClass != serverInfo->lastKnownClass.classType()) {
								Promise<std::pair<StorageServerInterface, ProcessClass>> currentInterfaceChanged = serverInfo->interfaceChanged;
								serverInfo->interfaceChanged = Promise<std::pair<StorageServerInterface, ProcessClass>>();
								serverInfo->onInterfaceChanged = Future<std::pair<StorageServerInterface, ProcessClass>>( serverInfo->interfaceChanged.getFuture() );
								currentInterfaceChanged.send( std::make_pair(ssi,processClass) );
							}
						} else if( !self->recruitingIds.count(ssi.id()) ) {
							self->addServer( ssi, processClass, self->serverTrackerErrorOut );
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
	if(type == self->storeType)
		Void _ = wait(Never());

	return type;
}

ACTOR Future<Void> storageServerTracker(
	DDTeamCollection *self,
	Database cx,
	TCServerInfo *server, //This actor is owned by this TCServerInfo
	ServerStatusMap *statusMap,
	MoveKeysLock lock,
	UID masterId,
	std::map<UID, Reference<TCServerInfo>>* other_servers,
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > changes,
	PromiseStream<Void> serverFailures,
	Promise<Void> errorOut)
{
	state Future<Void> failureTracker;
	state ServerStatus status( false, false, server->lastKnownInterface.locality );
	state bool lastIsUndesired = false;
	state Future<Void> metricsTracker = serverMetricsPolling( server );
	state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged = server->onInterfaceChanged;

	state Future<KeyValueStoreType> storeTracker = keyValueStoreTypeTracker( self, server );
	state bool hasWrongStoreType = false;

	changes.send( std::make_pair(server->id, server->lastKnownInterface) );

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
					otherChanges.push_back( statusMap->onChange( i->second->id ) );
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
				otherChanges.push_back( self->onOptimalTeamChange.getFuture() );
			}

			//If this storage server has the wrong key-value store type, then mark it undesired so it will be replaced with a server having the correct type
			if(hasWrongStoreType) {
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

			failureTracker = storageServerFailureTracker( cx, server->lastKnownInterface, statusMap, &status, serverFailures, &self->unhealthyServers, masterId );

			//We need to recruit new storage servers if the key value store type has changed
			if(hasWrongStoreType)
				self->restartRecruiting.trigger();

			if( lastIsUndesired && !status.isUndesired )
				self->doBuildTeams = true;
			lastIsUndesired = status.isUndesired;

			choose {
				when( Void _ = wait( failureTracker ) ) {
					// The server is failed AND all data has been removed from it, so permanently remove it.
					TraceEvent("StatusMapChange", masterId).detail("ServerID", server->id).detail("Status", "Removing");
					changes.send( std::make_pair(server->id, Optional<StorageServerInterface>()) );

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
					changes.send( std::make_pair(server->id, server->lastKnownInterface) );
					// We rely on the old failureTracker being actorCancelled since the old actor now has a pointer to an invalid location
					status = ServerStatus( status.isFailed, status.isUndesired, server->lastKnownInterface.locality );

					//Restart the storeTracker for the new interface
					storeTracker = keyValueStoreTypeTracker(self, server);
					hasWrongStoreType = false;
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
						.detail("DesiredType", self->storeType.toString());
					TEST(true); //KeyValueStore type changed

					storeTracker = Never();
					hasWrongStoreType = true;
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
	isr.storeType = self->storeType;
	isr.seedTag = invalidTag;
	isr.reqId = g_random->randomUniqueID();
	isr.interfaceId = interfaceId;

	TraceEvent("DDRecruiting").detail("State", "Sending request to worker").detail("WorkerID", candidateWorker.worker.id())
		.detail("WorkerLocality", candidateWorker.worker.locality.toString()).detail("interf", interfaceId).detail("addr", candidateWorker.worker.address());

	self->recruitingIds.insert(interfaceId);
	self->recruitingLocalities.insert(candidateWorker.worker.address());
	ErrorOr<StorageServerInterface> newServer = wait( candidateWorker.worker.storage.tryGetReply( isr, TaskDataDistribution ) );
	self->recruitingIds.erase(interfaceId);
	self->recruitingLocalities.erase(candidateWorker.worker.address());

	self->recruitingStream.set(self->recruitingStream.get()-1);

	TraceEvent("DDRecruiting").detail("State", "Finished request").detail("WorkerID", candidateWorker.worker.id())
		.detail("WorkerLocality", candidateWorker.worker.locality.toString()).detail("interf", interfaceId).detail("addr", candidateWorker.worker.address());

	if( newServer.isError() ) {
		TraceEvent(SevWarn, "DDRecruitmentError").error(newServer.getError());
		if( !newServer.isError( error_code_recruitment_failed ) && !newServer.isError( error_code_request_maybe_delivered ) )
			throw newServer.getError();
		Void _ = wait( delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskDataDistribution) );
	}
	else if( newServer.present() ) {
		if( !self->server_info.count( newServer.get().id() ) )
			self->addServer( newServer.get(), candidateWorker.processClass, self->serverTrackerErrorOut );
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
		} catch( Error &e ) {
			if(e.code() != error_code_timed_out) {
				throw;
			}
			TEST(true); //Storage recruitment timed out
		}
	}
}

// Keep track of servers and teams -- serves requests for getRandomTeam
ACTOR Future<Void> dataDistributionTeamCollection(
	Reference<InitialDataDistribution> initData,
	TeamCollectionInterface tci,
	Database cx,
	Reference<AsyncVar<struct ServerDBInfo>> db,
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
	MoveKeysLock lock,
	PromiseStream<RelocateShard> output,
	UID masterId, int teamSize,
	IRepPolicyRef replicationPolicy,
	KeyValueStoreType storeType,
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges,
	Future<Void> readyToStart )
{
	state DDTeamCollection self( cx, masterId, lock, output, shardsAffectedByTeamFailure, teamSize, replicationPolicy, storeType, serverChanges, readyToStart );

	state Future<Void> loggingTrigger = Void();
	state PromiseStream<Void> serverRemoved;
	state Future<Void> interfaceChanges;
	state Future<Void> error = actorCollection( self.addActor.getFuture() );
	state Future<Void> storageServerRecruitment;
	state Future<Void> storageServerRecruitmentMonitor;
	state Future<Void> trackExcluded;

	Void _ = wait( readyToStart );

	try {
		self.init( *initData );
		initData = Reference<InitialDataDistribution>();
		storageServerRecruitment = storageRecruiter( &self, db );
		storageServerRecruitmentMonitor = monitorStorageServerRecruitment( &self );
		interfaceChanges = waitServerListChange( &self, cx, serverRemoved.getFuture() );
		trackExcluded = trackExcludedServers( &self, cx );

		// SOMEDAY: Monitor FF/serverList for (new) servers that aren't in allServers and add or remove them

		loop choose {

			when( GetTeamRequest req = waitNext(tci.getTeam.getFuture()) ) {
				self.addActor.send( self.getTeam( &self, req ) );
			}
			when( UID removedServer = waitNext( self.removedServers.getFuture() ) ) {
				TEST(true);  // Storage server removed from database
				self.removeServer( removedServer );
				serverRemoved.send( Void() );

				self.restartRecruiting.trigger();
			}
			when( Void _ = waitNext( self.zeroHealthyTeams.getFuture() ) ) {
				self.restartRecruiting.trigger();
				self.noHealthyTeams();
			}
			when( Void _ = waitNext( self.optimalTeamChange.getFuture() ) ) {
				Promise<Void> oldChange = self.onOptimalTeamChange;
				self.onOptimalTeamChange = Promise<Void>();
				oldChange.send(Void());
			}
			when( Void _ = waitNext( self.serverFailures.getFuture() ) ) {
				self.restartRecruiting.trigger();
			}
			when( Void _ = wait( loggingTrigger ) ) {
				TraceEvent("TotalDataInFlight", masterId).detail("TotalBytes", self.getDebugTotalDataInFlight()).detail("UnhealthyServers", self.unhealthyServers).trackLatest(
					(cx->dbName.toString() + "/TotalDataInFlight").c_str());
				loggingTrigger = delay( SERVER_KNOBS->DATA_DISTRIBUTION_LOGGING_INTERVAL );
				self.countHealthyTeams();
			}
			when( Void _ = wait( self.serverTrackerErrorOut.getFuture() ) ) {} // Propagate errors from storageServerTracker
			when( Void _ = wait( interfaceChanges ) ) {}
			when( Void _ = wait( trackExcluded ) ) {}
			when( Void _ = wait( error ) ) {}
			when( Void _ = wait( storageServerRecruitment ) ) {}
		}
	} catch (Error& e) {
		if (e.code() != error_code_movekeys_conflict)
			TraceEvent(SevError, "dataDistributionTeamCollectionError", masterId).error(e);
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

ACTOR Future<Void> popOldTags( Database cx, Reference<ILogSystem> logSystem, Version recoveryCommitVersion ) {
	state Transaction tr(cx);

	if( recoveryCommitVersion == 1 )
		return Void();

	loop {
		try {
			Optional<Standalone<StringRef>> val = wait( tr.get(serverTagMaxKey) );
			if(!val.present())
				return Void();

			state Tag maxTag = decodeServerTagMaxValue( val.get() );

			Standalone<RangeResultRef> tags = wait( tr.getRange( serverTagKeys, CLIENT_KNOBS->TOO_MANY, true) );

			std::set<Tag> unusedTags;
			for(int i = 0; i <= maxTag; i++)
				unusedTags.insert(i);

			for(auto kv : tags)
				unusedTags.erase(decodeServerTagValue( kv.value ));

			for(auto tag : unusedTags)
				logSystem->pop(recoveryCommitVersion, tag);

			return Void();
		} catch( Error &e ) {
			Void _ = wait( tr.onError(e) );
		}
	}
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
		double* lastLimited)
{
	state Database cx = openDBOnServer(db, TaskDataDistributionLaunch, true, true);
	cx->locationCacheSize = SERVER_KNOBS->DD_LOCATION_CACHE_SIZE;

	state Transaction trVer(cx);
	loop {
		try {
			trVer.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			trVer.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );

			if( !g_network->isSimulated() ) {
				UID id(g_random->randomUniqueID());
				TraceEvent("UpgradeProcessClassTransaction", mi.id())
					.detail("TransactionUID", id);
				trVer.debugTransaction( id );
			}

			Optional<Value> val = wait(trVer.get(processClassVersionKey));

			if (val.present())
				break;

			Standalone<RangeResultRef> processClasses = wait( trVer.getRange( processClassKeys, CLIENT_KNOBS->TOO_MANY ) );
			ASSERT( !processClasses.more && processClasses.size() < CLIENT_KNOBS->TOO_MANY );

			trVer.clear(processClassKeys);
			trVer.set(processClassVersionKey, processClassVersionValue);
			for (auto it : processClasses) {
				UID processUid = decodeProcessClassKeyOld(it.key);
				trVer.set(processClassKeyFor(processUid.toString()), it.value);
			}

			Void _ = wait(trVer.commit());
			TraceEvent("ProcessClassUpgrade");
			break;
		}
		catch(Error &e) {
			Void _ = wait( trVer.onError(e) );
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
				state Reference<InitialDataDistribution> initData = wait( getInitialDataDistribution(cx, mi.id(), lock) );
				if(initData->shards.size() > 1) {
					TraceEvent("DDInitGotInitialDD", mi.id()).detail("b", printable(initData->shards.end()[-2].begin)).detail("e", printable(initData->shards.end()[-2].end)).detail("src", describe(initData->shards.end()[-2].value.first)).detail("dest", describe(initData->shards.end()[-2].value.second)).trackLatest("InitialDD");
				} else {
					TraceEvent("DDInitGotInitialDD", mi.id()).detail("b","").detail("e", "").detail("src", "[no items]").detail("dest", "[no items]").trackLatest("InitialDD");
				}

				if (initData->mode) break;
				TraceEvent("DataDistributionDisabled", mi.id());

				TraceEvent("MovingData", mi.id())
					.detail( "InFlight", 0 )
					.detail( "InQueue", 0 )
					.detail( "AverageShardSize", -1 )
					.detail( "LowPriorityRelocations", 0 )
					.detail( "HighPriorityRelocations", 0 )
					.detail( "HighestPriority", 0 )
					.trackLatest( format("%s/MovingData", printable(cx->dbName).c_str() ).c_str() );

				TraceEvent("TotalDataInFlight", mi.id()).detail("TotalBytes", 0)
					.trackLatest((cx->dbName.toString() + "/TotalDataInFlight").c_str());

				Void _ = wait( waitForDataDistributionEnabled(cx) );
				TraceEvent("DataDistributionEnabled");
			}

			// When/If this assertion fails, Evan owes Ben a pat on the back for his foresight
			ASSERT(configuration.storageTeamSize > 0);

			state PromiseStream<RelocateShard> output;
			state PromiseStream<Promise<int64_t>> getAverageShardBytes;
			state PromiseStream<GetMetricsRequest> getShardMetrics;
			state Promise<Void> readyToStart;
			TeamCollectionInterface tci;
			Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure( new ShardsAffectedByTeamFailure );
			vector<Future<Void>> actors;

			actors.push_back( pollMoveKeysLock(cx, lock) );
			actors.push_back( popOldTags( cx, logSystem, recoveryCommitVersion) );
			actors.push_back( reportErrorsExcept( dataDistributionTracker( initData, cx, shardsAffectedByTeamFailure, output, getShardMetrics, getAverageShardBytes.getFuture(), readyToStart, mi.id() ), "DDTracker", mi.id(), &normalDDQueueErrors() ) );
			actors.push_back( reportErrorsExcept( dataDistributionQueue( cx, output, getShardMetrics, tci, shardsAffectedByTeamFailure, lock, getAverageShardBytes, mi, configuration.storageTeamSize, configuration.durableStorageQuorum, lastLimited ), "DDQueue", mi.id(), &normalDDQueueErrors() ) );
			actors.push_back( reportErrorsExcept( dataDistributionTeamCollection( initData, tci, cx, db, shardsAffectedByTeamFailure, lock, output, mi.id(), configuration.storageTeamSize, configuration.storagePolicy, configuration.storageServerStoreType, serverChanges, readyToStart.getFuture() ), "DDTeamCollection", mi.id(), &normalDDQueueErrors() ) );

			Void _ = wait( waitForAll( actors ) );
			return Void();
		}
		catch( Error &e ) {
			state Error err = e;
			if( e.code() != error_code_movekeys_conflict )
				throw err;
			bool ddEnabled = wait( isDataDistributionEnabled(cx) );
			TraceEvent("DataDistributionMoveKeysConflict").detail("ddEnabled", ddEnabled);
			if( ddEnabled )
				throw err;
		}
	}
}

DDTeamCollection* testTeamCollection(int teamSize, IRepPolicyRef policy, int processCount) {
	Database database = DatabaseContext::create(
		Reference<AsyncVar<ClientDBInfo>>(new AsyncVar<ClientDBInfo>()),
		Never(),
		LocalityData(),
		false
	);

	DDTeamCollection* collection = new DDTeamCollection(
		database,
		UID(0, 0),
		MoveKeysLock(),
		PromiseStream<RelocateShard>(),
		Reference<ShardsAffectedByTeamFailure>(new ShardsAffectedByTeamFailure()),
		teamSize,
		policy,
		KeyValueStoreType(),
		PromiseStream<std::pair<UID, Optional<StorageServerInterface>>>(),
		Future<Void>(Void())
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
	}

	return collection;
}

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
