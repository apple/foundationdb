/*
 * TagPartitionedLogSystem.actor.cpp
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
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/WaitFailure.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/RecoveryState.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Version> minVersionWhenReady( Future<Void> f, std::vector<Future<Version>> replies) {
	wait(f);
	Version minVersion = std::numeric_limits<Version>::max();
	for(auto& reply : replies) {
		if(reply.isReady() && !reply.isError()) {
			minVersion = std::min(minVersion, reply.get());
		}
	}
	return minVersion;
}

struct OldLogData {
	std::vector<Reference<LogSet>> tLogs;
	int32_t logRouterTags;
	Version epochEnd;

	OldLogData() : epochEnd(0), logRouterTags(0) {}
};

struct LogLockInfo {
	Version epochEnd;
	bool isCurrent;
	Reference<LogSet> logSet;
	std::vector<Future<TLogLockResult>> replies;

	LogLockInfo() : epochEnd(std::numeric_limits<Version>::max()), isCurrent(false) {}
};

struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
	UID dbgid;
	int logSystemType;
	std::vector<Reference<LogSet>> tLogs;
	int expectedLogSets;
	int logRouterTags;
	UID recruitmentID;
	int repopulateRegionAntiQuorum;
	bool stopped;

	// new members
	Future<Void> rejoins;
	Future<Void> recoveryComplete;
	Future<Void> remoteRecovery;
	Future<Void> remoteRecoveryComplete;
	std::vector<LogLockInfo> lockResults;
	AsyncVar<bool> recoveryCompleteWrittenToCoreState;
	bool remoteLogsWrittenToCoreState;
	bool hasRemoteServers;

	Optional<Version> recoverAt;
	Optional<Version> recoveredAt;
	Version knownCommittedVersion;
	LocalityData locality;
	std::map< std::pair<UID, Tag>, std::pair<Version, Version> > outstandingPops;  // For each currently running popFromLog actor, (log server #, tag)->popped version
	Optional<PromiseStream<Future<Void>>> addActor;
	ActorCollection popActors;
	std::vector<OldLogData> oldLogData;
	AsyncTrigger logSystemConfigChanged;

	TagPartitionedLogSystem( UID dbgid, LocalityData locality, Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>() ) : dbgid(dbgid), locality(locality), addActor(addActor), popActors(false), recoveryCompleteWrittenToCoreState(false), remoteLogsWrittenToCoreState(false), logSystemType(0), logRouterTags(0), expectedLogSets(0), hasRemoteServers(false), stopped(false), repopulateRegionAntiQuorum(0) {}

	virtual void stopRejoins() {
		rejoins = Future<Void>();
	}

	virtual void addref() {
		ReferenceCounted<TagPartitionedLogSystem>::addref();
	}

	virtual void delref() {
		ReferenceCounted<TagPartitionedLogSystem>::delref();
	}

	virtual std::string describe() {
		std::string result;
		for( int i = 0; i < tLogs.size(); i++ ) {
			result += format("%d: ", i);
			for( int j = 0; j < tLogs[i]->logServers.size(); j++) {
				result += tLogs[i]->logServers[j]->get().id().toString() + ((j == tLogs[i]->logServers.size() - 1) ? " " : ", ");
			}
		}
		return result;
	}

	virtual UID getDebugID() {
		return dbgid;
	}

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality, bool forceRecovery) {
		return epochEnd( outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery );
	}

	static Reference<ILogSystem> fromLogSystemConfig( UID const& dbgid, LocalityData const& locality, LogSystemConfig const& lsConf, bool excludeRemote, bool useRecoveredAt, Optional<PromiseStream<Future<Void>>> addActor ) {
		ASSERT( lsConf.logSystemType == 2 || (lsConf.logSystemType == 0 && !lsConf.tLogs.size()) );
		//ASSERT(lsConf.epoch == epoch);  //< FIXME
		Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality, addActor) );

		logSystem->tLogs.reserve(lsConf.tLogs.size());
		logSystem->expectedLogSets = lsConf.expectedLogSets;
		logSystem->logRouterTags = lsConf.logRouterTags;
		logSystem->recruitmentID = lsConf.recruitmentID;
		logSystem->stopped = lsConf.stopped;
		if(useRecoveredAt) {
			logSystem->recoveredAt = lsConf.recoveredAt;
		}
		for( int i = 0; i < lsConf.tLogs.size(); i++ ) {
			TLogSet const& tLogSet = lsConf.tLogs[i];
			if(!excludeRemote || tLogSet.isLocal) {
				Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
				logSystem->tLogs.push_back( logSet );
				for( auto& log : tLogSet.tLogs) {
					logSet->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				for( auto& log : tLogSet.logRouters) {
					logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				logSet->tLogWriteAntiQuorum = tLogSet.tLogWriteAntiQuorum;
				logSet->tLogReplicationFactor = tLogSet.tLogReplicationFactor;
				logSet->tLogPolicy = tLogSet.tLogPolicy;
				logSet->tLogLocalities = tLogSet.tLogLocalities;
				logSet->isLocal = tLogSet.isLocal;
				logSet->locality = tLogSet.locality;
				logSet->startVersion = tLogSet.startVersion;
				logSet->satelliteTagLocations = tLogSet.satelliteTagLocations;
				filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
				logSet->updateLocalitySet(logSet->tLogLocalities);
			}
		}

		logSystem->oldLogData.resize(lsConf.oldTLogs.size());
		for( int i = 0; i < lsConf.oldTLogs.size(); i++ ) {
			logSystem->oldLogData[i].tLogs.resize(lsConf.oldTLogs[i].tLogs.size());
			for( int j = 0; j < lsConf.oldTLogs[i].tLogs.size(); j++ ) {
				Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
				logSystem->oldLogData[i].tLogs[j] = logSet;
				TLogSet const& tLogData = lsConf.oldTLogs[i].tLogs[j];
				for( auto & log : tLogData.tLogs) {
					logSet->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				for( auto & log : tLogData.logRouters) {
					logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				logSet->tLogWriteAntiQuorum = tLogData.tLogWriteAntiQuorum;
				logSet->tLogReplicationFactor = tLogData.tLogReplicationFactor;
				logSet->tLogPolicy = tLogData.tLogPolicy;
				logSet->tLogLocalities = tLogData.tLogLocalities;
				logSet->isLocal = tLogData.isLocal;
				logSet->locality = tLogData.locality;
				logSet->startVersion = tLogData.startVersion;
				logSet->satelliteTagLocations = tLogData.satelliteTagLocations;
				filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
				logSet->updateLocalitySet(logSet->tLogLocalities);
			}
			logSystem->oldLogData[i].logRouterTags = lsConf.oldTLogs[i].logRouterTags;
			logSystem->oldLogData[i].epochEnd = lsConf.oldTLogs[i].epochEnd;
		}

		logSystem->logSystemType = lsConf.logSystemType;
		return logSystem;
	}

	static Reference<ILogSystem> fromOldLogSystemConfig( UID const& dbgid, LocalityData const& locality, LogSystemConfig const& lsConf ) {
		ASSERT( lsConf.logSystemType == 2 || (lsConf.logSystemType == 0 && !lsConf.tLogs.size()) );
		//ASSERT(lsConf.epoch == epoch);  //< FIXME
		Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

		if(lsConf.oldTLogs.size()) {
			logSystem->tLogs.resize( lsConf.oldTLogs[0].tLogs.size());
			for( int i = 0; i < lsConf.oldTLogs[0].tLogs.size(); i++ ) {
				Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
				logSystem->tLogs[i] = logSet;
				TLogSet const& tLogSet = lsConf.oldTLogs[0].tLogs[i];
				for( auto & log : tLogSet.tLogs) {
					logSet->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				for( auto & log : tLogSet.logRouters) {
					logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				logSet->tLogWriteAntiQuorum = tLogSet.tLogWriteAntiQuorum;
				logSet->tLogReplicationFactor = tLogSet.tLogReplicationFactor;
				logSet->tLogPolicy = tLogSet.tLogPolicy;
				logSet->tLogLocalities = tLogSet.tLogLocalities;
				logSet->isLocal = tLogSet.isLocal;
				logSet->locality = tLogSet.locality;
				logSet->startVersion = tLogSet.startVersion;
				logSet->satelliteTagLocations = tLogSet.satelliteTagLocations;
				filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
				logSet->updateLocalitySet(logSet->tLogLocalities);
			}
			logSystem->logRouterTags = lsConf.oldTLogs[0].logRouterTags;
			//logSystem->epochEnd = lsConf.oldTLogs[0].epochEnd;

			logSystem->oldLogData.resize(lsConf.oldTLogs.size()-1);
			for( int i = 1; i < lsConf.oldTLogs.size(); i++ ) {
				logSystem->oldLogData[i-1].tLogs.resize(lsConf.oldTLogs[i].tLogs.size());
				for( int j = 0; j < lsConf.oldTLogs[i].tLogs.size(); j++ ) {
					Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
					logSystem->oldLogData[i-1].tLogs[j] = logSet;
					TLogSet const& tLogSet = lsConf.oldTLogs[i].tLogs[j];
					for( auto & log : tLogSet.tLogs) {
						logSet->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
					}
					for( auto & log : tLogSet.logRouters) {
						logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
					}
					logSet->tLogWriteAntiQuorum = tLogSet.tLogWriteAntiQuorum;
					logSet->tLogReplicationFactor = tLogSet.tLogReplicationFactor;
					logSet->tLogPolicy = tLogSet.tLogPolicy;
					logSet->tLogLocalities = tLogSet.tLogLocalities;
					logSet->isLocal = tLogSet.isLocal;
					logSet->locality = tLogSet.locality;
					logSet->startVersion = tLogSet.startVersion;
					logSet->satelliteTagLocations = tLogSet.satelliteTagLocations;
					filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
					logSet->updateLocalitySet(logSet->tLogLocalities);
				}
				logSystem->oldLogData[i-1].logRouterTags = lsConf.oldTLogs[i].logRouterTags;
				logSystem->oldLogData[i-1].epochEnd = lsConf.oldTLogs[i].epochEnd;
			}
		}
		logSystem->logSystemType = lsConf.logSystemType;
		logSystem->stopped = true;

		return logSystem;
	}

	virtual void toCoreState( DBCoreState& newState ) {
		if( recoveryComplete.isValid() && recoveryComplete.isError() )
			throw recoveryComplete.getError();

		if( remoteRecoveryComplete.isValid() && remoteRecoveryComplete.isError() )
			throw remoteRecoveryComplete.getError();

		newState.tLogs.clear();
		newState.logRouterTags = logRouterTags;
		for(auto &t : tLogs) {
			if(t->logServers.size()) {
				CoreTLogSet coreSet;
				for(auto &log : t->logServers) {
					coreSet.tLogs.push_back(log->get().id());
					coreSet.tLogLocalities.push_back(log->get().interf().locality);
				}
				coreSet.tLogWriteAntiQuorum = t->tLogWriteAntiQuorum;
				coreSet.tLogReplicationFactor = t->tLogReplicationFactor;
				coreSet.tLogPolicy = t->tLogPolicy;
				coreSet.isLocal = t->isLocal;
				coreSet.locality = t->locality;
				coreSet.startVersion = t->startVersion;
				coreSet.satelliteTagLocations = t->satelliteTagLocations;
				newState.tLogs.push_back(coreSet);
			}
		}

		newState.oldTLogData.clear();
		if(!recoveryComplete.isValid() || !recoveryComplete.isReady() || (repopulateRegionAntiQuorum == 0 && (!remoteRecoveryComplete.isValid() || !remoteRecoveryComplete.isReady()))) {
			newState.oldTLogData.resize(oldLogData.size());
			for(int i = 0; i < oldLogData.size(); i++) {
				for(auto &t : oldLogData[i].tLogs) {
					if(t->logServers.size()) {
						CoreTLogSet coreSet;
						for(auto &log : t->logServers) {
							coreSet.tLogs.push_back(log->get().id());
						}
						coreSet.tLogLocalities = t->tLogLocalities;
						coreSet.tLogWriteAntiQuorum = t->tLogWriteAntiQuorum;
						coreSet.tLogReplicationFactor = t->tLogReplicationFactor;
						coreSet.tLogPolicy = t->tLogPolicy;
						coreSet.isLocal = t->isLocal;
						coreSet.locality = t->locality;
						coreSet.startVersion = t->startVersion;
						coreSet.satelliteTagLocations = t->satelliteTagLocations;
						newState.oldTLogData[i].tLogs.push_back(coreSet);
					}
				}
				newState.oldTLogData[i].logRouterTags = oldLogData[i].logRouterTags;
				newState.oldTLogData[i].epochEnd = oldLogData[i].epochEnd;
			}
		}

		newState.logSystemType = logSystemType;
	}

	virtual bool remoteStorageRecovered() {
		return remoteRecoveryComplete.isValid() && remoteRecoveryComplete.isReady();
	}

	virtual Future<Void> onCoreStateChanged() {
		std::vector<Future<Void>> changes;
		changes.push_back(Never());
		if(recoveryComplete.isValid() && !recoveryComplete.isReady()) {
			changes.push_back(recoveryComplete);
		}
		if(remoteRecovery.isValid() && !remoteRecovery.isReady()) {
			changes.push_back(remoteRecovery);
		}
		if(remoteRecoveryComplete.isValid() && !remoteRecoveryComplete.isReady()) {
			changes.push_back(remoteRecoveryComplete);
		}
		return waitForAny(changes);
	}

	virtual void coreStateWritten( DBCoreState const& newState ) {
		if( !newState.oldTLogData.size() ) {
			recoveryCompleteWrittenToCoreState.set(true);
		}
		for(auto& t : newState.tLogs) {
			if(!t.isLocal) {
				TraceEvent("RemoteLogsWritten", dbgid);
				remoteLogsWrittenToCoreState = true;
				break;
			}
		}
	}

	virtual Future<Void> onError() {
		return onError_internal(this);
	}

	ACTOR static Future<Void> onError_internal( TagPartitionedLogSystem* self ) {
		// Never returns normally, but throws an error if the subsystem stops working
		loop {
			vector<Future<Void>> failed;
			vector<Future<Void>> changes;

			for(auto& it : self->tLogs) {
				for(auto &t : it->logServers) {
					if( t->get().present() ) {
						failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
					} else {
						changes.push_back(t->onChange());
					}
				}
				for(auto &t : it->logRouters) {
					if( t->get().present() ) {
						failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
					} else {
						changes.push_back(t->onChange());
					}
				}
			}

			if(!self->recoveryCompleteWrittenToCoreState.get()) {
				for(auto& old : self->oldLogData) {
					for(auto& it : old.tLogs) {
						for(auto &t : it->logRouters) {
							if( t->get().present() ) {
								failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
							} else {
								changes.push_back(t->onChange());
							}
						}
					}
				}
			}

			if(self->hasRemoteServers && ( !self->remoteRecovery.isReady() || self->remoteRecovery.isError() )) {
				changes.push_back(self->remoteRecovery);
			}

			changes.push_back(self->recoveryCompleteWrittenToCoreState.onChange());

			ASSERT( failed.size() >= 1 );
			wait( quorum(changes, 1) || tagError<Void>( quorum( failed, 1 ), master_tlog_failed() ) );
		}
	}

	virtual Future<Version> push( Version prevVersion, Version version, Version knownCommittedVersion, Version minKnownCommittedVersion, LogPushData& data, Optional<UID> debugID ) {
		// FIXME: Randomize request order as in LegacyLogSystem?
		vector<Future<Void>> quorumResults;
		vector<Future<Version>> allReplies;
		int location = 0;
		for(auto& it : tLogs) {
			if(it->isLocal && it->logServers.size()) {
				vector<Future<Void>> tLogCommitResults;
				for(int loc=0; loc< it->logServers.size(); loc++) {
					allReplies.push_back( it->logServers[loc]->get().interf().commit.getReply( TLogCommitRequest( data.getArena(), prevVersion, version, knownCommittedVersion, minKnownCommittedVersion, data.getMessages(location), debugID ), TaskTLogCommitReply ) );
					Future<Void> commitSuccess = success(allReplies.back());
					addActor.get().send(commitSuccess);
					tLogCommitResults.push_back(commitSuccess);
					location++;
				}
				quorumResults.push_back( quorum( tLogCommitResults, tLogCommitResults.size() - it->tLogWriteAntiQuorum ) );
			}
		}

		return minVersionWhenReady( waitForAll(quorumResults), allReplies);
	}

	Reference<IPeekCursor> peekAll( UID dbgid, Version begin, Version end, Tag tag, bool parallelGetMore ) {
		int bestSet = 0;
		std::vector<Reference<LogSet>> localSets;
		Version lastBegin = 0;
		bool foundSpecial = false;
		for(auto& log : tLogs) {
			if(log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
				foundSpecial = true;
			}
			if(log->isLocal && log->logServers.size() && (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded || log->locality == tag.locality ||
				tag == txsTag || tag.locality == tagLocalityLogRouter || (tag.locality == tagLocalityUpgraded && log->locality != tagLocalitySatellite))) {
				lastBegin = std::max(lastBegin, log->startVersion);
				localSets.push_back(log);
				if(log->locality != tagLocalitySatellite) {
					bestSet = localSets.size()-1;
				}
			}
		}

		if(!localSets.size()) {
			lastBegin = end;
		}

		if(begin >= lastBegin && localSets.size()) {
			TraceEvent("TLogPeekAllCurrentOnly", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("BestLogs", localSets[bestSet]->logServerString());
			return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( localSets, bestSet, localSets[bestSet]->bestLocationFor( tag ), tag, begin, end, parallelGetMore ) );
		} else {
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;

			if(lastBegin < end && localSets.size()) {
				TraceEvent("TLogPeekAllAddingCurrent", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("BestLogs", localSets[bestSet]->logServerString());
				cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( localSets, bestSet, localSets[bestSet]->bestLocationFor( tag ), tag, lastBegin, end, parallelGetMore)) );
			}
			int i = 0;
			while(begin < lastBegin) {
				if(i == oldLogData.size()) {
					if(tag == txsTag) {
						break;
					}
					TraceEvent("TLogPeekAllDead", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LastBegin", lastBegin).detail("OldLogDataSize", oldLogData.size());
					return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
				}

				int bestOldSet = 0;
				std::vector<Reference<LogSet>> localOldSets;
				Version thisBegin = begin;
				bool thisSpecial = false;
				for(auto& log : oldLogData[i].tLogs) {
					if(log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
						thisSpecial = true;
					}
					if(log->isLocal && log->logServers.size() && (log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded || log->locality == tag.locality ||
						tag == txsTag || tag.locality == tagLocalityLogRouter || (tag.locality == tagLocalityUpgraded && log->locality != tagLocalitySatellite))) {
						thisBegin = std::max(thisBegin, log->startVersion);
						localOldSets.push_back(log);
						if(log->locality != tagLocalitySatellite) {
							bestOldSet = localOldSets.size()-1;
						}
					}
				}

				if(!localOldSets.size()) {
					TraceEvent("TLogPeekAllNoLocalSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LastBegin", lastBegin);
					if(!cursors.size() && !foundSpecial) {
						i++;
						continue;
					}
					return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
				}
				if(thisSpecial) {
					foundSpecial = true;
				}

				if(thisBegin < lastBegin) {
					if(thisBegin < end) {
						TraceEvent("TLogPeekAllAddingOld", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("BestLogs", localOldSets[bestOldSet]->logServerString()).detail("LastBegin", lastBegin).detail("ThisBegin", thisBegin);
						cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( localOldSets, bestOldSet, localOldSets[bestOldSet]->bestLocationFor( tag ), tag, thisBegin, std::min(lastBegin, end), parallelGetMore)) );
						epochEnds.push_back(LogMessageVersion(std::min(lastBegin, end)));
					}
					lastBegin = thisBegin;
				}
				i++;
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	Reference<IPeekCursor> peekRemote( UID dbgid, Version begin, Tag tag, bool parallelGetMore ) {
		int bestSet = -1;
		Version lastBegin = recoveredAt.present() ? recoveredAt.get() + 1 : 0;
		for(int t = 0; t < tLogs.size(); t++) {
			if(tLogs[t]->isLocal) {
				lastBegin = std::max(lastBegin, tLogs[t]->startVersion);
			}

			if(tLogs[t]->logRouters.size()) {
				ASSERT(bestSet == -1);
				bestSet = t;
			}
		}
		if(bestSet == -1) {
			TraceEvent("TLogPeekRemoteNoBestSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		}
		if(begin >= lastBegin) {
			TraceEvent("TLogPeekRemoteBestOnly", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("BestSet", bestSet).detail("BestSetStart", lastBegin).detail("LogRouterIds", tLogs[bestSet]->logRouterString());
			return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[bestSet]->logRouters, -1, (int)tLogs[bestSet]->logRouters.size(), tag, begin, getPeekEnd(), false, std::vector<LocalityData>(), IRepPolicyRef(), 0 ) );
		} else {
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;
			TraceEvent("TLogPeekRemoteAddingBest", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("BestSet", bestSet).detail("BestSetStart", lastBegin).detail("LogRouterIds", tLogs[bestSet]->logRouterString());
			cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[bestSet]->logRouters, -1, (int)tLogs[bestSet]->logRouters.size(), tag, lastBegin, getPeekEnd(), false, std::vector<LocalityData>(), IRepPolicyRef(), 0 ) ) );
			int i = 0;
			while(begin < lastBegin) {
				if(i == oldLogData.size()) {
					TraceEvent("TLogPeekRemoteDead", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("LastBegin", lastBegin).detail("OldLogDataSize", oldLogData.size());
					return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
				}

				int bestOldSet = -1;
				Version thisBegin = begin;
				for(int t = 0; t < oldLogData[i].tLogs.size(); t++) {
					if(oldLogData[i].tLogs[t]->isLocal) {
						thisBegin = std::max(thisBegin, oldLogData[i].tLogs[t]->startVersion);
					}

					if(oldLogData[i].tLogs[t]->logRouters.size()) {
						ASSERT(bestOldSet == -1);
						bestOldSet = t;
					}
				}
				if(bestOldSet == -1) {
					TraceEvent("TLogPeekRemoteNoOldBestSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
					return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
				}

				if(thisBegin < lastBegin) {
					TraceEvent("TLogPeekRemoteAddingOldBest", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("BestOldSet", bestOldSet).detail("LogRouterIds", oldLogData[i].tLogs[bestOldSet]->logRouterString())
					.detail("LastBegin", lastBegin).detail("ThisBegin", thisBegin).detail("BestStartVer", oldLogData[i].tLogs[bestOldSet]->startVersion);
					cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor(  oldLogData[i].tLogs[bestOldSet]->logRouters, -1, (int)oldLogData[i].tLogs[bestOldSet]->logRouters.size(), tag,
						thisBegin, lastBegin, false, std::vector<LocalityData>(), IRepPolicyRef(), 0 ) ) );
					epochEnds.push_back(LogMessageVersion(lastBegin));
					lastBegin = thisBegin;
				}
				i++;
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual Reference<IPeekCursor> peek( UID dbgid, Version begin, Tag tag, bool parallelGetMore ) {
		if(!tLogs.size()) {
			TraceEvent("TLogPeekNoLogSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		}

		if(tag.locality == tagLocalityRemoteLog) {
			return peekRemote(dbgid, begin, tag, parallelGetMore);
		} else {
			return peekAll(dbgid, begin, getPeekEnd(), tag, parallelGetMore);
		}
	}

	virtual Reference<IPeekCursor> peek( UID dbgid, Version begin, Optional<Version> end, std::vector<Tag> tags, bool parallelGetMore ) {
		if(tags.empty()) {
			TraceEvent("TLogPeekNoTags", dbgid).detail("Begin", begin);
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), invalidTag, begin, getPeekEnd(), false, false ) );
		}

		if(tags.size() == 1) {
			return peek(dbgid, begin, tags[0], parallelGetMore);
		}

		std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
		for(auto tag : tags) {
			cursors.push_back(peek(dbgid, begin, tag, parallelGetMore));
		}
		return Reference<ILogSystem::BufferedCursor>( new ILogSystem::BufferedCursor(cursors, begin, end.present() ? end.get() + 1 : getPeekEnd(), tLogs[0]->locality == tagLocalityUpgraded) );
	}

	Reference<IPeekCursor> peekLocal( UID dbgid, Tag tag, Version begin, Version end, bool useMergePeekCursors, int8_t peekLocality = tagLocalityInvalid ) {
		if(tag.locality >= 0 || tag.locality == tagLocalityUpgraded) {
			peekLocality = tag.locality;
		}
		ASSERT(peekLocality >= 0 || peekLocality == tagLocalityUpgraded);

		int bestSet = -1;
		bool foundSpecial = false;
		int logCount = 0;
		for(int t = 0; t < tLogs.size(); t++) {
			if(tLogs[t]->logServers.size() && tLogs[t]->locality != tagLocalitySatellite) {
				logCount++;
			}
			if(tLogs[t]->logServers.size() && (tLogs[t]->locality == tagLocalitySpecial || tLogs[t]->locality == tagLocalityUpgraded || tLogs[t]->locality == peekLocality || peekLocality == tagLocalityUpgraded)) {
				if( tLogs[t]->locality == tagLocalitySpecial || tLogs[t]->locality == tagLocalityUpgraded ) {
					foundSpecial = true;
				}
				bestSet = t;
				break;
			}
		}
		if(bestSet == -1) {
			TraceEvent("TLogPeekLocalNoBestSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LogCount", logCount);
			if(useMergePeekCursors || logCount > 1) {
				throw worker_removed();
			} else {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
			}
		}

		if(begin >= tLogs[bestSet]->startVersion) {
			TraceEvent("TLogPeekLocalBestOnly", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("BestSet", bestSet).detail("BestSetStart", tLogs[bestSet]->startVersion).detail("LogId", tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor( tag )]->get().id());
			if(useMergePeekCursors) {
				return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[bestSet]->logServers, tLogs[bestSet]->bestLocationFor( tag ), tLogs[bestSet]->logServers.size() + 1 - tLogs[bestSet]->tLogReplicationFactor, tag,
							begin, end, true, tLogs[bestSet]->tLogLocalities, tLogs[bestSet]->tLogPolicy, tLogs[bestSet]->tLogReplicationFactor) );
			} else {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor( tag )], tag, begin, end, false, false ) );
			}
		} else {
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;

			if(tLogs[bestSet]->startVersion < end) {
				TraceEvent("TLogPeekLocalAddingBest", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("BestSet", bestSet).detail("BestSetStart", tLogs[bestSet]->startVersion).detail("LogId", tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor( tag )]->get().id());
				if(useMergePeekCursors) {
					cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[bestSet]->logServers, tLogs[bestSet]->bestLocationFor( tag ), tLogs[bestSet]->logServers.size() + 1 - tLogs[bestSet]->tLogReplicationFactor, tag,
								tLogs[bestSet]->startVersion, end, true, tLogs[bestSet]->tLogLocalities, tLogs[bestSet]->tLogPolicy, tLogs[bestSet]->tLogReplicationFactor) ) );
				} else {
					cursors.push_back( Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor( tag )], tag, tLogs[bestSet]->startVersion, end, false, false ) ) );
				}
			}
			Version lastBegin = tLogs[bestSet]->startVersion;
			int i = 0;
			while(begin < lastBegin) {
				if(i == oldLogData.size()) {
					if(tag == txsTag && cursors.size()) {
						break;
					}
					TraceEvent("TLogPeekLocalDead", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LastBegin", lastBegin).detail("OldLogDataSize", oldLogData.size());
					throw worker_removed();
				}

				int bestOldSet = -1;
				logCount = 0;
				bool nextFoundSpecial = false;
				for(int t = 0; t < oldLogData[i].tLogs.size(); t++) {
					if(oldLogData[i].tLogs[t]->logServers.size() && oldLogData[i].tLogs[t]->locality != tagLocalitySatellite) {
						logCount++;
					}
					if(oldLogData[i].tLogs[t]->logServers.size() && (oldLogData[i].tLogs[t]->locality == tagLocalitySpecial || oldLogData[i].tLogs[t]->locality == tagLocalityUpgraded || oldLogData[i].tLogs[t]->locality == peekLocality || peekLocality == tagLocalityUpgraded)) {
						if( oldLogData[i].tLogs[t]->locality == tagLocalitySpecial || oldLogData[i].tLogs[t]->locality == tagLocalityUpgraded ) {
							nextFoundSpecial = true;
						}
						if(foundSpecial && !oldLogData[i].tLogs[t]->isLocal) {
							TraceEvent("TLogPeekLocalRemoteBeforeSpecial", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LastBegin", lastBegin).detail("OldLogDataSize", oldLogData.size()).detail("Idx", i);
							throw worker_removed();
						}
						bestOldSet = t;
						break;
					}
				}

				if(bestOldSet == -1) {
					TraceEvent("TLogPeekLocalNoBestSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).detail("LastBegin", lastBegin).detail("OldLogDataSize", oldLogData.size()).detail("Idx", i).detail("LogRouterTags", oldLogData[i].logRouterTags).detail("LogCount", logCount).detail("FoundSpecial", foundSpecial);
					if(oldLogData[i].logRouterTags == 0 || logCount > 1 || foundSpecial) {
						throw worker_removed();
					}
					i++;
					continue;
				}

				foundSpecial = nextFoundSpecial;

				Version thisBegin = std::max(oldLogData[i].tLogs[bestOldSet]->startVersion, begin);
				if(thisBegin < lastBegin) {
					if(thisBegin < end) {
						TraceEvent("TLogPeekLocalAddingOldBest", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end)
							.detail("LogServers", oldLogData[i].tLogs[bestOldSet]->logServerString()).detail("ThisBegin", thisBegin).detail("LastBegin", lastBegin);
						cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( oldLogData[i].tLogs[bestOldSet]->logServers, oldLogData[i].tLogs[bestOldSet]->bestLocationFor( tag ), oldLogData[i].tLogs[bestOldSet]->logServers.size() + 1 - oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor, tag,
							thisBegin, std::min(lastBegin, end), useMergePeekCursors, oldLogData[i].tLogs[bestOldSet]->tLogLocalities, oldLogData[i].tLogs[bestOldSet]->tLogPolicy, oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor)));
						epochEnds.push_back(LogMessageVersion(std::min(lastBegin, end)));
					}
					lastBegin = thisBegin;
				}
				i++;
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual Reference<IPeekCursor> peekSpecial( UID dbgid, Version begin, Tag tag, int8_t peekLocality, Version localEnd ) {
		Version end = getEnd();
		TraceEvent("TLogPeekSpecial", dbgid).detail("Begin", begin).detail("End", end).detail("LocalEnd", localEnd).detail("PeekLocality", peekLocality);
		if(localEnd == invalidVersion || localEnd <= begin) {
			return peekAll(dbgid, begin, end, tag, true);
		}

		try {
			if(localEnd >= end) {
				return peekLocal(dbgid, tag, begin, end, true, peekLocality);
			}

			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;

			cursors.resize(2);
			cursors[1] = peekLocal(dbgid, tag, begin, localEnd, true, peekLocality);
			cursors[0] = peekAll(dbgid, localEnd, end, tag, true);
			epochEnds.push_back(LogMessageVersion(localEnd));

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		} catch( Error& e ) {
			if(e.code() == error_code_worker_removed) {
				return peekAll(dbgid, begin, end, tag, true);
			}
			throw;
		}
	}

	virtual Reference<IPeekCursor> peekSingle( UID dbgid, Version begin, Tag tag, vector<pair<Version,Tag>> history ) {
		while(history.size() && begin >= history.back().first) {
			history.pop_back();
		}

		if(history.size() == 0) {
			TraceEvent("TLogPeekSingleNoHistory", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
			return peekLocal(dbgid, tag, begin, getPeekEnd(), false);
		} else {
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;

			TraceEvent("TLogPeekSingleAddingLocal", dbgid).detail("Tag", tag.toString()).detail("Begin", history[0].first);
			cursors.push_back( peekLocal(dbgid, tag, history[0].first, getPeekEnd(), false) );

			for(int i = 0; i < history.size(); i++) {
				TraceEvent("TLogPeekSingleAddingOld", dbgid).detail("Tag", tag.toString()).detail("HistoryTag", history[i].second.toString()).detail("Begin", i+1 == history.size() ? begin : std::max(history[i+1].first, begin)).detail("End", history[i].first);
				cursors.push_back( peekLocal(dbgid, history[i].second, i+1 == history.size() ? begin : std::max(history[i+1].first, begin), history[i].first, false) );
				epochEnds.push_back(LogMessageVersion(history[i].first));
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual Reference<IPeekCursor> peekLogRouter( UID dbgid, Version begin, Tag tag ) {
		bool found = false;
		for( auto& log : tLogs ) {
			for( auto& router : log->logRouters ) {
				if(router->get().id() == dbgid) {
					found = true;
					break;
				}
			}
			if(found) {
				break;
			}
		}
		if( found ) {
			if(stopped) {
				std::vector<Reference<LogSet>> localSets;
				int bestSet = 0;
				for(auto& log : tLogs) {
					if(log->isLocal && log->logServers.size()) {
						TraceEvent("TLogPeekLogRouterLocalSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("LogServers", log->logServerString());
						localSets.push_back(log);
						if(log->locality != tagLocalitySatellite) {
							bestSet = localSets.size() - 1;
						}
					}
				}

				TraceEvent("TLogPeekLogRouterSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
				//FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies across the WAN
				return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( localSets, bestSet, localSets[bestSet]->bestLocationFor( tag ), tag, begin, getPeekEnd(), true ) );
			} else {
				for( auto& log : tLogs ) {
					if(log->logServers.size() && log->isLocal && log->locality != tagLocalitySatellite) {
						TraceEvent("TLogPeekLogRouterBestOnly", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("LogId", log->logServers[log->bestLocationFor( tag )]->get().id());
						return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( log->logServers[log->bestLocationFor( tag )], tag, begin, getPeekEnd(), false, true ) );
					}
				}
			}
		}
		bool firstOld = true;
		for(auto& old : oldLogData) {
			found = false;
			for( auto& log : old.tLogs ) {
				for( auto& router : log->logRouters ) {
					if(router->get().id() == dbgid) {
						found = true;
						break;
					}
				}
				if(found) {
					break;
				}
			}
			if( found ) {
				int bestSet = 0;
				std::vector<Reference<LogSet>> localSets;
				for(auto& log : old.tLogs) {
					if(log->isLocal && log->logServers.size()) {
						TraceEvent("TLogPeekLogRouterOldLocalSet", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("LogServers", log->logServerString());
						localSets.push_back(log);
						if(log->locality != tagLocalitySatellite) {
							bestSet = localSets.size()-1;
						}
					}
				}

				TraceEvent("TLogPeekLogRouterOldSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin).detail("OldEpoch", old.epochEnd).detail("RecoveredAt", recoveredAt.present() ? recoveredAt.get() : -1).detail("FirstOld", firstOld);
				//FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies across the WAN
				return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( localSets, bestSet, localSets[bestSet]->bestLocationFor( tag ), tag, begin, firstOld && recoveredAt.present() ? recoveredAt.get() + 1 : old.epochEnd, true ) );
			}
			firstOld = false;
		}
		return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
	}

	virtual Version getKnownCommittedVersion(int8_t loc) {
		for(auto& it : lockResults) {
			if(it.logSet->locality == loc) {
				auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, it);
				if(versions.present()) {
					return versions.get().first;
				}
				return invalidVersion;
			}
		}
		return invalidVersion;
	}

	virtual Future<Void> onKnownCommittedVersionChange(int8_t loc) {
		for(auto& it : lockResults) {
			if(it.logSet->locality == loc) {
				return TagPartitionedLogSystem::getDurableVersionChanged(it);
			}
		}
		return Never();
	}

	void popLogRouter( Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality ) { //FIXME: do not need to pop all generations of old logs
		if (!upTo) return;
		for(auto& t : tLogs) {
			if(t->locality == popLocality) {
				for(auto& log : t->logRouters) {
					Version prev = outstandingPops[std::make_pair(log->get().id(),tag)].first;
					if (prev < upTo)
						outstandingPops[std::make_pair(log->get().id(),tag)] = std::make_pair(upTo, durableKnownCommittedVersion);
					if (prev == 0) {
						popActors.add( popFromLog( this, log, tag, 0.0 ) ); //Fast pop time because log routers can only hold 5 seconds of data.
					}
				}
			}
		}

		for(auto& old : oldLogData) {
			for(auto& t : old.tLogs) {
				if(t->locality == popLocality) {
					for(auto& log : t->logRouters) {
						Version prev = outstandingPops[std::make_pair(log->get().id(),tag)].first;
						if (prev < upTo)
							outstandingPops[std::make_pair(log->get().id(),tag)] = std::make_pair(upTo, durableKnownCommittedVersion);
						if (prev == 0)
							popActors.add( popFromLog( this, log, tag, 0.0 ) );
					}
				}
			}
		}
	}

	virtual void pop( Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality ) {
		if (upTo <= 0) return;
		if( tag.locality == tagLocalityRemoteLog) {
			popLogRouter(upTo, tag, durableKnownCommittedVersion, popLocality);
			return;
		}
		for(auto& t : tLogs) {
			if(t->locality == tagLocalitySpecial || t->locality == tag.locality || tag.locality == tagLocalityUpgraded || (tag.locality < 0 && ((popLocality == tagLocalityInvalid) == t->isLocal))) {
				for(auto& log : t->logServers) {
					Version prev = outstandingPops[std::make_pair(log->get().id(),tag)].first;
					if (prev < upTo)
						outstandingPops[std::make_pair(log->get().id(),tag)] = std::make_pair(upTo, durableKnownCommittedVersion);
					if (prev == 0)
						popActors.add( popFromLog( this, log, tag, 1.0 ) ); //< FIXME: knob
				}
			}
		}
	}

	ACTOR static Future<Void> popFromLog( TagPartitionedLogSystem* self, Reference<AsyncVar<OptionalInterface<TLogInterface>>> log, Tag tag, double time ) {
		state Version last = 0;
		loop {
			wait( delay(time) );

			state std::pair<Version,Version> to = self->outstandingPops[ std::make_pair(log->get().id(),tag) ];

			if (to.first <= last) {
				self->outstandingPops.erase( std::make_pair(log->get().id(),tag) );
				return Void();
			}

			try {
				if( !log->get().present() )
					return Void();
				wait(log->get().interf().popMessages.getReply( TLogPopRequest( to.first, to.second, tag ) ) );

				last = to.first;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
				TraceEvent( (e.code() == error_code_broken_promise) ? SevInfo : SevError, "LogPopError", self->dbgid ).error(e).detail("Log", log->get().id());
				return Void();  // Leaving outstandingPops filled in means no further pop requests to this tlog from this logSystem
			}
		}
	}

	ACTOR static Future<Void> confirmEpochLive_internal(Reference<LogSet> logSet, Optional<UID> debugID) {
		state vector<Future<Void>> alive;
		int numPresent = 0;
		for(auto& t : logSet->logServers) {
			if( t->get().present() ) {
				alive.push_back( brokenPromiseToNever(
				    t->get().interf().confirmRunning.getReply( TLogConfirmRunningRequest(debugID),
				                                               TaskTLogConfirmRunningReply ) ) );
				numPresent++;
			} else {
				alive.push_back( Never() );
			}
		}

		wait( quorum( alive, std::min(logSet->tLogReplicationFactor, numPresent - logSet->tLogWriteAntiQuorum) ) );

		state Reference<LocalityGroup> locked(new LocalityGroup());
		state std::vector<bool> responded(alive.size());
		for (int i = 0; i < alive.size(); i++) {
			responded[i] = false;
		}
		loop {
			for (int i = 0; i < alive.size(); i++) {
				if (!responded[i] && alive[i].isReady() && !alive[i].isError()) {
					locked->add(logSet->tLogLocalities[i]);
					responded[i] = true;
				}
			}
			bool quorum_obtained = locked->validate(logSet->tLogPolicy);
			// We intentionally skip considering antiquorums, as the CPU cost of doing so is prohibitive.
			if (logSet->tLogReplicationFactor == 1 && locked->size() > 0) {
				ASSERT(quorum_obtained);
			}
			if (quorum_obtained) {
				return Void();
			}

			// The current set of responders that we have weren't enough to form a quorum, so we must
			// wait for more responses and try again.
			std::vector<Future<Void>> changes;
			for (int i = 0; i < alive.size(); i++) {
				if (!alive[i].isReady()) {
					changes.push_back( ready(alive[i]) );
				} else if (alive[i].isReady() && alive[i].isError() &&
				           alive[i].getError().code() == error_code_tlog_stopped) {
					// All commits must go to all TLogs.  If any TLog is stopped, then our epoch has ended.
					return Never();
				}
			}
			ASSERT(changes.size() != 0);
			wait( waitForAny(changes) );
		}
	}

	// Returns success after confirming that pushes in the current epoch are still possible
	virtual Future<Void> confirmEpochLive(Optional<UID> debugID) {
		vector<Future<Void>> quorumResults;
		for(auto& it : tLogs) {
			if(it->isLocal && it->logServers.size()) {
				quorumResults.push_back( confirmEpochLive_internal(it, debugID) );
			}
		}

		return waitForAll(quorumResults);
	}

	virtual Future<Void> endEpoch() {
		std::vector<Future<Void>> lockResults;
		for( auto& logSet : tLogs ) {
			for( auto& log : logSet->logServers ) {
				lockResults.push_back(success(lockTLog( dbgid, log )));
			}
		}
		return waitForAll(lockResults);
	}

	virtual Future<Reference<ILogSystem>> newEpoch( RecruitFromConfigurationReply const& recr, Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers, DatabaseConfiguration const& config, LogEpoch recoveryCount, int8_t primaryLocality, int8_t remoteLocality, std::vector<Tag> const& allTags, Reference<AsyncVar<bool>> const& recruitmentStalled ) {
		// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.  The new epoch
		// is only provisional until the caller updates the coordinated DBCoreState
		return newEpoch( Reference<TagPartitionedLogSystem>::addRef(this), recr, fRemoteWorkers, config, recoveryCount, primaryLocality, remoteLocality, allTags, recruitmentStalled );
	}

	virtual LogSystemConfig getLogSystemConfig() {
		LogSystemConfig logSystemConfig;
		logSystemConfig.logSystemType = logSystemType;
		logSystemConfig.expectedLogSets = expectedLogSets;
		logSystemConfig.logRouterTags = logRouterTags;
		logSystemConfig.recruitmentID = recruitmentID;
		logSystemConfig.stopped = stopped;
		logSystemConfig.recoveredAt = recoveredAt;
		for( int i = 0; i < tLogs.size(); i++ ) {
			Reference<LogSet> logSet = tLogs[i];
			if(logSet->isLocal || remoteLogsWrittenToCoreState) {
				logSystemConfig.tLogs.push_back(TLogSet());
				TLogSet& log = logSystemConfig.tLogs.back();
				log.tLogWriteAntiQuorum = logSet->tLogWriteAntiQuorum;
				log.tLogReplicationFactor = logSet->tLogReplicationFactor;
				log.tLogPolicy = logSet->tLogPolicy;
				log.tLogLocalities = logSet->tLogLocalities;
				log.isLocal = logSet->isLocal;
				log.locality = logSet->locality;
				log.startVersion = logSet->startVersion;
				log.satelliteTagLocations = logSet->satelliteTagLocations;

				for( int i = 0; i < logSet->logServers.size(); i++ ) {
					log.tLogs.push_back(logSet->logServers[i]->get());
				}

				for( int i = 0; i < logSet->logRouters.size(); i++ ) {
					log.logRouters.push_back(logSet->logRouters[i]->get());
				}
			}
		}

		if(!recoveryCompleteWrittenToCoreState.get()) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				logSystemConfig.oldTLogs.push_back(OldTLogConf());

				logSystemConfig.oldTLogs[i].tLogs.resize(oldLogData[i].tLogs.size());
				for( int j = 0; j < oldLogData[i].tLogs.size(); j++ ) {
					TLogSet& log = logSystemConfig.oldTLogs[i].tLogs[j];
					Reference<LogSet> logSet = oldLogData[i].tLogs[j];
					log.tLogWriteAntiQuorum = logSet->tLogWriteAntiQuorum;
					log.tLogReplicationFactor = logSet->tLogReplicationFactor;
					log.tLogPolicy = logSet->tLogPolicy;
					log.tLogLocalities = logSet->tLogLocalities;
					log.isLocal = logSet->isLocal;
					log.locality = logSet->locality;
					log.startVersion = logSet->startVersion;
					log.satelliteTagLocations = logSet->satelliteTagLocations;

					for( int i = 0; i < logSet->logServers.size(); i++ ) {
						log.tLogs.push_back(logSet->logServers[i]->get());
					}

					for( int i = 0; i < logSet->logRouters.size(); i++ ) {
						log.logRouters.push_back(logSet->logRouters[i]->get());
					}
				}
				logSystemConfig.oldTLogs[i].logRouterTags = oldLogData[i].logRouterTags;
				logSystemConfig.oldTLogs[i].epochEnd = oldLogData[i].epochEnd;
			}
		}
		return logSystemConfig;
	}

	virtual Standalone<StringRef> getLogsValue() {
		vector<std::pair<UID, NetworkAddress>> logs;
		vector<std::pair<UID, NetworkAddress>> oldLogs;
		for(auto& t : tLogs) {
			if(t->isLocal || remoteLogsWrittenToCoreState) {
				for( int i = 0; i < t->logServers.size(); i++ ) {
					logs.push_back(std::make_pair(t->logServers[i]->get().id(), t->logServers[i]->get().present() ? t->logServers[i]->get().interf().address() : NetworkAddress()));
				}
			}
		}
		if(!recoveryCompleteWrittenToCoreState.get()) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				for(auto& t : oldLogData[i].tLogs) {
					for( int j = 0; j < t->logServers.size(); j++ ) {
						oldLogs.push_back(std::make_pair(t->logServers[j]->get().id(), t->logServers[j]->get().present() ? t->logServers[j]->get().interf().address() : NetworkAddress()));
					}
				}
			}
		}
		return logsValue( logs, oldLogs );
	}

	virtual Future<Void> onLogSystemConfigChange() {
		std::vector<Future<Void>> changes;
		changes.push_back(logSystemConfigChanged.onTrigger());
		for(auto& t : tLogs) {
			for( int i = 0; i < t->logServers.size(); i++ ) {
				changes.push_back( t->logServers[i]->onChange() );
			}
		}
		for(int i = 0; i < oldLogData.size(); i++) {
			for(auto& t : oldLogData[i].tLogs) {
				for( int j = 0; j < t->logServers.size(); j++ ) {
					changes.push_back( t->logServers[j]->onChange() );
				}
			}
		}

		if(hasRemoteServers && !remoteRecovery.isReady()) {
			changes.push_back(remoteRecovery);
		}

		return waitForAny(changes);
	}

	virtual Version getEnd() {
		ASSERT( recoverAt.present() );
		return recoverAt.get() + 1;
	}

	Version getPeekEnd() {
		if (recoverAt.present())
			return getEnd();
		else
			return std::numeric_limits<Version>::max();
	}

	virtual void getPushLocations( std::vector<Tag> const& tags, std::vector<int>& locations ) {
		int locationOffset = 0;
		for(auto& log : tLogs) {
			if(log->isLocal && log->logServers.size()) {
				log->getPushLocations(tags, locations, locationOffset);
				locationOffset += log->logServers.size();
			}
		}
	}

	virtual bool hasRemoteLogs() {
		return logRouterTags > 0;
	}

	virtual Tag getRandomRouterTag() {
		return Tag(tagLocalityLogRouter, g_random->randomInt(0, logRouterTags));
	}

	ACTOR static Future<Void> monitorLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> logServer, Reference<AsyncVar<bool>> failed) {
		state Future<Void> waitFailure;
		loop {
			if(logServer->get().present())
				waitFailure = waitFailureTracker( logServer->get().interf().waitFailure, failed );
			else
				failed->set(true);
			wait( logServer->onChange() );
		}
	}

	Optional<std::pair<Version,Version>> static getDurableVersion(UID dbgid, LogLockInfo lockInfo, std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>(), Optional<Version> lastEnd = Optional<Version>()) {
		Reference<LogSet> logSet = lockInfo.logSet;
		// To ensure consistent recovery, the number of servers NOT in the write quorum plus the number of servers NOT in the read quorum
		// have to be strictly less than the replication factor.  Otherwise there could be a replica set consistent entirely of servers that
		// are out of date due to not being in the write quorum or unavailable due to not being in the read quorum.
		// So with N = # of tlogs, W = antiquorum, R = required count, F = replication factor,
		// W + (N - R) < F, and optimally (N-W)+(N-R)=F-1.  Thus R=N+1-F+W.
		int requiredCount = (int)logSet->logServers.size()+1 - logSet->tLogReplicationFactor + logSet->tLogWriteAntiQuorum;
		ASSERT( requiredCount > 0 && requiredCount <= logSet->logServers.size() );
		ASSERT( logSet->tLogReplicationFactor >= 1 && logSet->tLogReplicationFactor <= logSet->logServers.size() );
		ASSERT( logSet->tLogWriteAntiQuorum >= 0 && logSet->tLogWriteAntiQuorum < logSet->logServers.size() );

		std::vector<LocalityData> availableItems, badCombo;
		std::vector<TLogLockResult> results;
		std::string sServerState;
		LocalityGroup unResponsiveSet;

		for(int t=0; t<logSet->logServers.size(); t++) {
			if (lockInfo.replies[t].isReady() && !lockInfo.replies[t].isError() && (!failed.size() || !failed[t]->get())) {
				results.push_back(lockInfo.replies[t].get());
				availableItems.push_back(logSet->tLogLocalities[t]);
				sServerState += 'a';
			}
			else {
				unResponsiveSet.add(logSet->tLogLocalities[t]);
				sServerState += 'f';
			}
		}

		// Check if the list of results is not larger than the anti quorum
		bool bTooManyFailures = (results.size() <= logSet->tLogWriteAntiQuorum);

		// Check if failed logs complete the policy
		bTooManyFailures = bTooManyFailures || ((unResponsiveSet.size() >= logSet->tLogReplicationFactor)	&& (unResponsiveSet.validate(logSet->tLogPolicy)));

		// Check all combinations of the AntiQuorum within the failed
		if (!bTooManyFailures && (logSet->tLogWriteAntiQuorum) && (!validateAllCombinations(badCombo, unResponsiveSet, logSet->tLogPolicy, availableItems, logSet->tLogWriteAntiQuorum, false))) {
			TraceEvent("EpochEndBadCombo", dbgid).detail("Required", requiredCount).detail("Present", results.size()).detail("ServerState", sServerState);
			bTooManyFailures = true;
		}

		ASSERT(logSet->logServers.size() == lockInfo.replies.size());
		if (!bTooManyFailures) {
			std::sort( results.begin(), results.end(), sort_by_end() );
			int absent = logSet->logServers.size() - results.size();
			int safe_range_begin = logSet->tLogWriteAntiQuorum;
			int new_safe_range_begin = std::min(logSet->tLogWriteAntiQuorum, (int)(results.size()-1));
			int safe_range_end = logSet->tLogReplicationFactor - absent;

			if( !lastEnd.present() || ((safe_range_end > 0) && (safe_range_end-1 < results.size()) && results[ safe_range_end-1 ].end < lastEnd.get()) ) {
				Version knownCommittedVersion = results[ new_safe_range_begin ].end - (g_network->isSimulated() ? 10*SERVER_KNOBS->VERSIONS_PER_SECOND : SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS); //In simulation this must be the maximum MAX_READ_TRANSACTION_LIFE_VERSIONS
				for(int i = 0; i < results.size(); i++) {
					knownCommittedVersion = std::max(knownCommittedVersion, results[i].knownCommittedVersion);
				}

				TraceEvent("GetDurableResult", dbgid).detail("Required", requiredCount).detail("Present", results.size()).detail("ServerState", sServerState)
					.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
					.detail("EndVersion", results[ new_safe_range_begin ].end).detail("SafeBegin", safe_range_begin).detail("SafeEnd", safe_range_end)
					.detail("NewSafeBegin", new_safe_range_begin).detail("KnownCommittedVersion", knownCommittedVersion).detail("EpochEnd", lockInfo.epochEnd);

				return std::make_pair(knownCommittedVersion, results[ new_safe_range_begin ].end);
			}
		}
		TraceEvent("GetDurableResultWaiting", dbgid).detail("Required", requiredCount).detail("Present", results.size()).detail("ServerState", sServerState);
		return Optional<std::pair<Version,Version>>();
	}

	ACTOR static Future<Void> getDurableVersionChanged(LogLockInfo lockInfo, std::vector<Reference<AsyncVar<bool>>> failed = std::vector<Reference<AsyncVar<bool>>>()) {
		// Wait for anything relevant to change
		std::vector<Future<Void>> changes;
		for(int j=0; j < lockInfo.logSet->logServers.size(); j++) {
			if (!lockInfo.replies[j].isReady())
				changes.push_back( ready(lockInfo.replies[j]) );
			else {
				changes.push_back( lockInfo.logSet->logServers[j]->onChange() );
				if(failed.size()) {
					changes.push_back( failed[j]->onChange() );
				}
			}
		}
		ASSERT(changes.size());
		wait(waitForAny(changes));
		return Void();
	}

	ACTOR static Future<Void> epochEnd( Reference<AsyncVar<Reference<ILogSystem>>> outLogSystem, UID dbgid, DBCoreState prevState, FutureStream<TLogRejoinRequest> rejoinRequests, LocalityData locality, bool forceRecovery ) {
		// Stops a co-quorum of tlogs so that no further versions can be committed until the DBCoreState coordination state is changed
		// Creates a new logSystem representing the (now frozen) epoch
		// No other important side effects.
		// The writeQuorum in the master info is from the previous configuration

		if (!prevState.tLogs.size()) {
			// This is a brand new database
			Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );
			logSystem->logSystemType = prevState.logSystemType;
			logSystem->recoverAt = 0;
			logSystem->knownCommittedVersion = 0;
			logSystem->stopped = true;
			outLogSystem->set(logSystem);
			wait( Future<Void>(Never()) );
			throw internal_error();
		}

		if(forceRecovery) {
			DBCoreState modifiedState = prevState;

			int8_t primaryLocality = -1;
			for(auto& coreSet : modifiedState.tLogs) {
				if(coreSet.isLocal && coreSet.locality >= 0) {
					primaryLocality = coreSet.locality;
					break;
				}
			}

			bool foundRemote = false;
			int8_t remoteLocality = -1;
			int modifiedLogSets = 0;
			int removedLogSets = 0;
			if(primaryLocality >= 0) {
				auto copiedLogs = modifiedState.tLogs;
				for(auto& coreSet : copiedLogs) {
					if(coreSet.locality != primaryLocality && coreSet.locality >= 0) {
						foundRemote = true;
						remoteLocality = coreSet.locality;
						modifiedState.tLogs.clear();
						modifiedState.tLogs.push_back(coreSet);
						modifiedState.tLogs[0].isLocal = true;
						modifiedState.logRouterTags = 0;
						modifiedLogSets++;
						break;
					}
				}

				while( !foundRemote && modifiedState.oldTLogData.size() ) {
					for(auto& coreSet : modifiedState.oldTLogData[0].tLogs) {
						if(coreSet.locality != primaryLocality && coreSet.locality >= tagLocalitySpecial) {
							foundRemote = true;
							remoteLocality = coreSet.locality;
							modifiedState.tLogs.clear();
							modifiedState.tLogs.push_back(coreSet);
							modifiedState.tLogs[0].isLocal = true;
							modifiedState.logRouterTags = 0;
							modifiedLogSets++;
							break;
						}
					}
					modifiedState.oldTLogData.erase(modifiedState.oldTLogData.begin());
					removedLogSets++;
				}

				if(foundRemote) {
					for(int i = 0; i < modifiedState.oldTLogData.size(); i++) {
						bool found = false;
						auto copiedLogs = modifiedState.oldTLogData[i].tLogs;
						for(auto& coreSet : copiedLogs) {
							if(coreSet.locality == remoteLocality || coreSet.locality == tagLocalitySpecial) {
								found = true;
								if(!coreSet.isLocal || copiedLogs.size() > 1) {
									modifiedState.oldTLogData[i].tLogs.clear();
									modifiedState.oldTLogData[i].tLogs.push_back(coreSet);
									modifiedState.oldTLogData[i].tLogs[0].isLocal = true;
									modifiedState.oldTLogData[i].logRouterTags = 0;
									modifiedState.oldTLogData[i].epochEnd = ( i == 0 ? modifiedState.tLogs[0].startVersion : modifiedState.oldTLogData[i-1].tLogs[0].startVersion );
									modifiedLogSets++;
								}
								break;
							}
						}
						if(!found) {
							modifiedState.oldTLogData.erase(modifiedState.oldTLogData.begin()+i);
							removedLogSets++;
							i--;
						}
					}
					prevState = modifiedState;
				} else {
					forceRecovery = false;
				}
			}
			TraceEvent(SevWarnAlways, "ForcedRecovery", dbgid).detail("PrimaryLocality", primaryLocality).detail("RemoteLocality", remoteLocality).detail("FoundRemote", foundRemote).detail("Modified", modifiedLogSets).detail("Removed", removedLogSets);
		}

		TEST( true );	// Master recovery from pre-existing database

		// trackRejoins listens for rejoin requests from the tLogs that we are recovering from, to learn their TLogInterfaces
		state std::vector<LogLockInfo> lockResults;
		state std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> allLogServers;
		state std::vector<Reference<LogSet>> logServers;
		state std::vector<OldLogData> oldLogData;
		state std::vector<std::vector<Reference<AsyncVar<bool>>>> logFailed;
		state std::vector<Future<Void>> failureTrackers;

		logServers.resize(prevState.tLogs.size());
		for( int i = 0; i < prevState.tLogs.size(); i++ ) {
			Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
			logServers[i] = logSet;
			CoreTLogSet const& coreSet = prevState.tLogs[i];
			std::vector<Reference<AsyncVar<bool>>> failed;
			for(int j = 0; j < coreSet.tLogs.size(); j++) {
				Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(coreSet.tLogs[j]) ) );
				logSet->logServers.push_back( logVar );
				allLogServers.push_back( logVar );
				failed.push_back( Reference<AsyncVar<bool>>( new AsyncVar<bool>() ) );
				failureTrackers.push_back( monitorLog(logVar, failed[j] ) );
			}
			logSet->tLogReplicationFactor = coreSet.tLogReplicationFactor;
			logSet->tLogWriteAntiQuorum = coreSet.tLogWriteAntiQuorum;
			logSet->tLogPolicy = coreSet.tLogPolicy;
			logSet->tLogLocalities = coreSet.tLogLocalities;
			logSet->isLocal = coreSet.isLocal;
			logSet->locality = coreSet.locality;
			logSet->startVersion = coreSet.startVersion;
			logSet->satelliteTagLocations = coreSet.satelliteTagLocations;
			filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
			logSet->updateLocalitySet(logSet->tLogLocalities);
			logFailed.push_back(failed);
		}
		oldLogData.resize(prevState.oldTLogData.size());
		for( int i = 0; i < prevState.oldTLogData.size(); i++ ) {
			OldLogData& oldData = oldLogData[i];
			OldTLogCoreData const& old = prevState.oldTLogData[i];
			oldData.tLogs.resize(old.tLogs.size());
			for( int j = 0; j < old.tLogs.size(); j++ ) {
				Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
				oldData.tLogs[j] = logSet;
				CoreTLogSet const& log = old.tLogs[j];
				for(int k = 0; k < log.tLogs.size(); k++) {
					Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(log.tLogs[k]) ) );
					logSet->logServers.push_back( logVar );
					allLogServers.push_back( logVar );
				}
				logSet->tLogReplicationFactor = log.tLogReplicationFactor;
				logSet->tLogWriteAntiQuorum = log.tLogWriteAntiQuorum;
				logSet->tLogPolicy = log.tLogPolicy;
				logSet->tLogLocalities = log.tLogLocalities;
				logSet->isLocal = log.isLocal;
				logSet->locality = log.locality;
				logSet->startVersion = log.startVersion;
				logSet->satelliteTagLocations = log.satelliteTagLocations;
				filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
				logSet->updateLocalitySet(logSet->tLogLocalities);
			}
			oldData.epochEnd = old.epochEnd;
			oldData.logRouterTags = old.logRouterTags;
		}
		state Future<Void> rejoins = trackRejoins( dbgid, allLogServers, rejoinRequests );

		lockResults.resize(logServers.size());
		std::set<int8_t> lockedLocalities;
		bool foundSpecial = false;
		for( int i=0; i < logServers.size(); i++ ) {
			if(logServers[i]->locality == tagLocalitySpecial || logServers[i]->locality == tagLocalityUpgraded) {
				foundSpecial = true;
			}
			lockedLocalities.insert(logServers[i]->locality);
			lockResults[i].isCurrent = true;
			lockResults[i].logSet = logServers[i];
			for(int t=0; t<logServers[i]->logServers.size(); t++) {
				lockResults[i].replies.push_back( lockTLog( dbgid, logServers[i]->logServers[t]) );
			}
		}

		for( auto& old : oldLogData ) {
			if(foundSpecial) {
				break;
			}
			for( auto& log : old.tLogs ) {
				if(log->locality == tagLocalitySpecial || log->locality == tagLocalityUpgraded) {
					foundSpecial = true;
					break;
				}
				if(!lockedLocalities.count(log->locality)) {
					TraceEvent("EpochEndLockExtra").detail("Locality", log->locality);
					TEST(true); //locking old generations for version information
					lockedLocalities.insert(log->locality);
					LogLockInfo lockResult;
					lockResult.epochEnd = old.epochEnd;
					lockResult.logSet = log;
					for(int t=0; t<log->logServers.size(); t++) {
						lockResult.replies.push_back( lockTLog( dbgid, log->logServers[t]) );
					}
					lockResults.push_back(lockResult);
				}
			}
		}

		if(forceRecovery) {
			state std::vector<LogLockInfo> allLockResults;
			ASSERT( lockResults.size() == 1 );
			allLockResults.push_back(lockResults[0]);
			for( auto& old : oldLogData ) {
				ASSERT( old.tLogs.size() == 1 );
				LogLockInfo lockResult;
				lockResult.epochEnd = old.epochEnd;
				lockResult.logSet = old.tLogs[0];
				for(int t=0; t<old.tLogs[0]->logServers.size(); t++) {
					lockResult.replies.push_back( lockTLog( dbgid, old.tLogs[0]->logServers[t]) );
				}
				allLockResults.push_back(lockResult);
			}

			state int lockNum = 0;
			state Version maxRecoveryVersion = 0;
			state int maxRecoveryIndex = 0;
			while(lockNum < allLockResults.size()) {
				auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, allLockResults[lockNum]);
				if(versions.present()) {
					if(versions.get().second > maxRecoveryVersion) {
						TraceEvent("HigherRecoveryVersion", dbgid).detail("Idx", lockNum).detail("Ver", versions.get().second);
						maxRecoveryVersion = versions.get().second;
						maxRecoveryIndex = lockNum;
					}
					lockNum++;
				} else {
					wait( TagPartitionedLogSystem::getDurableVersionChanged(allLockResults[lockNum]) );
				}
			}
			if(maxRecoveryIndex > 0) {
				logServers = oldLogData[maxRecoveryIndex-1].tLogs;
				lockResults[0] = allLockResults[maxRecoveryIndex];
				lockResults[0].isCurrent = true;

				std::vector<Reference<AsyncVar<bool>>> failed;
				for(auto& log : logServers[0]->logServers) {
					failed.push_back( Reference<AsyncVar<bool>>( new AsyncVar<bool>() ) );
					failureTrackers.push_back( monitorLog(log, failed.back() ) );
				}
				ASSERT(logFailed.size() == 1);
				logFailed[0] = failed;
				oldLogData.erase(oldLogData.begin(), oldLogData.begin() + maxRecoveryIndex);
			}
		}

		state Optional<Version> lastEnd;
		state Version knownCommittedVersion = 0;
		loop {
			Version minEnd = std::numeric_limits<Version>::max();
			Version maxEnd = 0;
			std::vector<Future<Void>> changes;
			for(int log = 0; log < logServers.size(); log++) {
				if(!logServers[log]->isLocal) {
					continue;
				}
				auto versions = TagPartitionedLogSystem::getDurableVersion(dbgid, lockResults[log], logFailed[log], lastEnd);
				if(versions.present()) {
					knownCommittedVersion = std::max(knownCommittedVersion, versions.get().first);
					maxEnd = std::max(maxEnd, versions.get().second);
					minEnd = std::min(minEnd, versions.get().second);
				}
				changes.push_back(TagPartitionedLogSystem::getDurableVersionChanged(lockResults[log], logFailed[log]));
			}

			if(maxEnd > 0 && (!lastEnd.present() || maxEnd < lastEnd.get())) {
				TEST( lastEnd.present() );  // Restarting recovery at an earlier point

				Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

				lastEnd = minEnd;
				logSystem->tLogs = logServers;
				logSystem->logRouterTags = prevState.logRouterTags;
				logSystem->oldLogData = oldLogData;
				logSystem->logSystemType = prevState.logSystemType;
				logSystem->rejoins = rejoins;
				logSystem->lockResults = lockResults;
				logSystem->recoverAt = minEnd;
				logSystem->knownCommittedVersion = knownCommittedVersion;
				logSystem->remoteLogsWrittenToCoreState = true;
				logSystem->stopped = true;

				outLogSystem->set(logSystem);
			}

			wait( waitForAny(changes) );
		}
	}

	ACTOR static Future<Void> recruitOldLogRouters( TagPartitionedLogSystem* self, vector<WorkerInterface> workers, LogEpoch recoveryCount, int8_t locality, Version startVersion,
		std::vector<LocalityData> tLogLocalities, IRepPolicyRef tLogPolicy, bool forRemote ) {
		state vector<vector<Future<TLogInterface>>> logRouterInitializationReplies;
		state vector<Future<TLogInterface>> allReplies;
		int nextRouter = 0;
		Version lastStart = std::numeric_limits<Version>::max();

		if(!forRemote) {
			Version maxStart = 0;
			for(auto& logSet : self->tLogs) {
				if(logSet->isLocal) {
					maxStart = std::max(maxStart, logSet->startVersion);
				}
			}

			lastStart = std::max(startVersion, maxStart);
			if( self->logRouterTags == 0 ) {
				ASSERT_WE_THINK(false);
				self->logSystemConfigChanged.trigger();
				return Void();
			}

			bool found = false;
			for(auto& tLogs : self->tLogs) {
				if(tLogs->locality == locality) {
					found = true;
				}

				tLogs->logRouters.clear();
			}

			if(!found) {
				TraceEvent("RecruitingOldLogRoutersAddingLocality").detail("Locality", locality).detail("LastStart", lastStart);
				Reference<LogSet> newLogSet( new LogSet() );
				newLogSet->locality = locality;
				newLogSet->startVersion = lastStart;
				newLogSet->isLocal = false;
				self->tLogs.push_back(newLogSet);
			}

			for(auto& tLogs : self->tLogs) {
				//Recruit log routers for old generations of the primary locality
				if(tLogs->locality == locality) {
					logRouterInitializationReplies.push_back(vector<Future<TLogInterface>>());
					for( int i = 0; i < self->logRouterTags; i++) {
						InitializeLogRouterRequest req;
						req.recoveryCount = recoveryCount;
						req.routerTag = Tag(tagLocalityLogRouter, i);
						req.startVersion = lastStart;
						req.tLogLocalities = tLogLocalities;
						req.tLogPolicy = tLogPolicy;
						req.locality = locality;
						auto reply = transformErrors( throwErrorOr( workers[nextRouter].logRouter.getReplyUnlessFailedFor( req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() );
						logRouterInitializationReplies.back().push_back( reply );
						allReplies.push_back( reply );
						nextRouter = (nextRouter+1)%workers.size();
					}
				}
			}
		}

		for(auto& old : self->oldLogData) {
			Version maxStart = 0;
			for(auto& logSet : old.tLogs) {
				if(logSet->isLocal) {
					maxStart = std::max(maxStart, logSet->startVersion);
				}
			}

			if(old.logRouterTags == 0 || maxStart >= lastStart) {
				break;
			}
			lastStart = std::max(startVersion, maxStart);
			bool found = false;
			for(auto& tLogs : old.tLogs) {
				if(tLogs->locality == locality) {
					found = true;
				}
				tLogs->logRouters.clear();
			}

			if(!found) {
				TraceEvent("RecruitingOldLogRoutersAddingLocality").detail("Locality", locality).detail("LastStart", lastStart);
				Reference<LogSet> newLogSet( new LogSet() );
				newLogSet->locality = locality;
				newLogSet->startVersion = lastStart;
				old.tLogs.push_back(newLogSet);
			}

			for(auto& tLogs : old.tLogs) {
				//Recruit log routers for old generations of the primary locality
				if(tLogs->locality == locality) {
					logRouterInitializationReplies.push_back(vector<Future<TLogInterface>>());
					for( int i = 0; i < old.logRouterTags; i++) {
						InitializeLogRouterRequest req;
						req.recoveryCount = recoveryCount;
						req.routerTag = Tag(tagLocalityLogRouter, i);
						req.startVersion = lastStart;
						req.tLogLocalities = tLogLocalities;
						req.tLogPolicy = tLogPolicy;
						req.locality = locality;
						auto reply = transformErrors( throwErrorOr( workers[nextRouter].logRouter.getReplyUnlessFailedFor( req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() );
						logRouterInitializationReplies.back().push_back( reply );
						allReplies.push_back( reply );
						nextRouter = (nextRouter+1)%workers.size();
					}
				}
			}
		}

		wait( waitForAll(allReplies) );

		int nextReplies = 0;
		Version lastStart = std::numeric_limits<Version>::max();
		vector<Future<Void>> failed;

		if(!forRemote) {
			Version maxStart = 0;
			for(auto& logSet : self->tLogs) {
				if(logSet->isLocal) {
					maxStart = std::max(maxStart, logSet->startVersion);
				}
			}

			lastStart = std::max(startVersion, maxStart);
			for(auto& tLogs : self->tLogs) {
				if(tLogs->locality == locality) {
					for( int i = 0; i < logRouterInitializationReplies[nextReplies].size(); i++ ) {
						tLogs->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(logRouterInitializationReplies[nextReplies][i].get()) ) ) );
						failed.push_back( waitFailureClient( logRouterInitializationReplies[nextReplies][i].get().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
					}
					nextReplies++;
				}
			}
		}

		for(auto& old : self->oldLogData) {
			Version maxStart = 0;
			for(auto& logSet : old.tLogs) {
				if(logSet->isLocal) {
					maxStart = std::max(maxStart, logSet->startVersion);
				}
			}
			if(old.logRouterTags == 0 || maxStart >= lastStart) {
				break;
			}
			lastStart = std::max(startVersion, maxStart);
			for(auto& tLogs : old.tLogs) {
				if(tLogs->locality == locality) {
					for( int i = 0; i < logRouterInitializationReplies[nextReplies].size(); i++ ) {
						tLogs->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(logRouterInitializationReplies[nextReplies][i].get()) ) ) );
						if(!forRemote) {
							failed.push_back( waitFailureClient( logRouterInitializationReplies[nextReplies][i].get().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
						}
					}
					nextReplies++;
				}
			}
		}

		if(!forRemote) {
			self->logSystemConfigChanged.trigger();
			wait( failed.size() ? tagError<Void>( quorum( failed, 1 ), master_tlog_failed() ) : Future<Void>(Never()) );
			throw internal_error();
		}
		return Void();
	}

	ACTOR static Future<Void> newRemoteEpoch( TagPartitionedLogSystem* self, Reference<TagPartitionedLogSystem> oldLogSystem, Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount, int8_t remoteLocality, std::vector<Tag> allTags ) {
		TraceEvent("RemoteLogRecruitment_WaitingForWorkers");
		state RecruitRemoteFromConfigurationReply remoteWorkers = wait( fRemoteWorkers );

		state Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
		logSet->tLogReplicationFactor = configuration.getRemoteTLogReplicationFactor();
		logSet->tLogPolicy = configuration.getRemoteTLogPolicy();
		logSet->isLocal = false;
		logSet->locality = remoteLocality;

		logSet->startVersion = oldLogSystem->knownCommittedVersion + 1;
		state int lockNum = 0;
		while(lockNum < oldLogSystem->lockResults.size()) {
			if(oldLogSystem->lockResults[lockNum].logSet->locality == remoteLocality) {
				loop {
					auto versions = TagPartitionedLogSystem::getDurableVersion(self->dbgid, oldLogSystem->lockResults[lockNum]);
					if(versions.present()) {
						logSet->startVersion = std::min(std::min(versions.get().first+1, oldLogSystem->lockResults[lockNum].epochEnd), logSet->startVersion);
						break;
					}
					wait( TagPartitionedLogSystem::getDurableVersionChanged(oldLogSystem->lockResults[lockNum]) );
				}
				break;
			}
			lockNum++;
		}

		vector<LocalityData> localities;
		localities.resize(remoteWorkers.remoteTLogs.size());
		for(int i = 0; i < remoteWorkers.remoteTLogs.size(); i++) {
			localities[i] = remoteWorkers.remoteTLogs[i].locality;
		}

		state Future<Void> oldRouterRecruitment = Void();
		if(logSet->startVersion < oldLogSystem->knownCommittedVersion + 1) {
			ASSERT(oldLogSystem->logRouterTags > 0);
			oldRouterRecruitment = TagPartitionedLogSystem::recruitOldLogRouters(self, remoteWorkers.logRouters, recoveryCount, remoteLocality, logSet->startVersion, localities, logSet->tLogPolicy, true);
		}

		state vector<Future<TLogInterface>> logRouterInitializationReplies;
		for( int i = 0; i < self->logRouterTags; i++) {
			InitializeLogRouterRequest req;
			req.recoveryCount = recoveryCount;
			req.routerTag = Tag(tagLocalityLogRouter, i);
			req.startVersion = oldLogSystem->logRouterTags == 0 ? oldLogSystem->recoverAt.get() + 1 : std::max(self->tLogs[0]->startVersion, logSet->startVersion);
			req.tLogLocalities = localities;
			req.tLogPolicy = logSet->tLogPolicy;
			req.locality = remoteLocality;
			logRouterInitializationReplies.push_back( transformErrors( throwErrorOr( remoteWorkers.logRouters[i%remoteWorkers.logRouters.size()].logRouter.getReplyUnlessFailedFor( req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		}

		std::vector<Tag> localTags;
		for(auto& tag : allTags) {
			if(remoteLocality == tagLocalitySpecial || remoteLocality == tag.locality || tag.locality < 0) {
				localTags.push_back(tag);
			}
		}

		state vector<Future<TLogInterface>> remoteTLogInitializationReplies;
		vector< InitializeTLogRequest > remoteTLogReqs( remoteWorkers.remoteTLogs.size() );
		for( int i = 0; i < remoteWorkers.remoteTLogs.size(); i++ ) {
			InitializeTLogRequest &req = remoteTLogReqs[i];
			req.recruitmentID = self->recruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->recoverAt.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
			req.remoteTag = Tag(tagLocalityRemoteLog, i);
			req.locality = remoteLocality;
			req.isPrimary = false;
			req.allTags = localTags;
			req.startVersion = logSet->startVersion;
			req.logRouterTags = 0;
		}

		logSet->tLogLocalities.resize( remoteWorkers.remoteTLogs.size() );
		logSet->logServers.resize( remoteWorkers.remoteTLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSet->updateLocalitySet(localities);

		if(oldLogSystem->logRouterTags == 0) {
			std::vector<int> locations;
			for( Tag tag : localTags ) {
				locations.clear();
				logSet->getPushLocations( vector<Tag>(1, tag), locations, 0 );
				for(int loc : locations)
					remoteTLogReqs[ loc ].recoverTags.push_back( tag );
			}
		}

		for( int i = 0; i < remoteWorkers.remoteTLogs.size(); i++ )
			remoteTLogInitializationReplies.push_back( transformErrors( throwErrorOr( remoteWorkers.remoteTLogs[i].tLog.getReplyUnlessFailedFor( remoteTLogReqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		TraceEvent("RemoteLogRecruitment_InitializingRemoteLogs").detail("StartVersion", logSet->startVersion).detail("LocalStart", self->tLogs[0]->startVersion).detail("LogRouterTags", self->logRouterTags);
		wait( waitForAll(remoteTLogInitializationReplies) && waitForAll(logRouterInitializationReplies) && oldRouterRecruitment );

		for( int i = 0; i < logRouterInitializationReplies.size(); i++ ) {
			logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(logRouterInitializationReplies[i].get()) ) ) );
		}

		for( int i = 0; i < remoteTLogInitializationReplies.size(); i++ ) {
			logSet->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(remoteTLogInitializationReplies[i].get()) ) );
			logSet->tLogLocalities[i] = remoteWorkers.remoteTLogs[i].locality;
		}
		filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);

		std::vector<Future<Void>> recoveryComplete;
		for( int i = 0; i < logSet->logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSet->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		self->remoteRecoveryComplete = waitForAll(recoveryComplete);
		self->tLogs.push_back( logSet );
		TraceEvent("RemoteLogRecruitment_CompletingRecovery");
		return Void();
	}

	ACTOR static Future<Reference<ILogSystem>> newEpoch( Reference<TagPartitionedLogSystem> oldLogSystem, RecruitFromConfigurationReply recr, Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount,
		int8_t primaryLocality, int8_t remoteLocality, std::vector<Tag> allTags, Reference<AsyncVar<bool>> recruitmentStalled ) {
		state double startTime = now();
		state Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality) );
		logSystem->logSystemType = 2;
		logSystem->expectedLogSets = 1;
		logSystem->recoveredAt = oldLogSystem->recoverAt;
		logSystem->repopulateRegionAntiQuorum = configuration.repopulateRegionAntiQuorum;
		logSystem->recruitmentID = g_random->randomUniqueID();
		oldLogSystem->recruitmentID = logSystem->recruitmentID;

		if(configuration.usableRegions > 1) {
			logSystem->logRouterTags = recr.tLogs.size() * std::max<int>(1, configuration.desiredLogRouterCount / std::max<int>(1,recr.tLogs.size()));
			logSystem->expectedLogSets++;
		} else {
			logSystem->logRouterTags = 0;
		}

		logSystem->tLogs.push_back( Reference<LogSet>( new LogSet() ) );
		logSystem->tLogs[0]->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		logSystem->tLogs[0]->tLogReplicationFactor = configuration.tLogReplicationFactor;
		logSystem->tLogs[0]->tLogPolicy = configuration.tLogPolicy;
		logSystem->tLogs[0]->isLocal = true;
		logSystem->tLogs[0]->locality = primaryLocality;

		state RegionInfo region = configuration.getRegion(recr.dcId);

		if(region.satelliteTLogReplicationFactor > 0) {
			logSystem->tLogs.push_back( Reference<LogSet>( new LogSet() ) );
			if(recr.satelliteFallback) {
				logSystem->tLogs[1]->tLogWriteAntiQuorum = region.satelliteTLogWriteAntiQuorumFallback;
				logSystem->tLogs[1]->tLogReplicationFactor = region.satelliteTLogReplicationFactorFallback;
				logSystem->tLogs[1]->tLogPolicy = region.satelliteTLogPolicyFallback;
			} else {
				logSystem->tLogs[1]->tLogWriteAntiQuorum = region.satelliteTLogWriteAntiQuorum;
				logSystem->tLogs[1]->tLogReplicationFactor = region.satelliteTLogReplicationFactor;
				logSystem->tLogs[1]->tLogPolicy = region.satelliteTLogPolicy;
			}
			logSystem->tLogs[1]->isLocal = true;
			logSystem->tLogs[1]->locality = tagLocalitySatellite;
			logSystem->tLogs[1]->startVersion = oldLogSystem->knownCommittedVersion + 1;

			logSystem->tLogs[1]->tLogLocalities.resize( recr.satelliteTLogs.size() );
			for(int i = 0; i < recr.satelliteTLogs.size(); i++) {
				logSystem->tLogs[1]->tLogLocalities[i] = recr.satelliteTLogs[i].locality;
			}
			filterLocalityDataForPolicy(logSystem->tLogs[1]->tLogPolicy, &logSystem->tLogs[1]->tLogLocalities);

			logSystem->tLogs[1]->logServers.resize( recr.satelliteTLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
			logSystem->tLogs[1]->updateLocalitySet(logSystem->tLogs[1]->tLogLocalities);
			logSystem->tLogs[1]->populateSatelliteTagLocations(logSystem->logRouterTags,oldLogSystem->logRouterTags);
			logSystem->expectedLogSets++;
		}

		if(oldLogSystem->tLogs.size()) {
			logSystem->oldLogData.push_back(OldLogData());
			logSystem->oldLogData[0].tLogs = oldLogSystem->tLogs;
			logSystem->oldLogData[0].epochEnd = oldLogSystem->knownCommittedVersion + 1;
			logSystem->oldLogData[0].logRouterTags = oldLogSystem->logRouterTags;
		}

		for(int i = 0; i < oldLogSystem->oldLogData.size(); i++) {
			logSystem->oldLogData.push_back(oldLogSystem->oldLogData[i]);
		}

		logSystem->tLogs[0]->startVersion = oldLogSystem->knownCommittedVersion + 1;
		state int lockNum = 0;
		while(lockNum < oldLogSystem->lockResults.size()) {
			if(oldLogSystem->lockResults[lockNum].logSet->locality == primaryLocality) {
				if(oldLogSystem->lockResults[lockNum].isCurrent && oldLogSystem->lockResults[lockNum].logSet->isLocal) {
					break;
				}
				state Future<Void> stalledAfter = setAfter(recruitmentStalled, SERVER_KNOBS->MAX_RECOVERY_TIME, true);
				loop {
					auto versions = TagPartitionedLogSystem::getDurableVersion(logSystem->dbgid, oldLogSystem->lockResults[lockNum]);
					if(versions.present()) {
						logSystem->tLogs[0]->startVersion = std::min(std::min(versions.get().first+1, oldLogSystem->lockResults[lockNum].epochEnd), logSystem->tLogs[0]->startVersion);
						break;
					}
					wait( TagPartitionedLogSystem::getDurableVersionChanged(oldLogSystem->lockResults[lockNum]) );
				}
				stalledAfter.cancel();
				break;
			}
			lockNum++;
		}

		vector<LocalityData> localities;
		localities.resize(recr.tLogs.size());
		for(int i = 0; i < recr.tLogs.size(); i++) {
			localities[i] = recr.tLogs[i].locality;
		}

		state Future<Void> oldRouterRecruitment = Never();
		TraceEvent("NewEpochStartVersion", oldLogSystem->getDebugID()).detail("StartVersion", logSystem->tLogs[0]->startVersion).detail("EpochEnd", oldLogSystem->knownCommittedVersion + 1).detail("Locality", primaryLocality).detail("OldLogRouterTags", oldLogSystem->logRouterTags);
		if(oldLogSystem->logRouterTags > 0 || logSystem->tLogs[0]->startVersion < oldLogSystem->knownCommittedVersion + 1) {
			oldRouterRecruitment = TagPartitionedLogSystem::recruitOldLogRouters(oldLogSystem.getPtr(), recr.oldLogRouters, recoveryCount, primaryLocality, logSystem->tLogs[0]->startVersion, localities, logSystem->tLogs[0]->tLogPolicy, false);
			if(oldLogSystem->knownCommittedVersion - logSystem->tLogs[0]->startVersion > SERVER_KNOBS->MAX_RECOVERY_VERSIONS) {
				//make sure we can recover in the other DC.
				for(auto& lockResult : oldLogSystem->lockResults) {
					if(lockResult.logSet->locality == remoteLocality) {
						if( TagPartitionedLogSystem::getDurableVersion(logSystem->dbgid, lockResult).present() ) {
							recruitmentStalled->set(true);
						}
					}
				}
			}
		} else {
			oldLogSystem->logSystemConfigChanged.trigger();
		}

		std::vector<Tag> localTags;
		for(auto& tag : allTags) {
			if(primaryLocality == tagLocalitySpecial || primaryLocality == tag.locality || tag.locality < 0) {
				localTags.push_back(tag);
			}
		}

		state vector<Future<TLogInterface>> initializationReplies;
		vector< InitializeTLogRequest > reqs( recr.tLogs.size() );
		for( int i = 0; i < recr.tLogs.size(); i++ ) {
			InitializeTLogRequest &req = reqs[i];
			req.recruitmentID = logSystem->recruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->recoverAt.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
			req.locality = primaryLocality;
			req.remoteTag = Tag(tagLocalityRemoteLog, i);
			req.isPrimary = true;
			req.allTags = localTags;
			req.startVersion = logSystem->tLogs[0]->startVersion;
			req.logRouterTags = logSystem->logRouterTags;
		}

		logSystem->tLogs[0]->tLogLocalities.resize( recr.tLogs.size() );
		logSystem->tLogs[0]->logServers.resize( recr.tLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSystem->tLogs[0]->updateLocalitySet(localities);

		for(int i = 0; i < oldLogSystem->logRouterTags; i++) {
			Tag tag = Tag(tagLocalityLogRouter, i);
			reqs[ logSystem->tLogs[0]->bestLocationFor( tag ) ].recoverTags.push_back( tag );
		}
		std::vector<int> locations;
		for( Tag tag : localTags ) {
			locations.clear();
			logSystem->tLogs[0]->getPushLocations( vector<Tag>(1, tag), locations, 0 );
			for(int loc : locations)
				reqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < recr.tLogs.size(); i++ )
			initializationReplies.push_back( transformErrors( throwErrorOr( recr.tLogs[i].tLog.getReplyUnlessFailedFor( reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		state std::vector<Future<Void>> recoveryComplete;

		if(region.satelliteTLogReplicationFactor > 0) {
			std::vector<Tag> satelliteTags;
			satelliteTags.push_back(txsTag);

			state vector<Future<TLogInterface>> satelliteInitializationReplies;
			vector< InitializeTLogRequest > sreqs( recr.satelliteTLogs.size() );
			for( int i = 0; i < recr.satelliteTLogs.size(); i++ ) {
				InitializeTLogRequest &req = sreqs[i];
				req.recruitmentID = logSystem->recruitmentID;
				req.storeType = configuration.tLogDataStoreType;
				req.recoverFrom = oldLogSystem->getLogSystemConfig();
				req.recoverAt = oldLogSystem->recoverAt.get();
				req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
				req.epoch = recoveryCount;
				req.locality = tagLocalitySatellite;
				req.remoteTag = Tag();
				req.isPrimary = true;
				req.allTags = satelliteTags;
				req.startVersion = oldLogSystem->knownCommittedVersion + 1;
				req.logRouterTags = logSystem->logRouterTags;
			}

			for(int i = -1; i < oldLogSystem->logRouterTags; i++) {
				Tag tag = i == -1 ? txsTag : Tag(tagLocalityLogRouter, i);
				locations.clear();
				logSystem->tLogs[1]->getPushLocations( vector<Tag>(1, tag), locations, 0 );
				for(int loc : locations)
					sreqs[ loc ].recoverTags.push_back( tag );
			}

			for( int i = 0; i < recr.satelliteTLogs.size(); i++ )
				satelliteInitializationReplies.push_back( transformErrors( throwErrorOr( recr.satelliteTLogs[i].tLog.getReplyUnlessFailedFor( sreqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

			wait( waitForAll( satelliteInitializationReplies ) || oldRouterRecruitment );

			for( int i = 0; i < satelliteInitializationReplies.size(); i++ ) {
				logSystem->tLogs[1]->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(satelliteInitializationReplies[i].get()) ) );
			}

			for( int i = 0; i < logSystem->tLogs[1]->logServers.size(); i++)
				recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->tLogs[1]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		}

		wait( waitForAll( initializationReplies ) || oldRouterRecruitment );

		for( int i = 0; i < initializationReplies.size(); i++ ) {
			logSystem->tLogs[0]->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(initializationReplies[i].get()) ) );
			logSystem->tLogs[0]->tLogLocalities[i] = recr.tLogs[i].locality;
		}
		filterLocalityDataForPolicy(logSystem->tLogs[0]->tLogPolicy, &logSystem->tLogs[0]->tLogLocalities);

		//Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running recoveries causing tests to timeout
		if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator.speedUpSimulation) throw master_recovery_failed();

		for( int i = 0; i < logSystem->tLogs[0]->logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->tLogs[0]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		logSystem->recoveryComplete = waitForAll(recoveryComplete);

		if(configuration.usableRegions > 1) {
			logSystem->hasRemoteServers = true;
			logSystem->remoteRecovery = TagPartitionedLogSystem::newRemoteEpoch(logSystem.getPtr(), oldLogSystem, fRemoteWorkers, configuration, recoveryCount, remoteLocality, allTags);
			if(oldLogSystem->logRouterTags == 0) {
				//The wait is required so that we know both primary logs and remote logs have copied the data between the known committed version and the recovery version.
				//FIXME: we can remove this wait once we are able to have log routers which can ship data to the remote logs without using log router tags.
				wait(logSystem->remoteRecovery);
			}
		} else {
			logSystem->hasRemoteServers = false;
			logSystem->remoteRecovery = logSystem->recoveryComplete;
			logSystem->remoteRecoveryComplete = logSystem->recoveryComplete;
		}

		return logSystem;
	}

	ACTOR static Future<Void> trackRejoins( UID dbgid, std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers, FutureStream< struct TLogRejoinRequest > rejoinRequests ) {
		state std::map<UID,ReplyPromise<bool>> lastReply;

		try {
			loop {
				TLogRejoinRequest req = waitNext( rejoinRequests );
				int pos = -1;
				for( int i = 0; i < logServers.size(); i++ ) {
					if( logServers[i]->get().id() == req.myInterface.id() ) {
						pos = i;
						break;
					}
				}
				if ( pos != -1 ) {
					TraceEvent("TLogJoinedMe", dbgid).detail("TLog", req.myInterface.id()).detail("Address", req.myInterface.commit.getEndpoint().address.toString());
					if( !logServers[pos]->get().present() || req.myInterface.commit.getEndpoint() != logServers[pos]->get().interf().commit.getEndpoint())
						logServers[pos]->setUnconditional( OptionalInterface<TLogInterface>(req.myInterface) );
					lastReply[req.myInterface.id()].send(false);
					lastReply[req.myInterface.id()] = req.reply;
				}
				else {
					TraceEvent("TLogJoinedMeUnknown", dbgid).detail("TLog", req.myInterface.id()).detail("Address", req.myInterface.commit.getEndpoint().address.toString());
					req.reply.send(true);
				}
			}
		} catch (...) {
			for( auto it = lastReply.begin(); it != lastReply.end(); ++it)
				it->second.send(true);
			throw;
		}
	}

	ACTOR static Future<TLogLockResult> lockTLog( UID myID, Reference<AsyncVar<OptionalInterface<TLogInterface>>> tlog ) {
		TraceEvent("TLogLockStarted", myID).detail("TLog", tlog->get().id());
		loop {
			choose {
				when (TLogLockResult data = wait( tlog->get().present() ? brokenPromiseToNever( tlog->get().interf().lock.getReply<TLogLockResult>() ) : Never() )) {
					TraceEvent("TLogLocked", myID).detail("TLog", tlog->get().id()).detail("End", data.end);
					return data;
				}
				when (wait(tlog->onChange())) {}
			}
		}
	}

	//FIXME: disabled during merge, update and use in epochEnd()
	/*
	static void lockMinimalTLogSet(const UID& dbgid, const DBCoreState& prevState,
	                               const std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>>& logServers,
	                               const std::vector<Reference<AsyncVar<bool>>>& logFailed,
	                               vector<Future<TLogLockResult>>* tLogReply ) {
		// Invariant: tLogReply[i] must correspond to the tlog stored as logServers[i].
		ASSERT(tLogReply->size() == prevState.tLogLocalities.size());
		ASSERT(logFailed.size() == tLogReply->size());

		// For any given index, only one of the following will be true.
		auto locking_completed = [&logFailed, tLogReply](int index) {
			const auto& entry = tLogReply->at(index);
			return !logFailed[index]->get() && entry.isValid() && entry.isReady() && !entry.isError();
		};
		auto locking_failed = [&logFailed, tLogReply](int index) {
			const auto& entry = tLogReply->at(index);
			return logFailed[index]->get() || (entry.isValid() && entry.isReady() && entry.isError());
		};
		auto locking_pending = [&logFailed, tLogReply](int index) {
			const auto& entry = tLogReply->at(index);
			return !logFailed[index]->get() && (entry.isValid() && !entry.isReady());
		};
		auto locking_skipped = [&logFailed, tLogReply](int index) {
			const auto& entry = tLogReply->at(index);
			return !logFailed[index]->get() && !entry.isValid();
		};

		auto can_obtain_quorum = [&prevState](std::function<bool(int)> filter) {
			LocalityGroup filter_true;
			std::vector<LocalityData> filter_false, unused;
			for (int i = 0; i < prevState.tLogLocalities.size() ; i++) {
				if (filter(i)) {
					filter_true.add(prevState.tLogLocalities[i]);
				} else {
					filter_false.push_back(prevState.tLogLocalities[i]);
				}
			}
			bool valid = filter_true.validate(prevState.tLogPolicy);
			if (!valid && prevState.tLogWriteAntiQuorum > 0 ) {
				valid = !validateAllCombinations(unused, filter_true, prevState.tLogPolicy, filter_false, prevState.tLogWriteAntiQuorum, false);
			}
			return valid;
		};

		// Step 1: Verify that if all the failed TLogs come back, they can't form a quorum.
		if (can_obtain_quorum(locking_failed)) {
			TraceEvent(SevInfo, "MasterRecoveryTLogLockingImpossible", dbgid);
			return;
		}

		// Step 2: It's possible for us to succeed, but we need to lock additional logs.
		//
		// First, we need an accurate picture of what TLogs we're capable of locking. We can't tell the
		// difference between a temporarily failed TLog and a permanently failed TLog. Thus, we assume
		// all failures are permanent, and manually re-issue lock requests if they rejoin.
		for (int i = 0; i < logFailed.size(); i++) {
			const auto& r = tLogReply->at(i);
			TEST(locking_failed(i) && (r.isValid() && !r.isReady()));  // A TLog failed with a pending request.
			// The reboot_a_tlog BUGGIFY below should cause the above case to be hit.
			if (locking_failed(i)) {
				tLogReply->at(i) = Future<TLogLockResult>();
			}
		}

		// We're trying to paritition the set of old tlogs into two sets, L and R, such that:
		// (1). R does not validate the policy
		// (2). |R| is as large as possible
		// (3). L contains all the already-locked TLogs
		// and then we only issue lock requests to TLogs in L. This is safe, as R does not have quorum,
		// so no commits may occur.  It does not matter if L forms a quorum or not.
		//
		// We form these sets by starting with L as all machines and R as the empty set, and moving a
		// random machine from L to R until (1) or (2) no longer holds as true. Code-wise, L is
		// [0..end-can_omit), and R is [end-can_omit..end), and we move a random machine via randomizing
		// the order of the tlogs. Choosing a random machine was verified to generate a good-enough
		// result to be interesting intests sufficiently frequently that we don't need to try to
		// calculate the exact optimal solution.
		std::vector<std::pair<LocalityData, int>> tlogs;
		for (int i = 0; i < prevState.tLogLocalities.size(); i++) {
			tlogs.emplace_back(prevState.tLogLocalities[i], i);
		}
		g_random->randomShuffle(tlogs);
		// Rearrange the array such that things that the left is logs closer to being locked, and
		// the right is logs that can't be locked.  This makes us prefer locking already-locked TLogs,
		// which is how we respect the decisions made in the previous execution.
		auto idx_to_order = [&locking_completed, &locking_failed, &locking_pending, &locking_skipped](int index) {
			bool complete = locking_completed(index);
			bool pending = locking_pending(index);
			bool skipped = locking_skipped(index);
			bool failed = locking_failed(index);

			ASSERT( complete + pending + skipped + failed == 1 );

			if (complete) return 0;
			if (pending) return 1;
			if (skipped) return 2;
			if (failed) return 3;

			ASSERT(false);  // Programmer error.
			return -1;
		};
		std::sort(tlogs.begin(), tlogs.end(),
		    // TODO: Change long type to `auto` once toolchain supports C++17.
		    [&idx_to_order](const std::pair<LocalityData, int>& lhs, const std::pair<LocalityData, int>& rhs) {
		    	return idx_to_order(lhs.second) < idx_to_order(rhs.second);
		    });

		// Indexes that aren't in the vector are the ones we're considering omitting. Remove indexes until
		// the removed set forms a quorum.
		int can_omit = 0;
		std::vector<int> to_lock_indexes;
		for (auto it = tlogs.cbegin() ; it != tlogs.cend() - 1 ; it++ ) {
			to_lock_indexes.push_back(it->second);
		}
		auto filter = [&to_lock_indexes](int index) {
			return std::find(to_lock_indexes.cbegin(), to_lock_indexes.cend(), index) == to_lock_indexes.cend();
		};
		while(true) {
			if (can_obtain_quorum(filter)) {
				break;
			} else {
				can_omit++;
				ASSERT(can_omit < tlogs.size());
				to_lock_indexes.pop_back();
			}
		}

		if (prevState.tLogReplicationFactor - prevState.tLogWriteAntiQuorum == 1) {
			ASSERT(can_omit == 0);
		}
		// Our previous check of making sure there aren't too many failed logs should have prevented this.
		ASSERT(!locking_failed(tlogs[tlogs.size()-can_omit-1].second));

		// If we've managed to leave more tlogs unlocked than (RF-AQ), it means we've hit the case
		// where the policy engine has allowed us to have multiple logs in the same failure domain
		// with independant sets of data. This case will validated that no code is relying on the old
		// quorum=(RF-AQ) logic, and now goes through the policy engine instead.
		TEST(can_omit >= prevState.tLogReplicationFactor - prevState.tLogWriteAntiQuorum);  // Locking a subset of the TLogs while ending an epoch.
		const bool reboot_a_tlog = g_network->now() - g_simulator.lastConnectionFailure > g_simulator.connectionFailuresDisableDuration && BUGGIFY && g_random->random01() < 0.25;
		TraceEvent(SevInfo, "MasterRecoveryTLogLocking", dbgid)
		    detail("Locks", tlogs.size() - can_omit)
		    detail("Skipped", can_omit)
		    detail("Replication", prevState.tLogReplicationFactor)
		    detail("Antiquorum", prevState.tLogWriteAntiQuorum)
		    detail("RebootBuggify", reboot_a_tlog);
		for (int i = 0; i < tlogs.size() - can_omit; i++) {
			const int index = tlogs[i].second;
			Future<TLogLockResult>& entry = tLogReply->at(index);
			if (!entry.isValid()) {
				entry = lockTLog( dbgid, logServers[index] );
			}
		}
		if (reboot_a_tlog) {
			g_simulator.lastConnectionFailure = g_network->now();
			for (int i = 0; i < tlogs.size() - can_omit; i++) {
				const int index = tlogs[i].second;
				if (logServers[index]->get().present()) {
					g_simulator.rebootProcess(
					    g_simulator.getProcessByAddress(
					        logServers[index]->get().interf().address()),
					    ISimulator::RebootProcess);
					break;
				}
			}
		}
		// Intentionally leave `tlogs.size() - can_omit` .. `tlogs.size()` as !isValid() Futures.
	}*/

	template <class T>
	static vector<T> getReadyNonError( vector<Future<T>> const& futures ) {
		// Return the values of those futures which have (non-error) values ready
		std::vector<T> result;
		for(auto& f : futures)
			if (f.isReady() && !f.isError())
				result.push_back(f.get());
		return result;
	}

	struct sort_by_end {
		bool operator ()(TLogLockResult const&a, TLogLockResult const& b) const { return a.end < b.end; }
	};
};

Future<Void> ILogSystem::recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality, bool forceRecovery) {
	return TagPartitionedLogSystem::recoverAndEndEpoch( outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery );
}

Reference<ILogSystem> ILogSystem::fromLogSystemConfig( UID const& dbgid, struct LocalityData const& locality, struct LogSystemConfig const& conf, bool excludeRemote, bool useRecoveredAt, Optional<PromiseStream<Future<Void>>> addActor ) {
	if (conf.logSystemType == 0)
		return Reference<ILogSystem>();
	else if (conf.logSystemType == 2)
		return TagPartitionedLogSystem::fromLogSystemConfig( dbgid, locality, conf, excludeRemote, useRecoveredAt, addActor );
	else
		throw internal_error();
}

Reference<ILogSystem> ILogSystem::fromOldLogSystemConfig( UID const& dbgid, struct LocalityData const& locality, struct LogSystemConfig const& conf ) {
	if (conf.logSystemType == 0)
		return Reference<ILogSystem>();
	else if (conf.logSystemType == 2)
		return TagPartitionedLogSystem::fromOldLogSystemConfig( dbgid, locality, conf );
	else
		throw internal_error();
}

Reference<ILogSystem> ILogSystem::fromServerDBInfo( UID const& dbgid, ServerDBInfo const& dbInfo, bool useRecoveredAt, Optional<PromiseStream<Future<Void>>> addActor ) {
	return fromLogSystemConfig( dbgid, dbInfo.myLocality, dbInfo.logSystemConfig, false, useRecoveredAt, addActor );
}
