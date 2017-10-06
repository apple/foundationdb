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

#include "flow/actorcompiler.h"
#include "flow/ActorCollection.h"
#include "LogSystem.h"
#include "ServerDBInfo.h"
#include "DBCoreState.h"
#include "WaitFailure.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"

ACTOR static Future<Void> reportTLogCommitErrors( Future<Void> commitReply, UID debugID ) {
	try {
		Void _ = wait(commitReply);
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise)
			throw master_tlog_failed();
		else if (e.code() != error_code_actor_cancelled && e.code() != error_code_tlog_stopped)
			TraceEvent(SevError, "MasterTLogCommitRequestError", debugID).error(e);
		throw;
	}
}

struct OldLogData {
	std::vector<Reference<LogSet>> tLogs;
	Version epochEnd;

	OldLogData() : epochEnd(0) {}
};

struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
	UID dbgid;
	int logSystemType;
	std::vector<Reference<LogSet>> tLogs;
	int expectedLogSets;
	int minRouters;

	// new members
	Future<Void> rejoins;
	Future<Void> recoveryComplete;
	Future<Void> remoteRecovery;
	Future<Void> remoteRecoveryComplete;
	bool recoveryCompleteWrittenToCoreState;
	bool remoteLogsWrittenToCoreState;

	Optional<Version> epochEndVersion;
	std::set< Tag > epochEndTags;
	Version knownCommittedVersion;
	LocalityData locality;
	std::map< std::pair<UID, Tag>, Version > outstandingPops;  // For each currently running popFromLog actor, (log server #, tag)->popped version
	ActorCollection actors;
	std::vector<OldLogData> oldLogData;

	TagPartitionedLogSystem( UID dbgid, LocalityData locality ) : dbgid(dbgid), locality(locality), actors(false), recoveryCompleteWrittenToCoreState(false), remoteLogsWrittenToCoreState(false), logSystemType(0), minRouters(std::numeric_limits<int>::max()), expectedLogSets(0) {}

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
			result = format("%d: ", i);
			for( int j = 0; j < tLogs[i]->logServers.size(); j++) {
				result = result + tLogs[i]->logServers[j]->get().id().toString() + ((j == tLogs[i]->logServers.size() - 1) ? " " : ", ");
			}
		}
		return result;
	}

	virtual UID getDebugID() {
		return dbgid;
	}

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality) {
		return epochEnd( outLogSystem, dbgid, oldState, rejoins, locality );
	}

	static Reference<ILogSystem> fromLogSystemConfig( UID const& dbgid, LocalityData const& locality, LogSystemConfig const& lsConf ) {
		ASSERT( lsConf.logSystemType == 2 || (lsConf.logSystemType == 0 && !lsConf.tLogs.size()) );
		//ASSERT(lsConf.epoch == epoch);  //< FIXME
		Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

		logSystem->tLogs.resize(lsConf.tLogs.size());
		logSystem->expectedLogSets = lsConf.expectedLogSets;
		logSystem->minRouters = lsConf.minRouters;
		for( int i = 0; i < lsConf.tLogs.size(); i++ ) {
			Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
			logSystem->tLogs[i] = logSet;
			TLogSet const& tLogSet = lsConf.tLogs[i];
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
			logSet->hasBest = tLogSet.hasBest;
			logSet->locality = tLogSet.locality;
			logSet->updateLocalitySet();
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
				logSet->hasBest = tLogData.hasBest;
				logSet->locality = tLogData.locality;
				//logSet.UpdateLocalitySet(); we do not update the locality set, since we never push to old logs
			}
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
				logSet->hasBest = tLogSet.hasBest;
				logSet->locality = tLogSet.locality;
				//logSet->updateLocalitySet(); we do not update the locality set, since we never push to old logs
			}
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
					logSet->hasBest = tLogSet.hasBest;
					logSet->locality = tLogSet.locality;
					//logSet->updateLocalitySet(); we do not update the locality set, since we never push to old logs
				}
				logSystem->oldLogData[i-1].epochEnd = lsConf.oldTLogs[i].epochEnd;
			}
		}
		logSystem->logSystemType = lsConf.logSystemType;

		return logSystem;
	}

	virtual void toCoreState( DBCoreState& newState ) {
		if( recoveryComplete.isValid() && recoveryComplete.isError() )
			throw recoveryComplete.getError();

		newState.tLogs.clear();
		for(auto &t : tLogs) {
			CoreTLogSet coreSet;
			for(auto &log : t->logServers) {
				coreSet.tLogs.push_back(log->get().id());
				coreSet.tLogLocalities.push_back(log->get().interf().locality);
			}
			coreSet.tLogWriteAntiQuorum = t->tLogWriteAntiQuorum;
			coreSet.tLogReplicationFactor = t->tLogReplicationFactor;
			coreSet.tLogPolicy = t->tLogPolicy;
			coreSet.isLocal = t->isLocal;
			coreSet.hasBest = t->hasBest;
			coreSet.locality = t->locality;
			newState.tLogs.push_back(coreSet);
		}

		newState.oldTLogData.clear();
		if(!recoveryComplete.isValid() || !recoveryComplete.isReady()) {
			newState.oldTLogData.resize(oldLogData.size());
			for(int i = 0; i < oldLogData.size(); i++) {
				for(auto &t : oldLogData[i].tLogs) {
					CoreTLogSet coreSet;
					for(auto &log : t->logServers) {
						coreSet.tLogs.push_back(log->get().id());	
					}
					coreSet.tLogLocalities = t->tLogLocalities;
					coreSet.tLogWriteAntiQuorum = t->tLogWriteAntiQuorum;
					coreSet.tLogReplicationFactor = t->tLogReplicationFactor;
					coreSet.tLogPolicy = t->tLogPolicy;
					coreSet.isLocal = t->isLocal;
					coreSet.hasBest = t->hasBest;
					coreSet.locality = t->locality;
					newState.oldTLogData[i].tLogs.push_back(coreSet);
				}
				newState.oldTLogData[i].epochEnd = oldLogData[i].epochEnd;
			}
		}

		newState.logSystemType = logSystemType;
	}

	virtual Future<Void> onCoreStateChanged() {
		ASSERT(recoveryComplete.isValid() && remoteRecovery.isValid() );
		if( recoveryComplete.isReady() && remoteRecovery.isReady() ) {
			return Never();
		}
		if( remoteRecovery.isReady() ) {
			return recoveryComplete;
		}
		if( recoveryComplete.isReady() ) {
			return remoteRecovery;
		}
		return recoveryComplete || remoteRecovery;
	}

	virtual void coreStateWritten( DBCoreState const& newState ) {
		if( !newState.oldTLogData.size() ) {
			recoveryCompleteWrittenToCoreState = true;
		}
		for(auto& t : newState.tLogs) {
			if(!t.isLocal) {
				remoteLogsWrittenToCoreState = true;
				break;
			}
		}
	}

	virtual Future<Void> onError() {
		// Never returns normally, but throws an error if the subsystem stops working
		// FIXME: Run waitFailureClient on the master instead of these onFailedFor?
		vector<Future<Void>> failed;

		for( auto& it : tLogs ) {
			for(auto &t : it->logServers) {
				if( t->get().present() ) {
					failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
				}
			}
			for(auto &t : it->logRouters) {
				if( t->get().present() ) {
					failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
				}
			}
		}

		ASSERT( failed.size() >= 1 );
		return tagError<Void>( quorum( failed, 1 ), master_tlog_failed() ) || actors.getResult();
	}

	virtual Future<Void> push( Version prevVersion, Version version, Version knownCommittedVersion, LogPushData& data, Optional<UID> debugID ) {
		// FIXME: Randomize request order as in LegacyLogSystem?
		vector<Future<Void>> quorumResults;
		int location = 0;
		for(auto& it : tLogs) {
			if(it->isLocal) {
				vector<Future<Void>> tLogCommitResults;
				for(int loc=0; loc< it->logServers.size(); loc++) {
					Future<Void> commitMessage = reportTLogCommitErrors(
							it->logServers[loc]->get().interf().commit.getReply(
								TLogCommitRequest( data.getArena(), prevVersion, version, knownCommittedVersion, data.getMessages(location), data.getTags(location), debugID ), TaskTLogCommitReply ),
							getDebugID());
					actors.add(commitMessage);
					tLogCommitResults.push_back(commitMessage);
					location++;
				}
				quorumResults.push_back( quorum( tLogCommitResults, tLogCommitResults.size() - it->tLogWriteAntiQuorum ) );
			}
		}
		
		return waitForAll(quorumResults);
	}

	virtual Reference<IPeekCursor> peek( Version begin, Tag tag, bool parallelGetMore ) {
		if(tag.locality == tagLocalityRemoteLog) {
			if(tLogs.size() < 2) {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
			}

			//FIXME: non-static logRouters
			return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[1]->logRouters, -1, (int)tLogs[1]->logRouters.size(), tag, begin, getPeekEnd(), false ) );
		} else {
			if(tLogs.size() < 1) {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
			}
			int bestSet = -1;
			for(int t = 0; t < tLogs.size(); t++) {
				if(tLogs[t]->hasBest && (tLogs[t]->locality == tag.locality || tag.locality == tagLocalitySpecial)) {
					bestSet = t;
					break;
				}
			}
			if(oldLogData.size() == 0 || begin >= oldLogData[0].epochEnd) {
				return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( tLogs, std::max(0,bestSet), 
					bestSet >= 0 ? ( tLogs[bestSet]->logServers.size() ? tLogs[bestSet]->bestLocationFor( tag ) : -1 ) : -1, tag, begin, getPeekEnd(), parallelGetMore ) );
			} else {
				std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
				std::vector< LogMessageVersion > epochEnds;
				cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( tLogs, std::max(0,bestSet), 
					bestSet >= 0 ? ( tLogs[bestSet]->logServers.size() ? tLogs[bestSet]->bestLocationFor( tag ) : -1 ) : -1, tag, oldLogData[0].epochEnd, getPeekEnd(), parallelGetMore)) );
				for(int i = 0; i < oldLogData.size() && begin < oldLogData[i].epochEnd; i++) {
					int bestOldSet = -1;
					for(int t = 0; t < oldLogData[i].tLogs.size(); t++) {
						if(oldLogData[i].tLogs[t]->hasBest && (oldLogData[i].tLogs[t]->locality == tag.locality || tag.locality == tagLocalitySpecial)) {
							bestOldSet = t;
							break;
						}
					}
					cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( oldLogData[i].tLogs, std::max(0,bestOldSet), 
						bestOldSet >= 0 ? ( oldLogData[i].tLogs[bestOldSet]->logServers.size() ? oldLogData[i].tLogs[bestOldSet]->bestLocationFor( tag ) : -1 ) : -1, tag, i+1 == oldLogData.size() ? begin : std::max(oldLogData[i+1].epochEnd, begin), oldLogData[i].epochEnd, parallelGetMore)) );
					epochEnds.push_back(LogMessageVersion(oldLogData[i].epochEnd));
				}

				return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
			}
		}
	}

	virtual Reference<IPeekCursor> peekSingle( Version begin, Tag tag ) {
		if(tag.locality == tagLocalityLogRouter) {
			if(tLogs.size() < 1) {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
			}
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[0]->logServers.size() ?
					tLogs[0]->logServers[tLogs[0]->bestLocationFor( tag )] :
					Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		} else {
			int bestSet = -1;
			for(int t = 0; t < tLogs.size(); t++) {
				if(tLogs[t]->hasBest && (tLogs[t]->locality == tag.locality || tag.locality == tagLocalitySpecial)) {
					bestSet = t;
					break;
				}
			}
			
			if(tLogs.size() < 1 || bestSet < 0) {
				return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
			}
			
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[bestSet]->logServers.size() ?
				tLogs[bestSet]->logServers[tLogs[bestSet]->bestLocationFor( tag )] :
				Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		}
	}

	virtual void pop( Version upTo, Tag tag ) {
		if (!upTo) return;
		for(auto& t : tLogs) {
			std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>>& popServers = tag.locality == tagLocalityRemoteLog ? t->logRouters : t->logServers;
			for(auto& log : popServers) {
				Version prev = outstandingPops[std::make_pair(log->get().id(),tag)];
				if (prev < upTo)
					outstandingPops[std::make_pair(log->get().id(),tag)] = upTo;
				if (prev == 0)
					actors.add( popFromLog( this, log, tag ) );
			}
		}
	}

	ACTOR static Future<Void> popFromLog( TagPartitionedLogSystem* self, Reference<AsyncVar<OptionalInterface<TLogInterface>>> log, Tag tag ) {
		state Version last = 0;
		loop {
			Void _ = wait( delay(1.0) );  //< FIXME: knob

			state Version to = self->outstandingPops[ std::make_pair(log->get().id(),tag) ];

			if (to <= last) {
				self->outstandingPops.erase( std::make_pair(log->get().id(),tag) );
				return Void();
			}

			try {
				if( !log->get().present() )
					return Void();
				Void _ = wait(log->get().interf().popMessages.getReply( TLogPopRequest( to, tag ) ) );

				last = to;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
				TraceEvent( (e.code() == error_code_broken_promise) ? SevInfo : SevError, "LogPopError", self->dbgid ).detail("Log", log->get().id()).error(e);
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

		Void _ = wait( quorum( alive, std::min(logSet->tLogReplicationFactor, numPresent - logSet->tLogWriteAntiQuorum) ) );

		loop {
			LocalityGroup locked;
			std::vector<LocalityData> unlocked, unused;
			for (int i = 0; i < alive.size(); i++) {
				if (alive[i].isReady() && !alive[i].isError()) {
					locked.add(logSet->tLogLocalities[i]);
				} else {
					unlocked.push_back(logSet->tLogLocalities[i]);
				}
			}
			bool quorum_obtained = locked.validate(logSet->tLogPolicy);
			if (!quorum_obtained && logSet->tLogWriteAntiQuorum != 0) {
				quorum_obtained = !validateAllCombinations(unused, locked, logSet->tLogPolicy, unlocked, logSet->tLogWriteAntiQuorum, false);
			}
			if (logSet->tLogReplicationFactor - logSet->tLogWriteAntiQuorum == 1 && locked.size() > 0) {
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
			Void _ = wait( waitForAny(changes) );
		}
	}

	virtual Future<Void> confirmEpochLive(Optional<UID> debugID) {
		// Returns success after confirming that pushes in the current epoch are still possible
		// FIXME: This is way too conservative?
		vector<Future<Void>> quorumResults;
		for(auto& it : tLogs) {
			if(it->isLocal) {
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

	virtual Future<Reference<ILogSystem>> newEpoch( RecruitFromConfigurationReply const& recr, Future<RecruitRemoteFromConfigurationReply> const& fRemoteWorkers, DatabaseConfiguration const& config, LogEpoch recoveryCount, int8_t primaryLocality, int8_t remoteLocality ) {
		// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.  The new epoch
		// is only provisional until the caller updates the coordinated DBCoreState
		return newEpoch( Reference<TagPartitionedLogSystem>::addRef(this), recr, fRemoteWorkers, config, recoveryCount, primaryLocality, remoteLocality );
	}

	virtual LogSystemConfig getLogSystemConfig() {
		LogSystemConfig logSystemConfig;
		logSystemConfig.logSystemType = logSystemType;
		logSystemConfig.expectedLogSets = expectedLogSets;
		logSystemConfig.minRouters = minRouters;
		
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
				log.hasBest = logSet->hasBest;
				log.locality = logSet->locality;

				for( int i = 0; i < logSet->logServers.size(); i++ ) {
					log.tLogs.push_back(logSet->logServers[i]->get());
				}

				for( int i = 0; i < logSet->logRouters.size(); i++ ) {
					log.logRouters.push_back(logSet->logRouters[i]->get());
				}
			}
		}

		if(!recoveryCompleteWrittenToCoreState) {
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
					log.hasBest = logSet->hasBest;
					log.locality = logSet->locality;

					for( int i = 0; i < logSet->logServers.size(); i++ ) {
						log.tLogs.push_back(logSet->logServers[i]->get());
					}

					for( int i = 0; i < logSet->logRouters.size(); i++ ) {
						log.logRouters.push_back(logSet->logRouters[i]->get());
					}
				}
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
		if(!recoveryCompleteWrittenToCoreState) {
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
		changes.push_back(Never());
		for(auto& t : tLogs) {
			for( int i = 0; i < t->logServers.size(); i++ ) {
				changes.push_back( t->logServers[i]->onChange() );
			}
		}
		for( int i = 0; i < oldLogData.size(); i++ ) {
			for(auto& t : oldLogData[i].tLogs) {
				for( int j = 0; j < t->logServers.size(); j++ ) {
					changes.push_back( t->logServers[j]->onChange() );
				}
			}
		}

		return waitForAny(changes);
	}

	virtual Version getEnd() {
		ASSERT( epochEndVersion.present() );
		return epochEndVersion.get() + 1;
	}

	Version getPeekEnd() {
		if (epochEndVersion.present())
			return getEnd();
		else
			return std::numeric_limits<Version>::max();
	}

	virtual void addRemoteTags( int logSet, std::vector<Tag> const& originalTags, std::vector<int>& tags ) {
		tLogs[logSet]->getPushLocations(originalTags, tags, 0);
	}

	virtual void getPushLocations( std::vector<Tag> const& tags, std::vector<int>& locations ) {
		int locationOffset = 0;
		for(auto& log : tLogs) {
			if(log->isLocal) {
				log->getPushLocations(tags, locations, locationOffset);
				locationOffset += log->logServers.size();
			}
		}
	}

	virtual Tag getRandomRouterTag() {
		ASSERT(minRouters < 1e6);
		return Tag(tagLocalityLogRouter, g_random->randomInt(0, minRouters));
	}

	std::set< Tag > const& getEpochEndTags() const { return epochEndTags; }

	ACTOR static Future<Void> monitorLog(Reference<AsyncVar<OptionalInterface<TLogInterface>>> logServer, Reference<AsyncVar<bool>> failed) {
		state Future<Void> waitFailure;
		loop {
			if(logServer->get().present())
				waitFailure = waitFailureTracker( logServer->get().interf().waitFailure, failed );
			else
				failed->set(true);
			Void _ = wait( logServer->onChange() );
		}
	}

	ACTOR static Future<Void> epochEnd( Reference<AsyncVar<Reference<ILogSystem>>> outLogSystem, UID dbgid, DBCoreState prevState, FutureStream<TLogRejoinRequest> rejoinRequests, LocalityData locality ) {
		// Stops a co-quorum of tlogs so that no further versions can be committed until the DBCoreState coordination state is changed
		// Creates a new logSystem representing the (now frozen) epoch
		// No other important side effects.
		// The writeQuorum in the master info is from the previous configuration

		if (!prevState.tLogs.size()) {
			// This is a brand new database
			Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );
			logSystem->logSystemType = prevState.logSystemType;
			logSystem->epochEndVersion = 0;
			logSystem->knownCommittedVersion = 0;
			outLogSystem->set(logSystem);
			Void _ = wait( Future<Void>(Never()) );
			throw internal_error();
		}

		TEST( true );	// Master recovery from pre-existing database

		// trackRejoins listens for rejoin requests from the tLogs that we are recovering from, to learn their TLogInterfaces
		state std::vector<std::vector<Future<TLogLockResult>>> tLogReply;
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
			logSet->hasBest = coreSet.hasBest;
			logSet->locality = coreSet.locality;
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
				logSet->hasBest = log.hasBest;
				logSet->locality = log.locality;
			}
			oldData.epochEnd = old.epochEnd;
		}
		state Future<Void> rejoins = trackRejoins( dbgid, allLogServers, rejoinRequests );

		tLogReply.resize(logServers.size());
		for( int i=0; i < logServers.size(); i++ ) {
			for(int t=0; t<logServers[i]->logServers.size(); t++) {
				tLogReply[i].push_back( lockTLog( dbgid, logServers[i]->logServers[t]) );
			}
		}

		state Optional<Version> last_end;
		state int cycles = 0;

		loop {
			Optional<Version> end;
			Version knownCommittedVersion = 0;
			for(int log = 0; log < logServers.size(); log++) {
				if(!logServers[log]->isLocal) {
					continue;
				}

				// To ensure consistent recovery, the number of servers NOT in the write quorum plus the number of servers NOT in the read quorum
				// have to be strictly less than the replication factor.  Otherwise there could be a replica set consistent entirely of servers that
				// are out of date due to not being in the write quorum or unavailable due to not being in the read quorum.
				// So with N = # of tlogs, W = antiquorum, R = required count, F = replication factor,
				// W + (N - R) < F, and optimally (N-W)+(N-R)=F-1.  Thus R=N+1-F+W.
				state int requiredCount = (int)logServers[log]->logServers.size()+1 - logServers[log]->tLogReplicationFactor + logServers[log]->tLogWriteAntiQuorum;
				ASSERT( requiredCount > 0 && requiredCount <= logServers[log]->logServers.size() );
				ASSERT( logServers[log]->tLogReplicationFactor >= 1 && logServers[log]->tLogReplicationFactor <= logServers[log]->logServers.size() );
				ASSERT( logServers[log]->tLogWriteAntiQuorum >= 0 && logServers[log]->tLogWriteAntiQuorum < logServers[log]->logServers.size() );
			
				std::vector<LocalityData> availableItems, badCombo;
				std::vector<TLogLockResult> results;
				std::string sServerState;
				LocalityGroup unResponsiveSet;
				double t = timer();
				cycles++;

				for(int t=0; t<logServers[log]->logServers.size(); t++) {
					if (tLogReply[log][t].isReady() && !tLogReply[log][t].isError() && !logFailed[log][t]->get()) {
						results.push_back(tLogReply[log][t].get());
						availableItems.push_back(logServers[log]->tLogLocalities[t]);
						sServerState += 'a';
					}
					else {
						unResponsiveSet.add(logServers[log]->tLogLocalities[t]);
						sServerState += 'f';
					}
				}

				// Check if the list of results is not larger than the anti quorum
				bool bTooManyFailures = (results.size() <= logServers[log]->tLogWriteAntiQuorum);

				// Check if failed logs complete the policy
				bTooManyFailures = bTooManyFailures || ((unResponsiveSet.size() >= logServers[log]->tLogReplicationFactor)	&& (unResponsiveSet.validate(logServers[log]->tLogPolicy)));

				// Check all combinations of the AntiQuorum within the failed
				if ((!bTooManyFailures) && (logServers[log]->tLogWriteAntiQuorum) && (!validateAllCombinations(badCombo, unResponsiveSet, logServers[log]->tLogPolicy, availableItems, logServers[log]->tLogWriteAntiQuorum, false))) {
					TraceEvent("EpochEndBadCombo", dbgid).detail("Cycles", cycles)
						.detail("logNum", log)
						.detail("Required", requiredCount)
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("Absent", logServers[log]->logServers.size() - results.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", logServers[log]->tLogReplicationFactor)
						.detail("AntiQuorum", logServers[log]->tLogWriteAntiQuorum)
						.detail("Policy", logServers[log]->tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LogZones", ::describeZones(logServers[log]->tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(logServers[log]->tLogLocalities));
					bTooManyFailures = true;
				}

				// If too many TLogs are failed for recovery to be possible, we could wait forever here.
				//Void _ = wait( smartQuorum( tLogReply, requiredCount, SERVER_KNOBS->RECOVERY_TLOG_SMART_QUORUM_DELAY ) || rejoins );

				ASSERT(logServers[log]->logServers.size() == tLogReply[log].size());
				if (!bTooManyFailures) {
					std::sort( results.begin(), results.end(), sort_by_end() );
					int absent = logServers[log]->logServers.size() - results.size();
					int safe_range_begin = logServers[log]->tLogWriteAntiQuorum;
					int new_safe_range_begin = std::min(logServers[log]->tLogWriteAntiQuorum, (int)(results.size()-1));
					int safe_range_end = logServers[log]->tLogReplicationFactor - absent;

					if( ( prevState.logSystemType == 2 && (!last_end.present() || ((safe_range_end > 0) && (safe_range_end-1 < results.size()) && results[ safe_range_end-1 ].end < last_end.get())) ) ) {
						knownCommittedVersion = std::max(knownCommittedVersion, results[ new_safe_range_begin ].end - (g_network->isSimulated() ? 10*SERVER_KNOBS->VERSIONS_PER_SECOND : SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)); //In simulation this must be the maximum MAX_READ_TRANSACTION_LIFE_VERSIONS
						for(int i = 0; i < results.size(); i++) {
							knownCommittedVersion = std::max(knownCommittedVersion, results[i].knownCommittedVersion);
						}

						TraceEvent("LogSystemRecovery", dbgid).detail("Cycles", cycles)
							.detail("logNum", log)
							.detail("TotalServers", logServers[log]->logServers.size())
							.detail("Required", requiredCount)
							.detail("Present", results.size())
							.detail("Available", availableItems.size())
							.detail("Absent", logServers[log]->logServers.size() - results.size())
							.detail("ServerState", sServerState)
							.detail("ReplicationFactor", logServers[log]->tLogReplicationFactor)
							.detail("AntiQuorum", logServers[log]->tLogWriteAntiQuorum)
							.detail("Policy", logServers[log]->tLogPolicy->info())
							.detail("TooManyFailures", bTooManyFailures)
							.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
							.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
							.detail("EndVersion", results[ new_safe_range_begin ].end)
							.detail("SafeBegin", safe_range_begin)
							.detail("SafeEnd", safe_range_end)
							.detail("NewSafeBegin", new_safe_range_begin)
							.detail("LogZones", ::describeZones(logServers[log]->tLogLocalities))
							.detail("LogDataHalls", ::describeDataHalls(logServers[log]->tLogLocalities))
							.detail("tLogs", (int)prevState.tLogs.size())
							.detail("oldTlogsSize", (int)prevState.oldTLogData.size())
							.detail("logSystemType", prevState.logSystemType)
							.detail("knownCommittedVersion", knownCommittedVersion);

						if( !end.present() || results[ new_safe_range_begin ].end < end.get() ) {
							end = results[ new_safe_range_begin ].end;
						}
					}
					else {
						TraceEvent("LogSystemUnchangedRecovery", dbgid).detail("Cycles", cycles)
							.detail("logNum", log)
							.detail("TotalServers", logServers[log]->logServers.size())
							.detail("Required", requiredCount)
							.detail("Present", results.size())
							.detail("Available", availableItems.size())
							.detail("ServerState", sServerState)
							.detail("ReplicationFactor", logServers[log]->tLogReplicationFactor)
							.detail("AntiQuorum", logServers[log]->tLogWriteAntiQuorum)
							.detail("Policy", logServers[log]->tLogPolicy->info())
							.detail("TooManyFailures", bTooManyFailures)
							.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
							.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
							.detail("EndVersion", results[ new_safe_range_begin ].end)
							.detail("SafeBegin", safe_range_begin)
							.detail("SafeEnd", safe_range_end)
							.detail("NewSafeBegin", new_safe_range_begin)
							.detail("LogZones", ::describeZones(logServers[log]->tLogLocalities))
							.detail("LogDataHalls", ::describeDataHalls(logServers[log]->tLogLocalities))
							.detail("logSystemType", prevState.logSystemType);
					}
				}
				// Too many failures
				else {
					TraceEvent("LogSystemWaitingForRecovery", dbgid).detail("Cycles", cycles)
						.detail("logNum", log)
						.detail("AvailableServers", results.size())
						.detail("RequiredServers", requiredCount)
						.detail("TotalServers", logServers[log]->logServers.size())
						.detail("Required", requiredCount)
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", logServers[log]->tLogReplicationFactor)
						.detail("AntiQuorum", logServers[log]->tLogWriteAntiQuorum)
						.detail("Policy", logServers[log]->tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LogZones", ::describeZones(logServers[log]->tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(logServers[log]->tLogLocalities));
				}
			}

			if(end.present()) {
				TEST( last_end.present() );  // Restarting recovery at an earlier point

				Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

				last_end = end;
				logSystem->tLogs = logServers;
				logSystem->oldLogData = oldLogData;
				logSystem->logSystemType = prevState.logSystemType;
				logSystem->rejoins = rejoins;
				logSystem->epochEndVersion = end.get();
				logSystem->knownCommittedVersion = knownCommittedVersion;

				for(auto& s : tLogReply) {
					for(auto& r : s) {
						if( r.isReady() && !r.isError() ) {
							logSystem->epochEndTags.insert( r.get().tags.begin(), r.get().tags.end() );
						}
					}
				}

				outLogSystem->set(logSystem);
			}

			// Wait for anything relevant to change
			std::vector<Future<Void>> changes;
			for(int i=0; i < logServers.size(); i++) {
				if(logServers[i]->isLocal) {
					for(int j=0; j < logServers[i]->logServers.size(); j++) {
						if (!tLogReply[i][j].isReady())
							changes.push_back( ready(tLogReply[i][j]) );
						else {
							changes.push_back( logServers[i]->logServers[j]->onChange() );
							changes.push_back( logFailed[i][j]->onChange() );
						}
					}
				}
			}
			ASSERT(changes.size());
			Void _ = wait(waitForAny(changes));
		}
	}

	ACTOR static Future<Void> newRemoteEpoch( TagPartitionedLogSystem* self, Reference<TagPartitionedLogSystem> oldLogSystem, Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount, uint16_t minTag, int8_t remoteLocality ) 
	{
		state RecruitRemoteFromConfigurationReply remoteWorkers = wait( fRemoteWorkers );
		
		state Reference<LogSet> logSet = Reference<LogSet>( new LogSet() );
		logSet->tLogReplicationFactor = configuration.remoteTLogReplicationFactor;
		logSet->tLogPolicy = configuration.remoteTLogPolicy;
		logSet->isLocal = false;
		logSet->hasBest = true;
		logSet->locality = remoteLocality;
		
		//recruit temporary log routers and update registration with them
		state int tempLogRouters = std::max<int>(remoteWorkers.logRouters.size(), minTag + 1);
		state vector<Future<TLogInterface>> logRouterInitializationReplies;
		for( int i = 0; i < tempLogRouters; i++) {
			InitializeLogRouterRequest req;
			req.recoveryCount = recoveryCount;
			req.routerTag = Tag(tagLocalityLogRouter, i);
			req.logSet = self->tLogs.size();
			logRouterInitializationReplies.push_back( transformErrors( throwErrorOr( remoteWorkers.logRouters[i%remoteWorkers.logRouters.size()].logRouter.getReplyUnlessFailedFor( req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		}

		Void _ = wait( waitForAll(logRouterInitializationReplies) );

		for( int i = 0; i < logRouterInitializationReplies.size(); i++ ) {
			logSet->logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(logRouterInitializationReplies[i].get()) ) ) );
		}

		state double startTime = now();
		state UID remoteRecruitmentID = g_random->randomUniqueID();

		state vector<Future<TLogInterface>> remoteTLogInitializationReplies;
		
		vector< InitializeTLogRequest > remoteTLogReqs( remoteWorkers.remoteTLogs.size() );

		for( int i = 0; i < remoteWorkers.remoteTLogs.size(); i++ ) {
			InitializeTLogRequest &req = remoteTLogReqs[i];
			req.recruitmentID = remoteRecruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->epochEndVersion.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
			req.remoteTag = Tag(tagLocalityRemoteLog, i);
		}

		logSet->tLogLocalities.resize( remoteWorkers.remoteTLogs.size() );
		logSet->logServers.resize( remoteWorkers.remoteTLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSet->updateLocalitySet(remoteWorkers.remoteTLogs);

		vector<int> locations;
		for( Tag tag : oldLogSystem->epochEndTags ) {
			locations.clear();
			logSet->getPushLocations( vector<Tag>(1, tag), locations, 0 );
			for(int loc : locations)
				remoteTLogReqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < remoteWorkers.remoteTLogs.size(); i++ )
			remoteTLogInitializationReplies.push_back( transformErrors( throwErrorOr( remoteWorkers.remoteTLogs[i].tLog.getReplyUnlessFailedFor( remoteTLogReqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		Void _ = wait( waitForAll(remoteTLogInitializationReplies) );

		for( int i = 0; i < remoteTLogInitializationReplies.size(); i++ ) {
			logSet->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(remoteTLogInitializationReplies[i].get()) ) );
			logSet->tLogLocalities[i] = remoteWorkers.remoteTLogs[i].locality;
		}

		std::vector<Future<Void>> recoveryComplete;
		for( int i = 0; i < logSet->logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSet->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		
		self->remoteRecoveryComplete = waitForAll(recoveryComplete);
		logSet->logRouters.resize(remoteWorkers.remoteTLogs.size());
		self->tLogs.push_back( logSet );

		return Void();
	}

	ACTOR static Future<Reference<ILogSystem>> newEpoch( Reference<TagPartitionedLogSystem> oldLogSystem, RecruitFromConfigurationReply recr, Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount, int8_t primaryLocality, int8_t remoteLocality )
	{
		state double startTime = now();
		state Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality) );
		state UID recruitmentID = g_random->randomUniqueID();
		logSystem->logSystemType = 2;
		logSystem->minRouters = configuration.logRouterCount;
		logSystem->expectedLogSets = 1;
		
		logSystem->tLogs.push_back( Reference<LogSet>( new LogSet() ) );
		logSystem->tLogs[0]->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		logSystem->tLogs[0]->tLogReplicationFactor = configuration.tLogReplicationFactor;
		logSystem->tLogs[0]->tLogPolicy = configuration.tLogPolicy;
		logSystem->tLogs[0]->isLocal = true;
		logSystem->tLogs[0]->hasBest = true;
		logSystem->tLogs[0]->locality = primaryLocality;

		if(configuration.satelliteTLogReplicationFactor > 0) {
			logSystem->tLogs.push_back( Reference<LogSet>( new LogSet() ) );
			logSystem->tLogs[1]->tLogWriteAntiQuorum = configuration.satelliteTLogWriteAntiQuorum;
			logSystem->tLogs[1]->tLogReplicationFactor = configuration.satelliteTLogReplicationFactor;
			logSystem->tLogs[1]->tLogPolicy = configuration.satelliteTLogPolicy;
			logSystem->tLogs[1]->isLocal = true;
			logSystem->tLogs[1]->hasBest = false;
			logSystem->tLogs[1]->locality = -99;
			logSystem->expectedLogSets++;
		}

		if(configuration.remoteTLogReplicationFactor > 0) {
			logSystem->expectedLogSets++;
		}
		
		if(oldLogSystem->tLogs.size()) {
			logSystem->oldLogData.push_back(OldLogData());
			logSystem->oldLogData[0].tLogs = oldLogSystem->tLogs;
			logSystem->oldLogData[0].epochEnd = oldLogSystem->knownCommittedVersion + 1;
		}

		for(int i = 0; i < oldLogSystem->oldLogData.size(); i++) {
			logSystem->oldLogData.push_back(oldLogSystem->oldLogData[i]);
		}

		state vector<Future<TLogInterface>> initializationReplies;
		vector< InitializeTLogRequest > reqs( recr.tLogs.size() );

		for( int i = 0; i < recr.tLogs.size(); i++ ) {
			InitializeTLogRequest &req = reqs[i];
			req.recruitmentID = recruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->epochEndVersion.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
		}

		logSystem->tLogs[0]->tLogLocalities.resize( recr.tLogs.size() );
		logSystem->tLogs[0]->logServers.resize( recr.tLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSystem->tLogs[0]->updateLocalitySet(recr.tLogs);

		std::vector<int> locations;
		state uint16_t minTag = 0;
		for( Tag tag : oldLogSystem->getEpochEndTags() ) {
			minTag = std::min(minTag, tag.id);
			locations.clear();
			logSystem->tLogs[0]->getPushLocations( vector<Tag>(1, tag), locations, 0 );
			for(int loc : locations)
				reqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < recr.tLogs.size(); i++ )
			initializationReplies.push_back( transformErrors( throwErrorOr( recr.tLogs[i].tLog.getReplyUnlessFailedFor( reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		state std::vector<Future<Void>> recoveryComplete;

		if(configuration.satelliteTLogReplicationFactor > 0) {
			state vector<Future<TLogInterface>> satelliteInitializationReplies;
			vector< InitializeTLogRequest > sreqs( recr.satelliteTLogs.size() );

			for( int i = 0; i < recr.satelliteTLogs.size(); i++ ) {
				InitializeTLogRequest &req = sreqs[i];
				req.recruitmentID = recruitmentID;
				req.storeType = configuration.tLogDataStoreType;
				req.recoverFrom = oldLogSystem->getLogSystemConfig();
				req.recoverAt = oldLogSystem->epochEndVersion.get();
				req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
				req.epoch = recoveryCount;
			}

			logSystem->tLogs[1]->tLogLocalities.resize( recr.satelliteTLogs.size() );
			logSystem->tLogs[1]->logServers.resize( recr.satelliteTLogs.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
			logSystem->tLogs[1]->updateLocalitySet(recr.satelliteTLogs);

			for( Tag tag : oldLogSystem->getEpochEndTags() ) {
				minTag = std::min(minTag, tag.id);
				locations.clear();
				logSystem->tLogs[1]->getPushLocations( vector<Tag>(1, tag), locations, 0 );
				for(int loc : locations)
					sreqs[ loc ].recoverTags.push_back( tag );
			}

			for( int i = 0; i < recr.satelliteTLogs.size(); i++ )
				satelliteInitializationReplies.push_back( transformErrors( throwErrorOr( recr.satelliteTLogs[i].tLog.getReplyUnlessFailedFor( sreqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

			Void _ = wait( waitForAll( satelliteInitializationReplies ) );

			for( int i = 0; i < satelliteInitializationReplies.size(); i++ ) {
				logSystem->tLogs[1]->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(satelliteInitializationReplies[i].get()) ) );
				logSystem->tLogs[1]->tLogLocalities[i] = recr.satelliteTLogs[i].locality;
			}

			for( int i = 0; i < logSystem->tLogs[1]->logServers.size(); i++)
				recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->tLogs[1]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		}

		Void _ = wait( waitForAll( initializationReplies ) );

		for( int i = 0; i < initializationReplies.size(); i++ ) {
			logSystem->tLogs[0]->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(initializationReplies[i].get()) ) );
			logSystem->tLogs[0]->tLogLocalities[i] = recr.tLogs[i].locality;
		}

		//Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running recoveries causing tests to timeout
		if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator.speedUpSimulation) throw master_recovery_failed();

		for( int i = 0; i < logSystem->tLogs[0]->logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->tLogs[0]->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		logSystem->recoveryComplete = waitForAll(recoveryComplete);
		
		if(configuration.remoteTLogReplicationFactor > 0) {
			logSystem->remoteRecovery = TagPartitionedLogSystem::newRemoteEpoch(logSystem.getPtr(), oldLogSystem, fRemoteWorkers, configuration, recoveryCount, minTag, remoteLocality);
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
					TraceEvent("TLogLocked", myID).detail("TLog", tlog->get().id()).detail("end", data.end);
					return data;
				}
				when (Void _ = wait(tlog->onChange())) {}
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
		    .detail("locks", tlogs.size() - can_omit)
		    .detail("skipped", can_omit)
		    .detail("replication", prevState.tLogReplicationFactor)
		    .detail("antiquorum", prevState.tLogWriteAntiQuorum)
		    .detail("reboot_buggify", reboot_a_tlog);
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

Future<Void> ILogSystem::recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality ) {
	return TagPartitionedLogSystem::recoverAndEndEpoch( outLogSystem, dbgid, oldState, rejoins, locality );
}

Reference<ILogSystem> ILogSystem::fromLogSystemConfig( UID const& dbgid, struct LocalityData const& locality, struct LogSystemConfig const& conf ) {
	if (conf.logSystemType == 0)
		return Reference<ILogSystem>();
	else if (conf.logSystemType == 2)
		return TagPartitionedLogSystem::fromLogSystemConfig( dbgid, locality, conf );
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

Reference<ILogSystem> ILogSystem::fromServerDBInfo( UID const& dbgid, ServerDBInfo const& dbInfo ) {
	return fromLogSystemConfig( dbgid, dbInfo.myLocality, dbInfo.logSystemConfig );
}
