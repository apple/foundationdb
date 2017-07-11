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
	std::vector<LogSet> tLogs;
	Version epochEnd;

	OldLogData() : epochEnd(0) {}
};

struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
	UID dbgid;
	int logSystemType;
	std::vector<LogSet> tLogs;
	int minRouters;

	// new members
	Future<Void> rejoins;
	Future<Void> recoveryComplete;
	Future<Void> remoteRecovery;
	AsyncTrigger logSystemConfigChanges;
	bool recoveryCompleteWrittenToCoreState;

	Optional<Version> epochEndVersion;
	std::set< Tag > epochEndTags;
	Version knownCommittedVersion;
	LocalityData locality;
	std::map< std::pair<UID, Tag>, Version > outstandingPops;  // For each currently running popFromLog actor, (log server #, tag)->popped version
	ActorCollection actors;
	std::vector<OldLogData> oldLogData;

	TagPartitionedLogSystem( UID dbgid, LocalityData locality ) : dbgid(dbgid), locality(locality), actors(false), recoveryCompleteWrittenToCoreState(false), logSystemType(0), minRouters(std::numeric_limits<int>::max()) {}

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
			for( int j = 0; j < tLogs[i].logServers.size(); j++) {
				result = result + tLogs[i].logServers[j]->get().id().toString() + ((j == tLogs[i].logServers.size() - 1) ? " " : ", ");
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

		for( auto& it : lsConf.tLogs ) {
			LogSet logSet;
			for( auto& log : it.tLogs) {
				logSet.logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
			}
			for( auto& log : it.logRouters) {
				logSet.logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
			}
			logSet.tLogWriteAntiQuorum = it.tLogWriteAntiQuorum;
			logSet.tLogReplicationFactor = it.tLogReplicationFactor;
			logSet.tLogPolicy = it.tLogPolicy;
			logSet.tLogLocalities = it.tLogLocalities;
			logSet.isLocal = it.isLocal;
			logSet.hasBest = it.hasBest;
			logSet.updateLocalitySet();
			logSystem->tLogs.push_back(logSet);
			if(logSet.logRouters.size() > 0) logSystem->minRouters = std::min<int>(logSystem->minRouters, logSet.logRouters.size());
		}

		logSystem->oldLogData.resize(lsConf.oldTLogs.size());
		for( int i = 0; i < lsConf.oldTLogs.size(); i++ ) {
			for( auto& it : lsConf.oldTLogs[i].tLogs ) {
				LogSet logSet;
				for( auto & log : it.tLogs) {
					logSet.logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				for( auto & log : it.logRouters) {
					logSet.logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				logSet.tLogWriteAntiQuorum = it.tLogWriteAntiQuorum;
				logSet.tLogReplicationFactor = it.tLogReplicationFactor;
				logSet.tLogPolicy = it.tLogPolicy;
				logSet.tLogLocalities = it.tLogLocalities;
				logSet.isLocal = it.isLocal;
				logSet.hasBest = it.hasBest;
				//logSet.UpdateLocalitySet(); we do not update the locality set, since we never push to old logs
				logSystem->oldLogData[i].tLogs.push_back(logSet);
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
			for( auto& it : lsConf.oldTLogs[0].tLogs ) {
				LogSet logSet;
				for( auto & log : it.tLogs) {
					logSet.logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				for( auto & log : it.logRouters) {
					logSet.logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
				}
				logSet.tLogWriteAntiQuorum = it.tLogWriteAntiQuorum;
				logSet.tLogReplicationFactor = it.tLogReplicationFactor;
				logSet.tLogPolicy = it.tLogPolicy;
				logSet.tLogLocalities = it.tLogLocalities;
				logSet.isLocal = it.isLocal;
				logSet.hasBest = it.hasBest;
				//logSet.updateLocalitySet(); we do not update the locality set, since we never push to old logs
				logSystem->tLogs.push_back(logSet);
				if(logSet.logRouters.size() > 0) logSystem->minRouters = std::min<int>(logSystem->minRouters, logSet.logRouters.size());
			}
			//logSystem->epochEnd = lsConf.oldTLogs[0].epochEnd;
			
			logSystem->oldLogData.resize(lsConf.oldTLogs.size()-1);
			for( int i = 1; i < lsConf.oldTLogs.size(); i++ ) {
				for( auto& it : lsConf.oldTLogs[i].tLogs ) {
					LogSet logSet;
					for( auto & log : it.tLogs) {
						logSet.logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
					}
					for( auto & log : it.logRouters) {
						logSet.logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( log ) ) );
					}
					logSet.tLogWriteAntiQuorum = it.tLogWriteAntiQuorum;
					logSet.tLogReplicationFactor = it.tLogReplicationFactor;
					logSet.tLogPolicy = it.tLogPolicy;
					logSet.tLogLocalities = it.tLogLocalities;
					logSet.isLocal = it.isLocal;
					logSet.hasBest = it.hasBest;
					//logSet.updateLocalitySet(); we do not update the locality set, since we never push to old logs
					logSystem->oldLogData[i-1].tLogs.push_back(logSet);
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
			for(auto &log : t.logServers) {
				coreSet.tLogs.push_back(log->get().id());
				coreSet.tLogLocalities.push_back(log->get().interf().locality);
			}
			coreSet.tLogWriteAntiQuorum = t.tLogWriteAntiQuorum;
			coreSet.tLogReplicationFactor = t.tLogReplicationFactor;
			coreSet.tLogPolicy = t.tLogPolicy;
			coreSet.isLocal = t.isLocal;
			coreSet.hasBest = t.hasBest;
			newState.tLogs.push_back(coreSet);
		}

		newState.oldTLogData.clear();
		if(!recoveryComplete.isValid() || !recoveryComplete.isReady()) {
			newState.oldTLogData.resize(oldLogData.size());
			for(int i = 0; i < oldLogData.size(); i++) {
				for(auto &t : oldLogData[i].tLogs) {
					CoreTLogSet coreSet;
					for(auto &log : t.logServers) {
						coreSet.tLogs.push_back(log->get().id());	
					}
					coreSet.tLogLocalities = t.tLogLocalities;
					coreSet.tLogWriteAntiQuorum = t.tLogWriteAntiQuorum;
					coreSet.tLogReplicationFactor = t.tLogReplicationFactor;
					coreSet.tLogPolicy = t.tLogPolicy;
					coreSet.isLocal = t.isLocal;
					coreSet.hasBest = t.hasBest;
					newState.oldTLogData[i].tLogs.push_back(coreSet);
				}
				newState.oldTLogData[i].epochEnd = oldLogData[i].epochEnd;
			}
		}

		newState.logSystemType = logSystemType;
	}

	virtual Future<Void> onCoreStateChanged() {
		ASSERT(recoveryComplete.isValid());
		if( recoveryComplete.isReady() )
			return Never();
		return recoveryComplete;
	}

	virtual void coreStateWritten( DBCoreState const& newState ) {
		if( !newState.oldTLogData.size() )
			recoveryCompleteWrittenToCoreState = true;
	}

	virtual Future<Void> onError() {
		// Never returns normally, but throws an error if the subsystem stops working
		// FIXME: Run waitFailureClient on the master instead of these onFailedFor?
		vector<Future<Void>> failed;

		for( auto& it : tLogs ) {
			for(auto &t : it.logServers) {
				if( t->get().present() ) {
					failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );
				}
			}
			for(auto &t : it.logRouters) {
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
		for(auto it : tLogs) {
			if(it.isLocal) {
				vector<Future<Void>> tLogCommitResults;
				for(int loc=0; loc< it.logServers.size(); loc++) {
					Future<Void> commitMessage = reportTLogCommitErrors(
							it.logServers[loc]->get().interf().commit.getReply(
								TLogCommitRequest( data.getArena(), prevVersion, version, knownCommittedVersion, data.getMessages(location), data.getTags(location), debugID ), TaskTLogCommitReply ),
							getDebugID());
					actors.add(commitMessage);
					tLogCommitResults.push_back(commitMessage);
					location++;
				}
				quorumResults.push_back( quorum( tLogCommitResults, tLogCommitResults.size() - it.tLogWriteAntiQuorum ) );
			}
		}
		
		return waitForAll(quorumResults);
	}

	virtual Reference<IPeekCursor> peek( Version begin, Tag tag, bool parallelGetMore ) {
		if(tag >= SERVER_KNOBS->MAX_TAG) {
			//FIXME: non-static logRouters
			return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( tLogs[1].logRouters, -1, (int)tLogs[1].logRouters.size(), tag, begin, getPeekEnd(), false ) );
		} else {
			if(oldLogData.size() == 0 || begin >= oldLogData[0].epochEnd) {
				return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( tLogs, 1, tLogs[1].logServers.size() ? tLogs[1].bestLocationFor( tag ) : -1, tag, begin, getPeekEnd(), parallelGetMore ) );
			} else {
				std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
				std::vector< LogMessageVersion > epochEnds;
				cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( tLogs, 1, tLogs[1].logServers.size() ? tLogs[1].bestLocationFor( tag ) : -1, tag, oldLogData[0].epochEnd, getPeekEnd(), parallelGetMore)) );
				for(int i = 0; i < oldLogData.size() && begin < oldLogData[i].epochEnd; i++) {
					cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( oldLogData[i].tLogs, 1, oldLogData[i].tLogs[1].logServers.size() ? oldLogData[i].tLogs[1].bestLocationFor( tag ) : -1, tag, i+1 == oldLogData.size() ? begin : std::max(oldLogData[i+1].epochEnd, begin), oldLogData[i].epochEnd, parallelGetMore)) );
					epochEnds.push_back(LogMessageVersion(oldLogData[i].epochEnd));
				}

				return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
			}
		}
	}

	virtual Reference<IPeekCursor> peekSingle( Version begin, Tag tag ) {
		ASSERT(tag < SERVER_KNOBS->MAX_TAG);
		if(oldLogData.size() == 0 || begin >= oldLogData[0].epochEnd) {
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[1].logServers.size() ?
				tLogs[1].logServers[tLogs[1].bestLocationFor( tag )] :
				Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		} else {
			TEST(true); //peekSingle used during non-copying tlog recovery
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;
			cursors.push_back( Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( tLogs[1].logServers.size() ?
				tLogs[1].logServers[tLogs[1].bestLocationFor( tag )] :
				Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, oldLogData[0].epochEnd, getPeekEnd(), false, false) ) );
			for(int i = 0; i < oldLogData.size() && begin < oldLogData[i].epochEnd; i++) {
				cursors.push_back( Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( oldLogData[i].tLogs, 1, oldLogData[i].tLogs[1].logServers.size() ? oldLogData[i].tLogs[1].bestLocationFor( tag ) : -1, tag, i+1 == oldLogData.size() ? begin : std::max(oldLogData[i+1].epochEnd, begin), oldLogData[i].epochEnd, false)) );
				epochEnds.push_back(LogMessageVersion(oldLogData[i].epochEnd));
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual void pop( Version upTo, Tag tag ) {
		if (!upTo) return;
		for(auto& t : tLogs) {
			std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>>& popServers = tag >= SERVER_KNOBS->MAX_TAG ? t.logRouters : t.logServers;
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

	virtual Future<Void> confirmEpochLive(Optional<UID> debugID) {
		// Returns success after confirming that pushes in the current epoch are still possible
		// FIXME: This is way too conservative?
		vector<Future<Void>> quorumResults;
		for(auto& it : tLogs) {
			if(it.isLocal) {
				vector<Future<Void>> alive;
				for(auto& t : it.logServers) {
					if( t->get().present() ) alive.push_back( brokenPromiseToNever( t->get().interf().confirmRunning.getReply(TLogConfirmRunningRequest(debugID), TaskTLogConfirmRunningReply ) ) );
					else alive.push_back( Never() );
				}
				quorumResults.push_back( quorum( alive, alive.size() - it.tLogWriteAntiQuorum ) );
			}
		}
		
		return waitForAll(quorumResults);
	}

	virtual Future<Reference<ILogSystem>> newEpoch( vector<WorkerInterface> availableLogServers, vector<WorkerInterface> availableRemoteLogServers, vector<WorkerInterface> availableLogRouters, DatabaseConfiguration const& config, LogEpoch recoveryCount ) {
		// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.  The new epoch
		// is only provisional until the caller updates the coordinated DBCoreState
		return newEpoch( Reference<TagPartitionedLogSystem>::addRef(this), availableLogServers, availableRemoteLogServers, availableLogRouters, config, recoveryCount );
	}

	virtual LogSystemConfig getLogSystemConfig() {
		LogSystemConfig logSystemConfig;
		logSystemConfig.logSystemType = logSystemType;
		
		for( auto& t : tLogs ) {
			TLogSet log;
			log.tLogWriteAntiQuorum = t.tLogWriteAntiQuorum;
			log.tLogReplicationFactor = t.tLogReplicationFactor;
			log.tLogPolicy = t.tLogPolicy;
			log.tLogLocalities = t.tLogLocalities;
			log.isLocal = t.isLocal;
			log.hasBest = t.hasBest;

			for( int i = 0; i < t.logServers.size(); i++ ) {
				log.tLogs.push_back(t.logServers[i]->get());
			}

			for( int i = 0; i < t.logRouters.size(); i++ ) {
				log.logRouters.push_back(t.logRouters[i]->get());
			}

			logSystemConfig.tLogs.push_back(log);
		}

		if(!recoveryCompleteWrittenToCoreState) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				logSystemConfig.oldTLogs.push_back(OldTLogConf());

				for( auto& t : oldLogData[i].tLogs ) {
					TLogSet log;
					log.tLogWriteAntiQuorum = t.tLogWriteAntiQuorum;
					log.tLogReplicationFactor = t.tLogReplicationFactor;
					log.tLogPolicy = t.tLogPolicy;
					log.tLogLocalities = t.tLogLocalities;
					log.isLocal = t.isLocal;
					log.hasBest = t.hasBest;

					for( int i = 0; i < t.logServers.size(); i++ ) {
						log.tLogs.push_back(t.logServers[i]->get());
					}

					for( int i = 0; i < t.logRouters.size(); i++ ) {
						log.logRouters.push_back(t.logRouters[i]->get());
					}

					logSystemConfig.oldTLogs[i].tLogs.push_back(log);
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
			for( int i = 0; i < t.logServers.size(); i++ ) {
				logs.push_back(std::make_pair(t.logServers[i]->get().id(), t.logServers[i]->get().present() ? t.logServers[i]->get().interf().address() : NetworkAddress()));
			}
		}
		if(!recoveryCompleteWrittenToCoreState) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				for(auto& t : oldLogData[i].tLogs) {
					for( int j = 0; j < t.logServers.size(); j++ ) {
						oldLogs.push_back(std::make_pair(t.logServers[j]->get().id(), t.logServers[j]->get().present() ? t.logServers[j]->get().interf().address() : NetworkAddress()));
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
			for( int i = 0; i < t.logServers.size(); i++ ) {
				changes.push_back( t.logServers[i]->onChange() );
			}
		}
		for( int i = 0; i < oldLogData.size(); i++ ) {
			for(auto& t : oldLogData[i].tLogs) {
				for( int j = 0; j < t.logServers.size(); j++ ) {
					changes.push_back( t.logServers[j]->onChange() );
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

	virtual void addRemoteTags( int logSet, std::vector<Tag> originalTags, std::vector<int>& tags ) {
		tLogs[logSet].getPushLocations(originalTags, tags, SERVER_KNOBS->MAX_TAG);
	}

	virtual void getPushLocations( std::vector<Tag> const& tags, std::vector<int>& locations ) {
		int locationOffset = 0;
		for(auto& log : tLogs) {
			if(log.isLocal) {
				log.getPushLocations(tags, locations, locationOffset);
				locationOffset += log.logServers.size();
			}
		}
	}

	virtual Tag getRandomRouterTag() {
		return SERVER_KNOBS->MIN_TAG - g_random->randomInt(0, minRouters);
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
		state std::vector<LogSet> logServers;
		state std::vector<OldLogData> oldLogData;
		state std::vector<std::vector<Reference<AsyncVar<bool>>>> logFailed;
		state std::vector<Future<Void>> failureTrackers;
		for( auto& log : prevState.tLogs ) {
			LogSet logSet;
			std::vector<Reference<AsyncVar<bool>>> failed;
			for(int i = 0; i < log.tLogs.size(); i++) {
				Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(log.tLogs[i]) ) );
				logSet.logServers.push_back( logVar );
				allLogServers.push_back( logVar );
				failed.push_back( Reference<AsyncVar<bool>>( new AsyncVar<bool>() ) );
				failureTrackers.push_back( monitorLog(logVar, failed[i] ) );
			}
			logSet.tLogReplicationFactor = log.tLogReplicationFactor;
			logSet.tLogWriteAntiQuorum = log.tLogWriteAntiQuorum;
			logSet.tLogPolicy = log.tLogPolicy;
			logSet.tLogLocalities = log.tLogLocalities;
			logSet.isLocal = log.isLocal;
			logSet.hasBest = log.hasBest;
			logServers.push_back(logSet);
			logFailed.push_back(failed);
		}
		for( auto& old : prevState.oldTLogData ) {
			OldLogData oldData;
			for( auto& log : old.tLogs) {
				LogSet logSet;
				for(int j = 0; j < log.tLogs.size(); j++) {
					Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(log.tLogs[j]) ) );
					logSet.logServers.push_back( logVar );
					allLogServers.push_back( logVar );
				}
				logSet.tLogReplicationFactor = log.tLogReplicationFactor;
				logSet.tLogWriteAntiQuorum = log.tLogWriteAntiQuorum;
				logSet.tLogPolicy = log.tLogPolicy;
				logSet.tLogLocalities = log.tLogLocalities;
				logSet.isLocal = log.isLocal;
				logSet.hasBest = log.hasBest;
				oldData.tLogs.push_back(logSet);
			}
			oldData.epochEnd = old.epochEnd;
			oldLogData.push_back(oldData);
		}
		state Future<Void> rejoins = trackRejoins( dbgid, allLogServers, rejoinRequests );

		tLogReply.resize(logServers.size());
		for( int i=0; i < logServers.size(); i++ ) {
			for(int t=0; t<logServers[i].logServers.size(); t++) {
				tLogReply[i].push_back( lockTLog( dbgid, logServers[i].logServers[t]) );
			}
		}

		state Optional<Version> last_end;
		state int cycles = 0;

		loop {
			Optional<Version> end;
			Version knownCommittedVersion = 0;
			for(int log = 0; log < logServers.size(); log++) {
				if(!logServers[log].isLocal) {
					continue;
				}

				// To ensure consistent recovery, the number of servers NOT in the write quorum plus the number of servers NOT in the read quorum
				// have to be strictly less than the replication factor.  Otherwise there could be a replica set consistent entirely of servers that
				// are out of date due to not being in the write quorum or unavailable due to not being in the read quorum.
				// So (N - W) + (N - R) < F, and optimally (N-W)+(N-R)=F-1.  Thus R=2N+1-F-W.
				state int requiredCount = (int)logServers[log].logServers.size()+1 - logServers[log].tLogReplicationFactor + logServers[log].tLogWriteAntiQuorum;
				ASSERT( requiredCount > 0 && requiredCount <= logServers[log].logServers.size() );
				ASSERT( logServers[log].tLogReplicationFactor >= 1 && logServers[log].tLogReplicationFactor <= logServers[log].logServers.size() );
				ASSERT( logServers[log].tLogWriteAntiQuorum >= 0 && logServers[log].tLogWriteAntiQuorum < logServers[log].logServers.size() );
			
				std::vector<LocalityData> availableItems, badCombo;
				std::vector<TLogLockResult> results;
				std::string sServerState;
				LocalityGroup unResponsiveSet;
				double t = timer();
				cycles++;

				for(int t=0; t<logServers[log].logServers.size(); t++) {
					if (tLogReply[log][t].isReady() && !tLogReply[log][t].isError() && !logFailed[log][t]->get()) {
						results.push_back(tLogReply[log][t].get());
						availableItems.push_back(logServers[log].tLogLocalities[t]);
						sServerState += 'a';
					}
					else {
						unResponsiveSet.add(logServers[log].tLogLocalities[t]);
						sServerState += 'f';
					}
				}

				// Check if the list of results is not larger than the anti quorum
				bool bTooManyFailures = (results.size() <= logServers[log].tLogWriteAntiQuorum);

				// Check if failed logs complete the policy
				bTooManyFailures = bTooManyFailures || ((unResponsiveSet.size() >= logServers[log].tLogReplicationFactor)	&& (unResponsiveSet.validate(logServers[log].tLogPolicy)));

				// Check all combinations of the AntiQuorum within the failed
				if ((!bTooManyFailures) && (logServers[log].tLogWriteAntiQuorum) && (!validateAllCombinations(badCombo, unResponsiveSet, logServers[log].tLogPolicy, availableItems, logServers[log].tLogWriteAntiQuorum, false))) {
					TraceEvent("EpochEndBadCombo", dbgid).detail("Cycles", cycles)
						.detail("logNum", log)
						.detail("Required", requiredCount)
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("Absent", logServers[log].logServers.size() - results.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", logServers[log].tLogReplicationFactor)
						.detail("AntiQuorum", logServers[log].tLogWriteAntiQuorum)
						.detail("Policy", logServers[log].tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LogZones", ::describeZones(logServers[log].tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(logServers[log].tLogLocalities));
					bTooManyFailures = true;
				}

				// If too many TLogs are failed for recovery to be possible, we could wait forever here.
				//Void _ = wait( smartQuorum( tLogReply, requiredCount, SERVER_KNOBS->RECOVERY_TLOG_SMART_QUORUM_DELAY ) || rejoins );

				ASSERT(logServers[log].logServers.size() == tLogReply[log].size());
				if (!bTooManyFailures) {
					std::sort( results.begin(), results.end(), sort_by_end() );
					int absent = logServers[log].logServers.size() - results.size();
					int safe_range_begin = logServers[log].tLogWriteAntiQuorum;
					int new_safe_range_begin = std::min(logServers[log].tLogWriteAntiQuorum, (int)(results.size()-1));
					int safe_range_end = logServers[log].tLogReplicationFactor - absent;

					if( ( prevState.logSystemType == 2 && (!last_end.present() || ((safe_range_end > 0) && (safe_range_end-1 < results.size()) && results[ safe_range_end-1 ].end < last_end.get())) ) ) {
						knownCommittedVersion = std::max(knownCommittedVersion, end.get() - (g_network->isSimulated() ? 10*SERVER_KNOBS->VERSIONS_PER_SECOND : SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)); //In simulation this must be the maximum MAX_READ_TRANSACTION_LIFE_VERSIONS
						for(int i = 0; i < results.size(); i++) {
							knownCommittedVersion = std::max(knownCommittedVersion, results[i].knownCommittedVersion);
						}

						TraceEvent("LogSystemRecovery", dbgid).detail("Cycles", cycles)
							.detail("logNum", log)
							.detail("TotalServers", logServers[log].logServers.size())
							.detail("Required", requiredCount)
							.detail("Present", results.size())
							.detail("Available", availableItems.size())
							.detail("Absent", logServers[log].logServers.size() - results.size())
							.detail("ServerState", sServerState)
							.detail("ReplicationFactor", logServers[log].tLogReplicationFactor)
							.detail("AntiQuorum", logServers[log].tLogWriteAntiQuorum)
							.detail("Policy", logServers[log].tLogPolicy->info())
							.detail("TooManyFailures", bTooManyFailures)
							.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
							.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
							.detail("EndVersion", results[ new_safe_range_begin ].end)
							.detail("SafeBegin", safe_range_begin)
							.detail("SafeEnd", safe_range_end)
							.detail("NewSafeBegin", new_safe_range_begin)
							.detail("LogZones", ::describeZones(logServers[log].tLogLocalities))
							.detail("LogDataHalls", ::describeDataHalls(logServers[log].tLogLocalities))
							.detail("tLogs", (int)prevState.tLogs.size())
							.detail("oldTlogsSize", (int)prevState.oldTLogData.size())
							.detail("logSystemType", prevState.logSystemType)
							.detail("knownCommittedVersion", knownCommittedVersion);

						if(!end.present() == results[ new_safe_range_begin ].end < end.get() ) {
							end = results[ new_safe_range_begin ].end;
						}
					}
					else {
						TraceEvent("LogSystemUnchangedRecovery", dbgid).detail("Cycles", cycles)
							.detail("logNum", log)
							.detail("TotalServers", logServers[log].logServers.size())
							.detail("Required", requiredCount)
							.detail("Present", results.size())
							.detail("Available", availableItems.size())
							.detail("ServerState", sServerState)
							.detail("ReplicationFactor", logServers[log].tLogReplicationFactor)
							.detail("AntiQuorum", logServers[log].tLogWriteAntiQuorum)
							.detail("Policy", logServers[log].tLogPolicy->info())
							.detail("TooManyFailures", bTooManyFailures)
							.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
							.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
							.detail("EndVersion", results[ new_safe_range_begin ].end)
							.detail("SafeBegin", safe_range_begin)
							.detail("SafeEnd", safe_range_end)
							.detail("NewSafeBegin", new_safe_range_begin)
							.detail("LogZones", ::describeZones(logServers[log].tLogLocalities))
							.detail("LogDataHalls", ::describeDataHalls(logServers[log].tLogLocalities));
					}
				}
				// Too many failures
				else {
					TraceEvent("LogSystemWaitingForRecovery", dbgid).detail("Cycles", cycles)
						.detail("logNum", log)
						.detail("AvailableServers", results.size())
						.detail("RequiredServers", requiredCount)
						.detail("TotalServers", logServers[log].logServers.size())
						.detail("Required", requiredCount)
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", logServers[log].tLogReplicationFactor)
						.detail("AntiQuorum", logServers[log].tLogWriteAntiQuorum)
						.detail("Policy", logServers[log].tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LogZones", ::describeZones(logServers[log].tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(logServers[log].tLogLocalities));
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
				if(logServers[i].isLocal) {
					for(int j=0; j < logServers[i].logServers.size(); j++) {
						if (!tLogReply[i][j].isReady())
							changes.push_back( ready(tLogReply[i][j]) );
						else {
							changes.push_back( logServers[i].logServers[j]->onChange() );
							changes.push_back( logFailed[i][j]->onChange() );
						}
					}
				}
			}
			ASSERT(changes.size());
			Void _ = wait(waitForAny(changes));
		}
	}

	ACTOR static Future<Void> newRemoteEpoch( TagPartitionedLogSystem* self, vector<WorkerInterface> remoteTLogWorkers, vector<WorkerInterface> logRouterWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount, Version recoveryVersion, Tag minTag, int logNum ) 
	{
		//recruit temporary log routers and update registration with them
		state int tempLogRouters = std::max<int>(logRouterWorkers.size(), SERVER_KNOBS->MIN_TAG - minTag + 1);
		state vector<Future<TLogInterface>> logRouterInitializationReplies;
		for( int i = 0; i < tempLogRouters; i++) {
			InitializeLogRouterRequest req;
			req.recoveryCount = recoveryCount;
			req.routerTag = SERVER_KNOBS->MIN_TAG - i;
			req.logSet = logNum;
			logRouterInitializationReplies.push_back( transformErrors( throwErrorOr( logRouterWorkers[i%logRouterWorkers.size()].logRouter.getReplyUnlessFailedFor( req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		}

		Void _ = wait( waitForAll(logRouterInitializationReplies) );

		for( int i = 0; i < logRouterInitializationReplies.size(); i++ ) {
			self->tLogs[logNum].logRouters.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(logRouterInitializationReplies[i].get()) ) ) );
		}
		self->logSystemConfigChanges.trigger();

		state double startTime = now();
		state UID remoteRecruitmentID = g_random->randomUniqueID();

		state vector<Future<TLogInterface>> remoteTLogInitializationReplies;
		
		vector< InitializeTLogRequest > remoteTLogReqs( remoteTLogWorkers.size() );

		for( int i = 0; i < remoteTLogWorkers.size(); i++ ) {
			InitializeTLogRequest &req = remoteTLogReqs[i];
			req.recruitmentID = remoteRecruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = self->getLogSystemConfig();
			req.recoverAt = recoveryVersion;
			req.epoch = recoveryCount;
			req.remoteTag = SERVER_KNOBS->MAX_TAG + i;
		}

		self->tLogs[logNum].tLogLocalities.resize( remoteTLogWorkers.size() );
		self->tLogs[logNum].logServers.resize( remoteTLogWorkers.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		self->tLogs[logNum].updateLocalitySet(remoteTLogWorkers);

		vector<int> locations;
		for( Tag tag : self->epochEndTags ) {
			locations.clear();
			self->tLogs[logNum].getPushLocations( vector<Tag>(1, tag), locations, 0 );
			for(int loc : locations)
				remoteTLogReqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < remoteTLogWorkers.size(); i++ )
			remoteTLogInitializationReplies.push_back( transformErrors( throwErrorOr( remoteTLogWorkers[i].tLog.getReplyUnlessFailedFor( remoteTLogReqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		Void _ = wait( waitForAll(remoteTLogInitializationReplies) );

		for( int i = 0; i < remoteTLogInitializationReplies.size(); i++ ) {
			self->tLogs[logNum].logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(remoteTLogInitializationReplies[i].get()) ) ) );
		}

		std::vector<Future<Void>> recoveryComplete;
		for( int i = 0; i < self->tLogs[logNum].logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( self->tLogs[logNum].logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		
		Void _ = wait( waitForAll(recoveryComplete) );
		
		self->tLogs[logNum].logRouters.resize(logRouterWorkers.size());

		return Void();
	}

	ACTOR static Future<Reference<ILogSystem>> newEpoch(
		 Reference<TagPartitionedLogSystem> oldLogSystem, vector<WorkerInterface> tLogWorkers, vector<WorkerInterface> remoteTLogWorkers, vector<WorkerInterface> logRouterWorkers, DatabaseConfiguration configuration, LogEpoch recoveryCount )
	{
		state double startTime = now();
		state Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality) );
		state UID recruitmentID = g_random->randomUniqueID();
		logSystem->logSystemType = 2;
		logSystem->tLogs.resize(2);
		
		logSystem->tLogs[0].tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		logSystem->tLogs[0].tLogReplicationFactor = configuration.tLogReplicationFactor;
		logSystem->tLogs[0].tLogPolicy = configuration.tLogPolicy;
		
		logSystem->tLogs[1].tLogReplicationFactor = configuration.remoteTLogReplicationFactor;
		logSystem->tLogs[1].tLogPolicy = configuration.remoteTLogPolicy;

		if(oldLogSystem->tLogs.size()) {
			logSystem->oldLogData.push_back(OldLogData());
			logSystem->oldLogData[0].tLogs = oldLogSystem->tLogs;
			logSystem->oldLogData[0].epochEnd = oldLogSystem->knownCommittedVersion + 1;
		}

		for(int i = 0; i < oldLogSystem->oldLogData.size(); i++) {
			logSystem->oldLogData.push_back(oldLogSystem->oldLogData[i]);
		}

		state vector<Future<TLogInterface>> initializationReplies;
		vector< InitializeTLogRequest > reqs( tLogWorkers.size() );

		for( int i = 0; i < tLogWorkers.size(); i++ ) {
			InitializeTLogRequest &req = reqs[i];
			req.recruitmentID = recruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->epochEndVersion.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
		}

		logSystem->tLogs[0].tLogLocalities.resize( tLogWorkers.size() );
		logSystem->tLogs[0].logServers.resize( tLogWorkers.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size
		logSystem->tLogs[0].updateLocalitySet(tLogWorkers);

		std::vector<int> locations;
		state Tag minTag = 0;
		for( Tag tag : oldLogSystem->getEpochEndTags() ) {
			minTag = std::min(minTag, tag);
			locations.clear();
			logSystem->tLogs[0].getPushLocations( vector<Tag>(1, tag), locations, 0 );
			for(int loc : locations)
				reqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < tLogWorkers.size(); i++ )
			initializationReplies.push_back( transformErrors( throwErrorOr( tLogWorkers[i].tLog.getReplyUnlessFailedFor( reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		Void _ = wait( waitForAll( initializationReplies ) );

		for( int i = 0; i < initializationReplies.size(); i++ ) {
			logSystem->tLogs[0].logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(initializationReplies[i].get()) ) );
			logSystem->tLogs[0].tLogLocalities[i] = tLogWorkers[i].locality;
		}

		//Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running recoveries causing tests to timeout
		if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator.speedUpSimulation) throw master_recovery_failed();

		std::vector<Future<Void>> recoveryComplete;
		for( int i = 0; i < logSystem->tLogs[0].logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->tLogs[0].logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		logSystem->recoveryComplete = waitForAll(recoveryComplete);
		logSystem->remoteRecovery = TagPartitionedLogSystem::newRemoteEpoch(logSystem.getPtr(), remoteTLogWorkers, logRouterWorkers, configuration, recoveryCount, oldLogSystem->epochEndVersion.get(), minTag, 1);

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
