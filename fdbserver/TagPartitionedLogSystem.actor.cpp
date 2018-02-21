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
#include "RecoveryState.h"

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
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	IRepPolicyRef	tLogPolicy;

	Version epochEnd;

	OldLogData() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), epochEnd(0) {}
};

struct TagPartitionedLogSystem : ILogSystem, ReferenceCounted<TagPartitionedLogSystem> {
	UID dbgid;
	int tLogWriteAntiQuorum, tLogReplicationFactor, logSystemType;
	LocalitySetRef							logServerSet;
	std::vector<int>						logIndexArray;
	std::map<int,LocalityEntry>	logEntryMap;
	IRepPolicyRef								tLogPolicy;
	std::vector< LocalityData > tLogLocalities;

	// new members
	Future<Void> rejoins;
	Future<Void> recoveryComplete;
	bool recoveryCompleteWrittenToCoreState;

	Optional<Version> epochEndVersion;
	std::set< Tag > epochEndTags;
	Version knownCommittedVersion;
	LocalityData locality;
	std::map< std::pair<int, Tag>, Version > outstandingPops;  // For each currently running popFromLog actor, (log server #, tag)->popped version
	ActorCollection actors;
	std::vector<OldLogData> oldLogData;
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;

	TagPartitionedLogSystem( UID dbgid, LocalityData locality ) : dbgid(dbgid), locality(locality), actors(false), recoveryCompleteWrittenToCoreState(false), tLogWriteAntiQuorum(0), tLogReplicationFactor(0), logSystemType(0) {}

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
		for( auto it : logServers ) {
			result = result + it->get().id().toString() + ", ";
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

		for( int i = 0; i < lsConf.tLogs.size(); i++ )
			logSystem->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( lsConf.tLogs[i] ) ) );

		logSystem->oldLogData.resize(lsConf.oldTLogs.size());
		for( int i = 0; i < lsConf.oldTLogs.size(); i++ ) {
			for( int j = 0; j < lsConf.oldTLogs[i].tLogs.size(); j++) {
				logSystem->oldLogData[i].logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( lsConf.oldTLogs[i].tLogs[j] ) ) );
			}
			logSystem->oldLogData[i].tLogWriteAntiQuorum = lsConf.oldTLogs[i].tLogWriteAntiQuorum;
			logSystem->oldLogData[i].tLogReplicationFactor = lsConf.oldTLogs[i].tLogReplicationFactor;
			logSystem->oldLogData[i].tLogPolicy = lsConf.oldTLogs[i].tLogPolicy;
			logSystem->oldLogData[i].tLogLocalities = lsConf.oldTLogs[i].tLogLocalities;
			logSystem->oldLogData[i].epochEnd = lsConf.oldTLogs[i].epochEnd;
		}

		//logSystem->epoch = lsConf.epoch;
		logSystem->tLogWriteAntiQuorum = lsConf.tLogWriteAntiQuorum;
		logSystem->tLogReplicationFactor = lsConf.tLogReplicationFactor;
		logSystem->tLogPolicy = lsConf.tLogPolicy;
		logSystem->tLogLocalities = lsConf.tLogLocalities;
		logSystem->logSystemType = lsConf.logSystemType;
		logSystem->UpdateLocalitySet(lsConf.tLogs);
		filterLocalityDataForPolicy(logSystem->tLogPolicy, &logSystem->tLogLocalities);

		return logSystem;
	}

	static Reference<ILogSystem> fromOldLogSystemConfig( UID const& dbgid, LocalityData const& locality, LogSystemConfig const& lsConf ) {
		ASSERT( lsConf.logSystemType == 2 || (lsConf.logSystemType == 0 && !lsConf.tLogs.size()) );
		//ASSERT(lsConf.epoch == epoch);  //< FIXME
		Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

		if(lsConf.oldTLogs.size()) {
			for( int i = 0; i < lsConf.oldTLogs[0].tLogs.size(); i++ )
				logSystem->logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( lsConf.oldTLogs[0].tLogs[i] ) ) );

			//logSystem->epoch = lsConf.epoch;
			logSystem->tLogWriteAntiQuorum = lsConf.oldTLogs[0].tLogWriteAntiQuorum;
			logSystem->tLogReplicationFactor = lsConf.oldTLogs[0].tLogReplicationFactor;
			logSystem->tLogPolicy = lsConf.oldTLogs[0].tLogPolicy;
			logSystem->tLogLocalities = lsConf.oldTLogs[0].tLogLocalities;
			filterLocalityDataForPolicy(logSystem->tLogPolicy, &logSystem->tLogLocalities);

			logSystem->oldLogData.resize(lsConf.oldTLogs.size()-1);
			for( int i = 1; i < lsConf.oldTLogs.size(); i++ ) {
				for( int j = 0; j < lsConf.oldTLogs[i].tLogs.size(); j++) {
					logSystem->oldLogData[i-1].logServers.push_back( Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( lsConf.oldTLogs[i].tLogs[j] ) ) );
				}
				logSystem->oldLogData[i-1].tLogWriteAntiQuorum = lsConf.oldTLogs[i].tLogWriteAntiQuorum;
				logSystem->oldLogData[i-1].tLogReplicationFactor = lsConf.oldTLogs[i].tLogReplicationFactor;
				logSystem->oldLogData[i-1].tLogPolicy = lsConf.oldTLogs[i].tLogPolicy;
				logSystem->oldLogData[i-1].tLogLocalities = lsConf.oldTLogs[i].tLogLocalities;
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
		tLogLocalities.clear();
		for(auto &t : logServers) {
			newState.tLogs.push_back(t->get().id());
			tLogLocalities.push_back(t->get().interf().locality);
		}

		newState.oldTLogData.clear();
		if(!recoveryComplete.isValid() || !recoveryComplete.isReady()) {
			newState.oldTLogData.resize(oldLogData.size());
			for(int i = 0; i < oldLogData.size(); i++) {
				for(auto &t : oldLogData[i].logServers)
					newState.oldTLogData[i].tLogs.push_back(t->get().id());
				newState.oldTLogData[i].tLogWriteAntiQuorum = oldLogData[i].tLogWriteAntiQuorum;
				newState.oldTLogData[i].tLogReplicationFactor = oldLogData[i].tLogReplicationFactor;
				newState.oldTLogData[i].tLogPolicy = oldLogData[i].tLogPolicy;
				newState.oldTLogData[i].tLogLocalities = oldLogData[i].tLogLocalities;
				newState.oldTLogData[i].epochEnd = oldLogData[i].epochEnd;
			}
		}

		newState.tLogWriteAntiQuorum = tLogWriteAntiQuorum;
		newState.tLogReplicationFactor = tLogReplicationFactor;
		newState.tLogPolicy = tLogPolicy;
		newState.tLogLocalities = tLogLocalities;
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
		if (!logServers.size()) return Never();
		vector<Future<Void>> failed;

		for(auto &t : logServers)
			if( t->get().present() )
				failed.push_back( waitFailureClient( t->get().interf().waitFailure, SERVER_KNOBS->TLOG_TIMEOUT, -SERVER_KNOBS->TLOG_TIMEOUT/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY ) );

		ASSERT( failed.size() >= 1 );
		return tagError<Void>( quorum( failed, 1 ), master_tlog_failed() ) || actors.getResult();
	}

	virtual Future<Void> push( Version prevVersion, Version version, Version knownCommittedVersion, LogPushData& data, Optional<UID> debugID ) {
		// FIXME: Randomize request order as in LegacyLogSystem?
		vector<Future<Void>> tLogCommitResults;
		for(int loc=0; loc<logServers.size(); loc++) {
			Future<Void> commitMessage = reportTLogCommitErrors(
					logServers[loc]->get().interf().commit.getReply(
						TLogCommitRequest( data.getArena(), prevVersion, version, knownCommittedVersion, data.getMessages(loc), data.getTags(loc), debugID ), TaskTLogCommitReply ),
					getDebugID());
			actors.add(commitMessage);
			tLogCommitResults.push_back(commitMessage);
		}
		return quorum( tLogCommitResults, tLogCommitResults.size() - tLogWriteAntiQuorum );
	}

	virtual Reference<IPeekCursor> peek( Version begin, Tag tag, bool parallelGetMore ) {
		if(oldLogData.size() == 0 || begin >= oldLogData[0].epochEnd) {
			return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( logServers, logServers.size() ? bestLocationFor( tag ) : -1,
				(int)logServers.size() + 1 - tLogReplicationFactor, tag, begin, getPeekEnd(), parallelGetMore, tLogLocalities, tLogPolicy, tLogReplicationFactor));
		} else {
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;
			cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( logServers, logServers.size() ? bestLocationFor( tag ) : -1,
				(int)logServers.size() + 1 - tLogReplicationFactor, tag, oldLogData[0].epochEnd, getPeekEnd(), parallelGetMore, tLogLocalities, tLogPolicy, tLogReplicationFactor)) );
			for(int i = 0; i < oldLogData.size() && begin < oldLogData[i].epochEnd; i++) {
				cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( oldLogData[i].logServers, oldLogData[i].logServers.size() ? oldBestLocationFor( tag, i ) : -1,
					(int)oldLogData[i].logServers.size() + 1 - oldLogData[i].tLogReplicationFactor, tag, i+1 == oldLogData.size() ? begin : std::max(oldLogData[i+1].epochEnd, begin), oldLogData[i].epochEnd, parallelGetMore, oldLogData[i].tLogLocalities, oldLogData[i].tLogPolicy, oldLogData[i].tLogReplicationFactor)) );
				epochEnds.push_back(LogMessageVersion(oldLogData[i].epochEnd));
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual Reference<IPeekCursor> peekSingle( Version begin, Tag tag ) {
		if(oldLogData.size() == 0 || begin >= oldLogData[0].epochEnd) {
			return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( logServers.size() ?
				logServers[bestLocationFor( tag )] :
				Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, getPeekEnd(), false, false ) );
		} else {
			TEST(true); //peekSingle used during non-copying tlog recovery
			std::vector< Reference<ILogSystem::IPeekCursor> > cursors;
			std::vector< LogMessageVersion > epochEnds;
			cursors.push_back( Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( logServers.size() ?
				logServers[bestLocationFor( tag )] :
				Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, oldLogData[0].epochEnd, getPeekEnd(), false, false) ) );
			for(int i = 0; i < oldLogData.size() && begin < oldLogData[i].epochEnd; i++) {
				cursors.push_back( Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( oldLogData[i].logServers, oldLogData[i].logServers.size() ? oldBestLocationFor( tag, i ) : -1,
					(int)oldLogData[i].logServers.size() + 1 - oldLogData[i].tLogReplicationFactor, tag, i+1 == oldLogData.size() ? begin : std::max(oldLogData[i+1].epochEnd, begin), oldLogData[i].epochEnd, false,
					oldLogData[i].tLogLocalities, oldLogData[i].tLogPolicy, oldLogData[i].tLogReplicationFactor)) );
				epochEnds.push_back(LogMessageVersion(oldLogData[i].epochEnd));
			}

			return Reference<ILogSystem::MultiCursor>( new ILogSystem::MultiCursor(cursors, epochEnds) );
		}
	}

	virtual void pop( Version upTo, Tag _tag ) {
		if (!logServers.size() || !upTo) return;
		Tag tag = _tag;
		for(auto log=0; log<logServers.size(); log++) {
			Version prev = outstandingPops[std::make_pair(log,tag)];
			if (prev < upTo)
				outstandingPops[std::make_pair(log,tag)] = upTo;
			if (prev == 0)
				actors.add( popFromLog( this, log, tag ) );
		}
	}

	ACTOR static Future<Void> popFromLog( TagPartitionedLogSystem* self, int log, Tag tag ) {
		state Version last = 0;
		loop {
			Void _ = wait( delay(1.0) );  //< FIXME: knob

			state Version to = self->outstandingPops[ std::make_pair(log,tag) ];

			if (to <= last) {
				self->outstandingPops.erase( std::make_pair(log,tag) );
				return Void();
			}

			try {
				auto& interf = self->logServers[log];
				if( !interf->get().present() )
					return Void();
				Void _ = wait(interf->get().interf().popMessages.getReply( TLogPopRequest( to, tag ) ) );

				last = to;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) throw;
				TraceEvent( (e.code() == error_code_broken_promise) ? SevInfo : SevError, "LogPopError", self->dbgid ).detail("Log", self->logServers[log]->get().id()).error(e);
				return Void();  // Leaving outstandingPops filled in means no further pop requests to this tlog from this logSystem
			}
		}
	}

	ACTOR static Future<Void> confirmEpochLive_internal(TagPartitionedLogSystem* self, Optional<UID> debugID) {
		state vector<Future<Void>> alive;
		int numPresent = 0;
		for(auto& t : self->logServers) {
			if( t->get().present() ) {
				alive.push_back( brokenPromiseToNever(
				    t->get().interf().confirmRunning.getReply( TLogConfirmRunningRequest(debugID),
				                                               TaskTLogConfirmRunningReply ) ) );
				numPresent++;
			} else {
				alive.push_back( Never() );
			}
		}

		Void _ = wait( quorum( alive, std::min(self->tLogReplicationFactor, numPresent - self->tLogWriteAntiQuorum) ) );

		state Reference<LocalityGroup> locked(new LocalityGroup());
		state std::vector<bool> responded(alive.size());
		for (int i = 0; i < alive.size(); i++) {
			responded[i] = false;
		}
		loop {
			for (int i = 0; i < alive.size(); i++) {
				if (!responded[i] && alive[i].isReady() && !alive[i].isError()) {
					locked->add(self->tLogLocalities[i]);
					responded[i] = true;
				}
			}
			bool quorum_obtained = locked->validate(self->tLogPolicy);
			// We intentionally skip considering antiquorums, as the CPU cost of doing so is prohibitive.
			if (self->tLogReplicationFactor == 1 && locked->size() > 0) {
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

	// Returns success after confirming that pushes in the current epoch are still possible.
	virtual Future<Void> confirmEpochLive(Optional<UID> debugID) {
		return confirmEpochLive_internal(this, debugID);
	}

	virtual Future<Reference<ILogSystem>> newEpoch( vector<WorkerInterface> availableLogServers, DatabaseConfiguration const& config, LogEpoch recoveryCount ) {
		// Call only after end_epoch() has successfully completed.  Returns a new epoch immediately following this one.  The new epoch
		// is only provisional until the caller updates the coordinated DBCoreState
		return newEpoch( Reference<TagPartitionedLogSystem>::addRef(this), availableLogServers, config, recoveryCount );
	}

	virtual LogSystemConfig getLogSystemConfig() {
		LogSystemConfig logSystemConfig;
		logSystemConfig.logSystemType = logSystemType;
		logSystemConfig.tLogWriteAntiQuorum = tLogWriteAntiQuorum;
		logSystemConfig.tLogReplicationFactor = tLogReplicationFactor;
		logSystemConfig.tLogPolicy = tLogPolicy;
		logSystemConfig.tLogLocalities = tLogLocalities;

		for( int i = 0; i < logServers.size(); i++ )
			logSystemConfig.tLogs.push_back(logServers[i]->get());

		if(!recoveryCompleteWrittenToCoreState) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				logSystemConfig.oldTLogs.push_back(OldTLogConf());
				for( int j = 0; j < oldLogData[i].logServers.size(); j++ ) {
					logSystemConfig.oldTLogs[i].tLogs.push_back(oldLogData[i].logServers[j]->get());
				}
				logSystemConfig.oldTLogs[i].tLogWriteAntiQuorum = oldLogData[i].tLogWriteAntiQuorum;
				logSystemConfig.oldTLogs[i].tLogReplicationFactor = oldLogData[i].tLogReplicationFactor;
				logSystemConfig.oldTLogs[i].tLogPolicy = oldLogData[i].tLogPolicy;
				logSystemConfig.oldTLogs[i].tLogLocalities = oldLogData[i].tLogLocalities;
				logSystemConfig.oldTLogs[i].epochEnd = oldLogData[i].epochEnd;
			}
		}
		return logSystemConfig;
	}

	virtual Standalone<StringRef> getLogsValue() {
		vector<std::pair<UID, NetworkAddress>> logs;
		for( int i = 0; i < logServers.size(); i++ ) {
			logs.push_back(std::make_pair(logServers[i]->get().id(), logServers[i]->get().present() ? logServers[i]->get().interf().address() : NetworkAddress()));
		}

		vector<std::pair<UID, NetworkAddress>> oldLogs;
		if(!recoveryCompleteWrittenToCoreState) {
			for( int i = 0; i < oldLogData.size(); i++ ) {
				for( int j = 0; j < oldLogData[i].logServers.size(); j++ ) {
					oldLogs.push_back(std::make_pair(oldLogData[i].logServers[j]->get().id(), oldLogData[i].logServers[j]->get().present() ? oldLogData[i].logServers[j]->get().interf().address() : NetworkAddress()));
				}
			}
		}

		return logsValue( logs, oldLogs );
	}

	virtual Future<Void> onLogSystemConfigChange() {
		std::vector<Future<Void>> changes;
		changes.push_back(Never());
		for( int i = 0; i < logServers.size(); i++ )
			changes.push_back( logServers[i]->onChange() );
		for( int i = 0; i < oldLogData.size(); i++ ) {
			for( int j = 0; j < oldLogData[i].logServers.size(); j++ ) {
				changes.push_back( oldLogData[i].logServers[j]->onChange() );
			}
		}

		return waitForAny(changes);
	}

	virtual int getLogServerCount() { return logServers.size(); }

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

	int bestLocationFor( Tag tag ) {
		return tag % logServers.size();
	}

	int oldBestLocationFor( Tag tag, int idx ) {
		return tag % oldLogData[idx].logServers.size();
	}

	virtual void getPushLocations( std::vector<Tag> const& tags, std::vector<int>& locations ) {
		// Ensure that the replication server set and replication policy
		// have been defined
		ASSERT(logServerSet.getPtr());
		ASSERT(tLogPolicy.getPtr());

		std::vector<LocalityEntry>	alsoServers, resultEntries;

		for(auto& t : tags) {
			locations.push_back(bestLocationFor(t));
		}

		uniquify( locations );

		if (locations.size())
			alsoServers.reserve(locations.size());

		// Convert locations to the also servers
		for (auto location : locations) {
			ASSERT(logEntryMap[location]._id == location);
			alsoServers.push_back(logEntryMap[location]);
		}

		// Run the policy, assert if unable to satify
		bool result = logServerSet->selectReplicas(tLogPolicy, alsoServers, resultEntries);
		ASSERT(result);

		// Add the new servers to the location array
		LocalityMap<int>*			logServerMap = (LocalityMap<int>*) logServerSet.getPtr();
		for (auto entry : resultEntries) {
			locations.push_back(*logServerMap->getObject(entry));
		}

//		TraceEvent("getPushLocations").detail("Policy", tLogPolicy->info())
//			.detail("Results", locations.size()).detail("Selection", logServerSet->size())
//			.detail("Included", alsoServers.size()).detail("Duration", timer() - t);
	}

	void UpdateLocalitySet(vector<OptionalInterface<TLogInterface>> const& tlogs)
	{
		LocalityMap<int>*				logServerMap;
		logServerSet = LocalitySetRef(new LocalityMap<int>());
		logServerMap = (LocalityMap<int>*) logServerSet.getPtr();

		logEntryMap.clear();
		logIndexArray.clear();
		logIndexArray.reserve(tlogs.size());

		for( int i = 0; i < tlogs.size(); i++ ) {
			if (tlogs[i].present()) {
				logIndexArray.push_back(i);
				ASSERT(logEntryMap.find(i) == logEntryMap.end());
				logEntryMap[logIndexArray.back()] = logServerMap->add(tlogs[i].interf().locality, &logIndexArray.back());
			}
		}
	}

	void UpdateLocalitySet(
		vector<WorkerInterface>				const& workers,
		vector<InitializeTLogRequest>	const& reqs)
	{
		LocalityMap<int>*				logServerMap;

		logServerSet = LocalitySetRef(new LocalityMap<int>());
		logServerMap = (LocalityMap<int>*) logServerSet.getPtr();

		logEntryMap.clear();
		logIndexArray.clear();
		logIndexArray.reserve(workers.size());

		for( int i = 0; i < workers.size(); i++ ) {
			ASSERT(logEntryMap.find(i) == logEntryMap.end());
			logIndexArray.push_back(i);
			logEntryMap[logIndexArray.back()] = logServerMap->add(workers[i].locality, &logIndexArray.back());
		}
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
		state vector<Future<TLogLockResult>> tLogReply(prevState.tLogs.size());

		if (!prevState.tLogs.size()) {
			// This is a brand new database
			Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );
			logSystem->tLogWriteAntiQuorum = prevState.tLogWriteAntiQuorum;
			logSystem->tLogReplicationFactor = prevState.tLogReplicationFactor;
			logSystem->tLogPolicy = prevState.tLogPolicy;
			logSystem->tLogLocalities = prevState.tLogLocalities;
			logSystem->logSystemType = prevState.logSystemType;
			filterLocalityDataForPolicy(logSystem->tLogPolicy, &logSystem->tLogLocalities);

			logSystem->epochEndVersion = 0;
			logSystem->knownCommittedVersion = 0;
			outLogSystem->set(logSystem);
			Void _ = wait( Future<Void>(Never()) );
			throw internal_error();
		}

		TEST( true );	// Master recovery from pre-existing database

		// To ensure consistent recovery, the number of servers NOT in the write quorum plus the number of servers NOT in the read quorum
		// have to be strictly less than the replication factor.  Otherwise there could be a replica set consistent entirely of servers that
		// are out of date due to not being in the write quorum or unavailable due to not being in the read quorum.
		// So with N = # of tlogs, W = antiquorum, R = required count, F = replication factor,
		// W + (N - R) < F, and optimally (N-W)+(N-R)=F-1.  Thus R=N+1-F+W.
		state int requiredCount = (int)prevState.tLogs.size()+1 - prevState.tLogReplicationFactor + prevState.tLogWriteAntiQuorum;
		ASSERT( requiredCount > 0 && requiredCount <= prevState.tLogs.size() );
		ASSERT( prevState.tLogReplicationFactor >= 1 && prevState.tLogReplicationFactor <= prevState.tLogs.size() );
		ASSERT( prevState.tLogWriteAntiQuorum >= 0 && prevState.tLogWriteAntiQuorum < prevState.tLogs.size() );

		// trackRejoins listens for rejoin requests from the tLogs that we are recovering from, to learn their TLogInterfaces
		state std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
		state std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> allLogServers;
		state std::vector<OldLogData> oldLogData;
		state std::vector<Reference<AsyncVar<bool>>> logFailed;
		state std::vector<Future<Void>> failureTrackers;
		for( int i = 0; i < prevState.tLogs.size(); i++ ) {
			Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(prevState.tLogs[i]) ) );
			logServers.push_back( logVar );
			allLogServers.push_back( logVar );
			logFailed.push_back( Reference<AsyncVar<bool>>( new AsyncVar<bool>() ) );
			failureTrackers.push_back( monitorLog(logServers[i], logFailed[i] ) );
		}
		for( int i = 0; i < prevState.oldTLogData.size(); i++ ) {
			oldLogData.push_back(OldLogData());
			for(int j = 0; j < prevState.oldTLogData[i].tLogs.size(); j++) {
				Reference<AsyncVar<OptionalInterface<TLogInterface>>> logVar = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(prevState.oldTLogData[i].tLogs[j]) ) );
				oldLogData[i].logServers.push_back( logVar );
				allLogServers.push_back( logVar );
			}
			oldLogData[i].tLogReplicationFactor = prevState.oldTLogData[i].tLogReplicationFactor;
			oldLogData[i].tLogWriteAntiQuorum = prevState.oldTLogData[i].tLogWriteAntiQuorum;
			oldLogData[i].epochEnd = prevState.oldTLogData[i].epochEnd;
			oldLogData[i].tLogPolicy = prevState.oldTLogData[i].tLogPolicy;
			oldLogData[i].tLogLocalities = prevState.oldTLogData[i].tLogLocalities;
		}
		state Future<Void> rejoins = trackRejoins( dbgid, allLogServers, rejoinRequests );

		state bool buggify_lock_minimal_tlogs = BUGGIFY;
		if (!buggify_lock_minimal_tlogs) {
			for(int t=0; t<logServers.size(); t++) {
				tLogReply[t] = lockTLog( dbgid, logServers[t]);
			}
		}

		state Optional<Version> last_end;

		state bool lastWaitForRecovery = true;
		state int	cycles = 0;

		loop {
			if (buggify_lock_minimal_tlogs) {
				lockMinimalTLogSet( dbgid, prevState, logServers, logFailed, &tLogReply );
			}
			std::vector<LocalityData>	availableItems, badCombo;
			std::vector<TLogLockResult> results;
			std::string	sServerState;
			LocalityGroup	unResponsiveSet;
			std::string missingServerIds;

			double	t = timer();
			cycles ++;

			for(int t=0; t<logServers.size(); t++) {
				if (tLogReply[t].isValid() && tLogReply[t].isReady() && !tLogReply[t].isError() && !logFailed[t]->get()) {
					results.push_back(tLogReply[t].get());
					availableItems.push_back(prevState.tLogLocalities[t]);
					sServerState += 'a';
				}
				else {
					unResponsiveSet.add(prevState.tLogLocalities[t]);
					sServerState += 'f';
					if(missingServerIds.size()) {
						missingServerIds += ", ";
					}
					missingServerIds += logServers[t]->get().toString();
				}
			}

			// Check if the list of results is not larger than the anti quorum
			bool bTooManyFailures = (results.size() <= prevState.tLogWriteAntiQuorum);

			// Check if failed logs complete the policy
			bTooManyFailures = bTooManyFailures ||
				 ((unResponsiveSet.size() >= prevState.tLogReplicationFactor)	&&
				  (unResponsiveSet.validate(prevState.tLogPolicy))						);

			// Check all combinations of the AntiQuorum within the failed
			if ((!bTooManyFailures)							&&
					(prevState.tLogWriteAntiQuorum)	&&
					(!validateAllCombinations(badCombo, unResponsiveSet, prevState.tLogPolicy, availableItems, prevState.tLogWriteAntiQuorum, false)))
			{
				TraceEvent("EpochEndBadCombo", dbgid).detail("Cycles", cycles)
					.detail("Present", results.size())
					.detail("Available", availableItems.size())
					.detail("Absent", logServers.size() - results.size())
					.detail("ServerState", sServerState)
					.detail("ReplicationFactor", prevState.tLogReplicationFactor)
					.detail("AntiQuorum", prevState.tLogWriteAntiQuorum)
					.detail("Policy", prevState.tLogPolicy->info())
					.detail("TooManyFailures", bTooManyFailures)
					.detail("LogZones", ::describeZones(prevState.tLogLocalities))
					.detail("LogDataHalls", ::describeDataHalls(prevState.tLogLocalities));
				bTooManyFailures = true;
			}

			// If too many TLogs are failed for recovery to be possible, we could wait forever here.
			//Void _ = wait( smartQuorum( tLogReply, requiredCount, SERVER_KNOBS->RECOVERY_TLOG_SMART_QUORUM_DELAY ) || rejoins );

			ASSERT(logServers.size() == tLogReply.size());
			if (!bTooManyFailures) {
				std::sort( results.begin(), results.end(), sort_by_end() );
				int absent = logServers.size() - results.size();
				int safe_range_begin = prevState.tLogWriteAntiQuorum;
				int new_safe_range_begin = std::min(prevState.tLogWriteAntiQuorum, (int)(results.size()-1));
				int safe_range_end = prevState.tLogReplicationFactor - absent;

				Version end = results[ new_safe_range_begin ].end;
				Version knownCommittedVersion = end - (g_network->isSimulated() ? 10*SERVER_KNOBS->VERSIONS_PER_SECOND : SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS); //In simulation this must be the maximum MAX_READ_TRANSACTION_LIFE_VERSIONS
				for(int i = 0; i < results.size(); i++) {
					knownCommittedVersion = std::max(knownCommittedVersion, results[i].knownCommittedVersion);
				}

				if( ( prevState.logSystemType == 2 && (!last_end.present() || ((safe_range_end > 0) && (safe_range_end-1 < results.size()) && results[ safe_range_end-1 ].end < last_end.get())) ) ) {
					TEST( last_end.present() );  // Restarting recovery at an earlier point

					Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(dbgid, locality) );

					TraceEvent("LogSystemRecovery", dbgid).detail("Cycles", cycles)
						.detail("TotalServers", logServers.size())
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("Absent", logServers.size() - results.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", prevState.tLogReplicationFactor)
						.detail("AntiQuorum", prevState.tLogWriteAntiQuorum)
						.detail("Policy", prevState.tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
						.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
						.detail("EndVersion", end)
						.detail("SafeBegin", safe_range_begin)
						.detail("SafeEnd", safe_range_end)
						.detail("NewSafeBegin", new_safe_range_begin)
						.detail("LogZones", ::describeZones(prevState.tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(prevState.tLogLocalities))
						.detail("tLogs", (int)prevState.tLogs.size())
						.detail("oldTlogsSize", (int)prevState.oldTLogData.size())
						.detail("logSystemType", prevState.logSystemType)
						.detail("At", end).detail("AvailableServers", results.size())
						.detail("knownCommittedVersion", knownCommittedVersion);

					last_end = end;
					logSystem->logServers = logServers;
					logSystem->oldLogData = oldLogData;
					logSystem->tLogReplicationFactor = prevState.tLogReplicationFactor;
					logSystem->tLogWriteAntiQuorum = prevState.tLogWriteAntiQuorum;
					logSystem->tLogPolicy = prevState.tLogPolicy;
					logSystem->tLogLocalities = prevState.tLogLocalities;
					logSystem->logSystemType = prevState.logSystemType;
					logSystem->rejoins = rejoins;
					logSystem->epochEndVersion = end;
					logSystem->knownCommittedVersion = knownCommittedVersion;

					for(auto &r : results)
						logSystem->epochEndTags.insert( r.tags.begin(), r.tags.end() );

					outLogSystem->set(logSystem);
				}
				else {
					TraceEvent("LogSystemUnchangedRecovery", dbgid).detail("Cycles", cycles)
						.detail("TotalServers", logServers.size())
						.detail("Present", results.size())
						.detail("Available", availableItems.size())
						.detail("Absent", logServers.size() - results.size())
						.detail("ServerState", sServerState)
						.detail("ReplicationFactor", prevState.tLogReplicationFactor)
						.detail("AntiQuorum", prevState.tLogWriteAntiQuorum)
						.detail("Policy", prevState.tLogPolicy->info())
						.detail("TooManyFailures", bTooManyFailures)
						.detail("LastVersion", (last_end.present()) ? last_end.get() : -1L)
						.detail("RecoveryVersion", ((safe_range_end > 0) && (safe_range_end-1 < results.size())) ? results[ safe_range_end-1 ].end : -1)
						.detail("EndVersion", end)
						.detail("SafeBegin", safe_range_begin)
						.detail("SafeEnd", safe_range_end)
						.detail("NewSafeBegin", new_safe_range_begin)
						.detail("LogZones", ::describeZones(prevState.tLogLocalities))
						.detail("LogDataHalls", ::describeDataHalls(prevState.tLogLocalities));
				}
			} else {
				TraceEvent("MasterRecoveryState", dbgid)
					.detail("StatusCode", RecoveryStatus::locking_old_transaction_servers)
					.detail("Status", RecoveryStatus::names[RecoveryStatus::locking_old_transaction_servers])
					.detail("MissingIDs", missingServerIds)
					.trackLatest("MasterRecoveryState");
			}

			// Wait for anything relevant to change
			std::vector<Future<Void>> changes;
			for(int i=0; i<logServers.size(); i++) {
				if (tLogReply[i].isValid() && !tLogReply[i].isReady()) {
					changes.push_back( ready(tLogReply[i]) );
					if(buggify_lock_minimal_tlogs) {
						changes.push_back( logFailed[i]->onChange() );
					}
				} else {
					changes.push_back( logFailed[i]->onChange() );
					changes.push_back( logServers[i]->onChange() );
				}
			}
			ASSERT(changes.size());
			Void _ = wait(waitForAny(changes));
		}
	}

	ACTOR static Future<Reference<ILogSystem>> newEpoch(
		 Reference<TagPartitionedLogSystem> oldLogSystem, vector<WorkerInterface> workers, DatabaseConfiguration configuration, LogEpoch recoveryCount )
	{
		state double startTime = now();
		state Reference<TagPartitionedLogSystem> logSystem( new TagPartitionedLogSystem(oldLogSystem->getDebugID(), oldLogSystem->locality) );
		state UID recruitmentID = g_random->randomUniqueID();

		logSystem->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		logSystem->tLogReplicationFactor = configuration.tLogReplicationFactor;
		logSystem->tLogPolicy = configuration.tLogPolicy;
		logSystem->logSystemType = 2;

		if(oldLogSystem->logServers.size()) {
			logSystem->oldLogData.push_back(OldLogData());
			logSystem->oldLogData[0].tLogWriteAntiQuorum = oldLogSystem->tLogWriteAntiQuorum;
			logSystem->oldLogData[0].tLogReplicationFactor = oldLogSystem->tLogReplicationFactor;
			logSystem->oldLogData[0].tLogPolicy = oldLogSystem->tLogPolicy;
			logSystem->oldLogData[0].tLogLocalities = oldLogSystem->tLogLocalities;
			logSystem->oldLogData[0].epochEnd = oldLogSystem->knownCommittedVersion + 1;
			logSystem->oldLogData[0].logServers = oldLogSystem->logServers;
		}

		for(int i = 0; i < oldLogSystem->oldLogData.size(); i++) {
			logSystem->oldLogData.push_back(oldLogSystem->oldLogData[i]);
		}

		state vector<Future<TLogInterface>> initializationReplies;
		vector< InitializeTLogRequest > reqs( workers.size() );

		for( int i = 0; i < workers.size(); i++ ) {
			InitializeTLogRequest &req = reqs[i];
			req.recruitmentID = recruitmentID;
			req.storeType = configuration.tLogDataStoreType;
			req.recoverFrom = oldLogSystem->getLogSystemConfig();
			req.recoverAt = oldLogSystem->epochEndVersion.get();
			req.knownCommittedVersion = oldLogSystem->knownCommittedVersion;
			req.epoch = recoveryCount;
			TraceEvent("TLogInitializeRequest").detail("address", workers[i].tLog.getEndpoint().address);
		}

		logSystem->tLogLocalities.resize( workers.size() );
		logSystem->logServers.resize( workers.size() );  // Dummy interfaces, so that logSystem->getPushLocations() below uses the correct size

		// Send requests array (reqs) also
		logSystem->UpdateLocalitySet(workers, reqs);

		std::vector<int> locations;
		for( Tag tag : oldLogSystem->getEpochEndTags() ) {
			locations.clear();
			logSystem->getPushLocations( vector<Tag>(1, tag), locations );
			for(int loc : locations)
				reqs[ loc ].recoverTags.push_back( tag );
		}

		for( int i = 0; i < workers.size(); i++ )
			initializationReplies.push_back( transformErrors( throwErrorOr( workers[i].tLog.getReplyUnlessFailedFor( reqs[i], SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );

		Void _ = wait( waitForAll( initializationReplies ) );

		for( int i = 0; i < initializationReplies.size(); i++ ) {
			logSystem->logServers[i] = Reference<AsyncVar<OptionalInterface<TLogInterface>>>( new AsyncVar<OptionalInterface<TLogInterface>>( OptionalInterface<TLogInterface>(initializationReplies[i].get()) ) );
			logSystem->tLogLocalities[i] = workers[i].locality;
		}
		filterLocalityDataForPolicy(logSystem->tLogPolicy, &logSystem->tLogLocalities);

		//Don't force failure of recovery if it took us a long time to recover. This avoids multiple long running recoveries causing tests to timeout
		if (BUGGIFY && now() - startTime < 300 && g_network->isSimulated() && g_simulator.speedUpSimulation) throw master_recovery_failed();

		std::vector<Future<Void>> recoveryComplete;
		for( int i = 0; i < logSystem->logServers.size(); i++)
			recoveryComplete.push_back( transformErrors( throwErrorOr( logSystem->logServers[i]->get().interf().recoveryFinished.getReplyUnlessFailedFor( TLogRecoveryFinishedRequest(), SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) ), master_recovery_failed() ) );
		logSystem->recoveryComplete = waitForAll(recoveryComplete);

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
