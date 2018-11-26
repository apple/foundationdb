/*
 * LogSystemPeekCursor.actor.cpp
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

#include "fdbserver/LogSystem.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbserver/Knobs.h"
#include "fdbrpc/ReplicationUtils.h"

ILogSystem::ServerPeekCursor::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf, Tag tag, Version begin, Version end, bool returnIfBlocked, bool parallelGetMore )
			: interf(interf), tag(tag), messageVersion(begin), end(end), hasMsg(false), rd(results.arena, results.messages, Unversioned()), randomID(g_random->randomUniqueID()), poppedVersion(0), returnIfBlocked(returnIfBlocked), sequence(0), parallelGetMore(parallelGetMore) {
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;
	//TraceEvent("SPC_Starting", randomID).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).backtrace();
}

ILogSystem::ServerPeekCursor::ServerPeekCursor( TLogPeekReply const& results, LogMessageVersion const& messageVersion, LogMessageVersion const& end, int32_t messageLength, int32_t rawLength, bool hasMsg, Version poppedVersion, Tag tag )
			: results(results), tag(tag), rd(results.arena, results.messages, Unversioned()), messageVersion(messageVersion), end(end), messageLength(messageLength), rawLength(rawLength), hasMsg(hasMsg), randomID(g_random->randomUniqueID()), poppedVersion(poppedVersion), returnIfBlocked(false), sequence(0), parallelGetMore(false)
{
	//TraceEvent("SPC_Clone", randomID);
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;
	if(hasMsg)
		nextMessage();

	advanceTo(messageVersion);
}

Reference<ILogSystem::IPeekCursor> ILogSystem::ServerPeekCursor::cloneNoMore() {
	return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( results, messageVersion, end, messageLength, rawLength, hasMsg, poppedVersion, tag ) );
}

void ILogSystem::ServerPeekCursor::setProtocolVersion( uint64_t version ) {
	rd.setProtocolVersion(version);
}

Arena& ILogSystem::ServerPeekCursor::arena() { return results.arena; }

ArenaReader* ILogSystem::ServerPeekCursor::reader() {
	return &rd;
}

bool ILogSystem::ServerPeekCursor::hasMessage() {
	//TraceEvent("SPC_HasMessage", randomID).detail("HasMsg", hasMsg);
	return hasMsg;
}

void ILogSystem::ServerPeekCursor::nextMessage() {
	//TraceEvent("SPC_NextMessage", randomID).detail("MessageVersion", messageVersion.toString());
	ASSERT(hasMsg);
	if (rd.empty()) {
		messageVersion.reset(std::min(results.end, end.version));
		hasMsg = false;
		return;
	}
	if (*(int32_t*)rd.peekBytes(4) == -1) {
		// A version
		int32_t dummy;
		Version ver;
		rd >> dummy >> ver;

		//TraceEvent("SPC_ProcessSeq", randomID).detail("MessageVersion", messageVersion.toString()).detail("Ver", ver).detail("Tag", tag.toString());
		//ASSERT( ver >= messageVersion.version );

		messageVersion.reset(ver);

		if( messageVersion >= end ) {
			messageVersion = end;
			hasMsg = false;
			return;
		}
		ASSERT(!rd.empty());
	}

	uint16_t tagCount;
	rd.checkpoint();
	rd >> messageLength >> messageVersion.sub >> tagCount;
	tags.resize(tagCount);
	for(int i = 0; i < tagCount; i++) {
		rd >> tags[i];
	}
	rawLength = messageLength + sizeof(messageLength);
	messageLength -= (sizeof(messageVersion.sub) + sizeof(tagCount) + tagCount*sizeof(Tag));
	hasMsg = true;
	//TraceEvent("SPC_NextMessageB", randomID).detail("MessageVersion", messageVersion.toString());
}

StringRef ILogSystem::ServerPeekCursor::getMessage() {
	//TraceEvent("SPC_GetMessage", randomID);
	return StringRef( (uint8_t const*)rd.readBytes(messageLength), messageLength);
}

StringRef ILogSystem::ServerPeekCursor::getMessageWithTags() {
	rd.rewind();
	return StringRef( (uint8_t const*)rd.readBytes(rawLength), rawLength);
}

const std::vector<Tag>& ILogSystem::ServerPeekCursor::getTags() {
	return tags;
}

void ILogSystem::ServerPeekCursor::advanceTo(LogMessageVersion n) {
	//TraceEvent("SPC_AdvanceTo", randomID).detail("N", n.toString());
	while( messageVersion < n && hasMessage() ) {
		getMessage();
		nextMessage();
	}

	if( hasMessage() )
		return;

	//if( more.isValid() && !more.isReady() ) more.cancel();

	if( messageVersion < n ) {
		messageVersion = n;
	}
}

ACTOR Future<Void> serverPeekParallelGetMore( ILogSystem::ServerPeekCursor* self, int taskID ) {
	if( !self->interf || self->messageVersion >= self->end ) {
		wait( Future<Void>(Never()));
		throw internal_error();
	}

	if(!self->interfaceChanged.isValid()) {
		self->interfaceChanged = self->interf->onChange();
	}

	loop {
		state Version expectedBegin = self->messageVersion.version;
		try {
			while(self->futureResults.size() < SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS && self->interf->get().present()) {
				self->futureResults.push_back( brokenPromiseToNever( self->interf->get().interf().peekMessages.getReply(TLogPeekRequest(self->messageVersion.version,self->tag,self->returnIfBlocked, std::make_pair(self->randomID, self->sequence++)), taskID) ) );
			}

			choose {
				when( TLogPeekReply res = wait( self->interf->get().present() ? self->futureResults.front() : Never() ) ) {
					if(res.begin.get() != expectedBegin) {
						throw timed_out();
					}
					expectedBegin = res.end;
					self->futureResults.pop_front();
					self->results = res;
					if(res.popped.present())
						self->poppedVersion = std::min( std::max(self->poppedVersion, res.popped.get()), self->end.version );
					self->rd = ArenaReader( self->results.arena, self->results.messages, Unversioned() );
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					//TraceEvent("SPC_GetMoreB", self->randomID).detail("Has", self->hasMessage()).detail("End", res.end).detail("Popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when( wait( self->interfaceChanged ) ) {
					self->interfaceChanged = self->interf->onChange();
					self->randomID = g_random->randomUniqueID();
					self->sequence = 0;
					self->futureResults.clear();
				}
			}
		} catch( Error &e ) {
			if(e.code() == error_code_end_of_stream) {
				self->end.reset( self->messageVersion.version );
				return Void();
			} else if(e.code() == error_code_timed_out) {
				TraceEvent("PeekCursorTimedOut", self->randomID);
				self->interfaceChanged = self->interf->onChange();
				self->randomID = g_random->randomUniqueID();
				self->sequence = 0;
				self->futureResults.clear();
			} else {
				throw e;
			}
		}
	}
}

ACTOR Future<Void> serverPeekGetMore( ILogSystem::ServerPeekCursor* self, int taskID ) {
	if( !self->interf || self->messageVersion >= self->end ) {
		wait( Future<Void>(Never()));
		throw internal_error();
	}
	try {
		loop {
			choose {
				when( TLogPeekReply res = wait( self->interf->get().present() ?
					brokenPromiseToNever( self->interf->get().interf().peekMessages.getReply(TLogPeekRequest(self->messageVersion.version,self->tag,self->returnIfBlocked), taskID) ) : Never() ) ) {
					self->results = res;
					if(res.popped.present())
						self->poppedVersion = std::min( std::max(self->poppedVersion, res.popped.get()), self->end.version );
					self->rd = ArenaReader( self->results.arena, self->results.messages, Unversioned() );
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					//TraceEvent("SPC_GetMoreB", self->randomID).detail("Has", self->hasMessage()).detail("End", res.end).detail("Popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when( wait( self->interf->onChange() ) ) {}
			}
		}
	} catch( Error &e ) {
		if(e.code() == error_code_end_of_stream) {
			self->end.reset( self->messageVersion.version );
			return Void();
		}
		throw e;
	}
}

Future<Void> ILogSystem::ServerPeekCursor::getMore(int taskID) {
	//TraceEvent("SPC_GetMore", randomID).detail("HasMessage", hasMessage()).detail("More", !more.isValid() || more.isReady()).detail("MessageVersion", messageVersion.toString()).detail("End", end.toString());
	if( hasMessage() )
		return Void();
	if( !more.isValid() || more.isReady() ) {
		more = parallelGetMore ? serverPeekParallelGetMore(this, taskID) : serverPeekGetMore(this, taskID);
	}
	return more;
}

ACTOR Future<Void> serverPeekOnFailed( ILogSystem::ServerPeekCursor* self ) {
	loop {
		choose {
			when( wait( self->interf->get().present() ? IFailureMonitor::failureMonitor().onDisconnectOrFailure( self->interf->get().interf().peekMessages.getEndpoint() ) : Never() ) ) { return Void(); }
			when( wait( self->interf->onChange() ) ) {}
		}
	}
}

Future<Void> ILogSystem::ServerPeekCursor::onFailed() {
	return serverPeekOnFailed(this);
}

bool ILogSystem::ServerPeekCursor::isActive() {
	if( !interf->get().present() )
		return false;
	if( messageVersion >= end )
		return false;
	return IFailureMonitor::failureMonitor().getState( interf->get().interf().peekMessages.getEndpoint() ).isAvailable();
}

bool ILogSystem::ServerPeekCursor::isExhausted() {
	return messageVersion >= end;
}

const LogMessageVersion& ILogSystem::ServerPeekCursor::version() { return messageVersion; } // Call only after nextMessage().  The sequence of the current message, or results.end if nextMessage() has returned false.

Version ILogSystem::ServerPeekCursor::getMinKnownCommittedVersion() { return results.minKnownCommittedVersion; }

Version ILogSystem::ServerPeekCursor::popped() { return poppedVersion; }

ILogSystem::MergedPeekCursor::MergedPeekCursor( vector< Reference<ILogSystem::IPeekCursor> > const& serverCursors, Version begin )
	: serverCursors(serverCursors), bestServer(-1), readQuorum(serverCursors.size()), tag(invalidTag), currentCursor(0), hasNextMessage(false),
	messageVersion(begin), randomID(g_random->randomUniqueID()), tLogReplicationFactor(0) {
	sortedVersions.resize(serverCursors.size());
}

ILogSystem::MergedPeekCursor::MergedPeekCursor( std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers, int bestServer, int readQuorum, Tag tag, Version begin, Version end,
	bool parallelGetMore, std::vector< LocalityData > const& tLogLocalities, IRepPolicyRef const tLogPolicy, int tLogReplicationFactor )
	: bestServer(bestServer), readQuorum(readQuorum), tag(tag), currentCursor(0), hasNextMessage(false), messageVersion(begin), randomID(g_random->randomUniqueID()), tLogReplicationFactor(tLogReplicationFactor) {
	if(tLogPolicy) {
		logSet = Reference<LogSet>( new LogSet() );
		logSet->tLogPolicy = tLogPolicy;
		logSet->tLogLocalities = tLogLocalities;
		filterLocalityDataForPolicy(logSet->tLogPolicy, &logSet->tLogLocalities);
		logSet->updateLocalitySet(logSet->tLogLocalities);
	}

	for( int i = 0; i < logServers.size(); i++ ) {
		Reference<ILogSystem::ServerPeekCursor> cursor( new ILogSystem::ServerPeekCursor( logServers[i], tag, begin, end, bestServer >= 0, parallelGetMore ) );
		//TraceEvent("MPC_Starting", randomID).detail("Cursor", cursor->randomID).detail("End", end);
		serverCursors.push_back( cursor );
	}
	sortedVersions.resize(serverCursors.size());
}

ILogSystem::MergedPeekCursor::MergedPeekCursor( vector< Reference<ILogSystem::IPeekCursor> > const& serverCursors, LogMessageVersion const& messageVersion, int bestServer, int readQuorum, Optional<LogMessageVersion> nextVersion, Reference<LogSet> logSet, int tLogReplicationFactor )
	: serverCursors(serverCursors), bestServer(bestServer), readQuorum(readQuorum), currentCursor(0), hasNextMessage(false), messageVersion(messageVersion), nextVersion(nextVersion), logSet(logSet),
	randomID(g_random->randomUniqueID()), tLogReplicationFactor(tLogReplicationFactor) {
	sortedVersions.resize(serverCursors.size());
	calcHasMessage();
}

Reference<ILogSystem::IPeekCursor> ILogSystem::MergedPeekCursor::cloneNoMore() {
	vector< Reference<ILogSystem::IPeekCursor> > cursors;
	for( auto it : serverCursors ) {
		cursors.push_back(it->cloneNoMore());
	}
	return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( cursors, messageVersion, bestServer, readQuorum, nextVersion, logSet, tLogReplicationFactor ) );
}

void ILogSystem::MergedPeekCursor::setProtocolVersion( uint64_t version ) {
	for( auto it : serverCursors )
		if( it->hasMessage() )
			it->setProtocolVersion( version );
}

Arena& ILogSystem::MergedPeekCursor::arena() { return serverCursors[currentCursor]->arena(); }

ArenaReader* ILogSystem::MergedPeekCursor::reader() { return serverCursors[currentCursor]->reader(); }


void ILogSystem::MergedPeekCursor::calcHasMessage() {
	if(bestServer >= 0) {
		if(nextVersion.present()) serverCursors[bestServer]->advanceTo( nextVersion.get() );
		if( serverCursors[bestServer]->hasMessage() ) {
			messageVersion = serverCursors[bestServer]->version();
			currentCursor = bestServer;
			hasNextMessage = true;

			for (auto& c : serverCursors)
				c->advanceTo(messageVersion);

			return;
		}

		auto bestVersion = serverCursors[bestServer]->version();
		for (auto& c : serverCursors)
			c->advanceTo(bestVersion);
	}

	hasNextMessage = false;
	updateMessage(false);

	if(!hasNextMessage && logSet) {
		updateMessage(true);
	}
}

void ILogSystem::MergedPeekCursor::updateMessage(bool usePolicy) {
	loop {
		bool advancedPast = false;
		sortedVersions.clear();
		for(int i = 0; i < serverCursors.size(); i++) {
			auto& serverCursor = serverCursors[i];
			if (nextVersion.present()) serverCursor->advanceTo(nextVersion.get());
			sortedVersions.push_back(std::pair<LogMessageVersion, int>(serverCursor->version(), i));
		}

		if(usePolicy) {
			ASSERT(logSet->tLogPolicy);
			std::sort(sortedVersions.begin(), sortedVersions.end());

			locations.clear();
			for(auto sortedVersion : sortedVersions) {
				locations.push_back(logSet->logEntryArray[sortedVersion.second]);
				if( locations.size() >= tLogReplicationFactor && logSet->satisfiesPolicy(locations) ) {
					messageVersion = sortedVersion.first;
					break;
				}
			}
		} else {
			std::nth_element(sortedVersions.begin(), sortedVersions.end()-readQuorum, sortedVersions.end());
			messageVersion = sortedVersions[sortedVersions.size()-readQuorum].first;
		}

		for(int i = 0; i < serverCursors.size(); i++) {
			auto& c = serverCursors[i];
			auto start = c->version();
			c->advanceTo(messageVersion);
			if( start <= messageVersion && messageVersion < c->version() ) {
				advancedPast = true;
				TEST(true); //Merge peek cursor advanced past desired sequence
			}
		}

		if(!advancedPast)
			break;
	}

	for(int i = 0; i < serverCursors.size(); i++) {
		auto& c = serverCursors[i];
		ASSERT_WE_THINK( !c->hasMessage() || c->version() >= messageVersion );  // Seems like the loop above makes this unconditionally true
		if (c->version() == messageVersion && c->hasMessage()) {
			hasNextMessage = true;
			currentCursor = i;
			break;
		}
	}
}

bool ILogSystem::MergedPeekCursor::hasMessage() {
	return hasNextMessage;
}

void ILogSystem::MergedPeekCursor::nextMessage() {
	nextVersion = version();
	nextVersion.get().sub++;
	serverCursors[currentCursor]->nextMessage();
	calcHasMessage();
	ASSERT(hasMessage() || !version().sub);
}

StringRef ILogSystem::MergedPeekCursor::getMessage() { return serverCursors[currentCursor]->getMessage(); }

StringRef ILogSystem::MergedPeekCursor::getMessageWithTags() {
	return serverCursors[currentCursor]->getMessageWithTags();
}

const std::vector<Tag>& ILogSystem::MergedPeekCursor::getTags() {
	return serverCursors[currentCursor]->getTags();
}

void ILogSystem::MergedPeekCursor::advanceTo(LogMessageVersion n) {
	bool canChange = false;
	for (auto& c : serverCursors) {
		if(c->version() < n) {
			canChange = true;
			c->advanceTo(n);
		}
	}
	if(canChange) {
		calcHasMessage();
	}
}

ACTOR Future<Void> mergedPeekGetMore(ILogSystem::MergedPeekCursor* self, LogMessageVersion startVersion, int taskID) {
	loop {
		//TraceEvent("MPC_GetMoreA", self->randomID).detail("Start", startVersion.toString());
		if(self->bestServer >= 0 && self->serverCursors[self->bestServer]->isActive()) {
			ASSERT(!self->serverCursors[self->bestServer]->hasMessage());
			wait( self->serverCursors[self->bestServer]->getMore(taskID) || self->serverCursors[self->bestServer]->onFailed() );
		} else {
			vector<Future<Void>> q;
			for (auto& c : self->serverCursors)
				if (!c->hasMessage())
					q.push_back(c->getMore(taskID));
			wait(quorum(q, 1));
		}
		self->calcHasMessage();
		//TraceEvent("MPC_GetMoreB", self->randomID).detail("HasMessage", self->hasMessage()).detail("Start", startVersion.toString()).detail("Seq", self->version().toString());
		if (self->hasMessage() || self->version() > startVersion) {
			return Void();
		}
	}
}

Future<Void> ILogSystem::MergedPeekCursor::getMore(int taskID) {
	if(!serverCursors.size())
		return Never();
	
	auto startVersion = version();
	calcHasMessage();
	if( hasMessage() )
		return Void();
	if (nextVersion.present())
		advanceTo(nextVersion.get());
	ASSERT(!hasMessage());
	if (version() > startVersion)
		return Void();

	return mergedPeekGetMore(this, startVersion, taskID);
}

Future<Void> ILogSystem::MergedPeekCursor::onFailed() {
	ASSERT(false);
	return Never();
}

bool ILogSystem::MergedPeekCursor::isActive() {
	ASSERT(false);
	return false;
}

bool ILogSystem::MergedPeekCursor::isExhausted() {
	return serverCursors[currentCursor]->isExhausted();
}

const LogMessageVersion& ILogSystem::MergedPeekCursor::version() { return messageVersion; }

Version ILogSystem::MergedPeekCursor::getMinKnownCommittedVersion() {
	return serverCursors[currentCursor]->getMinKnownCommittedVersion();
}

Version ILogSystem::MergedPeekCursor::popped() {
	Version poppedVersion = 0;
	for (auto& c : serverCursors)
		poppedVersion = std::max(poppedVersion, c->popped());
	return poppedVersion;
}

ILogSystem::SetPeekCursor::SetPeekCursor( std::vector<Reference<LogSet>> const& logSets, int bestSet, int bestServer, Tag tag, Version begin, Version end, bool parallelGetMore )
	: logSets(logSets), bestSet(bestSet), bestServer(bestServer), tag(tag), currentCursor(0), currentSet(bestSet), hasNextMessage(false), messageVersion(begin), useBestSet(true), randomID(g_random->randomUniqueID()) {
	serverCursors.resize(logSets.size());
	int maxServers = 0;
	for( int i = 0; i < logSets.size(); i++ ) {
		for( int j = 0; j < logSets[i]->logServers.size(); j++) {
			Reference<ILogSystem::ServerPeekCursor> cursor( new ILogSystem::ServerPeekCursor( logSets[i]->logServers[j], tag, begin, end, true, parallelGetMore ) );
			serverCursors[i].push_back( cursor );
		}
		maxServers = std::max<int>(maxServers, serverCursors[i].size());
	}
	sortedVersions.resize(maxServers);
}

ILogSystem::SetPeekCursor::SetPeekCursor( std::vector<Reference<LogSet>> const& logSets, std::vector< std::vector< Reference<IPeekCursor> > > const& serverCursors, LogMessageVersion const& messageVersion, int bestSet, int bestServer, 
	Optional<LogMessageVersion> nextVersion, bool useBestSet ) : logSets(logSets), serverCursors(serverCursors), messageVersion(messageVersion), bestSet(bestSet), bestServer(bestServer), nextVersion(nextVersion), currentSet(bestSet), currentCursor(0),
	hasNextMessage(false), useBestSet(useBestSet), randomID(g_random->randomUniqueID()) {
	int maxServers = 0;
	for( int i = 0; i < logSets.size(); i++ ) {
		maxServers = std::max<int>(maxServers, serverCursors[i].size());
	}
	sortedVersions.resize(maxServers);
	calcHasMessage();
}

Reference<ILogSystem::IPeekCursor> ILogSystem::SetPeekCursor::cloneNoMore() {
	vector< vector< Reference<ILogSystem::IPeekCursor> > > cursors;
	cursors.resize(logSets.size());
	for( int i = 0; i < logSets.size(); i++ ) {
		for( int j = 0; j < logSets[i]->logServers.size(); j++) {
			cursors[i].push_back( serverCursors[i][j]->cloneNoMore() );
		}
	}
	return Reference<ILogSystem::SetPeekCursor>( new ILogSystem::SetPeekCursor( logSets, cursors, messageVersion, bestSet, bestServer, nextVersion, useBestSet ) );
}

void ILogSystem::SetPeekCursor::setProtocolVersion( uint64_t version ) {
	for( auto& cursors : serverCursors ) {
		for( auto& it : cursors ) {
			if( it->hasMessage() ) {
				it->setProtocolVersion( version );
			}
		}
	}
}

Arena& ILogSystem::SetPeekCursor::arena() { return serverCursors[currentSet][currentCursor]->arena(); }

ArenaReader* ILogSystem::SetPeekCursor::reader() { return serverCursors[currentSet][currentCursor]->reader(); }


void ILogSystem::SetPeekCursor::calcHasMessage() {
	if(bestSet >= 0 && bestServer >= 0) {
		if(nextVersion.present()) {
			//TraceEvent("LPC_CalcNext").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage).detail("NextVersion", nextVersion.get().toString());
			serverCursors[bestSet][bestServer]->advanceTo( nextVersion.get() );
		}
		if( serverCursors[bestSet][bestServer]->hasMessage() ) {
			messageVersion = serverCursors[bestSet][bestServer]->version();
			currentSet = bestSet;
			currentCursor = bestServer;
			hasNextMessage = true;

			//TraceEvent("LPC_Calc1").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage);

			for (auto& cursors : serverCursors) {
				for(auto& c : cursors) {
					c->advanceTo(messageVersion);
				}
			}

			return;
		}

		auto bestVersion = serverCursors[bestSet][bestServer]->version();
		for (auto& cursors : serverCursors) {
			for (auto& c : cursors) {
				c->advanceTo(bestVersion);
			}
		}
	}

	hasNextMessage = false;
	if(useBestSet) {
		updateMessage(bestSet, false); // Use Quorum logic

		//TraceEvent("LPC_Calc2").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage);
		if(!hasNextMessage) {
			updateMessage(bestSet, true);
			//TraceEvent("LPC_Calc3").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage);
		}
	} else {
		for(int i = 0; i < logSets.size() && !hasNextMessage; i++) {
			if(i != bestSet) {
				updateMessage(i, false); // Use Quorum logic
			}
		}
		//TraceEvent("LPC_Calc4").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage);
		for(int i = 0; i < logSets.size() && !hasNextMessage; i++) {
			if(i != bestSet) {
				updateMessage(i, true);
			}
		}
		//TraceEvent("LPC_Calc5").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage);
	}
}

void ILogSystem::SetPeekCursor::updateMessage(int logIdx, bool usePolicy) {
	loop {
		bool advancedPast = false;
		sortedVersions.clear();
		for(int i = 0; i < serverCursors[logIdx].size(); i++) {
			auto& serverCursor = serverCursors[logIdx][i];
			if (nextVersion.present()) serverCursor->advanceTo(nextVersion.get());
			sortedVersions.push_back(std::pair<LogMessageVersion, int>(serverCursor->version(), i));
			//TraceEvent("LPC_Update1").detail("Ver", messageVersion.toString()).detail("Tag", tag.toString()).detail("HasNextMessage", hasNextMessage).detail("ServerVer", serverCursor->version().toString()).detail("I", i);
		}

		if(usePolicy) {
			std::sort(sortedVersions.begin(), sortedVersions.end());
			locations.clear();
			for(auto sortedVersion : sortedVersions) {
				auto& locality = logSets[logIdx]->tLogLocalities[sortedVersion.second];
				locations.push_back(logSets[logIdx]->logEntryArray[sortedVersion.second]);
				if( locations.size() >= logSets[logIdx]->tLogReplicationFactor && logSets[logIdx]->satisfiesPolicy(locations) ) {
					messageVersion = sortedVersion.first;
					break;
				}
			}
		} else {
			//(int)oldLogData[i].logServers.size() + 1 - oldLogData[i].tLogReplicationFactor
			std::nth_element(sortedVersions.begin(), sortedVersions.end()-(logSets[logIdx]->logServers.size()+1-logSets[logIdx]->tLogReplicationFactor), sortedVersions.end());
			messageVersion = sortedVersions[sortedVersions.size()-(logSets[logIdx]->logServers.size()+1-logSets[logIdx]->tLogReplicationFactor)].first;
		}

		for (auto& cursors : serverCursors) {
			for (auto& c : cursors) {
				auto start = c->version();
				c->advanceTo(messageVersion);
				if( start <= messageVersion && messageVersion < c->version() ) {
					advancedPast = true;
					TEST(true); //Merge peek cursor advanced past desired sequence
				}
			}
		}

		if(!advancedPast)
			break;
	}

	for(int i = 0; i < serverCursors[logIdx].size(); i++) {
		auto& c = serverCursors[logIdx][i];
		ASSERT_WE_THINK( !c->hasMessage() || c->version() >= messageVersion );  // Seems like the loop above makes this unconditionally true
		if (c->version() == messageVersion && c->hasMessage()) {
			hasNextMessage = true;
			currentSet = logIdx;
			currentCursor = i;
			break;
		}
	}
}

bool ILogSystem::SetPeekCursor::hasMessage() {
	return hasNextMessage;
}

void ILogSystem::SetPeekCursor::nextMessage() {
	nextVersion = version();
	nextVersion.get().sub++;
	serverCursors[currentSet][currentCursor]->nextMessage();
	calcHasMessage();
	ASSERT(hasMessage() || !version().sub);
}

StringRef ILogSystem::SetPeekCursor::getMessage() { return serverCursors[currentSet][currentCursor]->getMessage(); }

StringRef ILogSystem::SetPeekCursor::getMessageWithTags() { return serverCursors[currentSet][currentCursor]->getMessageWithTags(); }

const std::vector<Tag>& ILogSystem::SetPeekCursor::getTags() {
	return serverCursors[currentSet][currentCursor]->getTags();
}

void ILogSystem::SetPeekCursor::advanceTo(LogMessageVersion n) {
	bool canChange = false;
	for( auto& cursors : serverCursors ) {
		for (auto& c : cursors) {
			if(c->version() < n) {
				canChange = true;
				c->advanceTo(n);
			}
		}
	}
	if(canChange) {
		calcHasMessage();
	}
}

ACTOR Future<Void> setPeekGetMore(ILogSystem::SetPeekCursor* self, LogMessageVersion startVersion, int taskID) {
	loop {
		//TraceEvent("LPC_GetMore1", self->randomID).detail("Start", startVersion.toString()).detail("Tag", self->tag);
		if(self->bestServer >= 0 && self->bestSet >= 0 && self->serverCursors[self->bestSet][self->bestServer]->isActive()) {
			ASSERT(!self->serverCursors[self->bestSet][self->bestServer]->hasMessage());
			//TraceEvent("LPC_GetMore2", self->randomID).detail("Start", startVersion.toString()).detail("Tag", self->tag);
			wait( self->serverCursors[self->bestSet][self->bestServer]->getMore(taskID) || self->serverCursors[self->bestSet][self->bestServer]->onFailed() );
			self->useBestSet = true;
		} else {
			//FIXME: if best set is exhausted, do not peek remote servers
			bool bestSetValid = self->bestSet >= 0;
			if(bestSetValid) {
				self->locations.clear();
				for( int i = 0; i < self->serverCursors[self->bestSet].size(); i++) {
					if(!self->serverCursors[self->bestSet][i]->isActive() && self->serverCursors[self->bestSet][i]->version() <= self->messageVersion) {
						self->locations.push_back(self->logSets[self->bestSet]->logEntryArray[i]);
					}
				}
				bestSetValid = self->locations.size() < self->logSets[self->bestSet]->tLogReplicationFactor || !self->logSets[self->bestSet]->satisfiesPolicy(self->locations);
			}
			if(bestSetValid || self->logSets.size() == 1) {
				if(!self->useBestSet) {
					self->useBestSet = true;
					self->calcHasMessage();
					if (self->hasMessage() || self->version() > startVersion)
						return Void();
				}

				//TraceEvent("LPC_GetMore3", self->randomID).detail("Start", startVersion.toString()).detail("Tag", self->tag.toString()).detail("BestSetSize", self->serverCursors[self->bestSet].size());
				vector<Future<Void>> q;
				for (auto& c : self->serverCursors[self->bestSet]) {
					if (!c->hasMessage()) {
						q.push_back(c->getMore(taskID));
						if(c->isActive()) {
							q.push_back(c->onFailed());
						}
					}
				}
				wait(quorum(q, 1));
			} else {
				//FIXME: this will peeking way too many cursors when satellites exist, and does not need to peek bestSet cursors since we cannot get anymore data from them
				vector<Future<Void>> q;
				//TraceEvent("LPC_GetMore4", self->randomID).detail("Start", startVersion.toString()).detail("Tag", self->tag);
				for(auto& cursors : self->serverCursors) {
					for (auto& c :cursors) {
						if (!c->hasMessage()) {
							q.push_back(c->getMore(taskID));
						}
					}
				}
				wait(quorum(q, 1));
				self->useBestSet = false;
			}
		}
		self->calcHasMessage();
		//TraceEvent("LPC_GetMoreB", self->randomID).detail("HasMessage", self->hasMessage()).detail("Start", startVersion.toString()).detail("Seq", self->version().toString());
		if (self->hasMessage() || self->version() > startVersion)
			return Void();
	}
}

Future<Void> ILogSystem::SetPeekCursor::getMore(int taskID) {
	auto startVersion = version();
	calcHasMessage();
	if( hasMessage() )
		return Void();
	if (nextVersion.present())
		advanceTo(nextVersion.get());
	ASSERT(!hasMessage());
	if (version() > startVersion)
		return Void();

	return setPeekGetMore(this, startVersion, taskID);
}

Future<Void> ILogSystem::SetPeekCursor::onFailed() {
	ASSERT(false);
	return Never();
}

bool ILogSystem::SetPeekCursor::isActive() {
	ASSERT(false);
	return false;
}

bool ILogSystem::SetPeekCursor::isExhausted() {
	return serverCursors[currentSet][currentCursor]->isExhausted();
}

const LogMessageVersion& ILogSystem::SetPeekCursor::version() { return messageVersion; }

Version ILogSystem::SetPeekCursor::getMinKnownCommittedVersion() {
	return serverCursors[currentSet][currentCursor]->getMinKnownCommittedVersion();
}

Version ILogSystem::SetPeekCursor::popped() {
	Version poppedVersion = 0;
	for (auto& cursors : serverCursors) {
		for(auto& c : cursors) {
			poppedVersion = std::max(poppedVersion, c->popped());
		}
	}
	return poppedVersion;
}

ILogSystem::MultiCursor::MultiCursor( std::vector<Reference<IPeekCursor>> cursors, std::vector<LogMessageVersion> epochEnds ) : cursors(cursors), epochEnds(epochEnds), poppedVersion(0) {
	for(int i = 0; i < std::min<int>(cursors.size(),SERVER_KNOBS->MULTI_CURSOR_PRE_FETCH_LIMIT); i++) {
		cursors[cursors.size()-i-1]->getMore();
	}
}

Reference<ILogSystem::IPeekCursor> ILogSystem::MultiCursor::cloneNoMore() {
	return cursors.back()->cloneNoMore();
}

void ILogSystem::MultiCursor::setProtocolVersion( uint64_t version ) {
	cursors.back()->setProtocolVersion(version);
}

Arena& ILogSystem::MultiCursor::arena() {
	return cursors.back()->arena();
}

ArenaReader* ILogSystem::MultiCursor::reader() {
	return cursors.back()->reader();
}

bool ILogSystem::MultiCursor::hasMessage() {
	return cursors.back()->hasMessage();
}

void ILogSystem::MultiCursor::nextMessage() {
	cursors.back()->nextMessage();
}

StringRef ILogSystem::MultiCursor::getMessage() {
	return cursors.back()->getMessage();
}

StringRef ILogSystem::MultiCursor::getMessageWithTags() {
	return cursors.back()->getMessageWithTags();
}

const std::vector<Tag>& ILogSystem::MultiCursor::getTags() {
	return cursors.back()->getTags();
}

void ILogSystem::MultiCursor::advanceTo(LogMessageVersion n) {
	while( cursors.size() > 1 && n >= epochEnds.back() ) {
		poppedVersion = std::max(poppedVersion, cursors.back()->popped());
		cursors.pop_back();
		epochEnds.pop_back();
	}
	cursors.back()->advanceTo(n);
}

Future<Void> ILogSystem::MultiCursor::getMore(int taskID) {
	LogMessageVersion startVersion = cursors.back()->version();
	while( cursors.size() > 1 && cursors.back()->version() >= epochEnds.back() ) {
		poppedVersion = std::max(poppedVersion, cursors.back()->popped());
		cursors.pop_back();
		epochEnds.pop_back();
	}
	if(cursors.back()->version() > startVersion) {
		return Void();
	}
	return cursors.back()->getMore(taskID);
}

Future<Void> ILogSystem::MultiCursor::onFailed() {
	return cursors.back()->onFailed();
}

bool ILogSystem::MultiCursor::isActive() {
	return cursors.back()->isActive();
}

bool ILogSystem::MultiCursor::isExhausted() {
	return cursors.back()->isExhausted();
}

const LogMessageVersion& ILogSystem::MultiCursor::version() {
	return cursors.back()->version();
}

Version ILogSystem::MultiCursor::getMinKnownCommittedVersion() {
	return cursors.back()->getMinKnownCommittedVersion();
}

Version ILogSystem::MultiCursor::popped() {
	return std::max(poppedVersion, cursors.back()->popped());
}

ILogSystem::BufferedCursor::BufferedCursor( std::vector<Reference<IPeekCursor>> cursors, Version begin, Version end, bool collectTags ) : cursors(cursors), messageVersion(begin), end(end), collectTags(collectTags), hasNextMessage(false), messageIndex(0) {
	messages.reserve(10000);
}

void ILogSystem::BufferedCursor::combineMessages() {
	if(!hasNextMessage) {
		return;
	}

	tags.clear();
	tags.push_back(messages[messageIndex].tags[0]);
	for(int i = messageIndex + 1; i < messages.size() && messages[messageIndex].version == messages[i].version; i++) {
		tags.push_back(messages[i].tags[0]);
		messageIndex = i;
	}
	auto& msg = messages[messageIndex];
	BinaryWriter messageWriter(Unversioned());
	messageWriter << uint32_t(msg.message.size() + sizeof(uint32_t) + sizeof(uint16_t) + tags.size()*sizeof(Tag)) << msg.version.sub << uint16_t(tags.size());
	for(auto& t : tags) {
		messageWriter << t;
	}
	messageWriter.serializeBytes(msg.message);
	msg.arena = Arena();
	msg.tags = tags;
	msg.message = StringRef(msg.arena, messageWriter.toStringRef());
}

Reference<ILogSystem::IPeekCursor> ILogSystem::BufferedCursor::cloneNoMore() {
	ASSERT(false);
	return Reference<ILogSystem::IPeekCursor>();
}

void ILogSystem::BufferedCursor::setProtocolVersion( uint64_t version ) {
	for(auto& c : cursors) {
		c->setProtocolVersion(version);
	}
}

Arena& ILogSystem::BufferedCursor::arena() {
	return messages[messageIndex].arena;
}

ArenaReader* ILogSystem::BufferedCursor::reader() {
	ASSERT(false);
	return cursors[0]->reader();
}

bool ILogSystem::BufferedCursor::hasMessage() {
	return hasNextMessage;
}

void ILogSystem::BufferedCursor::nextMessage() {
	messageIndex++;
	if(messageIndex == messages.size()) {
		hasNextMessage = false;
	}
	if(collectTags) {
		combineMessages();
	}
}

StringRef ILogSystem::BufferedCursor::getMessage() {
	ASSERT(false);
	return StringRef();
}

StringRef ILogSystem::BufferedCursor::getMessageWithTags() {
	return messages[messageIndex].message;
}

const std::vector<Tag>& ILogSystem::BufferedCursor::getTags() {
	return messages[messageIndex].tags;
}

void ILogSystem::BufferedCursor::advanceTo(LogMessageVersion n) {
	ASSERT(false);
}

ACTOR Future<Void> bufferedGetMoreLoader( ILogSystem::BufferedCursor* self, Reference<ILogSystem::IPeekCursor> cursor, Version maxVersion, int taskID ) {
	loop {
		wait(yield());
		if(cursor->version().version >= maxVersion) {
			return Void();
		}
		while(cursor->hasMessage()) {
			self->messages.push_back(ILogSystem::BufferedCursor::BufferedMessage(cursor->arena(), self->collectTags ? cursor->getMessage() : cursor->getMessageWithTags(), cursor->getTags(), cursor->version()));
			cursor->nextMessage();
			if(cursor->version().version >= maxVersion) {
				return Void();
			}
		}
		wait(cursor->getMore(taskID));
	}
}

ACTOR Future<Void> bufferedGetMore( ILogSystem::BufferedCursor* self, int taskID ) {
	if( self->messageVersion.version >= self->end ) {
		wait( Future<Void>(Never()));
		throw internal_error();
	}

	state Version targetVersion = std::min(self->end, self->messageVersion.version + SERVER_KNOBS->VERSIONS_PER_BATCH);
	self->messages.clear();

	std::vector<Future<Void>> loaders;
	loaders.reserve(self->cursors.size());
	for(auto& cursor : self->cursors) {
		loaders.push_back(bufferedGetMoreLoader(self, cursor, targetVersion, taskID));
	}
	wait( waitForAll(loaders) );
	wait(yield());

	if(self->collectTags) {
		std::sort(self->messages.begin(), self->messages.end());
	} else {
		uniquify(self->messages);
	}
	self->messageIndex = 0;
	self->hasNextMessage = self->messages.size() > 0;
	self->messageVersion = LogMessageVersion(targetVersion);

	if(self->collectTags) {
		self->combineMessages();
	}

	wait(yield());
	return Void();
}

Future<Void> ILogSystem::BufferedCursor::getMore(int taskID) {
	if( hasMessage() )
		return Void();
	return bufferedGetMore(this, taskID);
}

Future<Void> ILogSystem::BufferedCursor::onFailed() {
	ASSERT(false);
	return Never();
}

bool ILogSystem::BufferedCursor::isActive() {
	ASSERT(false);
	return false;
}

bool ILogSystem::BufferedCursor::isExhausted() {
	ASSERT(false);
	return false;
}

const LogMessageVersion& ILogSystem::BufferedCursor::version() {
	if(hasNextMessage) {
		return messages[messageIndex].version;
	}
	return messageVersion;
}

Version ILogSystem::BufferedCursor::getMinKnownCommittedVersion() {
	ASSERT(false);
	return invalidVersion;
}

Version ILogSystem::BufferedCursor::popped() {
	ASSERT(false);
	return invalidVersion;
}
