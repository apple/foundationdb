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

#include "LogSystem.h"
#include "fdbrpc/FailureMonitor.h"
#include "Knobs.h"
#include "fdbrpc/ReplicationUtils.h"

ILogSystem::ServerPeekCursor::ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf, Tag tag, Version begin, Version end, bool returnIfBlocked, bool parallelGetMore )
			: interf(interf), tag(tag), messageVersion(begin), end(end), hasMsg(false), rd(results.arena, results.messages, Unversioned()), randomID(g_random->randomUniqueID()), poppedVersion(0), returnIfBlocked(returnIfBlocked), sequence(0), parallelGetMore(parallelGetMore) {
	this->results.maxKnownVersion = 0;
	//TraceEvent("SPC_starting", randomID).detail("tag", printable(tag)).detail("begin", begin).detail("end", end);
}

ILogSystem::ServerPeekCursor::ServerPeekCursor( TLogPeekReply const& results, LogMessageVersion const& messageVersion, LogMessageVersion const& end, int32_t messageLength, bool hasMsg, Version poppedVersion )
			: results(results), rd(results.arena, results.messages, Unversioned()), messageVersion(messageVersion), end(end), messageLength(messageLength), hasMsg(hasMsg), randomID(g_random->randomUniqueID()), poppedVersion(poppedVersion), returnIfBlocked(false), sequence(0), parallelGetMore(false)
{
	//TraceEvent("SPC_clone", randomID);
	this->results.maxKnownVersion = 0;
	if(hasMsg)
		nextMessage();

	advanceTo(messageVersion);
}

Reference<ILogSystem::IPeekCursor> ILogSystem::ServerPeekCursor::cloneNoMore() {
	return Reference<ILogSystem::ServerPeekCursor>( new ILogSystem::ServerPeekCursor( results, messageVersion, end, messageLength, hasMsg, poppedVersion ) );
}

void ILogSystem::ServerPeekCursor::setProtocolVersion( uint64_t version ) {
	rd.setProtocolVersion(version);
}

Arena& ILogSystem::ServerPeekCursor::arena() { return results.arena; }

ArenaReader* ILogSystem::ServerPeekCursor::reader() {
	return &rd;
}

bool ILogSystem::ServerPeekCursor::hasMessage() {
	//TraceEvent("SPC_hasMessage", randomID).detail("hasMsg", hasMsg);
	return hasMsg;
}

void ILogSystem::ServerPeekCursor::nextMessage() {
	//TraceEvent("SPC_nextMessage", randomID).detail("messageVersion", messageVersion.toString());
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

		//TraceEvent("SPC_process_Seq", randomID).detail("messageVersion", messageVersion.toString()).detail("ver", ver);
		//ASSERT( ver >= messageVersion.version );

		messageVersion.reset(ver);

		if( messageVersion >= end ) {
			messageVersion = end;
			hasMsg = false;
			return;
		}
		ASSERT(!rd.empty());
	}

	rd >> messageLength >> messageVersion.sub;
	messageLength -= sizeof(messageVersion.sub);
	hasMsg = true;
	//TraceEvent("SPC_nextMessageB", randomID).detail("messageVersion", messageVersion.toString());
}

StringRef ILogSystem::ServerPeekCursor::getMessage() {
	//TraceEvent("SPC_getMessage", randomID);
	return StringRef( (uint8_t const*)rd.readBytes(messageLength), messageLength);
}

void ILogSystem::ServerPeekCursor::advanceTo(LogMessageVersion n) {
	//TraceEvent("SPC_advanceTo", randomID).detail("n", n.toString());
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
		Void _ = wait( Future<Void>(Never()));
		throw internal_error();
	}

	if(!self->interfaceChanged.isValid()) {
		self->interfaceChanged = self->interf->onChange();
	}

	loop {
		try {
			while(self->futureResults.size() < SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS && self->interf->get().present()) {
				self->futureResults.push_back( brokenPromiseToNever( self->interf->get().interf().peekMessages.getReply(TLogPeekRequest(self->messageVersion.version,self->tag,self->returnIfBlocked, std::make_pair(self->randomID, self->sequence++)), taskID) ) );
			}

			choose {
				when( TLogPeekReply res = wait( self->interf->get().present() ? self->futureResults.front() : Never() ) ) {
					self->futureResults.pop_front();
					self->results = res;
					if(res.popped.present())
						self->poppedVersion = std::min( std::max(self->poppedVersion, res.popped.get()), self->end.version );
					self->rd = ArenaReader( self->results.arena, self->results.messages, Unversioned() );
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					//TraceEvent("SPC_getMoreB", self->randomID).detail("has", self->hasMessage()).detail("end", res.end).detail("popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when( Void _ = wait( self->interfaceChanged ) ) {
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
		Void _ = wait( Future<Void>(Never()));
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
					//TraceEvent("SPC_getMoreB", self->randomID).detail("has", self->hasMessage()).detail("end", res.end).detail("popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when( Void _ = wait( self->interf->onChange() ) ) {}
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
	//TraceEvent("SPC_getMore", randomID).detail("hasMessage", hasMessage()).detail("more", !more.isValid() || more.isReady()).detail("messageVersion", messageVersion.toString()).detail("end", end.toString());
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
			when( Void _ = wait( self->interf->get().present() ? IFailureMonitor::failureMonitor().onDisconnectOrFailure( self->interf->get().interf().peekMessages.getEndpoint() ) : Never() ) ) { return Void(); }
			when( Void _ = wait( self->interf->onChange() ) ) {}
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

LogMessageVersion ILogSystem::ServerPeekCursor::version() { return messageVersion; } // Call only after nextMessage().  The sequence of the current message, or results.end if nextMessage() has returned false.

Version ILogSystem::ServerPeekCursor::popped() { return poppedVersion; }

ILogSystem::MergedPeekCursor::MergedPeekCursor( std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers, int bestServer, int readQuorum, Tag tag, Version begin, Version end, bool parallelGetMore, std::vector< LocalityData > const& tLogLocalities, IRepPolicyRef const tLogPolicy, int tLogReplicationFactor )
	: bestServer(bestServer), readQuorum(readQuorum), tag(tag), currentCursor(0), hasNextMessage(false), messageVersion(begin), randomID(g_random->randomUniqueID()), tLogLocalities(tLogLocalities), tLogPolicy(tLogPolicy), tLogReplicationFactor(tLogReplicationFactor) {
	for( int i = 0; i < logServers.size(); i++ ) {
		Reference<ILogSystem::ServerPeekCursor> cursor( new ILogSystem::ServerPeekCursor( logServers[i], tag, begin, end, true, parallelGetMore ) );
		//TraceEvent("MPC_starting", randomID).detail("cursor", cursor->randomID).detail("end", end);
		serverCursors.push_back( cursor );
	}
	sortedVersions.resize(serverCursors.size());
	filterLocalityDataForPolicy(this->tLogPolicy, &this->tLogLocalities);
}

ILogSystem::MergedPeekCursor::MergedPeekCursor( vector< Reference<ILogSystem::IPeekCursor> > const& serverCursors, LogMessageVersion const& messageVersion, int bestServer, int readQuorum, Optional<LogMessageVersion> nextVersion, std::vector< LocalityData > const& tLogLocalities, IRepPolicyRef const tLogPolicy, int tLogReplicationFactor )
	: serverCursors(serverCursors), bestServer(bestServer), readQuorum(readQuorum), currentCursor(0), hasNextMessage(false), messageVersion(messageVersion), nextVersion(nextVersion), randomID(g_random->randomUniqueID()), tLogLocalities(tLogLocalities), tLogPolicy(tLogPolicy), tLogReplicationFactor(tLogReplicationFactor) {
	sortedVersions.resize(serverCursors.size());
	calcHasMessage();
	filterLocalityDataForPolicy(this->tLogPolicy, &this->tLogLocalities);
}

Reference<ILogSystem::IPeekCursor> ILogSystem::MergedPeekCursor::cloneNoMore() {
	vector< Reference<ILogSystem::IPeekCursor> > cursors;
	for( auto it : serverCursors ) {
		cursors.push_back(it->cloneNoMore());
	}
	return Reference<ILogSystem::MergedPeekCursor>( new ILogSystem::MergedPeekCursor( cursors, messageVersion, bestServer, readQuorum, nextVersion, tLogLocalities, tLogPolicy, tLogReplicationFactor ) );
}

void ILogSystem::MergedPeekCursor::setProtocolVersion( uint64_t version ) {
	for( auto it : serverCursors )
		if( it->hasMessage() )
			it->setProtocolVersion( version );
}

Arena& ILogSystem::MergedPeekCursor::arena() { return serverCursors[currentCursor]->arena(); }

ArenaReader* ILogSystem::MergedPeekCursor::reader() { return serverCursors[currentCursor]->reader(); }


void ILogSystem::MergedPeekCursor::calcHasMessage() {
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

	hasNextMessage = false;
	updateMessage(false); // Use Quorum logic

	if(!hasNextMessage) {
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
			ASSERT(tLogPolicy);
			localityGroup.clear();
			std::sort(sortedVersions.begin(), sortedVersions.end());

			for(auto sortedVersion : sortedVersions) {
				auto& locality = tLogLocalities[sortedVersion.second];
				localityGroup.add(locality);

				if( localityGroup.size() >= tLogReplicationFactor && localityGroup.validate(tLogPolicy) ) {
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
			if( start < messageVersion && messageVersion < c->version() ) {
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

void ILogSystem::MergedPeekCursor::advanceTo(LogMessageVersion n) {
	for (auto& c : serverCursors)
		c->advanceTo(n);
	calcHasMessage();
}

ACTOR Future<Void> mergedPeekGetMore(ILogSystem::MergedPeekCursor* self, LogMessageVersion startVersion, int taskID) {
	loop {
		//TraceEvent("MPC_getMoreA", self->randomID).detail("start", startVersion.toString());
		if(self->serverCursors[self->bestServer]->isActive()) {
			ASSERT(!self->serverCursors[self->bestServer]->hasMessage());
			Void _ = wait( self->serverCursors[self->bestServer]->getMore(taskID) || self->serverCursors[self->bestServer]->onFailed() );
		} else {
			vector<Future<Void>> q;
			for (auto& c : self->serverCursors)
				if (!c->hasMessage())
					q.push_back(c->getMore(taskID));
			Void _ = wait(quorum(q, 1));
		}
		self->calcHasMessage();
		//TraceEvent("MPC_getMoreB", self->randomID).detail("hasMessage", self->hasMessage()).detail("start", startVersion.toString()).detail("seq", self->version().toString());
		if (self->hasMessage() || self->version() > startVersion)
			return Void();
	}
}

Future<Void> ILogSystem::MergedPeekCursor::getMore(int taskID) {
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

LogMessageVersion ILogSystem::MergedPeekCursor::version() { return messageVersion; }

Version ILogSystem::MergedPeekCursor::popped() {
	Version poppedVersion = 0;
	for (auto& c : serverCursors)
		poppedVersion = std::max(poppedVersion, c->popped());
	return poppedVersion;
}

ILogSystem::MultiCursor::MultiCursor( std::vector<Reference<IPeekCursor>> cursors, std::vector<LogMessageVersion> epochEnds ) : cursors(cursors), epochEnds(epochEnds), poppedVersion(0) {}

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

void ILogSystem::MultiCursor::advanceTo(LogMessageVersion n) {
	while( cursors.size() > 1 && n >= epochEnds.back() ) {
		poppedVersion = std::max(poppedVersion, cursors.back()->popped());
		cursors.pop_back();
		epochEnds.pop_back();
	}
	cursors.back()->advanceTo(n);
}

Future<Void> ILogSystem::MultiCursor::getMore(int taskID) {
	while( cursors.size() > 1 && cursors.back()->version() >= epochEnds.back() ) {
		poppedVersion = std::max(poppedVersion, cursors.back()->popped());
		cursors.pop_back();
		epochEnds.pop_back();
	}
	return cursors.back()->getMore(taskID);
}

Future<Void> ILogSystem::MultiCursor::onFailed() {
	return cursors.back()->onFailed();
}

bool ILogSystem::MultiCursor::isActive() {
	return cursors.back()->isActive();
}

LogMessageVersion ILogSystem::MultiCursor::version() {
	return cursors.back()->version();
}

Version ILogSystem::MultiCursor::popped() {
	return std::max(poppedVersion, cursors.back()->popped());
}
