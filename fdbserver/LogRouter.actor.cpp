/*
 * LogRouter.actor.cpp
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
#include "fdbclient/NativeAPI.h"
#include "WorkerInterface.h"
#include "WaitFailure.h"
#include "Knobs.h"
#include "ServerDBInfo.h"
#include "LogSystem.h"
#include "fdbclient/SystemData.h"
#include "ApplyMetadataMutation.h"
#include "RecoveryState.h"
#include "fdbclient/Atomic.h"
#include "flow/TDMetric.actor.h"
#include "flow/Stats.h"

struct LogRouterData {
	struct TagData {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		Version popped;
		Tag t;

		TagData( Version popped, Tag tag ) : popped(popped), t(tag) {}

		TagData(TagData&& r) noexcept(true) : version_messages(std::move(r.version_messages)), popped(r.popped) {}
		void operator= (TagData&& r) noexcept(true) {
			version_messages = std::move(r.version_messages);
			popped = r.popped;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore( TagData *self, Version before, LogRouterData *tlogData, int taskID ) {
			while(!self->version_messages.empty() && self->version_messages.front().first < before) {
				Version version = self->version_messages.front().first;
				int64_t messagesErased = 0;

				while(!self->version_messages.empty() && self->version_messages.front().first == version) {
					auto const& m = self->version_messages.front();
					++messagesErased;

					self->version_messages.pop_front();
				}

				Void _ = wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before, LogRouterData *tlogData, int taskID) {
			return eraseMessagesBefore(this, before, tlogData, taskID);
		}
	};

	UID dbgid;
	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	NotifiedVersion version;
	Version minPopped;
	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Map< Tag, TagData > tag_data;
	Tag routerTag;
	int logSet;

	LogRouterData(UID dbgid, Tag routerTag, int logSet) : dbgid(dbgid), routerTag(routerTag), logSet(logSet), logSystem(new AsyncVar<Reference<ILogSystem>>()) {}
};

void commitMessages( LogRouterData* self, Version version, Arena arena, StringRef messages, VectorRef< TagMessagesRef > tags) {
	if(!messages.size()) {
		return;
	}

	StringRef messages1;  // the first block of messages, if they aren't all stored contiguously.  otherwise empty

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if(self->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, messages.size()));
	}
	else {
		block = self->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	// If the current batch of messages doesn't fit entirely in the remainder of the last block in the list
	if(messages.size() + block.size() > block.capacity()) {
		// Find how many messages will fit
		LengthPrefixedStringRef r((uint32_t*)messages.begin());
		uint8_t const* end = messages.begin() + block.capacity() - block.size();
		while(r.toStringRef().end() <= end) {
			r = LengthPrefixedStringRef( (uint32_t*)r.toStringRef().end() );
		}

		// Fill up the rest of this block
		int bytes = (uint8_t*)r.getLengthPtr()-messages.begin();
		if (bytes) {
			TEST(true); // Splitting commit messages across multiple blocks
			messages1 = StringRef(block.end(), bytes);
			block.append(block.arena(), messages.begin(), bytes);
			self->messageBlocks.push_back( std::make_pair(version, block) );
			messages = messages.substr(bytes);
		}

		// Make a new block
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, messages.size()));
	}

	// Copy messages into block
	ASSERT(messages.size() <= block.capacity() - block.size());
	block.append(block.arena(), messages.begin(), messages.size());
	self->messageBlocks.push_back( std::make_pair(version, block) );
	messages = StringRef(block.end()-messages.size(), messages.size());

	for(auto tag = tags.begin(); tag != tags.end(); ++tag) {
		int64_t tagMessages = 0;

		auto tsm = self->tag_data.find(tag->tag);
		if (tsm == self->tag_data.end()) {
			tsm = self->tag_data.insert( mapPair(std::move(Tag(tag->tag)), LogRouterData::TagData(Version(0), tag->tag) ), false );
		}

		if (version >= tsm->value.popped) {
			for(int m = 0; m < tag->messageOffsets.size(); ++m) {
				int offs = tag->messageOffsets[m];
				uint8_t const* p = offs < messages1.size() ? messages1.begin() + offs : messages.begin() + offs - messages1.size();
				tsm->value.version_messages.push_back(std::make_pair(version, LengthPrefixedStringRef((uint32_t*)p)));
				if(tsm->value.version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage").detail("Size", tsm->value.version_messages.back().second.expectedSize());
				}
				++tagMessages;
			}
		}
	}
}

ACTOR Future<Void> pullAsyncData( LogRouterData *self, Tag tag ) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = self->version.get()+1;
	state Version tagPopped = 0;
	state Version lastVer = 0;
	state std::vector<int> tags;

	loop {
		loop {
			choose {
				when(Void _ = wait( r ? r->getMore() : Never() ) ) {
					break;
				}
				when( Void _ = wait( dbInfoChange ) ) { //FIXME: does this actually happen?
					if(r) tagPopped = std::max(tagPopped, r->popped());
					if( self->logSystem->get() )
						r = self->logSystem->get()->peekSingle( tagAt, tag );
					else
						r = Reference<ILogSystem::IPeekCursor>();
					dbInfoChange = self->logSystem->onChange();
				}
			}
		}

		Version ver = 0;
		Arena arena;
		BinaryWriter wr(Unversioned());
		Map<Tag, TagMessagesRef> tag_offsets;
		while (true) {
			bool foundMessage = r->hasMessage();
			if (!foundMessage || r->version().version != ver) {
				ASSERT(r->version().version > lastVer);
				if (ver) {
					VectorRef<TagMessagesRef> r;
					for(auto& t : tag_offsets)
						r.push_back( arena, t.value );
					commitMessages(self, ver, arena, wr.toStringRef(), r);
					self->version.set( ver );
					//TraceEvent("LogRouterVersion").detail("ver",ver);
				}
				lastVer = ver;
				ver = r->version().version;
				tag_offsets.clear();
				wr = BinaryWriter(Unversioned());
				arena = Arena();

				if (!foundMessage) {
					ver--; //ver is the next possible version we will get data for
					if(ver > self->version.get()) {
						commitMessages(self, ver, arena, StringRef(), VectorRef<TagMessagesRef>());
						self->version.set( ver );
					}
					break;
				}
			}

			StringRef msg = r->getMessage();
			auto originalTags = r->getTags();
			tags.clear();
			//FIXME: do we add txsTags?
			self->logSystem->get()->addRemoteTags(self->logSet, originalTags, tags);

			for(auto tag : tags) {
				auto it = tag_offsets.find(tag);
				if (it == tag_offsets.end()) {
					it = tag_offsets.insert(mapPair( Tag(tag), TagMessagesRef() ));
					it->value.tag = it->key;
				}
				it->value.messageOffsets.push_back( arena, wr.getLength() );
			}

			//FIXME: do not reserialize tags
			wr << uint32_t( msg.size() + sizeof(uint32_t) +sizeof(uint16_t) + originalTags.size()*sizeof(Tag) ) << r->version().sub << uint16_t(originalTags.size());
			for(auto t : originalTags) {
				wr << t;
			}
			wr.serializeBytes( msg );

			r->nextMessage();
		}

		tagAt = r->version().version;
	}
}

std::deque<std::pair<Version, LengthPrefixedStringRef>> & get_version_messages( LogRouterData* self, Tag tag ) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end()) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return mapIt->value.version_messages;
};

void peekMessagesFromMemory( LogRouterData* self, TLogPeekRequest const& req, BinaryWriter& messages, Version& endVersion ) {
	ASSERT( !messages.getLength() );

	auto& deque = get_version_messages(self, req.tag);
	//TraceEvent("tLogPeekMem", self->dbgid).detail("Tag", printable(req.tag1)).detail("pDS", self->persistentDataSequence).detail("pDDS", self->persistentDataDurableSequence).detail("Oldest", map1.empty() ? 0 : map1.begin()->key ).detail("OldestMsgCount", map1.empty() ? 0 : map1.begin()->value.size());

	auto it = std::lower_bound(deque.begin(), deque.end(), std::make_pair(req.begin, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());

	Version currentVersion = -1;
	for(; it != deque.end(); ++it) {
		if(it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = it->first;
				//TraceEvent("tLogPeekMessagesReached2", self->dbgid);
				break;
			}

			currentVersion = it->first;
			messages << int32_t(-1) << currentVersion;
		}

		messages << it->second.toStringRef();
	}
}

Version poppedVersion( LogRouterData* self, Tag tag) {
	auto mapIt = self->tag_data.find(tag);
	if (mapIt == self->tag_data.end())
		return Version(0);
	return mapIt->value.popped;
}

ACTOR Future<Void> logRouterPeekMessages( LogRouterData* self, TLogPeekRequest req ) {
	state BinaryWriter messages(Unversioned());

	//TraceEvent("LogRouterPeek1", self->dbgid).detail("from", req.reply.getEndpoint().address).detail("ver", self->version.get()).detail("begin", req.begin);
	if( req.returnIfBlocked && self->version.get() < req.begin ) {
		//TraceEvent("LogRouterPeek2", self->dbgid);
		req.reply.sendError(end_of_stream());
		return Void();
	}

	if( self->version.get() < req.begin ) {
		Void _ = wait( self->version.whenAtLeast( req.begin ) );
		Void _ = wait( delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()) );
	}

	Version poppedVer = poppedVersion(self, req.tag);

	if(poppedVer > req.begin) {
		//TraceEvent("LogRouterPeek3", self->dbgid);
		TLogPeekReply rep;
		rep.maxKnownVersion = self->version.get();
		rep.popped = poppedVer;
		rep.end = poppedVer;
		req.reply.send( rep );
		return Void();
	}

	Version endVersion = self->version.get() + 1;
	peekMessagesFromMemory( self, req, messages, endVersion );

	TLogPeekReply reply;
	reply.maxKnownVersion = self->version.get();
	reply.messages = messages.toStringRef();
	reply.end = endVersion;

	req.reply.send( reply );
	//TraceEvent("LogRouterPeek4", self->dbgid);
	return Void();
}

ACTOR Future<Void> logRouterPop( LogRouterData* self, TLogPopRequest req ) {
	auto ti = self->tag_data.find(req.tag);
	if (ti == self->tag_data.end()) {
		ti = self->tag_data.insert( mapPair(std::move(Tag(req.tag)), LogRouterData::TagData(req.to, req.tag)) );
	} else if (req.to > ti->value.popped) {
		ti->value.popped = req.to;
		Void _ = wait(ti->value.eraseMessagesBefore( req.to, self, TaskTLogPop ));
	}

	state Version minPopped = std::numeric_limits<Version>::max();
	for( auto& it : self->tag_data ) {
		minPopped = std::min( it.value.popped, minPopped );
	}

	while(!self->messageBlocks.empty() && self->messageBlocks.front().first <= minPopped) {
		self->messageBlocks.pop_front();
		Void _ = wait(yield(TaskUpdateStorage));
	}

	if(self->logSystem->get()) {
		self->logSystem->get()->pop(minPopped, self->routerTag);
	}
	req.reply.send(Void());
	return Void();
}

ACTOR Future<Void> logRouterCore(
	TLogInterface interf,
	Tag tag,
	int logSet,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	state LogRouterData logRouterData(interf.id(), tag, logSet);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );
	state Future<Void> dbInfoChange = Void();

	addActor.send( pullAsyncData(&logRouterData, tag) );

	loop choose {
		when( Void _ = wait( dbInfoChange ) ) {
			dbInfoChange = db->onChange();
			if( db->get().recoveryState >= RecoveryState::FULLY_RECOVERED && logSet < db->get().logSystemConfig.tLogs.size() &&
					std::count( db->get().logSystemConfig.tLogs[logSet].logRouters.begin(), db->get().logSystemConfig.tLogs[logSet].logRouters.end(), interf.id() ) ) {
				logRouterData.logSystem->set(ILogSystem::fromServerDBInfo( logRouterData.dbgid, db->get() ));
			} else {
				logRouterData.logSystem->set(Reference<ILogSystem>());
			}
		}
		when( TLogPeekRequest req = waitNext( interf.peekMessages.getFuture() ) ) {
			addActor.send( logRouterPeekMessages( &logRouterData, req ) );
		}
		when( TLogPopRequest req = waitNext( interf.popMessages.getFuture() ) ) {
			addActor.send( logRouterPop( &logRouterData, req ) );
		}
		when (Void _ = wait(error)) {}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount, TLogInterface myInterface, int logSet) {
	loop{
		if (db->get().recoveryCount >= recoveryCount && ( logSet >= db->get().logSystemConfig.expectedLogSets || ( logSet < db->get().logSystemConfig.tLogs.size() &&
			!std::count(db->get().logSystemConfig.tLogs[logSet].logRouters.begin(), db->get().logSystemConfig.tLogs[logSet].logRouters.end(), myInterface.id()) ) )) {
			throw worker_removed();
		}
		Void _ = wait(db->onChange());
	}
}

ACTOR Future<Void> logRouter(
	TLogInterface interf,
	InitializeLogRouterRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	try {
		state Future<Void> core = logRouterCore(interf, req.routerTag, req.logSet, db);
		loop choose{
			when(Void _ = wait(core)) { return Void(); }
			when(Void _ = wait(checkRemoved(db, req.recoveryCount, interf, req.logSet))) {}
		}
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed)
		{
			TraceEvent("LogRouterTerminated", interf.id()).error(e, true);
			return Void();
		}
		throw;
	}
}
