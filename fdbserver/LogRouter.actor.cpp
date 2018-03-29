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
	struct TagData : NonCopyable, public ReferenceCounted<TagData> {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		Version popped;
		Tag tag;

		TagData( Tag tag, Version popped ) : tag(tag), popped(popped) {}

		TagData(TagData&& r) noexcept(true) : version_messages(std::move(r.version_messages)), tag(r.tag), popped(r.popped) {}
		void operator= (TagData&& r) noexcept(true) {
			version_messages = std::move(r.version_messages);
			tag = r.tag;
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
	NotifiedVersion minPopped;
	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Tag routerTag;
	int logSet;

	std::vector<Reference<TagData>> tag_data; //we only store data for the remote tag locality

	Reference<TagData> getTagData(Tag tag) {
		ASSERT(tag.locality == tagLocalityRemoteLog);
		if(tag.id >= tag_data.size()) {
			tag_data.resize(tag.id+1);
		}
		return tag_data[tag.id];
	}

	//only callable after getTagData returns a null reference
	Reference<TagData> createTagData(Tag tag, Version popped) {
		Reference<TagData> newTagData = Reference<TagData>( new TagData(tag, popped) );
		tag_data[tag.id] = newTagData;
		return newTagData;
	}

	LogRouterData(UID dbgid, Tag routerTag, int logSet) : dbgid(dbgid), routerTag(routerTag), logSet(logSet), logSystem(new AsyncVar<Reference<ILogSystem>>()) {}
};

void commitMessages( LogRouterData* self, Version version, const std::vector<TagsAndMessage>& taggedMessages ) {
	if(!taggedMessages.size()) {
		return;
	}

	int msgSize = 0;
	for(auto& i : taggedMessages) {
		msgSize += i.message.size();
	}

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if(self->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
	}
	else {
		block = self->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	for(auto& msg : taggedMessages) {
		if(msg.message.size() > block.capacity() - block.size()) {
			self->messageBlocks.push_back( std::make_pair(version, block) );
			block = Standalone<VectorRef<uint8_t>>();
			block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
		}

		block.append(block.arena(), msg.message.begin(), msg.message.size());
		for(auto& tag : msg.tags) {
			auto tagData = self->getTagData(tag);
			if(!tagData) {
				tagData = self->createTagData(tag, 0);
			}

			if (version >= tagData->popped) {
				tagData->version_messages.push_back(std::make_pair(version, LengthPrefixedStringRef((uint32_t*)(block.end() - msg.message.size()))));
				if(tagData->version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage").detail("Size", tagData->version_messages.back().second.expectedSize());
				}
			}
		}
		
		msgSize -= msg.message.size();
	}
	self->messageBlocks.push_back( std::make_pair(version, block) );
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
						r = self->logSystem->get()->peekLogRouter( tagAt, tag, self->dbgid );
					else
						r = Reference<ILogSystem::IPeekCursor>();
					dbInfoChange = self->logSystem->onChange();
				}
			}
		}

		state Version ver = 0;
		state std::vector<TagsAndMessage> messages;
		while (true) {
			state bool foundMessage = r->hasMessage();
			if (!foundMessage || r->version().version != ver) {
				ASSERT(r->version().version > lastVer);
				if (ver) {
					Void _ = wait(self->minPopped.whenAtLeast(ver - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS));
					commitMessages(self, ver, messages);
					self->version.set( ver );
					//TraceEvent("LogRouterVersion").detail("ver",ver);
				}
				lastVer = ver;
				ver = r->version().version;
				messages.clear();

				if (!foundMessage) {
					ver--; //ver is the next possible version we will get data for
					if(ver > self->version.get()) {
						self->version.set( ver );
					}
					break;
				}
			}

			TagsAndMessage tagAndMsg;
			tagAndMsg.message = r->getMessageWithTags();
			tags.clear();
			self->logSystem->get()->addRemoteTags(self->logSet, r->getTags(), tags);
			for(auto t : tags) {
				tagAndMsg.tags.push_back(Tag(tagLocalityRemoteLog, t));
			}
			messages.push_back(std::move(tagAndMsg));

			r->nextMessage();
		}

		tagAt = r->version().version;
	}
}

std::deque<std::pair<Version, LengthPrefixedStringRef>> & get_version_messages( LogRouterData* self, Tag tag ) {
	auto tagData = self->getTagData(tag);
	if (!tagData) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return tagData->version_messages;
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
	auto tagData = self->getTagData(tag);
	if (!tagData)
		return Version(0);
	return tagData->popped;
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
	auto tagData = self->getTagData(req.tag);
	if (!tagData) {
		tagData = self->createTagData(req.tag, req.to);
	} else if (req.to > tagData->popped) {
		tagData->popped = req.to;
		Void _ = wait(tagData->eraseMessagesBefore( req.to, self, TaskTLogPop ));
	}

	state Version minPopped = std::numeric_limits<Version>::max();
	for( auto it : self->tag_data ) {
		if(it) {
			minPopped = std::min( it->popped, minPopped );
		}
	}

	while(!self->messageBlocks.empty() && self->messageBlocks.front().first <= minPopped) {
		self->messageBlocks.pop_front();
		Void _ = wait(yield(TaskUpdateStorage));
	}

	if(self->logSystem->get()) {
		self->logSystem->get()->pop(minPopped, self->routerTag);
	}
	req.reply.send(Void());
	self->minPopped.set(minPopped);
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
			logRouterData.logSystem->set(ILogSystem::fromServerDBInfo( logRouterData.dbgid, db->get() ));
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
		bool isDisplaced = ( (db->get().recoveryCount > recoveryCount && db->get().recoveryState != 0) || (db->get().recoveryCount == recoveryCount && db->get().recoveryState == 7) );
		if(isDisplaced) {
			for(auto& log : db->get().logSystemConfig.tLogs) {
				if( std::count( log.logRouters.begin(), log.logRouters.end(), myInterface.id() ) ) {
					isDisplaced = false;
					break;
				}
			}
		}
		if(isDisplaced) {
			for(auto& old : db->get().logSystemConfig.oldTLogs) {
				for(auto& log : old.tLogs) {
					 if( std::count( log.logRouters.begin(), log.logRouters.end(), myInterface.id() ) ) {
						isDisplaced = false;
						break;
					 }
				}
			}
		}
		if (isDisplaced) {
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
