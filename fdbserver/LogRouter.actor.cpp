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

#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/LogSystem.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/RecoveryState.h"
#include "fdbclient/Atomic.h"
#include "flow/TDMetric.actor.h"
#include "flow/Stats.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct LogRouterData {
	struct TagData : NonCopyable, public ReferenceCounted<TagData> {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		Version popped;
		Version durableKnownCommittedVersion;
		Tag tag;

		TagData( Tag tag, Version popped, Version durableKnownCommittedVersion ) : tag(tag), popped(popped), durableKnownCommittedVersion(durableKnownCommittedVersion) {}

		TagData(TagData&& r) noexcept
		  : version_messages(std::move(r.version_messages)), tag(r.tag), popped(r.popped),
		    durableKnownCommittedVersion(r.durableKnownCommittedVersion) {}
		void operator=(TagData&& r) noexcept {
			version_messages = std::move(r.version_messages);
			tag = r.tag;
			popped = r.popped;
			durableKnownCommittedVersion = r.durableKnownCommittedVersion;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore( TagData *self, Version before, LogRouterData *tlogData, TaskPriority taskID ) {
			while(!self->version_messages.empty() && self->version_messages.front().first < before) {
				Version version = self->version_messages.front().first;
				int64_t messagesErased = 0;

				while(!self->version_messages.empty() && self->version_messages.front().first == version) {
					++messagesErased;

					self->version_messages.pop_front();
				}

				wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before, LogRouterData *tlogData, TaskPriority taskID) {
			return eraseMessagesBefore(this, before, tlogData, taskID);
		}
	};

	const UID dbgid;
	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	NotifiedVersion version;
	NotifiedVersion minPopped;
	const Version startVersion;
	Version minKnownCommittedVersion;
	Version poppedVersion;
	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Tag routerTag;
	bool allowPops;
	LogSet logSet;
	bool foundEpochEnd;
	double waitForVersionTime = 0;
	double maxWaitForVersionTime = 0;
	double getMoreTime = 0;
	double maxGetMoreTime = 0;

	Version readTxnLifetime = 5 * SERVER_KNOBS->VERSIONS_PER_SECOND; // 5s versions

	struct PeekTrackerData {
		std::map<int, Promise<std::pair<Version, bool>>> sequence_version;
		double lastUpdate;
	};

	std::map<UID, PeekTrackerData> peekTracker;

	CounterCollection cc;
	Counter getMoreCount, getMoreBlockedCount;
	Future<Void> logger;
	Reference<EventCacheHolder> eventCacheHolder;

	std::vector<Reference<TagData>> tag_data; //we only store data for the remote tag locality

	Reference<TagData> getTagData(Tag tag) {
		ASSERT(tag.locality == tagLocalityRemoteLog);
		if(tag.id >= tag_data.size()) {
			tag_data.resize(tag.id+1);
		}
		return tag_data[tag.id];
	}

	//only callable after getTagData returns a null reference
	Reference<TagData> createTagData(Tag tag, Version popped, Version knownCommittedVersion) {
		Reference<TagData> newTagData(new TagData(tag, popped, knownCommittedVersion));
		tag_data[tag.id] = newTagData;
		return newTagData;
	}

	LogRouterData(UID dbgid, const InitializeLogRouterRequest& req)
	  : dbgid(dbgid), routerTag(req.routerTag), logSystem(new AsyncVar<Reference<ILogSystem>>()),
	    version(req.startVersion - 1), minPopped(0), startVersion(req.startVersion), allowPops(false),
	    minKnownCommittedVersion(0), poppedVersion(0), foundEpochEnd(false), cc("LogRouter", dbgid.toString()),
	    getMoreCount("GetMoreCount", cc), getMoreBlockedCount("GetMoreBlockedCount", cc),
	    readTxnLifetime(req.readTxnLifetime) {
		ASSERT_WE_THINK(req.readTxnLifetime > 0);
		//setup just enough of a logSet to be able to call getPushLocations
		logSet.logServers.resize(req.tLogLocalities.size());
		logSet.tLogPolicy = req.tLogPolicy;
		logSet.locality = req.locality;
		logSet.updateLocalitySet(req.tLogLocalities);

		for(int i = 0; i < req.tLogLocalities.size(); i++) {
			Tag tag(tagLocalityRemoteLog, i);
			auto tagData = getTagData(tag);
			if(!tagData) {
				tagData = createTagData(tag, 0, 0);
			}
		}

		eventCacheHolder = Reference<EventCacheHolder>( new EventCacheHolder(dbgid.shortString() + ".PeekLocation") );

		specialCounter(cc, "Version", [this](){ return this->version.get(); });
		specialCounter(cc, "MinPopped", [this](){ return this->minPopped.get(); });
		specialCounter(cc, "FetchedVersions", [this]() {
			return std::max<Version>(
			    0, std::min<Version>(this->readTxnLifetime, this->version.get() - this->minPopped.get()));
		});
		specialCounter(cc, "MinKnownCommittedVersion", [this](){ return this->minKnownCommittedVersion; });
		specialCounter(cc, "PoppedVersion", [this](){ return this->poppedVersion; });
		specialCounter(cc, "FoundEpochEnd", [this](){ return this->foundEpochEnd; });
		specialCounter(cc, "WaitForVersionMS", [this](){ double val = this->waitForVersionTime; this->waitForVersionTime = 0; return 1000*val; });
		specialCounter(cc, "WaitForVersionMaxMS", [this](){ double val = this->maxWaitForVersionTime; this->maxWaitForVersionTime = 0; return 1000*val; });
		specialCounter(cc, "GetMoreMS", [this](){ double val = this->getMoreTime; this->getMoreTime = 0; return 1000*val; });
		specialCounter(cc, "GetMoreMaxMS", [this](){ double val = this->maxGetMoreTime; this->maxGetMoreTime = 0; return 1000*val; });
		logger = traceCounters("LogRouterMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "LogRouterMetrics");
	}
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
			self->messageBlocks.emplace_back(version, block);
			block = Standalone<VectorRef<uint8_t>>();
			block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
		}

		block.append(block.arena(), msg.message.begin(), msg.message.size());
		for(auto& tag : msg.tags) {
			auto tagData = self->getTagData(tag);
			if(!tagData) {
				tagData = self->createTagData(tag, 0, 0);
			}

			if (version >= tagData->popped) {
				tagData->version_messages.emplace_back(version, LengthPrefixedStringRef((uint32_t*)(block.end() - msg.message.size())));
				if(tagData->version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage").detail("Size", tagData->version_messages.back().second.expectedSize());
				}
			}
		}

		msgSize -= msg.message.size();
	}
	self->messageBlocks.emplace_back(version, block);
}

ACTOR Future<Void> waitForVersion( LogRouterData *self, Version ver ) {
	// The only time the log router should allow a gap in versions larger than MAX_READ_TRANSACTION_LIFE_VERSIONS is when processing epoch end.
	// Since one set of log routers is created per generation of transaction logs, the gap caused by epoch end will be within MAX_VERSIONS_IN_FLIGHT of the log routers start version.
	state double startTime = now();
	if(self->version.get() < self->startVersion) {
		if(ver > self->startVersion) {
			self->version.set(self->startVersion);
			wait(self->minPopped.whenAtLeast(self->version.get()));
		}
		self->waitForVersionTime += now() - startTime;
		self->maxWaitForVersionTime = std::max(self->maxWaitForVersionTime, now() - startTime);
		return Void();
	}
	if(!self->foundEpochEnd) {
		wait(self->minPopped.whenAtLeast(std::min(self->version.get(), ver - self->readTxnLifetime)));
	} else {
		while (self->minPopped.get() + self->readTxnLifetime < ver) {
			if (self->minPopped.get() + self->readTxnLifetime > self->version.get()) {
				self->version.set(self->minPopped.get() + self->readTxnLifetime);
				wait(yield(TaskPriority::TLogCommit));
			} else {
				wait(self->minPopped.whenAtLeast((self->minPopped.get()+1)));
			}
		}
	}
	if(ver >= self->startVersion + SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT) {
		self->foundEpochEnd = true;
	}
	self->waitForVersionTime += now() - startTime;
	self->maxWaitForVersionTime = std::max(self->maxWaitForVersionTime, now() - startTime);
	return Void();
}

// Pull data from the last epoch
ACTOR Future<Void> pullAsyncData( LogRouterData *self ) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = self->version.get() + 1;
	state Version lastVer = 0;
	state std::vector<int> tags; // an optimization to avoid reallocating vector memory in every loop

	loop {
		loop {
			Future<Void> getMoreF = Never();
			if(r) {
				getMoreF = r->getMore(TaskPriority::TLogCommit);
				++self->getMoreCount;
				if(!getMoreF.isReady()) {
					++self->getMoreBlockedCount;
				}
			}
			state double startTime = now();
			choose {
				when(wait( getMoreF ) ) {
					self->getMoreTime += now() - startTime;
					self->maxGetMoreTime = std::max(self->maxGetMoreTime, now() - startTime);
					break;
				}
				when( wait( dbInfoChange ) ) { //FIXME: does this actually happen?
					if( self->logSystem->get() ) {
						r = self->logSystem->get()->peekLogRouter( self->dbgid, tagAt, self->routerTag );
						TraceEvent("LogRouterPeekLocation", self->dbgid).detail("LogID", r->getPrimaryPeekLocation()).trackLatest(self->eventCacheHolder->trackingKey);
					} else {
						r = Reference<ILogSystem::IPeekCursor>();
					}
					dbInfoChange = self->logSystem->onChange();
				}
			}
		}

		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		state Version ver = 0;
		state std::vector<TagsAndMessage> messages;
		state Arena arena;
		while (true) {
			state bool foundMessage = r->hasMessage();
			if (!foundMessage || r->version().version != ver) {
				ASSERT(r->version().version > lastVer);
				if (ver) {
					wait( waitForVersion(self, ver) );

					commitMessages(self, ver, messages);
					self->version.set( ver );
					wait(yield(TaskPriority::TLogCommit));
					//TraceEvent("LogRouterVersion").detail("Ver",ver);
				}
				lastVer = ver;
				ver = r->version().version;
				messages.clear();
				arena = Arena();

				if (!foundMessage) {
					ver--; //ver is the next possible version we will get data for
					if(ver > self->version.get()) {
						wait( waitForVersion(self, ver) );

						self->version.set( ver );
						wait(yield(TaskPriority::TLogCommit));
					}
					break;
				}
			}

			TagsAndMessage tagAndMsg;
			tagAndMsg.message = r->getMessageWithTags();
			tags.clear();
			self->logSet.getPushLocations(r->getTags(), tags, 0);
			tagAndMsg.tags.reserve(arena, tags.size());
			for (const auto& t : tags) {
				tagAndMsg.tags.push_back(arena, Tag(tagLocalityRemoteLog, t));
			}
			messages.push_back(std::move(tagAndMsg));

			r->nextMessage();
		}

		tagAt = std::max( r->version().version, self->version.get() + 1 );
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
	//TraceEvent("TLogPeekMem", self->dbgid).detail("Tag", req.tag1).detail("PDS", self->persistentDataSequence).detail("PDDS", self->persistentDataDurableSequence).detail("Oldest", map1.empty() ? 0 : map1.begin()->key ).detail("OldestMsgCount", map1.empty() ? 0 : map1.begin()->value.size());

	auto it = std::lower_bound(deque.begin(), deque.end(), std::make_pair(req.begin, LengthPrefixedStringRef()), CompareFirst<std::pair<Version, LengthPrefixedStringRef>>());

	Version currentVersion = -1;
	for(; it != deque.end(); ++it) {
		if(it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = currentVersion + 1;
				//TraceEvent("TLogPeekMessagesReached2", self->dbgid);
				break;
			}

			currentVersion = it->first;
			messages << VERSION_HEADER << currentVersion;
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
	state int sequence = -1;
	state UID peekId;

	if(req.sequence.present()) {
		try {
			peekId = req.sequence.get().first;
			sequence = req.sequence.get().second;
			if (sequence >= SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS && self->peekTracker.find(peekId) == self->peekTracker.end()) {
				throw operation_obsolete();
			}
			auto& trackerData = self->peekTracker[peekId];
			if (sequence == 0 && trackerData.sequence_version.find(0) == trackerData.sequence_version.end()) {
				trackerData.sequence_version[0].send(std::make_pair(req.begin, req.onlySpilled));
			}
			auto seqBegin = trackerData.sequence_version.begin();
			// The peek cursor and this comparison need to agree about the maximum number of in-flight requests.
			while(trackerData.sequence_version.size() && seqBegin->first <= sequence - SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS) {
				if(seqBegin->second.canBeSet()) {
					seqBegin->second.sendError(operation_obsolete());
				}
				trackerData.sequence_version.erase(seqBegin);
				seqBegin = trackerData.sequence_version.begin();
			}

			if(trackerData.sequence_version.size() && sequence < seqBegin->first) {
				throw operation_obsolete();
			}

			trackerData.lastUpdate = now();
			std::pair<Version, bool> prevPeekData = wait(trackerData.sequence_version[sequence].getFuture());
			req.begin = prevPeekData.first;
			req.onlySpilled = prevPeekData.second;
			wait(yield());
		} catch( Error &e ) {
			if(e.code() == error_code_timed_out || e.code() == error_code_operation_obsolete) {
				req.reply.sendError(e);
				return Void();
			} else {
				throw;
			}
		}
	}

	//TraceEvent("LogRouterPeek1", self->dbgid).detail("From", req.reply.getEndpoint().getPrimaryAddress()).detail("Ver", self->version.get()).detail("Begin", req.begin);
	if( req.returnIfBlocked && self->version.get() < req.begin ) {
		//TraceEvent("LogRouterPeek2", self->dbgid);
		req.reply.sendError(end_of_stream());
		if(req.sequence.present()) {
			auto& trackerData = self->peekTracker[peekId];
			auto& sequenceData = trackerData.sequence_version[sequence+1];
			if (!sequenceData.isSet()) {
				sequenceData.send(std::make_pair(req.begin, req.onlySpilled));
			}
		}
		return Void();
	}

	if( self->version.get() < req.begin ) {
		wait( self->version.whenAtLeast( req.begin ) );
		wait( delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()) );
	}

	Version poppedVer = poppedVersion(self, req.tag);

	if(poppedVer > req.begin || req.begin < self->startVersion) {
		//This should only happen if a packet is sent multiple times and the reply is not needed. 
		// Since we are using popped differently, do not send a reply.
		TraceEvent(SevWarnAlways, "LogRouterPeekPopped", self->dbgid).detail("Begin", req.begin).detail("Popped", poppedVer).detail("Start", self->startVersion);
		req.reply.send( Never() );
		if(req.sequence.present()) {
			auto& trackerData = self->peekTracker[peekId];
			auto& sequenceData = trackerData.sequence_version[sequence+1];
			if (!sequenceData.isSet()) {
				sequenceData.send(std::make_pair(req.begin, req.onlySpilled));
			}
		}
		return Void();
	}

	Version endVersion = self->version.get() + 1;
	peekMessagesFromMemory( self, req, messages, endVersion );

	TLogPeekReply reply;
	reply.maxKnownVersion = self->version.get();
	reply.minKnownCommittedVersion = self->poppedVersion;
	reply.messages = messages.toValue();
	reply.popped = self->minPopped.get() >= self->startVersion ? self->minPopped.get() : 0;
	reply.end = endVersion;
	reply.onlySpilled = false;

	if(req.sequence.present()) {
		auto& trackerData = self->peekTracker[peekId];
		trackerData.lastUpdate = now();
		auto& sequenceData = trackerData.sequence_version[sequence+1];
		if(trackerData.sequence_version.size() && sequence+1 < trackerData.sequence_version.begin()->first) {
			req.reply.sendError(operation_obsolete());
			if(!sequenceData.isSet())
				sequenceData.sendError(operation_obsolete());
			return Void();
		}
		if(sequenceData.isSet()) {
			if(sequenceData.getFuture().get().first != reply.end) {
				TEST(true); //tlog peek second attempt ended at a different version
				req.reply.sendError(operation_obsolete());
				return Void();
			}
		} else {
			sequenceData.send(std::make_pair(reply.end, reply.onlySpilled));
		}
		reply.begin = req.begin;
	}

	req.reply.send( reply );
	//TraceEvent("LogRouterPeek4", self->dbgid);
	return Void();
}

ACTOR Future<Void> cleanupPeekTrackers( LogRouterData* self ) {
	loop {
		double minTimeUntilExpiration = SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME;
		auto it = self->peekTracker.begin();
		while(it != self->peekTracker.end()) {
			double timeUntilExpiration = it->second.lastUpdate + SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME - now();
			if(timeUntilExpiration < 1.0e-6) {
				for(auto seq : it->second.sequence_version) {
					if(!seq.second.isSet()) {
						seq.second.sendError(timed_out());
					}
				}
				it = self->peekTracker.erase(it);
			} else {
				minTimeUntilExpiration = std::min(minTimeUntilExpiration, timeUntilExpiration);
				++it;
			}
		}

		wait( delay(minTimeUntilExpiration) );
	}
}

ACTOR Future<Void> logRouterPop( LogRouterData* self, TLogPopRequest req ) {
	auto tagData = self->getTagData(req.tag);
	if (!tagData) {
		tagData = self->createTagData(req.tag, req.to, req.durableKnownCommittedVersion);
	} else if (req.to > tagData->popped) {
		tagData->popped = req.to;
		tagData->durableKnownCommittedVersion = req.durableKnownCommittedVersion;
		wait(tagData->eraseMessagesBefore( req.to, self, TaskPriority::TLogPop ));
	}

	state Version minPopped = std::numeric_limits<Version>::max();
	state Version minKnownCommittedVersion = std::numeric_limits<Version>::max();
	for( auto it : self->tag_data ) {
		if(it) {
			minPopped = std::min( it->popped, minPopped );
			minKnownCommittedVersion = std::min( it->durableKnownCommittedVersion, minKnownCommittedVersion );
		}
	}

	while(!self->messageBlocks.empty() && self->messageBlocks.front().first < minPopped) {
		self->messageBlocks.pop_front();
		wait(yield(TaskPriority::TLogPop));
	}

	self->poppedVersion = std::min(minKnownCommittedVersion, self->minKnownCommittedVersion);
	if(self->logSystem->get() && self->allowPops) {
		const Tag popTag = self->logSystem->get()->getPseudoPopTag(self->routerTag, ProcessClass::LogRouterClass);
		self->logSystem->get()->pop(self->poppedVersion, popTag);
	}
	req.reply.send(Void());
	self->minPopped.set(std::max(minPopped, self->minPopped.get()));
	return Void();
}

ACTOR Future<Void> logRouterCore(
	TLogInterface interf,
	InitializeLogRouterRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	state LogRouterData logRouterData(interf.id(), req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );
	state Future<Void> dbInfoChange = Void();

	addActor.send( pullAsyncData(&logRouterData) );
	addActor.send( cleanupPeekTrackers(&logRouterData) );
	addActor.send( traceRole(Role::LOG_ROUTER, interf.id()) );

	loop choose {
		when( wait( dbInfoChange ) ) {
			dbInfoChange = db->onChange();
			logRouterData.allowPops = db->get().recoveryState == RecoveryState::FULLY_RECOVERED && db->get().recoveryCount >= req.recoveryCount;
			logRouterData.logSystem->set(ILogSystem::fromServerDBInfo( logRouterData.dbgid, db->get(), true ));
		}
		when( TLogPeekRequest req = waitNext( interf.peekMessages.getFuture() ) ) {
			addActor.send( logRouterPeekMessages( &logRouterData, req ) );
		}
		when( TLogPopRequest req = waitNext( interf.popMessages.getFuture() ) ) {
			addActor.send( logRouterPop( &logRouterData, req ) );
		}
		when (wait(error)) {}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount, TLogInterface myInterface) {
	loop {
		bool isDisplaced =
		    ((db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED) ||
		     (db->get().recoveryCount == recoveryCount && db->get().recoveryState == RecoveryState::FULLY_RECOVERED));
		isDisplaced = isDisplaced && !db->get().logSystemConfig.hasLogRouter(myInterface.id());
		if (isDisplaced) {
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> logRouter(
	TLogInterface interf,
	InitializeLogRouterRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	try {
		TraceEvent("LogRouterStart", interf.id()).detail("Start", req.startVersion).detail("Tag", req.routerTag.toString()).detail("Localities", req.tLogLocalities.size()).detail("Locality", req.locality);
		state Future<Void> core = logRouterCore(interf, req, db);
		loop choose{
			when(wait(core)) { return Void(); }
			when(wait(checkRemoved(db, req.recoveryCount, interf))) {}
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
