/*
 * TLogPeekCursor.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ptxn/TLogPeekCursor.actor.h"

#include <iterator>
#include <unordered_set>
#include <vector>

#include "fdbserver/Knobs.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "flow/Error.h"

#include "flow/actorcompiler.h" // This must be the last #include

namespace ptxn {

namespace {

// The deserializer will always expect the serialized data has a header. This function provides header-only serialized
// data for the consumption of the deserializer.
const Standalone<StringRef>& emptyCursorHeader() {
	static Standalone<StringRef> empty;
	if (empty.size() == 0) {
		StorageTeamID storageTeamID;
		ptxn::SubsequencedMessageSerializer serializer(storageTeamID);
		serializer.completeMessageWriting();
		empty = serializer.getSerialized();
	}
	return empty;
};

struct PeekRemoteContext {
	const Optional<UID> debugID;
	const StorageTeamID storageTeamID;

	// The last version being processed, the peek will request lastVersion + 1
	Version* pLastVersion;

	// The interface to the remote TLog server
	const std::vector<TLogInterfaceBase*>& pTLogInterfaces;

	// Deserializer
	SubsequencedMessageDeserializer* pDeserializer;

	// Deserializer iterator
	SubsequencedMessageDeserializer::iterator* pDeserializerIterator;

	// If not null, attach the arena in the reply to this arena, so the mutations will still be
	// accessible even the deserializer gets destructed.
	Arena* pAttachArena;

	Arena* pWorkArena;

	PeekRemoteContext(const Optional<UID>& debugID_,
	                  const StorageTeamID& storageTeamID_,
	                  Version* pLastVersion_,
	                  const std::vector<TLogInterfaceBase*>& pInterfaces_,
	                  SubsequencedMessageDeserializer* pDeserializer_,
	                  SubsequencedMessageDeserializer::iterator* pDeserializerIterator_,
					  Arena* pWorkArena_,
	                  Arena* pAttachArena_ = nullptr)
	  : debugID(debugID_), storageTeamID(storageTeamID_), pLastVersion(pLastVersion_), pTLogInterfaces(pInterfaces_),
	    pDeserializer(pDeserializer_), pDeserializerIterator(pDeserializerIterator_), pWorkArena(pWorkArena_), pAttachArena(pAttachArena_) {

		for (const auto pTLogInterface : pTLogInterfaces) {
			ASSERT(pTLogInterface != nullptr);
		}

		ASSERT(pDeserializer != nullptr && pDeserializerIterator != nullptr);
	}
};

ACTOR Future<bool> peekRemote(PeekRemoteContext peekRemoteContext) {
	state TLogPeekRequest request;
	// FIXME: use loadBalancer rather than picking up a random one
	state TLogInterfaceBase* pTLogInterface =
	    peekRemoteContext
	        .pTLogInterfaces[deterministicRandom()->randomInt(0, peekRemoteContext.pTLogInterfaces.size())];

	request.debugID = peekRemoteContext.debugID;
	request.beginVersion = *peekRemoteContext.pLastVersion + 1;
	request.endVersion = invalidVersion; // we *ALWAYS* try to extract *ALL* data
	request.storageTeamID = peekRemoteContext.storageTeamID;

	try {
		state TLogPeekReply reply = wait(pTLogInterface->peek.getReply(request));

		peekRemoteContext.pDeserializer->reset(reply.data);
		*peekRemoteContext.pLastVersion = peekRemoteContext.pDeserializer->getLastVersion();
		*peekRemoteContext.pDeserializerIterator = peekRemoteContext.pDeserializer->begin();

		if (*peekRemoteContext.pDeserializerIterator == peekRemoteContext.pDeserializer->end()) {
			// No new mutations incoming, and there is no new mutations responded from TLog in this request
			return false;
		}

		if (peekRemoteContext.pAttachArena != nullptr) {
			peekRemoteContext.pAttachArena->dependsOn(reply.arena);
		}

		(*peekRemoteContext.pWorkArena) = reply.arena;

		return true;
	} catch (Error& error) {
		// FIXME deal with possible errors
		return false;
	}
}

} // anonymous namespace

PeekCursorBase::iterator::iterator(PeekCursorBase* pCursor_, bool isEndIterator_)
  : pCursor(pCursor_), isEndIterator(isEndIterator_) {}

bool PeekCursorBase::iterator::operator==(const PeekCursorBase::iterator& another) const {
	// Since the iterator is not duplicable, no two iterators equal. This is a a hack to help determining if the
	// iterator is reaching the end of the data. See the comments of the constructor.
	return (!pCursor->hasRemaining() && another.isEndIterator && pCursor == another.pCursor);
}

bool PeekCursorBase::iterator::operator!=(const PeekCursorBase::iterator& another) const {
	return !this->operator==(another);
}

PeekCursorBase::iterator::reference PeekCursorBase::iterator::operator*() const {
	return pCursor->get();
}

PeekCursorBase::iterator::pointer PeekCursorBase::iterator::operator->() const {
	return &pCursor->get();
}

void PeekCursorBase::iterator::operator++() {
	pCursor->next();
}

PeekCursorBase::PeekCursorBase() : endIterator(this, true) {}

Future<bool> PeekCursorBase::remoteMoreAvailable() {
	return remoteMoreAvailableImpl();
}

const VersionSubsequenceMessage& PeekCursorBase::get() const {
	return getImpl();
}

void PeekCursorBase::next() {
	nextImpl();
}

bool PeekCursorBase::hasRemaining() const {
	return hasRemainingImpl();
}

StorageTeamPeekCursor::StorageTeamPeekCursor(const Version& beginVersion_,
                                             const StorageTeamID& storageTeamID_,
                                             TLogInterfaceBase* pTLogInterface_,
                                             Arena* pArena_)
  : StorageTeamPeekCursor(beginVersion_, storageTeamID_, std::vector<TLogInterfaceBase*>{ pTLogInterface_ }, pArena_) {}

StorageTeamPeekCursor::StorageTeamPeekCursor(const Version& beginVersion_,
                                             const StorageTeamID& storageTeamID_,
                                             const std::vector<TLogInterfaceBase*>& pTLogInterfaces_,
                                             Arena* pArena_)
  : storageTeamID(storageTeamID_), pTLogInterfaces(pTLogInterfaces_), pAttachArena(pArena_),
    deserializer(emptyCursorHeader()), deserializerIter(deserializer.begin()), beginVersion(beginVersion_),
    lastVersion(beginVersion_ - 1) {

	for (const auto pTLogInterface : pTLogInterfaces) {
		ASSERT(pTLogInterface != nullptr);
	}
}

const StorageTeamID& StorageTeamPeekCursor::getStorageTeamID() const {
	return storageTeamID;
}

const Version& StorageTeamPeekCursor::getLastVersion() const {
	return lastVersion;
}

const Version& StorageTeamPeekCursor::getBeginVersion() const {
	return beginVersion;
}

Future<bool> StorageTeamPeekCursor::remoteMoreAvailableImpl() {
	// FIXME Put debugID if necessary
	PeekRemoteContext context(Optional<UID>(),
	                          getStorageTeamID(),
	                          &lastVersion,
	                          pTLogInterfaces,
	                          &deserializer,
	                          &deserializerIter,
							  &workArena,
	                          pAttachArena);

	return peekRemote(context);
}

void StorageTeamPeekCursor::nextImpl() {
	++deserializerIter;
}

const VersionSubsequenceMessage& StorageTeamPeekCursor::getImpl() const {
	return *deserializerIter;
}

bool StorageTeamPeekCursor::hasRemainingImpl() const {
	return deserializerIter != deserializer.end();
}

ServerPeekCursor::ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> const& interf,
                                   Tag tag,
                                   StorageTeamID storageTeamId,
                                   TLogGroupID tLogGroupID,
                                   Version begin,
                                   Version end,
                                   bool returnIfBlocked,
                                   bool parallelGetMore)
  : interf(interf), tag(tag), storageTeamId(storageTeamId), tLogGroupID(tLogGroupID), messageVersion(begin), end(end),
    hasMsg(false), rd(results.arena, results.data, Unversioned()), randomID(deterministicRandom()->randomUniqueID()),
    poppedVersion(0), returnIfBlocked(returnIfBlocked), sequence(0), onlySpilled(false),
    parallelGetMore(parallelGetMore), lastReset(0), slowReplies(0), fastReplies(0), unknownReplies(0),
    resetCheck(Void()) {
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;
	//TraceEvent("SPC_Starting", randomID).detail("Tag", tag.toString()).detail("Begin", begin).detail("End", end).backtrace();
}

ServerPeekCursor::ServerPeekCursor(TLogPeekReply const& results,
                                   LogMessageVersion const& messageVersion,
                                   LogMessageVersion const& end,
                                   TagsAndMessage const& message,
                                   bool hasMsg,
                                   Version poppedVersion,
                                   Tag tag,
                                   StorageTeamID storageTeamId,
                                   TLogGroupID tLogGroupID)
  : results(results), tag(tag), storageTeamId(storageTeamId), tLogGroupID(tLogGroupID),
    rd(results.arena, results.data, Unversioned()), messageVersion(messageVersion), end(end), messageAndTags(message),
    hasMsg(hasMsg), randomID(deterministicRandom()->randomUniqueID()), poppedVersion(poppedVersion),
    returnIfBlocked(false), sequence(0), onlySpilled(false), parallelGetMore(false), lastReset(0), slowReplies(0),
    fastReplies(0), unknownReplies(0), resetCheck(Void()) {
	//TraceEvent("SPC_Clone", randomID);
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;
	if (hasMsg)
		nextMessage();

	advanceTo(messageVersion);
}

Reference<ILogSystem::IPeekCursor> ServerPeekCursor::cloneNoMore() {
	return makeReference<ServerPeekCursor>(
	    results, messageVersion, end, messageAndTags, hasMsg, poppedVersion, tag, storageTeamId, tLogGroupID);
}

void ServerPeekCursor::setProtocolVersion(ProtocolVersion version) {
	rd.setProtocolVersion(version);
}

Arena& ServerPeekCursor::arena() {
	return results.arena;
}

ArenaReader* ServerPeekCursor::reader() {
	return &rd;
}

bool ServerPeekCursor::hasMessage() const {
	//TraceEvent("SPC_HasMessage", randomID).detail("HasMsg", hasMsg);
	return hasMsg;
}

void ServerPeekCursor::nextMessage() {
	//TraceEvent("SPC_NextMessage", randomID).detail("MessageVersion", messageVersion.toString());
	ASSERT(hasMsg);
	if (rd.empty()) {
		messageVersion.reset(std::min(results.endVersion, end.version));
		hasMsg = false;
		return;
	}
	if (*(int32_t*)rd.peekBytes(4) == VERSION_HEADER) {
		// A version
		int32_t dummy;
		Version ver;
		rd >> dummy >> ver;

		//TraceEvent("SPC_ProcessSeq", randomID).detail("MessageVersion", messageVersion.toString()).detail("Ver", ver).detail("Tag", tag.toString());
		// ASSERT( ver >= messageVersion.version );

		messageVersion.reset(ver);

		if (messageVersion >= end) {
			messageVersion = end;
			hasMsg = false;
			return;
		}
		ASSERT(!rd.empty());
	}

	messageAndTags.loadFromArena(&rd, &messageVersion.sub);
	DEBUG_TAGS_AND_MESSAGE("ServerPeekCursor", messageVersion.version, messageAndTags.getRawMessage())
	    .detail("CursorID", this->randomID);
	// Rewind and consume the header so that reader() starts from the message.
	rd.rewind();
	rd.readBytes(messageAndTags.getHeaderSize());
	hasMsg = true;
	//TraceEvent("SPC_NextMessageB", randomID).detail("MessageVersion", messageVersion.toString());
}

StringRef ServerPeekCursor::getMessage() {
	//TraceEvent("SPC_GetMessage", randomID);
	StringRef message = messageAndTags.getMessageWithoutTags();
	rd.readBytes(message.size()); // Consumes the message.
	return message;
}

StringRef ServerPeekCursor::getMessageWithTags() {
	StringRef rawMessage = messageAndTags.getRawMessage();
	rd.readBytes(rawMessage.size() - messageAndTags.getHeaderSize()); // Consumes the message.
	return rawMessage;
}

VectorRef<Tag> ServerPeekCursor::getTags() const {
	return messageAndTags.tags;
}

void ServerPeekCursor::advanceTo(LogMessageVersion n) {
	//TraceEvent("SPC_AdvanceTo", randomID).detail("N", n.toString());
	while (messageVersion < n && hasMessage()) {
		getMessage();
		nextMessage();
	}

	if (hasMessage())
		return;

	// if( more.isValid() && !more.isReady() ) more.cancel();

	if (messageVersion < n) {
		messageVersion = n;
	}
}

ACTOR Future<Void> resetChecker(ServerPeekCursor* self, NetworkAddress addr) {
	self->slowReplies = 0;
	self->unknownReplies = 0;
	self->fastReplies = 0;
	wait(delay(SERVER_KNOBS->PEEK_STATS_INTERVAL));
	TraceEvent("SlowPeekStats", self->randomID)
	    .detail("PeerAddress", addr)
	    .detail("SlowReplies", self->slowReplies)
	    .detail("FastReplies", self->fastReplies)
	    .detail("UnknownReplies", self->unknownReplies);

	if (self->slowReplies >= SERVER_KNOBS->PEEK_STATS_SLOW_AMOUNT &&
	    self->slowReplies / double(self->slowReplies + self->fastReplies) >= SERVER_KNOBS->PEEK_STATS_SLOW_RATIO) {

		TraceEvent("ConnectionResetSlowPeek", self->randomID)
		    .detail("PeerAddress", addr)
		    .detail("SlowReplies", self->slowReplies)
		    .detail("FastReplies", self->fastReplies)
		    .detail("UnknownReplies", self->unknownReplies);
		FlowTransport::transport().resetConnection(addr);
		self->lastReset = now();
	}
	return Void();
}

ACTOR Future<TLogPeekReply> recordRequestMetrics(ServerPeekCursor* self,
                                                 NetworkAddress addr,
                                                 Future<TLogPeekReply> in) {
	try {
		state double startTime = now();
		TLogPeekReply t = wait(in);
		if (now() - self->lastReset > SERVER_KNOBS->PEEK_RESET_INTERVAL) {
			if (now() - startTime > SERVER_KNOBS->PEEK_MAX_LATENCY) {
				if (t.data.size() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES || SERVER_KNOBS->PEEK_COUNT_SMALL_MESSAGES) {
					if (self->resetCheck.isReady()) {
						self->resetCheck = resetChecker(self, addr);
					}
					self->slowReplies++;
				} else {
					self->unknownReplies++;
				}
			} else {
				self->fastReplies++;
			}
		}
		return t;
	} catch (Error& e) {
		if (e.code() != error_code_broken_promise)
			throw;
		wait(Never()); // never return
		throw internal_error(); // does not happen
	}
}

ACTOR Future<Void> serverPeekParallelGetMore(ServerPeekCursor* self, TaskPriority taskID) {
	if (!self->interf || self->messageVersion >= self->end) {
		if (self->hasMessage())
			return Void();
		wait(Future<Void>(Never()));
		throw internal_error();
	}

	if (!self->interfaceChanged.isValid()) {
		self->interfaceChanged = self->interf->onChange();
	}

	loop {
		state Version expectedBegin = self->messageVersion.version;
		try {
			if (self->parallelGetMore || self->onlySpilled) {
				while (self->futureResults.size() < SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS &&
				       self->interf->get().present()) {
					self->futureResults.push_back(recordRequestMetrics(
					    self,
					    self->interf->get().interf().peekMessages.getEndpoint().getPrimaryAddress(),
					    self->interf->get().interf().peekMessages.getReply(TLogPeekRequest(self->randomID,
					                                                                       self->messageVersion.version,
					                                                                       Optional<Version>(),
					                                                                       self->returnIfBlocked,
					                                                                       self->onlySpilled,
					                                                                       self->storageTeamId,
					                                                                       self->tLogGroupID),
					                                                       taskID)));
				}
				if (self->sequence == std::numeric_limits<decltype(self->sequence)>::max()) {
					throw operation_obsolete();
				}
			} else if (self->futureResults.size() == 0) {
				return Void();
			}

			if (self->hasMessage())
				return Void();

			choose {
				when(TLogPeekReply res = wait(self->interf->get().present() ? self->futureResults.front() : Never())) {
					if (res.beginVersion.get() != expectedBegin) {
						throw operation_obsolete();
					}
					expectedBegin = res.endVersion;
					self->futureResults.pop_front();
					self->results = res;
					self->onlySpilled = res.onlySpilled;
					if (res.popped.present())
						self->poppedVersion =
						    std::min(std::max(self->poppedVersion, res.popped.get()), self->end.version);
					self->rd = ArenaReader(self->results.arena, self->results.data, Unversioned());
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					//TraceEvent("SPC_GetMoreB", self->randomID).detail("Has", self->hasMessage()).detail("End", res.end).detail("Popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when(wait(self->interfaceChanged)) {
					self->interfaceChanged = self->interf->onChange();
					self->randomID = deterministicRandom()->randomUniqueID();
					self->sequence = 0;
					self->onlySpilled = false;
					self->futureResults.clear();
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				self->end.reset(self->messageVersion.version);
				return Void();
			} else if (e.code() == error_code_timed_out || e.code() == error_code_operation_obsolete) {
				TraceEvent("PeekCursorTimedOut", self->randomID).error(e);
				// We *should* never get timed_out(), as it means the TLog got stuck while handling a parallel peek,
				// and thus we've likely just wasted 10min.
				// timed_out() is sent by cleanupPeekTrackers as value PEEK_TRACKER_EXPIRATION_TIME
				ASSERT_WE_THINK(e.code() == error_code_operation_obsolete ||
				                SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME < 10);
				self->interfaceChanged = self->interf->onChange();
				self->randomID = deterministicRandom()->randomUniqueID();
				self->sequence = 0;
				self->futureResults.clear();
			} else {
				throw e;
			}
		}
	}
}

ACTOR Future<Void> serverPeekGetMore(ServerPeekCursor* self, TaskPriority taskID) {
	if (!self->interf || self->messageVersion >= self->end) {
		wait(Future<Void>(Never()));
		throw internal_error();
	}
	try {
		loop {
			choose {
				when(TLogPeekReply res =
				         wait(self->interf->get().present()
				                  ? brokenPromiseToNever(self->interf->get().interf().peekMessages.getReply(
				                        TLogPeekRequest(self->randomID,
				                                        self->messageVersion.version,
				                                        Optional<Version>(),
				                                        self->returnIfBlocked,
				                                        self->onlySpilled,
				                                        self->storageTeamId,
				                                        self->tLogGroupID),
				                        taskID))
				                  : Never())) {
					self->results = res;
					self->onlySpilled = res.onlySpilled;
					if (res.popped.present())
						self->poppedVersion =
						    std::min(std::max(self->poppedVersion, res.popped.get()), self->end.version);
					self->rd = ArenaReader(self->results.arena, self->results.data, Unversioned());
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					//TraceEvent("SPC_GetMoreB", self->randomID).detail("Has", self->hasMessage()).detail("End", res.end).detail("Popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when(wait(self->interf->onChange())) { self->onlySpilled = false; }
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			self->end.reset(self->messageVersion.version);
			return Void();
		}
		throw e;
	}
}

Future<Void> ServerPeekCursor::getMore(TaskPriority taskID) {
	//TraceEvent("SPC_GetMore", randomID).detail("HasMessage", hasMessage()).detail("More", !more.isValid() || more.isReady()).detail("MessageVersion", messageVersion.toString()).detail("End", end.toString());
	if (hasMessage() && !parallelGetMore)
		return Void();
	if (!more.isValid() || more.isReady()) {
		if (parallelGetMore || onlySpilled || futureResults.size()) {
			more = serverPeekParallelGetMore(this, taskID);
		} else {
			more = serverPeekGetMore(this, taskID);
		}
	}
	return more;
}

ACTOR Future<Void> serverPeekOnFailed(ServerPeekCursor* self) {
	loop {
		choose {
			when(wait(self->interf->get().present()
			              ? IFailureMonitor::failureMonitor().onStateEqual(
			                    self->interf->get().interf().peekMessages.getEndpoint(), FailureStatus())
			              : Never())) {
				return Void();
			}
			when(wait(self->interf->onChange())) {}
		}
	}
}

Future<Void> ServerPeekCursor::onFailed() {
	return serverPeekOnFailed(this);
}

bool ServerPeekCursor::isActive() const {
	if (!interf->get().present())
		return false;
	if (messageVersion >= end)
		return false;
	return IFailureMonitor::failureMonitor().getState(interf->get().interf().peekMessages.getEndpoint()).isAvailable();
}

bool ServerPeekCursor::isExhausted() const {
	return messageVersion >= end;
}

const LogMessageVersion& ServerPeekCursor::version() const {
	return messageVersion;
} // Call only after nextMessage().  The sequence of the current message, or results.end if nextMessage() has returned
// false.

Version ServerPeekCursor::getMinKnownCommittedVersion() const {
	return results.minKnownCommittedVersion;
}

Optional<UID> ServerPeekCursor::getPrimaryPeekLocation() const {
	if (interf && interf->get().present()) {
		return interf->get().id();
	}
	return Optional<UID>();
}

Optional<UID> ServerPeekCursor::getCurrentPeekLocation() const {
	return ServerPeekCursor::getPrimaryPeekLocation();
}

Version ServerPeekCursor::popped() const {
	return poppedVersion;
}

} // namespace ptxn