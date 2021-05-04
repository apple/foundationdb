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

#include "fdbserver/ptxn/TLogPeekCursor.h"

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
		TeamID teamID;
		ptxn::TLogStorageServerMessageSerializer serializer(teamID);
		serializer.completeMessageWriting();
		empty = serializer.getSerialized();
	}
	return empty;
};

struct PeekRemoteContext {
	const Optional<UID> debugID;
	const TeamID teamID;

	// The last version being processed, the peek will request lastVersion + 1
	Version* pLastVersion;

	// THe interface to the remote TLog server
	TLogInterfaceBase* pTLogInterface;

	// Deserializer
	TLogStorageServerMessageDeserializer* pDeserializer;

	// Deserializer iterator
	TLogStorageServerMessageDeserializer::iterator* pDeserializerIterator;

	// If not null, attach the arena in the reply to this arena, so the mutations will still be
	// accessible even the deserializer gets destructed.
	Arena* pAttachArena;

	PeekRemoteContext(const Optional<UID>& debugID_,
	                  const TeamID& teamID_,
	                  Version* pLastVersion_,
	                  TLogInterfaceBase* pInterface_,
	                  TLogStorageServerMessageDeserializer* pDeserializer_,
	                  TLogStorageServerMessageDeserializer::iterator* pDeserializerIterator_,
	                  Arena* pAttachArena_ = nullptr)
	  : debugID(debugID_), teamID(teamID_), pLastVersion(pLastVersion_), pTLogInterface(pInterface_),
	    pDeserializer(pDeserializer_), pDeserializerIterator(pDeserializerIterator_), pAttachArena(pAttachArena_) {

		ASSERT(pTLogInterface != nullptr && pDeserializer != nullptr && pDeserializerIterator != nullptr);
	}
};

ACTOR Future<bool> peekRemote(PeekRemoteContext peekRemoteContext) {
	state TLogPeekRequest request;

	request.debugID = peekRemoteContext.debugID;
	request.beginVersion = *peekRemoteContext.pLastVersion + 1;
	request.endVersion = -1; // we *ALWAYS* try to extract *ALL* data
	request.teamID = peekRemoteContext.teamID;

	try {
		state TLogPeekReply reply = wait(peekRemoteContext.pTLogInterface->peek.getReply(request));

		peekRemoteContext.pDeserializer->reset(reply.arena, reply.data);
		*peekRemoteContext.pLastVersion = peekRemoteContext.pDeserializer->getLastVersion();
		*peekRemoteContext.pDeserializerIterator = peekRemoteContext.pDeserializer->begin();

		if (*peekRemoteContext.pDeserializerIterator == peekRemoteContext.pDeserializer->end()) {
			// No new mutations incoming, and there is no new mutations responded from TLog in this request
			return false;
		}

		if (peekRemoteContext.pAttachArena != nullptr) {
			peekRemoteContext.pAttachArena->dependsOn(reply.arena);
		}

		return true;
	} catch (...) {
		// FIXME deal with possible errors
		return false;
	}
}

} // anonymous namespace

PeekCursorBase::PeekCursorBase(const Version& version_) : beginVersion(version_), lastVersion(version_ - 1) {}

const Version& PeekCursorBase::getBeginVersion() const {
	return beginVersion;
}

const Version& PeekCursorBase::getLastVersion() const {
	return lastVersion;
}

Future<bool> PeekCursorBase::remoteMoreAvailable() {
	return remoteMoreAvailableImpl();
}

const VersionSubsequenceMutation& PeekCursorBase::get() const {
	return getImpl();
}

void PeekCursorBase::next() {
	nextImpl();
}

bool PeekCursorBase::hasRemaining() const {
	return hasRemainingImpl();
}

ServerTeamPeekCursor::ServerTeamPeekCursor(const Version& beginVersion_,
                                           const TeamID& teamID_,
                                           TLogInterfaceBase* pTLogInterface_,
                                           Arena* pArena)
  : PeekCursorBase(beginVersion_), teamID(teamID_), pTLogInterface(pTLogInterface_), pAttachArena(pArena),
    deserializer(emptyCursorHeader()), deserializerIter(deserializer.begin()) {

	ASSERT(pTLogInterface);
}

const TeamID& ServerTeamPeekCursor::getTeamID() const {
	return teamID;
}

Future<bool> ServerTeamPeekCursor::remoteMoreAvailableImpl() {
	// FIXME Put debugID if necessary
	PeekRemoteContext context(
	    Optional<UID>(), getTeamID(), &lastVersion, pTLogInterface, &deserializer, &deserializerIter, pAttachArena);

	return peekRemote(context);
}

void ServerTeamPeekCursor::nextImpl() {
	++deserializerIter;
}

const VersionSubsequenceMutation& ServerTeamPeekCursor::getImpl() const {
	return *deserializerIter;
}

bool ServerTeamPeekCursor::hasRemainingImpl() const {
	return deserializerIter != deserializer.end();
}

} // namespace ptxn