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
	request.endVersion = -1; // we *ALWAYS* try to extract *ALL* data
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

} // namespace ptxn