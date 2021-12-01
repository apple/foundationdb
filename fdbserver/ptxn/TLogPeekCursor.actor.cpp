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

#include <algorithm>
#include <iterator>
#include <unordered_set>
#include <vector>

#include "fdbserver/Knobs.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/test/Delay.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "flow/Error.h"
#include "flow/Trace.h"

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

} // anonymous namespace

namespace details::StorageTeamPeekCursor {

struct PeekRemoteContext {
	const Optional<UID> debugID;
	const StorageTeamID storageTeamID;

	// The last version being processed, the peek will request lastVersion + 1
	Version* pLastVersion;

	// The interface to the remote TLog server
	const std::vector<TLogInterfaceBase*>& pTLogInterfaces;

	// Deserializer
	SubsequencedMessageDeserializer& deserializer;

	// Deserializer iterator
	ptxn::details::ArenaWrapper<SubsequencedMessageDeserializer::iterator>& wrappedDeserializerIter;

	// The pointer to the work arena. The work arena stores the data from TLogPeekReply
	Arena* pWorkArena;

	// If not null, attach the arena in the reply to this arena, so the mutations will still be
	// accessible even the deserializer gets destructed.
	Arena* pAttachArena;

	PeekRemoteContext(const Optional<UID>& debugID_,
	                  const StorageTeamID& storageTeamID_,
	                  Version* pLastVersion_,
	                  const std::vector<TLogInterfaceBase*>& pInterfaces_,
	                  SubsequencedMessageDeserializer& deserializer_,
	                  details::ArenaWrapper<SubsequencedMessageDeserializer::iterator>& wrappedDeserializerIterator_,
	                  Arena* pWorkArena_,
	                  Arena* pAttachArena_ = nullptr)
	  : debugID(debugID_), storageTeamID(storageTeamID_), pLastVersion(pLastVersion_), pTLogInterfaces(pInterfaces_),
	    deserializer(deserializer_), wrappedDeserializerIter(wrappedDeserializerIterator_), pWorkArena(pWorkArena_),
	    pAttachArena(pAttachArena_) {

		for (const auto pTLogInterface : pTLogInterfaces) {
			ASSERT(pTLogInterface != nullptr);
		}
		ASSERT(pWorkArena_);
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

	state TLogPeekReply reply = wait(pTLogInterface->peek.getReply(request));

	// In case the remote epoch ended, an end_of_stream exception will be thrown and it is the caller's responsible
	// to catch.

	peekRemoteContext.deserializer.reset(reply.data);
	peekRemoteContext.wrappedDeserializerIter = peekRemoteContext.deserializer.begin();
	if (peekRemoteContext.wrappedDeserializerIter.get() == peekRemoteContext.deserializer.end()) {
		// No new mutations incoming, and there is no new mutations responded from TLog in this request
		return false;
	}

	*peekRemoteContext.pLastVersion = reply.endVersion;
	*peekRemoteContext.pWorkArena = reply.arena;

	return true;
}

} // namespace details::StorageTeamPeekCursor

#pragma region PeekCursorBase

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

#pragma endregion PeekCursorBase

namespace details {

#pragma region VersionSubsequencePeekCursorBase

VersionSubsequencePeekCursorBase::VersionSubsequencePeekCursorBase(const Version version_,
                                                                   const Subsequence subsequence_)
  : PeekCursorBase() {}

const Version& VersionSubsequencePeekCursorBase::getVersion() const {
	return get().version;
}

const Subsequence& VersionSubsequencePeekCursorBase::getSubsequence() const {
	return get().subsequence;
}

int VersionSubsequencePeekCursorBase::operatorSpaceship(const VersionSubsequencePeekCursorBase& other) const {
	const Version& thisVersion = getVersion();
	const Version& otherVersion = other.getVersion();
	if (thisVersion < otherVersion) {
		return -1;
	} else if (thisVersion > otherVersion) {
		return 1;
	}

	const Subsequence& thisSubsequence = getSubsequence();
	const Subsequence& otherSubsequence = other.getSubsequence();
	if (thisSubsequence < otherSubsequence) {
		return -1;
	} else if (thisSubsequence > otherSubsequence) {
		return 1;
	}

	return 0;
}

#pragma endregion VersionSubsequencePeekCursorBase

} // namespace details

#pragma region StorageTeamPeekCursor

StorageTeamPeekCursor::StorageTeamPeekCursor(const Version& startingVersion_,
                                             const StorageTeamID& storageTeamID_,
                                             TLogInterfaceBase* pTLogInterface_,
                                             Arena* pArena_,
                                             const bool reportEmptyVersion_)
  : StorageTeamPeekCursor(startingVersion_,
                          storageTeamID_,
                          std::vector<TLogInterfaceBase*>{ pTLogInterface_ },
                          pArena_,
                          reportEmptyVersion_) {}

StorageTeamPeekCursor::StorageTeamPeekCursor(const Version& startingVersion_,
                                             const StorageTeamID& storageTeamID_,
                                             const std::vector<TLogInterfaceBase*>& pTLogInterfaces_,
                                             Arena* pArena_,
                                             const bool reportEmptyVersion_)
  : VersionSubsequencePeekCursorBase(), storageTeamID(storageTeamID_), pTLogInterfaces(pTLogInterfaces_),
    pAttachArena(pArena_), deserializer(emptyCursorHeader(), /* reportEmptyVersion_ */ true),
    wrappedDeserializerIter(deserializer.begin(), pArena_), startingVersion(startingVersion_),
    reportEmptyVersion(reportEmptyVersion_), lastVersion(startingVersion_ - 1) {

	for (const auto pTLogInterface : pTLogInterfaces) {
		ASSERT(pTLogInterface != nullptr);
	}
}

const StorageTeamID& StorageTeamPeekCursor::getStorageTeamID() const {
	return storageTeamID;
}

const Version& StorageTeamPeekCursor::getStartingVersion() const {
	return startingVersion;
}

Future<bool> StorageTeamPeekCursor::remoteMoreAvailableImpl() {
	// FIXME Put debugID if necessary
	details::StorageTeamPeekCursor::PeekRemoteContext context(Optional<UID>(),
	                                                          getStorageTeamID(),
	                                                          &lastVersion,
	                                                          pTLogInterfaces,
	                                                          deserializer,
	                                                          wrappedDeserializerIter,
	                                                          &workArena,
	                                                          pAttachArena);

	return details::StorageTeamPeekCursor::peekRemote(context);
}

void StorageTeamPeekCursor::nextImpl() {
	++(wrappedDeserializerIter.get());
}

const VersionSubsequenceMessage& StorageTeamPeekCursor::getImpl() const {
	return *wrappedDeserializerIter.get();
}

bool StorageTeamPeekCursor::hasRemainingImpl() const {
	if (!reportEmptyVersion) {
		while (wrappedDeserializerIter.get() != deserializer.end() &&
		       wrappedDeserializerIter.get()->message.getType() == Message::Type::EMPTY_VERSION_MESSAGE) {
			++(wrappedDeserializerIter.get());
		}
	}
	return wrappedDeserializerIter.get() != deserializer.end();
}

#pragma endregion StorageTeamPeekCursor

namespace merged {

namespace details {

#pragma region OrderedCursorContainer

bool OrderedCursorContainer::heapElementComparator(OrderedCursorContainer::element_t e1,
                                                   OrderedCursorContainer::element_t e2) {
	return *e1 > *e2;
}

OrderedCursorContainer::OrderedCursorContainer() {
	std::make_heap(std::begin(container), std::end(container), heapElementComparator);
}

void OrderedCursorContainer::pushImpl(const OrderedCursorContainer::element_t& pCursor) {
	container.push_back(pCursor);
	std::push_heap(std::begin(container), std::end(container), heapElementComparator);
}

void OrderedCursorContainer::popImpl() {
	std::pop_heap(std::begin(container), std::end(container), heapElementComparator);
	container.pop_back();
}

#pragma endregion OrderedCursorContainer

#pragma region UnorderedCursorContainer

void UnorderedCursorContainer::pushImpl(const UnorderedCursorContainer::element_t& pCursor) {
	container.push_back(pCursor);
}

void UnorderedCursorContainer::popImpl() {
	container.pop_front();
}

#pragma endregion UnorderedCursorContainer

#pragma region StorageTeamIDCursorMapperMixin

void StorageTeamIDCursorMapperMixin::addCursor(std::shared_ptr<StorageTeamPeekCursor>&& cursor) {
	addCursorImpl(std::move(cursor));
}

void StorageTeamIDCursorMapperMixin::addCursorImpl(std::shared_ptr<StorageTeamPeekCursor>&& cursor) {
	const StorageTeamID& storageTeamID = cursor->getStorageTeamID();
	ASSERT(!isCursorExists(storageTeamID));
	mapper[storageTeamID] = std::shared_ptr<StorageTeamPeekCursor>(std::move(cursor));
}

std::shared_ptr<StorageTeamPeekCursor> StorageTeamIDCursorMapperMixin::removeCursor(
    const StorageTeamID& storageTeamID) {

	ASSERT(isCursorExists(storageTeamID));
	return removeCursorImpl(storageTeamID);
}

std::shared_ptr<StorageTeamPeekCursor> StorageTeamIDCursorMapperMixin::removeCursorImpl(
    const StorageTeamID& storageTeamID) {

	std::shared_ptr<StorageTeamPeekCursor> result = mapper[storageTeamID];
	mapper.erase(storageTeamID);
	return result;
}

bool StorageTeamIDCursorMapperMixin::isCursorExists(const StorageTeamID& storageTeamID) const {
	return mapper.find(storageTeamID) != mapper.end();
}

StorageTeamPeekCursor& StorageTeamIDCursorMapperMixin::getCursor(const StorageTeamID& storageTeamID) {
	ASSERT(isCursorExists(storageTeamID));
	return *mapper[storageTeamID];
}

const StorageTeamPeekCursor& StorageTeamIDCursorMapperMixin::getCursor(const StorageTeamID& storageTeamID) const {
	ASSERT(isCursorExists(storageTeamID));
	return *mapper.at(storageTeamID);
}

std::shared_ptr<StorageTeamPeekCursor> StorageTeamIDCursorMapperMixin::getCursorPtr(
    const StorageTeamID& storageTeamID) {

	ASSERT(isCursorExists(storageTeamID));
	return mapper.at(storageTeamID);
}

int StorageTeamIDCursorMapperMixin::getNumCursors() const {
	return mapper.size();
}

StorageTeamIDCursorMapperMixin::StorageTeamIDCursorMapper::iterator StorageTeamIDCursorMapperMixin::cursorsBegin() {
	return std::begin(mapper);
}

StorageTeamIDCursorMapperMixin::StorageTeamIDCursorMapper::iterator StorageTeamIDCursorMapperMixin::cursorsEnd() {
	return std::end(mapper);
}

#pragma endregion StorageTeamIDCursorMapperMixin

namespace BroadcastedStorageTeamPeekCursor {

struct PeekRemoteContext {
	using GetCursorPtrFunc_t = std::function<std::shared_ptr<StorageTeamPeekCursor>(const StorageTeamID&)>;

	// Cursors that are empty
	std::list<StorageTeamID>& emptyCursorStorageTeamIDs;

	// Cursors that meets end-of-stream when querying the TLog server
	std::list<StorageTeamID>& retiredCursorStorageTeamIDs;

	// Function that called to get the corresponding cursor by storage team id
	GetCursorPtrFunc_t getCursorPtr;

	PeekRemoteContext(std::list<StorageTeamID>& emptyCursorStorageTeamIDs_,
	                  std::list<StorageTeamID>& retiredCursorStorageTeamIDs_,
	                  GetCursorPtrFunc_t getCursorPtr_)
	  : emptyCursorStorageTeamIDs(emptyCursorStorageTeamIDs_),
	    retiredCursorStorageTeamIDs(retiredCursorStorageTeamIDs_), getCursorPtr(getCursorPtr_) {}
};

struct PeekSingleCursorResult {
	bool retrievedData = false;
	bool endOfStream = false;
};

ACTOR Future<PeekSingleCursorResult> peekSingleCursor(std::shared_ptr<StorageTeamPeekCursor> pCursor) {
	state int i = 0;
	state ptxn::test::ExponentalBackoffDelay exponentalBackoff(SERVER_KNOBS->MERGE_CURSOR_RETRY_DELAY);
	exponentalBackoff.enable();

	while (i < SERVER_KNOBS->MERGE_CURSOR_RETRY_TIMES) {
		try {
			state bool receivedData = wait(pCursor->remoteMoreAvailable());
		} catch (Error& error) {
			if (error.code() == error_code_end_of_stream) {
				return PeekSingleCursorResult{ false, true };
			}
			throw;
		}
		if (receivedData) {
			exponentalBackoff.resetBackoffs();
			return PeekSingleCursorResult{ true, false };
		}
		if (++i == SERVER_KNOBS->MERGE_CURSOR_RETRY_TIMES) {
			break;
		}
		wait(exponentalBackoff());
	}

	return PeekSingleCursorResult{ false, false };
}

// Returns the number of cursors reported that has received messages from TLogs
ACTOR Future<bool> peekRemote(std::shared_ptr<PeekRemoteContext> pPeekRemoteContext) {
	if (pPeekRemoteContext->emptyCursorStorageTeamIDs.empty()) {
		throw end_of_stream();
	}

	std::vector<Future<PeekSingleCursorResult>> cursorFutures;
	std::transform(
	    std::begin(pPeekRemoteContext->emptyCursorStorageTeamIDs),
	    std::end(pPeekRemoteContext->emptyCursorStorageTeamIDs),
	    std::back_inserter(cursorFutures),
	    /* NOTE ACTORs are compiled into classes */
	    [this](const auto& storageTeamID) -> auto {
		    return peekSingleCursor(pPeekRemoteContext->getCursorPtr(storageTeamID));
	    });
	state std::vector<PeekSingleCursorResult> cursorResults = wait(getAll(cursorFutures));

	auto cursorResultsIter = std::begin(cursorResults);
	auto emptyCursorStorageTeamIDsIter = std::begin(pPeekRemoteContext->emptyCursorStorageTeamIDs);
	// For any cursors need to be refilled, if the final state is either filled or end_of_stream, the cursors will
	// be ready; otherwise, not ready.
	bool cursorsReady = true;
	while (cursorResultsIter != std::end(cursorResults)) {
		const auto& peekResult = *cursorResultsIter++;
		const StorageTeamID& storageTeamID = *emptyCursorStorageTeamIDsIter;

		// Timeout
		if (!peekResult.endOfStream && !peekResult.retrievedData) {
			TraceEvent(SevWarn, "CursorTimeOutError").detail("StorageTeamID", storageTeamID);
			cursorsReady = false;
			++emptyCursorStorageTeamIDsIter;
			continue;
		}

		if (peekResult.endOfStream) {
			TraceEvent(SevInfo, "CursorEndOfStream").detail("StorageTeamID", storageTeamID);
			pPeekRemoteContext->retiredCursorStorageTeamIDs.push_back(storageTeamID);
		}

		pPeekRemoteContext->emptyCursorStorageTeamIDs.erase(emptyCursorStorageTeamIDsIter++);
	}

	return cursorsReady;
}

} // namespace BroadcastedStorageTeamPeekCursor

} // namespace details

#pragma region BroadcastedStorageTeamPeekCursorBase

bool BroadcastedStorageTeamPeekCursorBase::tryFillCursorContainer() {
	ASSERT(getNumCursors() != 0);
	ASSERT(pCursorContainer && pCursorContainer->empty());

	for (const auto& storageTeamID : retiredCursorStorageTeamIDs) {
		removeCursor(storageTeamID);
	}

	if (getNumCursors() == 0) {
		// We have no active cursors, fail the cursor filling process
		// In this case, the caller should do the RPC, and peekRemote should throw end_of_stream to indicate the cursor
		// is expired.
		return false;
	}

	// Find the current version all the cursors are sharing
	currentVersion = invalidVersion;
	bool isFirstElement = true;
	for (auto iter = cursorsBegin(); iter != cursorsEnd(); ++iter) {
		auto pCursor = iter->second;
		if (!pCursor->hasRemaining()) {
			emptyCursorStorageTeamIDs.push_back(pCursor->getStorageTeamID());
			continue;
		}

		Version cursorVersion = iter->second->getVersion();
		if (isFirstElement) {
			currentVersion = cursorVersion;
			isFirstElement = false;
		} else {
			// In the broadcast model, the cursors must be in state of:
			//   * For cursors that have messages, they share the same version.
			// . * Otherwise, the cursor must have no remaining data, i.e. needs RPC to get refilled.
			// The cursors cannot be lagged behind, or the subsequence constraint cannot be fulfilled.
			UNSTOPPABLE_ASSERT(currentVersion == cursorVersion);
		}
	}

	if (!emptyCursorStorageTeamIDs.empty()) {
		// There are cursors locally consumed. Requires RPC to get a refill.
		return false;
	}

	// No cursor should report its version invalidVersion
	ASSERT(currentVersion != invalidVersion);

	// Now the cursors are all sharing the same version, fill the cursor container for consumption
	for (auto iter = cursorsBegin(); iter != cursorsEnd(); ++iter) {
		pCursorContainer->push(
		    std::dynamic_pointer_cast<ptxn::details::VersionSubsequencePeekCursorBase>(iter->second));
	}

	return true;
}

Future<bool> BroadcastedStorageTeamPeekCursorBase::remoteMoreAvailableImpl() {
	using PeekRemoteContext = details::BroadcastedStorageTeamPeekCursor::PeekRemoteContext;
	std::shared_ptr<PeekRemoteContext> pContext = std::make_shared<PeekRemoteContext>(
	    emptyCursorStorageTeamIDs, retiredCursorStorageTeamIDs, [this](const StorageTeamID& storageTeamID) -> auto {
		    return getCursorPtr(storageTeamID);
	    });
	return details::BroadcastedStorageTeamPeekCursor::peekRemote(pContext);
}

const VersionSubsequenceMessage& BroadcastedStorageTeamPeekCursorBase::getImpl() const {
	return pCursorContainer->front()->get();
}

void BroadcastedStorageTeamPeekCursorBase::addCursorImpl(std::shared_ptr<StorageTeamPeekCursor>&& cursor) {
	ASSERT(!cursor->isEmptyVersionsIgnored());
	emptyCursorStorageTeamIDs.push_back(cursor->getStorageTeamID());
	details::StorageTeamIDCursorMapperMixin::addCursorImpl(std::move(cursor));
}

bool BroadcastedStorageTeamPeekCursorBase::hasRemainingImpl() const {
	while (true) {
		// Remove all empty version message
		while (!pCursorContainer->empty() &&
		       pCursorContainer->front()->get().message.getType() == Message::Type::EMPTY_VERSION_MESSAGE) {
			auto& pCursorContainer = const_cast<BroadcastedStorageTeamPeekCursorBase*>(this)->pCursorContainer;
			auto pConsumedCursor = pCursorContainer->front();
			pConsumedCursor->next();
			pCursorContainer->pop();
		}

		if (!pCursorContainer->empty()) {
			return true;
		} else {
			// FIXME Rethink this... const_cast is bitter yet setting hasRemainingImpl non-const is also painful
			bool tryFillResult = const_cast<BroadcastedStorageTeamPeekCursorBase*>(this)->tryFillCursorContainer();
			if (!tryFillResult) {
				return false;
			}
			return true;
		}
	}
}

#pragma endregion BroadcastedStorageTeamPeekCursorBase

void BroadcastedStorageTeamPeekCursor_Ordered::nextImpl() {
	if (pCursorContainer->empty() && !tryFillCursorContainer()) {
		// Calling BroadcastedStorageTeamPeekCursor::next while hasRemaining is fals
		ASSERT(false);
	}

	details::OrderedCursorContainer::element_t pConsumedCursor = pCursorContainer->front();
	pCursorContainer->pop();
	pConsumedCursor->next();
	if (pConsumedCursor->hasRemaining() && pConsumedCursor->getVersion() == currentVersion) {
		// The current version is not completely consumed, push it back for consumption
		pCursorContainer->push(
		    std::dynamic_pointer_cast<ptxn::details::VersionSubsequencePeekCursorBase>(pConsumedCursor));
	}
}

void BroadcastedStorageTeamPeekCursor_Unordered::nextImpl() {
	if (pCursorContainer->empty() && !tryFillCursorContainer()) {
		// Calling BroadcastedStorageTeamPeekCursor::next while hasRemaining is fals
		ASSERT(false);
	}

	details::OrderedCursorContainer::element_t pConsumedCursor = pCursorContainer->front();
	pConsumedCursor->next();
	if (!pConsumedCursor->hasRemaining() || pConsumedCursor->getVersion() != currentVersion) {
		pCursorContainer->pop();
	}
}

} // namespace merged

// Moves the cursor so it locates to the given version/subsequence. If the version/subsequence does not exist, moves the
// cursor to the closest next mutation. If the version/subsequence is earlier than the current version/subsequence the
// cursor is located, then the code will do nothing.
ACTOR Future<Void> advanceTo(PeekCursorBase* cursor, Version version, Subsequence subsequence) {
	state PeekCursorBase::iterator iter = cursor->begin();

	loop {
		while (iter != cursor->end()) {
			// Is iter already past the version?
			if (iter->version > version) {
				return Void();
			}
			// Is iter current at the given version?
			if (iter->version == version) {
				while (iter != cursor->end() && iter->version == version && iter->subsequence < subsequence)
					++iter;
				if (iter->version > version || iter->subsequence >= subsequence) {
					return Void();
				}
			}
			++iter;
		}

		// Consumed local data, need to check remote TLog
		bool remoteAvailable = wait(cursor->remoteMoreAvailable());
		if (!remoteAvailable) {
			// The version/subsequence should be in the future
			// Throw error?
			return Void();
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////
// ServerPeekCursor used for demo
//////////////////////////////////////////////////////////////////////////////////

ServerPeekCursor::ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> interf,
                                   Tag tag,
                                   StorageTeamID storageTeamId,
                                   TLogGroupID tLogGroupID,
                                   Version begin,
                                   Version end,
                                   bool returnIfBlocked,
                                   bool parallelGetMore)
  : interf(interf), tag(tag), storageTeamId(storageTeamId), tLogGroupID(tLogGroupID),
    results(Optional<UID>(), emptyCursorHeader().arena(), emptyCursorHeader()),
    rd(results.arena, results.data, IncludeVersion(ProtocolVersion::withPartitionTransaction())), messageVersion(begin),
    end(end), dbgid(deterministicRandom()->randomUniqueID()), returnIfBlocked(returnIfBlocked),
    parallelGetMore(parallelGetMore) {
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;
	TraceEvent(SevDebug, "SPC_Starting", dbgid)
	    .detail("Team", storageTeamId)
	    .detail("Group", tLogGroupID)
	    .detail("Tag", tag.toString())
	    .detail("Begin", begin)
	    .detail("End", end);
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
  : tag(tag), storageTeamId(storageTeamId), tLogGroupID(tLogGroupID), results(results),
    rd(results.arena, results.data, IncludeVersion(ProtocolVersion::withPartitionTransaction())),
    messageVersion(messageVersion), end(end), poppedVersion(poppedVersion), messageAndTags(message), hasMsg(hasMsg),
    dbgid(deterministicRandom()->randomUniqueID()) {
	TraceEvent(SevDebug, "SPC_Clone", dbgid);
	this->results.maxKnownVersion = 0;
	this->results.minKnownCommittedVersion = 0;

	// Consume the message header
	details::MessageHeader messageHeader;
	rd >> messageHeader;

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
	TraceEvent(SevDebug, "SPC_HasMessage", dbgid).detail("HasMsg", hasMsg);
	return hasMsg;
}

void ServerPeekCursor::nextMessage() {
	TraceEvent(SevDebug, "SPC_NextMessage", dbgid).detail("MessageVersion", messageVersion.toString());
	ASSERT(hasMsg);
	if (rd.empty()) {
		messageVersion.reset(std::min(results.endVersion, end.version));
		hasMsg = false;
		return;
	}
	if (messageIndexInCurrentVersion >= numMessagesInCurrentVersion) {
		// Read the version header
		while (!rd.empty()) {
			details::SubsequencedItemsHeader sih;
			rd >> sih;
			if (sih.version >= end.version) {
				messageVersion.reset(sih.version);
				hasMsg = false;
				numMessagesInCurrentVersion = 0;
				messageIndexInCurrentVersion = 0;
				return;
			}
			messageVersion.reset(sih.version);
			hasMsg = sih.numItems > 0;
			numMessagesInCurrentVersion = sih.numItems;
			messageIndexInCurrentVersion = 0;
			if (hasMsg) {
				break;
			}
		}
		if (rd.empty()) {
			return;
		}
	}

	Subsequence subsequence;
	rd >> subsequence;
	messageVersion.sub = subsequence;
	hasMsg = true;
	++messageIndexInCurrentVersion;

	// StorageServer.actor.cpp will directly read message from ArenaReader.
	TraceEvent(SevDebug, "SPC_NextMessageB", dbgid).detail("MessageVersion", messageVersion.toString());
}

StringRef ServerPeekCursor::getMessage() {
	// This is not supported yet
	ASSERT(false);
	TraceEvent(SevDebug, "SPC_GetMessage", dbgid);
	StringRef message = messageAndTags.getMessageWithoutTags();
	rd.readBytes(message.size()); // Consumes the message.
	return message;
}

StringRef ServerPeekCursor::getMessageWithTags() {
	ASSERT(false);
	StringRef rawMessage = messageAndTags.getRawMessage();
	rd.readBytes(rawMessage.size() - messageAndTags.getHeaderSize()); // Consumes the message.
	return rawMessage;
}

VectorRef<Tag> ServerPeekCursor::getTags() const {
	ASSERT(false);
	return messageAndTags.tags;
}

void ServerPeekCursor::advanceTo(LogMessageVersion n) {
	TraceEvent(SevDebug, "SPC_AdvanceTo", dbgid).detail("N", n.toString());
	while (messageVersion < n && hasMessage()) {
		if (LogProtocolMessage::isNextIn(rd)) {
			LogProtocolMessage lpm;
			rd >> lpm;
			setProtocolVersion(rd.protocolVersion());
		} else if (rd.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(rd)) {
			SpanContextMessage scm;
			rd >> scm;
		} else {
			MutationRef msg;
			rd >> msg;
		}
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
	TraceEvent("SlowPeekStats", self->dbgid)
	    .detail("PeerAddress", addr)
	    .detail("SlowReplies", self->slowReplies)
	    .detail("FastReplies", self->fastReplies)
	    .detail("UnknownReplies", self->unknownReplies);

	if (self->slowReplies >= SERVER_KNOBS->PEEK_STATS_SLOW_AMOUNT &&
	    self->slowReplies / double(self->slowReplies + self->fastReplies) >= SERVER_KNOBS->PEEK_STATS_SLOW_RATIO) {

		TraceEvent("ConnectionResetSlowPeek", self->dbgid)
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
	// Not supported in DEMO
	ASSERT(false);
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
					    self->interf->get().interf().peek.getEndpoint().getPrimaryAddress(),
					    self->interf->get().interf().peek.getReply(TLogPeekRequest(self->dbgid,
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
					self->rd = ArenaReader(self->results.arena,
					                       self->results.data,
					                       IncludeVersion(ProtocolVersion::withPartitionTransaction()));
					details::MessageHeader messageHeader;
					self->rd >> messageHeader;
					LogMessageVersion skipSeq = self->messageVersion;
					self->hasMsg = true;
					self->nextMessage();
					self->advanceTo(skipSeq);
					TraceEvent(SevDebug, "SPC_GetMoreB", self->dbgid)
					    .detail("Has", self->hasMessage())
					    .detail("End", res.endVersion)
					    .detail("Popped", res.popped.present() ? res.popped.get() : 0);
					return Void();
				}
				when(wait(self->interfaceChanged)) {
					self->interfaceChanged = self->interf->onChange();
					self->dbgid = deterministicRandom()->randomUniqueID();
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
				TraceEvent("PeekCursorTimedOut", self->dbgid).error(e);
				// We *should* never get timed_out(), as it means the TLog got stuck while handling a parallel peek,
				// and thus we've likely just wasted 10min.
				// timed_out() is sent by cleanupPeekTrackers as value PEEK_TRACKER_EXPIRATION_TIME
				ASSERT_WE_THINK(e.code() == error_code_operation_obsolete ||
				                SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME < 10);
				self->interfaceChanged = self->interf->onChange();
				self->dbgid = deterministicRandom()->randomUniqueID();
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
		loop choose {
			when(TLogPeekReply res = wait(self->interf->get().present()
			                                  ? brokenPromiseToNever(self->interf->get().interf().peek.getReply(
			                                        TLogPeekRequest(self->dbgid,
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
					self->poppedVersion = std::min(std::max(self->poppedVersion, res.popped.get()), self->end.version);
				self->rd = ArenaReader(self->results.arena,
				                       self->results.data,
				                       IncludeVersion(ProtocolVersion::withPartitionTransaction()));
				details::MessageHeader messageHeader;
				self->rd >> messageHeader;
				LogMessageVersion skipSeq = self->messageVersion;
				self->hasMsg = true;
				self->nextMessage();
				self->advanceTo(skipSeq);
				TraceEvent(SevDebug, "SPC_GetMoreB", self->dbgid)
				    .detail("Has", self->hasMessage())
				    .detail("End", res.endVersion)
				    .detail("Popped", res.popped.present() ? res.popped.get() : 0);
				return Void();
			}
			when(wait(self->interf->onChange())) { self->onlySpilled = false; }
		}
	} catch (Error& e) {
		TraceEvent(SevDebug, "SPC_PeekGetMoreError", self->dbgid).error(e, true);
		if (e.code() == error_code_end_of_stream) {
			self->end.reset(self->messageVersion.version);
			return Void();
		}
		throw e;
	}
}

Future<Void> ServerPeekCursor::getMore(TaskPriority taskID) {
	TraceEvent(SevDebug, "SPC_GetMore", dbgid)
	    .detail("More", !more.isValid() || more.isReady())
	    .detail("MessageVersion", messageVersion.toString())
	    .detail("End", end.toString());
	if (hasMessage() && !parallelGetMore)
		return Void();
	if (!more.isValid() || more.isReady()) {
		if (parallelGetMore || onlySpilled || futureResults.size()) {
			ASSERT(false); // not used
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
			                    self->interf->get().interf().peek.getEndpoint(), FailureStatus())
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
	return IFailureMonitor::failureMonitor().getState(interf->get().interf().peek.getEndpoint()).isAvailable();
}

bool ServerPeekCursor::isExhausted() const {
	return messageVersion >= end;
}

// Call only after nextMessage(). The sequence of the current message, or
// results.end if nextMessage() has returned false.
const LogMessageVersion& ServerPeekCursor::version() const {
	return messageVersion;
}

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

//////////////////////////////////////////////////////////////////////////////////
// ServerPeekCursor used for demo -- end
//////////////////////////////////////////////////////////////////////////////////

} // namespace ptxn