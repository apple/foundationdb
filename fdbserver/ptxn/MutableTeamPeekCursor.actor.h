/*
 * MutableTeamPeekCursor.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_G_H)
#define FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_G_H
#include "fdbserver/ptxn/MutableTeamPeekCursor.actor.g.h"
#elif !defined(FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_H)
#define FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_H

#pragma once

#include <algorithm>
#include <functional>
#include <list>
#include <unordered_map>
#include <memory>
#include <set>
#include <vector>

#include "fdbclient/SystemData.h"
#include "fdbserver/ptxn/TLogPeekCursor.actor.h"
#include "flow/Trace.h"

namespace ptxn::merged {

namespace detail {

// BaseClass must be subclass of BroadcastedStorageTeamPeekCursorBase
template <class BaseClass>
class MutableTeamPeekCursor : public BaseClass {
public:
	using TLogInterfaceByStorageTeamIDFunc = std::function<std::vector<TLogInterfaceBase*>(const StorageTeamID&)>;

private:
	// The ID of the storage server
	const UID serverID;

	// The storageServerToTeamId key for the storage server
	const Key storageServerToTeamIDKey;

	// Storage team ID snapshot
	mutable StorageServerStorageTeams storageTeamIDsSnapshot;

	// Teams
	StorageServerStorageTeams storageTeamIDs;

	// The incoming new list of storage team IDs this storage server should subscribe
	StorageServerStorageTeams newStorageTeamIDs;

	// The function that translates storage team ID to TLogInterface
	TLogInterfaceByStorageTeamIDFunc getTLogInterfaceByStorageTeamID;

	// Creates new cursor
	std::shared_ptr<StorageTeamPeekCursor> createCursor(const StorageTeamID& storageTeamID);

	// Cursors that being removed when remoteMoreAvailable is called
	// NOTE: They might be reintroduced later when re-iterate the cursor, thus they are not immediately dropped
	std::unordered_map<StorageTeamID, std::shared_ptr<StorageTeamPeekCursor>> cursorsToBeRemoved;

	// Additional Storage team IDs that should be included in the next RPC
	std::list<StorageTeamID> storageTeamIDsToBeAdded;

	// Retired cursor that are removed by private mutations, not by remoteMoreAvailable, they should be added back when
	// reset
	std::list<StorageTeamID> retiredCursorsRemovedByPrivateMutations;

	// Refreshes storageTeamIDs by newStorageTeamIDs, add/remove corresponding storage teams
	bool updateStorageTeams();

	// A flag indicates if the new storage team information is retrieved from remote, stored to newStorageTeamIDs, and
	// should replace storageTeamIDs
	bool shouldUpdateStorageTeamCursor;

protected:
	virtual const VersionSubsequenceMessage& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
	virtual void resetImpl() override;
	virtual Future<bool> remoteMoreAvailableImpl() override;

public:
	MutableTeamPeekCursor(const UID serverID_,
	                      const StorageTeamID& privateMutationStorageTeamID_,
	                      const TLogInterfaceByStorageTeamIDFunc& getTLogInterfaceByStorageTeamID_,
	                      const Version& initialVersion_)

	  : // This minus one in BaseClass is necessary since all new cursor will start with BaseClass::currentVersion + 1
	    BaseClass(initialVersion_ - 1), serverID(serverID_),
	    storageServerToTeamIDKey(::storageServerToTeamIdKey(serverID_)),
	    storageTeamIDsSnapshot(privateMutationStorageTeamID_), storageTeamIDs(privateMutationStorageTeamID_),
	    newStorageTeamIDs(privateMutationStorageTeamID_),
	    getTLogInterfaceByStorageTeamID(getTLogInterfaceByStorageTeamID_), shouldUpdateStorageTeamCursor(false) {

		const auto& privateMutationStorageTeamID = storageTeamIDs.getPrivateMutationsStorageTeamID();

		BaseClass::addCursor(createCursor(privateMutationStorageTeamID));

		TraceEvent("MutableTeamPeekCursorCreated")
		    .detail("ServerID", serverID)
		    .detail("PrivateMutationStorageTeamID", privateMutationStorageTeamID);
	}

	MutableTeamPeekCursor(const UID serverID_,
	                      const StorageServerStorageTeams& storageTeams_,
	                      const TLogInterfaceByStorageTeamIDFunc& getTLogInterfaceByStorageTeamID_,
	                      const Version& initialVersion_)
	  : MutableTeamPeekCursor(serverID_,
	                          storageTeams_.getPrivateMutationsStorageTeamID(),
	                          getTLogInterfaceByStorageTeamID_,
	                          initialVersion_) {

		for (const auto& storageTeamID : storageTeams_.getStorageTeams()) {
			BaseClass::addCursor(createCursor(storageTeamID));
		}
	}

	const StorageServerStorageTeams getStorageTeamIDs() const { return storageTeamIDs; }
};

template <typename BaseClass>
std::shared_ptr<StorageTeamPeekCursor> MutableTeamPeekCursor<BaseClass>::createCursor(
    const StorageTeamID& storageTeamID) {

	const auto tLogInterfaces = getTLogInterfaceByStorageTeamID(storageTeamID);
	ASSERT(tLogInterfaces.size() > 0);
	return std::make_shared<StorageTeamPeekCursor>(
	    // NOTE: Do NOT use BaseCalss::getVersion() since it will trigger getImpl
	    // The new cursor will start peeking *AFTER* current version is completed. The peek will be called immediately
	    // since hasRemaining will return false after the cursor is added; however the version is one after the current
	    // so the mutations in the current version will not be included.
	    BaseClass::currentVersion + 1,
	    storageTeamID,
	    tLogInterfaces,
	    /* pArena */ nullptr,
	    /* reportEmptyVersion */ true);
}

template <typename BaseClass>
bool MutableTeamPeekCursor<BaseClass>::updateStorageTeams() {
	const auto& newStorageTeams = newStorageTeamIDs.getStorageTeams();
	const auto& oldStorageTeams = storageTeamIDs.getStorageTeams();

	// NOTE: Do NOT use BaseCalss::getVersion() since it will trigger getImpl
	TraceEvent("MutableTeamPeekCursorUpdateTeam")
	    .detail("StorageServerID", serverID)
	    .detail("Version", BaseClass::currentVersion);

	// Storage teams to be removed in the next version, oldStorageTeams - newStorageTeams
	std::list<StorageTeamID> teamsToBeRemoved;
	std::set_difference(std::begin(oldStorageTeams),
	                    std::end(oldStorageTeams),
	                    std::begin(newStorageTeams),
	                    std::end(newStorageTeams),
	                    std::back_inserter(teamsToBeRemoved));
	for (const auto& storageTeamID : teamsToBeRemoved) {
		if (!BaseClass::isCursorExists(storageTeamID)) {
			TraceEvent("MutableTeamPeekCursorRemoveTeam")
			    .detail("StorageServerID", serverID)
			    .detail("StorageTeamID", storageTeamID)
			    .detail("PreviouslyRemoved", true);
			continue;
		}

		ASSERT_EQ(cursorsToBeRemoved.count(storageTeamID), 0);
		// Remove cursor is done immediately -- otherwise the content will still be accessed when iterating.
		cursorsToBeRemoved[storageTeamID] = BaseClass::removeCursor(storageTeamID);

		if (BaseClass::retiredCursorStorageTeamIDs.count(storageTeamID) > 0) {
			// If the cursor is also retired, remove it from retired cursors list; otherwise remoteMoreAvailable will
			// remove the cursor again, causing ASSERT error.
			BaseClass::retiredCursorStorageTeamIDs.erase(storageTeamID);
			retiredCursorsRemovedByPrivateMutations.push_back(storageTeamID);
		}

		TraceEvent("MutableTeamPeekCursorRemoveTeam")
		    .detail("StorageServerID", serverID)
		    .detail("StorageTeamID", storageTeamID);
	}

	// Storage teams to be added, newStorageTeams - oldStorageTeams
	// NOTE: The new cursor might be a cursor previously removed, however in real situation this will happen rarely.
	// Thus, do not try to reuse old cursor.
	std::set_difference(std::begin(newStorageTeams),
	                    std::end(newStorageTeams),
	                    std::begin(oldStorageTeams),
	                    std::end(oldStorageTeams),
	                    std::back_inserter(storageTeamIDsToBeAdded));
	// NOTE: Adding new cursors will always cause RPC, so the cursors will be created at RPC step, i.e.,
	// remoteMoreAvailableImpl

	storageTeamIDs = newStorageTeamIDs;

	return storageTeamIDsToBeAdded.size() > 0;
}

template <typename BaseClass>
void MutableTeamPeekCursor<BaseClass>::resetImpl() {
	for (const auto& [_, cursor] : cursorsToBeRemoved) {
		BaseClass::addCursor(cursor);
	}
	cursorsToBeRemoved.clear();

	for (const auto& retiredCursorRemovedByPrivateMutations : retiredCursorsRemovedByPrivateMutations) {
		BaseClass::retiredCursorStorageTeamIDs.insert(retiredCursorRemovedByPrivateMutations);
	}
	retiredCursorsRemovedByPrivateMutations.clear();

	storageTeamIDsToBeAdded.clear();
	shouldUpdateStorageTeamCursor = false;
	storageTeamIDs = storageTeamIDsSnapshot;

	BaseClass::resetImpl();
}

template <typename BaseClass>
Future<bool> MutableTeamPeekCursor<BaseClass>::remoteMoreAvailableImpl() {
	// Since RPC will invalidate all local cursors, drop all cursors to be removed and create new cursors if needed
	retiredCursorsRemovedByPrivateMutations.clear();
	cursorsToBeRemoved.clear();
	for (const auto& storageTeamID : storageTeamIDsToBeAdded) {
		ASSERT(!BaseClass::isCursorExists(storageTeamID));

		auto newCursor = createCursor(storageTeamID);
		BaseClass::addCursor(newCursor);
		// NOTE: Do *NOT* remove the cursor from cursorsToBeRemoved list if it exists, otherwise the cursor will not
		// be reset properly

		TraceEvent("MutableTeamPeekCursorAddTeam")
		    .detail("StorageServerID", serverID)
		    .detail("StorageTeamID", storageTeamID)
		    .detail("BeginVersion", newCursor->getBeginVersion());
	}
	storageTeamIDsToBeAdded.clear();

	return BaseClass::remoteMoreAvailableImpl();
}

template <typename BaseClass>
const VersionSubsequenceMessage& MutableTeamPeekCursor<BaseClass>::getImpl() const {
	const auto& vsm = BaseClass::getImpl();

	if (vsm.message.getType() != Message::Type::MUTATION_REF) {
		return vsm;
	}

	const MutationRef& mutationRef = std::get<MutationRef>(vsm.message);
	if (!mutationRef.param1.startsWith(storageServerToTeamIDKey)) {
		return vsm;
	}

	const_cast<MutableTeamPeekCursor*>(this)->newStorageTeamIDs = StorageServerStorageTeams(mutationRef.param2);
	ASSERT(newStorageTeamIDs.getPrivateMutationsStorageTeamID() == storageTeamIDs.getPrivateMutationsStorageTeamID());
	const_cast<MutableTeamPeekCursor*>(this)->shouldUpdateStorageTeamCursor = true;

	return vsm;
}

template <typename BaseClass>
bool MutableTeamPeekCursor<BaseClass>::hasRemainingImpl() const {
	if (BaseClass::getNumCursors() == 0) {
		return false;
	}

	if (BaseClass::remoteMoreAvailableSnapshot.needSnapshot) {
		storageTeamIDsSnapshot = storageTeamIDs;
	}

	bool newCursorsAdded = false;
	if (BaseClass::isCursorContainerEmpty() && shouldUpdateStorageTeamCursor) {
		// One version is completely consumed, update the list of cursors
		// FIXME Rethink this const_cast, perhaps relax the function not to be const??
		newCursorsAdded = const_cast<MutableTeamPeekCursor*>(this)->updateStorageTeams();
		const_cast<MutableTeamPeekCursor*>(this)->shouldUpdateStorageTeamCursor = false;
	}

	// If no new cursor added, we call hasRemainingImpl to do a tryFillCursorContainer
	return !newCursorsAdded && BaseClass::hasRemainingImpl();
}

} // namespace detail

using OrderedMutableTeamPeekCursor = detail::MutableTeamPeekCursor<BroadcastedStorageTeamPeekCursor_Ordered>;
// TODO Add test for this cursor
using UnorderedMutableTeamPeekCursor = detail::MutableTeamPeekCursor<BroadcastedStorageTeamPeekCursor_Unordered>;

} // namespace ptxn::merged

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_H