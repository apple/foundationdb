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

	// Teams
	StorageServerStorageTeams storageTeamIDs;

	// The incoming new list of storage team IDs this storage server should subscribe
	StorageServerStorageTeams newStorageTeamIDs;

	// The function that translates storage team ID to TLogInterface
	TLogInterfaceByStorageTeamIDFunc getTLogInterfaceByStorageTeamID;

	// Creates new cursor
	std::shared_ptr<StorageTeamPeekCursor> createCursor(const StorageTeamID& storageTeamID);

	// Refreshes storageTeamIDs by newStorageTeamIDs, add/remove corresponding storage teams
	bool updateStorageTeams();

	// A flag indicates if the new storage team information is retrieved from remote, stored to newStorageTeamIDs, and
	// should replace storageTeamIDs
	bool shouldUpdateStorageTeamCursor;

protected:
	virtual const VersionSubsequenceMessage& getImpl() const override;

	virtual bool hasRemainingImpl() const override;

public:
	MutableTeamPeekCursor(const UID serverID_,
	                      const StorageTeamID& privateMutationStorageTeamID_,
	                      const TLogInterfaceByStorageTeamIDFunc& getTLogInterfaceByStorageTeamID_)
	  : BaseClass(), serverID(serverID_), storageServerToTeamIDKey(::storageServerToTeamIdKey(serverID_)),
	    storageTeamIDs(privateMutationStorageTeamID_), newStorageTeamIDs(privateMutationStorageTeamID_),
	    getTLogInterfaceByStorageTeamID(getTLogInterfaceByStorageTeamID_), shouldUpdateStorageTeamCursor(false) {

		const auto& privateMutationStorageTeamID = storageTeamIDs.getPrivateMutationsStorageTeamID();

		BaseClass::addCursor(createCursor(privateMutationStorageTeamID));

		TraceEvent("MutableTeamPeekCursorCreated")
		    .detail("ServerID", serverID.toString())
		    .detail("PrivateMutationStorageTeamID", privateMutationStorageTeamID.toString());
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
	    .detail("StorageServerID", serverID.toString())
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
			// The cursor might get end-of-stream already, and being removed automatically by tryFillCursorContainer
			TraceEvent("MutableTeamPeekCursorRemoveTeam")
			    .detail("StorageServerID", serverID.toString())
			    .detail("StorageTeamID", storageTeamID.toString())
			    .detail("PreviouslyRemoved", true);
			continue;
		}
		BaseClass::removeCursor(storageTeamID);
		TraceEvent("MutableTeamPeekCursorRemoveTeam")
		    .detail("StorageServerID", serverID.toString())
		    .detail("StorageTeamID", storageTeamID.toString());
	}

	// Storage teams to be added, newStorageTeams - oldStorageTeams
	std::list<StorageTeamID> teamsToBeAdded;
	std::set_difference(std::begin(newStorageTeams),
	                    std::end(newStorageTeams),
	                    std::begin(oldStorageTeams),
	                    std::end(oldStorageTeams),
	                    std::back_inserter(teamsToBeAdded));
	for (const auto& storageTeamID : teamsToBeAdded) {
		ASSERT(!BaseClass::isCursorExists(storageTeamID));

		auto newCursor = createCursor(storageTeamID);
		BaseClass::addCursor(newCursor);

		TraceEvent("MutableTeamPeekCursorAddTeam")
		    .detail("StorageServerID", serverID.toString())
		    .detail("StorageTeamID", storageTeamID)
		    .detail("BeginVersion", newCursor->getBeginVersion());
	}

	storageTeamIDs = newStorageTeamIDs;

	return teamsToBeAdded.size() > 0;
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
	bool newCursorsAdded = false;
	if (shouldUpdateStorageTeamCursor) {
		// FIXME Rethink this const_cast, perhaps relax the function not to be const??
		newCursorsAdded = const_cast<MutableTeamPeekCursor*>(this)->updateStorageTeams();
		const_cast<MutableTeamPeekCursor*>(this)->shouldUpdateStorageTeamCursor = false;
	}

	bool hasRemaining = BaseClass::hasRemainingImpl();

	return !newCursorsAdded && hasRemaining;
}

} // namespace detail

using OrderedMutableTeamPeekCursor = detail::MutableTeamPeekCursor<BroadcastedStorageTeamPeekCursor_Ordered>;
// TODO Add test for this cursor
using UnorderedMutableTeamPeekCursor = detail::MutableTeamPeekCursor<BroadcastedStorageTeamPeekCursor_Unordered>;

} // namespace ptxn::merged

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_MUTABLETEAMPEEKCURSOR_ACTOR_H