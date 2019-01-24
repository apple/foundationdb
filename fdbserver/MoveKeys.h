/*
 * MoveKeys.h
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

#ifndef FDBSERVER_MOVEKEYS_H
#define FDBSERVER_MOVEKEYS_H
#pragma once

#include "fdbclient/NativeAPI.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/MasterInterface.h"

struct MoveKeysLock {
	UID prevOwner, myOwner, prevWrite;
	template <class Ar>
	void serialize(Ar& ar) { serializer(ar, prevOwner, myOwner, prevWrite); }
};

Future<MoveKeysLock> takeMoveKeysLock( Database const& cx, UID const& masterId );
// Calling moveKeys, etc with the return value of this actor ensures that no movekeys, etc
// has been executed by a different locker since takeMoveKeysLock().
// takeMoveKeysLock itself is a read-only operation - it does not conflict with other
// attempts to take the lock.

Future<Void> checkMoveKeysLockReadOnly( Transaction* tr, MoveKeysLock lock );
// Checks that the a moveKeysLock has not changed since having taken it
// This does not modify the moveKeysLock

void seedShardServers(
	Arena& trArena,
	CommitTransactionRef &tr,
	vector<StorageServerInterface> servers );
// Called by the master server to write the very first transaction to the database
// establishing a set of shard servers and all invariants of the systemKeys.

Future<Void> moveKeys(
	Database const& occ,
	KeyRange const& keys,
	vector<UID> const& destinationTeam,
	vector<UID> const& healthyDestinations,
	MoveKeysLock const& lock,
	Promise<Void> const& dataMovementComplete,
	FlowLock* const& startMoveKeysParallelismLock,
	FlowLock* const& finishMoveKeysParallelismLock,
	Version const& recoveryVersion,
	bool const& hasRemote,
	UID const& relocationIntervalId); // for logging only
// Eventually moves the given keys to the given destination team
// Caller is responsible for cancelling it before issuing an overlapping move,
// for restarting the remainder, and for not otherwise cancelling it before
// it returns (since it needs to execute the finishMoveKeys transaction).

Future<std::pair<Version,Tag>> addStorageServer(
	Database const& cx,
	StorageServerInterface const& server );
// Adds a newly recruited storage server to a database (e.g. adding it to FF/serverList)
// Returns a Version in which the storage server is in the database
// This doesn't need to be called for the "seed" storage servers (see seedShardServers above)

Future<Void> removeStorageServer(
	Database const& cx,
	UID const& serverID,
	MoveKeysLock const& lock );
// Removes the given storage server permanently from the database.  It must already
// have no shards assigned to it.  The storage server MUST NOT be added again after this
// (though a new storage server with a new unique ID may be recruited from the same fdbserver).

Future<bool> canRemoveStorageServer( Transaction* const& tr, UID const& serverID );
// Returns true if the given storage server has no keys assigned to it and may be safely removed
// Obviously that could change later!

#endif
