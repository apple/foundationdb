/*
 * MoveKeys.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_MOVEKEYS_ACTOR_G_H)
	#define FDBSERVER_MOVEKEYS_ACTOR_G_H
	#include "fdbserver/MoveKeys.actor.g.h"
#elif !defined(FDBSERVER_MOVEKEYS_ACTOR_H)
	#define FDBSERVER_MOVEKEYS_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/MasterInterface.h"
#include "flow/actorcompiler.h"

struct MoveKeysLock {
	UID prevOwner, myOwner, prevWrite;
	template <class Ar>
	void serialize(Ar& ar) { serializer(ar, prevOwner, myOwner, prevWrite); }
};

// Calling moveKeys, etc with the return value of this actor ensures that no movekeys, etc
// has been executed by a different locker since takeMoveKeysLock(), as calling
// takeMoveKeysLock() updates "moveKeysLockOwnerKey" to a random UID.
ACTOR Future<MoveKeysLock> takeMoveKeysLock(Database cx, UID ddId);

// Checks that the a moveKeysLock has not changed since having taken it
// This does not modify the moveKeysLock
Future<Void> checkMoveKeysLockReadOnly(Transaction* tr, MoveKeysLock lock);

bool isDDEnabled();
// checks if the in-memory DDEnabled flag is set

bool setDDEnabled(bool status, UID snapUID);
// sets the in-memory DDEnabled flag

void seedShardServers(
	Arena& trArena,
	CommitTransactionRef &tr,
	vector<StorageServerInterface> servers );
// Called by the master server to write the very first transaction to the database
// establishing a set of shard servers and all invariants of the systemKeys.

ACTOR Future<Void> moveKeys(Database occ, KeyRange keys, vector<UID> destinationTeam, vector<UID> healthyDestinations,
                            MoveKeysLock lock, Promise<Void> dataMovementComplete,
                            FlowLock* startMoveKeysParallelismLock, FlowLock* finishMoveKeysParallelismLock,
                            bool hasRemote,
                            UID relocationIntervalId); // for logging only
// Eventually moves the given keys to the given destination team
// Caller is responsible for cancelling it before issuing an overlapping move,
// for restarting the remainder, and for not otherwise cancelling it before
// it returns (since it needs to execute the finishMoveKeys transaction).

ACTOR Future<std::pair<Version, Tag>> addStorageServer(Database cx, StorageServerInterface server);
// Adds a newly recruited storage server to a database (e.g. adding it to FF/serverList)
// Returns a Version in which the storage server is in the database
// This doesn't need to be called for the "seed" storage servers (see seedShardServers above)

ACTOR Future<Void> removeStorageServer(Database cx, UID serverID, MoveKeysLock lock);
// Removes the given storage server permanently from the database.  It must already
// have no shards assigned to it.  The storage server MUST NOT be added again after this
// (though a new storage server with a new unique ID may be recruited from the same fdbserver).

ACTOR Future<bool> canRemoveStorageServer(Transaction* tr, UID serverID);
// Returns true if the given storage server has no keys assigned to it and may be safely removed
// Obviously that could change later!
ACTOR Future<Void> removeKeysFromFailedServer(Database cx, UID serverID, MoveKeysLock lock);
// Directly removes serverID from serverKeys and keyServers system keyspace.
// Performed when a storage server is marked as permanently failed.

#include "flow/unactorcompiler.h"
#endif
