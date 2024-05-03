/*
 * MoveKeys.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_MOVEKEYS_ACTOR_G_H)
#define FDBSERVER_MOVEKEYS_ACTOR_G_H
#include "fdbserver/MoveKeys.actor.g.h"
#elif !defined(FDBSERVER_MOVEKEYS_ACTOR_H)
#define FDBSERVER_MOVEKEYS_ACTOR_H

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/MasterInterface.h"
#include "flow/BooleanParam.h"
#include "flow/actorcompiler.h"

FDB_BOOLEAN_PARAM(CancelConflictingDataMoves);

struct MoveKeysLock {
	UID prevOwner, myOwner, prevWrite;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, prevOwner, myOwner, prevWrite);
	}
};

// An transient state of DD. Some functionality of DD maybe disabled depends on the enabled state.
// If the process restarts, the state will be forgotten and become the default value.
class DDEnabledState {
	enum Value {
		ENABLED, // DD is enabled
		SNAPSHOT, // disabled for snapshot
		BLOB_RESTORE_PREPARING // disabled for hybrid restore
	};

	// in-memory flag to disable DD
	UID ddEnabledStatusUID;
	Value stateValue = ENABLED;

public:
	bool sameId(const UID&) const;

	bool isEnabled() const;

	bool isBlobRestorePreparing() const;

	// All tryXXX() methods return true if the in-memory state change succeed.
	bool trySetEnabled(UID requesterId);

	bool trySetSnapshot(UID requesterId);

	bool trySetBlobRestorePreparing(UID requesterId);
};

struct MoveKeysParams {
	UID dataMoveId;

	// Only one of `keys` and `ranges` can be set. `ranges` is created mainly for physical shard moves to move a full
	// physical shard with multiple key ranges.
	Optional<KeyRange> keys;
	Optional<std::vector<KeyRange>> ranges;

	std::vector<UID> destinationTeam, healthyDestinations;
	MoveKeysLock lock;
	Promise<Void> dataMovementComplete;
	FlowLock* startMoveKeysParallelismLock = nullptr;
	FlowLock* finishMoveKeysParallelismLock = nullptr;
	bool hasRemote;
	UID relocationIntervalId;
	const DDEnabledState* ddEnabledState = nullptr;
	CancelConflictingDataMoves cancelConflictingDataMoves = CancelConflictingDataMoves::False;

	MoveKeysParams() {}

	MoveKeysParams(UID dataMoveId,
	               const KeyRange& keys,
	               const std::vector<UID>& destinationTeam,
	               const std::vector<UID>& healthyDestinations,
	               const MoveKeysLock& lock,
	               const Promise<Void>& dataMovementComplete,
	               FlowLock* startMoveKeysParallelismLock,
	               FlowLock* finishMoveKeysParallelismLock,
	               bool hasRemote,
	               UID relocationIntervalId,
	               const DDEnabledState* ddEnabledState,
	               CancelConflictingDataMoves cancelConflictingDataMoves)
	  : dataMoveId(dataMoveId), keys(keys), destinationTeam(destinationTeam), healthyDestinations(healthyDestinations),
	    lock(lock), dataMovementComplete(dataMovementComplete),
	    startMoveKeysParallelismLock(startMoveKeysParallelismLock),
	    finishMoveKeysParallelismLock(finishMoveKeysParallelismLock), hasRemote(hasRemote),
	    relocationIntervalId(relocationIntervalId), ddEnabledState(ddEnabledState),
	    cancelConflictingDataMoves(cancelConflictingDataMoves) {}

	MoveKeysParams(UID dataMoveId,
	               const std::vector<KeyRange>& ranges,
	               const std::vector<UID>& destinationTeam,
	               const std::vector<UID>& healthyDestinations,
	               const MoveKeysLock& lock,
	               const Promise<Void>& dataMovementComplete,
	               FlowLock* startMoveKeysParallelismLock,
	               FlowLock* finishMoveKeysParallelismLock,
	               bool hasRemote,
	               UID relocationIntervalId,
	               const DDEnabledState* ddEnabledState,
	               CancelConflictingDataMoves cancelConflictingDataMoves)
	  : dataMoveId(dataMoveId), ranges(ranges), destinationTeam(destinationTeam),
	    healthyDestinations(healthyDestinations), lock(lock), dataMovementComplete(dataMovementComplete),
	    startMoveKeysParallelismLock(startMoveKeysParallelismLock),
	    finishMoveKeysParallelismLock(finishMoveKeysParallelismLock), hasRemote(hasRemote),
	    relocationIntervalId(relocationIntervalId), ddEnabledState(ddEnabledState),
	    cancelConflictingDataMoves(cancelConflictingDataMoves) {}
};

// read the lock value in system keyspace but do not change anything
ACTOR Future<MoveKeysLock> readMoveKeysLock(Database cx);

// Calling moveKeys, etc with the return value of this actor ensures that no movekeys, etc
// has been executed by a different locker since takeMoveKeysLock(), as calling
// takeMoveKeysLock() updates "moveKeysLockOwnerKey" to a random UID.
ACTOR Future<MoveKeysLock> takeMoveKeysLock(Database cx, UID ddId);

// Checks that the a moveKeysLock has not changed since having taken it
// This does not modify the moveKeysLock
Future<Void> checkMoveKeysLockReadOnly(Transaction* tr, MoveKeysLock lock, const DDEnabledState* ddEnabledState);

void seedShardServers(Arena& trArena, CommitTransactionRef& tr, std::vector<StorageServerInterface> servers);
// Called by the master server to write the very first transaction to the database
// establishing a set of shard servers and all invariants of the systemKeys.

Future<Void> rawStartMovement(Database occ,
                              const MoveKeysParams& params,
                              std::map<UID, StorageServerInterface>& tssMapping);

Future<Void> rawFinishMovement(Database occ,
                               const MoveKeysParams& params,
                               const std::map<UID, StorageServerInterface>& tssMapping);
// Eventually moves the given keys to the given destination team
// Caller is responsible for cancelling it before issuing an overlapping move,
// for restarting the remainder, and for not otherwise cancelling it before
// it returns (since it needs to execute the finishMoveKeys transaction).
// When dataMoveId.isValid(), the keyrange will be moved to a shard designated as dataMoveId.
ACTOR Future<Void> moveKeys(Database occ, MoveKeysParams params);

// Cancels a data move designated by dataMoveId.
ACTOR Future<Void> cleanUpDataMove(
    Database occ,
    UID dataMoveId,
    MoveKeysLock lock,
    FlowLock* cleanUpDataMoveParallelismLock,
    KeyRange range,
    const DDEnabledState* ddEnabledState,
    Optional<PromiseStream<Future<Void>>> addCleanUpDataMoveActor = Optional<PromiseStream<Future<Void>>>());

ACTOR Future<std::pair<Version, Tag>> addStorageServer(Database cx, StorageServerInterface server);
// Adds a newly recruited storage server to a database (e.g. adding it to FF/serverList)
// Returns a Version in which the storage server is in the database
// This doesn't need to be called for the "seed" storage servers (see seedShardServers above)

ACTOR Future<Void> removeStorageServer(Database cx,
                                       UID serverID,
                                       Optional<UID> tssPairID, // if serverID is a tss, set to its ss pair id
                                       MoveKeysLock lock,
                                       const DDEnabledState* ddEnabledState);
// Removes the given storage server permanently from the database.  It must already
// have no shards assigned to it.  The storage server MUST NOT be added again after this
// (though a new storage server with a new unique ID may be recruited from the same fdbserver).

ACTOR Future<bool> canRemoveStorageServer(Reference<ReadYourWritesTransaction> tr, UID serverID);
// Returns true if the given storage server has no keys assigned to it and may be safely removed
// Obviously that could change later!
ACTOR Future<Void> removeKeysFromFailedServer(Database cx,
                                              UID serverID,
                                              std::vector<UID> teamForDroppedRange,
                                              MoveKeysLock lock,
                                              const DDEnabledState* ddEnabledState);
// Directly removes serverID from serverKeys and keyServers system keyspace.
// Performed when a storage server is marked as permanently failed.

// Prepare for data migration for given key range. Reassign key ranges to the storage server interface hold by blob
// migrator
ACTOR Future<Void> prepareBlobRestore(Database occ,
                                      MoveKeysLock lock,
                                      const DDEnabledState* ddEnabledState,
                                      UID traceId,
                                      KeyRangeRef keys,
                                      UID bmId,
                                      UID reqId = UID());

#include "flow/unactorcompiler.h"
#endif
