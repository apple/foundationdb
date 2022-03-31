/*
 *ServerCheckpoint.actor.cpp
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

#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		return newRocksDBCheckpointReader(checkpoint, logID);
	} else if (checkpoint.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return nullptr;
}

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));

	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		wait(deleteRocksCFCheckpoint(checkpoint));
	} else if (checkpoint.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	state CheckpointMetaData result;
	if (initialState.getFormat() == RocksDBColumnFamily) {
		CheckpointMetaData _result = wait(fetchRocksDBCheckpoint(cx, initialState, dir, cFun));
		result = _result;
	} else if (initialState.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return result;
}