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

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint,
                                       const CheckpointAsKeyValues checkpointAsKeyValues,
                                       UID logID) {
	const CheckpointFormat format = checkpoint.getFormat();
	if (format == DataMoveRocksCF || format == RocksDB) {
		return newRocksDBCheckpointReader(checkpoint, checkpointAsKeyValues, logID);
	} else {
		throw not_implemented();
	}

	return nullptr;
}

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));
	state CheckpointFormat format = checkpoint.getFormat();
	if (format == DataMoveRocksCF || format == RocksDB) {
		wait(deleteRocksCheckpoint(checkpoint));
	} else {
		throw not_implemented();
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 CheckpointAsKeyValues checkpointAsKeyValues,
                                                 std::vector<KeyRange> ranges,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent("FetchCheckpointBegin", initialState.checkpointID).detail("CheckpointMetaData", initialState.toString());
	ASSERT(!ranges.empty() || !checkpointAsKeyValues);
	state CheckpointMetaData result;
	CheckpointFormat format = initialState.getFormat();
	if (checkpointAsKeyValues && format != RocksDBKeyValues) {
		initialState.setFormat(RocksDBKeyValues);
		format = RocksDBKeyValues;
		initialState.serializedCheckpoint = ObjectWriter::toValue(RocksDBCheckpointKeyValues(), IncludeVersion());
	}
	if (format == DataMoveRocksCF || format == RocksDB || format == RocksDBKeyValues) {
		wait(store(result, fetchRocksDBCheckpoint(cx, initialState, dir, checkpointAsKeyValues, ranges, cFun)));
	} else {
		throw not_implemented();
	}

	TraceEvent("FetchCheckpointEnd", initialState.checkpointID).detail("CheckpointMetaData", result.toString());
	return result;
}