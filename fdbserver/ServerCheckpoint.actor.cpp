/*
 *ServerCheckpoint.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
	const CheckpointFormat format = checkpoint.getFormat();
	if (format == DataMoveRocksCF || format == RocksDB || format == RocksDBKeyValues) {
		if (!checkpoint.dir.empty()) {
			platform::eraseDirectoryRecursive(checkpoint.dir);
		} else {
			TraceEvent(SevWarn, "CheckpointDirNotFound").detail("Checkpoint", checkpoint.toString());
		}
	} else {
		throw not_implemented();
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent("FetchCheckpointBegin", initialState.checkpointID).detail("CheckpointMetaData", initialState.toString());

	state CheckpointMetaData result;
	const CheckpointFormat format = initialState.getFormat();
	ASSERT(format != RocksDBKeyValues);
	if (format == DataMoveRocksCF || format == RocksDB) {
		wait(store(result, fetchRocksDBCheckpoint(cx, initialState, dir, cFun)));
	} else {
		throw not_implemented();
	}

	TraceEvent("FetchCheckpointEnd", initialState.checkpointID).detail("CheckpointMetaData", result.toString());
	return result;
}

ACTOR Future<CheckpointMetaData> fetchCheckpointRanges(Database cx,
                                                       CheckpointMetaData initialState,
                                                       std::string dir,
                                                       std::vector<KeyRange> ranges,
                                                       std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	TraceEvent(SevDebug, "FetchCheckpointRangesBegin", initialState.checkpointID)
	    .detail("CheckpointMetaData", initialState.toString())
	    .detail("Ranges", describe(ranges));
	ASSERT(!ranges.empty());

	state CheckpointMetaData result;
	const CheckpointFormat format = initialState.getFormat();
	if (format != RocksDBKeyValues) {
		if (format != DataMoveRocksCF) {
			throw not_implemented();
		}
		initialState.setFormat(RocksDBKeyValues);
		initialState.ranges = ranges;
		initialState.dir = dir;
		initialState.setSerializedCheckpoint(
		    ObjectWriter::toValue(RocksDBCheckpointKeyValues(ranges), IncludeVersion()));
	}

	wait(store(result, fetchRocksDBCheckpoint(cx, initialState, dir, cFun)));

	TraceEvent(SevDebug, "FetchCheckpointRangesEnd", initialState.checkpointID)
	    .detail("CheckpointMetaData", result.toString())
	    .detail("Ranges", describe(ranges));
	return result;
}

std::string serverCheckpointDir(const std::string& baseDir, const UID& checkpointId) {
	return joinPath(baseDir, checkpointId.toString());
}

std::string fetchedCheckpointDir(const std::string& baseDir, const UID& checkpointId) {
	return joinPath(baseDir, UID(checkpointId.first(), deterministicRandom()->randomUInt64()).toString());
}
