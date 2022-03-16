/*
 *ServerCheckpoint.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_SERVER_CHECKPOINT_ACTOR_G_H)
#define FDBSERVER_SERVER_CHECKPOINT_ACTOR_G_H
#include "fdbserver/ServerCheckpoint.actor.g.h"
#elif !defined(FDBSERVER_SERVER_CHECKPOINT_ACTOR_H)
#define FDBSERVER_SERVER_CHECKPOINT_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageCheckpoint.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

// An ICheckpointReader can read the contents of a checkpoint created from a KV store,
// i.e., by IKeyValueStore::checkpoint().
class ICheckpointReader {
public:
	// `token` is a serialized object defined by each derived ICheckpointReader class, to specify the
	// starting point for the underlying checkpoint.
	virtual Future<Void> init(StringRef token) = 0;

	// Scans the checkpoint, and returns the key-value pairs.
	virtual Future<RangeResult> nextKeyValues(const int rowLimit, const int ByteLimit) = 0;

	// Returns the next chunk of the serialized checkpoint.
	virtual Future<Standalone<StringRef>> nextChunk(const int ByteLimit) = 0;

	virtual Future<Void> close() = 0;

protected:
	virtual ~ICheckpointReader() {}
};

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint, UID logID);

// Delete a checkpoint.
ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint);

// Fetchs checkpoint to a local `dir`, `initialState` provides the checkpoint formats, location, restart point, etc.
// If cFun is provided, the progress can be checkpointed.
// Returns a CheckpointMetaData, which could contain KVS-specific results, e.g., the list of fetched checkpoint files.
ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun = nullptr);
#endif