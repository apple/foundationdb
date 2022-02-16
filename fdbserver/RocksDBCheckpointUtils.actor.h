/*
 *RocksDBCheckpointUtils.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_G_H)
#define FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_G_H
#include "fdbserver/RocksDBCheckpointUtils.actor.g.h"
#elif !defined(FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_H)
#define FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageCheckpoint.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

// Fetch the checkpoint file(s) to local dir, the checkpoint is specified by initialState.
// If cFun is provided, the fetch progress can be checkpointed, so that next time, the fetch process
// can be continued, in case of crash.
ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(
    Database cx,
    CheckpointMetaData initialState,
    std::string dir,
    std::function<Future<Void>(const CheckpointMetaData&)> cFun = nullptr);
#endif