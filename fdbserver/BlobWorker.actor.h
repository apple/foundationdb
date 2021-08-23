/*
 * BlobWorker.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BLOBWORKER_ACTOR_G_H)
#define FDBSERVER_BLOBWORKER_ACTOR_G_H
#include "fdbserver/BlobWorker.actor.g.h"
#elif !defined(FDBSERVER_BLOBWORKER_ACTOR_H)
#define FDBSERVER_BLOBWORKER_ACTOR_H

#include "fdbclient/BlobWorkerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO this whole file should go away once blob worker isn't embedded in other roles

ACTOR Future<Void> blobWorker(BlobWorkerInterface bwInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);

#include "flow/unactorcompiler.h"
#endif