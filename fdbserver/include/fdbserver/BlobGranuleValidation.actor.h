/*
 * BlobGranuleValidation.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BLOBGRANULEVALIDATION_ACTOR_G_H)
#define FDBSERVER_BLOBGRANULEVALIDATION_ACTOR_G_H
#include "fdbserver/BlobGranuleValidation.actor.g.h"
#elif !defined(FDBSERVER_BLOBGRANULEVALIDATION_ACTOR_H)
#define FDBSERVER_BLOBGRANULEVALIDATION_ACTOR_H

#pragma once

#include "flow/flow.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "flow/actorcompiler.h" // has to be last include

/* Contains utility functions for validating blob granule data */

ACTOR Future<std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>> readFromBlob(
    Database cx,
    Reference<BlobConnectionProvider> bstore,
    KeyRange range,
    Version beginVersion,
    Version readVersion,
    Optional<Reference<Tenant>> tenant = Optional<Reference<Tenant>>());

ACTOR Future<std::pair<RangeResult, Version>> readFromFDB(Database cx, KeyRange range);

bool compareFDBAndBlob(RangeResult fdb,
                       std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob,
                       KeyRange range,
                       Version v,
                       bool debug);

void printGranuleChunks(const Standalone<VectorRef<BlobGranuleChunkRef>>& chunks);

ACTOR Future<Void> clearAndAwaitMerge(Database cx, KeyRange range);

ACTOR Future<Void> validateGranuleSummaries(Database cx,
                                            KeyRange range,
                                            Optional<Reference<Tenant>> tenantName,
                                            Promise<Void> testComplete);

ACTOR Future<Void> validateForceFlushing(Database cx,
                                         KeyRange range, // raw key range (includes tenant)
                                         double testDuration,
                                         Promise<Void> testComplete);

ACTOR Future<Void> checkFeedCleanup(Database cx, bool debug);

ACTOR Future<Void> killBlobWorkers(Database cx);
#include "flow/unactorcompiler.h"

#endif
