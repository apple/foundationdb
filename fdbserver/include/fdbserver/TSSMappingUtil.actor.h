/*
 * TSSMappingUtil.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(TSS_MAPPING_UTIL_SERVER_G_H)
#define TSS_MAPPING_UTIL_SERVER_G_H
#include "fdbserver/TSSMappingUtil.actor.g.h"
#elif !defined(TSS_MAPPING_UTIL_SERVER_H)
#define TSS_MAPPING_UTIL_SERVER_H

#include "fdbclient/StorageServerInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * Collection of utility functions for dealing with the TSS mapping
 */

// Reads the current cluster TSS mapping as part of the RYW transaction
ACTOR Future<Void> readTSSMappingRYW(Reference<ReadYourWritesTransaction> tr,
                                     std::map<UID, StorageServerInterface>* tssMapping);

// Reads the current cluster TSS mapping as part of the given Transaction
ACTOR Future<Void> readTSSMapping(Transaction* tr, std::map<UID, StorageServerInterface>* tssMapping);

// Removes the TSS pairs from the cluster
ACTOR Future<Void> removeTSSPairsFromCluster(Database cx, std::vector<std::pair<UID, UID>> pairsToRemove);

#include "flow/unactorcompiler.h"
#endif
