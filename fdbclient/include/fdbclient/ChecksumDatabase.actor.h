/*
 * ChecksumDatabase.actor.h
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
#ifndef FDBCLIENT_CHECKSUMDATABASE_ACTOR_H
#define FDBCLIENT_CHECKSUMDATABASE_ACTOR_H
#pragma once

#define XXH_STATIC_LINKING_ONLY // Ensure full xxhash definitions are exposed

// Includes that might be needed by ChecksumResult or the actor
#include <cstdint> // For uint64_t, int64_t
#include "fdbclient/FDBTypes.h" // For KeyRange, Optional
#include "flow/flow.h" // For Future, etc.
#include "flow/xxhash.h" // For XXH64_state_t

// Define ChecksumResult struct globally within the header, inside its namespace
namespace fdb {
struct ChecksumResult {
	uint64_t checksum;
	int64_t totalBytes;
	int64_t totalKeys;
};
} // namespace fdb

#ifdef NO_INTELLISENSE
#ifndef FDBCLIENT_CHECKSUMDATABASE_ACTOR_G_H_WRAPPER // New guard specific to this block
#define FDBCLIENT_CHECKSUMDATABASE_ACTOR_G_H_WRAPPER
#include "fdbclient/ChecksumDatabase.actor.g.h"
#endif // FDBCLIENT_CHECKSUMDATABASE_ACTOR_G_H_WRAPPER
#else
// For Intellisense (NO_INTELLISENSE is not defined)
// #include "flow/actorcompiler.h" // This was correctly commented out previously
#include "fdbclient/DatabaseContext.h" // Provides 'Database' type, needed for the signature

#ifndef ACTOR_COMPILER
namespace fdb {
// Actor declared within fdb namespace, returning the namespaced fdb::ChecksumResult struct
// The ChecksumResult struct is now defined globally above.
ACTOR Future<fdb::ChecksumResult> calculateDatabaseChecksum(Database cx,
                                                            Optional<KeyRange> range = Optional<KeyRange>());

} // namespace fdb
#endif // ACTOR_COMPILER

#endif // NO_INTELLISENSE

#endif // FDBCLIENT_CHECKSUMDATABASE_ACTOR_H