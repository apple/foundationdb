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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CHECKSUMDATABASE_ACTOR_G_H)
#define FDBCLIENT_CHECKSUMDATABASE_ACTOR_G_H
#include "fdbclient/ChecksumDatabase.actor.g.h"
#elif !defined(FDBCLIENT_CHECKSUMDATABASE_ACTOR_H)
#define FDBCLIENT_CHECKSUMDATABASE_ACTOR_H

// Define XXH_STATIC_LINKING_ONLY before including xxhash.h
#define XXH_STATIC_LINKING_ONLY
#include "flow/xxhash.h" // For XXH64_state_t and checksum functions

// Includes for types used in actor signatures and ChecksumResult
#include "fdbclient/DatabaseContext.h" // For Database
#include "fdbclient/FDBTypes.h" // For TenantName, KeyRange, Key, Version
#include "flow/flow.h" // For Future, Optional, serializer, etc.
#include "flow/FileIdentifier.h" // For FileIdentifier (if used, else remove)

namespace fdb {
struct ChecksumResult {
	uint64_t checksum;
	int64_t totalBytes;
	int64_t totalKeys;

	ChecksumResult() : checksum(0), totalBytes(0), totalKeys(0) {}
	ChecksumResult(uint64_t checksum, int64_t totalBytes, int64_t totalKeys)
	  : checksum(checksum), totalBytes(totalBytes), totalKeys(totalKeys) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checksum, totalBytes, totalKeys);
	}
};
} // namespace fdb

// This must be the last #include.
#include "flow/actorcompiler.h"

namespace fdb {
ACTOR Future<ChecksumResult> calculateDatabaseChecksum(Database cx, Optional<KeyRange> range);
} // namespace fdb

#include "flow/unactorcompiler.h"

#endif // FDBCLIENT_CHECKSUMDATABASE_ACTOR_H