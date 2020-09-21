/*
 * LockRange.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_LOCKRANGE_ACTOR_G_H)
	#define FDBCLIENT_LOCKRANGE_ACTOR_G_H
	#include "fdbclient/LockRange.actor.g.h"
#elif !defined(FDBCLIENT_LOCKRANGE_ACTOR_H)
	#define FDBCLIENT_LOCKRANGE_ACTOR_H

#include <vector>
#include <map>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/FDBTypes.h"

#include "flow/actorcompiler.h" // has to be last include

// Locks a range in the normal key space. If the database is already locked,
// then a database_locked error is thrown. If (part of) the range is already
// locked, then a range_locked error is thrown during commit.
ACTOR Future<Void> lockRange(Database cx, KeyRangeRef range, LockMode mode = LOCK_EXCLUSIVE);
ACTOR Future<Void> lockRanges(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode = LOCK_EXCLUSIVE);

// Unlocks a range in the normal key space. If the database is already locked,
// then a database_locked error is thrown. If the range is not locked, then
// a range_unlocked error is thrown during commit.
ACTOR Future<Void> unlockRange(Database cx, KeyRangeRef range, LockMode mode = LOCK_EXCLUSIVE);
ACTOR Future<Void> unlockRanges(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode = LOCK_EXCLUSIVE);

class RangeLockCache {
public:
	using Snapshot = std::vector<KeyRange>;
	using Mutations = Standalone<VectorRef<MutationRef>>;

	RangeLockCache(Snapshot snapshot, Version ver);

	void add(Version version, Mutations mutations);

	// Expire cached snapshots or mutations up to the given version.
	void expire(Version upTo);

	bool check(KeyRef key, Version version);
	bool intersects(KeyRangeRef range, Version version);

	// Serializes all mutations from the given version and on.
	Value getChanges(Version from);

private:
	// A version ordered locked ranges
	std::map<Version, Snapshot> snapshots;
	std::map<Version, Mutations> mutations;
};

#include "flow/unactorcompiler.h"
#endif
