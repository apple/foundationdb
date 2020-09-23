/*
 * LockRange.actor.cpp
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

#include <cinttypes>
#include <vector>

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/LockRange.actor.h"
#include "fdbclient/ReadYourWrites.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

const char* getLockModeText(LockMode mode) {
	switch (mode) {
	case LockMode::LOCK_EXCLUSIVE:
		return "LOCK_EXCLUSIVE";

	case LockMode::LOCK_READ_SHARED:
		return "LOCK_READ_SHARED";

	case LockMode::UNLOCK_EXCLUSIVE:
		return "UNLOCK_EXCLUSIVE";

	case LockMode::UNLOCK_READ_SHARED:
		return "UNLOCK_READ_SHARED";

	default:
		return "<undefined>";
	}
}

ACTOR Future<Void> lockRange(Transaction* tr, KeyRangeRef range, bool checkDBLock, LockMode mode) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	tr->atomicOp(rangeLockKey, encodeRangeLock(LockRequest(range, mode)), MutationRef::LockRange);
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}
	tr->addWriteConflictRange(range);
	return Void();
}

ACTOR Future<Void> lockRange(Database cx, KeyRangeRef range, LockMode mode) {
	state Transaction tr(cx);
	loop {
		try {
			wait(lockRange(&tr, range, true, mode));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> lockRanges(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode) {
	if (ranges.size() > 1) {
		// Check ranges are disjoint
		std::sort(ranges.begin(), ranges.end(), [](const KeyRangeRef& a, const KeyRangeRef& b) {
			return a.begin == b.begin ? a.end < b.end : a.begin < b.begin;
		});
		for (int i = 1; i < ranges.size(); i++) {
			if (ranges[i - 1].intersects(ranges[i])) {
				throw client_invalid_operation();
			}
		}
	}

	state Transaction tr(cx);
	loop {
		try {
			state bool checkDBLock = true;
			for (const auto& range : ranges) {
				wait(lockRange(&tr, range, checkDBLock, mode));
				checkDBLock = false;
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockRange(Transaction* tr, KeyRangeRef range, bool checkDBLock, LockMode mode) {
	ASSERT(mode == LockMode::UNLOCK_EXCLUSIVE || mode == LockMode::UNLOCK_READ_SHARED);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	tr->atomicOp(rangeLockKey, encodeRangeLock(LockRequest(range, mode)), MutationRef::LockRange);
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}
	tr->addWriteConflictRange(range);
	return Void();
}

ACTOR Future<Void> unlockRange(Database cx, KeyRangeRef range, LockMode mode) {
	ASSERT(mode == LockMode::UNLOCK_EXCLUSIVE || mode == LockMode::UNLOCK_READ_SHARED);
	state Transaction tr(cx);
	loop {
		try {
			wait(unlockRange(&tr, range, true, mode));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockRange(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode) {
	if (ranges.size() > 1) {
		// Check ranges are disjoint
		std::sort(ranges.begin(), ranges.end(), [](const KeyRangeRef& a, const KeyRangeRef& b) {
			return a.begin == b.begin ? a.end < b.end : a.begin < b.begin;
		});
		for (int i = 1; i < ranges.size(); i++) {
			if (ranges[i - 1].intersects(ranges[i])) {
				throw client_invalid_operation();
			}
		}
	}

	state Transaction tr(cx);
	loop {
		try {
			state bool checkDBLock = true;
			for (const auto& range : ranges) {
				wait(unlockRange(&tr, range, checkDBLock, mode));
				checkDBLock = false;
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

RangeLockCache::RangeLockCache(Snapshot snapshot, Version version) {
	snapshots[version] = snapshot;
}

void RangeLockCache::add(Version version, Mutations more) {
	auto& current = mutations[version];
	current.append(current.arena(), more.begin(), more.size());
	current.arena().dependsOn(more.arena());
}

void RangeLockCache::expire(Version upTo) {
	// Make sure we have a snapshot right after "upTo" version
	auto snapIter = snapshots.upper_bound(upTo);
	auto mutIter = mutations.upper_bound(upTo);
	if (snapIter != snapshots.end()) {
		if (mutIter != mutations.end() && mutIter->first < snapIter->first) {
			ensureSnapshot(mutIter->first);
		}
	} else if (mutIter != mutations.end()) {
		ensureSnapshot(mutIter->first);
	} else {
		TraceEvent("RLCExpireNone");
		return;
	}

	TraceEvent("RLCExpire").detail("Version", upTo);
	for (auto it = snapshots.begin(); it != snapshots.end() && it->first <= upTo;) {
		it = snapshots.erase(it);
	}

	for (auto it = mutations.begin(); it != mutations.end() && it->first <= upTo;) {
		it = mutations.erase(it);
	}
}

bool RangeLockCache::hasVersion(Version version) {
	return snapshots.find(version) != snapshots.end() || mutations.find(version) != mutations.end();
}

bool RangeLockCache::check(KeyRef key, Version version) {
	ensureSnapshot(version);
	return true;
}

bool RangeLockCache::check(KeyRangeRef range, Version version) {
	ensureSnapshot(version);
	return true;
}

Value RangeLockCache::getChanges(Version from) {
	ASSERT(false);
	return Value();
}

void RangeLockCache::ensureSnapshot(Version version) {
	// TODO
}
