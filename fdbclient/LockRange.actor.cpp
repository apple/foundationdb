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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/LockRange.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

const std::string lockModeToString(LockMode mode) {
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

const std::string lockStatusToString(LockStatus s) {
	switch (s) {
	case LockStatus::EXCLUSIVE_LOCKED:
		return "EXCLUSIVE_LOCKED";
	case LockStatus::READ_LOCKED:
		return "READ_LOCKED";
	case LockStatus::UNLOCKED:
		return "UNLOCKED";
	}

	ASSERT(false);
	throw internal_error();
}

ACTOR Future<Void> lockRange(Transaction* tr, RangeLockCache* cache, LockRequest request, bool checkDBLock) {
	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	if (systemKeys.intersects(request.range)) {
		throw range_locks_access_denied();
	}

	RangeLockCache::Reason reason = cache->tryAdd(tr, request);
	if (reason == RangeLockCache::ALREADY_UNLOCKED && isUnlocking(request.mode)) {
		// When getting unknown_result and retrying, short-circuit unlock request.
		return Void();
	}
	if (reason != RangeLockCache::OK) {
		throw range_locks_access_denied();
	}

	// Update range lock version
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}

	if (request.mode == LockMode::LOCK_EXCLUSIVE || request.mode == LockMode::LOCK_READ_SHARED) {
		tr->addReadConflictRange(KeyRangeRef(request.range));
	}

	return Void();
}

ACTOR Future<Void> lockRange(Database cx, LockRequest request) {
	state Transaction tr(cx);

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			TraceEvent(SevDebug, "LockRangeRequest")
			    .detail("Range", request.range)
			    .detail("Mode", lockModeToString(request.mode));
			wait(lockRange(&tr, &cx.getPtr()->rangeLockCache, request, true));

			// Make concurrent lockRange() calls conflict.
			tr.addReadConflictRange(KeyRangeRef(rangeLockVersionKey, keyAfter(rangeLockVersionKey)));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw;
			TraceEvent(SevDebug, "LockRangeRequestError")
			    .detail("Range", request.range)
			    .detail("Mode", lockModeToString(request.mode))
			    .error(e, true);
			if (e.code() == error_code_transaction_too_old || e.code() == error_code_commit_unknown_result) {
				tr.fullReset();
			} else {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> lockRanges(Database cx, std::vector<LockRequest> requests) {
	state Transaction tr(cx);
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);

	loop {
		try {
			state bool checkDBLock = true;
			for (const auto& r : requests) {
				wait(lockRange(&tr, &cx.getPtr()->rangeLockCache, r, checkDBLock));
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

RangeLockCache::Reason RangeLockCache::tryAdd(Transaction*tr, const LockRequest& request) {
	LockStatus oldValue = locks[request.range.end];
	auto ranges = locks.intersectingRanges(request.range);

	if (isLocking(request.mode)) {
		for (const auto& r : ranges) {
			if (r.cvalue() == LockStatus::UNLOCKED) continue;
			if (request.mode == LockMode::LOCK_EXCLUSIVE) return ALREADY_LOCKED;
		}

		LockStatus newValue =
		    request.mode == LockMode::LOCK_EXCLUSIVE ? LockStatus::EXCLUSIVE_LOCKED : LockStatus::READ_LOCKED;
		krmSetPreviouslyEmptyRange(tr, lockedKeyRanges.begin, request.range,
		                           StringRef(reinterpret_cast<uint8_t*>(&newValue), sizeof(uint8_t)),
		                           StringRef(reinterpret_cast<uint8_t*>(&oldValue), sizeof(uint8_t)));
		return OK;
	}

	// Handle unlock requests
	if (ranges.empty()) return ALREADY_UNLOCKED;

	LockStatus expectedStatus =
	    request.mode == LockMode::UNLOCK_EXCLUSIVE ? LockStatus::EXCLUSIVE_LOCKED : LockStatus::READ_LOCKED;
	for (const auto& r : ranges) {
		if (r.cvalue() != expectedStatus) {
			throw range_locks_access_denied();
		}
	}

	// Use toString() to be consistent with how krmSetPreviouslyEmptyRange sets keys
	KeyRange withPrefix = KeyRangeRef(lockedKeyRanges.begin.toString() + request.range.begin.toString(),
	                                  lockedKeyRanges.begin.toString() + request.range.end.toString());
	tr->clear(withPrefix);
	LockStatus newValue = LockStatus::UNLOCKED;
	tr->set(withPrefix.begin, StringRef(reinterpret_cast<uint8_t*>(&newValue), sizeof(uint8_t)));
	if (newValue != oldValue) {
		tr->set(withPrefix.end, StringRef(reinterpret_cast<uint8_t*>(&oldValue), sizeof(uint8_t)));
	}
	return OK;
}

RangeLockCache::Reason RangeLockCache::check(KeyRef key, bool write) const {
	if (ignoreLocks) return OK;
	Key end = keyAfter(key);
	return check(KeyRangeRef(key, end), write);
}

RangeLockCache::Reason RangeLockCache::check(KeyRangeRef range, bool write) const {
	if (ignoreLocks) return OK;
	auto ranges = locks.intersectingRanges(range);

	for (const auto& r : ranges) {
		if (r.cvalue() == LockStatus::EXCLUSIVE_LOCKED) {
			return DENIED_EXCLUSIVE_LOCK;
		}
		if (write && r.cvalue() == LockStatus::READ_LOCKED) {
			return DENIED_READ_LOCK;
		}
	}
	return OK;
}

std::string RangeLockCache::toString() const {
	std::string s;
	s.append(format("Version: %" PRId64 "\n", lockVersion));
	for (const auto& r : locks.ranges()) {
		s.append(r.begin().toString()).append(" - ").append(r.end().toString());
		s.append(" " + lockStatusToString(r.cvalue()) + "\n");
	}

	return s;
}

void RangeLockCache::add(KeyRef beginKey, LockStatus status, Version commitVersion) {
	ASSERT(commitVersion >= lockVersion);
	lockVersion = commitVersion;

	if (beginKey == allKeys.end) return;
	KeyRef end = locks.rangeContaining(beginKey).end();
	locks.insert(KeyRangeRef(beginKey, end), status);
}

void RangeLockCache::clear(KeyRangeRef range) {
	locks.insert(range, LockStatus::UNLOCKED);
}

std::pair<RangeLockCache::Snapshot, Version> RangeLockCache::getSnapshot() const {
	Snapshot snapshot;

	for (const auto& r : locks.ranges()) {
		snapshot.emplace_back(r.range(), r.cvalue());
	}
	return { snapshot, lockVersion };
}

void RangeLockCache::setSnapshot(Version version, const RangeLockCache::Snapshot& snapshot) {
	lockVersion = version;
	clear(allKeys);

	for (const auto& pair : snapshot) {
		locks.insert(pair.first, pair.second);
	}
}

namespace {
TEST_CASE("/fdbclient/LockRange/KeyRangeMap") {
	KeyRangeMap<LockStatus> locks;

	LockStatus a = locks["a"_sr];
	ASSERT(a == LockStatus::UNLOCKED);
	std::cout << "a status is: " << static_cast<uint32_t>(a) << "\n";

	RangeLockCache cache;

	const Version v1 = 100;
	cache.add("a"_sr, LockStatus::EXCLUSIVE_LOCKED, v1);
	cache.add("d"_sr, LockStatus::UNLOCKED, v1);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("d"_sr, v1), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), true), RangeLockCache::OK);

	const Version v2 = 200;
	cache.add("d"_sr, LockStatus::EXCLUSIVE_LOCKED, v2);
	cache.add("e"_sr, LockStatus::UNLOCKED, v2);
	cache.add("ee"_sr, LockStatus::READ_LOCKED, v2);
	cache.add("exxx"_sr, LockStatus::UNLOCKED, v2);

	ASSERT_EQ(cache.hasVersion(v1), false);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), true), RangeLockCache::DENIED_READ_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), true), RangeLockCache::OK);

	std::cout << "Cache: " << cache.toString() << "\n";
	return Void();
}

TEST_CASE("/fdbclient/LockRange/Unlock") {
	RangeLockCache cache;
	const Version v1 = 100;
	cache.add("a"_sr, LockStatus::EXCLUSIVE_LOCKED, v1);
	cache.add("d"_sr, LockStatus::UNLOCKED, v1);

	const Version v2 = 200;
	cache.add("d"_sr, LockStatus::EXCLUSIVE_LOCKED, v2);
	cache.add("e"_sr, LockStatus::UNLOCKED, v2);
	cache.add("ee"_sr, LockStatus::READ_LOCKED, v2);
	cache.add("exxx"_sr, LockStatus::UNLOCKED, v2);

	const Version v3 = 300;
	// clear [d, e) lock
	cache.clear(KeyRangeRef("d"_sr, "e"_sr));
	cache.add("p"_sr, LockStatus::EXCLUSIVE_LOCKED, v3);
	cache.add("t"_sr, LockStatus::UNLOCKED, v3);

	ASSERT_EQ(cache.hasVersion(v1), false);
	ASSERT_EQ(cache.hasVersion(v2), false);
	ASSERT_EQ(cache.hasVersion(v3), true);
	std::cout << "Cache: " << cache.toString() << "\n";

	auto pair = cache.getSnapshot();
	RangeLockCache cache2;
	cache2.setSnapshot(pair.second, pair.first);
	std::cout << "Cache2: " << cache.toString() << "\n";
	ASSERT_EQ(cache2.hasVersion(v3), true);

	ASSERT_EQ(cache2.check("a"_sr, true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache2.check("b"_sr, true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache2.check("d"_sr, true), RangeLockCache::OK);
	ASSERT_EQ(cache2.check("p"_sr, true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache2.check("q"_sr, true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);

	ASSERT_EQ(cache2.check(KeyRangeRef("b"_sr, "d"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache2.check(KeyRangeRef("c"_sr, "e"_sr), true), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache2.check(KeyRangeRef("d"_sr, "e"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache2.check(KeyRangeRef("0"_sr, "9"_sr), true), RangeLockCache::OK);

	ASSERT_EQ(cache2.check(KeyRangeRef("ef"_sr, "eg"_sr), true), RangeLockCache::DENIED_READ_LOCK);
	ASSERT_EQ(cache2.check(KeyRangeRef("ef"_sr, "eg"_sr), /*write=*/false), RangeLockCache::OK);

	ASSERT_EQ(cache2.check(KeyRangeRef("ea"_sr, "ee"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache2.check(KeyRangeRef("ea"_sr, "ee"_sr), /*write=*/false), RangeLockCache::OK);
	ASSERT_EQ(cache2.check(KeyRangeRef("ey"_sr, "g"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache2.check(KeyRangeRef("f"_sr, "p"_sr), true), RangeLockCache::OK);
	ASSERT_EQ(cache2.check(KeyRangeRef("t"_sr, "ta"_sr), true), RangeLockCache::OK);

	return Void();
}

} // anonymous namespace
