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

ACTOR Future<Void> lockRange(Transaction* tr, RangeLockCache* cache, LockRequest request, bool checkDBLock) {
	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	// TODO: use cache to check request and generate mutations
	RangeLockCache::Reason reason = cache->tryAdd(tr, request);
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
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);

	loop {
		try {
			wait(lockRange(&tr, &cx.getPtr()->rangeLockCache, request, true));

			// Make concurrent lockRange() calls conflict.
			tr.addReadConflictRange(KeyRangeRef(rangeLockVersionKey, keyAfter(rangeLockVersionKey)));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
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

/*
void krmSetPreviouslyEmptyRange( Transaction* tr, const KeyRef& mapPrefix, const KeyRangeRef& keys, const ValueRef& newValue, const ValueRef& oldEndValue )
{
	KeyRange withPrefix = KeyRangeRef( mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString() );
	tr->set( withPrefix.begin, newValue );
	tr->set( withPrefix.end, oldEndValue );
}*/

RangeLockCache::Reason RangeLockCache::tryAdd(Transaction*tr, const LockRequest& request) {
	LockStatus oldValue = locks[request.range.end];
	auto ranges = locks.intersectingRanges(request.range);

	if (isLocking(request.mode)) {
		// For now, deny any repeated locking ops, including READ locks.
		// In the future, we may allow upgrade read locks to exclusive locks.
		if (!ranges.empty()) return ALREADY_LOCKED;

		LockStatus newValue =
		    request.mode == LockMode::LOCK_EXCLUSIVE ? LockStatus::EXCLUSIVE_LOCKED : LockStatus::SHARED_READ_LOCKED;
		krmSetPreviouslyEmptyRange(tr, lockedKeyRanges.begin, request.range,
		                           StringRef(reinterpret_cast<uint8_t*>(&newValue), sizeof(uint8_t)),
		                           StringRef(reinterpret_cast<uint8_t*>(&oldValue), sizeof(uint8_t)));
		return OK;
	}

	// Handle unlock requests
	if (ranges.empty()) return ALREADY_UNLOCKED;

	LockStatus expectedStatus =
	    request.mode == LockMode::UNLOCK_EXCLUSIVE ? LockStatus::EXCLUSIVE_LOCKED : LockStatus::SHARED_READ_LOCKED;
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
	tr->set(withPrefix.end, StringRef(reinterpret_cast<uint8_t*>(&oldValue), sizeof(uint8_t)));

	return OK;
}


RangeLockCache::Reason RangeLockCache::check(KeyRef key, bool write) {
	Key end = keyAfter(key);
	return check(KeyRangeRef(key, end), write);
}

RangeLockCache::Reason RangeLockCache::check(KeyRangeRef range, bool write) {
	// TODO: change snapshot for O(log N) cost
	return OK;
}

std::string RangeLockCache::toString() {
	std::string s;
	// TODO
	return s;
}

void RangeLockCache::add(KeyRef beginKey, LockStatus status, Version commitVersion) {
	lockVersion = commitVersion;

	// TODO
}

void RangeLockCache::clear(KeyRangeRef range) {
	// TODO
}

RangeLockCache::Snapshot RangeLockCache::getSnapshot() {
	return Value();
}

void RangeLockCache::setSnapshot(Version version, RangeLockCache::Snapshot snapshot) {
	// TODO
}

namespace {
TEST_CASE("/fdbclient/LockRange/KeyRangeMap") {
	KeyRangeMap<LockStatus> locks;

	LockStatus a = locks["a"_sr];
	ASSERT(a == LockStatus::UNLOCKED);
	std::cout << "a status is: " << static_cast<uint32_t>(a) << "\n";

	return Void();
}

TEST_CASE("/fdbclient/LockRange/Cache") {
	RangeLockCache cache;
	Transaction tr;

	LockRequest r1(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE);
	cache.tryAdd(&tr, r1);
/*
	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("d"_sr, v1), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v1), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v1), RangeLockCache::OK);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2), RangeLockCache::DENIED_READ_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v2), RangeLockCache::OK);
*/
	std::cout << cache.toString() << "\n";
	return Void();
}

#if 0
TEST_CASE("/fdbclient/LockRange/CacheExpire") {
	RangeLockCache cache;
	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v1 - 1), false);
	ASSERT_EQ(cache.hasVersion(v1 + 1), false);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.hasVersion(v2 - 10), false);
	ASSERT_EQ(cache.hasVersion(v2 + 100), false);

	cache.expire(v1);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_OLD_VERSION);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "6"_sr), v1, /*write=*/false), RangeLockCache::DENIED_OLD_VERSION);

	ASSERT_EQ(cache.check("a"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("d"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v2), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2), RangeLockCache::DENIED_READ_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2, /*write=*/false), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2, /*write=*/false), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v2), RangeLockCache::OK);
	std::cout << cache.toString() << "\n";
	return Void();
}

TEST_CASE("/fdbclient/LockRange/Unlock") {
	RangeLockCache cache;

	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	RangeLockCache::Requests requests3; // Unlocks [d, e), Lock [p, t)
	const Version v3 = 300;
	requests3.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::UNLOCK_EXCLUSIVE));
	requests3.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("p"_sr, "t"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v3, requests3);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.hasVersion(v3), true);

	std::cout << "Before expire: \n" << cache.toString() << "\n";
	cache.expire(v2);

	ASSERT_EQ(cache.check("a"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	std::cout << cache.toString() << "\n";
	ASSERT_EQ(cache.check("d"_sr, v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check("p"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("q"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v3), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v3), RangeLockCache::DENIED_READ_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v3, /*write=*/false), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v3, /*write=*/false), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("f"_sr, "p"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("t"_sr, "ta"_sr), v3), RangeLockCache::OK);
	return Void();
}
#endif

} // anonymous namespace
