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

	//BinaryWriter wr(IncludeVersion(ProtocolVersion::withLockRangeValue()));
	//wr << request;
	//tr->set(rangeLockKey, wr.toValue(), MutationRef::SetVersionstampedKey);

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

void RangeLockCache::add(Version version, const Requests& more) {
	auto& current = requests[version];
	current.append(current.arena(), more.begin(), more.size());
	current.arena().dependsOn(more.arena());
}

void RangeLockCache::add(Version version, const LockRequest& request) {
	auto& current = requests[version];
	current.push_back_deep(current.arena(), request);
	TraceEvent("RangeLockCache::add").detail("V", toString());
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

// PRE-CONDITION: there are lock versions larger than "upTo".
void RangeLockCache::expire(Version upTo) {
	// Make sure we have a snapshot right after "upTo" version
	auto snapIter = snapshots.upper_bound(upTo);
	auto reqIter = requests.upper_bound(upTo);
	if (snapIter != snapshots.end()) {
		if (reqIter != requests.end() && reqIter->first < snapIter->first) {
			ensureSnapshot(reqIter->first);
		}
	} else if (reqIter != requests.end()) {
		ensureSnapshot(reqIter->first);
	} else {
		TraceEvent(SevError, "RLCExpireNone");
		return;
	}

	TraceEvent("RLCExpire").detail("Version", upTo);
	for (auto it = snapshots.begin(); it != snapshots.end() && it->first <= upTo;) {
		it = snapshots.erase(it);
	}

	for (auto it = requests.begin(); it != requests.end() && it->first <= upTo;) {
		it = requests.erase(it);
	}
}

bool RangeLockCache::hasVersion(Version version) {
	return snapshots.find(version) != snapshots.end() || requests.find(version) != requests.end();
}

RangeLockCache::Reason RangeLockCache::check(KeyRef key, Version version, bool write) {
	Key end = keyAfter(key);
	return check(KeyRangeRef(key, end), version, write);
}

RangeLockCache::Reason RangeLockCache::check(KeyRangeRef range, Version version, bool write) {
	if (!hasVersion(version)) return DENIED_OLD_VERSION;

	ensureSnapshot(version);
	auto& snapshot = snapshots[version];
	// TODO: change snapshot for O(log N) cost
	for (const auto& lock : snapshot) {
		if (range.end <= lock.range.begin) break;
		if (range.begin >= lock.range.end) continue;

		if (lock.mode == LockMode::LOCK_EXCLUSIVE) {
			return DENIED_EXCLUSIVE_LOCK;
		} else if (lock.mode == LockMode::LOCK_READ_SHARED && write) {
			return DENIED_READ_LOCK;
		}
	}
	return OK;
}

RangeLockCache::Reason RangeLockCache::check(const LockRequest& request, Version version) {
	switch (request.mode) {
	case LockMode::LOCK_EXCLUSIVE:
	case LockMode::UNLOCK_EXCLUSIVE:
		return check(request.range, version, true);

	case LockMode::LOCK_READ_SHARED:
	case LockMode::UNLOCK_READ_SHARED:
		return check(request.range, version, false);

	default: break;
	}

	ASSERT(false);
	return OK;
}

Value RangeLockCache::getChanges(Version from) {
	ASSERT(false);
	return Value();
}

RangeLockCache::Snapshot RangeLockCache::getSnapshot(Version at) {
	ASSERT(hasVersion(at));
	ensureSnapshot(at);
	return snapshots[at];
}

// TODO: serialization across restarts? return value is stored at txnStateStore
Value RangeLockCache::getSnapshotValue(Version at) {
	Snapshot snapshot = getSnapshot(at);
	std::cout << __FUNCTION__ << toString() << "\n";
	return BinaryWriter::toValue(snapshot, IncludeVersion());
}

void RangeLockCache::setSnapshotValue(Version at, Value value) {
	TraceEvent("LockRangeValueDecode").detail("V", value);

	Snapshot snapshot = BinaryReader::fromStringRef<Snapshot>(value, IncludeVersion());
	setSnapshot(at, snapshot);
	std::cout << __FUNCTION__ << toString() << "\n";
}

std::string RangeLockCache::toString(Version version, Snapshot snapshot) {
	std::string s;
	char buf[16];
	sprintf(buf, "%" PRId64, version);
	s.append("Version: ").append(buf);
	s.append("\n");
	for (const auto& req : snapshot) {
		s.append("  Range: " + printable(req.range));
		s.append(", mode: ").append(getLockModeText(req.mode));
		s.append("\n");
	}
	return s;
}

std::string RangeLockCache::toString() {
	std::string s;
	bool first = true;
	for (const auto& [version, snapshot] : snapshots) {
		if (first) {
			s.append("=== Snapshots ===\n");
			first = false;
		} else {
			s.append("\n");
		}
		s.append(toString(version, snapshot));
	}

	first = true;
	for (const auto& [version, snapshot] : requests) {
		if (first) {
			s.append("=== Requests ===\n");
			first = false;
		} else {
			s.append("\n");
		}
		s.append(toString(version, snapshot));
	}
	return s;
}

void RangeLockCache::ensureSnapshot(Version version) {
	auto snapIter = snapshots.lower_bound(version);
	if (snapIter != snapshots.end() && snapIter->first == version) return;

	ASSERT(requests.find(version) != requests.end());

	if (snapIter != snapshots.begin()) {
		snapIter--;
	}
	if (snapIter == snapshots.end()) {
		// No snapshots before, build first one.
		const auto reqIter = requests.begin();
		ASSERT(reqIter != requests.end());

		std::vector<LockRequest> fLocks;
		for (const auto& req : reqIter->second) {
			ASSERT(req.mode == LockMode::LOCK_EXCLUSIVE || req.mode == LockMode::LOCK_READ_SHARED);
			fLocks.push_back(req);
		}
		std::sort(fLocks.begin(), fLocks.end(), RangeLockCache::lockLess);

		auto& snap = snapshots[reqIter->first];
		for (const auto& req : fLocks) {
			snap.push_back_deep(snap.arena(), req);
		}
		snapIter = snapshots.begin();
	}

	// Start building snapshots till version
	while (snapIter->first < version) {
		snapIter = buildSnapshot(snapIter);
	}
	ASSERT(snapIter->first == version);
}

RangeLockCache::SnapshotIterator RangeLockCache::buildSnapshot(RangeLockCache::SnapshotIterator it) {
	const auto reqIter = requests.lower_bound(it->first + 1);
	ASSERT(reqIter != requests.end());

	std::map<KeyRangeRef, LockRequest, bool (*)(const KeyRangeRef&, const KeyRangeRef&)> locks(rangeLess);
	for (const auto& req : it->second) {
		locks.insert(std::make_pair(req.range, req));
	}

	for (const auto& req : reqIter->second) {
		auto it = locks.lower_bound(req.range);

		switch (req.mode) {
		case LockMode::LOCK_EXCLUSIVE:
		case LockMode::LOCK_READ_SHARED:
			if (it == locks.end()) {
				if (!locks.empty()) {
					ASSERT(rangeLess(locks.rbegin()->first, req.range));
				}
			} else {
				ASSERT(req.range.end <= it->first.begin); // No overlap
			}
			locks.insert(std::make_pair(req.range, req));
			break;

		case LockMode::UNLOCK_EXCLUSIVE:
		case LockMode::UNLOCK_READ_SHARED:
			ASSERT(it != locks.end());
			ASSERT(it->first == req.range);
			locks.erase(it);
			break;

		default:
			ASSERT(false);
		}
	}

	Snapshot& snapshot = snapshots[reqIter->first];
	ASSERT_EQ(snapshot.size(), 0);
	for (const auto& [_, req] : locks) {
		snapshot.push_back_deep(snapshot.arena(), req);
	}
	it++;
	return it;
}

namespace {
TEST_CASE("/fdbclient/LockRange/KeyRangeMap") {
	KeyRangeMap<LockStatus> locks;

	LockStatus a = locks["a"_sr];
	ASSERT(a == LockStatus::UNLOCKED);
	std::cout << "a status is: " << (uint8_t)a << "\n";

	return Void();
}

TEST_CASE("/fdbclient/LockRange/Cache") {
	RangeLockCache cache;

	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

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

	std::cout << cache.toString() << "\n";
	return Void();
}

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

} // anonymous namespace
